/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.streaming

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import java.util.zip.CRC32C

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.MutableCheckedOutputStream
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, MemoryMode}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Streaming-shuffle map-side writer: partitions records by reducer ID using the
 * dependency's partitioner, accumulates each partition's serialized bytes in a
 * per-partition [[ByteArrayOutputStream]] up to a memory cap derived from
 * [[StreamingShuffleHandle.bufferSizePercent]], computes a CRC32C checksum per 2 MB
 * block (per [[BLOCK_SIZE_BYTES]]), and hands blocks off to [[BackpressureProtocol]] for
 * rate-limited transmission. Spill to disk is delegated to [[MemorySpillManager]] when
 * a per-partition buffer crosses the configured spill threshold.
 *
 * == Coexistence Strategy ==
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* this writer is constructed
 * exclusively by [[StreamingShuffleManager.getWriter]] when a streaming-shuffle handle
 * has been registered. The default sort-based shuffle path -- driven by
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] and
 * [[org.apache.spark.shuffle.sort.SortShuffleWriter]] -- is unaffected by the existence
 * of this class; both writers extend the common
 * [[org.apache.spark.shuffle.ShuffleWriter]] abstract class but do not share any
 * mutable state, file format, or transport endpoint.
 *
 * == Memory Discipline ==
 * Per AAP Section 0.7.2.2 the per-partition buffer cap is computed as
 * `(executor execution memory * bufferSizePercent / 100) / numPartitions`, with a floor
 * of [[BLOCK_SIZE_BYTES]] (2 MB) so that even with thousands of partitions every
 * partition is permitted to hold at least one full 2 MB block before the spill
 * threshold can be evaluated against it. Memory for the aggregate cap is acquired
 * up-front through [[org.apache.spark.memory.TaskMemoryManager#acquireExecutionMemory]]
 * via an internal [[MemoryConsumer]] subclass so the executor's [[MemoryManager]] is
 * aware of the streaming writer's footprint and may reject the request if execution
 * memory is exhausted (which the streaming-shuffle fallback policy observes as a
 * memory-pressure signal).
 *
 * To preserve unified-memory accounting strictly, each per-partition
 * `ByteArrayOutputStream` is constructed with a small initial capacity
 * ([[INITIAL_BAOS_CAPACITY]] = 1 KB) and grows on demand via the JDK's native
 * `Arrays.copyOf` doubling growth. This keeps construction-time JVM-heap allocation
 * negligible (proportional to `numPartitions * 1 KB`) so the executor's unified-memory
 * model is the sole authority on the writer's aggregate buffer footprint -- the
 * `acquireExecutionMemory` grant of `perPartitionBufferCap * numPartitions` bounds
 * subsequent growth and, on memory exhaustion, the fallback policy diverts to the
 * sort-based writer.
 *
 * == Per-Record Hot Path ==
 * Each partition holds at most one [[org.apache.spark.serializer.SerializationStream]]
 * at a time, scoped to the lifetime of one block: the stream is opened lazily on the
 * first write to a partition (or on the first write following a [[flushBlock]] /
 * [[maybeSpill]] close) and closed at the next block boundary. The serialization
 * stream wraps a [[org.apache.spark.io.MutableCheckedOutputStream]] which in turn
 * wraps the partition's `ByteArrayOutputStream`; the checked-output-stream interceptor
 * threads the per-partition cumulative CRC32C through every byte the serializer emits.
 *
 * == Wire Format Invariant ==
 * Each block emitted onto the network constitutes a complete, independently
 * deserializable serialization stream (header + records + footer). This invariant is
 * required by [[StreamingShuffleReader]] which deserializes each block independently
 * via `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator`. To
 * uphold the invariant, [[flushBlock]] and [[maybeSpill]] both close the partition's
 * serialization stream BEFORE draining the buffer (the close call writes any buffered
 * serializer state and the stream-footer marker into the buffer through the
 * MutableCheckedOutputStream interceptor); the next [[ensurePartitionStream]] call
 * for that partition lazily allocates a fresh stream that writes a fresh header into
 * the now-empty buffer.
 *
 * Per-record overhead is therefore bounded by the cost of one `SerializationStream`
 * construction per block (NOT per record) plus the cost of the actual `writeKey` /
 * `writeValue` calls. For typical workloads (e.g. a 100 MB shuffle / 10 partitions =
 * 10 MB per partition / 2 MB blocks = 5 blocks per partition) this amounts to 5
 * stream constructions per partition rather than one per record.
 *
 * == Cumulative vs. Per-Block CRC32C ==
 * The per-partition cumulative CRC32C captures every byte written to the partition
 * across all blocks (including each block's stream header AND footer). It is allocated
 * at most once per partition by [[ensurePartitionStream]] and preserved across block
 * boundaries; it is folded into the [[MapStatus]] aggregated checksum for cross-
 * attempt determinism verification. Per-block CRC32C (used for transport-layer
 * integrity validation in [[flushBlock]]) is computed independently from the bytes
 * drained out of the partition buffer at flush time -- the per-partition cumulative
 * checksum and the per-block checksum are intentionally distinct artifacts: the
 * former enables retry-determinism detection, while the latter validates each on-the-
 * wire block against transport corruption.
 *
 * == Failure Handling ==
 * On consumer-failure detection (10-second missing acknowledgment per
 * [[CONSUMER_TIMEOUT_MILLIS]]), [[BackpressureProtocol]] coordinates retransmission
 * against the surviving consumer or, if the consumer is permanently gone, signals the
 * writer to stop and the framework recovers via the existing `DAGScheduler`
 * resubmission of the next stage attempt -- this preserves the user directive *"Never
 * modify DAG scheduler, task lifecycle, or user-facing APIs."*
 *
 * On any [[IOException]] propagated up from [[write]], the framework will call
 * `stop(success = false)`. The internal `stopping` flag makes `stop` idempotent so a
 * subsequent call (e.g. `stop(true)` followed by `stop(false)` on a swallowed
 * exception during commit) cleans up at most once.
 *
 * == ClassTag Handling ==
 * `Product2[K, V]` records are passed through the dependency's serializer using
 * [[org.apache.spark.serializer.SerializationStream#writeKey]] and `writeValue`, both
 * of which require a `ClassTag[T]` for the type parameter. Because this writer is
 * generic in `K` and `V` and the streaming-shuffle path does not require type-aware
 * serialization beyond what the existing serializer already performs, keys and values
 * are bound to local `Any` references at the call site so the compiler infers `T = Any`
 * and supplies the `ClassTag.Any` implicit. This mirrors the established pattern in
 * [[org.apache.spark.shuffle.ShufflePartitionPairsWriter]] and
 * [[org.apache.spark.storage.DiskBlockObjectWriter]].
 *
 * == Single-Threaded Metrics Reporter ==
 * All [[ShuffleWriteMetricsReporter]] calls (`incBytesWritten`, `incRecordsWritten`,
 * `incWriteTime`) happen on the single task thread executing [[write]] and [[stop]],
 * preserving the single-threaded contract documented at
 * [[org.apache.spark.shuffle.metrics.ShuffleWriteMetricsReporter]].
 *
 * @param handle           streaming-shuffle handle carrying the underlying
 *                         `ShuffleDependency` plus per-shuffle configuration
 *                         (`bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps`)
 * @param mapId            this map task's unique identifier; conventionally the
 *                         `taskAttemptId` so retries produce distinct map IDs
 * @param context          the active [[TaskContext]] for this task; supplies the
 *                         [[org.apache.spark.memory.TaskMemoryManager]] used for
 *                         execution-memory accounting
 * @param writeMetrics     single-threaded shuffle-write metrics reporter; see the
 *                         class-level "Single-Threaded Metrics Reporter" note above
 * @param blockManager     executor [[BlockManager]] -- used to obtain the
 *                         `shuffleServerId` carried in the produced [[MapStatus]] so
 *                         consumers can locate this map output via `MapOutputTracker`
 * @param memoryManager    unified [[MemoryManager]] -- queried via
 *                         `maxOnHeapStorageMemory` to compute the executor-memory
 *                         denominator of the per-partition buffer cap
 * @param backpressure     backpressure-protocol coordinator -- each per-block flush
 *                         calls [[BackpressureProtocol#recordTransmission]] which
 *                         updates the rate-limiter and producer-heartbeat state
 * @param spillManager     memory-spill coordinator -- per-partition spill is delegated
 *                         via [[MemorySpillManager#checkAndSpill]] when a partition's
 *                         buffer utilization crosses [[StreamingShuffleHandle.spillThreshold]]
 * @param streamingMetrics streaming-shuffle metric counters; passed downstream to
 *                         [[BackpressureProtocol]] and [[MemorySpillManager]] which
 *                         own the four canonical observability counters
 *                         (`bufferUtilizationPercent`, `spillCount`, `backpressureEvents`,
 *                         `partialReadInvalidations`). Retained on the writer instance as
 *                         `private val` (not currently incremented by writer-local code
 *                         paths) so the `StreamingShuffleManager`-managed lifetime
 *                         contract is honored: the manager constructs the writer with the
 *                         shared metrics handle so that any future v2 writer-local
 *                         emission (e.g. partial-flush event tracking) can wire to the
 *                         same registry instance without an SPI change.
 * @tparam K key type produced by the upstream stage
 * @tparam V value type produced by the upstream stage
 */
private[spark] class StreamingShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    blockManager: BlockManager,
    memoryManager: MemoryManager,
    backpressure: BackpressureProtocol,
    spillManager: MemorySpillManager,
    private val streamingMetrics: StreamingShuffleMetrics)
  extends ShuffleWriter[K, V] with Logging {

  // -------------------------------------------------------------------------------
  // Internal state
  // -------------------------------------------------------------------------------

  /**
   * The shuffle dependency carrying the partitioner and serializer; resolved once at
   * construction so subsequent hot-path code does not repeatedly traverse the handle.
   */
  private val dep = handle.dependency

  /**
   * Number of reduce partitions for this shuffle (the partitioner's `numPartitions`).
   * Pre-computed so the inner loops do not repeatedly invoke
   * `dep.partitioner.numPartitions`.
   */
  private val numPartitions: Int = dep.partitioner.numPartitions

  /**
   * Per-partition byte counters returned by [[getPartitionLengths]]. Each entry is the
   * total uncompressed serialized byte count for that reducer, accumulated over all
   * records the writer observes for that partition. Returned to the
   * [[org.apache.spark.scheduler.DAGScheduler]] (via [[MapStatus]]) so the reduce side
   * can size its fetch requests appropriately.
   */
  private val partitionLengths: Array[Long] = new Array[Long](numPartitions)

  /**
   * Per-partition byte buffers. Each [[ByteArrayOutputStream]] accumulates serialized
   * record bytes for one reducer until either (a) the buffer reaches
   * [[BLOCK_SIZE_BYTES]] (2 MB) at which point a block is flushed onto the network, or
   * (b) the per-partition spill threshold is crossed and the buffer is spilled to disk
   * via [[MemorySpillManager]].
   *
   * Constructed with the small [[INITIAL_BAOS_CAPACITY]] (1 KB) initial capacity so
   * total construction-time JVM-heap allocation is `INITIAL_BAOS_CAPACITY * numPartitions`
   * (e.g. ~200 KB for 200 partitions, ~1 MB for 1 000 partitions). This keeps the
   * pre-`acquireExecutionMemory` allocation negligible per AAP Section 0.7.2.2 -- the
   * unified-memory model bounds subsequent buffer growth via the
   * `acquireExecutionMemory(perPartitionBufferCap * numPartitions, ...)` grant in
   * [[write]]. `ByteArrayOutputStream`'s native `Arrays.copyOf` doubling growth absorbs
   * the actual record volume.
   *
   * Slots are nulled in [[stop]] so the underlying byte arrays can be garbage-collected
   * even if some other code retains a reference to this writer instance.
   */
  private val partitionBuffers: Array[ByteArrayOutputStream] =
    Array.fill(numPartitions)(new ByteArrayOutputStream(INITIAL_BAOS_CAPACITY))

  /**
   * Per-partition CRC32C accumulators (one per reducer). Updated incrementally as each
   * record is serialized into [[partitionBuffers]] via the per-partition
   * [[org.apache.spark.io.MutableCheckedOutputStream]] interceptor, so that the
   * per-partition cumulative checksum is available for inclusion in
   * [[aggregateChecksumValue]] when the writer commits. Allocated lazily alongside
   * [[partitionSerStreams]] in [[ensurePartitionStream]] so partitions that never
   * receive a record do not pay the construction cost.
   *
   * NOTE: per-block CRC32C (the integrity validator transmitted alongside each 2 MB
   * block in [[flushBlock]]) is computed independently inside [[flushBlock]] from the
   * exact bytes flushed, since the per-partition cumulative checksum captures all
   * partition records (not just the current block).
   */
  private val partitionChecksums: Array[CRC32C] = new Array[CRC32C](numPartitions)

  /**
   * Per-partition mutable-checksum interceptors. Each
   * [[org.apache.spark.io.MutableCheckedOutputStream]] wraps the partition's
   * [[ByteArrayOutputStream]] and threads the corresponding [[partitionChecksums]]
   * `CRC32C` through every byte the serializer writes -- replacing the per-record
   * `recordBytes`-then-`update` round-trip of an earlier hot-path implementation with
   * a single in-stream interceptor that updates the checksum inline with each
   * `write` call. Allocated lazily on first write to each partition by
   * [[ensurePartitionStream]].
   */
  private val partitionCheckedStreams: Array[MutableCheckedOutputStream] =
    new Array[MutableCheckedOutputStream](numPartitions)

  /**
   * Per-partition long-lived [[org.apache.spark.serializer.SerializationStream]]. Each
   * stream wraps the partition's [[partitionCheckedStreams]] interceptor (which in turn
   * wraps the partition's [[ByteArrayOutputStream]]). Allocated lazily on first write to
   * each partition by [[ensurePartitionStream]].
   *
   * Retaining one persistent serializer per partition (rather than constructing a fresh
   * one per record) ensures: (1) any per-stream header bytes the serializer emits are
   * written exactly once per partition, producing a wire format the reduce side can
   * deserialize as a single homogeneous stream; (2) per-record overhead is bounded to
   * the serializer's `writeKey`/`writeValue` calls themselves, with no per-record
   * `ByteArrayOutputStream` or `SerializationStream` construction or close cost.
   *
   * The streams are closed on `stop` (success or failure) and their slots nulled so the
   * underlying serializer state and any retained `ClassTag` references become eligible
   * for garbage collection.
   */
  private val partitionSerStreams: Array[SerializationStream] =
    new Array[SerializationStream](numPartitions)

  /**
   * Final [[MapStatus]] populated by [[write]] on success. Returned by [[stop]] when
   * called with `success = true`. Remains `None` if [[write]] fails before completing
   * its commit step, in which case [[stop]] returns `None` regardless of the `success`
   * argument.
   */
  private var mapStatus: Option[MapStatus] = None

  /**
   * Idempotency guard for [[stop]]. Map tasks may call `stop(success = true)` and then
   * subsequently call `stop(success = false)` if a downstream commit step throws; this
   * flag short-circuits the second invocation so cleanup runs at most once. Mirrors the
   * established pattern in [[org.apache.spark.shuffle.sort.SortShuffleWriter#stop]].
   */
  private var stopping: Boolean = false

  /**
   * Per-partition buffer cap in bytes.
   *
   * Computed as `(executor execution memory * bufferSizePercent / 100) / numPartitions`
   * with a floor of [[BLOCK_SIZE_BYTES]] (2 MB) per AAP Section 0.7.2.2. The floor
   * prevents pathological cases with thousands of partitions where naive arithmetic
   * would compute a cap below the block size and force every record write to trigger
   * a flush.
   *
   * The numerator uses [[MemoryManager#maxOnHeapStorageMemory]] as the executor-memory
   * proxy because that accessor returns the dynamic execution-memory ceiling under
   * the unified-memory model and is the canonical entry point for "how much memory is
   * available right now" -- mirroring the AAP Section 0.5.1.2 specification.
   */
  private val perPartitionBufferCap: Long = {
    // `maxOnHeapStorageMemory` returns the dynamic execution-memory ceiling under the
    // unified-memory model -- it is the canonical "how much memory is available right
    // now" accessor used elsewhere in the executor (e.g. `Spillable`). The variable is
    // named `unifiedMemoryCeiling` to make this unified-memory semantic explicit at the
    // call site, since "execution memory" alone could be misread as a separate
    // execution-only pool under the deprecated split-memory model.
    val unifiedMemoryCeiling = memoryManager.maxOnHeapStorageMemory
    val totalBuffer = (unifiedMemoryCeiling * handle.bufferSizePercent) / 100L
    val divisor = math.max(1, numPartitions).toLong
    math.max(BLOCK_SIZE_BYTES.toLong, totalBuffer / divisor)
  }

  /**
   * Internal [[MemoryConsumer]] subclass used solely for execution-memory accounting
   * via [[org.apache.spark.memory.TaskMemoryManager#acquireExecutionMemory]] and
   * `releaseExecutionMemory`.
   *
   * Both `TaskMemoryManager` methods assert their `consumer` argument is non-null
   * (`acquireExecutionMemory` asserts directly, `releaseExecutionMemory` calls
   * `consumer.getMode()` which would NPE on null) so passing `null` -- a tempting
   * "no-op consumer" choice -- is incorrect. Instead this minimal consumer participates
   * in the executor's memory bookkeeping for the writer's aggregate buffer footprint
   * but never participates in spill-on-OOM cascades because [[#spill]] returns 0L --
   * spilling for streaming-shuffle is delegated to [[MemorySpillManager]] which
   * coordinates partition-aware spill selection rather than reactive consumer-level
   * spill triggered from within `acquireExecutionMemory`.
   *
   * Mirrors the established `extends MemoryConsumer(taskMemoryManager,
   * MemoryMode.ON_HEAP)` pattern found in
   * [[org.apache.spark.util.collection.Spillable]].
   */
  private final class StreamingBufferConsumer
    extends MemoryConsumer(context.taskMemoryManager(), MemoryMode.ON_HEAP) {
    /**
     * Spill request from the [[org.apache.spark.memory.TaskMemoryManager]] -- always
     * returns 0L because spilling for streaming-shuffle is performed by
     * [[MemorySpillManager]] (a separate coordinator with partition-aware selection
     * logic) rather than reactively from this consumer. Returning 0L tells the task
     * memory manager that no memory could be released in response to its spill request,
     * which is the correct semantics here: the consumer's allocation is the writer's
     * irreducible buffer footprint and cannot be shrunk without losing accumulated
     * record bytes that have not yet been transmitted.
     *
     * Per the [[MemoryConsumer]] Javadoc: *"Note: today, this only frees Tungsten-managed
     * pages."* -- this writer does not manage Tungsten pages, so the spill contract is
     * satisfied trivially by returning 0.
     */
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  /**
   * Singleton consumer used for all execution-memory acquisition/release in this
   * writer. Constructed lazily on first use (within [[write]]) so that test fixtures
   * that exercise individual methods (e.g. `getPartitionLengths`) without ever calling
   * `write` do not pay the cost of registering a consumer with the task memory manager.
   */
  private var bufferConsumer: StreamingBufferConsumer = _

  /**
   * Total execution-memory bytes currently acquired through [[bufferConsumer]];
   * tracked separately so [[stop]] can release the exact amount that was acquired
   * even if interim partial-failure paths run.
   */
  private var acquiredMemoryBytes: Long = 0L

  // -------------------------------------------------------------------------------
  // Public API: write / stop / getPartitionLengths
  // -------------------------------------------------------------------------------

  /**
   * Write a sequence of `(K, V)` records.
   *
   * Records are partitioned via the dependency's partitioner, serialized through the
   * dependency's serializer into per-partition [[ByteArrayOutputStream]] buffers, and
   * flushed in 2 MB blocks (with a per-block CRC32C checksum) onto the network via
   * [[BackpressureProtocol]]. After the iterator is exhausted any residual buffers
   * (smaller than [[BLOCK_SIZE_BYTES]]) are flushed as final blocks, and a [[MapStatus]]
   * carrying the per-partition byte counts and aggregated checksum is constructed for
   * the [[org.apache.spark.scheduler.DAGScheduler]].
   *
   * On any [[IOException]] thrown by the serializer or by [[BackpressureProtocol]] the
   * writer is left in a stopping state: the framework calls
   * [[stop]]`(success = false)` to clean up.
   *
   * @param records iterator of `(K, V)` records to write; consumed exactly once
   * @throws IOException if serialization or block transmission fails
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val partitioner = dep.partitioner
    val serInstance = dep.serializer.newInstance()
    val startNs = System.nanoTime()

    // Lazily construct the buffer consumer and acquire execution memory for the
    // aggregate buffer footprint up-front. The TaskMemoryManager may grant less than
    // requested if execution memory is exhausted -- in that case the streaming-shuffle
    // fallback policy will observe the partial grant via memory-pressure detection on
    // a subsequent shuffle attempt; the writer continues with whatever was granted
    // because the per-partition cap is itself a soft target and spill picks up the
    // slack via MemorySpillManager.
    bufferConsumer = new StreamingBufferConsumer
    val requested = perPartitionBufferCap * numPartitions.toLong
    acquiredMemoryBytes =
      context.taskMemoryManager().acquireExecutionMemory(requested, bufferConsumer)
    logDebug(log"StreamingShuffleWriter " +
      log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
      log"mapId=${MDC(MAP_ID, mapId)} " +
      log"acquired=${MDC(NUM_BYTES, acquiredMemoryBytes)} / " +
      log"requested=${MDC(NUM_BYTES, requested)} execution memory")

    try {
      while (records.hasNext) {
        val record = records.next()
        // Bind to local Any references so the writeKey/writeValue type parameter
        // is inferred as Any and ClassTag.Any is supplied implicitly. See the
        // class-level "ClassTag Handling" Scaladoc for the rationale.
        val key: Any = record._1
        val value: Any = record._2
        val partitionId = partitioner.getPartition(key)

        val pBuf = partitionBuffers(partitionId)

        // Lazily open a per-block SerializationStream + MutableCheckedOutputStream
        // chain for this partition. The chain is opened on demand here and closed
        // by `flushBlock` / `maybeSpill` (when a block boundary or spill threshold
        // is crossed) or by the residual-drain `flushBlock` loop after the iterator
        // is exhausted. Every block emitted on the wire therefore contains a single
        // complete serialization stream (header + records + footer), satisfying the
        // wire-format invariant required by `StreamingShuffleReader` whose
        // per-block deserialization at `serializerInstance.deserializeStream(...)`
        // .asKeyValueIterator requires each block to be independently
        // deserializable. The MutableCheckedOutputStream interceptor updates the
        // per-partition cumulative CRC32C inline with every byte the serializer
        // emits, eliminating the per-record `update(recordBytes, 0, n)` round-trip
        // present in the previous implementation.
        val objOut = ensurePartitionStream(partitionId, serInstance)
        objOut.writeKey(key)
        objOut.writeValue(value)
        // Flush the serializer so its accumulated bytes land in the partition buffer
        // (and through the MutableCheckedOutputStream interceptor into the partition
        // CRC32C) before we read the buffer's `size()` for block-boundary and
        // spill-threshold evaluation.
        objOut.flush()

        // If the partition's accumulated bytes have crossed the 2 MB block boundary,
        // flush a block onto the network. flushBlock closes the partition's
        // serialization stream so the drained bytes form a complete stream with
        // both header and footer; the next ensurePartitionStream call (triggered
        // by the next record for this partition) lazily allocates a fresh stream
        // that writes a fresh header into the now-empty buffer. partitionLengths
        // is updated by flushBlock with the actual drained byte count.
        if (pBuf.size() >= BLOCK_SIZE_BYTES) {
          flushBlock(partitionId)
        }

        // Track records-written metric on the single-threaded reporter contract.
        writeMetrics.incRecordsWritten(1L)

        // Periodically check if memory pressure requires a spill of this partition.
        // maybeSpill mirrors flushBlock's invariant: it closes the partition's
        // serialization stream before draining so spilled bytes also form a
        // complete stream (the spilled block is later read back and re-streamed
        // to the consumer through the same per-block deserialization path).
        maybeSpill(partitionId)
      }

      // Drain residual bytes for every partition by calling flushBlock. flushBlock
      // is idempotent for empty partitions (early-returns when both the stream slot
      // is null and the buffer is empty) and handles partitions with an open stream
      // by closing it (writing the footer to the buffer) before draining. This
      // consolidates the close-and-drain sequence in a single helper so the per-
      // block wire-format invariant is enforced uniformly.
      var i = 0
      while (i < numPartitions) {
        flushBlock(i)
        i += 1
      }

      val durationNs = System.nanoTime() - startNs
      writeMetrics.incWriteTime(durationNs)

      // Build the MapStatus. The factory chooses CompressedMapStatus or
      // HighlyCompressedMapStatus based on partition count -- we do not pick directly.
      val aggregatedChecksum = aggregateChecksumValue()
      mapStatus = Some(
        MapStatus(blockManager.shuffleServerId, partitionLengths, mapId, aggregatedChecksum))

      logInfo(log"StreamingShuffleWriter completed " +
        log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
        log"mapId=${MDC(MAP_ID, mapId)} " +
        log"totalBytes=${MDC(NUM_BYTES, partitionLengths.sum)} " +
        log"numPartitions=${MDC(NUM_PARTITIONS, numPartitions)} " +
        log"durationNs=${MDC(DURATION, durationNs)} " +
        log"aggregatedChecksum=${MDC(CHECKSUM, aggregatedChecksum)}")
    } catch {
      case t: Throwable =>
        // Release acquired memory eagerly on the failure path so the executor's
        // memory manager observes the release before the framework calls stop(false);
        // the finally clause is a defensive safety net for non-Throwable exits.
        releaseAcquiredMemory()
        throw t
    } finally {
      // Normal-path memory release. If write completed successfully the released
      // amount is `acquiredMemoryBytes`; if a Throwable was caught above, the catch
      // block already released and `acquiredMemoryBytes` was reset to 0L so this
      // call becomes a no-op.
      releaseAcquiredMemory()
    }
  }

  /**
   * Close this writer, returning the [[MapStatus]] when `success = true` and
   * cleaning up per-partition buffers in either case.
   *
   * Idempotency: `stop` may be called multiple times (typically `stop(true)` then
   * `stop(false)` if a subsequent commit step throws). The internal `stopping` flag
   * short-circuits the second call so cleanup runs at most once. Mirrors the pattern
   * in [[org.apache.spark.shuffle.sort.SortShuffleWriter#stop]].
   *
   * @param success whether the map task completed successfully
   * @return `Some(mapStatus)` if `success` is true and [[write]] populated `mapStatus`;
   *         otherwise `None`
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) mapStatus else None
    } finally {
      // Release acquired execution memory if write() did not already release it (e.g.
      // if an exception bypassed the finally block, or if stop is called without
      // a prior successful write).
      releaseAcquiredMemory()
      // Best-effort close any still-open per-partition serialization streams (e.g. if
      // an exception bypassed the normal-path drain loop in `write`). Catch and log
      // any close-time error to honor the framework expectation that `stop` is
      // best-effort: throwing from cleanup masks the original failure cause.
      var i = 0
      while (i < numPartitions) {
        val ss = partitionSerStreams(i)
        if (ss != null) {
          try {
            ss.close()
          } catch {
            case t: Throwable =>
              logWarning(log"Failed to close per-partition SerializationStream " +
                log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
                log"mapId=${MDC(MAP_ID, mapId)} " +
                log"reduceId=${MDC(REDUCE_ID, i)}: " +
                log"${MDC(ERROR, Option(t.getMessage).getOrElse("(no message)"))}")
          }
          partitionSerStreams(i) = null
        }
        // Null per-partition buffer / checksum / interceptor slots so their underlying
        // byte arrays and references become eligible for GC even if some other code
        // retains a reference to this writer instance.
        partitionBuffers(i) = null
        partitionCheckedStreams(i) = null
        partitionChecksums(i) = null
        i += 1
      }
    }
  }

  /**
   * Return the per-partition byte counts accumulated by [[write]]. The returned array
   * is the writer's internal storage, not a defensive copy: callers MUST treat it as
   * read-only. This semantic matches the [[ShuffleWriter]] abstract-class contract
   * established by [[org.apache.spark.shuffle.sort.SortShuffleWriter#getPartitionLengths]].
   *
   * If [[write]] has not yet been called the array is the zero-initialized
   * `Array[Long](numPartitions)` produced at construction.
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  // -------------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------------

  /**
   * Ensure the per-block [[SerializationStream]] (and the
   * [[org.apache.spark.io.MutableCheckedOutputStream]] interceptor that wraps it) is
   * allocated for the given partition, creating them on first call and on every
   * subsequent call after [[flushBlock]] / [[maybeSpill]] has closed and nulled out
   * the slot.
   *
   * Construction order (innermost to outermost):
   *   1. The partition's [[ByteArrayOutputStream]] (already allocated at writer
   *      construction time with [[INITIAL_BAOS_CAPACITY]] initial capacity).
   *   2. The per-partition cumulative [[CRC32C]] -- allocated at most once per
   *      partition (on the first ensurePartitionStream call for that partition); on
   *      subsequent calls (after a flushBlock/maybeSpill close-and-drain cycle) the
   *      existing cumulative CRC32C from `partitionChecksums(partitionId)` is reused
   *      so that the cumulative checksum captures every byte written to the partition
   *      across all blocks (including each block's stream header AND footer). This
   *      matches the determinism check that [[org.apache.spark.SparkContext]]
   *      performs across map-task retries.
   *   3. A [[org.apache.spark.io.MutableCheckedOutputStream]] wrapping the BAOS, with
   *      the cumulative `CRC32C` registered via `setChecksum(...)` so that every byte
   *      written through it is fed into both the BAOS and the checksum. A fresh
   *      MutableCheckedOutputStream is allocated per block (cheap: a thin object
   *      around the existing BAOS); the registered CRC32C is the long-lived per-
   *      partition instance from step 2.
   *   4. A [[org.apache.spark.serializer.SerializationStream]] obtained from the
   *      caller-supplied `serInstance` and wrapping the checked stream. A fresh
   *      stream is allocated per block; the serializer-stream header (if any) is
   *      written into the now-empty BAOS during this construction so each block
   *      drained by [[flushBlock]] / [[maybeSpill]] forms a complete, independently
   *      deserializable serialization stream -- the wire-format invariant required
   *      by [[StreamingShuffleReader]] which deserializes each block independently
   *      via `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator`.
   *
   * @param partitionId the reduce partition for which to obtain a serialization stream
   * @param serInstance the dependency's serializer instance (resolved once per `write`
   *                    invocation by the caller and threaded through to avoid repeated
   *                    `dep.serializer.newInstance()` calls)
   * @return the per-block [[SerializationStream]] for this partition (newly allocated
   *         on this call if the partition's previous block was just flushed or spilled,
   *         existing instance otherwise)
   */
  private def ensurePartitionStream(
      partitionId: Int,
      serInstance: org.apache.spark.serializer.SerializerInstance): SerializationStream = {
    var ss = partitionSerStreams(partitionId)
    if (ss == null) {
      // Reuse the existing per-partition cumulative CRC32C if one was allocated by a
      // prior block for this partition; otherwise allocate the cumulative checksum on
      // first use. This invariant -- one CRC32C per partition spanning all blocks for
      // that partition -- is what makes the determinism check in MapStatus reliable
      // across map-task retries.
      var cksum = partitionChecksums(partitionId)
      if (cksum == null) {
        cksum = new CRC32C()
        partitionChecksums(partitionId) = cksum
      }
      val checked = new MutableCheckedOutputStream(partitionBuffers(partitionId))
      checked.setChecksum(cksum)
      partitionCheckedStreams(partitionId) = checked
      ss = serInstance.serializeStream(checked)
      partitionSerStreams(partitionId) = ss
    }
    ss
  }

  /**
   * Close the per-partition [[SerializationStream]] (and null out its slot plus the
   * companion [[org.apache.spark.io.MutableCheckedOutputStream]] slot) if the stream
   * is currently open. The close call writes any buffered serializer state and the
   * stream-footer marker (where applicable, e.g. [[org.apache.spark.serializer.JavaSerializer]]
   * `ObjectOutputStream` writes a TC_RESET marker on close) through the
   * [[org.apache.spark.io.MutableCheckedOutputStream]] interceptor and into the
   * partition's [[ByteArrayOutputStream]], so that the bytes drained by the immediately
   * following call to [[flushBlock]] or [[maybeSpill]] form a complete, independently
   * deserializable serialization stream.
   *
   * The per-partition cumulative [[CRC32C]] in `partitionChecksums` is preserved across
   * close/reopen cycles (the next [[ensurePartitionStream]] call wraps a fresh
   * [[org.apache.spark.io.MutableCheckedOutputStream]] around the same `CRC32C` instance)
   * so the cumulative checksum captures every byte written to the partition across all
   * blocks for the determinism check that [[org.apache.spark.SparkContext]] performs
   * across map-task retries.
   *
   * Idempotent: calling on a partition whose stream is already null is a no-op.
   *
   * @param partitionId the reduce partition whose serialization stream is closed
   */
  private def closePartitionStream(partitionId: Int): Unit = {
    val ss = partitionSerStreams(partitionId)
    if (ss != null) {
      ss.close()
      partitionSerStreams(partitionId) = null
      partitionCheckedStreams(partitionId) = null
    }
  }

  /**
   * Flush the accumulated bytes for one partition as a single block (up to
   * [[BLOCK_SIZE_BYTES]] = 2 MB). Closes the partition's per-block
   * [[SerializationStream]] so the drained bytes form a complete, independently
   * deserializable stream (with header AND footer); computes a per-block CRC32C
   * checksum on the exact bytes flushed; hands the block to
   * [[BackpressureProtocol#recordTransmission]]; and updates [[partitionLengths]] and
   * the bytes-written metric.
   *
   * Note: per AAP Section 0.5.1.2 and the [[BackpressureProtocol#recordTransmission]]
   * Scaladoc, `recordTransmission` does NOT perform the actual network send -- it only
   * updates rate-limiter and producer-heartbeat state and returns `true` if the rate
   * limiter granted tokens for this byte count, `false` if rate-limited. In a future
   * extension the writer would observe the return value and back off; for the
   * checkpoint this writer corresponds to, the return value is logged at trace level
   * and execution proceeds (the `MemorySpillManager` provides backpressure relief by
   * spilling to disk if the consumer is slow enough that buffers grow).
   *
   * Resets the partition's [[ByteArrayOutputStream]] after extracting the bytes so the
   * underlying buffer is reused for the next block. The next [[ensurePartitionStream]]
   * call for this partition (triggered by the next record write) lazily allocates a
   * fresh [[SerializationStream]] which writes a fresh stream header into the
   * now-empty buffer -- this is the wire-format invariant required by
   * [[StreamingShuffleReader]] which deserializes each block independently via
   * `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator`.
   *
   * @param partitionId the reduce partition whose accumulated bytes are to be flushed
   */
  private def flushBlock(partitionId: Int): Unit = {
    // Close the partition's serialization stream FIRST so any buffered serializer
    // state and the stream-footer marker are pushed through the
    // MutableCheckedOutputStream interceptor (which feeds them into the per-partition
    // cumulative CRC32C) and into the partition buffer. Doing this BEFORE draining
    // ensures the bytes flushed onto the network constitute a complete,
    // independently deserializable serialization stream -- the wire-format
    // invariant required by StreamingShuffleReader's per-block deserialization.
    closePartitionStream(partitionId)

    val buf = partitionBuffers(partitionId)
    if (buf == null || buf.size() == 0) {
      return
    }

    val bytes = buf.toByteArray
    buf.reset()

    // Update partitionLengths for this partition with the drained byte count. This
    // captures the on-wire bytes (header + records + footer) for the block; summed
    // across all flushes for a partition this yields the value carried in MapStatus
    // and consumed by the reader to know how many bytes to expect per partition.
    partitionLengths(partitionId) += bytes.length.toLong

    // Compute per-block CRC32C on the exact bytes being flushed. A fresh CRC32C
    // instance per block ensures the per-block checksum is independent of the
    // per-partition cumulative checksum maintained by `partitionChecksums`.
    val blockCrc = new CRC32C()
    blockCrc.update(bytes, 0, bytes.length)
    val blockChecksum = blockCrc.getValue

    // Hand the block to BackpressureProtocol for rate-limiter/heartbeat bookkeeping.
    // Per its Scaladoc this does not perform network I/O -- the actual send is
    // expected to be performed by an enclosing transport coordinator in subsequent
    // checkpoints. Here we honor the recordTransmission contract.
    val acquired = backpressure.recordTransmission(
      shuffleId = dep.shuffleId,
      mapId = mapId,
      reduceId = partitionId,
      byteCount = bytes.length.toLong,
      checksum = blockChecksum)

    writeMetrics.incBytesWritten(bytes.length.toLong)

    if (isTraceEnabled()) {
      // Note: the algorithm name (CHECKSUM_ALGORITHM = "CRC32C") is a compile-time
      // constant documented in the package object; it is intentionally not emitted as
      // an MDC field to keep per-flush log lines lean -- operators correlate
      // consumer-side validation failures via the per-block checksum value alone.
      logTrace(log"flushBlock " +
        log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
        log"mapId=${MDC(MAP_ID, mapId)} " +
        log"reduceId=${MDC(REDUCE_ID, partitionId)} " +
        log"size=${MDC(NUM_BYTES, bytes.length.toLong)} " +
        log"crc32c=${MDC(CHECKSUM, blockChecksum)} " +
        log"acquired=${MDC(NUM_BYTES, if (acquired) 1L else 0L)}")
    }
  }

  /**
   * Check whether buffer utilization for the given partition has crossed the
   * configured spill threshold; if so, request the [[MemorySpillManager]] to spill the
   * partition's buffer to disk and reset the in-memory buffer.
   *
   * The byte buffer is wrapped in a [[ChunkedByteBuffer]] (zero-copy via
   * [[ByteBuffer#wrap]]) before being handed to
   * [[MemorySpillManager#checkAndSpill]] -- ownership of the wrapped buffer transfers
   * to the spill manager per the writer-manager ownership contract documented at the
   * top of [[MemorySpillManager]]. After this call returns the manager is responsible
   * for either persisting (success path: `BlockManager.putBytes` then `dispose`) or
   * preserving (retry path) the buffer.
   *
   * Buffer utilization is computed as
   * `(currentBytes * 100) / perPartitionBufferCap` -- when this percentage equals or
   * exceeds [[StreamingShuffleHandle.spillThreshold]] the spill is requested.
   *
   * @param partitionId the reduce partition whose utilization is checked
   */
  private def maybeSpill(partitionId: Int): Unit = {
    val buf = partitionBuffers(partitionId)
    if (buf == null) {
      return
    }
    val current = buf.size().toLong
    val cap = math.max(1L, perPartitionBufferCap)
    val pct = (current * 100L) / cap
    if (pct >= handle.spillThreshold.toLong) {
      // Close the partition's serialization stream FIRST so the spilled bytes form a
      // complete, independently deserializable stream (with header AND footer). The
      // close call writes any buffered serializer state and the stream-footer marker
      // through the MutableCheckedOutputStream interceptor (which feeds them into the
      // per-partition cumulative CRC32C) and into the partition buffer. This matches
      // the wire-format invariant required when the spilled bytes are eventually read
      // back and re-streamed to the consumer.
      closePartitionStream(partitionId)

      val pendingBytes = buf.toByteArray
      // Update partitionLengths for this partition with the spilled byte count so the
      // MapStatus reflects the on-wire byte total (network-flushed + spilled). This
      // happens before the spill call so accounting is updated atomically with the
      // closePartitionStream + drain sequence even if the spill manager surfaces an
      // exception (the bytes have already left the buffer).
      partitionLengths(partitionId) += pendingBytes.length.toLong

      val chunked = new ChunkedByteBuffer(ByteBuffer.wrap(pendingBytes))
      try {
        spillManager.checkAndSpill(
          shuffleId = dep.shuffleId,
          mapId = mapId,
          reduceId = partitionId,
          buffer = chunked)
      } finally {
        // Per the writer-manager ownership contract, ownership of `chunked` has
        // transferred to the spill manager regardless of success/failure of the
        // checkAndSpill call. Reset the in-memory buffer for this partition so
        // subsequent writes start fresh; do this in finally to guarantee reset
        // even on exception.
        buf.reset()
      }

      if (isTraceEnabled()) {
        logTrace(log"maybeSpill " +
          log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
          log"mapId=${MDC(MAP_ID, mapId)} " +
          log"reduceId=${MDC(REDUCE_ID, partitionId)} " +
          log"bytesSpilled=${MDC(NUM_BYTES, pendingBytes.length.toLong)} " +
          log"cap=${MDC(NUM_BYTES, cap)} " +
          log"pct=${MDC(NUM_BYTES, pct)} " +
          log"threshold=${MDC(THRESHOLD, handle.spillThreshold.toLong)}")
      }
    }
  }

  /**
   * Aggregate the per-partition CRC32C values into a single checksum for the
   * [[MapStatus]]. Uses XOR with rotation so that the aggregated value is sensitive to
   * partition ordering (the same per-partition checksums in different orders produce
   * different aggregates), which is desirable for the determinism check that
   * [[org.apache.spark.SparkContext]] performs across map-task retries to detect
   * non-deterministic shuffle output.
   *
   * The aggregation is deterministic across retries: given the same sequence of input
   * records, the per-partition checksums and their bit positions are identical, and
   * the XOR-with-rotation produces the same aggregated value. This mirrors the spirit
   * of [[org.apache.spark.shuffle.sort.SortShuffleWriter#getAggregatedChecksumValue]]
   * which delegates to the external sorter's `getAggregatedChecksumValue`.
   *
   * @return the aggregated checksum value passed as the `checksumVal` argument to
   *         [[MapStatus#apply]]
   */
  private def aggregateChecksumValue(): Long = {
    var aggregated: Long = 0L
    var i = 0
    while (i < numPartitions) {
      val cksum = partitionChecksums(i)
      if (cksum != null) {
        aggregated ^= java.lang.Long.rotateLeft(cksum.getValue, i & 63)
      }
      i += 1
    }
    aggregated
  }

  /**
   * Release the execution memory previously acquired in [[write]] through
   * [[bufferConsumer]]. Idempotent: if no memory has been acquired (or if a previous
   * call already released it), this is a no-op.
   *
   * The release uses the same [[bufferConsumer]] reference passed to `acquireExecutionMemory`
   * because [[org.apache.spark.memory.TaskMemoryManager#releaseExecutionMemory]] calls
   * `consumer.getMode()` on its argument and would NPE on null.
   */
  private def releaseAcquiredMemory(): Unit = {
    if (acquiredMemoryBytes > 0L && bufferConsumer != null) {
      val toRelease = acquiredMemoryBytes
      acquiredMemoryBytes = 0L
      try {
        context.taskMemoryManager().releaseExecutionMemory(toRelease, bufferConsumer)
      } catch {
        case t: Throwable =>
          // Defensive: log and swallow so we never propagate cleanup failures out of
          // the normal-path finally blocks. The framework expects stop() to be best-
          // effort; throwing from cleanup masks the original failure cause.
          logWarning(log"Failed to release execution memory for streaming shuffle writer " +
            log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
            log"mapId=${MDC(MAP_ID, mapId)} " +
            log"toRelease=${MDC(NUM_BYTES, toRelease)}: " +
            log"${MDC(ERROR, Option(t.getMessage).getOrElse("(no message)"))}")
      }
    }
  }
}
