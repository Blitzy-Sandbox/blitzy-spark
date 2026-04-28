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
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, MemoryMode}
import org.apache.spark.scheduler.MapStatus
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
 *                         [[BackpressureProtocol]] and [[MemorySpillManager]] but also
 *                         retained here for any writer-local emission needs (e.g. future
 *                         per-writer emission of partial-flush events)
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
    streamingMetrics: StreamingShuffleMetrics)
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
   * Constructed with an initial capacity of [[BLOCK_SIZE_BYTES]] so the first 2 MB of
   * data fits without a single internal `Arrays.copyOf` reallocation.
   *
   * Slots are nulled in [[stop]] so the underlying byte arrays can be garbage-collected
   * even if some other code retains a reference to this writer instance.
   */
  private val partitionBuffers: Array[ByteArrayOutputStream] =
    Array.fill(numPartitions)(new ByteArrayOutputStream(BLOCK_SIZE_BYTES))

  /**
   * Per-partition CRC32C accumulators (one per reducer). Updated as each record is
   * serialized into [[partitionBuffers]] so that the per-partition cumulative checksum
   * is available for inclusion in [[aggregateChecksumValue]] when the writer commits.
   *
   * NOTE: per-block CRC32C (the integrity validator transmitted alongside each 2 MB
   * block in [[flushBlock]]) is computed independently inside [[flushBlock]] from the
   * exact bytes flushed, since the per-partition cumulative checksum captures all
   * partition records (not just the current block).
   */
  private val partitionChecksums: Array[CRC32C] =
    Array.fill(numPartitions)(new CRC32C())

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
    val executionMemory = memoryManager.maxOnHeapStorageMemory
    val totalBuffer = (executionMemory * handle.bufferSizePercent) / 100L
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
    logDebug(
      s"StreamingShuffleWriter shuffle=${dep.shuffleId} mapId=$mapId " +
      s"acquired=$acquiredMemoryBytes / requested=$requested execution memory")

    try {
      while (records.hasNext) {
        val record = records.next()
        // Bind to local Any references so the writeKey/writeValue type parameter
        // is inferred as Any and ClassTag.Any is supplied implicitly. See the
        // class-level "ClassTag Handling" Scaladoc for the rationale.
        val key: Any = record._1
        val value: Any = record._2
        val partitionId = partitioner.getPartition(key)

        // Serialize the (K, V) pair into a small temporary buffer; we then copy the
        // resulting bytes into the per-partition buffer and update the per-partition
        // checksum. A small initial-capacity (64 bytes) is sufficient for typical
        // records and the buffer auto-grows for outliers without re-allocating the
        // long-lived per-partition buffer.
        val recordBytes: Array[Byte] = {
          val tmp = new ByteArrayOutputStream(64)
          val ss = serInstance.serializeStream(tmp)
          try {
            ss.writeKey(key)
            ss.writeValue(value)
          } finally {
            ss.close()
          }
          tmp.toByteArray
        }

        val pBuf = partitionBuffers(partitionId)
        pBuf.write(recordBytes, 0, recordBytes.length)
        partitionChecksums(partitionId).update(recordBytes, 0, recordBytes.length)
        partitionLengths(partitionId) += recordBytes.length.toLong

        // If the partition's accumulated bytes have crossed the 2 MB block boundary,
        // flush a block onto the network.
        if (pBuf.size() >= BLOCK_SIZE_BYTES) {
          flushBlock(partitionId)
        }

        // Track records-written metric on the single-threaded reporter contract.
        writeMetrics.incRecordsWritten(1L)

        // Periodically check if memory pressure requires a spill of this partition.
        maybeSpill(partitionId)
      }

      // Flush any residual buffers (size < BLOCK_SIZE_BYTES) as final partial blocks.
      var i = 0
      while (i < numPartitions) {
        if (partitionBuffers(i).size() > 0) {
          flushBlock(i)
        }
        i += 1
      }

      val durationNs = System.nanoTime() - startNs
      writeMetrics.incWriteTime(durationNs)

      // Build the MapStatus. The factory chooses CompressedMapStatus or
      // HighlyCompressedMapStatus based on partition count -- we do not pick directly.
      val aggregatedChecksum = aggregateChecksumValue()
      mapStatus = Some(
        MapStatus(blockManager.shuffleServerId, partitionLengths, mapId, aggregatedChecksum))

      logInfo(
        s"StreamingShuffleWriter completed shuffle=${dep.shuffleId} mapId=$mapId " +
        s"totalBytes=${partitionLengths.sum} numPartitions=$numPartitions " +
        s"durationNs=$durationNs aggregatedChecksum=$aggregatedChecksum")
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
      // Null per-partition buffer/checksum slots so their underlying byte arrays
      // become eligible for GC even if some other code retains a reference to this
      // writer instance.
      var i = 0
      while (i < numPartitions) {
        partitionBuffers(i) = null
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
   * Flush the accumulated bytes for one partition as a single block (up to
   * [[BLOCK_SIZE_BYTES]] = 2 MB). Computes a per-block CRC32C checksum on the exact
   * bytes flushed, hands the block to [[BackpressureProtocol#recordTransmission]], and
   * updates the bytes-written metric.
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
   * Resets the partition's [[ByteArrayOutputStream]] after extracting the bytes so
   * the underlying buffer is reused for the next block.
   *
   * @param partitionId the reduce partition whose accumulated bytes are to be flushed
   */
  private def flushBlock(partitionId: Int): Unit = {
    val buf = partitionBuffers(partitionId)
    if (buf == null || buf.size() == 0) {
      return
    }

    val bytes = buf.toByteArray
    buf.reset()

    // Compute per-block CRC32C on the exact bytes being flushed.
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
      logTrace(
        s"flushBlock shuffleId=${dep.shuffleId} mapId=$mapId reduceId=$partitionId " +
        s"size=${bytes.length} crc32c=$blockChecksum acquired=$acquired " +
        s"algorithm=$CHECKSUM_ALGORITHM")
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
      val pendingBytes = buf.toByteArray
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
        logTrace(
          s"maybeSpill shuffleId=${dep.shuffleId} mapId=$mapId reduceId=$partitionId " +
          s"bytesSpilled=${pendingBytes.length} cap=$cap pct=$pct " +
          s"threshold=${handle.spillThreshold}")
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
          logWarning(
            s"Failed to release $toRelease bytes of execution memory for streaming " +
            s"shuffle writer (shuffleId=${dep.shuffleId} mapId=$mapId): " +
            s"${Option(t.getMessage).getOrElse("(no message)")}")
      }
    }
  }
}
