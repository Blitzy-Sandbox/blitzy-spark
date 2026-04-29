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
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
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
 * == Dual-Channel Wire/Persist Format Invariant ==
 * The writer maintains two parallel serialization channels per partition: a *wire-
 * format channel* (block-by-block) and a *persist channel* (single continuous stream).
 * Each record written by `write()` is serialized through BOTH channels.
 *
 *   1. The wire-format channel is materialized as
 *      [[org.apache.spark.serializer.SerializationStream]] instances stored in
 *      `partitionSerStreams`, wrapping the partition's
 *      [[org.apache.spark.io.MutableCheckedOutputStream]]
 *      which in turn wraps the partition's [[ByteArrayOutputStream]]
 *      (`partitionBuffers`). The wire-format channel close-and-reopens at every block
 *      boundary (driven by [[flushBlock]] / [[maybeSpill]]) so that each drained block
 *      constitutes a complete, independently deserializable serialization stream
 *      (header + records + footer). This invariant exists so that a future v2
 *      streaming-transport-layer extension can deserialize each in-flight block
 *      independently via `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator`
 *      WITHOUT receiving the full partition.
 *
 *   2. The persist channel is materialized as
 *      [[org.apache.spark.serializer.SerializationStream]] instances stored in
 *      `partitionPersistSerStreams`, wrapping the partition's persist accumulator
 *      [[ByteArrayOutputStream]] (`partitionPersistBuffers`). The persist channel stays
 *      OPEN for the entire `write()` lifetime and is closed exactly once per partition
 *      by [[closeAllPartitionPersistStreams]] just before [[persistPartitionsForReader]]
 *      runs. The result is a single continuous (header + all records + footer)
 *      serialization stream per partition -- the format the [[StreamingShuffleReader]]
 *      requires when it calls `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator`
 *      ONCE over the whole fetched partition (since the v1 reader fetches the entire
 *      partition as a single block via `BlockManager.fetchBlockSync`).
 *
 * Per-record overhead is therefore bounded by the cost of TWO `SerializationStream`
 * `writeKey`/`writeValue` calls. The wire-format channel constructs one
 * `SerializationStream` per block (NOT per record); the persist channel constructs
 * exactly one `SerializationStream` per partition. For typical workloads (e.g. a
 * 100 MB shuffle / 10 partitions = 10 MB per partition / 2 MB blocks = 5 blocks per
 * partition) this amounts to (5 wire-format + 1 persist) = 6 stream constructions per
 * partition rather than one per record.
 *
 * Records are serialized twice (once into each channel) but the same `SerializerInstance`
 * is reused for both, so the cost is bounded by JVM memory bandwidth rather than
 * additional serializer-state construction. This trade-off is acceptable for v1 because
 * (a) streaming-shuffle is opt-in, (b) the persist channel is the primary data plane
 * (the v1 wire-format channel does NOT perform actual network I/O --
 * [[BackpressureProtocol#recordTransmission]] is bookkeeping only), and (c) eliminating
 * the persist channel would require the reader to deserialize per-block streams in
 * sequence, complicating its `asKeyValueIterator` contract.
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
 * @param debugEnabled     cached value of `spark.shuffle.streaming.debug`, gating
 *                         streaming-shuffle DEBUG/TRACE log emission at the source-site
 *                         per AAP Section 0.1.2 user directive *"Debug logging disabled
 *                         by default (enable via `spark.shuffle.streaming.debug=true`)"*.
 *                         WARN/ERROR statements pass freely regardless of this flag.
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
    private val streamingMetrics: StreamingShuffleMetrics,
    debugEnabled: Boolean)
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
   * Per-partition cumulative byte accumulators that capture EVERY serialized byte
   * drained from [[partitionBuffers]] across the entire lifetime of [[write]] -- the
   * cumulative source consumed by [[persistPartitionsForReader]] to publish each
   * partition's complete output to the executor's [[BlockManager]] under the
   * streaming-shuffle blockId pattern `ShuffleBlockId(shuffleId, mapId, reduceId)` so
   * that downstream [[StreamingShuffleReader]] fetches via
   * `BlockManager.getLocalBlockData` resolve through this manager's
   * [[StreamingShuffleManager.shuffleBlockResolver]].
   *
   * Both [[flushBlock]] (block-boundary network flush path) and [[maybeSpill]]
   * (spill-threshold disk-spill path) append the bytes they drain from
   * `partitionBuffers(i)` into `partitionPersistBuffers(i)` so the accumulator
   * captures the full per-partition wire stream regardless of which code path drained
   * the in-flight buffer. The accumulator is populated as a SIDE EFFECT of those
   * existing flush/spill operations and does not perturb their semantics: the network
   * transmission via [[BackpressureProtocol]] (in `flushBlock`) and the in-progress
   * spill via [[MemorySpillManager]] (in `maybeSpill`) continue to operate exactly as
   * before. The accumulator is consumed exactly once at the end of [[write]] by
   * [[persistPartitionsForReader]] which converts each partition's accumulated bytes
   * into a [[ChunkedByteBuffer]] and stores it via [[BlockManager#putBytes]] under the
   * disk-only storage level. Because [[MemorySpillManager#checkAndSpill]] also stores
   * spilled bytes under the same `ShuffleBlockId(shuffleId, mapId, reduceId)`,
   * [[persistPartitionsForReader]] calls [[BlockManager#removeBlock]] defensively
   * before [[BlockManager#putBytes]] so the cumulative bytes replace any prior partial
   * spill -- the reader always sees one canonical block per
   * `(shuffleId, mapId, reduceId)`.
   *
   * Memory cost: each accumulator grows to the partition's full byte total over the
   * lifetime of `write`. In workloads that fit in memory the writer therefore holds
   * the partition output twice (once in `partitionBuffers` for the block/spill window,
   * plus once in this accumulator) before `persistPartitionsForReader` runs and the
   * accumulators become eligible for GC at the end of `write` once `putBytes` has
   * copied the bytes into the [[org.apache.spark.storage.DiskStore]]. The accumulator
   * slots are nulled in [[stop]] so the underlying byte arrays can be garbage-
   * collected even if some other code retains a reference to this writer instance.
   *
   * Constructed with the small [[INITIAL_BAOS_CAPACITY]] (1 KB) initial capacity so
   * total construction-time JVM-heap allocation is `INITIAL_BAOS_CAPACITY * numPartitions`,
   * matching the construction-cost discipline applied to [[partitionBuffers]].
   */
  private val partitionPersistBuffers: Array[ByteArrayOutputStream] =
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
   * Per-partition long-lived [[org.apache.spark.serializer.SerializationStream]] that
   * targets [[partitionPersistBuffers]] -- the persist-channel companion to
   * [[partitionSerStreams]] (which targets [[partitionBuffers]] and is opened/closed at
   * every block boundary).
   *
   * == Why a Separate Stream ==
   * The wire-format stream chain (`partitionSerStreams`) closes at every
   * [[BLOCK_SIZE_BYTES]] block boundary (in [[flushBlock]] and [[maybeSpill]]) so that
   * EACH BLOCK on the wire is an independently deserializable stream -- the wire-format
   * invariant required by [[StreamingShuffleReader]]'s per-block deserialization. This
   * close-and-reopen cycle writes a Kryo (or Java) stream FOOTER before every drain and
   * a fresh HEADER on the next [[ensurePartitionStream]] call, so the bytes drained from
   * `partitionBuffers` across N flushes form N complete-with-header-and-footer streams
   * concatenated together.
   *
   * If we attempted to populate [[partitionPersistBuffers]] from those drained-and-
   * concatenated bytes (the prior implementation), the resulting per-partition byte
   * total would be a CONCATENATION OF MULTIPLE STREAMS -- which the reader's
   * `serializerInstance.deserializeStream(persistBytes).asKeyValueIterator` cannot
   * deserialize as a single stream (it would read past the first stream's footer into
   * the second stream's header and throw `KryoException: Stream is corrupted` or the
   * Java-serializer equivalent).
   *
   * The persist channel therefore needs ITS OWN long-lived [[SerializationStream]] that
   * stays open for the entire lifetime of [[write]] -- writing exactly ONE header at
   * first record per partition, accumulating all subsequent records, and writing ONE
   * footer when explicitly closed by [[closeAllPartitionPersistStreams]] just before
   * [[persistPartitionsForReader]] runs. The resulting per-partition byte stream in
   * [[partitionPersistBuffers]] is a single complete Kryo (or Java) stream that the
   * reader can deserialize end-to-end.
   *
   * == Cost Trade-Off ==
   * Each record is now serialized TWICE: once into the wire-format chain
   * (`partitionSerStreams` -> `partitionCheckedStreams` -> `partitionBuffers`) and once
   * into the persist chain (this field -> `partitionPersistBuffers`). This doubles
   * per-record serialization CPU cost. For v1 the cost is acceptable because:
   *   - the streaming-shuffle path is opt-in;
   *   - the persist channel is the primary data plane consumed by the reader (the
   *     wire-format channel's `BackpressureProtocol.recordTransmission` does NOT
   *     perform actual network I/O in the v1 implementation -- it only records
   *     rate-limit/heartbeat bookkeeping);
   *   - the doubled cost remains bounded by the serializer's per-record amortized
   *     cost, which is dominated by JVM memory bandwidth on the doubled-buffer write
   *     and not by additional serializer state (the serializer instance is reused).
   *
   * A future v2 optimization could collapse the two chains into one by either
   * (a) having the reader deserialize the wire-format multi-stream concatenation by
   * peeking-EOF-and-reopening between records, or (b) having the writer produce a
   * single continuous wire stream and emit byte-range delimiters out-of-band; both
   * are out of scope for v1 per the AAP rule "*Make only changes necessary to
   * implement streaming shuffle capability within ShuffleManager abstraction
   * boundary.*"
   *
   * == Lifecycle ==
   *   1. Slot is `null` at writer construction.
   *   2. First call to [[ensurePartitionPersistStream]] for a partition (triggered by
   *      the first write to that partition in [[write]]) lazily wraps
   *      `serInstance.serializeStream(partitionPersistBuffers(partitionId))` and stores
   *      the result in this slot. The serializer header is written into the BAOS.
   *   3. Subsequent record writes call `ss.writeKey(k); ss.writeValue(v)` on the same
   *      slot (NO close-and-reopen).
   *   4. After the residual-drain `flushBlock` loop in [[write]] but before
   *      [[persistPartitionsForReader]] runs, [[closeAllPartitionPersistStreams]]
   *      closes each non-null slot which writes the serializer footer into the BAOS;
   *      the slot is then nulled so [[stop]]'s defensive close-and-null pass observes
   *      the closed state.
   *   5. [[stop]] defensively closes any slot that survived an exception path through
   *      [[write]] (e.g. a serializer error) so no `SerializationStream` reference
   *      leaks beyond the writer's lifetime.
   *
   * == Memory Discipline ==
   * Each `SerializationStream` reference is a thin object around the serializer's
   * internal buffer plus a reference to `partitionPersistBuffers(i)`. Construction
   * cost is `O(numActivePartitions)` (one per partition that observes records), not
   * `O(numPartitions)` -- partitions that never receive a record never pay the
   * ensurePartitionPersistStream cost.
   */
  private val partitionPersistSerStreams: Array[SerializationStream] =
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
    if (debugEnabled) {
      logDebug(log"StreamingShuffleWriter " +
        log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
        log"mapId=${MDC(MAP_ID, mapId)} " +
        log"acquired=${MDC(NUM_BYTES, acquiredMemoryBytes)} / " +
        log"requested=${MDC(NUM_BYTES, requested)} execution memory")
    }

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

        // Mirror the record write into the per-partition long-lived persist stream so
        // [[partitionPersistBuffers]] accumulates one continuous, deserializable Kryo
        // (or Java) stream per partition (header + all records + footer), regardless
        // of how many block-boundary close/reopen cycles the wire-format chain
        // performs above. Per the field-level Scaladoc on
        // [[partitionPersistSerStreams]], this is the persist channel that backs the
        // reader's `fetchBlockSync` round trip via [[StreamingShuffleManager.shuffleBlockResolver]]
        // -- the reader's `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator`
        // requires the persisted bytes to be a single contiguous stream rather than a
        // concatenation of multiple complete-with-header-and-footer streams.
        //
        // No per-record flush() here: the persist stream is only drained at end-of-
        // write by [[closeAllPartitionPersistStreams]] which writes the footer once
        // per partition and (along with any serializer-internal buffer flush done by
        // close) lands all accumulated bytes into [[partitionPersistBuffers]].
        val pStream = ensurePartitionPersistStream(partitionId, serInstance)
        pStream.writeKey(key)
        pStream.writeValue(value)

        // If the partition's accumulated bytes have crossed the 2 MB block boundary,
        // flush a block onto the (notional) network. flushBlock closes the partition's
        // serialization stream so the drained bytes form a complete stream with both
        // header and footer; the next ensurePartitionStream call (triggered by the
        // next record for this partition) lazily allocates a fresh stream that writes
        // a fresh header into the now-empty buffer.
        //
        // The persist channel is NOT closed at block boundaries -- only the wire-
        // format channel is. This is the entire point of the persist channel's
        // existence: per-partition single-stream invariant for reader correctness.
        // partitionLengths is reconciled at end-of-write after persistPartitionsForReader
        // so MapStatus reflects the actual persisted byte count regardless of how many
        // wire-format blocks were emitted along the way.
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

      // Close each per-partition long-lived persist stream so the serializer footer
      // is written into [[partitionPersistBuffers]] EXACTLY ONCE per partition. After
      // this call, each `partitionPersistBuffers(i)` contains a single complete and
      // independently deserializable serialization stream (header + all records +
      // footer) -- the format that
      // [[StreamingShuffleReader]]'s `serializerInstance.deserializeStream(blockBytes)
      // .asKeyValueIterator` requires for end-to-end deserialization.
      //
      // Distinct from the wire-format channel's residual-drain `flushBlock` loop above
      // (which closed-and-reopened the wire-format streams at every block boundary
      // throughout the loop), the persist channel's streams are closed here for the
      // first and only time. Per the [[partitionPersistSerStreams]] field Scaladoc.
      closeAllPartitionPersistStreams()

      // Publish each partition's cumulative bytes to the executor's [[BlockManager]]
      // under the streaming-shuffle blockId pattern
      // `ShuffleBlockId(dep.shuffleId, mapId, partitionId)`. This is the data plane
      // that exposes streaming-shuffle output to downstream
      // [[StreamingShuffleReader]] fetches: the reader's `fetchBlockSync` call routes
      // through `BlockManager.getLocalBlockData` which dispatches shuffle block lookups
      // to `shuffleManager.shuffleBlockResolver.getBlockData` -- and
      // [[StreamingShuffleManager.shuffleBlockResolver]] is a custom resolver that
      // serves blocks from the [[org.apache.spark.storage.DiskStore]] populated by
      // this call. This MUST happen before the MapStatus is published (the next
      // statement) because the DAG scheduler treats the MapStatus as the signal that
      // the map output is available for fetch -- if the bytes were not yet on disk,
      // a reader on the same executor could observe a missing block.
      persistPartitionsForReader()

      // Reconcile [[partitionLengths]] to match the actual byte counts persisted to
      // [[org.apache.spark.storage.BlockManager]] by [[persistPartitionsForReader]].
      // The per-partition wire-format byte counts incremented in [[flushBlock]] and
      // [[maybeSpill]] reflect the wire-format channel's [header+records+footer]-per-
      // block sequence; the persisted bytes (single Kryo stream per partition: one
      // header + all records + one footer) have a slightly different length. Setting
      // [[partitionLengths]] to the persisted byte count ensures
      // [[org.apache.spark.scheduler.MapStatus.getSizeForBlock]] returns the value
      // the reader will actually fetch via `BlockManager.fetchBlockSync` -- making
      // [[org.apache.spark.shuffle.metrics.ShuffleReadMetricsReporter#incRemoteBytesRead]]
      // accurate and ensuring [[org.apache.spark.scheduler.HighlyCompressedMapStatus]]'
      // average-size heuristic reflects the actual fetched byte volume.
      var k = 0
      while (k < numPartitions) {
        val acc = partitionPersistBuffers(k)
        if (acc != null) {
          partitionLengths(k) = acc.size().toLong
        }
        k += 1
      }

      val durationNs = System.nanoTime() - startNs
      writeMetrics.incWriteTime(durationNs)

      // Build the MapStatus. The factory chooses CompressedMapStatus or
      // HighlyCompressedMapStatus based on partition count -- we do not pick directly.
      val aggregatedChecksum = aggregateChecksumValue()
      mapStatus = Some(
        MapStatus(blockManager.shuffleServerId, partitionLengths, mapId, aggregatedChecksum))

      // Per-task completion logging is gated by the streaming-shuffle debug flag and
      // emitted at DEBUG level rather than INFO to honor the AAP Section 0.7.2.5
      // quality budget: "Log volume capped at <10MB/hour per executor for streaming
      // events ... INFO/DEBUG logs must be rate-limited or sampled; only WARN/ERROR
      // may pass freely." A 10-partition shuffle with 50 concurrent streams (per the
      // stress-test workload) emits up to 50 writer-completion lines per second; an
      // INFO line per completion would exceed the 10 MB/hour budget under sustained
      // load. Operators retain visibility into per-task timing and byte counts via
      // the existing `ShuffleWriteMetricsReporter` (which feeds the Web UI Stages
      // tab and the executor metrics) and via the streaming-shuffle Dropwizard
      // metrics (`shuffle.streaming.*`). Enable verbose per-task traces by setting
      // `spark.shuffle.streaming.debug=true` AND log4j level DEBUG for this logger.
      if (debugEnabled) {
        logDebug(log"StreamingShuffleWriter completed " +
          log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
          log"mapId=${MDC(MAP_ID, mapId)} " +
          log"totalBytes=${MDC(NUM_BYTES, partitionLengths.sum)} " +
          log"numPartitions=${MDC(NUM_PARTITIONS, numPartitions)} " +
          log"durationNs=${MDC(DURATION, durationNs)} " +
          log"aggregatedChecksum=${MDC(CHECKSUM, aggregatedChecksum)}")
      }
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
      //
      // Two parallel close loops: (1) the wire-format channel via [[partitionSerStreams]]
      // which closes-and-reopens per block during normal writes -- in stop's failure
      // path the loop's last block may still be open; and (2) the persist channel via
      // [[partitionPersistSerStreams]] which is normally closed by
      // [[closeAllPartitionPersistStreams]] just before [[persistPartitionsForReader]]
      // in the success path -- in failure paths the persist streams may still be open.
      // Both are closed defensively here so JVM file-descriptor and direct-memory
      // resources held by the underlying serializers (e.g. Kryo's pooled buffers) are
      // released regardless of write-path success or failure.
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
        // Defensively close any still-open persist channel SerializationStream slot.
        // In the success path [[closeAllPartitionPersistStreams]] already nulled this
        // slot before [[persistPartitionsForReader]] ran; in failure paths the slot
        // may still be non-null so we close it here to release serializer-internal
        // resources. As with the wire-format close above we catch and log any error
        // because `stop` must be best-effort.
        val ps = partitionPersistSerStreams(i)
        if (ps != null) {
          try {
            ps.close()
          } catch {
            case t: Throwable =>
              logWarning(log"Failed to close per-partition persist " +
                log"SerializationStream " +
                log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
                log"mapId=${MDC(MAP_ID, mapId)} " +
                log"reduceId=${MDC(REDUCE_ID, i)}: " +
                log"${MDC(ERROR, Option(t.getMessage).getOrElse("(no message)"))}")
          }
          partitionPersistSerStreams(i) = null
        }
        // Null per-partition buffer / checksum / interceptor / persist-accumulator
        // slots so their underlying byte arrays and references become eligible for GC
        // even if some other code retains a reference to this writer instance. The
        // persist accumulator is nulled here AFTER `persistPartitionsForReader` ran
        // (during write) so the cumulative bytes for the partition were already
        // copied into the [[org.apache.spark.storage.DiskStore]] via
        // `BlockManager.putBytes`; the in-memory accumulator is no longer needed.
        partitionBuffers(i) = null
        partitionCheckedStreams(i) = null
        partitionChecksums(i) = null
        partitionPersistBuffers(i) = null
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
   * Lazily allocate the per-partition long-lived persist [[SerializationStream]] on
   * first use. Unlike [[ensurePartitionStream]] (the wire-format channel which
   * close-and-reopens at every block boundary), this stream stays open for the entire
   * `write()` lifetime and accumulates all records for the partition into a single
   * [header + records + footer] sequence inside [[partitionPersistBuffers]] -- the
   * exact format the downstream [[StreamingShuffleReader]] expects when it calls
   * `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator` once over the
   * whole fetched partition.
   *
   * Constructs the stream by wrapping [[partitionPersistBuffers]] (a
   * [[java.io.ByteArrayOutputStream]]) via
   * [[org.apache.spark.serializer.SerializerManager#wrapStream]] BEFORE handing the
   * resulting [[java.io.OutputStream]] to `serInstance.serializeStream(...)`. This
   * write-side wrap is REQUIRED for symmetry with the reader: the
   * [[StreamingShuffleReader#read]] path applies
   * `serializerManager.wrapStream(blockId, byteStream)` on the input side BEFORE
   * `serializerInstance.deserializeStream(...)`, so the bytes emitted into
   * `partitionPersistBuffers(partitionId)` MUST be in the post-wrap encoding (encrypted
   * if `spark.io.encryption.enabled=true`, then LZ4-compressed if `spark.shuffle.compress=true`,
   * the Spark default). Without the symmetric write-side wrap, the LZ4 decompressor on
   * the read side reports `Stream is corrupted` because raw Kryo bytes lack LZ4's magic
   * header. No `MutableCheckedOutputStream` is interposed because the persist channel
   * does NOT participate in per-block CRC32C computation -- those checksums are computed
   * by the wire-format channel via [[partitionCheckedStreams]] for in-flight integrity
   * validation, and the persist channel's bytes are fetched via
   * [[org.apache.spark.storage.BlockManager]] which has its own integrity guarantees
   * (atomic file rename + at-most-once block id).
   *
   * Idempotent: calling for a partition whose persist stream is already non-null is a
   * no-op (returns the existing stream).
   *
   * Memory discipline: the stream's buffer is the partition's
   * [[partitionPersistBuffers]] [[java.io.ByteArrayOutputStream]] which grows as
   * records are appended. The buffer is released at end-of-write by
   * [[persistPartitionsForReader]] (after the bytes are published to
   * [[org.apache.spark.storage.BlockManager]]) and on failure by [[stop]].
   *
   * @param partitionId  the reduce partition whose persist stream is being ensured
   * @param serInstance  the [[org.apache.spark.serializer.SerializerInstance]] used to
   *                     wrap the buffer; must be the same instance used for the
   *                     wire-format channel so both channels produce binary-equivalent
   *                     records (the bytes differ only in the wire-format channel's
   *                     close-and-reopen markers vs. the persist channel's single
   *                     contiguous stream)
   * @return the long-lived [[SerializationStream]] for `partitionId`
   */
  private def ensurePartitionPersistStream(
      partitionId: Int,
      serInstance: org.apache.spark.serializer.SerializerInstance): SerializationStream = {
    var ss = partitionPersistSerStreams(partitionId)
    if (ss == null) {
      // Wrap the persist accumulator via the SerializerManager so the persisted bytes are
      // encrypted (if shuffle encryption is enabled) and compressed (if
      // `spark.shuffle.compress=true`, the default) prior to Kryo/Java serialization.
      // The downstream [[StreamingShuffleReader#read]] applies the symmetric
      // `serializerManager.wrapStream(blockId, byteStream)` on the input side BEFORE
      // calling `deserializeStream`, so omitting this write-side wrap causes the LZ4
      // (or other) compression codec on the read side to fail with
      // `KryoException: java.io.IOException: Stream is corrupted` when the raw Kryo bytes
      // lack the codec's magic header. The `blockId` here MUST match the blockId used by
      // [[persistPartitionsForReader]] when handing the accumulator's bytes to
      // [[BlockManager#putBytes]] (`ShuffleBlockId(dep.shuffleId, mapId, partitionId)`)
      // because [[org.apache.spark.serializer.SerializerManager#shouldCompress]] dispatches
      // on the blockId's runtime type and the reader uses the same blockId on the read side.
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, partitionId)
      val wrappedOut: java.io.OutputStream = blockManager.serializerManager.wrapStream(
        blockId, partitionPersistBuffers(partitionId))
      ss = serInstance.serializeStream(wrappedOut)
      partitionPersistSerStreams(partitionId) = ss
    }
    ss
  }

  /**
   * Close every long-lived per-partition persist [[SerializationStream]] in
   * [[partitionPersistSerStreams]] exactly once, writing the serializer footer (e.g.
   * Kryo's stream-footer marker) into [[partitionPersistBuffers]] for each non-null
   * slot, and nulls each slot.
   *
   * Called from `write()` after the residual-drain `flushBlock` loop and BEFORE
   * [[persistPartitionsForReader]] so that, by the time bytes are handed to
   * [[org.apache.spark.storage.BlockManager#putBytes]], each partition's accumulator
   * holds a single complete and independently deserializable Kryo stream. Also called
   * defensively from [[stop]]'s cleanup loop in case `write()` aborted before reaching
   * this normal-path call.
   *
   * Per-stream errors during close are logged at warn level and execution continues so
   * that one partition's close failure does not strand resources for other partitions.
   * Memory accounting is unaffected by close (the underlying
   * [[java.io.ByteArrayOutputStream]] retains its bytes until consumed by
   * [[persistPartitionsForReader]] or released by [[releaseAcquiredMemory]]).
   *
   * Idempotent: re-invocation after the first call is a no-op (every slot is null).
   */
  private def closeAllPartitionPersistStreams(): Unit = {
    var i = 0
    while (i < numPartitions) {
      val ss = partitionPersistSerStreams(i)
      if (ss != null) {
        try {
          ss.close()
        } catch {
          case t: Throwable =>
            logWarning(
              s"Error closing persist stream for shuffle ${dep.shuffleId} mapId=$mapId " +
                s"partition=$i; bytes already accumulated will still be persisted but the " +
                s"stream footer may be missing", t)
        }
        partitionPersistSerStreams(i) = null
      }
      i += 1
    }
  }

  /**
   * Flush the accumulated bytes for one partition as a single wire-format block (up
   * to [[BLOCK_SIZE_BYTES]] = 2 MB). Closes the partition's per-block
   * [[SerializationStream]] so the drained bytes form a complete, independently
   * deserializable stream (with header AND footer); computes a per-block CRC32C
   * checksum on the exact bytes flushed; hands the block to
   * [[BackpressureProtocol#recordTransmission]]; and updates the bytes-written metric.
   *
   * NOTE: this method does NOT update [[partitionLengths]] -- the wire-format byte
   * counts incremented per-block here are NOT used because [[partitionLengths]] is
   * reconciled at end-of-write in `write()` to match the persist channel's actual
   * persisted byte counts (see the dual-channel design in the class-level Scaladoc and
   * the [[partitionPersistSerStreams]] field Scaladoc).
   *
   * NOTE: this method also does NOT append to the persist channel ([[partitionPersistBuffers]]).
   * The persist channel maintains its own long-lived [[SerializationStream]] per
   * partition (via [[ensurePartitionPersistStream]] called from `write()`) which
   * accumulates a single contiguous [header + records + footer] sequence. Appending
   * the wire-format multi-block byte sequence here would corrupt the persist channel's
   * single-stream invariant.
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
   * now-empty buffer -- this is the wire-format invariant.
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

    // Note: the persist channel does NOT receive the drained wire-format bytes here.
    // The drained `bytes` represent the wire-format channel's per-block sequence of
    // [Kryo-header + records + footer], which when concatenated across multiple blocks
    // would produce a multi-header byte sequence that downstream
    // [[StreamingShuffleReader]] cannot deserialize as a single Kryo stream.
    // Instead, the persist channel maintains its own long-lived [[SerializationStream]]
    // per partition (via [[ensurePartitionPersistStream]] called from `write()`) which
    // accumulates a single contiguous [header + all records + footer] sequence in
    // [[partitionPersistBuffers]] -- the format the reader expects when it calls
    // `serializerInstance.deserializeStream(blockBytes).asKeyValueIterator` once over
    // the whole fetched partition. See the [[partitionPersistSerStreams]] field
    // Scaladoc and [[ensurePartitionPersistStream]] for the dual-channel design.
    //
    // partitionLengths is reconciled at end-of-write in `write()` AFTER
    // [[persistPartitionsForReader]] runs, by setting
    // `partitionLengths(i) = partitionPersistBuffers(i).size().toLong` so MapStatus
    // reflects the actual byte counts the reader will fetch from BlockManager. The
    // wire-format byte counts incremented per-block here are NOT used because the
    // wire-format channel does not perform network I/O in this checkpoint -- it exists
    // for forward compatibility with a future streaming transport layer.

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

    if (debugEnabled && isTraceEnabled()) {
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

      // Note: the persist channel does NOT receive the drained wire-format bytes here.
      // The persist channel maintains its own long-lived [[SerializationStream]] per
      // partition (via [[ensurePartitionPersistStream]] called from `write()`) which
      // continues to accumulate records into [[partitionPersistBuffers]] across
      // close-and-reopen cycles of the wire-format channel. See
      // [[partitionPersistSerStreams]] field Scaladoc and the equivalent comment in
      // [[flushBlock]] for the dual-channel rationale: appending the wire-format
      // multi-header bytes to the persist accumulator would corrupt the single-stream
      // invariant the reader requires.
      //
      // Note on `MemorySpillManager#checkAndSpill` interaction: [[MemorySpillManager
      // #checkAndSpill]] also writes to the same `ShuffleBlockId` via
      // `BlockManager.putBytes`. [[persistPartitionsForReader]] runs at end-of-write
      // and performs a defensive [[BlockManager#removeBlock]] before
      // [[BlockManager#putBytes]] so the persisted persist-channel stream replaces any
      // prior spill, ensuring the reader sees one canonical block per
      // `(shuffleId, mapId, reduceId)`.
      //
      // partitionLengths is reconciled at end-of-write in `write()` AFTER
      // [[persistPartitionsForReader]] runs.

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

      if (debugEnabled && isTraceEnabled()) {
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
   * Persist each partition's cumulative bytes from [[partitionPersistBuffers]] to the
   * executor's [[BlockManager]] under the streaming-shuffle blockId pattern
   * `ShuffleBlockId(dep.shuffleId, mapId, partitionId)`. This is the producer side of
   * the streaming-shuffle data plane: each block stored here becomes addressable by
   * the [[StreamingShuffleReader]] via
   * [[BlockManager#blockTransferService]]`.fetchBlockSync(...)` calls which route
   * (in local mode through Netty's loopback path or in cluster mode through the
   * external shuffle service) back into `BlockManager.getLocalBlockData` which
   * dispatches shuffle block requests to
   * `shuffleManager.shuffleBlockResolver.getBlockData` -- and the resolver served by
   * [[StreamingShuffleManager.shuffleBlockResolver]] is a custom resolver that reads
   * from the [[org.apache.spark.storage.DiskStore]] entry populated by this call.
   *
   * Per-partition write sequence:
   *   1. If the partition's accumulator is empty (no records were written for that
   *      partition), skip -- the reader is allowed to observe a zero-byte partition
   *      via [[org.apache.spark.scheduler.MapStatus]] returning 0L for that reducer.
   *   2. Defensively call [[BlockManager#removeBlock]]`(blockId, tellMaster = false)`
   *      to clear any prior block at the same blockId. This handles the case where
   *      [[MemorySpillManager#checkAndSpill]] previously persisted a partial spill to
   *      the same `ShuffleBlockId` (since both the spill path and this end-of-write
   *      path target the same `ShuffleBlockId(shuffleId, mapId, reduceId)` namespace).
   *      `tellMaster = false` skips publishing a "block removed" event to the
   *      `BlockManagerMaster` so we do not generate a notification for a transient
   *      pre-existing block that the master may not even know about. The
   *      [[BlockManager#removeBlock]] call is itself idempotent and best-effort: if
   *      the block does not exist it logs a warning and returns silently. We catch
   *      and log any unexpected exception so a single partition's removal failure
   *      does not abort the entire end-of-write persist sequence -- the immediately
   *      following [[BlockManager#putBytes]] call will itself surface a meaningful
   *      error if the block actually still exists.
   *   3. Wrap the cumulative bytes in a [[ChunkedByteBuffer]] (zero-copy via
   *      [[ByteBuffer#wrap]]) and call [[BlockManager#putBytes]] with
   *      `StorageLevel.DISK_ONLY` and `tellMaster = true` -- mirroring the storage
   *      semantics applied by [[MemorySpillManager#checkAndSpill]] so the
   *      `BlockManagerMaster` tracks the produced block via the standard storage-
   *      status reporting path. `tellMaster = true` is essential here because the
   *      `MapOutputTracker` uses the block-existence reports to satisfy reader
   *      lookups for non-local executors.
   *   4. Dispose the [[ChunkedByteBuffer]] in a `finally` to release any direct-
   *      memory reference held by the wrapper (the underlying byte array is still
   *      referenced by the accumulator until [[stop]] nulls the slot, but `dispose`
   *      releases any wrapper-internal state to be GC-friendly).
   *
   * Idempotency: if a caller invokes [[write]] a second time on the same writer
   * instance (not a supported usage -- the SPI contract calls `write` once per
   * writer), this method would re-publish the cumulative bytes and the defensive
   * [[BlockManager#removeBlock]] would clear the prior block. In normal operation
   * this method runs exactly once per writer instance from [[write]] just before
   * `mapStatus` is built.
   */
  private def persistPartitionsForReader(): Unit = {
    var i = 0
    while (i < numPartitions) {
      val accumulator = partitionPersistBuffers(i)
      if (accumulator != null && accumulator.size() > 0) {
        val bytes = accumulator.toByteArray
        val blockId = ShuffleBlockId(dep.shuffleId, mapId, i)

        // Step 2: defensive removeBlock so a prior partial spill (or a retry attempt
        // of the same map task) does not block the putBytes that follows. The
        // tellMaster = false flag suppresses a BlockManagerMaster update for a block
        // that the master may not even have a record of.
        try {
          blockManager.removeBlock(blockId, tellMaster = false)
        } catch {
          case t: Throwable =>
            // BlockManager.removeBlock is idempotent and best-effort; if the block
            // is absent it logs a warning and returns. Any other exception (e.g.
            // a lock-acquisition failure) is logged and absorbed -- the immediately
            // following putBytes call surfaces the actionable error if the block
            // still exists.
            if (debugEnabled) {
              logDebug(log"removeBlock pre-cleanup failed for " +
                log"${MDC(BLOCK_ID, blockId.toString)}: " +
                log"${MDC(ERROR, Option(t.getMessage).getOrElse("(no message)"))}")
            }
        }

        // Step 3: wrap and persist via BlockManager.putBytes(DISK_ONLY).
        // ByteBuffer.wrap(bytes) produces a heap ByteBuffer with position = 0 which
        // satisfies ChunkedByteBuffer's invariant.
        val chunked = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))
        try {
          val stored = blockManager.putBytes(
            blockId,
            chunked,
            StorageLevel.DISK_ONLY,
            tellMaster = true)(scala.reflect.ClassTag.Byte)
          if (!stored) {
            // BlockManager.putBytes returns false when the doPut path exited via the
            // "block already exists" early return at BlockManager.scala:1576-1578 --
            // i.e. the prior removeBlock did not succeed in clearing the block.
            // The reader will observe whatever bytes are currently in the DiskStore;
            // log a warning so operators can correlate this with the prior spill.
            // Operations runbook: see blitzy-docs/streaming-shuffle/observability.md
            // section "BlockManager.putBytes returns false" for triage and recovery
            // guidance. WARN passes the source-site debug gate freely per AAP
            // Section 0.7.2.5 *"Log volume capped at <10MB/hour per executor"*; only
            // INFO/DEBUG/TRACE statements are subject to the debugEnabled gate.
            logWarning(log"BlockManager.putBytes reported block-already-exists for " +
              log"${MDC(BLOCK_ID, blockId.toString)} " +
              log"shuffleId=${MDC(SHUFFLE_ID, dep.shuffleId)} " +
              log"mapId=${MDC(MAP_ID, mapId)} " +
              log"reduceId=${MDC(REDUCE_ID, i)}; " +
              log"reader may observe stale partial-spill bytes")
          } else if (debugEnabled && isTraceEnabled()) {
            logTrace(log"Persisted ${MDC(BLOCK_ID, blockId.toString)} " +
              log"size=${MDC(NUM_BYTES, bytes.length.toLong)} for streaming reader")
          }
        } finally {
          // Step 4: dispose the ChunkedByteBuffer wrapper. This is a best-effort
          // release; any retained direct memory is freed back to the buffer pool.
          // Swallow exceptions so a single dispose failure does not abort the
          // end-of-write persist sequence for the remaining partitions.
          try {
            chunked.dispose()
          } catch {
            case t: Throwable =>
              if (debugEnabled) {
                logDebug(log"ChunkedByteBuffer.dispose failed for " +
                  log"${MDC(BLOCK_ID, blockId.toString)}: " +
                  log"${MDC(ERROR, Option(t.getMessage).getOrElse("(no message)"))}")
              }
          }
        }
      }
      i += 1
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
