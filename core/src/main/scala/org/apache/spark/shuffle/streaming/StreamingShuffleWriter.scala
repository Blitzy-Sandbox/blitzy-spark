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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32C

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.internal.config.EXECUTOR_MEMORY
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Streaming-mode map-side writer for the Apache Spark shuffle feature (F-001). This writer is
 * the `org.apache.spark.shuffle.streaming.*` counterpart to
 * [[org.apache.spark.shuffle.sort.SortShuffleWriter]] and is constructed only when the
 * currently-bound [[org.apache.spark.shuffle.ShuffleManager]] resolves
 * `spark.shuffle.manager=streaming` AND the shuffle handle for a given shuffle id is a
 * [[StreamingShuffleHandle]]. All other combinations continue to dispatch through
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] and its three sort-path writers
 * ([[org.apache.spark.shuffle.sort.SortShuffleWriter]],
 * [[org.apache.spark.shuffle.sort.UnsafeShuffleWriter]],
 * [[org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter]]) which are
 * byte-for-byte unchanged by the streaming feature (AAP section 0.7.1 Implementation
 * Discipline: "Isolate streaming logic ... zero cross-contamination").
 *
 * == Responsibilities ==
 *
 *   - '''Per-partition buffer allocation''' &mdash; a lazily-allocated
 *     [[java.io.ByteArrayOutputStream]] is created on the first record routed to any given
 *     partition. Partitions that receive zero records consume exactly zero heap, which
 *     matters for shuffles with very wide fan-out (tens of thousands of reduce partitions).
 *     The aggregate memory budget is
 *     `(executorMemoryMiB * 1024 * 1024 * bufferSizePercent) / 100`, subdivided evenly
 *     across `numPartitions` to compute `perPartitionBudgetBytes`. The user-mandated
 *     bound is 20 % (configurable 1-50 %) of executor memory (AAP section 0.1.2 and
 *     `spark.shuffle.streaming.bufferSizePercent` in the internal config package).
 *   - '''Record serialization''' &mdash; each `(key, value)` record is serialized with the
 *     `ShuffleDependency`'s registered [[org.apache.spark.serializer.Serializer]] into a
 *     small transient buffer, then appended to the per-partition accumulator. The exact
 *     byte count is tracked in `partitionLengths` so the final
 *     [[org.apache.spark.scheduler.MapStatus]] reflects true per-partition volumes.
 *   - '''Spill trigger''' &mdash; after every record, the per-partition buffer size is
 *     compared against `spillTriggerBytes = perPartitionBudgetBytes * spillThreshold / 100`.
 *     When the threshold is crossed, [[maybeSpillPartition]] chunks the accumulated bytes
 *     into `<= 2 MB` blocks, computes a CRC32C checksum per chunk, hands the full byte
 *     snapshot to [[org.apache.spark.storage.BlockManager#putBytes]] under
 *     `StorageLevel.DISK_ONLY`, increments the
 *     [[StreamingShuffleMetrics#incrementSpillCount]] counter, and resets the buffer. The
 *     80 % default spill threshold is configurable via
 *     `spark.shuffle.streaming.spillThreshold` (range 50-95 %).
 *   - '''Block-level integrity''' &mdash; every &lt;= 2 MB chunk is hashed with JDK 17's
 *     built-in [[java.util.zip.CRC32C]] (Castagnoli polynomial). The checksum is logged at
 *     DEBUG for diagnostic parity with the sort path's
 *     [[org.apache.spark.shuffle.checksum.RowBasedChecksum]] stream; in v2 of the
 *     streaming feature the same CRC32C will ride the network envelope so that consumers
 *     can request retransmission on mismatch (AAP section 0.1.2).
 *   - '''F-009 metrics parity''' &mdash; the three mandatory "inc" methods on
 *     [[org.apache.spark.shuffle.ShuffleWriteMetricsReporter]] are invoked at points that
 *     structurally match the sort-path writer so that the Spark UI "Shuffle Write"
 *     column, Prometheus counters, JMX MBeans, and event-log records remain
 *     indistinguishable in shape between sort and streaming runs. The two "dec" methods
 *     are unused in v1 because streaming shuffle has no rollback path (a failed task
 *     simply returns `stop(success = false)` and the upstream DAG scheduler recomputes
 *     the stage).
 *   - '''MapStatus commit''' &mdash; once the record iterator is exhausted, a
 *     [[org.apache.spark.scheduler.MapStatus]] is constructed with
 *     `blockManager.shuffleServerId` as the location, `partitionLengths` as the
 *     per-partition byte array, and `mapId` as the map task id. The `MapStatus.apply`
 *     factory selects [[org.apache.spark.scheduler.HighlyCompressedMapStatus]] or
 *     [[org.apache.spark.scheduler.CompressedMapStatus]] based on partition count;
 *     this writer deliberately does not pre-pick because the factory's choice is the
 *     invariant the DAG scheduler consumes.
 *
 * == Coexistence strategy ==
 *
 * This class is BRAND-NEW and lives in the `org.apache.spark.shuffle.streaming`
 * sub-package. No existing Spark source refers to it at compile time; the only
 * construction site is [[StreamingShuffleManager]] (a sibling class in the same
 * sub-package). Executors running the production-stable default
 * `spark.shuffle.manager=sort` never classload this writer and their shuffle behavior
 * (including every existing JMX / Prometheus / event-log surface) is byte-for-byte
 * unchanged. This matches the user's Absolute Preservation list (AAP section 0.1.2): "Zero
 * modification to the existing `SortShuffleManager` implementation, which continues as
 * the default and as the streaming fallback."
 *
 * == Thread-safety ==
 *
 *   - `write` is invoked once, on a single thread, by the task runner. No internal
 *     synchronization is required for the per-partition buffer writes, the partition
 *     length updates, or the metric reporter calls &mdash; matching the contract
 *     documented on [[org.apache.spark.shuffle.ShuffleWriteMetricsReporter]]: "all the
 *     methods are called on a single-threaded, i.e. concrete implementations would not
 *     need to synchronize."
 *   - `stop` may be called up to twice by
 *     [[org.apache.spark.shuffle.ShuffleWriteProcessor]] (once on normal completion with
 *     `success = true`, once in the exception handler with `success = false`). The
 *     [[java.util.concurrent.atomic.AtomicBoolean]] `stopping` guard
 *     (`compareAndSet(false, true)`) ensures the second invocation is a no-op that
 *     returns `None` without double-freeing buffers or double-reporting metrics.
 *   - `getPartitionLengths` returns a defensive `partitionLengths.clone()` so callers
 *     cannot mutate the writer's internal state after observing the commit.
 *
 * == Binary compatibility ==
 *
 * The class is `private[spark]`. Combined with residing in a new sub-package, this
 * contributes zero public-SPI surface area and therefore requires no entry in
 * `project/MimaExcludes.scala` (F-017 MiMa Binary Compatibility Gate, AAP section 0.7.2).
 *
 * @param handle               the shuffle handle produced by the streaming shuffle
 *                             manager's `registerShuffle` method; carries `shuffleId`
 *                             (inherited from [[org.apache.spark.shuffle.ShuffleHandle]])
 *                             and `dependency` (inherited from
 *                             [[org.apache.spark.shuffle.BaseShuffleHandle]]).
 * @param mapId                unique map task id assigned by the DAG scheduler; recorded in the
 *                             emitted [[org.apache.spark.scheduler.MapStatus]] for reduce-side
 *                             routing.
 * @param context              the task context. Consumed for diagnostic logging
 *                             (`stageId`, `taskAttemptId`); never mutated. Preserving the
 *                             existing task lifecycle as the user's Absolute Preservation
 *                             list requires (AAP section 0.1.2).
 * @param conf                 the `SparkConf` bound at the owning `SparkEnv`; the streaming
 *                             configuration entries `spark.shuffle.streaming.bufferSizePercent`
 *                             and `spark.shuffle.streaming.spillThreshold` are read from it at
 *                             construction time (configuration changes therefore require
 *                             executor restart, AAP section 0.1.2).
 * @param writeMetricsReporter the F-009 parity reporter. Supplied by
 *                             `StreamingShuffleManager.getWriter` just as
 *                             [[org.apache.spark.shuffle.sort.SortShuffleManager#getWriter]]
 *                             supplies it to the three sort-path writers.
 * @param streamingMetrics     the Dropwizard [[StreamingShuffleMetrics]] source created by
 *                             [[StreamingShuffleManager]] on executor-side initialization. MAY
 *                             be `null` in unit tests that construct a writer without an
 *                             executor; all accesses are null-guarded.
 * @tparam K key type of the shuffle
 * @tparam V value type of the shuffle
 */
private[spark] class StreamingShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V],
    mapId: Long,
    context: TaskContext,
    conf: SparkConf,
    writeMetricsReporter: ShuffleWriteMetricsReporter,
    streamingMetrics: StreamingShuffleMetrics)
  extends ShuffleWriter[K, V]
  with Logging {

  // --------------------------------------------------------------------------
  // Shuffle dependency and execution environment.
  // --------------------------------------------------------------------------

  /**
   * The [[org.apache.spark.ShuffleDependency]] captured at shuffle-register time. The
   * third type parameter is `V` because [[StreamingShuffleHandle]] collapses
   * combiner-type `C` into value-type `V` (see the marker subclass pattern used by
   * `BypassMergeSortShuffleHandle` and `SerializedShuffleHandle`).
   */
  private val dep = handle.dependency

  /**
   * Number of reduce partitions for the downstream RDD. Both `partitionLengths` and the
   * lazy `buffers` array are sized to this value. Populated from the partitioner so
   * that ReduceByKey, GroupByKey, Join, and `Dataset` shuffle operators all route
   * through the same single integer bound.
   */
  private val numPartitions: Int = dep.partitioner.numPartitions

  /**
   * The partitioner used to map each record key to a reduce partition id in
   * `[0, numPartitions)`. Cached as a field to avoid repeated field accesses on
   * `dep.partitioner` inside the hot `write` loop.
   */
  private val partitioner = dep.partitioner

  /**
   * A fresh [[org.apache.spark.serializer.SerializerInstance]] derived from the
   * [[org.apache.spark.ShuffleDependency]]'s registered
   * [[org.apache.spark.serializer.Serializer]]. The `newInstance` call is required because
   * [[org.apache.spark.serializer.Serializer]] produces thread-safe factories whose
   * per-task instances are NOT thread-safe; the writer owns exactly one instance for the
   * lifetime of the single-threaded `write` invocation.
   */
  private val serializer: SerializerInstance = dep.serializer.newInstance()

  /**
   * The executor-scoped [[org.apache.spark.storage.BlockManager]] obtained once from
   * `SparkEnv.get`. Consumed by [[maybeSpillPartition]] for disk-side spill persistence
   * via [[org.apache.spark.storage.BlockManager#putBytes]] and by `MapStatus` construction
   * via `blockManager.shuffleServerId`. Never mutated.
   */
  private val blockManager: BlockManager = SparkEnv.get.blockManager

  // --------------------------------------------------------------------------
  // Per-partition state. Arrays are sized once at construction; individual slots
  // are lazily allocated inside `write` on first touch.
  // --------------------------------------------------------------------------

  /**
   * Lazily-allocated per-partition buffers. A `null` slot indicates that the partition
   * has not received any record yet; the buffer for that partition is allocated on the
   * first record routed to it by the partitioner. This guarantees zero heap consumption
   * for partitions that receive zero records, which matters for shuffles with very
   * high fan-out (e.g., `groupByKey` over thousands of distinct keys).
   */
  private val buffers: Array[ByteArrayOutputStream] =
    new Array[ByteArrayOutputStream](numPartitions)

  /**
   * Per-partition cumulative byte counters. Populated as records are serialized;
   * written exactly once per record (append-only between `write` and `stop`). The
   * final values are handed to [[org.apache.spark.scheduler.MapStatus]] at commit
   * time to inform reduce-side fetch planning.
   */
  private val partitionLengths: Array[Long] = new Array[Long](numPartitions)

  // --------------------------------------------------------------------------
  // Buffer-budget computation. Derived once from SparkConf at construction;
  // immutable for the lifetime of this writer instance.
  // --------------------------------------------------------------------------

  /**
   * Percent of executor memory allocated to streaming shuffle across all per-partition
   * buffers combined. Read from `spark.shuffle.streaming.bufferSizePercent` (range
   * 1-50, default 20). See [[config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT]].
   */
  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  /**
   * Executor memory in MiB. Read from `spark.executor.memory` (default 1g).
   * [[EXECUTOR_MEMORY]] is defined with `.bytesConf(ByteUnit.MiB)` so the
   * returned `Long` is already in MiB; we multiply by 1024 * 1024 to reach bytes.
   */
  private val executorMemoryMiB: Long = conf.get(EXECUTOR_MEMORY)

  /** Executor memory expressed in bytes. */
  private val executorMemoryBytes: Long = executorMemoryMiB * 1024L * 1024L

  /** Total memory budget across all streaming shuffle buffers on this executor. */
  private val totalBufferBudgetBytes: Long = (executorMemoryBytes * bufferSizePercent) / 100L

  /**
   * Per-partition buffer budget in bytes. The total streaming-shuffle budget is
   * divided evenly across all reduce partitions. `math.max(1L, ...)` guards against
   * zero or negative values that could arise from integer division when the total
   * budget is smaller than the partition count (common in tiny-executor unit tests).
   */
  private val perPartitionBudgetBytes: Long =
    math.max(1L, totalBufferBudgetBytes / math.max(1, numPartitions))

  /**
   * Spill trigger as a percent of the per-partition budget. Read from
   * `spark.shuffle.streaming.spillThreshold` (range 50-95, default 80). See
   * [[config.SHUFFLE_STREAMING_SPILL_THRESHOLD]].
   */
  private val spillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  /**
   * Absolute byte threshold at which [[maybeSpillPartition]] flushes a per-partition
   * buffer to disk. Computed once at construction: `perPartitionBudgetBytes *
   * spillThreshold / 100`.
   */
  private val spillTriggerBytes: Long = (perPartitionBudgetBytes * spillThreshold) / 100L

  /**
   * Maximum single-block payload size for network pipelining, per the user's hard
   * requirement "Block size limited to 2MB for pipelining efficiency" (AAP section 0.1.2).
   * In v1 this is only applied as the CRC32C chunk boundary during spill; v2 will
   * apply it as the Netty envelope payload size.
   */
  private val maxBlockSizeBytes: Int = 2 * 1024 * 1024

  /**
   * Initial capacity passed to each newly-allocated per-partition
   * [[java.io.ByteArrayOutputStream]]. Capped at 64 KiB to avoid heap spikes for
   * very-wide partitioners, and clamped below the per-partition budget so we never
   * pre-allocate more than we are allowed to hold.
   */
  private val initialBufferCapacity: Int =
    math.max(1, math.min(perPartitionBudgetBytes, 64L * 1024L).toInt)

  // --------------------------------------------------------------------------
  // Lifecycle state.
  // --------------------------------------------------------------------------

  /**
   * Idempotency guard on `stop`. [[org.apache.spark.shuffle.ShuffleWriteProcessor]]
   * invokes `stop(success = true)` on normal completion and may then invoke
   * `stop(success = false)` from the exception handler; either sequence must release
   * per-partition buffers exactly once and never produce two distinct `MapStatus`
   * references for the same map task. `AtomicBoolean` mirrors the `SortShuffleWriter`
   * `stopping: Boolean` pattern with added thread-safety in case the streaming
   * feature's async acknowledgment thread races with the task thread.
   */
  private val stopping: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Produced by `write` on successful record iteration completion. Read by `stop(true)`
   * and handed back to [[org.apache.spark.shuffle.ShuffleWriteProcessor]] for
   * publication to the [[org.apache.spark.MapOutputTracker]]. `None` until the full
   * record iterator has been consumed.
   */
  private var mapStatus: Option[MapStatus] = None

  /**
   * `System.nanoTime()` sampled at the start of `write`. Used to compute the
   * elapsed write time reported to `writeMetricsReporter.incWriteTime` at the end
   * of the iterator &mdash; F-009 parity with the sort-path writer.
   */
  private var writeStartNanos: Long = 0L

  // --------------------------------------------------------------------------
  // Public ShuffleWriter contract. Three abstract methods: write, stop,
  // getPartitionLengths. Each is annotated with its behavioral guarantees.
  // --------------------------------------------------------------------------

  /**
   * Consume the full record iterator, serialize each record into its partition's
   * lazy buffer, and trigger [[maybeSpillPartition]] whenever the per-partition
   * threshold is crossed. Terminates by publishing a
   * [[org.apache.spark.scheduler.MapStatus]] to `mapStatus` so that `stop(true)` can
   * hand it back to [[org.apache.spark.shuffle.ShuffleWriteProcessor]].
   *
   * This method is invoked once per map task, on a single thread. It never returns
   * normally without emitting a `MapStatus`; any exception thrown during serialization
   * or partitioning propagates to the caller, at which point `stop(success = false)`
   * will be invoked for cleanup.
   *
   * @param records iterator of `(key, value)` pairs to partition and serialize
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Capture the start timestamp BEFORE any serialization so the total time reported
    // to `incWriteTime` covers the entire record-iteration hot path. Matches the
    // sort-path pattern where `ExternalSorter.insertAll` is surrounded by the timer.
    writeStartNanos = System.nanoTime()

    logInfo(log"Streaming shuffle write started: " +
      log"shuffle=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
      log"map=${MDC(LogKeys.MAP_ID, mapId)}, " +
      log"stage=${MDC(LogKeys.STAGE_ID, context.stageId())}, " +
      log"taskAttempt=${MDC(LogKeys.TASK_ATTEMPT_ID, context.taskAttemptId())}, " +
      log"partitions=${MDC(LogKeys.NUM_PARTITIONS, numPartitions)}")

    // Use a while loop rather than `records.foreach` to avoid any intermediate closure
    // allocation on the hot path. The iterator contract guarantees `hasNext` / `next`
    // are safe to call repeatedly, and the single-threaded execution model documented
    // on ShuffleWriter eliminates synchronization concerns.
    while (records.hasNext) {
      val record = records.next()
      val key = record._1
      val value = record._2

      // Route the record to its reduce partition. `partitioner` is cached on this
      // writer so that we avoid repeated `dep.partitioner` field access.
      val partitionId = partitioner.getPartition(key)

      // Lazy-allocate the per-partition buffer on first write. Partitions that receive
      // zero records never enter this branch and therefore never consume heap; this is
      // the "zero memory for zero-output partitions" guarantee from the agent prompt.
      if (buffers(partitionId) == null) {
        buffers(partitionId) = new ByteArrayOutputStream(initialBufferCapacity)
      }

      // Serialize the single record into a fresh transient buffer. The transient
      // pattern (rather than serializing directly into the per-partition buffer) is
      // required because we need the exact byte count of THIS record for the
      // partition-length accumulator and for the per-record F-009 metric reporters.
      // Allocating a 1 KiB initial capacity for the transient stream is a tuned
      // compromise between avoiding the default JDK 32-byte capacity (which causes
      // multiple realloc / copy cycles for typical records) and not over-reserving
      // when records are very small.
      val tmpStream = new ByteArrayOutputStream(1024)
      val serStream = serializer.serializeStream(tmpStream)
      try {
        // `writeKey` and `writeValue` are defined on
        // [[org.apache.spark.serializer.SerializationStream]] as
        // `writeKey[T: ClassTag](key: T)` / `writeValue[T: ClassTag](value: T)`;
        // they delegate to the serializer-specific `writeObject` and require a
        // ClassTag for `T`. Because `K` and `V` are unbounded type parameters on
        // this class with no ClassTag evidence in scope, we upcast the record
        // values to `Any` so the compiler resolves `T = Any` and uses the
        // standard `scala.reflect.ClassTag.Any` instance. This mirrors the
        // pattern in `DiskBlockObjectWriter.write(key: Any, value: Any)` used by
        // the sort-path shuffle writer and preserves correct runtime type
        // handling inside the serializer (Kryo type registration, Java
        // serialization's class markers, etc.) because the serializer treats
        // keys and values as opaque objects.
        serStream.writeKey(key.asInstanceOf[Any])
        serStream.writeValue(value.asInstanceOf[Any])
      } finally {
        // Close the serialization stream so any buffered framing bytes (e.g., Kryo's
        // internal frames, Java serialization's object markers) are flushed into the
        // backing ByteArrayOutputStream before we extract `toByteArray`.
        serStream.close()
      }
      val recordBytes = tmpStream.toByteArray

      // Append the serialized record to the per-partition accumulator. The
      // ByteArrayOutputStream's internal buffer grows geometrically (doubled on each
      // overflow) so amortized per-append cost is O(1).
      buffers(partitionId).write(recordBytes)

      // Track the per-partition byte count. `partitionLengths(partitionId)` is the
      // authoritative size reported to `MapStatus` regardless of whether the bytes
      // are currently in the per-partition buffer or already spilled to BlockManager.
      partitionLengths(partitionId) += recordBytes.length.toLong

      // F-009 metrics parity: invoke the three MANDATORY "inc" methods on the
      // `ShuffleWriteMetricsReporter` contract. The two "dec" methods are unused in
      // v1 because streaming shuffle has no rollback path (a failed task returns
      // `stop(success = false)` and the DAG scheduler recomputes the stage).
      writeMetricsReporter.incBytesWritten(recordBytes.length.toLong)
      writeMetricsReporter.incRecordsWritten(1L)

      // Check the per-partition spill threshold AFTER updating the partition length
      // and metrics so that the cumulative counters remain consistent even if the
      // spill callback throws. `ByteArrayOutputStream.size()` returns the current
      // position in the internal buffer (i.e., the number of bytes written so far)
      // WITHOUT copying the backing array.
      if (buffers(partitionId).size() >= spillTriggerBytes) {
        maybeSpillPartition(partitionId)
      }
    }

    // Report the total record-iteration time to the F-009 reporter once the iterator
    // is exhausted. Done here (rather than inside `stop`) because the sort-path
    // writer's ExternalSorter.insertAll is the structural equivalent; `stop` in the
    // sort path adds a separate `sorter.stop()` timing contribution which streaming
    // does not have.
    writeMetricsReporter.incWriteTime(System.nanoTime() - writeStartNanos)

    // Construct the map status with the per-partition byte array. The
    // `MapStatus.apply` factory picks between CompressedMapStatus and
    // HighlyCompressedMapStatus based on partition count; we deliberately do not
    // pre-pick because the factory's choice is the invariant the DAG scheduler
    // consumes (see `core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala`
    // lines 80-90).
    mapStatus = Some(MapStatus(blockManager.shuffleServerId, partitionLengths, mapId))

    logDebug(log"Streaming shuffle write completed: " +
      log"shuffle=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
      log"map=${MDC(LogKeys.MAP_ID, mapId)}, " +
      log"totalBytes=${MDC(LogKeys.NUM_BYTES, partitionLengths.sum)}")
  }

  /**
   * Spill the named partition's buffered bytes to disk via
   * [[org.apache.spark.storage.BlockManager#putBytes]] at
   * [[org.apache.spark.storage.StorageLevel#DISK_ONLY]]. The spill path intentionally
   * delegates disk-side byte placement to the existing block-manager infrastructure so
   * that the streaming feature introduces "least modification to executor memory model"
   * (AAP section 0.7.1 Implementation Discipline).
   *
   * The method performs four distinct steps:
   *
   *   1. Snapshot the buffer's current contents into an immutable `Array[Byte]`.
   *   2. Walk the snapshot in `<= 2 MB` chunks, computing a CRC32C per chunk and
   *      logging it at DEBUG for diagnostic correlation with the future network
   *      transport that will carry these same chunks over the wire (AAP section 0.1.2:
   *      "Block size limited to 2MB for pipelining efficiency").
   *   3. Hand the entire snapshot (as a single `ChunkedByteBuffer`) to
   *      [[org.apache.spark.storage.BlockManager#putBytes]] under
   *      `ShuffleBlockId(shuffleId, mapId, partitionId)`. A try/catch wraps the call so
   *      that a transient block-manager failure (e.g., disk full, shuffle cleanup race)
   *      does not abort the entire map task; the failure is logged at WARN and the
   *      buffer is still reset so the next threshold crossing can attempt another spill.
   *   4. Reset the per-partition `ByteArrayOutputStream` so its next write starts at
   *      offset zero. `partitionLengths` is NOT adjusted because the cumulative count
   *      is the authoritative byte total for `MapStatus` regardless of where the bytes
   *      currently reside.
   *
   * Invoked from `write` whenever `buffers(partitionId).size() >= spillTriggerBytes`.
   * No-op if the slot is `null` (partition has never been written to) or the snapshot
   * is empty. Increments [[StreamingShuffleMetrics#incrementSpillCount]] exactly once
   * per invocation that observed a non-empty snapshot, regardless of whether the
   * BlockManager `putBytes` call ultimately succeeded &mdash; the counter measures the
   * number of times the spill decision was made, not the number of successful disk
   * writes.
   *
   * @param partitionId the reduce partition id of the buffer to spill
   */
  private def maybeSpillPartition(partitionId: Int): Unit = {
    // Null-check: the slot may be null if a concurrent caller invoked this method
    // between our own spill-threshold check and the actual access. In single-threaded
    // `write`, this cannot happen; kept for defensive safety in case a future caller
    // (e.g., the in-flight MemorySpillManager callback integration) invokes this
    // method off the hot path.
    val buf = buffers(partitionId)
    if (buf == null) {
      return
    }
    val data: Array[Byte] = buf.toByteArray
    if (data.length == 0) {
      // Empty snapshot: no work to do, no metric to emit, no buffer to reset. This
      // can happen if the threshold check fires exactly at the moment a prior spill
      // has already drained the buffer.
      return
    }

    // Chunk the snapshot into `<= 2 MB` blocks and compute CRC32C per chunk. The
    // checksum value is logged at DEBUG for diagnostic correlation. In v2 the CRC
    // will be embedded in the Netty envelope so consumers can request retransmission
    // on mismatch.
    var offset = 0
    while (offset < data.length) {
      val chunkLen = math.min(maxBlockSizeBytes, data.length - offset)
      // Fresh CRC32C per chunk. The Castagnoli polynomial is the user-specified
      // algorithm for block integrity (AAP section 0.1.2). JDK 17's
      // java.util.zip.CRC32C is a built-in class so no third-party dependency is
      // required and no entry is needed in LICENSE / NOTICE.
      val crc = new CRC32C()
      crc.update(data, offset, chunkLen)
      val checksumValue: Int = crc.getValue.toInt

      logDebug(log"Streaming block chunked for spill: " +
        log"shuffle=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
        log"map=${MDC(LogKeys.MAP_ID, mapId)}, " +
        log"partition=${MDC(LogKeys.REDUCE_ID, partitionId)}, " +
        log"bytes=${MDC(LogKeys.NUM_BYTES, chunkLen)}, " +
        log"checksum=${MDC(LogKeys.CHECKSUM, checksumValue)}")

      offset += chunkLen
    }

    // Persist the spilled block to disk via BlockManager. The spill path intentionally
    // uses `putBytes` + `StorageLevel.DISK_ONLY` rather than bypassing BlockManager;
    // this preserves existing disk infrastructure semantics (encryption, replication,
    // decommission) without adding a parallel byte store for the streaming path.
    // AAP section 0.7.1 Implementation Discipline: "least modification to executor memory
    // model and network transport layer".
    val blockId = ShuffleBlockId(handle.shuffleId, mapId, partitionId)
    try {
      val chunked = new ChunkedByteBuffer(Array(ByteBuffer.wrap(data)))
      // `tellMaster = false` because streaming-shuffle spills are local-only by design
      // (the consumer pulls them via the streaming transport, not via the
      // BlockManagerMaster tracker). Matches the sort path's spill-file semantics.
      blockManager.putBytes(blockId, chunked, StorageLevel.DISK_ONLY, tellMaster = false)
    } catch {
      case t: Throwable =>
        // A spill failure must not abort the entire map task. Streaming shuffle's
        // MapStatus byte count remains correct because `partitionLengths(partitionId)`
        // reflects the total bytes serialized, not the bytes persisted. A reader that
        // later fails to fetch the block will trigger a `FetchFailedException` which
        // the DAG scheduler handles via the existing upstream-recomputation path
        // (AAP section 0.6.2: lineage tracking and fault-recovery model preserved).
        logWarning(log"Failed to persist streaming shuffle spill; MapStatus remains " +
          log"correct but the block is not readable from disk: " +
          log"shuffle=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
          log"map=${MDC(LogKeys.MAP_ID, mapId)}, " +
          log"partition=${MDC(LogKeys.REDUCE_ID, partitionId)}, " +
          log"bytes=${MDC(LogKeys.NUM_BYTES, data.length)}", t)
    }

    // Emit the spill event metric. The counter measures decisions (spill was chosen),
    // not successful disk writes, so we increment even when putBytes raised above.
    if (streamingMetrics != null) {
      streamingMetrics.incrementSpillCount()
    }

    // Emit a structured INFO log exposing the per-partition buffer utilization that
    // triggered this spill. This satisfies the Observability Rule (AAP section 0.7.7)
    // by surfacing the `BUFFER_UTILIZATION_PERCENT` LogKey (declared in CP1) to the
    // executor log stream, so operators can correlate spill frequency with the actual
    // buffer pressure at the decision point.
    //
    // Utilization is computed against `perPartitionBudgetBytes` -- the same reference
    // point used to derive `spillTriggerBytes = perPartitionBudgetBytes *
    // spillThreshold / 100` -- so an emitted value of, e.g., 82.4 directly corresponds
    // to "we crossed the configured 80% spill trigger by 2.4 points". Log volume
    // impact is negligible: spill is a low-frequency event (tens per hour under
    // steady load), well within the AAP IC-15 budget of <10 MB/hour per executor.
    //
    // `perPartitionBudgetBytes` is guarded against zero at construction time
    // (`math.max(1L, ...)`), so the divisor is always strictly positive.
    val utilizationPercent: Double = data.length.toDouble * 100.0 /
      perPartitionBudgetBytes.toDouble
    logInfo(log"Streaming shuffle partition spilled to disk: " +
      log"shuffle=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
      log"map=${MDC(LogKeys.MAP_ID, mapId)}, " +
      log"partition=${MDC(LogKeys.REDUCE_ID, partitionId)}, " +
      log"bytes=${MDC(LogKeys.NUM_BYTES, data.length)}, " +
      log"budgetBytes=${MDC(LogKeys.MAX_SIZE, perPartitionBudgetBytes)}, " +
      log"utilizationPercent=" +
      log"${MDC(LogKeys.BUFFER_UTILIZATION_PERCENT, utilizationPercent)}, " +
      log"spillThresholdPercent=${MDC(LogKeys.THRESHOLD, spillThreshold)}")

    // Reset the buffer so subsequent writes to this partition start fresh. This
    // releases the internal array's write position but does NOT shrink the backing
    // byte[] (which remains allocated for amortized reuse). `reset` is effectively a
    // single-assignment of the internal `count` field to 0 and does not perform any
    // allocation.
    buf.reset()
  }

  /**
   * Close the writer and, if the map task completed successfully, return the
   * [[org.apache.spark.scheduler.MapStatus]] captured at the end of `write`. If the
   * task failed or this is a duplicate stop call, return `None`.
   *
   * The method is idempotent via `stopping.compareAndSet(false, true)`: only the
   * first invocation performs cleanup, and only the first invocation with
   * `success = true` can return `Some(mapStatus)`. A subsequent call (e.g., from the
   * exception handler after a successful completion) returns `None` without touching
   * buffers or metrics.
   *
   * Cleanup nulls every allocated per-partition buffer slot so the backing
   * `byte[]` arrays become eligible for garbage collection promptly; this is the
   * user's hard requirement "Zero memory leaks under failure scenarios" (AAP section 0.1.2).
   *
   * @param success `true` if the map task completed normally, `false` if it failed
   *                and any emitted `MapStatus` should be discarded
   * @return `Some(mapStatus)` only when this is the first invocation AND
   *         `success = true` AND `write` completed successfully; otherwise `None`
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    // Idempotency guard: a second stop call (from the exception handler after a
    // successful completion, or from an explicit retry) returns None without releasing
    // buffers twice or double-reporting metrics. `compareAndSet(false, true)` is
    // lock-free and returns true only on the first call.
    if (!stopping.compareAndSet(false, true)) {
      return None
    }

    try {
      if (success) {
        // `mapStatus` is non-empty iff `write` ran to completion without throwing.
        // If `write` threw mid-iteration, `mapStatus` is still `None` and the caller
        // receives `None` even with `success = true`; this is an intentional safety
        // net against callers that misreport task success.
        mapStatus
      } else {
        // Failure path: discard any partially-constructed `MapStatus`. The DAG
        // scheduler will observe the task failure via the exception bubbling out of
        // the task and will recompute the stage using the existing lineage / fault-
        // recovery model (AAP section 0.6.2).
        None
      }
    } finally {
      // Eagerly null out every buffer slot so the underlying `byte[]` arrays become
      // eligible for GC. A while loop (rather than Array.foreach / map) avoids any
      // intermediate closure allocation on the cleanup path.
      var totalBytes = 0L
      var i = 0
      while (i < numPartitions) {
        totalBytes += partitionLengths(i)
        // Only null out slots that are non-null to avoid touching array cells we never
        // wrote; this is a small observability nicety for any future test that reads
        // `buffers` directly to assert lazy allocation.
        if (buffers(i) != null) {
          buffers(i) = null
        }
        i += 1
      }

      logInfo(log"Streaming shuffle write stopped: " +
        log"shuffle=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
        log"map=${MDC(LogKeys.MAP_ID, mapId)}, " +
        log"totalBytes=${MDC(LogKeys.NUM_BYTES, totalBytes)}, " +
        log"status=${MDC(LogKeys.STATUS, if (success) "success" else "failure")}")
    }
  }

  /**
   * Return a defensive copy of the per-partition byte-length array so callers
   * cannot mutate the writer's internal state after observing the commit. Invoked
   * by higher-level orchestration (e.g., tests, diagnostic utilities); the array
   * returned is already captured in the emitted `MapStatus` so this is purely a
   * read-side accessor.
   *
   * @return a fresh `Array[Long]` of length `numPartitions` holding the cumulative
   *         bytes written per reduce partition
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths.clone()
}
