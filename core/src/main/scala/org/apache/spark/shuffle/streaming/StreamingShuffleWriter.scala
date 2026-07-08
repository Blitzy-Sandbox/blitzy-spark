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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, UnifiedMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}

/**
 * The streaming map-side shuffle writer for the streaming shuffle backend
 * (`spark.shuffle.manager=streaming`). It implements the [[ShuffleWriter]] SPI and composes an
 * inner [[MemoryConsumer]] so that the in-memory streaming buffers it allocates are tracked and
 * spilled through Spark's existing task memory manager -- no change is made to the executor memory
 * model. Both `ShuffleWriter` and `MemoryConsumer` are abstract classes and Scala permits extending
 * only one, so the memory consumer is held by '''composition''' -- exactly as `SortShuffleWriter`
 * tracks memory through its `ExternalSorter` / `Spillable` collaborator rather than by inheritance.
 * This keeps the SPI return type `ShuffleWriter[K, V]` intact (the type `getWriter` must return)
 * while still registering the streaming buffers with the existing `TaskMemoryManager`.
 *
 * ==Where this fits==
 * `StreamingShuffleManager.getWriter` constructs one instance of this class per map task whenever a
 * shuffle was registered with a [[StreamingShuffleHandle]] (i.e. streaming is active and no
 * fallback condition applies). Every non-streaming handle, and every fallback case, is delegated by
 * the manager to the inner `SortShuffleManager` instead, so this writer never touches the
 * production-stable sort code path and vice versa. All streaming write logic is contained here and
 * in its collaborators inside the `org.apache.spark.shuffle.streaming` package, honoring the
 * feature's "zero cross-contamination" isolation discipline.
 *
 * ==v1 status: off the production path==
 * In v1 the wire transport is a logging-only stub that advertises no wire-transfer capability
 * ([[StreamingShuffleTransport.isWireTransferAvailable]] is `false`), so the manager's
 * `canUseStreaming` gate forces every shuffle to sort fallback and no map task is ever issued a
 * streaming handle in production. This writer is therefore v2 groundwork exercised by its unit
 * suite rather than a production code path. Its durability contract is the '''persist/spill
 * channel''' below (records buffered on heap, bounded and spilled to disk); the wire channel is
 * disabled while the transport reports no capability, so it can neither put bytes on the wire nor
 * discard buffered bytes after a no-op send. This keeps the writer internally honest: it never
 * counts or publishes bytes it did not durably retain.
 *
 * ==Dual serialization channels==
 * Every record is serialized exactly once by the shared [[SerializerInstance]] and routed through
 * two sinks that share those bytes but differ in destination:
 *
 *  - '''Persist channel''' (always active) -- serialized bytes are appended into the reduce
 *    partition's on-heap [[StreamingBuffer]]. Each buffer is registered with the
 *    [[MemorySpillManager]], which spills the largest / least-recently-used buffers to disk
 *    ([[org.apache.spark.storage.StorageLevel.DISK_ONLY]]) under memory pressure and reclaims them
 *    within 100 ms of a consumer acknowledgment. Every spill -- background poll, cooperative
 *    memory-manager spill, and per-partition cap -- flows through the single shared, atomic,
 *    tracked routine [[MemorySpillManager.spillBufferToDisk]], so buffered data is never lost and
 *    every spilled block is registered for cleanup on `unregisterShuffle`.
 *  - '''Wire channel''' (v2, disabled in v1) -- once a partition's buffer reaches the 2 MB block
 *    boundary, the bytes are framed into a [[StreamingBlockEnvelope]] (a fixed 32-byte big-endian
 *    header plus a payload of at most 2 MB, stamped with a CRC32C), gated by the
 *    [[BackpressureProtocol]], and handed to the [[StreamingShuffleTransport]]. This channel is
 *    engaged only when the transport advertises wire-transfer capability; in v1 it is skipped
 *    entirely so no buffered bytes are ever reset after a no-op send.
 *
 * ==Memory discipline==
 * The per-executor streaming buffer budget is `executionMemory x bufferSizePercent / 100` and each
 * of the `numPartitions` reduce partitions receives an equal share of it, floored at one 2 MB wire
 * block (the preserved user formula `(executorMemory * bufferPercent) / numPartitions`). The base
 * is the executor's '''execution''' memory (`UnifiedMemoryManager.maxHeapMemory`), the pool that
 * actually backs a [[MemoryConsumer]], not storage memory. That per-partition share is enforced as
 * a real cap: when a partition buffer reaches it, the partition is spilled durably to bound its
 * on-heap footprint. Execution memory reserved through the inner [[MemoryConsumer]] is continuously
 * reconciled with the live buffered footprint; `reconcileMemory` '''honors''' the value returned by
 * `acquireMemory` and, when the manager grants less than the footprint requires, spills the
 * writer's own buffers until the footprint fits the reservation, so heap never exceeds accounted
 * execution memory. When the task memory manager cannot satisfy an allocation it invokes the
 * consumer's `spill`, which forwards to this writer's public [[spill]] to persist the largest
 * buffers to disk and free their heap.
 *
 * ==Failure handling==
 * [[stop]] is idempotent: a map task may call `stop(success = true)` and then, on a later error,
 * `stop(success = false)`. The first call wins; every call releases and unregisters the partition
 * buffers exactly once so no memory or tracking state leaks under failure.
 *
 * ==Thread-safety==
 * A single task thread drives [[write]]; the [[MemorySpillManager]] daemon may read a partition
 * buffer concurrently while scanning for spill candidates, which the [[StreamingBuffer]]'s own
 * synchronization tolerates. The inner consumer's `spill` may be invoked on the task thread from
 * within an `acquireMemory` call and never calls `acquireMemory` itself (per the contract), so it
 * cannot deadlock.
 *
 * @tparam K the type of the keys being shuffled
 * @tparam V the type of the values being shuffled
 * @tparam C the type of the combined values if map-side aggregation is used (else same as V)
 * @param handle           the streaming shuffle handle carrying the per-shuffle resource envelope
 * @param mapId            the id of the map (producer) task this writer serves
 * @param context          the task context; source of the task memory manager for buffers
 * @param metricsReporter  Spark's per-task shuffle write metrics reporter
 * @param blockManager     the executor block manager (map output location and spill persistence)
 * @param transport        the streaming transport (v1 logging stub reusing `BlockTransferService`)
 * @param spillManager     the memory-pressure monitor and disk-spill coordinator
 * @param backpressure     the token-bucket + heartbeat flow-control engine
 * @param streamingMetrics the streaming-shuffle telemetry holder
 * @param conf             the typed streaming-shuffle configuration accessor
 */
@Since("4.2.0")
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    metricsReporter: ShuffleWriteMetricsReporter,
    blockManager: BlockManager,
    transport: StreamingShuffleTransport,
    spillManager: MemorySpillManager,
    backpressure: BackpressureProtocol,
    streamingMetrics: StreamingShuffleMetrics,
    conf: StreamingShuffleConfig)
  extends ShuffleWriter[K, V]
  with Logging {

  // Streaming-only MDC correlation-id keys/formatters, defined inside the streaming package to keep
  // shared LogKeys untouched (see StreamingShuffleLogKeys for the coexistence rationale).
  import StreamingShuffleLogKeys.{ATTEMPT_ID, REDUCE_PARTITION_RANGE, singlePartition}

  // -- Derived shuffle metadata ------------------------------------------------------------------

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = dep.shuffleId

  // Contract guard (finding M4): the streaming writer has no map-side aggregation path -- it
  // serializes and buffers raw (key, value) records and never applies dep.aggregator. Map-side
  // combine must therefore be routed to the sort path. `StreamingShuffleManager` already excludes
  // map-side-combine dependencies from streaming eligibility, so a streaming handle is never made
  // for one; this require makes that invariant explicit and fail-fast, so the writer never silently
  // drops the combine step and produces semantically wrong (uncombined) output.
  require(!dep.mapSideCombine,
    s"StreamingShuffleWriter does not support map-side combine (shuffleId=$shuffleId); the " +
      "StreamingShuffleManager must route map-side-combine shuffles to the sort path")

  // Single serializer instance shared by both serialization channels (wire + persist). Records are
  // serialized as self-contained units so a wire block may be sliced at any 2 MB byte boundary.
  private val serInstance: SerializerInstance = dep.serializer.newInstance()

  // Cached streaming debug flag (spark.shuffle.streaming.debug); gates verbose per-block logging.
  private val debugEnabled: Boolean = conf.debug

  // -- Buffer sizing (memory discipline, AAP 0.7.1) ----------------------------------------------

  // Executor on-heap memory used as the buffer-budget base (finding M2). The streaming buffers are
  // accounted against the inner MemoryConsumer, which draws from the executor's EXECUTION memory
  // pool, so the budget base must be that pool's size -- `UnifiedMemoryManager.maxHeapMemory`, the
  // total on-heap pool shared by execution and storage -- and NOT `maxOnHeapStorageMemory`, which
  // reports only the storage half and would systematically undersize the buffers relative to the
  // memory they actually consume. Gated on SparkEnv for local/test-mode safety: when no memory
  // manager is initialized (e.g. a unit test constructing the writer directly) a nominal default is
  // used so sizing stays well-defined and the 2 MB per-partition floor still applies. A non-unified
  // MemoryManager (none ships with Spark today) falls back to its storage-memory accessor.
  private val executorMemoryBytes: Long = {
    val env = SparkEnv.get
    if (env != null && env.memoryManager != null) {
      env.memoryManager match {
        case unified: UnifiedMemoryManager => unified.maxHeapMemory
        case other => other.maxOnHeapStorageMemory
      }
    } else {
      StreamingShuffleWriter.DEFAULT_EXECUTOR_MEMORY_BYTES
    }
  }

  // Total per-executor streaming buffer budget: executorMemory x bufferSizePercent / 100. This is
  // the same denominator the MemorySpillManager uses for its 80% utilization spill trigger.
  private val totalBufferBudgetBytes: Long =
    math.max(0L, executorMemoryBytes * handle.bufferSizePercent / 100L)

  // Per-partition share of the budget with a 2 MB floor: (executionMemory x bufferPercent / 100) /
  // numPartitions, floored at one 2 MB wire block so a partition can always stage a full block.
  // This is enforced as a REAL cap (finding M2): `write` spills a partition durably as soon as its
  // buffer reaches this size, bounding each partition's on-heap footprint rather than merely sizing
  // the initial allocation.
  private val perPartitionBudgetBytes: Long =
    math.max(totalBufferBudgetBytes / math.max(1, numPartitions),
      StreamingShuffleWriter.MIN_PARTITION_BUFFER_BYTES)

  /**
   * The enforced per-partition on-heap buffer capacity, in bytes. Exposed so tests and operators
   * can verify the memory-discipline contract (finding M2): `(executionMemory x bufferSizePercent /
   * 100) / numPartitions`, floored at one 2 MB block. When a partition buffer reaches this size the
   * writer spills it durably, so no single partition can grow the writer's footprint without bound.
   *
   * @return the per-partition buffer capacity in bytes that the writer enforces via spilling
   */
  def perPartitionBufferCapacityBytes: Long = perPartitionBudgetBytes

  // Initial backing-store capacity for a partition buffer: the per-partition budget, but never more
  // than the default hint so high-fan-out shuffles do not over-allocate upfront (buffers grow on
  // demand and the collective cap is enforced by the spill manager against the total budget).
  private val initialBufferCapacity: Int =
    math.min(perPartitionBudgetBytes, StreamingBuffer.DEFAULT_INITIAL_CAPACITY.toLong).toInt

  // Publish the total budget so the spill manager's utilization/threshold logic and this writer
  // size against one consistent denominator.
  spillManager.setBufferBudgetBytes(totalBufferBudgetBytes)

  // -- Writer state ------------------------------------------------------------------------------

  // PERSIST channel: one lazily-allocated on-heap buffer per reduce partition (see bufferFor).
  private val buffers = new Array[StreamingBuffer](numPartitions)

  // Serialized bytes routed to each reduce partition; surfaced via MapStatus.getSizeForBlock.
  private val partitionLengths = new Array[Long](numPartitions)

  // Monotonic per-partition block sequence number stamped into each wire envelope for
  // ordering/deduplication on the read side.
  private val sequenceNumbers = new Array[Int](numPartitions)

  // Reusable scratch sink for per-record serialization (see serializeRecord).
  private val serializationScratch =
    new ByteArrayOutputStream(StreamingShuffleWriter.RECORD_SCRATCH_INITIAL_BYTES)

  // stop() is idempotent: map tasks may call stop(true) then stop(false) on a later error.
  private var stopping = false

  // Guards releaseAllResources() so buffers are freed and unregistered exactly once.
  private var released = false

  // The MapStatus produced by a successful write(); returned by a successful stop().
  private var mapStatus: MapStatus = _

  // -- Memory tracking (composition, not inheritance) --------------------------------------------

  // `ShuffleWriter` and `MemoryConsumer` are both abstract classes and Scala permits extending only
  // one; the SPI requires this writer to be a `ShuffleWriter[K, V]` (the type `getWriter` returns),
  // so the executor memory manager is engaged by holding an inner MemoryConsumer via composition --
  // the same shape `SortShuffleWriter` uses through its `ExternalSorter` / `Spillable`. Registering
  // this consumer with the existing `TaskMemoryManager` is what accounts the streaming buffers
  // against execution memory with no memory-model redesign: `reconcileMemory` acquires and frees
  // against it as buffers grow and drain, and under pressure the manager drives its `spill`
  // callback below. Because the writer cannot itself extend `MemoryConsumer`, the abstract
  // `spill(size, trigger)` contract is instead exposed on the writer as a public method (finding
  // M1) and the inner consumer simply forwards to it -- so the same spill contract the memory
  // manager invokes is directly visible and unit-testable on this writer.
  private val memoryConsumer: MemoryConsumer =
    new MemoryConsumer(
        context.taskMemoryManager(),
        context.taskMemoryManager().pageSizeBytes(),
        MemoryMode.ON_HEAP) {
      @throws(classOf[IOException])
      override def spill(size: Long, trigger: MemoryConsumer): Long =
        StreamingShuffleWriter.this.spill(size, trigger)
    }

  // Structured init log with the streaming-shuffle correlation identifiers (finding M8): shuffle,
  // map, and task-attempt ids are emitted through MDC so log aggregation can correlate every
  // streaming event of this map task, alongside the resolved buffer-budget sizing.
  logInfo(log"Initialized streaming shuffle writer " +
    log"(shuffleId=${MDC(LogKeys.SHUFFLE_ID, shuffleId)}, " +
    log"mapId=${MDC(LogKeys.MAP_ID, mapId)}, " +
    log"attemptId=${MDC(ATTEMPT_ID, context.taskAttemptId())}, " +
    log"numPartitions=${MDC(LogKeys.NUM_PARTITIONS, numPartitions)}, " +
    log"bufferSizePercent=${MDC(LogKeys.PERCENT, handle.bufferSizePercent)}, " +
    log"totalBufferBudgetBytes=${MDC(LogKeys.NUM_BYTES, totalBufferBudgetBytes)}, " +
    log"perPartitionCapacityBytes=${MDC(LogKeys.MEMORY_SIZE, perPartitionBudgetBytes)})")

  // -- ShuffleWriter SPI -------------------------------------------------------------------------

  /**
   * Stream this map task's records to their reduce partitions. Each record is serialized once, has
   * its bytes appended to the destination partition's [[StreamingBuffer]] (persist channel) and,
   * once a buffer crosses the 2 MB block boundary, framed into a [[StreamingBlockEnvelope]] and
   * dispatched through the transport (wire channel). On completion a [[MapStatus]] is published for
   * the reduce side. Mirrors the accounting shape of `SortShuffleWriter.write`.
   *
   * @param records the map task's output records
   */
  @throws(classOf[IOException])
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val writeStartNanos = System.nanoTime()
    while (records.hasNext) {
      val record = records.next()
      val key = record._1
      val partitionId = partitioner.getPartition(key)
      val recordBytes = serializeRecord(key, record._2)
      val buffer = bufferFor(partitionId)
      buffer.append(recordBytes)
      partitionLengths(partitionId) += recordBytes.length.toLong
      metricsReporter.incRecordsWritten(1L)
      // Account serialized output bytes as they are produced. In v1 the wire channel puts nothing
      // on the wire (the transport is a stub and the manager forces sort fallback), so
      // bytes-written is counted here at serialization rather than at wire send -- keeping the
      // metric honest and identical across v1 and v2 (it reflects bytes actually serialized and
      // buffered, never bytes claimed by a no-op send).
      metricsReporter.incBytesWritten(recordBytes.length.toLong)
      // Keep the reserved execution memory aligned with the buffer's new footprint; reconcileMemory
      // honors the acquireMemory grant and spills any shortfall under pressure (finding M3).
      reconcileMemory()
      // Enforce the per-partition on-heap cap (finding M2): once a partition reaches its capacity,
      // spill it durably so no single partition grows the writer's footprint without bound.
      enforcePartitionCap(partitionId, buffer)
      // Dispatch any full 2 MB wire blocks the append completed (no-op in v1: the wire channel is
      // gated on the transport advertising wire-transfer capability).
      maybeFlushWireBlocks(partitionId, buffer)
    }
    // Emit each partition's trailing sub-block to complete its stream (no-op in v1: gated on the
    // transport's wire-transfer capability).
    flushResidualBlocks()
    metricsReporter.incWriteTime(System.nanoTime() - writeStartNanos)
    // Publish the map output location and per-partition sizes. The 3-arg MapStatus.apply defaults
    // the checksum to 0: streaming integrity is per-block CRC32C carried in the envelope, not here.
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  /**
   * Spill the largest partition buffers to disk to release at least `size` bytes of execution
   * memory, returning the bytes actually freed. This is the writer's public `MemoryConsumer`-style
   * spill contract (finding M1): the writer cannot extend `MemoryConsumer` (Scala permits extending
   * only one class and the SPI requires it to be a `ShuffleWriter[K, V]`), so the contract is
   * exposed here and the inner consumer forwards to it -- making the exact behavior the task memory
   * manager invokes under pressure directly visible and unit-testable on this writer. It is the
   * synchronous, pressure-driven complement to the [[MemorySpillManager]]'s periodic 80% poll.
   *
   * Buffers are ordered largest-first so the fewest spills reclaim the most, and every spill flows
   * through the single shared, atomic, tracked routine [[MemorySpillManager.spillBufferToDisk]]:
   * buffered data is never lost (a buffer is reset only on a confirmed durable store) and each
   * spilled block is registered for cleanup on `unregisterShuffle` (findings M16 and M17). Per the
   * [[MemoryConsumer]] contract this never calls `acquireMemory`, so it cannot recurse into memory
   * acquisition or deadlock.
   *
   * @param size    the number of bytes the memory manager is asking the consumer to release
   * @param trigger the consumer that triggered the spill (unused: this writer always spills its own
   *                buffers); present to satisfy the `MemoryConsumer.spill` signature
   * @return the number of bytes actually freed
   */
  @throws(classOf[IOException])
  def spill(size: Long, trigger: MemoryConsumer): Long = {
    var freed = 0L
    val candidates = collectSpillCandidates()
    val iterator = candidates.iterator
    while (iterator.hasNext && freed < size) {
      val (partitionId, buffer) = iterator.next()
      freed += spillPartition(partitionId, buffer, releaseReservation = true)
    }
    freed
  }

  /**
   * Enforce the per-partition on-heap cap (finding M2): when a partition buffer reaches
   * [[perPartitionBufferCapacityBytes]], spill it durably so no single partition can grow the
   * writer's footprint without bound. Called after every append in [[write]].
   *
   * @param partitionId the reduce partition just appended to
   * @param buffer      that partition's buffer
   */
  @throws(classOf[IOException])
  private def enforcePartitionCap(partitionId: Int, buffer: StreamingBuffer): Unit = {
    if (buffer.size >= perPartitionBudgetBytes) {
      spillPartition(partitionId, buffer, releaseReservation = true)
    }
  }

  /**
   * Spill a single partition's buffer to disk through the shared spill routine and, optionally,
   * release the corresponding execution-memory reservation.
   *
   * Routing through [[MemorySpillManager.spillBufferToDisk]] guarantees every streaming spill --
   * the background poll, the cooperative memory-manager spill, and the per-partition cap --
   * persists and TRACKS the block identically (findings M16 and M17): the spill is atomic (no
   * append can interleave), buffered data is never lost on failure, and the block's
   * [[ShuffleBlockId]] is recorded so `StreamingShuffleManager.unregisterShuffle` ->
   * [[MemorySpillManager.removeShuffle]] can later delete it. The spill manager owns integrity,
   * tracking, the spill-count metric, and INFO logging; this method only frees the reclaimed bytes
   * from the inner consumer's reservation when `releaseReservation` is set (the cooperative and cap
   * paths, which release memory the writer holds; the reconcile shortfall path passes `false`
   * because that memory was never granted).
   *
   * @param partitionId        the reduce partition to spill
   * @param buffer             the partition's buffer
   * @param releaseReservation whether to free the reclaimed bytes from the inner consumer's
   *                           reservation
   * @return the number of bytes reclaimed from memory (0 if empty or the store was not confirmed)
   */
  private def spillPartition(
      partitionId: Int, buffer: StreamingBuffer, releaseReservation: Boolean): Long = {
    val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
    val freed = spillManager.spillBufferToDisk(blockId, buffer)
    if (freed > 0L && releaseReservation) {
      memoryConsumer.freeMemory(freed)
    }
    freed
  }

  /**
   * Close this writer, returning the [[MapStatus]] on success. Idempotent: the first call wins and
   * subsequent calls return `None`. Buffer memory is released and unregistered exactly once in the
   * `finally` block regardless of outcome, mirroring `SortShuffleWriter.stop`.
   *
   * @param success whether the map task completed successfully
   * @return the map status on success, otherwise `None`
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      releaseAllResources()
    }
  }

  /** Get the lengths, in serialized bytes, written to each reduce partition. */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  // -- Internal helpers --------------------------------------------------------------------------

  /**
   * Serialize a single (key, value) record into a self-contained byte array using the shared
   * [[SerializerInstance]]. A fresh serialization stream is opened over the reusable scratch sink
   * for each record so the produced bytes are independently framed; this lets the wire channel
   * split a partition's buffered bytes at arbitrary 2 MB block boundaries without straddling a
   * partially written record. Keys and values are typed as `Any` (mirroring
   * `DiskBlockObjectWriter.write`) so the serializer's `ClassTag` resolves to `ClassTag[Any]`.
   *
   * @param key   the record key
   * @param value the record value
   * @return the serialized bytes for this record
   */
  private def serializeRecord(key: Any, value: Any): Array[Byte] = {
    serializationScratch.reset()
    val stream = serInstance.serializeStream(serializationScratch)
    stream.writeKey(key)
    stream.writeValue(value)
    stream.close()
    serializationScratch.toByteArray
  }

  /**
   * Return the partition's [[StreamingBuffer]], allocating and registering it with the
   * [[MemorySpillManager]] on first use. Allocation is lazy so empty or skewed partitions never
   * reserve memory until they receive their first record.
   *
   * @param partitionId the reduce partition id
   * @return the (possibly newly allocated) buffer for the partition
   */
  private def bufferFor(partitionId: Int): StreamingBuffer = {
    var buffer = buffers(partitionId)
    if (buffer == null) {
      buffer = new StreamingBuffer(shuffleId, mapId, partitionId, initialBufferCapacity)
      buffers(partitionId) = buffer
      spillManager.register(shuffleId, mapId, partitionId, buffer)
    }
    buffer
  }

  /**
   * Frame and dispatch full 2 MB wire blocks for a partition whose buffer has reached the block
   * boundary. Each block is copied out of the buffer snapshot, wrapped in a
   * [[StreamingBlockEnvelope]] (which stamps a CRC32C over the payload), gated through the
   * [[BackpressureProtocol]], and handed to the transport. The buffer is then reset and any
   * sub-block remainder re-appended, bounding each partition's on-heap footprint to roughly one
   * 2 MB block between flushes; [[reconcileMemory]] releases the drained execution memory.
   *
   * '''Gated on transport capability (finding C1).''' In v1 the transport advertises no
   * wire-transfer capability, so this method is a no-op: no bytes are put on the wire and -- the
   * key correctness point -- no buffered bytes are ever reset after a no-op send. The persist/spill
   * channel therefore remains the sole, durable output path, and the writer never discards data it
   * has counted. The framing/backpressure/transport logic below is retained for v2, when the
   * transport can actually transfer bytes.
   *
   * @param partitionId the reduce partition id
   * @param buffer      the partition's buffer, known to be non-null
   */
  private def maybeFlushWireBlocks(partitionId: Int, buffer: StreamingBuffer): Unit = {
    if (!transport.isWireTransferAvailable) {
      return
    }
    while (buffer.size >= StreamingBlockEnvelope.MAX_PAYLOAD_BYTES) {
      val snapshot = buffer.snapshot()
      val blockLength = math.min(StreamingBlockEnvelope.MAX_PAYLOAD_BYTES, snapshot.length)
      val block = snapshot.slice(0, blockLength)
      sendBlock(partitionId, block)
      // Retain any bytes beyond this 2 MB block for the next flush (or the residual flush). Reset
      // (not truncate) so the previously grown backing array becomes eligible for GC.
      buffer.reset()
      if (snapshot.length > blockLength) {
        buffer.append(snapshot, blockLength, snapshot.length - blockLength)
      }
      reconcileMemory()
    }
  }

  /**
   * Flush every partition's sub-2 MB tail as a final wire block at end of [[write]]. These residual
   * blocks complete the partition streams; the underlying buffers are left registered so the spill
   * manager can still reclaim them and are ultimately released by [[stop]].
   *
   * '''Gated on transport capability (finding C1).''' Like [[maybeFlushWireBlocks]], this is a
   * no-op in v1 (the transport advertises no wire-transfer capability), so no residual bytes are
   * sent or discarded; the buffered tails remain in the durable persist/spill channel and are
   * released by [[stop]].
   */
  private def flushResidualBlocks(): Unit = {
    if (!transport.isWireTransferAvailable) {
      return
    }
    var partitionId = 0
    while (partitionId < numPartitions) {
      val buffer = buffers(partitionId)
      if (buffer != null && buffer.size > 0) {
        // buffer.size is < MAX_PAYLOAD_BYTES here (maybeFlushWireBlocks drained full blocks).
        sendBlock(partitionId, buffer.snapshot())
      }
      partitionId += 1
    }
  }

  /**
   * Frame a single &le;2 MB block into a [[StreamingBlockEnvelope]] (CRC32C computed by the
   * envelope factory), consult the [[BackpressureProtocol]] send gate, and route the block through
   * the [[StreamingShuffleTransport]]. Only reached when the transport advertises wire-transfer
   * capability (the v2 path); in v1 the callers [[maybeFlushWireBlocks]] and
   * [[flushResidualBlocks]] gate this off entirely. The producer map id is narrowed to the
   * envelope's 4-byte wire field. Serialized-byte accounting is done once at record production in
   * [[write]], never here, so bytes are never double-counted or attributed to a send.
   *
   * @param partitionId the reduce partition id
   * @param block       the block payload, guaranteed to be at most 2 MB
   */
  private def sendBlock(partitionId: Int, block: Array[Byte]): Unit = {
    val sequenceNumber = sequenceNumbers(partitionId)
    sequenceNumbers(partitionId) += 1
    val envelope =
      StreamingBlockEnvelope(shuffleId, mapId.toInt, partitionId, sequenceNumber, block)
    val admitted = backpressure.acquire(block.length.toLong)
    if (!admitted && debugEnabled) {
      logDebug(log"streaming-shuffle backpressure signaled " +
        log"(shuffleId=${MDC(LogKeys.SHUFFLE_ID, shuffleId)}, " +
        log"mapId=${MDC(LogKeys.MAP_ID, mapId)}, " +
        log"reducePartitionRange=${MDC(REDUCE_PARTITION_RANGE, singlePartition(partitionId))}, " +
        log"bytes=${MDC(LogKeys.NUM_BYTES, block.length)})")
    }
    transport.send(envelope)
  }

  /**
   * Reconcile the execution memory reserved through the inner [[MemoryConsumer]] with the live
   * bytes buffered on heap, '''honoring''' the value returned by `acquireMemory` (finding M3).
   * Growing buffers request the shortfall via `acquireMemory` (which may cooperatively invoke the
   * consumer's `spill` under pressure); drained or reset buffers release the excess via
   * `freeMemory`. Because `acquireMemory` can grant less than requested, its return value is
   * checked: when the grant falls short the writer spills its own buffers until the footprint fits
   * the reservation, so on-heap usage never exceeds accounted execution memory. Keeping the
   * consumer's `getUsed()` aligned with the buffered footprint is what lets the task memory manager
   * account streaming buffers with no change to the executor memory model.
   */
  private def reconcileMemory(): Unit = {
    val buffered = totalBufferedBytes
    val held = memoryConsumer.getUsed()
    if (buffered > held) {
      val required = buffered - held
      val granted = memoryConsumer.acquireMemory(required)
      if (granted < required) {
        // M3: the task memory manager granted less than the live footprint needs (even after any
        // cooperative spill it triggered via the consumer's spill callback). Honor the shortfall by
        // spilling our own buffers until the on-heap footprint fits within the memory we actually
        // hold, so heap never silently exceeds accounted execution memory.
        spillToFitReservation()
      }
    } else if (buffered < held) {
      memoryConsumer.freeMemory(held - buffered)
    }
  }

  /**
   * Spill largest-first, without releasing the reservation, until the live buffered footprint no
   * longer exceeds the execution memory reserved through the inner [[MemoryConsumer]]. Used by
   * [[reconcileMemory]] when `acquireMemory` grants less than the footprint requires (finding M3):
   * the shortfall bytes were never granted, so the spilled heap is released while the reservation
   * is kept, converging `totalBufferedBytes` down to `getUsed()`. Any resulting over-reservation is
   * trimmed by the next [[reconcileMemory]] (its `buffered < held` branch) or by [[stop]].
   */
  private def spillToFitReservation(): Unit = {
    val candidates = collectSpillCandidates().iterator
    while (totalBufferedBytes > memoryConsumer.getUsed() && candidates.hasNext) {
      val (partitionId, buffer) = candidates.next()
      spillPartition(partitionId, buffer, releaseReservation = false)
    }
  }

  /** Sum of the live on-heap bytes across all allocated partition buffers. */
  private def totalBufferedBytes: Long = {
    var total = 0L
    var partitionId = 0
    while (partitionId < numPartitions) {
      val buffer = buffers(partitionId)
      if (buffer != null) {
        total += buffer.size
      }
      partitionId += 1
    }
    total
  }

  /** Snapshot the allocated, non-empty partition buffers ordered largest-first for spilling. */
  private def collectSpillCandidates(): Seq[(Int, StreamingBuffer)] = {
    val candidates = ArrayBuffer.empty[(Int, StreamingBuffer)]
    var partitionId = 0
    while (partitionId < numPartitions) {
      val buffer = buffers(partitionId)
      if (buffer != null && buffer.size > 0) {
        candidates += ((partitionId, buffer))
      }
      partitionId += 1
    }
    candidates.sortBy { case (_, buffer) => -buffer.size }.toSeq
  }

  /**
   * Reset, unregister, and free every allocated partition buffer, and release all execution memory
   * still reserved through the inner [[MemoryConsumer]]. Guarded by [[released]] so it runs at most
   * once even though [[stop]] may be called more than once.
   */
  private def releaseAllResources(): Unit = {
    if (!released) {
      released = true
      var partitionId = 0
      while (partitionId < numPartitions) {
        val buffer = buffers(partitionId)
        if (buffer != null) {
          buffer.reset()
          spillManager.unregister(shuffleId, mapId, partitionId)
          buffers(partitionId) = null
        }
        partitionId += 1
      }
      val held = memoryConsumer.getUsed()
      if (held > 0L) {
        memoryConsumer.freeMemory(held)
      }
    }
  }
}

/**
 * Constants for [[StreamingShuffleWriter]].
 */
private[spark] object StreamingShuffleWriter {

  /**
   * Nominal executor on-heap memory assumed for buffer sizing when no `SparkEnv` memory manager is
   * available (local or unit-test construction). Only affects the buffer budget; the 2 MB
   * per-partition floor still applies.
   */
  private val DEFAULT_EXECUTOR_MEMORY_BYTES: Long = 1024L * 1024L * 1024L

  /**
   * Minimum per-partition buffer budget: one 2 MB block, so every partition can stage a full block
   * before spilling ("2 MB block limit for pipelining efficiency").
   */
  private val MIN_PARTITION_BUFFER_BYTES: Long = 2L * 1024L * 1024L

  /** Initial capacity of the reusable per-record serialization scratch sink (64 KiB). */
  private val RECORD_SCRATCH_INITIAL_BYTES: Int = 64 * 1024
}
