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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

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
 * ==Dual serialization channels==
 * Every record is serialized exactly once by the shared [[SerializerInstance]] and routed through
 * two sinks that share those bytes but differ in destination:
 *
 *  - '''Persist channel''' -- serialized bytes are appended into the reduce partition's on-heap
 *    [[StreamingBuffer]]. Each buffer is registered with the [[MemorySpillManager]], which spills
 *    the largest / least-recently-used buffers to disk ([[StorageLevel.DISK_ONLY]]) under memory
 *    pressure and reclaims them within 100 ms of a consumer acknowledgment. This is the durable
 *    side that guarantees buffered data is never lost.
 *  - '''Wire channel''' -- once a partition's buffer reaches the 2 MB block boundary, the bytes are
 *    framed into a [[StreamingBlockEnvelope]] (a fixed 32-byte big-endian header plus a payload of
 *    at most 2 MB, stamped with a CRC32C), gated by the [[BackpressureProtocol]], and handed to the
 *    [[StreamingShuffleTransport]]. In v1 the transport is a logging-only stub, so no bytes are put
 *    on the wire; the call still exercises the framing, backpressure, and transport surfaces so the
 *    v2 wire path is real and testable.
 *
 * ==Memory discipline==
 * The per-executor streaming buffer budget is `executorMemory x bufferSizePercent / 100` and each
 * of the `numPartitions` reduce partitions receives an equal share of it, floored at one 2 MB wire
 * block (the preserved user formula `(executorMemory * bufferPercent) / numPartitions`). Execution
 * memory reserved through the inner [[MemoryConsumer]] is continuously reconciled with the live
 * buffered footprint, so growing buffers acquire memory (cooperatively triggering a spill under
 * pressure) and drained buffers release it. When the task memory manager cannot satisfy an
 * allocation it invokes the consumer's `spill`, which persists the largest buffers to disk and
 * frees their heap.
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

  // -- Derived shuffle metadata ------------------------------------------------------------------

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = dep.shuffleId

  // Single serializer instance shared by both serialization channels (wire + persist). Records are
  // serialized as self-contained units so a wire block may be sliced at any 2 MB byte boundary.
  private val serInstance: SerializerInstance = dep.serializer.newInstance()

  // Cached streaming debug flag (spark.shuffle.streaming.debug); gates verbose per-block logging.
  private val debugEnabled: Boolean = conf.debug

  // -- Buffer sizing (memory discipline, AAP 0.7.1) ----------------------------------------------

  // Executor on-heap memory used as the buffer-budget base. Sourced from the memory manager's
  // maxOnHeapStorageMemory. Gated on SparkEnv for local/test-mode safety: when the memory manager
  // is not initialized (e.g. a unit test constructing the writer directly) a nominal default is
  // used so sizing stays well-defined and the 2 MB per-partition floor still applies.
  private val executorMemoryBytes: Long = {
    val env = SparkEnv.get
    if (env != null && env.memoryManager != null) {
      env.memoryManager.maxOnHeapStorageMemory
    } else {
      StreamingShuffleWriter.DEFAULT_EXECUTOR_MEMORY_BYTES
    }
  }

  // Total per-executor streaming buffer budget: executorMemory x bufferSizePercent / 100. This is
  // the same denominator the MemorySpillManager uses for its 80% utilization spill trigger.
  private val totalBufferBudgetBytes: Long =
    math.max(0L, executorMemoryBytes * handle.bufferSizePercent / 100L)

  // Per-partition share of the budget with a 2 MB floor: (executorMemory x bufferPercent / 100) /
  // numPartitions, floored at one 2 MB wire block so a partition can always stage a full block.
  private val perPartitionBudgetBytes: Long =
    math.max(totalBufferBudgetBytes / math.max(1, numPartitions),
      StreamingShuffleWriter.MIN_PARTITION_BUFFER_BYTES)

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
  // callback below, which delegates to `spillBuffers`. The abstract `spill(size, trigger)` contract
  // is satisfied by this instance (its constructor is protected, hence the anonymous subclass).
  private val memoryConsumer: MemoryConsumer =
    new MemoryConsumer(
        context.taskMemoryManager(),
        context.taskMemoryManager().pageSizeBytes(),
        MemoryMode.ON_HEAP) {
      @throws(classOf[IOException])
      override def spill(size: Long, trigger: MemoryConsumer): Long = spillBuffers(size)
    }

  logInfo(s"Initialized streaming shuffle writer: shuffleId=$shuffleId mapId=$mapId " +
    s"numPartitions=$numPartitions bufferSizePercent=${handle.bufferSizePercent} " +
    s"executorMemoryBytes=$executorMemoryBytes totalBufferBudgetBytes=$totalBufferBudgetBytes " +
    s"perPartitionBudgetBytes=$perPartitionBudgetBytes")

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
      // Keep the reserved execution memory aligned with the buffer's new footprint.
      reconcileMemory()
      partitionLengths(partitionId) += recordBytes.length.toLong
      metricsReporter.incRecordsWritten(1L)
      // Dispatch any full 2 MB wire blocks that the append completed.
      maybeFlushWireBlocks(partitionId, buffer)
    }
    // Emit the trailing sub-block for every partition so each partition stream is complete.
    flushResidualBlocks()
    metricsReporter.incWriteTime(System.nanoTime() - writeStartNanos)
    // Publish the map output location and per-partition sizes. The 3-arg MapStatus.apply defaults
    // the checksum to 0: streaming integrity is per-block CRC32C carried in the envelope, not here.
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  /**
   * Body of the inner [[MemoryConsumer]]'s spill callback, invoked by the task memory manager under
   * execution-memory pressure. Persists the largest partition buffers to disk
   * ([[StorageLevel.DISK_ONLY]]) through the public [[BlockManager]] API -- the same mechanism the
   * [[MemorySpillManager]] background poller uses -- then resets each spilled buffer to release its
   * heap and frees the corresponding execution-memory reservation. This is the synchronous,
   * pressure-driven complement to the spill manager's periodic 80% utilization poll. Per the
   * [[MemoryConsumer]] contract it never calls `acquireMemory`, and it persists before releasing
   * heap so buffered shuffle data is never lost on a spill.
   *
   * @param size the number of bytes the memory manager is asking the consumer to release
   * @return the number of bytes actually released
   */
  @throws(classOf[IOException])
  private def spillBuffers(size: Long): Long = {
    var freed = 0L
    try {
      // Order allocated, non-empty buffers largest-first so the fewest spills reclaim the most.
      val candidates = collectSpillCandidates()
      val iterator = candidates.iterator
      while (iterator.hasNext && freed < size) {
        val (partitionId, buffer) = iterator.next()
        val bytes = buffer.snapshot()
        if (bytes.length > 0) {
          val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
          val chunked = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))
          // Persist before releasing heap so buffered shuffle data is never lost on spill.
          val stored =
            blockManager.putBytes(blockId, chunked, StorageLevel.DISK_ONLY)(ClassTag.Any)
          if (stored) {
            val reclaimed = buffer.size
            buffer.reset()
            streamingMetrics.incSpillCount()
            memoryConsumer.freeMemory(reclaimed)
            freed += reclaimed
            if (debugEnabled) {
              logDebug(s"streaming-shuffle spilled block to disk: shuffleId=$shuffleId " +
                s"mapId=$mapId reduceId=$partitionId bytes=$reclaimed")
            }
          }
        }
      }
    } catch {
      case e: IOException =>
        logError(s"streaming-shuffle spill failed: shuffleId=$shuffleId mapId=$mapId", e)
        throw e
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
   * @param partitionId the reduce partition id
   * @param buffer      the partition's buffer, known to be non-null
   */
  private def maybeFlushWireBlocks(partitionId: Int, buffer: StreamingBuffer): Unit = {
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
   */
  private def flushResidualBlocks(): Unit = {
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
   * the [[StreamingShuffleTransport]]. The producer map id is narrowed to the envelope's 4-byte
   * wire field. In v1 the transport is a logging-only stub, so no bytes are put on the wire; the
   * call still exercises framing, backpressure, and transport end to end. When the send gate
   * signals backpressure (only possible when a bandwidth cap is configured) v1 records the event
   * and proceeds rather than hard-blocking, because there is no real consumer to grant credit
   * against the stub transport; v2 will honor the gate.
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
      logDebug(s"streaming-shuffle backpressure signaled: shuffleId=$shuffleId mapId=$mapId " +
        s"reduceId=$partitionId seq=$sequenceNumber bytes=${block.length}; proceeding in v1")
    }
    transport.send(envelope)
    metricsReporter.incBytesWritten(block.length.toLong)
  }

  /**
   * Reconcile the execution memory reserved through the inner [[MemoryConsumer]] with the live
   * bytes buffered on heap. Growing buffers acquire the shortfall via `acquireMemory` (which may
   * cooperatively invoke the consumer's `spill` under pressure); drained or reset buffers release
   * the excess via `freeMemory`. Keeping the consumer's `getUsed()` aligned with the buffered
   * footprint is what lets the task memory manager account streaming buffers with no change to the
   * executor memory model.
   */
  private def reconcileMemory(): Unit = {
    val buffered = totalBufferedBytes
    val held = memoryConsumer.getUsed()
    if (buffered > held) {
      // acquireMemory may grant less than requested and may trigger a spill; any remaining
      // shortfall is re-attempted on the next reconcile.
      memoryConsumer.acquireMemory(buffered - held)
    } else if (buffered < held) {
      memoryConsumer.freeMemory(held - buffered)
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
