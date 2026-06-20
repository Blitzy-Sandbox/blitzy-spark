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

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport

/**
 * The map-side writer of the opt-in streaming shuffle backend.
 *
 * Instead of fully materializing a map task's output to local disk before any reduce-side fetch can
 * begin (the sort-based path), this writer buffers each partition's serialized output in a bounded
 * in-memory [[StreamingBuffer]], frames it into 2 MB CRC32C blocks, applies token-bucket
 * backpressure, coordinates disk spill under memory pressure, and (in v1) hands each block to
 * the logging-only [[StreamingShuffleTransport]]. The buffered bytes are served to the reduce side
 * directly from memory by `StreamingShuffleBlockResolver`, which is precisely what lets a reducer
 * read map output before it is ever written to disk.
 *
 * ==Composition over inheritance (the two-abstract-classes constraint)==
 *
 * Participating in the executor memory model requires being a [[MemoryConsumer]] so the
 * [[TaskMemoryManager]] can ask this writer to spill under pressure. But both [[ShuffleWriter]] and
 * [[MemoryConsumer]] are abstract '''classes''', and Scala permits only one class parent. This
 * writer therefore extends [[ShuffleWriter]] and '''composes''' a private inner
 * [[BufferMemoryConsumer]] (constructed from `context.taskMemoryManager()`) rather than extending
 * both. The inner consumer's `spill` delegates to the shared [[MemorySpillManager]].
 *
 * ==Write algorithm==
 *
 *   1. For each record, the [[org.apache.spark.Partitioner]] selects a reduce partition; the record
 *      is serialized through a per-partition serialization stream (one independent
 *      `SerializerInstance` per partition so interleaved writes never corrupt serializer state).
 *   1. When a partition's staged serialized bytes reach the 2 MB block size, they are appended to
 *      that partition's [[StreamingBuffer]] (which frames and checksums them), counted toward
 *      the partition length, and framed into wire envelopes that are sent under backpressure.
 *   1. Residual bytes below the block size are flushed when the partition is finalized at end of
 *      write.
 *
 * ==Dual-channel wire/persist invariant==
 *
 * The [[StreamingBuffer]] encodes its bytes as the exact same canonical
 * [[StreamingBlockEnvelope]] frames that travel on the wire, so spilled and streamed bytes are
 * byte-for-byte interchangeable: a reducer cannot tell if a partition was served from memory or
 * rehydrated from a disk spill.
 *
 * ==Memory model and the no-leak guarantee==
 *
 * As buffers grow the writer reserves execution memory through the composed consumer; a shortfall
 * triggers `spill`, which drains the largest buffers to disk via [[MemorySpillManager]]. On
 * [[stop]] the writer releases '''all''' execution memory it reserved (whether the map succeeded or
 * failed), so the task leaves no leaked allocation behind. This is verified by the stress suite
 * under `spark.unsafe.exceptionOnMemoryLeak=true`.
 *
 * ==Coexistence with the sort-based shuffle==
 *
 * This writer is constructed only on the streaming path and never touches the sort-based
 * `SortShuffleManager`, which remains the automatic fallback. When the streaming backend falls
 * back, this class is simply not instantiated.
 *
 * @param handle           the streaming shuffle handle carrying the dependency and tuning values
 * @param mapId            the map task id (a Long task-attempt id) this writer produces output for
 * @param context          the task context, source of the [[TaskMemoryManager]] and attempt id
 * @param metrics          the write-metrics reporter for bytes/records/time accounting
 * @param config           the typed streaming configuration and shared invariants
 * @param streamingMetrics the shared streaming telemetry holder (observed for debug logging)
 * @param backpressure     the flow-control protocol gating sends and tracking consumer liveness
 * @param spillManager     the memory spill manager buffers are registered with for disk spill
 * @param transport        the v1 logging-only transport the framed blocks are handed to
 * @param blockResolver    the block resolver buffers are tracked with so the reduce side can read
 *                         them from memory before they are ever spilled
 */
private[spark] class StreamingShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter,
    config: StreamingShuffleConfig,
    streamingMetrics: StreamingShuffleMetrics,
    backpressure: BackpressureProtocol,
    spillManager: MemorySpillManager,
    transport: StreamingShuffleTransport,
    blockResolver: StreamingShuffleBlockResolver)
  extends ShuffleWriter[K, V] with Logging {

  import StreamingShuffleWriter._

  /**
   * The composed [[MemoryConsumer]] through which this writer participates in the executor memory
   * model. It registers with the task's [[TaskMemoryManager]] when it first acquires memory so the
   * manager can later ask this writer to spill under pressure; the spill is
   * satisfied by draining buffers to disk through the shared [[MemorySpillManager]].
   *
   * @param tmm the task's memory manager, from `TaskContext.taskMemoryManager()`
   */
  private class BufferMemoryConsumer(tmm: TaskMemoryManager)
    extends MemoryConsumer(tmm, MemoryMode.ON_HEAP) {

    /**
     * Releases memory at the task memory manager's request by draining the largest buffered
     * partitions to disk via the shared spill manager, then freeing the matching execution-memory
     * reservation. Never calls `acquireMemory` (which is forbidden from within `spill`).
     *
     * @param size    the number of bytes the memory manager is asking to release
     * @param trigger the consumer that triggered this spill (possibly this consumer)
     * @return the number of execution-memory bytes actually released
     */
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      val reclaimed = spillManager.maybeSpill()
      if (reclaimed <= 0L) {
        0L
      } else {
        val release = math.min(reclaimed, getUsed)
        if (release > 0L) {
          freeMemory(release)
        }
        release
      }
    }
  }

  /** The shuffle dependency: the single source of the partitioner, serializer, and shuffle id. */
  private val dep = handle.dependency

  /** The id of the shuffle this map task contributes output to. */
  private val shuffleId: Int = dep.shuffleId

  /** Maps each record key to its destination reduce partition. */
  private val partitioner = dep.partitioner

  /** The number of reduce partitions; the length of [[partitionLengths]] and the MapStatus. */
  private val numPartitions: Int = partitioner.numPartitions

  /** The running SparkEnv, or null in local-mode / unit-test contexts without an executor env. */
  private val sparkEnv = SparkEnv.get

  /** The executor BlockManager (for the map-output location), or null when no SparkEnv exists. */
  private val blockManager = if (sparkEnv != null) sparkEnv.blockManager else null

  /** The MemoryManager whose maxOnHeapStorageMemory sizes per-partition buffers; may be null. */
  private val memoryManager = if (sparkEnv != null) sparkEnv.memoryManager else null

  /** Composed memory consumer: the composition-over-inheritance answer to two abstract classes. */
  private val memoryConsumer = new BufferMemoryConsumer(context.taskMemoryManager())

  /** Per-partition in-memory buffers (the read-side / spill source), created lazily. */
  private val buffers = new Array[StreamingBuffer](numPartitions)

  /** Per-partition serialization streams over [[stagingStreams]], created lazily on first use. */
  private val partitionSerializers = new Array[SerializationStream](numPartitions)

  /** Per-partition serialization staging buffers, drained into [[buffers]] at the 2 MB boundary. */
  private val stagingStreams = new Array[ByteArrayOutputStream](numPartitions)

  /** Per-partition monotonically increasing block sequence numbers for the wire envelopes. */
  private val sequenceNumbers = new Array[Long](numPartitions)

  /** Tracks which partitions have a backpressure stream registered, for clean teardown. */
  private val streamRegistered = new Array[Boolean](numPartitions)

  /** Per-partition written byte counts, returned by [[getPartitionLengths]] for the MapStatus. */
  private val partitionLengths = new Array[Long](numPartitions)

  /** Total payload bytes written across all partitions (for metric rollback on failure). */
  private var totalBytesWritten: Long = 0L

  /** Total records written across all partitions (for metric rollback on failure). */
  private var totalRecordsWritten: Long = 0L

  /** Guards against a double stop: stop(true) followed by stop(false) on the exception path. */
  private var stopping: Boolean = false

  /** The MapStatus produced by a successful [[write]]; null until write completes. */
  private var mapStatus: MapStatus = null

  /**
   * Writes a map task's records to the streaming shuffle output. Each record is routed to its
   * reduce partition, serialized through that partition's stream, buffered in memory, framed into
   * 2 MB CRC32C blocks, and sent under backpressure. On return every partition's bytes are buffered
   * (and observable by the reduce side) and the [[MapStatus]] for this map output is built.
   *
   * @param records the (key, value) records produced by this map task, in arbitrary partition order
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val startNanos = System.nanoTime()
    logInfo(log"Streaming shuffle write starting " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
      log"attempt=${MDC(TASK_ATTEMPT_ID, context.taskAttemptId())} " +
      log"partitions=${MDC(NUM_PARTITIONS, numPartitions)}")
    while (records.hasNext) {
      val record = records.next()
      val key = record._1
      val value = record._2
      val partition = partitioner.getPartition(key)
      ensurePartition(partition)
      partitionSerializers(partition).writeKey(key: Any).writeValue(value: Any)
      totalRecordsWritten += 1L
      metrics.incRecordsWritten(1L)
      if (stagingStreams(partition).size() >= StreamingShuffleConfig.BLOCK_SIZE_BYTES) {
        flushAndDrain(partition)
      }
    }
    finalizeAllPartitions()
    mapStatus = buildMapStatus()
    metrics.incWriteTime(System.nanoTime() - startNanos)
    logInfo(log"Streaming shuffle write completed " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
      log"bytes=${MDC(NUM_BYTES, totalBytesWritten)} records=${MDC(COUNT, totalRecordsWritten)}")
    if (config.debug) {
      logDebug(s"Streaming shuffle observed spills=${streamingMetrics.spillCount} " +
        s"backpressureEvents=${streamingMetrics.backpressureEvents}")
    }
  }

  /**
   * Closes this writer. On success the buffered partitions are intentionally left registered with
   * the spill manager and block resolver so the reduce side can fetch them; on failure the partial
   * output is discarded and the reported write metrics are rolled back. In both cases every
   * execution-memory reservation is released so the task leaves no memory leak.
   *
   * @param success whether the map task completed successfully
   * @return `Some(mapStatus)` on success (or `None` without a location); `None` on failure
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    if (success) {
      stopOnSuccess()
    } else {
      stopOnFailure()
    }
  }

  /** @return the per-partition written byte counts backing the MapStatus. */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  /**
   * Lazily creates the buffer, serialization stream, and staging stream for `partition` on first
   * use, registering the buffer with the spill manager and block resolver and the stream with the
   * backpressure protocol. A re-entrant call for an already-initialized partition is a no-op.
   *
   * @param partition the reduce partition to initialize
   */
  private def ensurePartition(partition: Int): Unit = {
    if (buffers(partition) == null) {
      val buffer = new StreamingBuffer(shuffleId, mapId, partition, perPartitionBufferBytes())
      buffers(partition) = buffer
      spillManager.register(buffer)
      blockResolver.trackBuffer(buffer)
      val staging = new ByteArrayOutputStream(STAGING_BUFFER_INITIAL_BYTES)
      stagingStreams(partition) = staging
      partitionSerializers(partition) = dep.serializer.newInstance().serializeStream(staging)
      backpressure.registerStream(BackpressureProtocol.StreamKey(shuffleId, mapId, partition))
      streamRegistered(partition) = true
    }
  }

  /**
   * Computes the per-partition buffer capacity in bytes from the executor on-heap storage memory
   * budget (the same denominator the spill manager measures against), applying the configured
   * percentage and the 2 MB floor. Falls back to the JVM max heap when no SparkEnv is present.
   *
   * @return the per-partition buffer capacity in bytes (never below 2 MB)
   */
  private def perPartitionBufferBytes(): Long = {
    val executorMemoryBytes =
      if (memoryManager != null) memoryManager.maxOnHeapStorageMemory
      else Runtime.getRuntime.maxMemory()
    config.perPartitionBufferBytes(executorMemoryBytes, numPartitions)
  }

  /**
   * Flushes a partition's serialization stream and drains its staged bytes into the buffer. Invoked
   * when the staged size reaches the 2 MB block boundary during the write loop.
   *
   * @param partition the reduce partition to flush and drain
   */
  private def flushAndDrain(partition: Int): Unit = {
    partitionSerializers(partition).flush()
    drainBytes(partition)
  }

  /**
   * Drains a partition's currently staged serialized bytes into its [[StreamingBuffer]] (which
   * frames and checksums them), updates the partition length and write metrics, frames the bytes
   * into wire envelopes sent under backpressure, then resets the staging stream. Reading the bytes
   * from the local `pending` array rather than from the buffer keeps draining safe even if the
   * spill manager's poll loop concurrently drains the buffer.
   *
   * @param partition the reduce partition to drain
   */
  private def drainBytes(partition: Int): Unit = {
    val staging = stagingStreams(partition)
    val pending = staging.toByteArray
    if (pending.length > 0) {
      acquireMemoryFor(pending.length)
      buffers(partition).append(pending)
      // MapStatus must report the PHYSICAL block size the resolver/spill path serves, not the raw
      // payload: both `StreamingBuffer.toByteArray` (in-memory serve) and the disk spill frame the
      // bytes as the in-order concatenation of one StreamingBlockEnvelope per 2 MB block, adding a
      // fixed 32-byte header per block. `append` and `sendFramed` split `pending` into the same
      // ceil(len / 2 MB) blocks, so the served byte count is `pending.length + numBlocks * header`.
      // Reporting the framed size keeps `partitionLengths` consistent with the bytes a reduce task
      // actually fetches, avoiding a 32-byte-per-block under-count in fetch/scheduling accounting.
      val numBlocks = (pending.length + StreamingShuffleConfig.BLOCK_SIZE_BYTES - 1) /
        StreamingShuffleConfig.BLOCK_SIZE_BYTES
      val framedLength =
        pending.length.toLong + numBlocks.toLong * StreamingBlockEnvelope.HEADER_BYTES.toLong
      partitionLengths(partition) += framedLength
      // Write-volume telemetry remains the LOGICAL payload size (excludes envelope framing) so the
      // bytes-written metric reflects user data produced, independent of the wire/persist format.
      totalBytesWritten += pending.length.toLong
      metrics.incBytesWritten(pending.length.toLong)
      val streamKey = BackpressureProtocol.StreamKey(shuffleId, mapId, partition)
      sendFramed(streamKey, partition, pending)
      maybeHandleConsumerTimeout(streamKey, partition)
      staging.reset()
    }
  }

  /**
   * Frames `bytes` into blocks of at most 2 MB, acquiring a backpressure send permit per block,
   * building the canonical [[StreamingBlockEnvelope]], handing it to the v1 logging-only transport,
   * and recording the unacknowledged byte count. The real data plane is the reduce side's
   * `BlockTransferService.fetchBlockSync`, so the transport hand-off is observational in v1.
   *
   * @param streamKey the (shuffleId, mapId, reduceId) identity of this block stream
   * @param reduceId  the destination reduce partition
   * @param bytes     the serialized bytes to frame and send
   */
  private def sendFramed(
      streamKey: BackpressureProtocol.StreamKey,
      reduceId: Int,
      bytes: Array[Byte]): Unit = {
    val target = shuffleServerLocation
    var offset = 0
    while (offset < bytes.length) {
      val len = math.min(StreamingShuffleConfig.BLOCK_SIZE_BYTES, bytes.length - offset)
      val payload = bytes.slice(offset, offset + len)
      backpressure.acquireSendPermit(len)
      val seq = sequenceNumbers(reduceId)
      sequenceNumbers(reduceId) = seq + 1L
      val envelope = StreamingBlockEnvelope.create(shuffleId, mapId, reduceId, seq, payload)
      target.foreach(location => transport.sendBlock(envelope, location))
      backpressure.recordSend(streamKey, len.toLong)
      offset += len
    }
  }

  /**
   * Implements the consumer-failure protocol: when the backpressure protocol has flagged the
   * consumer as timed out (no acks within the 10 s window), the unacknowledged data is persisted to
   * disk (spilling if the buffer is above its threshold) so it is not lost, and a retransmit is
   * recorded under exponential backoff. The actual re-delivery rides the reduce-side fetch path in
   * v1. This path only engages once the protocol's background scan is running and the timeout has
   * elapsed, so it is inert for short-lived writes.
   *
   * @param streamKey the (shuffleId, mapId, reduceId) identity of this block stream
   * @param reduceId  the destination reduce partition
   */
  private def maybeHandleConsumerTimeout(
      streamKey: BackpressureProtocol.StreamKey, reduceId: Int): Unit = {
    if (backpressure.isConsumerTimedOut(streamKey)) {
      val unacked = backpressure.unackedBytes(streamKey)
      spillManager.maybeSpill()
      if (backpressure.isRetransmitDue(streamKey)) {
        backpressure.recordRetransmit(streamKey)
        logWarning(log"Streaming consumer unresponsive; buffered for retransmit " +
          log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
          log"reduce=${MDC(REDUCE_ID, reduceId)} unacked=${MDC(NUM_BYTES, unacked)}")
      }
    }
  }

  /**
   * Reserves execution memory to reflect heap growth as buffers fill. A shortfall causes the task
   * memory manager to invoke spilling (possibly this consumer), which the spill manager
   * satisfies by draining buffers to disk. The granted amount may be less than asked; the buffer
   * still holds the bytes regardless.
   *
   * @param bytes the number of bytes just buffered
   */
  private def acquireMemoryFor(bytes: Int): Unit = {
    if (bytes > 0) {
      memoryConsumer.acquireMemory(bytes.toLong)
    }
  }

  /** @return the local map-output location, or `None` when no BlockManager is available. */
  private def shuffleServerLocation =
    if (blockManager != null) Option(blockManager.shuffleServerId) else None

  /**
   * Closes each still-open partition serialization stream (flushing residual serialized bytes into
   * its staging stream) and drains those residual bytes into the buffer. The buffers themselves are
   * retained so the reduce side can read them.
   */
  private def finalizeAllPartitions(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      if (partitionSerializers(partition) != null) {
        partitionSerializers(partition).close()
        drainBytes(partition)
        partitionSerializers(partition) = null
        stagingStreams(partition) = null
      }
      partition += 1
    }
  }

  /** @return the MapStatus for this map output, or null when no location is available. */
  private def buildMapStatus(): MapStatus = {
    shuffleServerLocation match {
      case Some(location) => MapStatus(location, partitionLengths, mapId)
      case None => null
    }
  }

  /**
   * Success teardown: returns the produced MapStatus while keeping the buffers registered for the
   * reduce side, and releases this task's execution-memory reservation.
   *
   * @return `Some(mapStatus)`, or `None` if no map-output location was available
   */
  private def stopOnSuccess(): Option[MapStatus] = {
    try {
      Option(mapStatus)
    } finally {
      cleanup(discardBuffers = false)
    }
  }

  /**
   * Failure teardown: rolls back the reported write metrics so a failed attempt does not inflate
   * totals, discards the partial buffered output, and releases all execution memory.
   *
   * @return always `None`
   */
  private def stopOnFailure(): Option[MapStatus] = {
    try {
      if (totalBytesWritten > 0L) {
        metrics.decBytesWritten(totalBytesWritten)
      }
      if (totalRecordsWritten > 0L) {
        metrics.decRecordsWritten(totalRecordsWritten)
      }
      None
    } finally {
      cleanup(discardBuffers = true)
    }
  }

  /**
   * Common teardown: closes any open streams, unregisters the backpressure streams, optionally
   * discards the buffered output (on failure), and always releases all reserved execution memory so
   * the task leaves no leak.
   *
   * @param discardBuffers whether to clear and unregister the in-memory buffers (true on failure)
   */
  private def cleanup(discardBuffers: Boolean): Unit = {
    closeOpenStreams()
    unregisterStreams()
    if (discardBuffers) {
      discardTrackedBuffers()
    }
    releaseAllMemory()
  }

  /** Closes any serialization streams still open (e.g. on a failure mid-write), ignoring errors. */
  private def closeOpenStreams(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val serializer = partitionSerializers(partition)
      if (serializer != null) {
        try {
          serializer.close()
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to close streaming serializer for partition " +
              log"${MDC(PARTITION_ID, partition)} during teardown", e)
        }
        partitionSerializers(partition) = null
        stagingStreams(partition) = null
      }
      partition += 1
    }
  }

  /** Unregisters every backpressure stream this writer registered. */
  private def unregisterStreams(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      if (streamRegistered(partition)) {
        backpressure.unregisterStream(
          BackpressureProtocol.StreamKey(shuffleId, mapId, partition))
        streamRegistered(partition) = false
      }
      partition += 1
    }
  }

  /** Clears and unregisters the in-memory buffers; used on failure to drop the partial output. */
  private def discardTrackedBuffers(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val buffer = buffers(partition)
      if (buffer != null) {
        spillManager.unregister(MemorySpillManager.keyFor(buffer))
        buffer.clear()
        buffers(partition) = null
      }
      partition += 1
    }
  }

  /** Releases all execution memory reserved by the composed consumer (the no-leak guarantee). */
  private def releaseAllMemory(): Unit = {
    val used = memoryConsumer.getUsed
    if (used > 0L) {
      memoryConsumer.freeMemory(used)
    }
  }
}

/**
 * Companion holding the writer's compile-time constants.
 */
private[spark] object StreamingShuffleWriter {

  /** Initial capacity, in bytes, of each partition's serialization staging stream (64 KB). */
  private val STAGING_BUFFER_INITIAL_BYTES: Int = 64 * 1024
}
