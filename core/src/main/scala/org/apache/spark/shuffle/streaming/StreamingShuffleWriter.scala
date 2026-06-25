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

import java.io.{File, FileOutputStream, OutputStream}
import java.util.zip.CRC32C

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.TempLocalBlockId

/**
 * Producer (map) side writer for the streaming shuffle data path (feature F-103).
 *
 * Unlike the sort-based writer, which materializes a single merged file per map task before any
 * reducer can fetch, this writer accumulates each map task's output into bounded, per-partition
 * in-memory buffers and frames that data into self-describing, CRC32C-protected blocks of at most
 * 2 MiB so it can be pipelined toward consumers without a write-to-disk-then-fetch barrier. When
 * the streaming buffers approach their memory budget the largest partitions are spilled to local
 * disk, guaranteeing that a memory-bound map task degrades gracefully rather than failing.
 *
 * '''Single-inheritance design.''' Both [[ShuffleWriter]] and
 * [[org.apache.spark.memory.MemoryConsumer]] are concrete (abstract) classes, and Scala forbids
 * extending two classes. Because the object returned by `StreamingShuffleManager.getWriter` must
 * be a `ShuffleWriter[K, V]`, this class extends [[ShuffleWriter]] and '''composes''' a private
 * inner [[MemoryConsumer]] ([[StreamingShuffleMemoryConsumer]]) to participate in Spark's
 * cooperative spilling protocol. This mirrors how the sort path layers
 * `o.a.s.util.collection.Spillable` over a `MemoryConsumer`.
 *
 * '''Memory budget.''' The total budget for streaming buffers is a configurable percentage of
 * executor memory: `streamingMemoryBudget = (executorMemory * bufferSizePercent) / 100`, where
 * `executorMemory` is read (read-only) from
 * `SparkEnv.get.memoryManager.maxOnHeapStorageMemory` and `bufferSizePercent` comes from the
 * per-shuffle [[StreamingShuffleHandle]]. The documented per-partition budget is
 * `perPartitionBudget = streamingMemoryBudget / numPartitions`. Spilling is triggered proactively
 * once resident bytes reach `spillThreshold`% of the total budget, and reactively whenever the
 * [[org.apache.spark.memory.TaskMemoryManager]] asks the composed consumer to release memory.
 *
 * '''Zero data loss.''' Records are never silently dropped: every record is routed to exactly one
 * partition buffer and its serialized length is accounted in [[getPartitionLengths]]. Buffers and
 * all granted execution memory are released on both the success and failure paths in `stop`'s
 * `finally` block. On the failure path the spilled temporary blocks are deleted so that the DAG
 * scheduler's normal recomputation regenerates the output cleanly.
 *
 * '''v1 data plane.''' In this version the on-the-wire data plane is a logging-only stub (feature
 * F-115, `StreamingShuffleTransport`); the writer still produces the canonical 2 MiB
 * [[StreamingBlockEnvelope]] frames (with their per-block CRC32C) so the consumer's per-block
 * validation works once the Netty data plane lands, but it does not itself transmit them. The
 * authoritative output of `stop` is the [[MapStatus]] describing per-partition sizes, the map
 * task location, and an aggregate CRC32C over all serialized bytes.
 *
 * Instances are single-threaded with respect to [[write]]; the composed consumer's `spill` is
 * invoked synchronously on the same task thread by the `TaskMemoryManager`.
 *
 * @param handle       the streaming shuffle handle carrying the dependency and tuning parameters
 * @param mapId        the unique id of this shuffle map task
 * @param context      the task context, used to obtain the task memory manager
 * @param writeMetrics sink for bytes/records/time write metrics
 * @param config       the resolved streaming shuffle configuration (used for debug logging)
 */
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    config: StreamingShuffleConfig)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private val shuffleId = dep.shuffleId

  private val partitioner = dep.partitioner

  private val numPartitions = partitioner.numPartitions

  /** A fresh serializer instance for this task (mirrors how the sort path serializes records). */
  private val serializerInstance = dep.serializer.newInstance()

  /**
   * Per-shuffle tuning captured at registration time. The [[StreamingShuffleHandle]] is the
   * authoritative source for these values; [[config]] holds the same effective settings and is
   * used for debug logging.
   */
  private val bufferSizePercent = handle.bufferSizePercent
  private val spillThreshold = handle.spillThreshold

  /**
   * Executor memory ceiling used as the denominator of the streaming buffer budget. Read-only
   * snapshot of the active [[org.apache.spark.memory.MemoryManager]]'s maximum on-heap storage
   * memory; the writer never mutates the memory manager.
   */
  private val executorMemoryBytes = SparkEnv.get.memoryManager.maxOnHeapStorageMemory

  /** Total budget, in bytes, for all per-partition streaming buffers. */
  val streamingMemoryBudget: Long = (executorMemoryBytes * bufferSizePercent) / 100L

  /**
   * Per-partition budget, in bytes: `(executorMemory * bufferSizePercent / 100) / numPartitions`.
   * Exposed for observability and testing.
   */
  val perPartitionBudget: Long =
    if (numPartitions > 0) streamingMemoryBudget / numPartitions else streamingMemoryBudget

  /** Resident-byte threshold at which a proactive spill of the largest buffer is triggered. */
  private val spillTriggerBytes: Long = (streamingMemoryBudget * spillThreshold) / 100L

  /** Per-partition output lengths, accumulated as serialized bytes flow through the writer. */
  private val partitionLengths = new Array[Long](numPartitions)

  /** Per-partition buffers, created lazily so empty partitions stay zero-length. */
  private val buffers = new Array[StreamingBuffer](numPartitions)

  /** Per-partition serialization streams, created lazily alongside their buffers. */
  private val serStreams = new Array[SerializationStream](numPartitions)

  /**
   * Aggregate CRC32C over every serialized byte, in write order. Used as the [[MapStatus]]
   * checksum so map-task retries that produce identical output yield an identical value. Mutated
   * only on the single task thread via [[BufferBackedOutputStream]].
   */
  private val aggregateChecksum = new CRC32C()

  /** Registry of buffers spilled to local disk, retained so they can be cleaned up on failure. */
  private val spilledSegments = new ArrayBuffer[SpilledSegment]()

  /** Composed memory consumer that lets this writer participate in cooperative spilling. */
  private val memoryConsumer = new StreamingShuffleMemoryConsumer(context.taskMemoryManager())

  // Running totals, all mutated only on the task thread.
  private var totalBytesWritten = 0L
  private var totalRecordsWritten = 0L
  private var blocksGenerated = 0
  private var wireBytesGenerated = 0L
  private var spillCount = 0
  private var spilledBytesTotal = 0L

  // Are we in the process of stopping? Map tasks can call stop(success = true) and then
  // stop(success = false) on a later exception, so guard against double cleanup.
  private var stopping = false
  private var released = false
  private var mapStatus: MapStatus = null

  if (config.debug) {
    logDebug(s"StreamingShuffleWriter(shuffle=$shuffleId, map=$mapId): numPartitions=" +
      s"$numPartitions, executorMemory=$executorMemoryBytes, bufferSizePercent=" +
      s"$bufferSizePercent, streamingMemoryBudget=$streamingMemoryBudget, " +
      s"perPartitionBudget=$perPartitionBudget, spillThreshold=$spillThreshold " +
      s"(spillTriggerBytes=$spillTriggerBytes)")
  }

  /**
   * Write all records produced by this map task. Each record is routed to its reduce partition,
   * serialized into that partition's buffer, and the buffers are periodically checked for
   * spilling. After all records are consumed the buffers are framed into 2 MiB blocks, the write
   * metrics are published, and the [[MapStatus]] is built.
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val startNanos = System.nanoTime()
    var recordsSinceSpillCheck = 0
    while (records.hasNext) {
      val record = records.next()
      val key: Any = record._1
      val value: Any = record._2
      val partitionId = partitioner.getPartition(key)
      val stream = serStreamFor(partitionId)
      stream.writeKey(key)
      stream.writeValue(value)
      totalRecordsWritten += 1
      recordsSinceSpillCheck += 1
      if (recordsSinceSpillCheck >= StreamingShuffleWriter.SPILL_CHECK_RECORD_INTERVAL) {
        recordsSinceSpillCheck = 0
        maybeSpill()
      }
    }
    // Flush so every serialized byte has landed in its buffer before we frame and measure.
    flushAllStreams()
    pipelineBlocks()
    writeMetrics.incRecordsWritten(totalRecordsWritten)
    writeMetrics.incBytesWritten(totalBytesWritten)
    writeMetrics.incWriteTime(System.nanoTime() - startNanos)
    mapStatus = MapStatus(
      blockManager.shuffleServerId, partitionLengths, mapId, aggregateChecksum.getValue())
    if (config.debug) {
      logDebug(s"StreamingShuffleWriter(shuffle=$shuffleId, map=$mapId) wrote " +
        s"$totalRecordsWritten records / $totalBytesWritten bytes across $numPartitions " +
        s"partitions; generated $blocksGenerated blocks ($wireBytesGenerated wire bytes), " +
        s"spilled $spillCount time(s) / $spilledBytesTotal bytes")
    }
  }

  /**
   * Close this writer, returning the [[MapStatus]] on success. Mirrors the sort writer's
   * double-stop guard and always releases buffers and granted execution memory in the `finally`
   * block. On the failure path the spilled temporary blocks are deleted.
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      } else {
        stopping = true
        if (success) {
          Option(mapStatus)
        } else {
          None
        }
      }
    } finally {
      releaseResources(deleteSpilled = !success)
    }
  }

  /** Per-partition output lengths in bytes. */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  // ---------------------------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * Lazily create (on first use) the serialization stream for `partitionId`, along with its
   * backing [[StreamingBuffer]]. Creating streams lazily avoids writing a serializer header into
   * partitions that never receive a record, keeping their reported length at zero.
   */
  private def serStreamFor(partitionId: Int): SerializationStream = {
    if (serStreams(partitionId) == null) {
      buffers(partitionId) =
        new StreamingBuffer(partitionId, StreamingShuffleWriter.INITIAL_BUFFER_CAPACITY)
      val out = new BufferBackedOutputStream(partitionId)
      serStreams(partitionId) = serializerInstance.serializeStream(out)
    }
    serStreams(partitionId)
  }

  /** Flush every open serialization stream so its bytes are visible in the backing buffer. */
  private def flushAllStreams(): Unit = {
    var i = 0
    while (i < numPartitions) {
      val s = serStreams(i)
      if (s != null) {
        s.flush()
      }
      i += 1
    }
  }

  /** Sum of the currently resident (not-yet-spilled) bytes across all partition buffers. */
  private def totalResidentBytes(): Long = {
    var sum = 0L
    var i = 0
    while (i < numPartitions) {
      val b = buffers(i)
      if (b != null) {
        sum += b.size
      }
      i += 1
    }
    sum
  }

  /**
   * Keep the task memory manager's accounting in step with resident buffer bytes and, when the
   * proactive `spillThreshold` is crossed, spill the largest buffers back down toward the
   * threshold. Reserving memory may itself trigger a cooperative spill, so the resident total is
   * recomputed before the proactive decision.
   */
  private def maybeSpill(): Unit = {
    if (streamingMemoryBudget > 0L) {
      memoryConsumer.reserve(totalResidentBytes())
      val resident = totalResidentBytes()
      if (resident >= spillTriggerBytes) {
        val freed = spillLargestBuffers(math.max(resident - spillTriggerBytes, 1L))
        memoryConsumer.release(freed)
      }
    }
  }

  /**
   * Spill the largest resident buffers to local disk until at least `targetFreeBytes` have been
   * freed or no resident buffer remains. Returns the number of bytes freed.
   */
  private def spillLargestBuffers(targetFreeBytes: Long): Long = {
    var freed = 0L
    var continue = true
    while (continue && freed < targetFreeBytes) {
      largestResidentBuffer() match {
        case Some(buf) => freed += spillBuffer(buf)
        case None => continue = false
      }
    }
    freed
  }

  /** The largest resident, non-empty buffer, or `None` if every buffer is empty. */
  private def largestResidentBuffer(): Option[StreamingBuffer] = {
    var best: StreamingBuffer = null
    var i = 0
    while (i < numPartitions) {
      val b = buffers(i)
      if (b != null && b.size > 0L && (best == null || b.size > best.size)) {
        best = b
      }
      i += 1
    }
    Option(best)
  }

  /**
   * Spill a single buffer to a temporary local block, preserving its bytes on disk before
   * resetting it to reclaim heap. The spilled segment is recorded so it can be deleted if the map
   * task ultimately fails. Returns the number of bytes freed.
   */
  private def spillBuffer(buffer: StreamingBuffer): Long = {
    val snap = buffer.snapshot()
    if (snap.size <= 0L) {
      0L
    } else {
      val (blockId, file) = blockManager.diskBlockManager.createTempLocalBlock()
      val out = new FileOutputStream(file)
      try {
        out.write(snap.bytes)
      } finally {
        out.close()
      }
      spilledSegments +=
        SpilledSegment(buffer.partitionId, blockId, file, snap.size, snap.checksum)
      buffer.reset()
      spillCount += 1
      spilledBytesTotal += snap.size
      if (config.debug) {
        logDebug(s"StreamingShuffleWriter(shuffle=$shuffleId, map=$mapId) spilled partition " +
          s"${buffer.partitionId} (${snap.size} bytes) to ${file.getName}")
      }
      snap.size
    }
  }

  /** Frame every resident partition buffer into 2 MiB, CRC32C-protected block envelopes. */
  private def pipelineBlocks(): Unit = {
    var p = 0
    while (p < numPartitions) {
      val buf = buffers(p)
      if (buf != null && buf.size > 0L) {
        pipelinePartition(p, buf.snapshot().bytes)
      }
      p += 1
    }
  }

  /**
   * Frame a single partition's bytes into consecutive blocks of at most
   * [[StreamingShuffleWriter.BLOCK_SIZE]] bytes, encoding each into a [[StreamingBlockEnvelope]]
   * (which computes the per-block CRC32C and prepends the 32-byte header) and handing it to the
   * v1 transport.
   */
  private def pipelinePartition(partitionId: Int, bytes: Array[Byte]): Unit = {
    val total = bytes.length
    var offset = 0
    while (offset < total) {
      val len = math.min(StreamingShuffleWriter.BLOCK_SIZE, total - offset)
      val payload =
        if (offset == 0 && len == total) bytes
        else java.util.Arrays.copyOfRange(bytes, offset, offset + len)
      val wire = StreamingBlockEnvelope.encode(shuffleId, mapId, partitionId, payload)
      transmitBlock(partitionId, wire.remaining())
      offset += len
    }
  }

  /**
   * v1 data-plane hand-off. The on-the-wire transport is intentionally a logging-only stub in
   * this version (feature F-115, `StreamingShuffleTransport`): the encoded envelope remains
   * resident in its per-partition buffer (or has been spilled to disk) and is advertised through
   * the returned [[MapStatus]]. When the Netty data plane lands, the envelope is handed to the
   * transport here, gated by the backpressure protocol; the v1 path performs no blocking work.
   */
  private def transmitBlock(partitionId: Int, wireSize: Int): Unit = {
    blocksGenerated += 1
    wireBytesGenerated += wireSize
    if (config.debug) {
      logTrace(s"StreamingShuffleWriter(shuffle=$shuffleId, map=$mapId) framed block for " +
        s"partition $partitionId ($wireSize wire bytes; $blocksGenerated blocks so far)")
    }
  }

  /**
   * Release all resources held by this writer. Idempotent: safe to call from a double `stop`. The
   * serialization streams are closed, every buffer is reset and dereferenced, and all granted
   * execution memory is returned. When `deleteSpilled` is true (the failure path) the spilled
   * temporary files are deleted so recomputation starts from a clean slate.
   */
  private def releaseResources(deleteSpilled: Boolean): Unit = {
    if (!released) {
      released = true
      var i = 0
      while (i < numPartitions) {
        val s = serStreams(i)
        if (s != null) {
          try {
            s.close()
          } catch {
            case NonFatal(e) =>
              logWarning(s"Error closing streaming serialization stream for partition $i", e)
          }
          serStreams(i) = null
        }
        val b = buffers(i)
        if (b != null) {
          b.reset()
          buffers(i) = null
        }
        i += 1
      }
      memoryConsumer.releaseAll()
      if (deleteSpilled) {
        spilledSegments.foreach { seg =>
          if (seg.file.exists()) {
            seg.file.delete()
          }
        }
      }
      spilledSegments.clear()
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Test / observability accessors
  // ---------------------------------------------------------------------------------------------

  /** Number of times a buffer has been spilled to disk by this writer. */
  private[spark] def numSpills: Int = spillCount

  /** Total bytes spilled to disk by this writer. */
  private[spark] def spilledBytes: Long = spilledBytesTotal

  /** Number of 2 MiB block envelopes framed by this writer. */
  private[spark] def numBlocksGenerated: Int = blocksGenerated

  // ---------------------------------------------------------------------------------------------
  // Composed memory consumer
  // ---------------------------------------------------------------------------------------------

  /**
   * Private [[MemoryConsumer]] that registers this writer's streaming buffers with the task's
   * [[TaskMemoryManager]]. Growing the buffers acquires execution memory through [[reserve]];
   * spilling (proactive or manager-triggered) releases it. The manager-triggered [[spill]]
   * mirrors `o.a.s.util.collection.Spillable`: it only responds to requests from a different
   * consumer while running on-heap and never calls `acquireMemory` from within `spill`.
   */
  private final class StreamingShuffleMemoryConsumer(tmm: TaskMemoryManager)
    extends MemoryConsumer(tmm, MemoryMode.ON_HEAP) {

    /** Grow granted execution memory so it covers `target` resident bytes (best-effort). */
    def reserve(target: Long): Unit = {
      val deficit = target - getUsed()
      if (deficit > 0L) {
        // acquireMemory may grant less than requested under memory pressure; that shortfall is
        // itself a signal handled by the proactive spill check in maybeSpill().
        acquireMemory(deficit)
      }
    }

    /** Release up to `bytes` of previously granted execution memory. */
    def release(bytes: Long): Unit = {
      val toFree = math.min(bytes, getUsed())
      if (toFree > 0L) {
        freeMemory(toFree)
      }
    }

    /** Release all granted execution memory. Used during writer close. */
    def releaseAll(): Unit = {
      val used = getUsed()
      if (used > 0L) {
        freeMemory(used)
      }
    }

    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
        val freed = spillLargestBuffers(size)
        if (freed > 0L) {
          freeMemory(math.min(freed, getUsed()))
        }
        freed
      } else {
        0L
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Output stream that bridges a SerializationStream to a StreamingBuffer
  // ---------------------------------------------------------------------------------------------

  /**
   * An [[OutputStream]] that appends everything written to it directly into a partition's
   * [[StreamingBuffer]] (no intermediate copy), while accumulating that partition's output
   * length, the aggregate CRC32C, and the running total of bytes written. Used as the sink of a
   * per-partition [[SerializationStream]].
   */
  private final class BufferBackedOutputStream(partitionId: Int) extends OutputStream {

    private val oneByte = new Array[Byte](1)

    override def write(b: Int): Unit = {
      oneByte(0) = b.toByte
      write(oneByte, 0, 1)
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      buffers(partitionId).append(b, off, len)
      partitionLengths(partitionId) += len
      aggregateChecksum.update(b, off, len)
      totalBytesWritten += len
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Spilled-segment bookkeeping
  // ---------------------------------------------------------------------------------------------

  /**
   * Record of a partition buffer that was spilled to a temporary local block.
   *
   * @param partitionId the reduce partition the spilled bytes belong to
   * @param blockId     the temporary local block id allocated for the spill
   * @param file        the on-disk file backing the temporary block
   * @param length      the number of bytes spilled
   * @param checksum    the CRC32C of the spilled bytes, for integrity validation
   */
  private case class SpilledSegment(
      partitionId: Int,
      blockId: TempLocalBlockId,
      file: File,
      length: Long,
      checksum: Long)
}

private[spark] object StreamingShuffleWriter {

  /**
   * Canonical streaming block size: 2 MiB. A partition's accumulated bytes are framed into blocks
   * of at most this many bytes, matching [[StreamingBlockEnvelope.MAX_PAYLOAD_SIZE]] so each
   * block fits in a single self-describing wire envelope.
   */
  val BLOCK_SIZE: Int = 2 * 1024 * 1024

  /**
   * Initial capacity, in bytes, of each per-partition [[StreamingBuffer]]. Kept small so that
   * empty or lightly used partitions impose negligible heap overhead; the buffer grows on demand.
   */
  val INITIAL_BUFFER_CAPACITY: Int = 64 * 1024

  /**
   * Number of records written between successive spill checks. Bounding the check frequency keeps
   * the per-record write path cheap while still reacting promptly to buffer growth.
   */
  val SPILL_CHECK_RECORD_INTERVAL: Int = 1024
}
