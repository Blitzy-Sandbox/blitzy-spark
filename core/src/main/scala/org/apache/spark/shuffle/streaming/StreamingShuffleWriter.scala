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

import java.io.{BufferedOutputStream, FileOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32C

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter,
  ShuffleWriter}
import org.apache.spark.shuffle.streaming.MemorySpillManager.{BufferKey, SpilledSegment}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{ShuffleBlockId, StorageLevel, TempLocalBlockId}
import org.apache.spark.util.io.ChunkedByteBuffer

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
 * Implements [[ShuffleWriter]] and '''composes''' a private inner [[MemoryConsumer]]
 * ([[StreamingShuffleMemoryConsumer]]) to participate in Spark's cooperative spilling protocol.
 * The rationale for composition over inheritance is recorded in the decision log (ADR-03).
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
 * '''Zero data loss.''' Records are never silently dropped: every record is routed to exactly
 * one partition buffer, and at commit each partition's complete byte stream is reconstructed by
 * concatenating its spilled segments (in spill order) ahead of its final resident bytes, so
 * spilled bytes are published rather than discarded. Buffers and all granted execution memory are
 * released on both the success and failure paths in `stop`'s `finally` block, and the transient
 * spill blocks are always removed. On the failure path any committed output is removed so the DAG
 * scheduler's normal recomputation regenerates it cleanly.
 *
 * '''Publication (v1 data plane).''' The writer publishes through the standard fetchable channel
 * (the on-the-wire data plane is a logging-only stub in v1; see decision log ADR-04, F-115): it
 * frames every partition into the canonical 2 MiB [[StreamingBlockEnvelope]] frames (each with
 * its per-block CRC32C), writes those frames sequentially to a single temporary data file, and
 * commits the file and its per-partition index atomically via the shared
 * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]]. After the commit a reducer fetches a
 * partition through the standard `MapOutputTracker` + `BlockTransferService` path and decodes the
 * frames. The [[MapStatus]] returned from `stop` therefore describes the actual on-disk framed
 * per-partition sizes, the map-task location, and an aggregate CRC32C over all serialized bytes.
 *
 * Instances are single-threaded with respect to [[write]]; the composed consumer's `spill` is
 * invoked synchronously on the same task thread by the `TaskMemoryManager`.
 *
 * @param handle         the streaming shuffle handle carrying the dependency and tuning
 *                       parameters
 * @param mapId          the unique id of this shuffle map task
 * @param context        the task context, used to obtain the task memory manager
 * @param writeMetrics   sink for bytes/records/time write metrics
 * @param config         the resolved streaming shuffle configuration (used for debug logging)
 * @param indexResolver  the shared block resolver used to commit the framed data file and its
 *                       per-partition index so reducers can fetch the output; defaults to the
 *                       executor's active shuffle block resolver
 * @param spillManagerOpt the optional executor-wide [[MemorySpillManager]]; when present, this
 *                       writer registers its buffers with it so the 100 ms utilization monitor
 *                       can protect executor memory and routes its own spills through the same
 *                       ledger, and when absent the writer spills to disk through a writer-local
 *                       ledger
 */
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    config: StreamingShuffleConfig,
    indexResolver: IndexShuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
    spillManagerOpt: Option[MemorySpillManager] = None)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private val shuffleId = dep.shuffleId

  private val partitioner = dep.partitioner

  private val numPartitions = partitioner.numPartitions

  /** A fresh serializer instance for this task (mirrors how the sort path serializes records). */
  private val serializerInstance = dep.serializer.newInstance()

  /**
   * Wraps each partition's output stream for compression and encryption exactly as the sort path
   * does, so streaming output is byte-compatible with the symmetric unwrap on the consumer side
   * ([[StreamingShuffleReader]] mirrors [[org.apache.spark.shuffle.BlockStoreShuffleReader]],
   * which wraps every fetched block with `wrapStream`). Omitting this would corrupt reads
   * whenever shuffle compression or encryption is enabled (both honor the existing
   * `spark.shuffle.*` settings; compression is on by default).
   */
  private val serializerManager: SerializerManager = SparkEnv.get.serializerManager

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

  /**
   * Per-partition raw serialized output lengths, accumulated as bytes flow through the writer.
   * These are the un-framed byte counts; the authoritative published lengths (which include the
   * per-block envelope headers) are captured in [[publishedLengths]] at commit time.
   */
  private val partitionLengths = new Array[Long](numPartitions)

  /**
   * Per-partition framed (on-the-wire) output lengths, populated at commit time. Each entry is
   * the total number of bytes the partition occupies in the committed data file (the sum of its
   * envelope header + payload bytes), so it is consistent with the offsets recorded in the index
   * file and with the block sizes advertised in the returned [[MapStatus]].
   */
  private val publishedLengths = new Array[Long](numPartitions)

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

  /**
   * Writer-local spill ledger, used only when no executor-wide [[MemorySpillManager]] is injected
   * (for example in unit tests). Maps a reduce partition id to the ordered DISK_ONLY segments its
   * buffer was spilled to. When a [[MemorySpillManager]] is present, the manager owns the ledger
   * instead and this map stays empty. The segment order is the on-the-wire byte order and is read
   * back at commit time ahead of the buffer's resident bytes.
   */
  private val localLedger = new HashMap[Int, ArrayBuffer[SpilledSegment]]()

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
  // Set once the framed data file and its index have been committed via the index resolver, so
  // the failure-path cleanup knows whether a committed map output must be removed.
  private var committed = false

  if (config.debug) {
    logDebug(log"StreamingShuffleWriter(shuffle=${MDC(SHUFFLE_ID, shuffleId)}, " +
      log"map=${MDC(MAP_ID, mapId)}): numPartitions=${MDC(NUM_PARTITIONS, numPartitions)}, " +
      log"executorMemory=${MDC(MEMORY_SIZE, executorMemoryBytes)}, " +
      log"bufferSizePercent=${MDC(PERCENT, bufferSizePercent)}, " +
      log"streamingMemoryBudget=${MDC(STORAGE_MEMORY_SIZE, streamingMemoryBudget)}, " +
      log"perPartitionBudget=${MDC(NUM_BYTES, perPartitionBudget)}, " +
      log"spillThreshold=${MDC(THRESHOLD, spillThreshold)} " +
      log"(spillTriggerBytes=${MDC(NUM_BYTES_TO_WARN, spillTriggerBytes)})")
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
    // Close every serialization stream so each partition's complete byte stream (including any
    // serializer trailer) has landed in its buffer before we frame and commit.
    finishAllStreams()
    // Frame each partition (spilled segments in order, then resident bytes) into <= 2 MiB
    // envelopes, write them to a single temporary data file, and commit it and its per-partition
    // index so the output is fetchable through the standard MapOutputTracker +
    // BlockTransferService path.
    frameAndCommitOutput()
    writeMetrics.incRecordsWritten(totalRecordsWritten)
    writeMetrics.incBytesWritten(totalBytesWritten)
    writeMetrics.incWriteTime(System.nanoTime() - startNanos)
    mapStatus = MapStatus(
      blockManager.shuffleServerId, publishedLengths, mapId, aggregateChecksum.getValue())
    if (config.debug) {
      logDebug(log"StreamingShuffleWriter wrote map output " +
        log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
        log"attempt=${MDC(TASK_ATTEMPT_ID, context.taskAttemptId())}: " +
        log"records=${MDC(RECORDS, totalRecordsWritten)} / " +
        log"${MDC(NUM_BYTES, totalBytesWritten)} bytes across " +
        log"${MDC(NUM_PARTITIONS, numPartitions)} partitions; generated " +
        log"${MDC(NUM_BLOCKS, blocksGenerated)} blocks " +
        log"(${MDC(NUM_BYTES_CURRENT, wireBytesGenerated)} wire bytes); spilled " +
        log"${MDC(NUM_SPILLS, spillCount)} time(s) / " +
        log"${MDC(NUM_BYTES_USED, spilledBytesTotal)} bytes")
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
      releaseResources(success = success)
    }
  }

  /**
   * Per-partition output lengths in bytes. After the output has been committed this returns the
   * framed on-disk lengths ([[publishedLengths]]), which are consistent with the block sizes
   * advertised in the [[MapStatus]] and the offsets in the committed index; before commit it
   * returns the raw serialized lengths accumulated during writing.
   */
  override def getPartitionLengths(): Array[Long] =
    if (committed) publishedLengths else partitionLengths

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
      val buffer = new StreamingBuffer(
        partitionId, StreamingShuffleWriter.INITIAL_BUFFER_CAPACITY)
      buffers(partitionId) = buffer
      // Register the buffer with the executor-wide spill monitor (when present) so its
      // utilization is accounted and it is eligible for the 100 ms threshold spill (features
      // F-109 / wiring of F-103 buffers into the monitor). The manager and this writer share one
      // spill ledger keyed by the same BufferKey, so spills triggered by either path remain
      // correctly ordered.
      spillManagerOpt.foreach(_.registerBuffer(bufferKey(partitionId), buffer))
      // Wrap the buffer-backed sink for compression and encryption (keyed by the reduce
      // partition's shuffle block id) before serializing, symmetric with the consumer's
      // `serializerManager.wrapStream` unwrap. The wrapped stream's trailer is flushed when the
      // serialization stream is closed in `finishAllStreams`, so the framed bytes are complete.
      val out = new BufferBackedOutputStream(partitionId)
      val wrapped =
        serializerManager.wrapStream(ShuffleBlockId(shuffleId, mapId, partitionId), out)
      serStreams(partitionId) = serializerInstance.serializeStream(wrapped)
    }
    serStreams(partitionId)
  }

  /**
   * The shared spill-ledger / registry key identifying this map task's buffer for `partitionId`.
   */
  private def bufferKey(partitionId: Int): BufferKey = BufferKey(shuffleId, mapId, partitionId)

  /**
   * Close every open serialization stream so its complete byte stream (including any serializer
   * trailer written only on close) is visible in the backing buffer, then null the slot so the
   * idempotent [[releaseResources]] does not attempt to close it again. Closing the
   * [[SerializationStream]] does not close the underlying [[StreamingBuffer]], which remains
   * readable for framing.
   */
  private def finishAllStreams(): Unit = {
    var i = 0
    while (i < numPartitions) {
      val s = serStreams(i)
      if (s != null) {
        s.close()
        serStreams(i) = null
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
        case Some(buf) => freed += spillOnePartition(buf.partitionId, buf)
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
   * Spill a single partition's buffer to a DISK_ONLY block, releasing its heap, and record the
   * resulting segment in the shared ledger. Routes through the injected [[MemorySpillManager]]
   * when present (so the manager and writer share one ordered ledger) or through the writer-local
   * ledger otherwise. The drain, store and heap release happen atomically under the buffer's
   * monitor via [[StreamingBuffer#spillUnderLock]], so a buffer that has been finalized for
   * commit or is empty is never spilled and a failed store loses no data. Returns the number of
   * heap bytes freed.
   *
   * @param partitionId the reduce partition whose buffer is being spilled
   * @param buffer      the buffer to spill
   * @return the number of bytes freed on a successful spill, otherwise 0
   */
  private def spillOnePartition(partitionId: Int, buffer: StreamingBuffer): Long = {
    val freed = spillManagerOpt match {
      case Some(mgr) => mgr.spillBuffer(bufferKey(partitionId), buffer)
      case None => localSpillStore(partitionId, buffer)
    }
    if (freed > 0L) {
      spillCount += 1
      spilledBytesTotal += freed
      if (config.debug) {
        logDebug(log"StreamingShuffleWriter spilled partition " +
          log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
          log"partition=${MDC(PARTITION_ID, partitionId)} (${MDC(NUM_BYTES, freed)} bytes) " +
          log"to a DISK_ONLY block")
      }
    }
    freed
  }

  /**
   * Drain a partition's buffer to a DISK_ONLY [[org.apache.spark.storage.BlockManager]] block and
   * append the segment to the writer-local ledger. Used only when no [[MemorySpillManager]] is
   * injected. The atomic drain/store/reset is delegated to [[StreamingBuffer#spillUnderLock]].
   *
   * @param partitionId the reduce partition whose buffer is being spilled
   * @param buffer      the buffer to spill
   * @return the number of heap bytes released on a successful store, otherwise 0
   */
  private def localSpillStore(partitionId: Int, buffer: StreamingBuffer): Long = {
    buffer.spillUnderLock { snapshot =>
      try {
        val blockId = TempLocalBlockId(UUID.randomUUID())
        val stored = blockManager.putBytes(
          blockId,
          new ChunkedByteBuffer(ByteBuffer.wrap(snapshot.bytes)),
          StorageLevel.DISK_ONLY,
          tellMaster = false)
        if (stored) {
          localLedger.getOrElseUpdate(partitionId, new ArrayBuffer[SpilledSegment]())
            .append(SpilledSegment(blockId, snapshot.size, snapshot.checksum))
          true
        } else {
          logWarning(log"Failed to spill streaming buffer to disk; retaining it in memory: " +
            log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
            log"partition=${MDC(PARTITION_ID, partitionId)}")
          false
        }
      } catch {
        case NonFatal(t) =>
          logWarning(log"Error spilling streaming buffer to disk; retaining it in memory: " +
            log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
            log"partition=${MDC(PARTITION_ID, partitionId)}", t)
          false
      }
    }
  }

  /**
   * The ordered spilled segments for `partitionId`, oldest spill first, read from the shared
   * [[MemorySpillManager]] ledger when present or the writer-local ledger otherwise.
   *
   * @param partitionId the reduce partition whose spilled segments to retrieve
   * @return the spilled segments in spill (on-the-wire) order
   */
  private def spilledSegmentsFor(partitionId: Int): Seq[SpilledSegment] = {
    spillManagerOpt match {
      case Some(mgr) => mgr.spilledSegmentsFor(bufferKey(partitionId))
      case None => localLedger.get(partitionId).map(_.toSeq).getOrElse(Seq.empty)
    }
  }

  /**
   * Frame every partition's complete byte stream into at most 2 MiB CRC32C-protected envelopes,
   * write them sequentially to a single temporary data file, and commit that file together with
   * its per-partition index through the shared [[IndexShuffleBlockResolver]]. After this returns
   * the map output is fetchable by reducers through the standard `MapOutputTracker` +
   * `BlockTransferService` path, and [[publishedLengths]] holds the framed per-partition byte
   * counts. The commit is atomic: a partially written temp file is deleted unless the commit
   * succeeds.
   */
  private def frameAndCommitOutput(): Unit = {
    val dataFile = indexResolver.getDataFile(shuffleId, mapId)
    val dataTmp = indexResolver.createTempFile(dataFile)
    val out = new BufferedOutputStream(new FileOutputStream(dataTmp))
    var closed = false
    try {
      var p = 0
      while (p < numPartitions) {
        publishedLengths(p) = framePartition(p, out)
        p += 1
      }
      out.flush()
      out.close()
      closed = true
      indexResolver.writeMetadataFileAndCommit(
        shuffleId, mapId, publishedLengths, Array.empty[Long], dataTmp)
      committed = true
      if (config.debug) {
        logDebug(log"StreamingShuffleWriter committed fetchable map output " +
          log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)}: " +
          log"${MDC(NUM_BLOCKS, blocksGenerated)} framed block(s) / " +
          log"${MDC(NUM_BYTES, wireBytesGenerated)} wire bytes across " +
          log"${MDC(NUM_PARTITIONS, numPartitions)} partitions")
      }
    } finally {
      if (!closed) {
        try {
          out.close()
        } catch {
          case NonFatal(e) =>
            logWarning(log"Error closing streaming data temp file " +
              log"${MDC(TEMP_FILE, dataTmp)} for shuffle=${MDC(SHUFFLE_ID, shuffleId)} " +
              log"map=${MDC(MAP_ID, mapId)}", e)
        }
      }
      if (!committed && dataTmp.exists()) {
        dataTmp.delete()
      }
    }
  }

  /**
   * Frame a single partition into the data stream `out`: read each spilled segment back in spill
   * order, then the buffer's finalized resident bytes, slicing the concatenation into envelopes
   * of at most [[StreamingShuffleWriter.BLOCK_SIZE]] bytes (each encoded by
   * [[StreamingBlockEnvelope]] with its 32-byte header and per-block CRC32C). The buffer is
   * finalized BEFORE the ledger is read so no concurrent spill can append a segment after the
   * resident bytes are captured; the ledger is therefore complete and correctly ordered. Returns
   * the total framed (header + payload) byte count written for the partition, which is 0 for an
   * empty partition.
   */
  private def framePartition(partitionId: Int, out: OutputStream): Long = {
    val residentSnapshot = Option(buffers(partitionId)).map(_.finalizeForCommit())
    val segments = spilledSegmentsFor(partitionId)
    if (segments.isEmpty && residentSnapshot.forall(_.size <= 0L)) {
      0L
    } else {
      val framer = new PartitionFramer(partitionId, out)
      segments.foreach(seg => feedSegment(partitionId, seg, framer))
      residentSnapshot.foreach { snap =>
        if (snap.size > 0L) {
          framer.feed(snap.bytes, 0, snap.bytes.length)
        }
      }
      framer.finish()
      framer.framedBytes
    }
  }

  /**
   * Read a spilled segment's bytes back from its DISK_ONLY block and feed them through `framer`
   * in bounded chunks, so commit never re-materializes a whole partition on the heap. The block
   * read holds a read lock that is released (and the block data disposed) before returning. A
   * missing spilled block is unrecoverable for this map output, so it is raised as a
   * [[SparkException]]; the DAG scheduler then recomputes the map task cleanly (zero data loss).
   *
   * The read lock is released without an explicit `TaskContext`: `getLocalBytes` records the lock
   * under `BlockInfoManager.currentTaskAttemptId` (i.e. `TaskContext.get()`), so the matching
   * release must use the same basis. Passing an explicit context whose attempt id differs from
   * `TaskContext.get()` (as happens when no task is installed on the calling thread) would target
   * the wrong lock holder, leaving the read lock pinned and deadlocking the subsequent
   * `removeBlock` write-lock acquisition during cleanup.
   */
  private def feedSegment(
      partitionId: Int, segment: SpilledSegment, framer: PartitionFramer): Unit = {
    blockManager.getLocalBytes(segment.blockId) match {
      case Some(blockData) =>
        try {
          val in = blockData.toInputStream()
          try {
            val chunk = new Array[Byte](StreamingShuffleWriter.SEGMENT_READBACK_CHUNK)
            var read = in.read(chunk)
            while (read >= 0) {
              if (read > 0) {
                framer.feed(chunk, 0, read)
              }
              read = in.read(chunk)
            }
          } finally {
            in.close()
          }
        } finally {
          blockManager.releaseLockAndDispose(segment.blockId, blockData)
        }
      case None =>
        throw new SparkException(s"Streaming shuffle spilled block ${segment.blockId} for " +
          s"shuffle $shuffleId map $mapId partition $partitionId is missing; cannot " +
          s"reconstruct the map output")
    }
  }

  /**
   * Release all resources held by this writer. Idempotent: safe to call from a double `stop`. The
   * serialization streams are closed, every buffer's transient DISK_ONLY spill blocks are
   * removed, every buffer is reset (releasing its heap) and dereferenced (and deregistered from
   * the spill manager when one is present), and all granted execution memory is returned. On the
   * failure path (`success == false`) any already-committed map output is removed so the DAG
   * scheduler's recomputation starts from a clean slate.
   *
   * @param success whether the map task succeeded; on failure committed output is removed
   */
  private def releaseResources(success: Boolean): Unit = {
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
              logWarning(log"Error closing streaming serialization stream for " +
                log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
                log"partition ${MDC(PARTITION_ID, i)}", e)
          }
          serStreams(i) = null
        }
        cleanupPartitionSpillState(i)
        val b = buffers(i)
        if (b != null) {
          b.reset()
          buffers(i) = null
        }
        i += 1
      }
      localLedger.clear()
      memoryConsumer.releaseAll()
      if (!success && committed) {
        try {
          indexResolver.removeDataByMap(shuffleId, mapId)
        } catch {
          case NonFatal(e) =>
            logWarning(log"Error removing committed streaming map output for " +
              log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} after a " +
              log"failed map task", e)
        }
      }
    }
  }

  /**
   * Remove the transient DISK_ONLY spill blocks recorded for `partitionId` and release the
   * partition's buffer. When a [[MemorySpillManager]] is present this delegates to
   * [[MemorySpillManager#reclaim]] (which removes the blocks, resets the buffer, clears the
   * ledger and deregisters the buffer); otherwise the writer-local ledger's blocks are removed
   * directly.
   *
   * @param partitionId the reduce partition whose spill state to clean up
   */
  private def cleanupPartitionSpillState(partitionId: Int): Unit = {
    spillManagerOpt match {
      case Some(mgr) =>
        mgr.reclaim(bufferKey(partitionId))
      case None =>
        localLedger.get(partitionId).foreach { segments =>
          segments.foreach { seg =>
            try {
              blockManager.removeBlock(seg.blockId, tellMaster = false)
            } catch {
              case NonFatal(e) =>
                logWarning(log"Error removing spilled streaming block " +
                  log"${MDC(BLOCK_ID, seg.blockId)} for shuffle=${MDC(SHUFFLE_ID, shuffleId)} " +
                  log"map=${MDC(MAP_ID, mapId)} partition=${MDC(REDUCE_ID, partitionId)}", e)
            }
          }
        }
        localLedger.remove(partitionId)
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
  // Bounded partition framer
  // ---------------------------------------------------------------------------------------------

  /**
   * Slices a partition's byte stream into consecutive [[StreamingBlockEnvelope]] frames of at
   * most [[StreamingShuffleWriter.BLOCK_SIZE]] payload bytes and writes them to the data stream
   * `out`.
   *
   * Bytes are fed incrementally via [[feed]] (the producing caller streams spilled segments and
   * the resident snapshot through it), so at most one [[StreamingShuffleWriter.BLOCK_SIZE]] block
   * plus the input chunk is held on the heap at a time -- commit never re-materializes a whole
   * partition. A full block is emitted as soon as the staging buffer fills; [[finish]] flushes
   * the final partial block. Each emitted frame is encoded with its 32-byte header and per-block
   * CRC32C, and the running [[StreamingShuffleWriter.blocksGenerated]] / `wireBytesGenerated`
   * observability counters are advanced. Single-threaded; used only on the writer's task thread
   * at commit.
   *
   * @param partitionId the reduce partition being framed (stamped into each envelope header)
   * @param out         the data-file output stream the framed envelopes are written to
   */
  private final class PartitionFramer(partitionId: Int, out: OutputStream) {

    private val block = new Array[Byte](StreamingShuffleWriter.BLOCK_SIZE)
    private var fill = 0
    private var framed = 0L

    /**
     * Append `len` bytes from `src` at `off`, emitting full blocks as the staging buffer fills.
     */
    def feed(src: Array[Byte], off: Int, len: Int): Unit = {
      var pos = off
      var remaining = len
      while (remaining > 0) {
        val space = StreamingShuffleWriter.BLOCK_SIZE - fill
        val n = math.min(space, remaining)
        System.arraycopy(src, pos, block, fill, n)
        fill += n
        pos += n
        remaining -= n
        if (fill == StreamingShuffleWriter.BLOCK_SIZE) {
          emit(fill)
          fill = 0
        }
      }
    }

    /** Flush any buffered remainder as a final, smaller-than-full envelope. */
    def finish(): Unit = {
      if (fill > 0) {
        emit(fill)
        fill = 0
      }
    }

    /** Total framed (header + payload) bytes written for this partition. */
    def framedBytes: Long = framed

    private def emit(len: Int): Unit = {
      val payload = if (len == block.length) block else java.util.Arrays.copyOf(block, len)
      // encode copies payload into a fresh, flipped buffer, so reusing `block` afterward is safe.
      val wire = StreamingBlockEnvelope.encode(shuffleId, mapId, partitionId, payload)
      val arr = new Array[Byte](wire.remaining())
      wire.get(arr)
      out.write(arr)
      framed += arr.length
      blocksGenerated += 1
      wireBytesGenerated += arr.length.toLong
    }
  }
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

  /**
   * Chunk size, in bytes, used to read a spilled segment back from its DISK_ONLY block when
   * framing at commit. Reading in bounded chunks (rather than materializing an entire segment)
   * keeps commit memory bounded to roughly one block plus this chunk per partition.
   */
  val SEGMENT_READBACK_CHUNK: Int = 64 * 1024
}
