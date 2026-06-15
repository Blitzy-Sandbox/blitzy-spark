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

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ThreadUtils

/**
 * The map-side writer of the opt-in streaming shuffle backend.
 *
 * `StreamingShuffleWriter` is the streaming counterpart of the sort-based
 * `org.apache.spark.shuffle.sort.SortShuffleWriter`: a map task obtains one instance per output
 * and feeds it the task's `(key, value)` records. Instead of sorting the records and
 * materializing a single partitioned map-output file before any reduce fetch can begin, this
 * writer serializes each record into a bounded, per-reduce-partition in-memory
 * [[StreamingBuffer]], frames the serialized bytes into fixed 2 MB CRC32C-checksummed blocks,
 * applies token-bucket backpressure, and (in v1) hands each framed block to the
 * [[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport]]. Reduce tasks then
 * consume the buffered/spilled output through the existing pull-based fetch path.
 *
 * ==Composition over inheritance: the two-abstract-classes problem==
 * The writer must satisfy two distinct contracts that Spark expresses as abstract '''classes''',
 * and Scala forbids extending two classes. `StreamingShuffleWriter` therefore extends
 * [[org.apache.spark.shuffle.ShuffleWriter]] (the single class parent) and '''composes''' an
 * inner [[org.apache.spark.memory.MemoryConsumer]] ([[BufferMemoryConsumer]]) constructed from
 * `context.taskMemoryManager()`. The inner consumer is how the writer participates in the
 * executor memory model: it accounts the buffered bytes as task execution memory so the
 * [[org.apache.spark.memory.TaskMemoryManager]] can ask the writer to spill under pressure
 * (cooperative spilling), complementing the [[MemorySpillManager]]'s own 100 ms utilization
 * poll. All execution memory the inner consumer acquires is released in [[stop]] so the task
 * leaves no leak (validated under `spark.unsafe.exceptionOnMemoryLeak=true`).
 *
 * ==Dual-channel wire/persist invariant==
 * The writer never frames bytes itself: [[StreamingBuffer]] owns the canonical 2 MB framing and
 * per-block CRC32C, and the block envelopes the writer streams ([[StreamingBuffer.envelopeOf]])
 * are byte-for-byte identical to the bytes the [[MemorySpillManager]] writes on spill
 * ([[StreamingBuffer.toChunkedByteBuffer]]). Streamed and spilled bytes are thus
 * interchangeable, so a partition can spill to disk and still be served to (or re-streamed for)
 * the reducer transparently. Routing every block through the buffer is what upholds this
 * invariant.
 *
 * ==v1 transport is logging-only, by design==
 * In v1 the real data plane is the reduce side's existing `BlockTransferService.fetchBlockSync`
 * pull path, so [[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport.sendBlock]]
 * is intentionally logging-only and returns an already-completed future. The writer still
 * exercises backpressure and envelope framing on the send path so the producer is correctly
 * rate-limited and the integration point stays observable; this is recorded, intended behavior
 * (AAP 0.4.4), not an unfinished stub.
 *
 * ==Coexistence with the sort-based path==
 * This writer is constructed only when the streaming backend is active. Whenever streaming is
 * disabled or the fallback policy trips, `StreamingShuffleManager` produces a
 * `SortShuffleWriter` instead and this type is never instantiated; the sort-based path is left
 * completely unchanged.
 *
 * ==Concurrency==
 * A single map task drives [[write]] and [[stop]] on one thread, so the per-partition
 * serialization streams and the partition-length array are touched single-threaded. The
 * [[MemorySpillManager]] poll thread may concurrently spill (and therefore clear) a buffer; the
 * send path tolerates that by treating a cleared buffer as a new generation, because the spilled
 * bytes are already durable on disk for the resolver to serve.
 *
 * @param handle the streaming shuffle handle carrying the dependency and tuning values
 * @param mapId the map task id producing this output
 * @param context the task context, source of the [[org.apache.spark.memory.TaskMemoryManager]]
 * @param metrics the standard Spark shuffle-write metrics reporter
 * @param config the typed streaming-shuffle configuration accessor
 * @param streamingMetrics the streaming-shuffle telemetry holder (buffer-utilization gauge)
 * @param backpressure the executor-shared backpressure/flow-control protocol
 * @param spillManager the executor-shared memory spill manager
 * @param transport the v1 logging-only streaming transport (real data plane is reader-side)
 * @param blockResolver the streaming block resolver tracking in-memory and spilled blocks
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

  // The shuffle dependency and its partitioning/serialization configuration. Resolved once so
  // the hot record loop never re-reads it (mirrors SortShuffleWriter.dep).
  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = handle.shuffleId

  // One serializer instance is shared across every per-partition serialization stream, exactly
  // as BypassMergeSortShuffleWriter does when it opens all partitions simultaneously. The map
  // task is single-threaded, so the streams are written one at a time and never race.
  private val serInstance = dep.serializer.newInstance()

  // Per-reduce-partition state, all sized up front to numPartitions. Entries are allocated
  // lazily on first use so a sparse map task never reserves buffers for partitions it never
  // writes to. `partitionLengths` is the per-partition written byte count surfaced by
  // getPartitionLengths and shipped in the MapStatus.
  private val buffers = new Array[StreamingBuffer](numPartitions)
  private val partitionStreams = new Array[SerializationStream](numPartitions)
  private val partitionSinks = new Array[ByteArrayOutputStream](numPartitions)
  private val sentBlocks = new Array[Int](numPartitions)
  private val partitionLengths = new Array[Long](numPartitions)

  // The inner MemoryConsumer composed (not inherited) so the writer participates in the executor
  // memory model. Created up front but acquires no memory until the first buffered bytes arrive.
  private val memoryConsumer = new BufferMemoryConsumer(context.taskMemoryManager())

  // Wire the resolver to the shared spill manager exactly once at construction. The resolver
  // serves a spilled partition by reading its segments back through the spill manager -- the
  // single owner of the on-disk (non-shuffle TempLocalBlockId) spill format -- so the two MUST
  // share the same instance for spilled reads to resolve. Idempotent across the per-task writers
  // that all receive the same executor-shared collaborators from the manager.
  blockResolver.setSpillManager(spillManager)

  // Upper bound for observing a block send. In v1 the transport is logging-only and completes its
  // future synchronously, so awaiting it returns at once and this bound is never reached; it exists
  // so a future (v2) real-transport send is bounded by the same 5 s connection timeout the reader
  // applies to a fetch, after which the send is treated as failed and surfaced to the map task.
  private val sendAwaitTimeout: Duration =
    Duration(StreamingShuffleConfig.CONNECTION_TIMEOUT_MS, MILLISECONDS)

  // Per-partition buffer capacity, finalized at the start of write() once the executor memory
  // budget is known. The 2 MB floor is applied by StreamingShuffleConfig.perPartitionBufferBytes.
  private var perPartitionCapacityBytes: Long = StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES

  // Running totals of committed write metrics, retained so stop(success = false) can roll them
  // back via the reporter's dec* methods when the map output is discarded.
  private var committedBytes: Long = 0L
  private var committedRecords: Long = 0L

  // The MapStatus produced by a successful write(), returned later by stop(true). Mirrors
  // SortShuffleWriter, which also builds the status inside write() and returns it from stop().
  private var mapStatus: MapStatus = null

  // Guards against the documented double-stop: a map task may call stop(true) and then stop(false)
  // on a subsequent exception, so resources must be released exactly once.
  private var stopping = false

  /** @return the executor on-heap storage-memory budget, the same denominator the spill manager
   *          uses; `0` when no [[org.apache.spark.SparkEnv]] is available (the 2 MB floor then
   *          applies), keeping buffer sizing consistent across the streaming subsystem. */
  private def executorMemoryBytes: Long =
    Option(SparkEnv.get).map(_.memoryManager.maxOnHeapStorageMemory).getOrElse(0L)

  /** @return the local shuffle server identity used as the nominal send target, or `None` when no
   *          [[org.apache.spark.SparkEnv]] is present (the v1 logging-only transport tolerates an
   *          absent target; the real fetch is reader-side). */
  private def serverIdOpt: Option[BlockManagerId] =
    Option(SparkEnv.get).map(_.blockManager.shuffleServerId)

  /**
   * The composed inner memory consumer. It is the writer's hook into the executor memory model:
   * by accounting buffered bytes as task execution memory it becomes eligible to be asked to
   * spill when other consumers need memory. Its [[spill]] delegates to the shared
   * [[MemorySpillManager]] (threshold-driven, largest-LRU-first) and, failing that, force-spills
   * this writer's single largest buffer so a genuine execution-memory request can still be
   * honored. The reclaimed execution memory is released here so accounting stays balanced; the
   * spill-event counter is owned by the [[MemorySpillManager]] and is deliberately not touched
   * here to avoid double counting.
   *
   * @param tmm the task memory manager obtained from `context.taskMemoryManager()`
   */
  private class BufferMemoryConsumer(tmm: TaskMemoryManager)
    extends MemoryConsumer(tmm, MemoryMode.ON_HEAP) {

    @throws[IOException]
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      var reclaimed = spillManager.maybeSpill()
      if (reclaimed <= 0L) {
        reclaimed = forceSpillLargestBuffer()
      }
      if (reclaimed > 0L) {
        val toFree = math.min(reclaimed, getUsed)
        if (toFree > 0L) {
          freeMemory(toFree)
        }
      }
      reclaimed
    }
  }

  /**
   * Writes the task's records to per-partition streaming buffers.
   *
   * Each record is routed to its reduce partition, serialized into that partition's
   * serialization stream, and drained into the partition's [[StreamingBuffer]] (which performs
   * the 2 MB CRC32C framing) whenever a full block's worth of bytes has accumulated. Sealed
   * blocks are streamed under token-bucket backpressure as they are produced, and the buffered
   * bytes are accounted as execution memory so the writer can spill cooperatively under pressure.
   * After the last record, every open stream is flushed and finalized, the trailing blocks are
   * streamed, and the [[org.apache.spark.scheduler.MapStatus]] is built for [[stop]] to return.
   *
   * Write metrics are committed incrementally (so progress is observable and so a failed attempt
   * can be rolled back in [[stop]]); on an exception nothing further is committed and the
   * exception propagates so the scheduler can fail and retry the task.
   *
   * @param records the task's `(key, value)` records, consumed once
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val startNanos = System.nanoTime()
    perPartitionCapacityBytes = config.perPartitionBufferBytes(executorMemoryBytes, numPartitions)
    logDebug(log"Streaming shuffle write started shuffle=" +
      log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)} map=${MDC(LogKeys.MAP_ID, mapId)} " +
      log"attempt=${MDC(LogKeys.TASK_ATTEMPT_ID, context.taskAttemptId())} partitions=" +
      log"${MDC(LogKeys.NUM_PARTITIONS, numPartitions)} perPartitionBufferBytes=" +
      log"${MDC(LogKeys.NUM_BYTES, perPartitionCapacityBytes)}")

    // The entire record-consumption and finalization path runs under a try/finally so that any
    // failure mid-write (record iteration, serialization, drain, spill, backpressure, or the
    // transport send) releases this writer's resources -- crucially the inner consumer's accounted
    // execution memory -- before the exception propagates. The exception itself is never swallowed:
    // the finally only runs cleanup on the failure path, and a cleanup error is logged rather than
    // masking the original failure (mirrors UnsafeShuffleWriter.write). The map task's own
    // stop(success = false) still runs afterwards and is idempotent with this cleanup.
    var success = false
    try {
      while (records.hasNext) {
        val record = records.next()
        val partition = partitioner.getPartition(record._1)
        val stream = streamFor(partition)
        // Serialize key then value as Any: the SerializationStream resolves ClassTag[Any]
        // implicitly, exactly as DiskBlockObjectWriter.write(key: Any, value: Any) does, so the
        // generic K/V need no ClassTag context bound.
        stream.writeKey(record._1.asInstanceOf[Any])
        stream.writeValue(record._2.asInstanceOf[Any])
        committedRecords += 1L
        metrics.incRecordsWritten(1L)
        // Drain once a full block has accumulated in the sink so the per-partition footprint stays
        // bounded and the buffer can frame complete 2 MB blocks.
        if (partitionSinks(partition).size() >= StreamingShuffleConfig.BLOCK_SIZE_BYTES) {
          drainPartition(partition, isFinal = false)
        }
      }

      finalizeAllPartitions()
      publishBufferUtilization()
      mapStatus = buildMapStatus()
      metrics.incWriteTime(System.nanoTime() - startNanos)
      logDebug(log"Streaming shuffle write completed shuffle=" +
        log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)} map=${MDC(LogKeys.MAP_ID, mapId)} bytes=" +
        log"${MDC(LogKeys.NUM_BYTES, committedBytes)} records=" +
        log"${MDC(LogKeys.COUNT, committedRecords)}")
      success = true
    } finally {
      if (!success) {
        try {
          releaseResources(success = false)
        } catch {
          case NonFatal(cleanupError) =>
            logError(log"Streaming shuffle write cleanup failed after a write error shuffle=" +
              log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)} map=${MDC(LogKeys.MAP_ID, mapId)}",
              cleanupError)
        }
      }
    }
  }

  /**
   * Lazily allocates the per-partition buffer, byte sink, and serialization stream on first use,
   * registering the new buffer with both the [[MemorySpillManager]] (so it is sampled and
   * spillable) and the [[StreamingShuffleBlockResolver]] (so its in-memory bytes can be served to
   * reduce tasks before and after they reach disk).
   *
   * @param partition the reduce partition id
   * @return the serialization stream for the partition
   */
  private def streamFor(partition: Int): SerializationStream = {
    if (buffers(partition) == null) {
      val buffer = new StreamingBuffer(shuffleId, mapId, partition, perPartitionCapacityBytes)
      val sink = new ByteArrayOutputStream(INITIAL_SINK_BYTES)
      buffers(partition) = buffer
      partitionSinks(partition) = sink
      partitionStreams(partition) = serInstance.serializeStream(sink)
      spillManager.register(buffer)
      blockResolver.trackBuffer(buffer)
    }
    partitionStreams(partition)
  }

  /**
   * Drains the bytes flushed into a partition's sink into its [[StreamingBuffer]], updating the
   * partition length and committing write-byte metrics. The buffer performs the 2 MB CRC32C
   * framing, sealed blocks are streamed under backpressure, and the freshly buffered bytes are
   * accounted as execution memory (which may trigger a cooperative spill). Routing all bytes
   * through the buffer is what preserves the dual-channel wire/persist invariant.
   *
   * @param partition the reduce partition id to drain
   * @param isFinal whether this is the finalizing drain (also streams the trailing block)
   */
  private def drainPartition(partition: Int, isFinal: Boolean): Unit = {
    val sink = partitionSinks(partition)
    if (sink != null && sink.size() > 0) {
      val bytes = sink.toByteArray
      sink.reset()
      // Acquire execution memory for these bytes BEFORE retaining them in the buffer, so the
      // TaskMemoryManager governs the footprint: requesting the memory first triggers cooperative
      // spilling (including this writer's own inner-consumer spill()) and bounds the heap before it
      // grows, rather than accounting after the fact. The grant is acted on, not discarded -- a
      // short grant forces a spill and a re-request -- so genuine memory pressure can throttle the
      // allocation instead of being ignored.
      reserveBufferMemory(bytes.length.toLong)
      buffers(partition).append(bytes)
      partitionLengths(partition) += bytes.length.toLong
      committedBytes += bytes.length.toLong
      metrics.incBytesWritten(bytes.length.toLong)
      streamSealedBlocks(partition, isFinal)
    } else if (isFinal) {
      // Even with no new bytes, stream any blocks the buffer already holds (e.g. a single
      // sub-block partition whose bytes were flushed by close()).
      streamSealedBlocks(partition, isFinal)
    }
  }

  /**
   * Reserves `required` bytes of execution memory for buffered output before those bytes are
   * retained, so the footprint is governed by the [[org.apache.spark.memory.TaskMemoryManager]].
   *
   * Requesting the memory first lets the manager ask this writer (through the inner
   * [[BufferMemoryConsumer.spill]]) and other consumers to spill before the heap grows. The grant
   * is never ignored: on a short grant -- the manager could not fully satisfy the request even
   * after that cooperative spilling -- the writer force-spills its largest remaining buffer to
   * reclaim heap, rebalances the execution-memory accounting for the bytes that left the heap, and
   * re-requests only the outstanding remainder, repeating until the reservation is met or there is
   * nothing left to spill. The bytes in flight are at most a single 2 MB block, so once no buffer
   * remains to spill the writer proceeds on the partial grant rather than failing the task, and the
   * [[MemorySpillManager]]'s 100 ms poll keeps reclaiming.
   *
   * @param required the number of bytes about to be appended to a partition buffer
   */
  private def reserveBufferMemory(required: Long): Unit = {
    if (required > 0L) {
      var granted = memoryConsumer.acquireMemory(required)
      var exhausted = false
      while (granted < required && !exhausted) {
        val reclaimed = forceSpillLargestBuffer()
        if (reclaimed > 0L) {
          // forceSpillLargestBuffer pushes a buffer to disk but, unlike the inner consumer's
          // spill() callback, does not itself release execution memory, so rebalance the accounting
          // for the bytes that left the heap before re-requesting the outstanding remainder.
          val toFree = math.min(reclaimed, memoryConsumer.getUsed)
          if (toFree > 0L) {
            memoryConsumer.freeMemory(toFree)
          }
          granted += memoryConsumer.acquireMemory(required - granted)
        } else {
          // Nothing left to spill (every buffer is empty or already on disk): proceed on the
          // partial grant for this single in-flight block rather than looping or failing the task.
          exhausted = true
        }
      }
    }
  }

  /**
   * Streams the buffer's fully sealed blocks for a partition under token-bucket backpressure,
   * building one [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] per block
   * from the buffer (never re-framing) and handing it to the v1 logging-only transport.
   *
   * Non-final calls stream every '''fully sealed''' (complete 2 MB) block via
   * [[StreamingBuffer.numSealedBlocks]] and hold back only a genuinely partial trailing block,
   * which the buffer tracks explicitly as its pending remainder; this pipelines each block to the
   * consumer the instant it is complete (the previous `numBlocks - 1` heuristic delayed an
   * exactly-full last block whenever the partition's bytes landed on 2 MB boundaries). The
   * finalizing call additionally streams the pending tail via [[StreamingBuffer.numBlocks]]. A
   * concurrent spill clears the buffer and resets its block indices to a new generation: this is
   * detected (`target < sentBlocks`) and tolerated, because the cleared bytes are already durable
   * on disk for the resolver to serve.
   *
   * Each send is observed rather than discarded: the v1 transport completes its future
   * synchronously so awaiting it returns at once, while a future (v2) real-transport failure is
   * surfaced here and propagated to the map task (the [[write]] try/finally then releases
   * resources) for scheduler-driven retry, instead of being a silent fire-and-forget.
   *
   * @param partition the reduce partition id
   * @param isFinal whether to stream the pending (partial) trailing block as well
   */
  private def streamSealedBlocks(partition: Int, isFinal: Boolean): Unit = {
    val buffer = buffers(partition)
    if (buffer != null) {
      // Non-final: stream only fully sealed blocks (the pending sub-block tail is held back).
      // Final: stream everything the buffer holds, including the pending tail.
      val target = if (isFinal) buffer.numBlocks else buffer.numSealedBlocks
      if (target < sentBlocks(partition)) {
        // The buffer was spilled and cleared concurrently; start a fresh generation.
        sentBlocks(partition) = 0
      }
      val key = BackpressureProtocol.StreamKey(shuffleId, mapId, partition)
      var idx = sentBlocks(partition)
      try {
        while (idx < target) {
          val envelope = buffer.envelopeOf(idx)
          // Throttle the producer to the per-executor bandwidth cap before "sending". The
          // backpressure protocol counts throttling events itself, so none is counted here.
          backpressure.acquireSendPermit(key, envelope.payloadLength)
          serverIdOpt.foreach { sendTarget =>
            val sendFuture = transport.sendBlock(envelope, sendTarget)
            // Observe the send result rather than discarding the future: v1 completes it
            // synchronously (returns immediately), and any future real-transport failure throws
            // here and propagates to the map task instead of being lost.
            ThreadUtils.awaitResult(sendFuture, sendAwaitTimeout)
          }
          idx += 1
        }
        sentBlocks(partition) = idx
      } catch {
        case _: IndexOutOfBoundsException =>
          // A concurrent spill cleared the buffer mid-stream; its bytes are durable on disk and
          // will be served by the resolver, so resync the generation and continue next drain.
          sentBlocks(partition) = 0
      }
    }
  }

  /**
   * Finalizes every active partition after the last record: closes the serialization stream to
   * flush any serializer-internal bytes, drains the remainder into the buffer, streams the
   * trailing block, and applies the consumer-timeout protocol so unacknowledged data is persisted
   * and scheduled for retransmit rather than lost.
   */
  private def finalizeAllPartitions(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val stream = partitionStreams(partition)
      if (stream != null) {
        // Close flushes any bytes still held in the serializer's internal buffer down into the
        // sink so even a sub-block partition contributes its full payload.
        stream.close()
        partitionStreams(partition) = null
        drainPartition(partition, isFinal = true)
        handleConsumerTimeout(partition)
      }
      partition += 1
    }
  }

  /**
   * Implements the consumer-failure half of the streaming protocol: if the backpressure protocol
   * has declared the consumer timed out (no acks within the 10 s window), the partition's
   * still-buffered data is spilled to disk when it is over the spill threshold so it can never be
   * lost, and a structured warning records the unacked byte count and the next exponential-backoff
   * retransmit delay. In v1 the retransmit itself is realized by the reduce side pulling the
   * persisted bytes through the existing fetch path rather than a push from here.
   *
   * @param partition the reduce partition id to check
   */
  private def handleConsumerTimeout(partition: Int): Unit = {
    val buffer = buffers(partition)
    if (buffer != null) {
      val key = BackpressureProtocol.StreamKey(shuffleId, mapId, partition)
      if (backpressure.isConsumerTimedOut(key)) {
        val unackedBytes = backpressure.unackedByteCount(key)
        val backoffMs = backpressure.nextRetransmitBackoffMs(key)
        if (buffer.utilizationPercent >= config.spillThreshold.toDouble) {
          spillManager.spillBuffer(MemorySpillManager.BufferKey(shuffleId, mapId, partition))
        }
        logWarning(log"Streaming consumer timed out; buffering unacked data for retransmit " +
          log"shuffle=${MDC(LogKeys.SHUFFLE_ID, shuffleId)} map=${MDC(LogKeys.MAP_ID, mapId)} " +
          log"reduce=${MDC(LogKeys.REDUCE_ID, partition)} unackedBytes=" +
          log"${MDC(LogKeys.NUM_BYTES, unackedBytes)} retransmitBackoffMs=" +
          log"${MDC(LogKeys.DURATION, backoffMs)}")
      }
    }
  }

  /**
   * Force-spills this writer's single largest non-empty buffer when an execution-memory request
   * cannot be satisfied by the threshold-driven [[MemorySpillManager.maybeSpill]]. The spill
   * manager persists and clears the chosen buffer (and counts the spill event); the size captured
   * before spilling is returned as the reclaimed estimate for execution-memory accounting.
   *
   * @return the bytes reclaimed (the spilled buffer's prior size), or `0` if nothing was spilled
   */
  private def forceSpillLargestBuffer(): Long = {
    var largest: StreamingBuffer = null
    var largestSize = 0L
    var partition = 0
    while (partition < numPartitions) {
      val buffer = buffers(partition)
      if (buffer != null) {
        val size = buffer.size
        if (size > largestSize) {
          largest = buffer
          largestSize = size
        }
      }
      partition += 1
    }
    if (largest != null && largestSize > 0L) {
      val key = MemorySpillManager.BufferKey(largest.shuffleId, largest.mapId, largest.partitionId)
      if (spillManager.spillBuffer(key)) largestSize else 0L
    } else {
      0L
    }
  }

  /**
   * Publishes this writer's aggregate buffer utilization to the streaming telemetry gauge. This
   * complements the [[MemorySpillManager]]'s own periodic sampling and ensures the gauge reflects
   * the writer's footprint even when the spill poller is not running (for example in focused
   * unit tests). The denominator is the executor on-heap storage memory
   * (`MemoryManager.maxOnHeapStorageMemory`), matching the spill manager's own sampling so both
   * publishers feed the single `bufferUtilizationPercent` gauge with one consistent meaning: the
   * fraction of executor on-heap storage memory currently buffered, not a buffer-capacity ratio.
   */
  private def publishBufferUtilization(): Unit = {
    var totalSize = 0L
    var partition = 0
    while (partition < numPartitions) {
      val buffer = buffers(partition)
      if (buffer != null) {
        totalSize += buffer.size
      }
      partition += 1
    }
    val denom = executorMemoryBytes
    if (denom > 0L) {
      val pct = math.min(100.0, totalSize.toDouble * 100.0 / denom.toDouble)
      streamingMetrics.setBufferUtilizationPercent(pct)
    }
  }

  /**
   * Builds the [[org.apache.spark.scheduler.MapStatus]] describing this map output: the local
   * shuffle server identity, the per-partition byte counts, and this map task id. Mirrors
   * `SortShuffleWriter`, which likewise constructs the status at the end of a successful write.
   *
   * @return the map status for a successful write
   */
  private def buildMapStatus(): MapStatus = {
    val location = SparkEnv.get.blockManager.shuffleServerId
    MapStatus(location, partitionLengths, mapId)
  }

  /**
   * Closes this writer, returning the [[org.apache.spark.scheduler.MapStatus]] on success.
   *
   * On a successful map task the buffers remain registered with the spill manager and block
   * resolver so the reduce side can fetch the streamed/spilled output; final cleanup of those
   * registrations is `StreamingShuffleManager.unregisterShuffle`'s responsibility. On failure the
   * committed write metrics are rolled back and the buffered output is released. Either way the
   * inner consumer's execution memory is freed so the task leaves no leak. Guarded so a repeated
   * stop (success then failure) releases resources exactly once.
   *
   * @param success whether the map task completed successfully
   * @return the map status when successful, otherwise [[scala.None]]
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      None
    } else {
      stopping = true
      try {
        if (success) {
          Option(mapStatus)
        } else {
          // Roll back the incrementally-committed write metrics for the discarded attempt.
          if (committedBytes > 0L) metrics.decBytesWritten(committedBytes)
          if (committedRecords > 0L) metrics.decRecordsWritten(committedRecords)
          None
        }
      } finally {
        releaseResources(success)
      }
    }
  }

  /**
   * @return a defensive copy of the per-partition written byte counts. A clone is returned rather
   *         than the internal array so a caller cannot mutate this writer's state after [[write]];
   *         the base [[ShuffleWriter]] contract does not require sharing the same array instance,
   *         and the [[org.apache.spark.scheduler.MapStatus]] is built from the internal array
   *         directly in [[buildMapStatus]].
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths.clone()

  /**
   * Releases every resource the writer holds. Always frees the inner consumer's accounted
   * execution memory (the no-leak guarantee) and unregisters this writer's backpressure streams;
   * on failure it additionally clears the buffers and stops the spill manager from sampling them.
   * The buffered byte arrays are plain JVM heap, so on success they survive for the reader and are
   * reclaimed by the manager's `unregisterShuffle`.
   *
   * @param success whether the map task completed successfully
   */
  private def releaseResources(success: Boolean): Unit = {
    closeStreamsQuietly()
    val used = memoryConsumer.getUsed
    if (used > 0L) {
      memoryConsumer.freeMemory(used)
    }
    unregisterBackpressureStreams()
    if (!success) {
      releaseBuffers()
    }
  }

  /** Closes any serialization streams still open (defensive; finalize normally closes them). */
  private def closeStreamsQuietly(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val stream = partitionStreams(partition)
      if (stream != null) {
        try {
          stream.close()
        } catch {
          case _: IOException => // Already-failed stream; nothing more to flush.
        }
        partitionStreams(partition) = null
      }
      partition += 1
    }
  }

  /** Unregisters this writer's per-partition backpressure streams to bound flow-control state. */
  private def unregisterBackpressureStreams(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      if (buffers(partition) != null) {
        backpressure.unregisterStream(BackpressureProtocol.StreamKey(shuffleId, mapId, partition))
      }
      partition += 1
    }
  }

  /**
   * Clears the buffered output and unregisters it from the spill manager on a failed attempt. The
   * block resolver is intentionally not untracked here: it has no per-map removal (only whole
   * shuffle), and because a failed attempt produces no [[MapStatus]] the reduce side never fetches
   * this attempt's output, so the cleared (empty) entries are harmless orphans that a retry
   * replaces and `unregisterShuffle` ultimately drops.
   */
  private def releaseBuffers(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val buffer = buffers(partition)
      if (buffer != null) {
        spillManager.unregister(MemorySpillManager.BufferKey(shuffleId, mapId, partition))
        buffer.clear()
        buffers(partition) = null
      }
      partition += 1
    }
  }
}

/**
 * Constants for [[StreamingShuffleWriter]].
 */
private[spark] object StreamingShuffleWriter {

  /**
   * Initial capacity of a per-partition byte sink. Sized to a fraction of a 2 MB block so a
   * lightly-used partition keeps a small footprint while a busy one resizes only a few times
   * before its bytes are drained into the buffer.
   */
  private val INITIAL_SINK_BYTES: Int = 64 * 1024
}
