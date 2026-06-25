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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, EncryptedManagedBuffer,
  ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, ThreadUtils}
import org.apache.spark.util.collection.ExternalSorter

/**
 * Consumer (reduce) side reader for the streaming shuffle data path (feature F-104).
 *
 * Where the sort-based [[org.apache.spark.shuffle.BlockStoreShuffleReader]] fetches fully
 * materialized shuffle files, this reader pulls '''in-progress''' streaming blocks directly from
 * producers: it resolves the producer locations through the existing
 * [[org.apache.spark.MapOutputTracker]], fetches each self-describing
 * [[StreamingBlockEnvelope]] block through the executor's existing
 * [[org.apache.spark.network.BlockTransferService]] (reusing the established transport rather
 * than introducing a new one), validates the per-block CRC32C, and acknowledges every validated
 * block so the producer can reclaim its buffer. Once a block's payload is validated it is fed
 * through a deserialize/aggregate/sort tail that is '''byte-identical''' to the sort path, so the
 * reduce-side semantics (aggregator, key ordering, and map-side combine) are exactly preserved.
 *
 * '''Zero data loss (the central invariant).''' Streaming reads can fail in ways a materialized
 * read cannot: a producer can stall before a block is fully available, or a block can arrive
 * corrupt. Both are unrecoverable for the in-flight read in v1, so this reader '''invalidates'''
 * the partial read rather than ever returning truncated or corrupt data. On a producer-connection
 * timeout (5 s; see [[StreamingShuffleReader.PRODUCER_CONNECTION_TIMEOUT_MS]]) or a CRC32C
 * mismatch that cannot be retransmitted, it (1) discards the partial read, (2) increments
 * `partialReadInvalidations` via [[StreamingShuffleMetrics]], and (3) constructs and throws a
 * [[org.apache.spark.shuffle.FetchFailedException]] '''immediately'''. The
 * `FetchFailedException` constructor records the failure on the [[org.apache.spark.TaskContext]]
 * (SPARK-19276), so it must be thrown the instant it is created; the existing DAG scheduler then
 * recomputes the upstream stage with no scheduler modification whatsoever.
 *
 * '''Bounded retransmission before invalidation.''' Transient transport errors (including a block
 * that is momentarily not yet materialized) are retried with exponential backoff
 * (see [[StreamingShuffleReader.INITIAL_RETRY_BACKOFF_MS]], doubling each attempt, capped at
 * [[StreamingShuffleReader.MAX_RETRANSMIT_ATTEMPTS]] attempts) while the producer-connection
 * deadline has not elapsed. Only after retransmission is exhausted, or the 5 s deadline passes,
 * is the read invalidated.
 *
 * '''Acknowledgment protocol.''' Each validated block is acknowledged through the shared
 * [[BackpressureProtocol]]: the monotonic acknowledgment high-water mark is advanced, the
 * consumed bytes are returned to the flow-control credit window so the producer can reclaim its
 * buffer (within the 100 ms reclaim SLA), and consumer liveness is stamped via a heartbeat. In
 * v1 these mutate the local protocol state; the cross-executor carrier of these signals is the
 * `BackpressureRpcEndpoint` (feature F-108).
 *
 * '''v1 transport.''' The streaming data plane ships as a logging-only stub in v1 (feature
 * F-115); the reader fetches through the existing block transfer service. See decision log
 * ADR-15.
 *
 * Instances are used by a single reduce task; the returned iterator is lazy, so the
 * fetch/validate/acknowledge work for each block happens as the consumer pulls it, mirroring how
 * the sort path drives its fetcher iterator.
 *
 * @param handle                the streaming shuffle handle carrying the dependency and tuning
 *                              parameters for this shuffle
 * @param startMapIndex         the start map index (inclusive) of the producer range to read
 * @param endMapIndex           the end map index (exclusive) of the producer range to read
 * @param startPartition        the start reduce partition (inclusive) this task reads
 * @param endPartition          the end reduce partition (exclusive) this task reads
 * @param context               the task context, used for metrics and task-cancellation support
 * @param readMetrics           sink for shuffle read metrics (records read, etc.)
 * @param backpressure          the shared flow-control protocol used to acknowledge blocks
 * @param metrics               the streaming shuffle metrics holder; partial-read invalidations
 *                              are tallied through it
 * @param mapOutputTracker      resolves producer locations for the requested partition range
 * @param blockManager          provides the block transfer service used to fetch blocks
 * @param serializerManager     wraps each validated block stream (compression/encryption) exactly
 *                              as the sort path does
 * @param producerTimeoutMs     producer-connection timeout, in milliseconds, before invalidation
 * @param maxRetransmitAttempts maximum transient-transport retransmission attempts per block
 * @param initialRetryBackoffMs initial exponential-backoff interval, in milliseconds
 * @param spillManagerOpt       the producer's spill manager when co-located (v1, single
 *                              executor); each acknowledged block reclaims its per-partition
 *                              buffer through it. Reclaim on an unknown key is a safe no-op, so
 *                              this is also correct when producer and consumer are on different
 *                              executors and the authoritative reclaim happens producer-side.
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    backpressure: BackpressureProtocol,
    metrics: StreamingShuffleMetrics,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    producerTimeoutMs: Long = StreamingShuffleReader.PRODUCER_CONNECTION_TIMEOUT_MS,
    maxRetransmitAttempts: Int = StreamingShuffleReader.MAX_RETRANSMIT_ATTEMPTS,
    initialRetryBackoffMs: Long = StreamingShuffleReader.INITIAL_RETRY_BACKOFF_MS,
    spillManagerOpt: Option[MemorySpillManager] = None)
  extends ShuffleReader[K, C] with Logging {

  /** The shuffle dependency, the authoritative source of serializer/ordering/aggregator. */
  private val dep = handle.dependency

  /** The id of the shuffle being read; used for fetches, messages, and failure reporting. */
  private val shuffleId = handle.shuffleId

  /** Monotonic per-reader sequence number used to label consumer acknowledgments. */
  private val ackSequence = new AtomicLong(0L)

  /** Read the combined key-values for this reduce task. */
  override def read(): Iterator[Product2[K, C]] = {
    // Resolve the producer (map output) locations for this reducer's partition range exactly as
    // the sort path does: per producing executor, the blocks (sizes and map indices) to read.
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    // Lazily fetch, CRC32C-validate, and acknowledge each in-progress streaming block, yielding a
    // (blockId, deserialization-ready stream) pair per block as the consumer pulls it.
    val wrappedStreams: Iterator[(BlockId, InputStream)] =
      blocksByAddress.flatMap { case (address, blockInfos) =>
        blockInfos.iterator.map { case (blockId, blockSize, mapIndex) =>
          val payloadStream = fetchValidateAndAck(address, blockId, mapIndex, blockSize)
          (blockId, serializerManager.wrapStream(blockId, payloadStream))
        }
      }

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream. The asKeyValueIterator wraps a key/value
    // iterator inside a NextIterator, which closes the underlying stream once fully read.
    val recordIter = wrappedStreams.flatMap { case (_, wrappedStream) =>
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read (mirrors BlockStoreShuffleReader).
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation.
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val resultIter: Iterator[Product2[K, C]] = {
      // Sort the output if there is a sort ordering defined.
      if (dep.keyOrdering.isDefined) {
        // Create an ExternalSorter to sort the data.
        val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
          if (dep.mapSideCombine) {
            new ExternalSorter[K, C, C](context,
              Option(new Aggregator[K, C, C](identity,
                dep.aggregator.get.mergeCombiners,
                dep.aggregator.get.mergeCombiners)),
              ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
          } else {
            new ExternalSorter[K, Nothing, C](context,
              dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
              ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
          }
        } else {
          new ExternalSorter[K, C, C](context, ordering = Some(dep.keyOrdering.get),
            serializer = dep.serializer)
        }
        sorter.insertAllAndUpdateMetrics(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])
      } else if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // We are reading values that are already combined.
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // We don't know the value type, but also don't care -- the dependency *should*
          // have made sure it's compatible w/ this aggregator, which will convert the value
          // type to the combined type C.
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      }
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as the aggregator
        // or(and) sorter may have consumed the previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * Fetch a single in-progress streaming block, validate its CRC32C, acknowledge it, and return a
   * deserialization-ready stream over the validated payload. Every failure mode -- producer
   * timeout, transport error after exhausting retransmission, an I/O error reading the fetched
   * bytes, or a CRC32C / structural validation failure -- goes through [[invalidateAndThrow]]
   * so the partial read is discarded and the upstream stage is recomputed (zero data loss).
   *
   * @param address      producer block-manager id; the fetch source and failure-report location
   * @param blockId      the shuffle block id to fetch
   * @param mapIndex     the map index of the producing task (reported on invalidation)
   * @param expectedSize the producer-published (approximate) size of this block, used as the
   *                     upfront fetch-budget guard before any byte is read
   * @return a stream positioned at the validated, concatenated block payload
   */
  private def fetchValidateAndAck(
      address: BlockManagerId,
      blockId: BlockId,
      mapIndex: Int,
      expectedSize: Long): InputStream = {
    val (mapId, reduceId) = mapAndReduceId(blockId)
    val buffer = fetchWithRetry(address, blockId, mapId, mapIndex, reduceId)
    val payload =
      decodeFramesFromBuffer(buffer, address, mapId, mapIndex, reduceId, expectedSize)
    acknowledge(blockId, payload.length)
    new ByteArrayInputStream(payload)
  }

  /**
   * Fetch a single block through the executor's existing block transfer service, polling/retrying
   * transient transport failures (including a block not yet materialized) with
   * exponential backoff. The total wait is bounded by [[producerTimeoutMs]]; once the deadline
   * passes or [[maxRetransmitAttempts]] is reached, the read is invalidated (which throws). The
   * loop is expressed without an early `return` so it produces no unreachable-code warnings under
   * the strict `-Wconf:any:e` gate.
   *
   * Each individual fetch is awaited for at most the time '''remaining''' to the producer
   * deadline (see [[fetchBlockBounded]]). This is the difference between this method and a naive
   * `fetchBlockSync` loop: `fetchBlockSync` awaits with `Duration.Inf`, so a producer that stalls
   * mid-fetch would block the reduce thread forever and the 5 s deadline check below would never
   * be reached. By bounding every await, a stalled producer surfaces as a `TimeoutException` and
   * the partial read is invalidated immediately at the deadline (zero data loss).
   *
   * @return the fetched [[ManagedBuffer]] (never null and never empty)
   */
  private def fetchWithRetry(
      address: BlockManagerId,
      blockId: BlockId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int): ManagedBuffer = {
    val deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(producerTimeoutMs)
    var result: ManagedBuffer = null
    var attempt = 0
    var backoffMs = initialRetryBackoffMs
    while (result == null) {
      // Bound this fetch attempt by the time remaining to the producer deadline so that a
      // stalled producer cannot block the reduce thread past the 5 s timeout (zero data loss).
      val remainingMsForFetch = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime())
      if (remainingMsForFetch <= 0L) {
        invalidateAndThrow(address, mapId, mapIndex, reduceId,
          s"Producer connection timed out after $attempt attempt(s) / ${producerTimeoutMs}ms " +
            s"fetching in-progress streaming block $blockId (deadline elapsed before fetch)")
      }
      try {
        val buf = fetchBlockBounded(address, blockId, remainingMsForFetch)
        if (buf == null || buf.size() <= 0L) {
          // Treat a missing/empty block as "not yet materialized": retry until the deadline.
          throw new IllegalStateException(
            s"Empty buffer received for in-progress streaming block $blockId")
        }
        result = buf
      } catch {
        case NonFatal(e) =>
          attempt += 1
          val remainingNs = deadlineNs - System.nanoTime()
          if (attempt >= maxRetransmitAttempts || remainingNs <= 0L) {
            invalidateAndThrow(address, mapId, mapIndex, reduceId,
              s"Producer connection timed out after $attempt attempt(s) / " +
                s"${producerTimeoutMs}ms fetching in-progress streaming block $blockId",
              e)
          }
          // Exponential backoff, bounded by the time remaining to the producer deadline.
          val remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNs)
          val sleepMs = math.max(0L, math.min(backoffMs, remainingMs))
          if (sleepMs > 0L) {
            sleepBeforeRetry(sleepMs)
          }
          backoffMs = backoffMs * 2
      }
    }
    result
  }

  /**
   * Fetch a single block through the executor's existing [[org.apache.spark.network.
   * BlockTransferService]], awaiting the result for at most `awaitMs` milliseconds. This is the
   * bounded analogue of `BlockTransferService.fetchBlockSync`, which awaits with `Duration.Inf`
   * and therefore can never honor the producer-connection deadline if a producer stalls
   * mid-fetch. The success path mirrors `fetchBlockSync`: file-backed and encrypted buffers are
   * passed through, while an in-memory transport buffer is copied onto the heap (as a
   * [[NioManagedBuffer]]) so it survives after the asynchronous network callback returns.
   *
   * A finite-duration `awaitResult` throws an unwrapped `TimeoutException` (it is `NonFatal`),
   * which the [[fetchWithRetry]] loop converts into a deadline check and, ultimately, an
   * invalidation. The fetch is issued through the existing transfer service rather than a new
   * transport, consistent with the v1 logging-only data-plane stub (feature F-115).
   *
   * @return the fetched [[ManagedBuffer]]
   */
  private def fetchBlockBounded(
      address: BlockManagerId,
      blockId: BlockId,
      awaitMs: Long): ManagedBuffer = {
    val result = Promise[ManagedBuffer]()
    blockManager.blockTransferService.fetchBlocks(
      address.host,
      address.port,
      address.executorId,
      Array(blockId.toString),
      new BlockFetchingListener {
        override def onBlockFetchFailure(failedId: String, exception: Throwable): Unit = {
          result.tryFailure(exception)
        }
        override def onBlockFetchSuccess(fetchedId: String, data: ManagedBuffer): Unit = {
          data match {
            case f: FileSegmentManagedBuffer => result.trySuccess(f)
            case e: EncryptedManagedBuffer => result.trySuccess(e)
            case _ =>
              try {
                val copy = ByteBuffer.allocate(data.size().toInt)
                copy.put(data.nioByteBuffer())
                copy.flip()
                result.trySuccess(new NioManagedBuffer(copy))
              } catch {
                case NonFatal(e) => result.tryFailure(e)
              }
          }
        }
      },
      null)
    ThreadUtils.awaitResult(result.future, Duration(awaitMs, TimeUnit.MILLISECONDS))
  }

  /**
   * Decode the fetched block as one or more concatenated [[StreamingBlockEnvelope]] frames,
   * validating each frame's structure and CRC32C, and return the concatenated validated payloads.
   *
   * Unlike a "read the whole buffer, then decode" approach, this reads '''frame by frame''' from
   * the buffer's input stream and never allocates an array sized from an unvalidated length: for
   * each frame it (1) reads the fixed [[StreamingBlockEnvelope.HEADER_SIZE]]-byte header, (2)
   * validates the declared payload length is within `[0, MAX_PAYLOAD_SIZE]` '''before''' the
   * payload buffer is allocated, then (3) reads exactly that many payload bytes and verifies the
   * CRC32C through the canonical envelope codec. A corrupt or non-streaming block therefore
   * cannot force an unbounded allocation: the largest single allocation is one header plus one
   * `<= 2 MiB` frame. An upfront budget guard additionally rejects a fetched block whose
   * transport size grossly exceeds the size the producer published for this partition, before any
   * byte is touched. Any structural decode error or checksum mismatch invalidates the read (which
   * throws); a mismatch is not retransmitted in v1 (see decision log D-5).
   */
  private def decodeFramesFromBuffer(
      buffer: ManagedBuffer,
      address: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      expectedSize: Long): Array[Byte] = {
    // Upfront budget guard: bound the fetched block size before reading any byte. The budget is a
    // generous multiple of the producer-published (approximate) partition size plus one full
    // frame of slack for envelope framing overhead and the lossy MapStatus size compression, so
    // it never false-trips on a legitimate block but still rejects a wildly oversized buffer.
    val maxFetchBytes =
      math.max(expectedSize, 0L) * StreamingShuffleReader.FETCH_BUDGET_SLACK_FACTOR +
        StreamingBlockEnvelope.MAX_PAYLOAD_SIZE
    if (buffer.size() > maxFetchBytes) {
      buffer.release()
      invalidateAndThrow(address, mapId, mapIndex, reduceId,
        s"Fetched streaming block for shuffle $shuffleId (map $mapId, reduce $reduceId) is " +
          s"${buffer.size()} bytes, exceeding the maximum fetch budget of $maxFetchBytes bytes")
    }
    val assembled = new ByteArrayOutputStream(
      math.max(StreamingBlockEnvelope.HEADER_SIZE, math.min(buffer.size(), 64L * 1024L).toInt))
    val in =
      try {
        new DataInputStream(buffer.createInputStream())
      } catch {
        case NonFatal(e) =>
          buffer.release()
          invalidateAndThrow(address, mapId, mapIndex, reduceId,
            s"Failed to open fetched streaming block (shuffle $shuffleId, map $mapId, " +
              s"reduce $reduceId): ${e.getMessage}", e)
      }
    try {
      val header = new Array[Byte](StreamingBlockEnvelope.HEADER_SIZE)
      var continue = true
      while (continue) {
        // Peek a single byte to distinguish a clean inter-frame EOF from a truncated header.
        val first =
          try {
            in.read()
          } catch {
            case NonFatal(e) =>
              invalidateAndThrow(address, mapId, mapIndex, reduceId,
                s"I/O error reading streaming block frame header (shuffle $shuffleId, map " +
                  s"$mapId, reduce $reduceId): ${e.getMessage}", e)
          }
        if (first < 0) {
          continue = false
        } else {
          header(0) = first.toByte
          try {
            in.readFully(header, 1, StreamingBlockEnvelope.HEADER_SIZE - 1)
          } catch {
            case NonFatal(e) =>
              invalidateAndThrow(address, mapId, mapIndex, reduceId,
                s"Truncated streaming block frame header (shuffle $shuffleId, map $mapId, " +
                  s"reduce $reduceId): ${e.getMessage}", e)
          }
          // Validate the declared payload length BEFORE allocating the payload buffer so a
          // corrupt/oversized length cannot force an unbounded allocation.
          val payloadLength =
            ByteBuffer.wrap(header).getInt(StreamingShuffleReader.PAYLOAD_LENGTH_OFFSET)
          if (payloadLength < 0 || payloadLength > StreamingBlockEnvelope.MAX_PAYLOAD_SIZE) {
            invalidateAndThrow(address, mapId, mapIndex, reduceId,
              s"Streaming block frame for shuffle $shuffleId (map $mapId, reduce $reduceId) " +
                s"declares an out-of-range payload length $payloadLength " +
                s"(max ${StreamingBlockEnvelope.MAX_PAYLOAD_SIZE})")
          }
          // Reassemble the full frame and reuse the canonical envelope decode + CRC32C check, so
          // framing/checksum rules stay byte-identical to the producer's writer.
          val frame = new Array[Byte](StreamingBlockEnvelope.HEADER_SIZE + payloadLength)
          System.arraycopy(header, 0, frame, 0, StreamingBlockEnvelope.HEADER_SIZE)
          try {
            in.readFully(frame, StreamingBlockEnvelope.HEADER_SIZE, payloadLength)
          } catch {
            case NonFatal(e) =>
              invalidateAndThrow(address, mapId, mapIndex, reduceId,
                s"Truncated streaming block frame for shuffle $shuffleId (map $mapId, reduce " +
                  s"$reduceId): expected $payloadLength payload bytes (${e.getMessage})", e)
          }
          val envelope =
            try {
              StreamingBlockEnvelope.decode(ByteBuffer.wrap(frame))
            } catch {
              case NonFatal(e) =>
                invalidateAndThrow(address, mapId, mapIndex, reduceId,
                  s"Corrupt streaming block envelope (shuffle $shuffleId, map $mapId, " +
                    s"reduce $reduceId): ${e.getMessage}", e)
            }
          if (!envelope.verifyChecksum()) {
            invalidateAndThrow(address, mapId, mapIndex, reduceId,
              s"CRC32C checksum mismatch for streaming block (shuffle $shuffleId, map $mapId, " +
                s"reduce $reduceId, ${envelope.payloadLength} payload bytes)")
          }
          assembled.write(envelope.payload)
        }
      }
    } finally {
      // Close the stream and always release the transport buffer. A close error must never mask
      // an in-flight invalidation, so it is swallowed (the validated payload, if any, is already
      // assembled on the heap).
      try {
        in.close()
      } catch {
        case NonFatal(_) =>
      }
      buffer.release()
    }
    assembled.toByteArray()
  }

  /**
   * Run the consumer-side acknowledgment protocol for a validated block: advance the ack
   * high-water mark, return the consumed bytes to the flow-control credit window so the producer
   * can reclaim its buffer (within the 100 ms reclaim SLA), stamp consumer liveness via a
   * heartbeat, and -- when the producer's spill manager is co-located -- reclaim the producer's
   * per-partition buffer for the acknowledged block (feature F-109). Reclaim on an unknown key is
   * a safe no-op, so this is correct (best-effort) when producer and consumer are on different
   * executors, where the authoritative reclaim happens producer-side. In v1 the flow-control
   * updates mutate the local [[BackpressureProtocol]] state; the cross-executor carrier of these
   * signals is the backpressure RPC endpoint (feature F-108).
   */
  private def acknowledge(blockId: BlockId, payloadBytes: Int): Unit = {
    val (mapId, reduceId) = mapAndReduceId(blockId)
    val seqNo = ackSequence.incrementAndGet()
    // Identify this consumer stream so the ack advances exactly this stream's watermark (and
    // stamps its consumer-liveness), never an unrelated stream's. blockManagerId is always set on
    // a live executor; the defensive fallback keeps this correct under a mocked BlockManager.
    val consumerExecutorId =
      Option(blockManager.blockManagerId).map(_.executorId).getOrElse("unknown")
    val streamKey = StreamKey(shuffleId, reduceId, context.taskAttemptId(), consumerExecutorId)
    backpressure.mergeAck(streamKey, seqNo)
    backpressure.refill(payloadBytes.toLong)
    backpressure.recordHeartbeat()
    spillManagerOpt.foreach { manager =>
      manager.reclaim(MemorySpillManager.BufferKey(shuffleId, mapId, reduceId))
    }
    logDebug(log"Acknowledged streaming block ${MDC(BLOCK_ID, blockId)} " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} (ackSeq=${MDC(COUNT, seqNo)}, " +
      log"${MDC(NUM_BYTES, payloadBytes)} bytes)")
  }

  /**
   * Invalidate a partial read and throw a [[org.apache.spark.shuffle.FetchFailedException]].
   *
   * The invalidation is tallied and logged BEFORE the exception is constructed because, per
   * SPARK-19276, the `FetchFailedException` constructor records the fetch failure on the
   * [[org.apache.spark.TaskContext]]; it must therefore be thrown the instant it is created and
   * never stored, wrapped conditionally, or interleaved with other evaluations. This method's
   * return type is [[scala.Nothing]] so callers can use it in expression position.
   */
  private def invalidateAndThrow(
      address: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      reason: String,
      cause: Throwable = null): Nothing = {
    metrics.incrementPartialReadInvalidations()
    logWarning(log"Invalidating partial streaming read; deferring to DAG-scheduler " +
      log"recomputation: shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
      log"reduce=${MDC(REDUCE_ID, reduceId)} " +
      log"range=${MDC(RANGE, s"[$startPartition, $endPartition)")} " +
      log"attempt=${MDC(TASK_ATTEMPT_ID, context.taskAttemptId())} " +
      log"reason=${MDC(REASON, reason)}")
    throw new FetchFailedException(address, shuffleId, mapId, mapIndex, reduceId, reason, cause)
  }

  /**
   * Extract the (mapId, reduceId) of a shuffle block for failure reporting. Streaming shuffle
   * blocks are always [[ShuffleBlockId]]s; any other block id is unexpected and defensively maps
   * to the reducer's first partition so an invalidation can still be reported.
   */
  private def mapAndReduceId(blockId: BlockId): (Long, Int) = blockId match {
    case ShuffleBlockId(_, mapId, reduceId) => (mapId, reduceId)
    case other =>
      logWarning(log"Unexpected non-ShuffleBlockId ${MDC(BLOCK_ID, other)} for streaming " +
        log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
      (-1L, startPartition)
  }

  /**
   * Sleep for the given backoff interval, preserving the thread's interrupt status so that task
   * cancellation is honored upstream rather than swallowed.
   */
  private def sleepBeforeRetry(millis: Long): Unit = {
    try {
      Thread.sleep(millis)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }

}

/**
 * Constants for [[StreamingShuffleReader]] derived from the streaming-shuffle timing semantics
 * (AAP 0.2.2.2). They are the production defaults for the reader's optional timing parameters and
 * are exposed so tests can reference them and override the corresponding constructor arguments
 * with small values for fast, deterministic failure-injection coverage.
 */
private[spark] object StreamingShuffleReader {

  /**
   * Producer connection timeout: max wall-clock time spent polling/awaiting an in-progress
   * streaming block from a producer before the read is invalidated and recomputation starts.
   * Five seconds.
   */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = 5000L

  /**
   * Maximum number of transient-transport retransmission attempts for a single block before the
   * read is invalidated. Exponential backoff (see [[INITIAL_RETRY_BACKOFF_MS]]) applies between
   * attempts, always bounded by [[PRODUCER_CONNECTION_TIMEOUT_MS]].
   */
  val MAX_RETRANSMIT_ATTEMPTS: Int = 5

  /**
   * Initial retransmission backoff of one second, doubled after each failure (exponential),
   * and capped by the time remaining to the producer-connection deadline.
   */
  val INITIAL_RETRY_BACKOFF_MS: Long = 1000L

  /**
   * Byte offset of the big-endian `payloadLength` field inside a [[StreamingBlockEnvelope]]
   * header. Read directly off the 32-byte header so a frame's declared payload length can be
   * range-checked before the payload buffer is allocated (the M7 bounded-decode guard).
   */
  val PAYLOAD_LENGTH_OFFSET: Int = 20

  /**
   * Slack multiplier applied to the producer-published (approximate) partition size to derive the
   * upfront fetch-budget guard. A fetched block larger than `expectedSize * factor + one full
   * frame` is rejected before any byte is read. The factor is generous so it never false-trips on
   * a legitimate block (envelope framing overhead plus the lossy `MapStatus` size compression),
   * while still bounding a wildly oversized corrupt or non-streaming buffer.
   */
  val FETCH_BUDGET_SLACK_FACTOR: Long = 4L
}
