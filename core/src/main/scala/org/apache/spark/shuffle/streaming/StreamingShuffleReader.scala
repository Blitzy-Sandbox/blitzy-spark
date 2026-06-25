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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
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
 * F-115); this reader is nonetheless a complete, production-grade implementation of the consumer
 * protocol and validation discipline, fetching through the existing block transfer service so it
 * is correct end-to-end the moment the Netty data plane lands.
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
    initialRetryBackoffMs: Long = StreamingShuffleReader.INITIAL_RETRY_BACKOFF_MS)
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
        blockInfos.iterator.map { case (blockId, _, mapIndex) =>
          val payloadStream = fetchValidateAndAck(address, blockId, mapIndex)
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
   * @param address  producer block-manager id; the fetch source and failure-report location
   * @param blockId  the shuffle block id to fetch
   * @param mapIndex the map index of the producing task (reported on invalidation)
   * @return a stream positioned at the validated, concatenated block payload
   */
  private def fetchValidateAndAck(
      address: BlockManagerId,
      blockId: BlockId,
      mapIndex: Int): InputStream = {
    val (mapId, reduceId) = mapAndReduceId(blockId)
    val buffer = fetchWithRetry(address, blockId, mapId, mapIndex, reduceId)
    val rawBytes =
      try {
        readFully(buffer)
      } catch {
        case NonFatal(e) =>
          invalidateAndThrow(address, mapId, mapIndex, reduceId,
            s"Failed to read fetched bytes for streaming block $blockId: ${e.getMessage}", e)
      }
    val payload = decodeAndValidate(rawBytes, address, mapId, mapIndex, reduceId)
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
      try {
        val buf = blockManager.blockTransferService.fetchBlockSync(
          address.host, address.port, address.executorId, blockId.toString, null)
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
   * Read the entire contents of a [[ManagedBuffer]] into a byte array, releasing the buffer's
   * reference even if reading fails. The full payload is needed up front so the CRC32C
   * can be validated before any byte is handed to deserialization.
   */
  private def readFully(buffer: ManagedBuffer): Array[Byte] = {
    try {
      val nio = buffer.nioByteBuffer()
      val bytes = new Array[Byte](nio.remaining())
      nio.get(bytes)
      bytes
    } finally {
      buffer.release()
    }
  }

  /**
   * Decode the fetched bytes as one or more concatenated [[StreamingBlockEnvelope]] frames,
   * validating each frame's CRC32C, and return the concatenated validated payloads. A producer
   * frames a partition into consecutive blocks of at most [[StreamingBlockEnvelope.HEADER_SIZE]]
   * plus 2 MiB, so a single fetched block may contain several frames. Any structural decode error
   * or checksum mismatch invalidates the read (which throws); a mismatch is, by design, not
   * retransmitted in v1.
   */
  private def decodeAndValidate(
      bytes: Array[Byte],
      address: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int): Array[Byte] = {
    val assembled =
      new ByteArrayOutputStream(math.max(bytes.length, StreamingBlockEnvelope.HEADER_SIZE))
    var offset = 0
    while (offset < bytes.length) {
      val slice = ByteBuffer.wrap(bytes, offset, bytes.length - offset)
      val envelope =
        try {
          StreamingBlockEnvelope.decode(slice)
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
      offset += StreamingBlockEnvelope.HEADER_SIZE + envelope.payloadLength
    }
    assembled.toByteArray()
  }

  /**
   * Run the consumer-side acknowledgment protocol for a validated block: advance the ack
   * high-water mark, return the consumed bytes to the flow-control credit window so the producer
   * can reclaim its buffer (within the 100 ms reclaim SLA), and stamp consumer liveness via a
   * heartbeat. In v1 these update the local [[BackpressureProtocol]] state; the cross-executor
   * carrier of these signals is the backpressure RPC endpoint (feature F-108).
   */
  private def acknowledge(blockId: BlockId, payloadBytes: Int): Unit = {
    val seqNo = ackSequence.incrementAndGet()
    backpressure.mergeAck(seqNo)
    backpressure.refill(payloadBytes.toLong)
    backpressure.recordHeartbeat()
    logDebug(s"Acknowledged streaming block $blockId (ackSeq=$seqNo, $payloadBytes bytes) " +
      s"for shuffle $shuffleId")
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
    logWarning(s"Invalidating partial streaming read for shuffle $shuffleId (map $mapId, " +
      s"mapIndex $mapIndex, reduce $reduceId); deferring to DAG-scheduler recomputation. " +
      s"Reason: $reason")
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
      logWarning(s"Unexpected non-ShuffleBlockId $other for streaming shuffle $shuffleId")
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
}

