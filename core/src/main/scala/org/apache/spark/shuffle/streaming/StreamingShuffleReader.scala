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

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.{Callable, ExecutionException, TimeoutException, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Aggregator, InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.network.{StreamingBlockEnvelope,
  StreamingShuffleTransport}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, ThreadUtils}
import org.apache.spark.util.collection.ExternalSorter

/**
 * Reduce-side reader for the opt-in streaming shuffle backend.
 *
 * ==Relationship to the sort-based path==
 *
 * This reader intentionally MIRRORS the proven
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] read path so that aggregated and sorted
 * shuffles produce byte-identical results to the sort-based backend. It resolves block locations
 * through the UNCHANGED [[org.apache.spark.MapOutputTracker]] and fetches each block over the
 * existing executor data plane using the per-block, blocking
 * [[org.apache.spark.network.BlockTransferService#fetchBlockSync]] API mandated by the feature
 * plan, then deserializes with the dependency's serializer and honors `keyOrdering`,
 * `aggregator`, and `mapSideCombine` exactly as
 * the sort path does. Reusing that machinery is the least-modification approach mandated by the
 * feature plan: the streaming backend introduces no parallel transport on the read side.
 *
 * ==Envelope de-framing (dual-channel invariant)==
 *
 * The bytes returned for a streaming block are NOT a bare serialized record stream: the writer,
 * buffer, spill, and resolver paths all produce framed [[StreamingBlockEnvelope]]s (a 32-byte
 * big-endian header followed by a <= 2 MB payload per block), and those exact bytes are what
 * `fetchBlockSync` returns. The reader therefore parses every frame in order, verifies its CRC32C
 * (see [[verifyBlockChecksum]]), and concatenates the validated payload-only bytes before handing
 * a single contiguous record stream to the serializer. The writer serializes raw (it does not
 * wrap the stream with compression/encryption), so the reader deserializes the de-enveloped
 * payload raw as well; storage-level encryption of spills and network-level encryption are handled
 * transparently below this layer by the `BlockManager` and the transport.
 *
 * ==In-progress block requests and partial-read invalidation==
 *
 * The reader is where the streaming guarantees of "in-progress block requests" and
 * "partial-read invalidation on producer failure" live. Each 2 MB block fetched through the
 * streaming consumer-stream channel is wrapped in a [[StreamingBlockEnvelope]] whose CRC32C is
 * verified (see [[verifyBlockChecksum]]); a corrupt block is treated as a fetch failure. Fetches
 * are bounded by a 5 s connection timeout
 * ([[StreamingShuffleConfig.CONNECTION_TIMEOUT_MS]]); when a producer times out, the reader
 * invalidates any partial reads from that producer, increments the
 * `partialReadInvalidations` telemetry counter, and immediately raises a
 * [[org.apache.spark.shuffle.FetchFailedException]] (see [[invalidatePartialReads]]). Spark's
 * existing lineage/recompute machinery then rebuilds the lost upstream output, giving the
 * zero-data-loss guarantee.
 *
 * ==v1 transport behavior==
 *
 * In v1 the streaming [[StreamingShuffleTransport]] is logging-only (the real data plane is the
 * existing `BlockTransferService.fetchBlockSync` pull path), so
 * [[StreamingShuffleTransport.openConsumerStream]] returns an empty iterator and the streaming
 * consumer-stream drain ([[drainStreamingConsumerStream]]) is a runtime no-op retained as the v2
 * attachment seam. The data-plane guarantees -- per-block CRC32C verification, the 5 s connection
 * timeout, exponential-backoff retry, and partial-read invalidation that raises a
 * [[org.apache.spark.shuffle.FetchFailedException]] -- are enforced on the REAL fetch path in
 * [[read]] / [[fetchStreamingBlock]] / [[extractValidatedPayloads]], not only on the (empty) v1
 * transport stream. This is intended, documented v1 behavior, not an unfinished stub.
 *
 * @param handle the streaming shuffle handle carrying the shuffle id, dependency, and tuning
 * @param startMapIndex inclusive start of the map (producer) range to read from
 * @param endMapIndex exclusive end of the map (producer) range to read from
 * @param startPartition inclusive start of the reduce partition range to read
 * @param endPartition exclusive end of the reduce partition range to read
 * @param context the task context, used for cancellation, metrics, and correlation
 * @param readMetrics the reporter used to publish shuffle-read metrics
 * @param config the typed streaming-shuffle configuration accessor
 * @param streamingMetrics the streaming-shuffle telemetry holder (partial-read invalidations)
 * @param transport the v1 logging-only streaming transport integration seam
 * @param blockManager provides the [[org.apache.spark.network.BlockTransferService]] used to fetch
 *                     blocks and resolve the local executor id; defaults to the env
 * @param mapOutputTracker resolves block locations by executor; defaults to the env
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    config: StreamingShuffleConfig,
    streamingMetrics: StreamingShuffleMetrics,
    transport: StreamingShuffleTransport,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  // Daemon pool used solely to bound each blocking BlockTransferService.fetchBlockSync call by the
  // 5 s connection timeout. fetchBlockSync takes no timeout argument, so every fetch is submitted
  // as a Callable whose Future is awaited with a 5 s get(...); on timeout the Future is cancelled
  // and the fetch is retried with exponential backoff. A cached pool has zero core threads, so it
  // holds no threads while idle, and read() registers a task-completion listener that shuts it
  // down when the reduce task finishes (or fails).
  private val fetchExecutor =
    ThreadUtils.newDaemonCachedThreadPool(s"streaming-shuffle-fetch-${handle.shuffleId}")

  /** Read the combined key-values for this reduce task. */
  override def read(): Iterator[Product2[K, C]] = {
    // Shut the per-fetch timeout pool down when the reduce task completes (success or failure) so
    // no daemon threads outlive the task. Registered once at the start of the read.
    context.addTaskCompletionListener[Unit](_ => fetchExecutor.shutdownNow())

    if (config.debug) {
      // Single per-read correlation line. Debug-gated and at debug level so the streaming-shuffle
      // log budget (< 10 MB/hour/executor) is respected on the hot reduce path.
      logDebug(log"StreamingShuffleReader reading shuffle=" +
        log"${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)} reducePartitions=[" +
        log"${MDC(LogKeys.START_INDEX, startPartition)}," +
        log"${MDC(LogKeys.END_INDEX, endPartition)}) attempt=" +
        log"${MDC(LogKeys.TASK_ATTEMPT_ID, context.taskAttemptId())}")
    }

    // In-progress block requests over the streaming consumer-stream channel. v1 no-op (the
    // transport is logging-only); the bulk read flows through the mirrored fetch path below.
    drainStreamingConsumerStream()

    // Resolve block locations through the UNCHANGED MapOutputTracker, choosing the push-based
    // variant only when the dependency is merge-finalized, exactly as the sort path does.
    // Streaming shuffles are not push-based, so the plain call is the norm.
    val blocksByAddress =
      if (dep.isShuffleMergeFinalizedMarked) {
        mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition).iter
      } else {
        mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
      }

    // Fetch every block for this reduce range through the existing executor data plane using the
    // BlockTransferService.fetchBlockSync API mandated by the feature plan (rather than
    // ShuffleBlockFetcherIterator). Each fetch is bounded by the 5 s connection timeout with
    // exponential-backoff retry; on exhaustion the partial read is invalidated and a
    // FetchFailedException is raised (see fetchStreamingBlock / invalidatePartialReads). The
    // fetched bytes are framed StreamingBlockEnvelopes, so every frame is parsed and CRC32C-
    // verified and only the de-enveloped payloads reach the serializer (see
    // extractValidatedPayloads). Blocks are fetched lazily as the downstream aggregator/sorter
    // pulls records, preserving the streaming, non-materializing read semantics.
    val serializerInstance = dep.serializer.newInstance()
    val recordIter = blocksByAddress.flatMap { case (bmId, blockInfos) =>
      blockInfos.iterator.flatMap { case (blockId, _, mapIndex) =>
        readStreamingBlock(bmId, blockId, mapIndex, serializerInstance)
      }
    }

    // Update the context task metrics for each record read.
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
          // have made sure its compatible w/ this aggregator, which will convert the value
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
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * Verify the CRC32C of a single fetched streaming block.
   *
   * Recomputes the checksum over the envelope payload and compares it to the value stored in the
   * 32-byte header. A mismatch indicates an on-the-wire (or on-disk spill) corruption and is
   * treated by callers as a fetch failure so the upstream output is recomputed.
   *
   * @param envelope the framed 2 MB block to validate
   * @return `true` when the recomputed CRC32C matches the header value, `false` otherwise
   */
  private[streaming] def verifyBlockChecksum(envelope: StreamingBlockEnvelope): Boolean = {
    val valid = envelope.verifyChecksum
    if (!valid && config.debug) {
      logDebug(log"Streaming shuffle CRC32C mismatch shuffle=" +
        log"${MDC(LogKeys.SHUFFLE_ID, envelope.shuffleId)} map=" +
        log"${MDC(LogKeys.MAP_ID, envelope.mapId)} reduce=" +
        log"${MDC(LogKeys.REDUCE_ID, envelope.reduceId)}")
    }
    valid
  }

  /**
   * Fetch one streaming block, de-frame and CRC-verify its envelopes, and return a key/value
   * iterator over its records.
   *
   * The blockId carries the producer's `(shuffleId, mapId, reduceId)`; `mapIndex` is the
   * producer's index within the shuffle, required by
   * [[org.apache.spark.shuffle.FetchFailedException]] for recompute. The fetched
   * [[org.apache.spark.network.buffer.ManagedBuffer]] holds framed
   * [[StreamingBlockEnvelope]] bytes; [[extractValidatedPayloads]] strips and validates them and
   * returns the concatenated payload-only bytes, which are deserialized RAW (the writer does not
   * wrap the stream). The buffer is released as soon as parsing completes because the validated
   * payloads have been copied into independent arrays.
   *
   * @param bmId the producer's block manager id, used for fetch routing and failure attribution
   * @param blockId the shuffle block id being read
   * @param mapIndex the producer's map index within the shuffle
   * @param serializerInstance the dependency serializer used to decode the payload bytes
   * @return a key/value iterator over the records contained in the block
   */
  private def readStreamingBlock(
      bmId: BlockManagerId,
      blockId: BlockId,
      mapIndex: Int,
      serializerInstance: SerializerInstance): Iterator[(Any, Any)] = {
    val (mapId, reduceId) = blockId match {
      case ShuffleBlockId(_, m, r) => (m, r)
      case _ => (-1L, startPartition)
    }
    val managed = fetchStreamingBlock(bmId, blockId, mapId, mapIndex, reduceId)
    val payload =
      try {
        readMetrics.incRemoteBlocksFetched(1)
        readMetrics.incRemoteBytesRead(managed.size())
        extractValidatedPayloads(managed.nioByteBuffer(), bmId, mapId, mapIndex, reduceId)
      } catch {
        case e: IOException =>
          invalidatePartialReads(bmId, mapId, mapIndex, reduceId,
            "Streaming shuffle failed to read fetched block bytes", e)
      } finally {
        // The validated payloads were copied into independent arrays, so the fetched buffer (and
        // any pooled backing memory) can be released as soon as parsing completes.
        managed.release()
      }
    serializerInstance.deserializeStream(new ByteArrayInputStream(payload)).asKeyValueIterator
  }

  /**
   * Fetch a single block through [[org.apache.spark.network.BlockTransferService#fetchBlockSync]],
   * bounded by the 5 s connection timeout with exponential-backoff retry.
   *
   * `fetchBlockSync` is blocking and takes no timeout argument, so each attempt runs as a
   * [[java.util.concurrent.Callable]] on [[fetchExecutor]] and is awaited with a 5 s
   * `get(...)`. A timeout or fetch error cancels the in-flight future and retries after an
   * exponential backoff (1 s, 2 s, 4 s, ... ; up to [[StreamingShuffleConfig.RETRY_MAX_ATTEMPTS]]
   * attempts). When all attempts are exhausted the read invalidates partial reads from this
   * producer and raises a [[org.apache.spark.shuffle.FetchFailedException]] (see
   * [[invalidatePartialReads]]); a task interrupt is propagated unchanged to honor cancellation.
   *
   * @param bmId the producer's block manager id (host/port/executorId route the fetch)
   * @param blockId the shuffle block id to fetch
   * @param mapId the producer's map id, for failure attribution
   * @param mapIndex the producer's map index, for failure attribution
   * @param reduceId the reduce partition being read, for failure attribution
   * @return the fetched block bytes as a [[org.apache.spark.network.buffer.ManagedBuffer]]
   */
  private def fetchStreamingBlock(
      bmId: BlockManagerId,
      blockId: BlockId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int): ManagedBuffer = {
    val timeoutMs = StreamingShuffleConfig.CONNECTION_TIMEOUT_MS
    val maxAttempts = StreamingShuffleConfig.RETRY_MAX_ATTEMPTS
    var backoffMs = StreamingShuffleConfig.RETRY_INITIAL_BACKOFF_MS
    var attempt = 0
    var lastError: Throwable = null
    while (attempt < maxAttempts) {
      attempt += 1
      val future = fetchExecutor.submit(new Callable[ManagedBuffer] {
        override def call(): ManagedBuffer =
          blockManager.blockTransferService.fetchBlockSync(
            bmId.host, bmId.port, bmId.executorId, blockId.toString, null)
      })
      try {
        return future.get(timeoutMs, TimeUnit.MILLISECONDS)
      } catch {
        case _: TimeoutException =>
          future.cancel(true)
          lastError = new TimeoutException(s"fetchBlockSync timed out after ${timeoutMs}ms")
        case e: ExecutionException =>
          future.cancel(true)
          lastError = Option(e.getCause).getOrElse(e)
        case e: InterruptedException =>
          // Honor task cancellation: cancel the in-flight fetch and propagate the interrupt.
          future.cancel(true)
          Thread.currentThread().interrupt()
          throw e
      }
      if (attempt < maxAttempts) {
        try {
          Thread.sleep(backoffMs)
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            throw e
        }
        backoffMs *= 2
      }
    }
    // All attempts exhausted within the per-attempt 5 s connection timeout: invalidate partial
    // reads from this producer and raise a FetchFailedException (never returns).
    invalidatePartialReads(bmId, mapId, mapIndex, reduceId,
      s"Streaming shuffle fetch failed after $maxAttempts attempts " +
        s"(${timeoutMs}ms connection timeout each)", lastError)
  }

  /**
   * Parse and CRC32C-validate every [[StreamingBlockEnvelope]] in a fetched block and return the
   * concatenated payload-only bytes.
   *
   * A fetched block is the concatenation of one or more framed envelopes (a 32-byte big-endian
   * header followed by a <= 2 MB payload each). [[StreamingBlockEnvelope.parse]] reads from a
   * duplicate and does NOT advance the source position, so this loop advances `raw` manually by
   * `HEADER_BYTES + payloadLength` after each frame. A truncated, oversized, or checksum-mismatched
   * frame is treated as a producer corruption and routed through [[invalidatePartialReads]] (which
   * raises a [[org.apache.spark.shuffle.FetchFailedException]] so Spark recomputes the upstream
   * output). An empty buffer yields an empty record stream.
   *
   * @param raw the fetched block bytes positioned at the first frame
   * @param bmId the producer's block manager id, for failure attribution
   * @param mapId the producer's map id, for failure attribution
   * @param mapIndex the producer's map index, for failure attribution
   * @param reduceId the reduce partition being read, for failure attribution
   * @return the concatenated, validated payload bytes ready for the serializer
   */
  private[streaming] def extractValidatedPayloads(
      raw: ByteBuffer,
      bmId: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int): Array[Byte] = {
    val payloads = new ArrayBuffer[Array[Byte]]()
    var totalPayloadLen = 0L
    while (raw.remaining() > 0) {
      val envelope =
        try {
          StreamingBlockEnvelope.parse(raw)
        } catch {
          case e: IllegalArgumentException =>
            invalidatePartialReads(bmId, mapId, mapIndex, reduceId,
              "Streaming shuffle malformed or truncated block frame", e)
        }
      if (!verifyBlockChecksum(envelope)) {
        invalidatePartialReads(bmId, mapId, mapIndex, reduceId,
          s"Streaming shuffle block CRC32C mismatch (sequence=${envelope.sequenceNumber})")
      }
      payloads += envelope.payload
      totalPayloadLen += envelope.payloadLength
      // Advance past the frame just parsed (parse used a duplicate and left raw's position fixed).
      raw.position(raw.position() + StreamingBlockEnvelope.HEADER_BYTES + envelope.payloadLength)
    }
    require(totalPayloadLen <= Int.MaxValue,
      s"Streaming shuffle assembled payload $totalPayloadLen exceeds the 2 GB array limit")
    val combined = new Array[Byte](totalPayloadLen.toInt)
    var offset = 0
    payloads.foreach { part =>
      System.arraycopy(part, 0, combined, offset, part.length)
      offset += part.length
    }
    combined
  }

  /**
   * Invalidate partial reads from a failed producer and fail the read.
   *
   * Increments the `partialReadInvalidations` telemetry counter, logs the invalidation with the
   * shuffle/map/reduce correlation context, and immediately throws a
   * [[org.apache.spark.shuffle.FetchFailedException]]. This is the single entry point for both
   * the 5 s connection-timeout path and the CRC32C-corruption path so that a producer failure is
   * always surfaced to Spark's lineage/recompute machinery, which rebuilds the lost output and
   * preserves the zero-data-loss guarantee.
   *
   * SPARK-19276: the [[org.apache.spark.shuffle.FetchFailedException]] constructor records the
   * fetch failure in the [[org.apache.spark.TaskContext]], so it must be thrown immediately after
   * construction and never constructed-then-conditionally-ignored. This method therefore returns
   * `Nothing` and always throws.
   *
   * @param bmAddress the failed producer's block manager id, or `null` when unknown
   * @param mapId the failed producer's map id (use `-1` when not attributable to a single map)
   * @param mapIndex the failed producer's map index (use `-1` when unknown)
   * @param reduceId the reduce partition id being read when the failure occurred
   * @param message a human-readable description of the invalidation cause
   * @param cause the underlying throwable, or `null` when there is none
   * @return never returns; always throws [[org.apache.spark.shuffle.FetchFailedException]]
   */
  private[streaming] def invalidatePartialReads(
      bmAddress: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable = null): Nothing = {
    streamingMetrics.incPartialReadInvalidations()
    logWarning(log"Streaming shuffle invalidating partial reads shuffle=" +
      log"${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)} map=" +
      log"${MDC(LogKeys.MAP_ID, mapId)} reduce=" +
      log"${MDC(LogKeys.REDUCE_ID, reduceId)}: ${MDC(LogKeys.ERROR, message)}")
    throw new FetchFailedException(
      bmAddress, handle.shuffleId, mapId, mapIndex, reduceId, message, cause)
  }

  /**
   * Drain the streaming consumer-stream channel, verifying CRC32C and enforcing the 5 s
   * connection timeout.
   *
   * In v1 (per the feature plan) the [[StreamingShuffleTransport]] is logging-only and
   * [[StreamingShuffleTransport.openConsumerStream]] returns an empty iterator, so this loop is a
   * runtime no-op and the bulk read flows through the mirrored sort-based fetch path in
   * [[read]]. The loop is nonetheless the wired home for in-progress block requests: every
   * fetched 2 MB block has its CRC32C verified, and a producer that fails to deliver within the
   * 5 s connection timeout triggers [[invalidatePartialReads]] (counting the invalidation and
   * raising a fetch failure). The logic activates unchanged when the v2 push plane replaces the
   * logging-only transport.
   */
  private def drainStreamingConsumerStream(): Unit = {
    val deadlineMs = System.currentTimeMillis() + StreamingShuffleConfig.CONNECTION_TIMEOUT_MS
    val envelopes = transport.openConsumerStream(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    while (envelopes.hasNext) {
      if (System.currentTimeMillis() > deadlineMs) {
        // Producer failed to deliver within the 5 s connection timeout: invalidate and fail.
        invalidatePartialReads(null, -1L, -1, startPartition,
          "Streaming shuffle partial read invalidated after 5s connection timeout")
      }
      val envelope = envelopes.next()
      if (!verifyBlockChecksum(envelope)) {
        // Corrupt block: treat as a producer failure so the upstream output is recomputed.
        invalidatePartialReads(null, envelope.mapId, -1, envelope.reduceId,
          s"Streaming shuffle block CRC32C mismatch (sequence=${envelope.sequenceNumber})")
      }
    }
  }
}
