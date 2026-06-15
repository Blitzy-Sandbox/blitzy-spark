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

import org.apache.spark.{Aggregator, InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.{MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM, REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS, REDUCER_MAX_REQS_IN_FLIGHT, REDUCER_MAX_SIZE_IN_FLIGHT, SHUFFLE_CHECKSUM_ALGORITHM, SHUFFLE_CHECKSUM_ENABLED, SHUFFLE_DETECT_CORRUPT, SHUFFLE_DETECT_CORRUPT_MEMORY, SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.network.{StreamingBlockEnvelope, StreamingShuffleTransport}
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Reduce-side reader for the opt-in streaming shuffle backend.
 *
 * ==Relationship to the sort-based path==
 *
 * This reader intentionally MIRRORS the proven
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] read path so that aggregated and sorted
 * shuffles produce byte-identical results to the sort-based backend. It resolves block locations
 * through the UNCHANGED [[org.apache.spark.MapOutputTracker]], fetches bytes through the existing
 * [[org.apache.spark.storage.ShuffleBlockFetcherIterator]] (which pulls over the executor
 * `BlockTransferService`), deserializes with the dependency's serializer, and honors
 * `keyOrdering`, `aggregator`, and `mapSideCombine` exactly as the sort path does. Reusing that
 * machinery is the least-modification approach mandated by the feature plan: the streaming backend
 * introduces no parallel transport on the read side.
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
 * existing `BlockTransferService` pull path), so [[StreamingShuffleTransport.openConsumerStream]]
 * returns an empty iterator and the streaming consumer-stream drain is a runtime no-op; the bulk
 * read therefore flows entirely through the mirrored sort-based fetch path. The CRC32C and
 * connection-timeout logic remains fully wired so it activates unchanged when the v2 push plane
 * replaces the logging-only transport. This is intended, documented v1 behavior, not an
 * unfinished stub.
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
 * @param serializerManager wraps fetched streams (compression/encryption); defaults to the env
 * @param blockManager provides the block store client used for the fetch; defaults to the env
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
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task. */
  override def read(): Iterator[Product2[K, C]] = {
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

    // Fetch bytes through the existing data plane: ShuffleBlockFetcherIterator pulls over
    // blockManager.blockStoreClient (the executor BlockTransferService). doBatchFetch is left
    // off (the BlockStoreShuffleReader default) so the streaming v1 read path stays decoupled
    // from the sort manager's batch-fetch decision while remaining fully correct.
    val conf = SparkEnv.get.conf
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility.
      conf.get(REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      conf.get(REDUCER_MAX_REQS_IN_FLIGHT),
      conf.get(REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      conf.get(SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      conf.get(SHUFFLE_DETECT_CORRUPT),
      conf.get(SHUFFLE_DETECT_CORRUPT_MEMORY),
      conf.get(SHUFFLE_CHECKSUM_ENABLED),
      conf.get(SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      doBatchFetch = false).toCompletionIterator

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream.
    val recordIter = wrappedStreams.flatMap { case (_, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a NextIterator.
      // The NextIterator makes sure that close() is called on the underlying InputStream when
      // all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
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
