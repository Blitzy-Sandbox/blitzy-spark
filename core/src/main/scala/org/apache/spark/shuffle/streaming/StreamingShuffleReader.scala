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

import java.io.ByteArrayInputStream
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.{Aggregator, InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.BlockTransferService
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, ThreadUtils}
import org.apache.spark.util.collection.ExternalSorter

/**
 * The reduce-side reader for the opt-in streaming shuffle backend.
 *
 * ==Relationship to the sort-based reader==
 *
 * This reader deliberately MIRRORS [[org.apache.spark.shuffle.BlockStoreShuffleReader]]: the
 * deserialize, per-record metric, [[CompletionIterator]] / [[InterruptibleIterator]], and
 * aggregation/sort stages are copied verbatim so that aggregated (`aggregator`) and sorted
 * (`keyOrdering`) shuffles behave byte-for-byte identically to the proven sort-based path. The
 * only behavioral difference is the fetch stage: instead of materializing the whole reduce
 * partition before reading, the streaming reader issues "in-progress block requests" -- it pulls
 * one 2 MB [[StreamingBlockEnvelope]] at a time, lazily, as the downstream iterator is consumed.
 *
 * ==Data plane (least-modification, v1)==
 *
 * Per the least-modification directive, the reader reuses the UNCHANGED platform data plane and
 * introduces no parallel transport: block locations come from the unchanged
 * [[org.apache.spark.MapOutputTracker]] and bytes are moved by the existing
 * [[org.apache.spark.network.BlockTransferService]].`fetchBlockSync`. The companion
 * [[StreamingShuffleTransport]] is a v1 logging-only seam: the reader sources its
 * [[org.apache.spark.network.BlockTransferService]] from
 * [[StreamingShuffleTransport.transferService]] (falling back to the
 * [[org.apache.spark.storage.BlockManager]] in local mode) and announces the consumer stream
 * through [[StreamingShuffleTransport.openConsumerStream]], whose empty result is intentionally
 * unused in v1.
 *
 * ==Integrity and failure handling (zero data loss)==
 *
 * Every fetched block arrives as a [[StreamingBlockEnvelope]] and is validated with its CRC32C
 * checksum ([[StreamingBlockEnvelope.verifyChecksum]]); a mismatch is treated as a fetch failure.
 * Each fetch is bounded by the 5 s connection timeout
 * ([[StreamingShuffleConfig.CONNECTION_TIMEOUT_MS]]). On a connection timeout, a corrupt block, or
 * any decode error from a producer, the reader atomically invalidates the partial reads from that
 * producer (incrementing `shuffle.streaming.partialReadInvalidations`), discards the buffered
 * accounting, and raises a [[org.apache.spark.shuffle.FetchFailedException]]. Spark's existing
 * lineage / recompute machinery then resubmits the upstream stage and the lost output is
 * regenerated, so no records are silently dropped.
 *
 * ==SPARK-19276 invariant==
 *
 * [[org.apache.spark.shuffle.FetchFailedException]]'s constructor registers the fetch failure on
 * the [[TaskContext]], so it must be thrown immediately after construction and never created,
 * inspected, and conditionally ignored. All construction is funneled through
 * [[invalidatePartialRead]], whose return type is `Nothing` precisely because it always throws.
 *
 * @tparam K the key type produced for the reduce task
 * @tparam C the combined-value type produced for the reduce task
 * @param handle           the streaming shuffle handle carrying the dependency and tuning values
 * @param startMapIndex    inclusive start of the map-output index range to read
 * @param endMapIndex      exclusive end of the map-output index range to read
 * @param startPartition   inclusive start of the reduce-partition range to read
 * @param endPartition     exclusive end of the reduce-partition range to read
 * @param context          the task context for this reduce task (metrics, cancellation, cleanup)
 * @param readMetrics      the reporter used to publish shuffle-read metrics
 * @param config           the typed streaming-shuffle configuration (debug flag, invariants)
 * @param streamingMetrics the streaming telemetry holder (partial-read-invalidation counter)
 * @param transport        the v1 logging-only transport seam (source of the transfer service)
 * @param backpressure     the flow-control state machine; the reader records producer liveness on
 *                         every inbound block and consults its producer-timeout flag to fail fast
 * @param serializerManager stream wrapper for (de)compression and encryption; defaults to the env
 * @param blockManager     the block manager; defaults to the running env's block manager
 * @param mapOutputTracker the unchanged map-output tracker; defaults to the running env's tracker
 * @param backpressureEndpoint the executor-local backpressure RPC endpoint reference supplied by
 *                         `StreamingShuffleManager` when this reader runs on an executor, or
 *                         [[scala.None]] on the driver / in local mode (where no endpoint is
 *                         registered). When defined, the reader closes the consumer->producer
 *                         control loop by delivering remote heartbeats and byte acks to a
 *                         co-located producer's [[BackpressureProtocol]] through this reference.
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
    backpressure: BackpressureProtocol,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    backpressureEndpoint: Option[RpcEndpointRef] = None)
  extends ShuffleReader[K, C] with Logging {

  /** The shuffle dependency, mirroring `BlockStoreShuffleReader`'s `dep`. */
  private val dep = handle.dependency

  /** The shuffle id this reduce task reads from. */
  private val shuffleId: Int = handle.shuffleId

  /**
   * Per-producer tally of payload bytes read so far during this `read()`. It backs the
   * "discard partial buffers from the failed producer" semantics: on an invalidation the tally is
   * logged and cleared, making the discarded volume observable. It is touched only by the single
   * reduce-task thread that drives the lazy fetch iterator, so it needs no synchronization.
   */
  private val partialReadBytesByProducer = mutable.HashMap.empty[BlockManagerId, Long]

  /**
   * Streams to which this reader has already sent a consumer rate-limit request, so the
   * operator-configured bandwidth cap is propagated to each co-located producer exactly once per
   * stream rather than on every fetched block. Empty (and never written) when no bandwidth cap is
   * configured -- the default -- so the rate-limit lever is silent by default. Concurrent-safe out
   * of caution even though the lazy fetch iterator is driven by a single reduce-task thread.
   */
  private val rateLimitRequestedStreams =
    ConcurrentHashMap.newKeySet[BackpressureProtocol.StreamKey]()

  /**
   * Reads the combined key-values for this reduce task.
   *
   * The fetch stage streams blocks lazily with per-block CRC32C validation and a 5 s connection
   * timeout; the deserialize and aggregation/sort stages are identical to
   * [[org.apache.spark.shuffle.BlockStoreShuffleReader.read]].
   *
   * @return an interruptible iterator over the reduce task's combined `(key, value)` pairs
   */
  override def read(): Iterator[Product2[K, C]] = {
    // One structured, correlation-tagged line per reduce task. Per-block detail is logged only at
    // DEBUG (and only when streaming debug is enabled), keeping volume under the executor budget.
    logInfo(log"Streaming shuffle read starting: " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} " +
      log"maps=[${MDC(START_INDEX, startMapIndex)}, ${MDC(END_INDEX, endMapIndex)}) " +
      log"range=${MDC(StreamingShuffleLogKeys.REDUCE_PARTITION_RANGE,
        s"[$startPartition,$endPartition)")} " +
      log"attempt=${MDC(StreamingShuffleLogKeys.ATTEMPT_ID, context.taskAttemptId())}")

    // v1 logging-only seam: announce the consumer stream on the transport. By design (AAP 0.4.4)
    // it returns an empty iterator -- the real data plane is `fetchBlockSync` below -- so its
    // result is intentionally not consumed here.
    transport.openConsumerStream(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    // (1)-(2) Resolve block locations through the UNCHANGED MapOutputTracker. Streaming shuffle is
    // not push-based, so the plain call is the norm; the push-based variant is used only when the
    // dependency was merge-finalized, exactly mirroring the sort-based read path.
    val blocksByAddress =
      if (dep.isShuffleMergeFinalizedMarked) {
        mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
          shuffleId, startMapIndex, endMapIndex, startPartition, endPartition).iter
      } else {
        mapOutputTracker.getMapSizesByExecutorId(
          shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
      }

    // Reuse the existing transfer service (no parallel transport): prefer the one held by the
    // transport seam and fall back to the block manager's service in local mode / on the driver.
    val transferService = transport.transferService.getOrElse(blockManager.blockTransferService)

    // A dedicated daemon executor lets each block fetch be bounded by the 5 s connection timeout
    // via ThreadUtils.awaitResult. It is shut down when the task completes, so no thread or heap
    // is retained after the read finishes (or fails).
    val fetchPool = ThreadUtils.newDaemonSingleThreadExecutor(
      s"streaming-shuffle-reader-$shuffleId-$startPartition")
    val fetchEc: ExecutionContext = ExecutionContext.fromExecutorService(fetchPool)
    context.addTaskCompletionListener[Unit](_ => fetchPool.shutdownNow())

    // (3)-(4) Lazily fetch each Spark shuffle block and decode EVERY 2 MB frame it carries. A
    // fetched block for a multi-block partition is the in-order concatenation of one verified
    // StreamingBlockEnvelope per 2 MB frame, so a single fetch can yield several envelopes; each is
    // flattened into the stream so trailing frames are never dropped (zero-data-loss). The iterator
    // is lazy, so a block is fetched only as the consumer pulls it ("in-progress block requests");
    // a timeout, a malformed/trailing frame, or a CRC failure aborts the whole read via
    // FetchFailedException.
    val blockStream: Iterator[(BlockId, StreamingBlockEnvelope)] =
      blocksByAddress.flatMap { case (address, blocks) =>
        blocks.iterator.flatMap { case (blockId, _, mapIndex) =>
          fetchEnvelopes(transferService, fetchEc, address, blockId, mapIndex)
            .iterator.map(envelope => (blockId, envelope))
        }
      }

    val serializerInstance = dep.serializer.newInstance()

    // (5) Build a key/value iterator from each verified payload, wrapping the payload stream with
    // the serializer manager exactly as BlockStoreShuffleReader wraps fetched streams.
    val recordIter = blockStream.flatMap { case (blockId, envelope) =>
      val payloadStream =
        serializerManager.wrapStream(blockId, new ByteArrayInputStream(envelope.payload))
      serializerInstance.deserializeStream(payloadStream).asKeyValueIterator
    }

    // (6) Update the task read metrics per record, then make the iterator interruptible so the
    // task can be cancelled -- identical to BlockStoreShuffleReader.read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation.
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // (7) Apply aggregation / sort honoring keyOrdering, aggregator, and mapSideCombine. Copied
    // verbatim from BlockStoreShuffleReader.read so aggregated/sorted shuffles are identical.
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
          // We are reading values that are already combined
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // We don't know the value type, but also don't care -- the dependency *should*
          // have made sure its compatible w/ this aggregator, which will convert the value
          // type to the combined type C
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
   * Fetches one Spark shuffle block and decodes EVERY 2 MB frame it carries into a sequence of
   * verified [[StreamingBlockEnvelope]]s, bounded by the 5 s connection timeout.
   *
   * A fetched block for a multi-block partition is the in-order concatenation of one envelope per
   * 2 MB frame (the canonical dual-channel encoding shared with the spill path), so this method
   * decodes all of them via [[StreamingBlockEnvelope.parseAll]] rather than only the first frame;
   * dropping trailing frames would silently lose data. The block is fetched over the UNCHANGED
   * transfer service on a dedicated daemon thread so that [[ThreadUtils.awaitResult]] can enforce
   * the connection-timeout deadline. A timeout, a malformed or trailing frame, or any frame's CRC
   * mismatch is converted -- via [[invalidatePartialRead]] -- into a fetch failure so Spark
   * recomputes the lost output (zero data loss).
   *
   * @param transferService the block transfer service performing the actual fetch
   * @param fetchEc         the execution context backing the timeout-bounded fetch
   * @param address         the producer (map-side) block manager serving the block
   * @param blockId         the shuffle block id to fetch
   * @param mapIndex        the map index of the block, forwarded to the failure on a timeout
   * @return the parsed, checksum-verified envelopes for the requested block, in wire order
   */
  private def fetchEnvelopes(
      transferService: BlockTransferService,
      fetchEc: ExecutionContext,
      address: BlockManagerId,
      blockId: BlockId,
      mapIndex: Int): Seq[StreamingBlockEnvelope] = {
    val (mapId, reduceId) = blockId match {
      case ShuffleBlockId(_, m, r) => (m, r)
      case _ => (-1L, startPartition)
    }

    // Wire the explicit backpressure producer-timeout state machine into the read path. The 5 s
    // `fetchBlockSync` deadline below remains the PRIMARY v1 timeout; this is a SECOND, scan-driven
    // signal that COEXISTS with it. `registerStream` starts the producer-timeout clock for this
    // (shuffleId, mapId, reduceId) stream (idempotent), and a pre-fetch `isProducerTimedOut` check
    // fails the read fast via the `invalidatePartialRead` path when the 1 s liveness scan
    // has already declared this producer dead (e.g. missed heartbeats across earlier blocks or an
    // inbound Timeout RPC), so a stalled producer need not wait out a fresh 5 s fetch deadline.
    val streamKey = BackpressureProtocol.StreamKey(shuffleId, mapId, reduceId)
    backpressure.registerStream(streamKey)
    if (backpressure.isProducerTimedOut(streamKey)) {
      invalidatePartialRead(address, mapId, mapIndex, reduceId,
        "Streaming shuffle partial read invalidated: backpressure declared the producer timed out",
        null)
    }

    val host = address.host
    val port = address.port
    val execId = address.executorId
    val blockName = blockId.name

    // Run the (internally unbounded) blocking fetch on the daemon executor and await it with the
    // 5 s connection-timeout deadline. awaitResult surfaces a TimeoutException on the deadline and
    // wraps other fetch errors; both are NonFatal and trigger partial-read invalidation.
    val fetchFuture = Future(
      transferService.fetchBlockSync(host, port, execId, blockName, null))(fetchEc)
    val managedBuffer =
      try {
        ThreadUtils.awaitResult(fetchFuture,
          Duration(StreamingShuffleConfig.CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      } catch {
        case NonFatal(e) =>
          invalidatePartialRead(address, mapId, mapIndex, reduceId,
            "Streaming shuffle partial read invalidated after 5s connection timeout", e)
      }

    // The inbound block is producer-liveness evidence from the consumer's vantage point: refresh
    // the producer-timeout clock (and clear any stale timeout flag) per the protocol's "a heartbeat
    // or an inbound block observed on the consumer side" contract, so a producer that keeps
    // delivering blocks is never spuriously declared timed out by the background liveness scan.
    backpressure.onHeartbeat(streamKey)

    // Decode EVERY canonical wire frame in the fetched bytes; a malformed frame or trailing bytes
    // that do not form a complete frame are themselves a fetch failure (parseAll throws). The
    // managed buffer is always released, even when decoding throws.
    val envelopes =
      try {
        StreamingBlockEnvelope.parseAll(managedBuffer.nioByteBuffer())
      } catch {
        case NonFatal(e) =>
          invalidatePartialRead(address, mapId, mapIndex, reduceId,
            "Streaming shuffle block decode failed; treating as fetch failure", e)
      } finally {
        managedBuffer.release()
      }

    // Verify each 2 MB frame's CRC32C; any mismatch is a fetch failure (recompute -> zero data
    // loss). Validation runs over all frames so a corrupt trailing block cannot slip through.
    envelopes.foreach { envelope =>
      if (!envelope.verifyChecksum) {
        invalidatePartialRead(address, mapId, mapIndex, reduceId,
          "Streaming shuffle block failed CRC32C validation; treating as fetch failure", null)
      }
    }

    // Account for the read bytes (used by the discard-on-invalidation path) and publish read
    // metrics through the standard reporter: one remote block was fetched, and its byte count is
    // the sum of all decoded frame payloads.
    val totalPayloadBytes = envelopes.map(_.payloadLength.toLong).sum
    partialReadBytesByProducer(address) =
      partialReadBytesByProducer.getOrElse(address, 0L) + totalPayloadBytes
    readMetrics.incRemoteBlocksFetched(1L)
    readMetrics.incRemoteBytesRead(totalPayloadBytes)

    // Close the consumer->producer control loop. The local `backpressure.onHeartbeat(streamKey)`
    // above refreshes THIS reader's protocol view; this additionally delivers a remote Heartbeat
    // and byte Ack to the PRODUCER executor's backpressure endpoint over the real RpcEnv, so the
    // producer's own BackpressureProtocol drives the consumer-timeout and unacked-byte state the
    // StreamingShuffleWriter polls. Fire-and-forget and fully guarded, so a control-plane hiccup
    // can never disturb this data path.
    maybeSendConsumerControl(address, streamKey, totalPayloadBytes)

    if (config.debug) {
      logDebug(log"Fetched streaming block " +
        log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
        log"reduce=${MDC(REDUCE_ID, reduceId)} frames=${MDC(COUNT, envelopes.size)} " +
        log"bytes=${MDC(NUM_BYTES, totalPayloadBytes)}")
    }
    envelopes
  }

  /**
   * Closes the consumer->producer backpressure loop for a fetched block by delivering a remote byte
   * [[BackpressureRpcEndpoint.Ack]] and [[BackpressureRpcEndpoint.Heartbeat]] to the producer's
   * backpressure endpoint -- but ONLY when (a) this reader was supplied an endpoint reference
   * (executor mode; it is [[scala.None]] on the driver and in local mode where no endpoint is
   * registered) and (b) the producer is co-located on THIS executor, so the manager-supplied
   * executor-local endpoint genuinely is the producer's mailbox. Resolving a remote producer's
   * endpoint from map-output metadata is a documented v2 item (AAP 0.5.2). Delivery is
   * fire-and-forget and guarded inside [[BackpressureRpcSender]], so it never affects the read.
   *
   * @param address    the producer block-manager location the block was fetched from
   * @param streamKey  the (shuffleId, mapId, reduceId) stream just read
   * @param bytesAcked the decoded payload byte count to acknowledge to the producer
   */
  private def maybeSendConsumerControl(
      address: BlockManagerId,
      streamKey: BackpressureProtocol.StreamKey,
      bytesAcked: Long): Unit = {
    backpressureEndpoint.foreach { ref =>
      if (isColocatedProducer(address)) {
        BackpressureRpcSender.sendAck(ref, streamKey, bytesAcked)
        BackpressureRpcSender.sendHeartbeat(ref, streamKey)
        // Consumer->producer rate-limit lever: propagate the operator-configured per-executor
        // bandwidth cap to the producer's token bucket exactly once per stream. Skipped entirely
        // when bandwidth is unlimited (the default), so it adds no control traffic unless an
        // operator opts into a cap; `add` returns false on repeats, bounding this to one send per
        // stream regardless of how many blocks the stream yields.
        if (!config.isBandwidthUnlimited && rateLimitRequestedStreams.add(streamKey)) {
          BackpressureRpcSender.sendRateLimitRequest(
            ref, streamKey, config.effectiveBandwidthBytesPerSec)
        }
      }
    }
  }

  /**
   * @param address the producer block-manager location a block was fetched from
   * @return whether that producer ran on THIS executor -- i.e. whether the executor-local
   *         backpressure endpoint reference is also the producer's mailbox. Null-guards a missing
   *         block manager (as a mock supplies in unit tests) so the control-plane send is simply
   *         skipped rather than failing the read.
   */
  private def isColocatedProducer(address: BlockManagerId): Boolean = {
    blockManager != null && blockManager.blockManagerId != null &&
      address.executorId == blockManager.blockManagerId.executorId
  }

  /**
   * Invalidates the partial reads accumulated from a failed producer and raises a
   * [[org.apache.spark.shuffle.FetchFailedException]] so Spark recomputes the lost map output.
   *
   * This method always throws (hence the `Nothing` result), satisfying the SPARK-19276 contract:
   * the exception registers the failure on the [[TaskContext]] in its constructor, so it is
   * constructed and thrown in one indivisible step and never inspected-then-ignored. The
   * partial-read-invalidation counter is incremented and the per-producer byte tally is cleared
   * (the "discard" step) before the exception is thrown.
   *
   * @param address  the producer block manager that failed; passed as the failure's `bmAddress`
   * @param mapId    the failed block's map (producer) task id
   * @param mapIndex the failed block's map index
   * @param reduceId the reduce partition being read when the failure occurred
   * @param message  the human-readable failure message attached to the exception
   * @param cause    the underlying cause, or `null` when none applies (e.g. a checksum mismatch)
   * @return never returns normally; it always throws
   */
  private def invalidatePartialRead(
      address: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable): Nothing = {
    val discardedBytes = partialReadBytesByProducer.values.sum
    partialReadBytesByProducer.clear()
    streamingMetrics.incPartialReadInvalidations()
    logError(log"Streaming shuffle partial read invalidated; discarding partial reads. " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
      log"reduce=${MDC(REDUCE_ID, reduceId)} producer=${MDC(HOST_PORT, address.hostPort)} " +
      log"discardedBytes=${MDC(NUM_BYTES, discardedBytes)}", cause)
    throw new FetchFailedException(address, shuffleId, mapId, mapIndex, reduceId, message, cause)
  }
}
