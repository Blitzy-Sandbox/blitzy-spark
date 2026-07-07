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

import java.io.InputStream
import java.net.{SocketException, SocketTimeoutException}
import java.util.concurrent.{TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection
import scala.util.control.NonFatal

import org.apache.spark.{Aggregator, InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockBatchId,
  ShuffleBlockFetcherIterator, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * The reduce-side reader for the streaming shuffle backend.
 *
 * This reader is the streaming counterpart of
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] and deliberately mirrors that reader's
 * combine and ordering pipeline byte-for-byte: it fetches blocks through a
 * [[ShuffleBlockFetcherIterator]], deserializes each stream into key/value records, honors
 * `dep.aggregator` (map-side or reduce-side combine), applies `dep.keyOrdering` through an
 * [[ExternalSorter]] when a sort ordering is defined, and composes every stage lazily so no
 * records are materialized eagerly. Preserving these semantics guarantees that a shuffle read
 * produces identical results whether the sort or the streaming backend is selected -- which is the
 * cornerstone of the streaming feature's zero-regression coexistence with the sort path.
 *
 * On top of the mirrored pipeline the streaming reader adds '''in-progress reads''' with
 * '''partial-read invalidation'''. Producer (map) executors may still be streaming when a reduce
 * task begins consuming, so a producer can fail after the reader has already read part of a block.
 * Every fetched stream is therefore wrapped so that a producer connection that fails or stalls for
 * longer than `PRODUCER_CONNECTION_TIMEOUT_MS` (5 seconds) is detected. On such a timeout the
 * reader (1) increments `StreamingShuffleMetrics.incPartialReadInvalidations`, (2) atomically
 * discards the partial buffer already read for the failed block, and (3) throws a standard
 * [[FetchFailedException]].
 *
 * Throwing [[FetchFailedException]] is the '''only''' point at which this reader touches the
 * scheduler. Its constructor sets the fetch-failed flag on the current [[TaskContext]]
 * (SPARK-19276), and the DAGScheduler then recomputes the upstream map stage through the existing
 * fault path with no scheduler code change. Because of that side effect the exception is always
 * constructed and thrown in a single statement; it is never built, stored, and conditionally
 * re-checked.
 *
 * The reader consumes only public, unmodified engine APIs -- [[MapOutputTracker]] locations are
 * resolved by the caller (`StreamingShuffleManager.getReader`) and passed in as `blocksByAddress`,
 * and blocks are fetched with the existing [[ShuffleBlockFetcherIterator]] over the reused
 * `BlockTransferService`. It changes nothing in the sort path or the scheduler.
 *
 * Block integrity in v1: because fetching reuses [[ShuffleBlockFetcherIterator]], per-block
 * corruption detection is provided by Spark's existing shuffle checksum (governed by
 * `spark.shuffle.checksum.enabled` / `spark.shuffle.checksum.algorithm`) on that reused fetch
 * path -- the same integrity mechanism the sort path relies on. This reader does not parse
 * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] or call its CRC32C
 * `verifyChecksum`; the envelope's verify-and-retransmit path belongs to the v2 wire transport
 * and is deferred until that transport streams enveloped blocks (Architectural Decision Log #2 --
 * the v1 transport is an intentional logging-only stub). Consequently v1 does not yet perform
 * envelope-level CRC32C verification or block retransmission on the read side.
 *
 * Producer-connection retry in v1: like the envelope's CRC32C path above, the mandated
 * exponential-backoff retry (1 s start, doubling, up to 5 attempts) is implemented and unit-tested
 * as an isolated transport primitive --
 * [[org.apache.spark.shuffle.streaming.network.StreamingShuffleRetryPolicy]] -- and is wired into
 * [[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport.send]]. This reader itself
 * performs '''no''' per-block retry: on a producer-connection timeout it invalidates the partial
 * read and throws [[FetchFailedException]] so the DAGScheduler recomputes the upstream stage.
 * Because the v1 transport is a logging-only stub that raises no retriable connection failure, the
 * retry loop turns over only once the v2 wire transport can raise one; v1 reduce-side recovery is
 * driven entirely by standard DAG recomputation (Architectural Decision Log #2).
 *
 * @tparam K the type of the keys being read
 * @tparam C the type of the combined values produced for the reduce task
 * @param handle the streaming shuffle handle carrying the shuffle dependency and resource envelope
 * @param blocksByAddress the producer blocks to fetch, grouped by [[BlockManagerId]]; each entry is
 *                        `(blockId, approximate size in bytes, mapIndex)`
 * @param context the [[TaskContext]] of the running reduce task, used for metrics and cancellation
 * @param readMetrics the reporter used to record shuffle-read metrics for this task
 * @param streamingMetrics the streaming telemetry sink; receives partial-read invalidations
 * @param conf the typed streaming configuration accessor; gates DEBUG logging via `conf.debug`
 * @param serializerManager the serializer manager used to wrap and decode fetched streams
 * @param blockManager the block manager providing the block store client for fetches
 * @param mapOutputTracker the map-output tracker passed through to the fetch iterator
 * @param shouldBatchFetch whether contiguous shuffle blocks may be fetched in a single batch
 */
@Since("4.2.0")
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    streamingMetrics: StreamingShuffleMetrics,
    conf: StreamingShuffleConfig,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C] with Logging {

  import StreamingShuffleReader.{MAX_CAUSE_DEPTH, PRODUCER_CONNECTION_TIMEOUT_MS, UNKNOWN_MAP_ID,
    UNKNOWN_MAP_INDEX, UNKNOWN_REDUCE_ID}

  private val dep = handle.dependency

  // The producer block descriptors, materialized once. This captures only lightweight metadata
  // (BlockManagerId, BlockId, size, mapIndex) -- never shuffle records -- so the record path below
  // stays fully lazy. The fetch iterator itself drains blocksByAddress eagerly at construction, so
  // materializing it here first does not change streaming behavior; it merely lets us report the
  // exact producer coordinates when a producer connection times out mid-read.
  private val blocksByAddressSeq = blocksByAddress.toIndexedSeq

  // Reverse index from a fetched block to its producer address and map index, used to build an
  // accurate FetchFailedException when a producer connection times out.
  private val blockLocations: Map[BlockId, (BlockManagerId, Int)] = {
    val builder = Map.newBuilder[BlockId, (BlockManagerId, Int)]
    blocksByAddressSeq.foreach { case (address, blocks) =>
      blocks.foreach { case (blockId, _, mapIndex) =>
        builder += (blockId -> (address, mapIndex))
      }
    }
    builder.result()
  }

  // Ensures the partial-read invalidation side effects (metric increment, buffer discard) happen
  // exactly once, even if several stream operations observe the same producer failure concurrently.
  private val partialReadInvalidated = new AtomicBoolean(false)

  /**
   * Whether continuous shuffle blocks may be fetched in a single batch. This is copied verbatim
   * from [[org.apache.spark.shuffle.BlockStoreShuffleReader]] so the streaming reader honors the
   * exact same compatibility gates (serializer relocation, compression codec concatenation, old
   * fetch protocol, and IO encryption) as the sort path.
   */
  private def fetchContinuousBlocksInBatch: Boolean = {
    val sparkConf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = sparkConf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      val codec = CompressionCodec.createCodec(sparkConf)
      CompressionCodec.supportsConcatenationOfSerializedStreams(codec)
    } else {
      true
    }
    val useOldFetchProtocol = sparkConf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: Fetching continuous blocks in batch is incompatible with io encryption.
    val ioEncryption = sparkConf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }

  /** Read the combined key-values for this reduce task. */
  override def read(): Iterator[Product2[K, C]] = {
    logStreamingReadStart()

    // Mirror BlockStoreShuffleReader.read(): fetch blocks via the standard fetch iterator over the
    // reused BlockTransferService. Streaming changes only: pass blocksByAddressSeq.iterator (a
    // re-iterable view of the captured descriptors) and wrap the completion iterator below for
    // producer-timeout detection.
    // Block integrity: this reused path applies Spark's existing shuffle checksum (the
    // SHUFFLE_CHECKSUM_ENABLED / SHUFFLE_CHECKSUM_ALGORITHM arguments passed below), which is the
    // v1 corruption-detection mechanism. StreamingBlockEnvelope's CRC32C verify/retransmit is a v2
    // wire-transport concern and is not exercised here (see the class Scaladoc and ADL #2).
    val rawStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddressSeq.iterator,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator

    // Wrap the fetch stream so an in-progress producer that fails or stalls beyond the 5s timeout
    // triggers partial-read invalidation and a FetchFailedException (see the wrappers below).
    val wrappedStreams = new ProducerTimeoutAwareIterator(rawStreams)

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
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

    // Use another interruptible iterator here to support task cancellation, as the aggregator or
    // sorter may have consumed the previous interruptible iterator.
    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * Emits a DEBUG line at the start of a streaming read when `spark.shuffle.streaming.debug` is on.
   * The message carries the shuffle-scoped correlation ids used across the streaming subsystem so
   * an operator can join reader activity with producer-side and spill-side logs.
   */
  private def logStreamingReadStart(): Unit = {
    if (conf.debug) {
      logDebug(
        log"Starting streaming shuffle read for shuffle " +
          log"${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)} reduce partition " +
          log"${MDC(LogKeys.REDUCE_ID, context.partitionId())} attempt " +
          log"${MDC(LogKeys.TASK_ATTEMPT_ID, context.taskAttemptId())}")
    }
  }

  /**
   * Classifies a fetch/read failure as a producer connection timeout. A failure counts as a
   * producer timeout if the operation stalled for at least `PRODUCER_CONNECTION_TIMEOUT_MS` before
   * failing, or if a connection-level failure (connect/reset/timeout) appears anywhere in the
   * throwable's cause chain.
   *
   * @param t the throwable raised while fetching or reading a producer block
   * @param elapsedMs how long the failing operation blocked before raising `t`
   * @return true if the failure should trigger partial-read invalidation
   */
  private def isProducerConnectionTimeout(t: Throwable, elapsedMs: Long): Boolean = {
    elapsedMs >= PRODUCER_CONNECTION_TIMEOUT_MS || connectionFailureInChain(t)
  }

  /** Walks up to `MAX_CAUSE_DEPTH` causes looking for a socket/connect/timeout failure. */
  private def connectionFailureInChain(t: Throwable): Boolean = {
    var cause: Throwable = t
    var depth = 0
    var found = false
    while (cause != null && depth < MAX_CAUSE_DEPTH && !found) {
      if (isConnectionFailure(cause)) {
        found = true
      } else {
        cause = cause.getCause
        depth += 1
      }
    }
    found
  }

  /** True if the throwable itself is a socket, connect, or timeout failure. */
  private def isConnectionFailure(t: Throwable): Boolean = t match {
    case _: SocketTimeoutException => true
    case _: SocketException => true
    case _: TimeoutException => true
    case _ => false
  }

  /**
   * Atomically invalidates the in-progress read for a failed producer block and fails the task.
   *
   * This performs the three streaming-specific timeout actions: it increments
   * `StreamingShuffleMetrics.incPartialReadInvalidations` exactly once, discards any partial buffer
   * already read for the block (by closing the underlying stream), and then throws a
   * [[FetchFailedException]]. Per SPARK-19276 the exception is constructed and thrown in a single
   * statement -- its constructor sets the [[TaskContext]] fetch-failed flag, so it must never be
   * built and conditionally skipped. The DAGScheduler observes the failure and recomputes the
   * upstream map stage; this is the only scheduler touchpoint.
   *
   * @param blockIdOpt the failed block if known, used to recover shuffle/map/reduce coordinates
   * @param addressOpt the producer address if known (may be absent; a null address is permitted)
   * @param mapIndex the producer map index if known, else the unknown sentinel
   * @param streamOpt the partially-read stream to discard, if any
   * @param cause the underlying producer failure
   * @return never returns normally; always throws [[FetchFailedException]]
   */
  private def invalidatePartialReadAndFail(
      blockIdOpt: Option[BlockId],
      addressOpt: Option[BlockManagerId],
      mapIndex: Int,
      streamOpt: Option[InputStream],
      cause: Throwable): Nothing = {
    if (partialReadInvalidated.compareAndSet(false, true)) {
      streamingMetrics.incPartialReadInvalidations()
    }
    // Discard any partially-read buffer so no partial data escapes into the reduce iterator.
    streamOpt.foreach { stream =>
      try {
        stream.close()
      } catch {
        case NonFatal(_) => // best-effort discard; ignore secondary close failures
      }
    }
    val shuffleId = handle.shuffleId
    val (mapId, reduceId) = blockIdOpt match {
      case Some(ShuffleBlockId(_, m, r)) => (m, r)
      case Some(ShuffleBlockBatchId(_, m, sr, _)) => (m, sr)
      case _ => (UNKNOWN_MAP_ID, UNKNOWN_REDUCE_ID)
    }
    val address = addressOpt.orNull
    logWarning(
      log"Streaming shuffle producer connection timed out after " +
        log"${MDC(LogKeys.TIMEOUT, PRODUCER_CONNECTION_TIMEOUT_MS)} ms; " +
        log"invalidating partial read for shuffle " +
        log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)} map ${MDC(LogKeys.MAP_ID, mapId)} " +
        log"reduce ${MDC(LogKeys.REDUCE_ID, reduceId)} attempt " +
        log"${MDC(LogKeys.TASK_ATTEMPT_ID, context.taskAttemptId())}",
      cause)
    // SPARK-19276: construct-and-throw in one statement so the TaskContext fetch-failed flag set by
    // the constructor is never leaked by a build-then-skip path.
    throw new FetchFailedException(
      address, shuffleId, mapId, mapIndex, reduceId,
      s"streaming producer connection timeout after ${PRODUCER_CONNECTION_TIMEOUT_MS}ms", cause)
  }

  /**
   * Runs a fetch/read operation, translating a producer connection timeout into partial-read
   * invalidation. Any [[scala.util.control.NonFatal]] failure that `isProducerConnectionTimeout`
   * classifies as a producer timeout is converted to a [[FetchFailedException]] via
   * `invalidatePartialReadAndFail`; every other failure is rethrown unchanged so corruption and
   * unrelated errors keep their existing semantics.
   *
   * @param blockIdOpt the block being fetched/read if known, for accurate failure coordinates
   * @param streamOpt the stream being read if known, to discard on invalidation
   * @param op the fetch or read operation to guard
   * @tparam T the operation's result type
   * @return the operation's result when it succeeds
   */
  private def guardProducerFetch[T](
      blockIdOpt: Option[BlockId],
      streamOpt: Option[InputStream],
      op: => T): T = {
    val startNs = System.nanoTime()
    try {
      op
    } catch {
      case NonFatal(t) =>
        val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)
        if (isProducerConnectionTimeout(t, elapsedMs)) {
          val location = blockIdOpt.flatMap(blockLocations.get)
          invalidatePartialReadAndFail(
            blockIdOpt, location.map(_._1), location.map(_._2).getOrElse(UNKNOWN_MAP_INDEX),
            streamOpt, t)
        } else {
          throw t
        }
    }
  }

  /**
   * Wraps the block-fetch completion iterator so that a producer failing to deliver the next block
   * is detected and converted into partial-read invalidation. Each yielded stream is further
   * wrapped in a [[ProducerTimeoutAwareInputStream]] so a producer that drops mid-block (after a
   * partial read has begun) is also detected while records are being deserialized.
   */
  private class ProducerTimeoutAwareIterator(
      delegate: Iterator[(BlockId, InputStream)])
    extends Iterator[(BlockId, InputStream)] {

    override def hasNext: Boolean = guardProducerFetch(None, None, delegate.hasNext)

    override def next(): (BlockId, InputStream) = {
      val (blockId, stream) = guardProducerFetch(None, None, delegate.next())
      (blockId, new ProducerTimeoutAwareInputStream(blockId, stream))
    }
  }

  /**
   * A thin [[InputStream]] decorator that guards each read of a producer block. A producer
   * connection that fails or stalls beyond the timeout while records are being deserialized
   * triggers partial-read invalidation for exactly this block, with accurate coordinates.
   */
  private class ProducerTimeoutAwareInputStream(
      blockId: BlockId,
      delegate: InputStream)
    extends InputStream {

    override def read(): Int =
      guardProducerFetch(Some(blockId), Some(delegate), delegate.read())

    override def read(b: Array[Byte]): Int =
      guardProducerFetch(Some(blockId), Some(delegate), delegate.read(b))

    override def read(b: Array[Byte], off: Int, len: Int): Int =
      guardProducerFetch(Some(blockId), Some(delegate), delegate.read(b, off, len))

    override def skip(n: Long): Long =
      guardProducerFetch(Some(blockId), Some(delegate), delegate.skip(n))

    override def available(): Int =
      guardProducerFetch(Some(blockId), Some(delegate), delegate.available())

    override def reset(): Unit =
      guardProducerFetch(Some(blockId), Some(delegate), delegate.reset())

    override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

    override def markSupported(): Boolean = delegate.markSupported()

    override def close(): Unit = delegate.close()
  }
}

/**
 * Constants for [[StreamingShuffleReader]].
 */
private[spark] object StreamingShuffleReader {

  /**
   * Producer connection timeout in milliseconds. When a producer (map) executor fails to deliver a
   * block within this window the in-progress read is invalidated and a
   * [[org.apache.spark.shuffle.FetchFailedException]] is thrown so the DAGScheduler recomputes the
   * upstream stage. Fixed at 5 seconds per the streaming-shuffle failure-tolerance contract.
   */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = 5000L

  /** Maximum depth walked in an exception's cause chain when classifying connection failures. */
  val MAX_CAUSE_DEPTH: Int = 16

  /** Sentinel map id used when a failed block's coordinates cannot be determined. */
  val UNKNOWN_MAP_ID: Long = -1L

  /** Sentinel reduce id used when a failed block's coordinates cannot be determined. */
  val UNKNOWN_REDUCE_ID: Int = -1

  /** Sentinel map index used when a failed producer's map index is unknown. */
  val UNKNOWN_MAP_INDEX: Int = -1
}
