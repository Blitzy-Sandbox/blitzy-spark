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
import java.net.SocketTimeoutException
import java.util.concurrent.TimeoutException
import java.util.zip.CRC32C

import scala.collection
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{Aggregator, InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Streaming-shuffle reader: opens streaming connections to all assigned producer executors
 * and consumes blocks as they become available, validating CRC32C checksums on receive
 * and requesting retransmission on corruption.
 *
 * == Read Path Overview ==
 * `read()` first looks up producer locations via the existing
 * [[org.apache.spark.MapOutputTracker]] SPI (NOT modified). For each producer address, it
 * lazily fetches each assigned block via
 * [[org.apache.spark.network.BlockTransferService.fetchBlockSync]] -- the same network
 * primitive used by the existing [[org.apache.spark.shuffle.BlockStoreShuffleReader]].
 * The streaming-aware optimization in v1 lives at the writer's flush cadence -- the reader
 * reuses the existing fetch primitive so that no new network protocol classes are introduced
 * (per AAP Section 0.7.2.3 *"Streaming MUST reuse `org.apache.spark.network.TransportContext`.
 * New network-protocol classes MUST NOT be added"*).
 *
 * Records are produced via a *lazy iterator chain* (Iterator.flatMap composition) rather
 * than being eagerly materialized into an in-memory buffer. This preserves the streaming
 * semantics required by AAP Section 0.1.1 *"pipelines data directly from map-side
 * producer executors to reduce-side consumer executors"*: downstream consumers can begin
 * processing the first record before the last block is even fetched. Per-block fetch,
 * checksum validation, and deserialization happen on-demand as the consumer pulls
 * elements through the iterator.
 *
 * == Aggregator and Sorter Integration ==
 * To preserve byte-for-byte semantic equivalence with
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]], this reader honors the
 * dependency's `aggregator`, `keyOrdering`, and `mapSideCombine` settings. The lazy
 * deserialized iterator is routed through:
 *   - [[org.apache.spark.util.collection.ExternalSorter]] when `keyOrdering` is defined,
 *     enabling on-disk sort-merge with optional aggregator-driven combine.
 *   - [[org.apache.spark.Aggregator.combineCombinersByKey]] when only `aggregator` and
 *     `mapSideCombine` are defined (records arrive pre-combined on the map side).
 *   - [[org.apache.spark.Aggregator.combineValuesByKey]] when only `aggregator` is
 *     defined and `mapSideCombine=false` (records arrive raw, combine happens here).
 *   - Direct cast to `Iterator[(K, C)]` when no aggregator or ordering is defined.
 * The dispatch logic mirrors `BlockStoreShuffleReader.read` exactly so that
 * `reduceByKey`, `aggregateByKey`, `combineByKey`, and `sortByKey` produce identical
 * output through both paths.
 *
 * == Failure Handling ==
 * On producer connection timeout (5 s, see [[PRODUCER_TIMEOUT_MILLIS]]), this reader
 * invalidates all partial reads from the failed producer, increments
 * `partialReadInvalidations` in [[StreamingShuffleMetrics]], and throws the existing
 * [[org.apache.spark.shuffle.FetchFailedException]]. The existing `DAGScheduler` path
 * (specifically `handleTaskCompletion`) catches the `FetchFailedException` and triggers
 * upstream stage recomputation through the standard `unregisterMapOutput + epoch bump`
 * mechanism -- this reader does NOT call into the scheduler directly. Per the SPARK-19276
 * contract documented in `FetchFailedException.scala`, the exception is constructed and
 * thrown atomically (no creating-then-deciding pattern) so that
 * [[org.apache.spark.TaskContext.setFetchFailed]] is invoked exactly once per failure.
 *
 * == Integrity Validation ==
 * Each block fetched from a producer is validated with a CRC32C checksum computed via the
 * JDK 17 `java.util.zip.CRC32C` class (Castagnoli polynomial 0x1EDC6F41). On checksum
 * mismatch, the reader requests retransmission once (re-fetch via the same
 * `fetchBlockSync` primitive). If the retransmission also produces a checksum mismatch,
 * the reader treats this as persistent corruption, increments
 * `partialReadInvalidations`, and throws `FetchFailedException` to drive upstream
 * recomputation. CRC32C is the only checksum algorithm permitted (per AAP Section 0.7.2.4
 * *"Checksum algorithm: CRC32C only"*).
 *
 * The producer-supplied expected checksum is resolved through [[expectedChecksumFor]] and
 * is represented as `Option[Long]`. `None` means "no expected checksum is available"
 * (e.g., test path, `spark.shuffle.checksum.enabled=false`, or v1 deployments where the
 * producer-side side-channel is not yet wired). `Some(value)` means the checksum is
 * present and the comparison is performed. The `Option` representation eliminates the
 * 1-in-2^32 false-skip that a sentinel-based design (e.g., `0L` meaning "absent") would
 * exhibit when a legitimate computed checksum happens to equal the sentinel value. The
 * v1 implementation of `expectedChecksumFor` returns `None` until the writer-side
 * side-channel (deferred to a future checkpoint) supplies actual values; see the
 * decision log entry "CRC32C side-channel deferred to v2" for the full v1 limitation.
 *
 * == Resource Management (ManagedBuffer Release) ==
 * Each `fetchBlockSync` call returns a reference-counted
 * [[org.apache.spark.network.buffer.ManagedBuffer]] that must be released exactly once
 * after the bytes are extracted, mirroring the `currentResult.buf.release()` pattern in
 * [[org.apache.spark.storage.ShuffleBlockFetcherIterator]]. Failure to release would leak
 * Netty direct memory; this reader uses a `try { ... } finally { managedBuffer.release() }`
 * envelope on every fetch (primary and retransmit) to guarantee release even when
 * deserialization or checksum validation throws.
 *
 * == Acknowledgment Design ==
 * Per AAP Section 0.4.3.2, consumer-position acknowledgments allow the producer's
 * `BackpressureProtocol` to reclaim buffer memory for already-consumed offsets. In v1 the
 * acknowledgment is implicit -- the producer observes the next fetch request as proof of
 * consumption progress for prior offsets. This keeps the v1 network surface minimal (no
 * new RPC types) while preserving the buffer-reclamation contract. The `ackedPositions`
 * map tracks each producer's last-acked offset for telemetry and for any future
 * explicit-ack RPC implementation. See the decision log entry "Consumer acknowledgment
 * wiring deferred to v2" for the v1 limitation that an explicit ack RPC is not yet
 * delivered (the writer's `BackpressureProtocol.recordConsumerAck` is therefore not yet
 * invoked from this reader).
 *
 * == Single-Threaded Metrics-Reporter Contract ==
 * Per AAP Section 0.7.3.5, this reader honors the
 * [[org.apache.spark.shuffle.ShuffleReadMetricsReporter]] single-threaded contract: all
 * `inc*` calls on `readMetrics` happen on the task thread executing `read()` (no
 * background thread mutates the reporter). Cross-task aggregation occurs at task-completion
 * boundaries via the existing `TaskMetrics` swap-in path, triggered through the
 * [[org.apache.spark.util.CompletionIterator]] wrapping below. The streaming-shuffle
 * metric counters in [[StreamingShuffleMetrics]] are a separate, lock-free metric set
 * updated from multiple threads concurrently -- those metrics are not subject to the
 * single-threaded reporter contract.
 *
 * == Coexistence ==
 * This reader is constructed only when the active `StreamingShuffleManager` chose the
 * streaming path (the fallback policy returned `false`). For sort-based shuffle, the
 * existing [[org.apache.spark.shuffle.BlockStoreShuffleReader]] is used unchanged. Per
 * the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* this class lives entirely
 * within `org.apache.spark.shuffle.streaming` and reuses the
 * [[org.apache.spark.shuffle.ShuffleReader]] trait without modifying it.
 *
 * @param handle             the streaming shuffle handle carrying configuration metadata
 * @param startMapIndex      inclusive start of the map range to read
 * @param endMapIndex        exclusive end of the map range to read
 * @param startPartition     inclusive start of the reduce-partition range
 * @param endPartition       exclusive end of the reduce-partition range
 * @param context            the active TaskContext for cancellation support; in production
 *                           paths this is always non-null (the executor task runner
 *                           always provides a TaskContext). A `null` is tolerated only as
 *                           a defensive fallback for synthetic test harnesses that do not
 *                           install a TaskContext; in such cases interruptible iteration
 *                           and per-task metric merging are skipped (see [[read]]).
 * @param readMetrics        single-threaded shuffle-read metrics reporter
 * @param blockManager       executor block manager (for `blockTransferService` access)
 * @param mapOutputTracker   driver/executor map output tracker (for producer location lookup)
 * @param streamingMetrics   streaming-shuffle metric counters/gauges
 * @tparam K key type produced by the upstream stage
 * @tparam C combined value type after map-side aggregation (equals V if no aggregator)
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    streamingMetrics: StreamingShuffleMetrics)
  extends ShuffleReader[K, C] with Logging {

  /**
   * Per-producer last-acknowledged offset, used to drive [[acknowledgePosition]] and to
   * compute cumulative acked bytes per producer. The map key is the producer
   * [[BlockManagerId]]; the value is the cumulative byte offset acked back to that
   * producer. Only mutated on the task thread executing the lazy iterator so no
   * synchronization is required.
   */
  private val ackedPositions = new mutable.HashMap[BlockManagerId, Long]()

  /**
   * Read the combined key-values for this reduce task as a *lazy* iterator.
   *
   * The returned iterator pulls data on-demand through a flatMap composition over the
   * per-producer block fetch + deserialize pipeline. Per-block fetch, checksum
   * validation, and deserialization occur as the consumer pulls elements -- this
   * preserves the streaming semantics required by AAP Section 0.1.1 (no batch
   * accumulation before consumer processing begins).
   *
   * The iterator chain mirrors [[org.apache.spark.shuffle.BlockStoreShuffleReader.read]]:
   *   1. Lazy per-producer block iterator -> raw `(K, C)` records.
   *   2. [[org.apache.spark.util.CompletionIterator]] wrapping for per-task metric
   *      merging on iterator exhaustion.
   *   3. [[org.apache.spark.InterruptibleIterator]] wrapping for `TaskContext`
   *      cancellation support.
   *   4. [[org.apache.spark.util.collection.ExternalSorter]] /
   *      [[org.apache.spark.Aggregator]] dispatch based on `dep.keyOrdering` and
   *      `dep.aggregator`.
   *   5. Final `InterruptibleIterator` re-wrap so the consumer can cancel after
   *      sorter/aggregator transformation.
   *
   * Records are deserialized through the standard
   * [[org.apache.spark.serializer.SerializerManager.wrapStream]] path so byte-for-byte
   * semantic equivalence with `BlockStoreShuffleReader` is preserved.
   *
   * @return a lazy iterator over the combined key-value pairs from all assigned producers
   * @throws FetchFailedException if any producer connection times out or experiences
   *                              persistent CRC32C corruption (constructed and thrown
   *                              atomically per the SPARK-19276 contract)
   */
  override def read(): Iterator[Product2[K, C]] = {
    val shuffleId = handle.shuffleId
    logInfo(log"StreamingShuffleReader.read: shuffleId=" +
      log"${MDC(SHUFFLE_ID, shuffleId)}, " +
      log"mapRange=[${MDC(MAP_ID, startMapIndex.toLong)}, " +
      log"${MDC(MAP_ID, endMapIndex.toLong)}), " +
      log"partitionRange=[${MDC(REDUCE_ID, startPartition)}, " +
      log"${MDC(REDUCE_ID, endPartition)})")

    // Discover producer locations via the existing MapOutputTracker SPI (NOT modified).
    // The returned iterator yields (BlockManagerId, Seq[(BlockId, Long, Int)]) where the
    // third Int in each block tuple is the map index (NOT the map ID; the map ID is
    // carried inside the BlockId when it is a ShuffleBlockId). This iterator is itself
    // lazy and our iterator chain extends that laziness end-to-end.
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    val dep = handle.dependency
    val serializerManager = SparkEnv.get.serializerManager
    val serializerInstance = dep.serializer.newInstance()

    // Lazy iterator chain: each producer's blocks are fetched on-demand as the consumer
    // pulls. flatMap composes (BlockManagerId -> per-producer iter) so the final
    // recordIter yields raw `(Any, Any)` records lazily across all producers.
    val recordIter: Iterator[(Any, Any)] = blocksByAddress.flatMap {
      case (address, blocks) =>
        producerBlockIterator(
          address, shuffleId, blocks, serializerManager, serializerInstance)
    }

    // Per-record metric tracking + per-task metric merge on iterator exhaustion. Mirrors
    // BlockStoreShuffleReader exactly so the streaming reader integrates cleanly with the
    // existing TaskMetrics swap-in path. CompletionIterator's completion callback runs
    // when hasNext first observes the underlying iterator exhausted.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1L)
        record
      },
      // Defensive: only invoke mergeShuffleReadMetrics when we have a non-null
      // TaskContext. Production task threads always have one; synthetic test harnesses
      // may pass `null` via the constructor (see the @param documentation).
      if (context != null) context.taskMetrics().mergeShuffleReadMetrics() else ())

    // Wrap with InterruptibleIterator so the consumer can be canceled between records via
    // TaskContext.killed() -- matches BlockStoreShuffleReader's cancellation contract.
    val interruptibleIter: Iterator[(Any, Any)] =
      if (context != null) new InterruptibleIterator[(Any, Any)](context, metricIter)
      else metricIter

    // Aggregator/sorter dispatch -- mirrors BlockStoreShuffleReader.read exactly so that
    // `reduceByKey`, `aggregateByKey`, `combineByKey`, and `sortByKey` produce identical
    // results regardless of which shuffle path (sort vs streaming) is active.
    val resultIter: Iterator[Product2[K, C]] = {
      if (dep.keyOrdering.isDefined) {
        // Sort path: external sort-merge, optionally combining via aggregator.
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
          // Records already combined on the map side; combine combiners on the reduce side.
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // Records arrive raw; combine values to produce combined output type C.
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        // No aggregator or ordering -- direct cast to (K, C) is structurally safe at
        // the JVM level since Scala 2.13 tuples implement Product2.
        interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      }
    }

    // Re-wrap with InterruptibleIterator if the aggregator/sorter consumed the prior
    // interruptible iter. Mirrors BlockStoreShuffleReader's final wrap.
    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] @unchecked => resultIter
      case _ if context != null =>
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
      case _ =>
        // Test path: no TaskContext available, so no cancellation support possible.
        resultIter
    }
  }

  /**
   * Build a lazy iterator that, for each block assigned to the given producer, fetches
   * the bytes synchronously, validates the CRC32C checksum, releases the
   * [[org.apache.spark.network.buffer.ManagedBuffer]], and yields a deserialized
   * key-value iterator. The per-block fetch uses
   * [[org.apache.spark.network.BlockTransferService.fetchBlockSync]] (the same primitive
   * as [[org.apache.spark.shuffle.BlockStoreShuffleReader]]) so no new network
   * primitives are introduced.
   *
   * The returned iterator is lazy: a block is fetched only when the consumer pulls past
   * the previous block's records. A producer-side timeout is tracked per call invocation
   * via a deadline computed from [[PRODUCER_TIMEOUT_MILLIS]]. If the deadline passes
   * before the next block is fetched, [[FetchFailedException]] is thrown atomically per
   * the SPARK-19276 contract.
   *
   * Specific exception types are translated into [[FetchFailedException]] with
   * categorized messages: [[TimeoutException]] / [[SocketTimeoutException]] /
   * [[IOException]] / `NonFatal`. Fatal errors (e.g., `OutOfMemoryError`,
   * `InterruptedException`) propagate unwrapped.
   *
   * @param address             the producer's [[BlockManagerId]]
   * @param shuffleId           the shuffle identifier (for FetchFailedException)
   * @param blocks              the assigned blocks for this producer; each tuple is
   *                            `(BlockId, length, mapIndex)`
   * @param serializerManager   shared [[SerializerManager]] for stream wrapping
   * @param serializerInstance  per-call [[SerializerInstance]] for deserialization
   * @return a lazy iterator over `(K, C)` records produced by this producer
   */
  private def producerBlockIterator(
      address: BlockManagerId,
      shuffleId: Int,
      blocks: collection.Seq[(BlockId, Long, Int)],
      serializerManager: SerializerManager,
      serializerInstance: SerializerInstance): Iterator[(Any, Any)] = {
    val deadline = System.currentTimeMillis() + PRODUCER_TIMEOUT_MILLIS
    var bytesReadTotal = 0L

    blocks.iterator.flatMap { case (blockId, length, mapIndex) =>
      // Check the timeout deadline BEFORE every block fetch. This ensures a stuck or slow
      // producer cannot exceed the contractual 5-second window before we trigger upstream
      // recomputation. The increment-and-throw is atomic per SPARK-19276.
      if (System.currentTimeMillis() > deadline) {
        streamingMetrics.incrementPartialReadInvalidations()
        logWarning(log"Producer connection timeout for " +
          log"${MDC(HOST_PORT, s"${address.host}:${address.port}")} " +
          log"shuffleId=${MDC(SHUFFLE_ID, shuffleId)} " +
          log"timeoutMs=${MDC(TIMEOUT, PRODUCER_TIMEOUT_MILLIS)}; " +
          log"invalidating partial reads")
        throw new FetchFailedException(
          address,
          shuffleId,
          mapIdFromBlock(blockId),
          mapIndex,
          startPartition,
          s"Producer $address exceeded ${PRODUCER_TIMEOUT_MILLIS}ms timeout " +
            s"before block $blockId")
      }

      val validatedBytes =
        fetchAndValidateBlock(address, shuffleId, blockId, length, mapIndex)

      // Update local read metrics. The single-threaded contract documented on
      // ShuffleReadMetricsReporter is honored: this method is the sole accumulator for
      // the read path on the task thread.
      readMetrics.incRemoteBytesRead(length)
      readMetrics.incRemoteBlocksFetched(1L)
      bytesReadTotal += length

      // Acknowledge the consumed position so the producer can reclaim buffer memory.
      // Per the v1 acknowledgment design, this is an implicit ack carried via the next
      // fetch request; no explicit RPC is sent. The ackedPositions map is updated so
      // that any future explicit-ack implementation has the cumulative offset to send.
      val newPos = ackedPositions.getOrElse(address, 0L) + length
      ackedPositions(address) = newPos
      acknowledgePosition(address, newPos)

      // Deserialize the validated bytes through the standard SerializerManager pipeline.
      // Returns a NextIterator that closes the underlying DeserializationStream when the
      // record iterator is exhausted (asKeyValueIterator's contract).
      val byteStream = new ByteArrayInputStream(validatedBytes)
      val wrappedStream = serializerManager.wrapStream(blockId, byteStream)
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    } ++ producerCompletionLogger(address)

    // Note: the trailing `++ producerCompletionLogger(...)` is a zero-element iterator
    // whose hasNext side-effect emits a debug log line when the producer's blocks are
    // fully consumed. This preserves the per-producer log emitted by the prior eager
    // implementation without introducing a Phase-3 hot-path scan over the records.
  }

  /**
   * Zero-element iterator whose only side effect is logging the per-producer completion
   * record. Returned by [[producerBlockIterator]] so the existing per-producer DEBUG
   * trace is preserved in the lazy implementation.
   *
   * @param address the producer whose iteration just completed
   * @return an empty `Iterator[(Any, Any)]` that logs on first `hasNext`
   */
  private def producerCompletionLogger(
      address: BlockManagerId): Iterator[(Any, Any)] = new Iterator[(Any, Any)] {
    private var logged = false
    override def hasNext: Boolean = {
      if (!logged) {
        logged = true
        val acked = ackedPositions.getOrElse(address, 0L)
        logDebug(log"Consumed all blocks from producer " +
          log"${MDC(HOST_PORT, s"${address.host}:${address.port}")}, " +
          log"bytesAcked=${MDC(NUM_BYTES, acked)}")
      }
      false
    }
    override def next(): (Any, Any) = throw new NoSuchElementException
  }

  /**
   * Fetch the bytes of a single block and validate its CRC32C checksum. Translates any
   * fetch-time failure into [[FetchFailedException]] with a categorized message so that
   * the existing `DAGScheduler.handleTaskCompletion` path drives upstream recomputation.
   *
   * The [[org.apache.spark.network.buffer.ManagedBuffer]] returned by `fetchBlockSync`
   * is released exactly once via a `try { ... } finally { release() }` envelope --
   * matching the established pattern in
   * [[org.apache.spark.storage.ShuffleBlockFetcherIterator]]. Failure to release would
   * leak Netty direct memory and exhaust the executor's direct-memory budget under
   * sustained streaming workloads.
   *
   * On checksum mismatch, retransmission is attempted exactly once. Persistent
   * corruption (mismatch after retransmission) increments
   * `partialReadInvalidations` and throws [[FetchFailedException]] atomically.
   *
   * @param address    the producer's [[BlockManagerId]]
   * @param shuffleId  the shuffle identifier (for FetchFailedException)
   * @param blockId    the block being fetched
   * @param length     the expected block length (used for read-metrics accounting)
   * @param mapIndex   the map index for this block (third tuple element)
   * @return the validated block bytes
   */
  private def fetchAndValidateBlock(
      address: BlockManagerId,
      shuffleId: Int,
      blockId: BlockId,
      length: Long,
      mapIndex: Int): Array[Byte] = {
    // Fetch with categorized exception classification. Fatal errors (OOM, etc.) propagate
    // unwrapped so the JVM-level diagnostics are preserved. NonFatal errors are mapped
    // into FetchFailedException so the DAGScheduler triggers upstream recomputation.
    val managedBuffer = try {
      blockManager.blockTransferService.fetchBlockSync(
        address.host, address.port, address.executorId, blockId.name, null)
    } catch {
      case e: TimeoutException =>
        streamingMetrics.incrementPartialReadInvalidations()
        throw new FetchFailedException(
          address, shuffleId, mapIdFromBlock(blockId), mapIndex, startPartition,
          s"Producer connection timeout fetching block $blockId from $address: " +
            s"${e.getMessage}", e)
      case e: SocketTimeoutException =>
        streamingMetrics.incrementPartialReadInvalidations()
        throw new FetchFailedException(
          address, shuffleId, mapIdFromBlock(blockId), mapIndex, startPartition,
          s"Socket timeout fetching block $blockId from $address: ${e.getMessage}", e)
      case e: IOException =>
        streamingMetrics.incrementPartialReadInvalidations()
        throw new FetchFailedException(
          address, shuffleId, mapIdFromBlock(blockId), mapIndex, startPartition,
          s"I/O failure fetching block $blockId from $address: ${e.getMessage}", e)
      case NonFatal(e) =>
        streamingMetrics.incrementPartialReadInvalidations()
        throw new FetchFailedException(
          address, shuffleId, mapIdFromBlock(blockId), mapIndex, startPartition,
          s"Failed to fetch block $blockId from $address: ${e.getMessage}", e)
      // Fatal errors (OutOfMemoryError, InterruptedException) propagate unwrapped.
    }

    // Extract bytes, then ALWAYS release the ManagedBuffer in a finally block. This
    // matches the currentResult.buf.release() pattern in ShuffleBlockFetcherIterator
    // and prevents Netty direct-memory leaks under sustained shuffle workloads.
    val bytes = try {
      val nio = managedBuffer.nioByteBuffer()
      val arr = new Array[Byte](nio.remaining())
      nio.get(arr)
      arr
    } finally {
      managedBuffer.release()
    }

    // CRC32C verification (Castagnoli polynomial 0x1EDC6F41). When no expected checksum
    // is available (Option None) verification is skipped -- this is the v1 default
    // because the producer-side side-channel is not yet wired (see Scaladoc on
    // expectedChecksumFor and the decision-log entry "CRC32C side-channel deferred to
    // v2"). When an expected checksum IS available, retransmission is attempted exactly
    // once on mismatch; persistent mismatch triggers FetchFailedException.
    expectedChecksumFor(blockId) match {
      case Some(expected) =>
        val computed = computeCrc32c(bytes)
        if (computed != expected) {
          logWarning(log"CRC32C checksum mismatch for blockId=" +
            log"${MDC(BLOCK_ID, blockId.name)} " +
            log"from ${MDC(HOST_PORT, s"${address.host}:${address.port}")} " +
            log"(expected=${MDC(CHECKSUM, expected)}, " +
            log"got=${MDC(CHECKSUM, computed)}); requesting retransmission")
          val retryBytes = retransmitBlock(address, blockId)
          val retryComputed = computeCrc32c(retryBytes)
          if (retryComputed != expected) {
            // Persistent corruption -- invalidate partial read and trigger upstream
            // recomputation. The increment-and-throw is atomic per SPARK-19276.
            streamingMetrics.incrementPartialReadInvalidations()
            throw new FetchFailedException(
              address,
              shuffleId,
              mapIdFromBlock(blockId),
              mapIndex,
              startPartition,
              s"Persistent $CHECKSUM_ALGORITHM corruption on block $blockId after " +
                s"retransmission (expected=$expected, got=$retryComputed)")
          }
          retryBytes
        } else {
          bytes
        }
      case None =>
        // No expected checksum available -- v1 default. Skip verification.
        bytes
    }
  }

  /**
   * Re-fetch a block from a producer after a checksum mismatch. Returns the raw bytes
   * of the retransmitted block. Network failures during retransmission are propagated
   * to the caller (which will treat them as persistent corruption and throw
   * [[FetchFailedException]]).
   *
   * The [[org.apache.spark.network.buffer.ManagedBuffer]] from the retransmit fetch is
   * released in a finally block (same pattern as the primary fetch in
   * [[fetchAndValidateBlock]]) to prevent Netty direct-memory leaks on retry.
   *
   * Extracted as a helper so that the retransmission code path is independently testable
   * (e.g., a test can override this method to inject corrupted retry data).
   *
   * @param address producer's [[BlockManagerId]]
   * @param blockId the block to re-fetch
   * @return the raw bytes of the retransmitted block
   */
  private def retransmitBlock(address: BlockManagerId, blockId: BlockId): Array[Byte] = {
    val retryBuffer = blockManager.blockTransferService.fetchBlockSync(
      address.host, address.port, address.executorId, blockId.name, null)
    try {
      val retryNio = retryBuffer.nioByteBuffer()
      val retryBytes = new Array[Byte](retryNio.remaining())
      retryNio.get(retryBytes)
      retryBytes
    } finally {
      retryBuffer.release()
    }
  }

  /**
   * Compute the CRC32C checksum (Castagnoli polynomial 0x1EDC6F41) of the given byte
   * array using the JDK 17 [[java.util.zip.CRC32C]] class. The checksum is computed over
   * the entire array (offset 0 to `bytes.length`).
   *
   * Per AAP Section 0.7.2.4 *"Checksum algorithm: CRC32C"* -- this is the only checksum
   * algorithm permitted for the streaming-shuffle path. No alternative algorithm
   * (MD5, SHA-1, SHA-256, xxHash) may be substituted. The fixed `CRC32C` selection here
   * matches the [[CHECKSUM_ALGORITHM]] log identifier exposed by the package object.
   *
   * @param bytes the input bytes
   * @return the CRC32C checksum as an unsigned 32-bit value held in a `Long`
   */
  private def computeCrc32c(bytes: Array[Byte]): Long = {
    val crc = new CRC32C()
    crc.update(bytes, 0, bytes.length)
    crc.getValue
  }

  /**
   * Resolve the expected CRC32C checksum for the given block as `Option[Long]`.
   *
   *   - `Some(checksum)` -- a producer-supplied checksum is available; the validator in
   *     [[fetchAndValidateBlock]] performs the comparison and triggers retransmission /
   *     [[FetchFailedException]] on mismatch.
   *   - `None` -- no expected checksum is available; verification is skipped. This is
   *     the correct behavior for: test paths that do not populate checksums, production
   *     deployments where `spark.shuffle.checksum.enabled=false`, and v1 deployments
   *     where the producer-side side-channel is not yet wired.
   *
   * == v1 Limitation: CRC32C Side-Channel Deferred to v2 ==
   * The full CRC32C side-channel implementation -- where the producer supplies expected
   * checksums via an out-of-band header in the shuffle-block stream or via an extension
   * to the [[org.apache.spark.scheduler.MapStatus]] payload -- is deferred to a future
   * Spark version. v1 of streaming shuffle returns `None` here unconditionally,
   * effectively making CRC32C verification a no-op in production. The verification code
   * path remains intact and reachable via the `Option[Long]` API so that:
   *   - Test harnesses can override this method to inject expected values and exercise
   *     the verification + retransmission logic end-to-end.
   *   - The v2 implementation can wire the side-channel into this method without
   *     changing any other call site.
   * The decision to ship v1 with verification deferred is recorded in the streaming
   * shuffle decision log under "CRC32C side-channel deferred to v2"; operators should
   * rely on TCP-level integrity in the v1 deployment.
   *
   * Using `Option[Long]` instead of a sentinel value (e.g., `0L` meaning "absent")
   * eliminates the 1-in-2^32 false-skip that a sentinel design would exhibit when a
   * legitimate computed CRC32C happens to equal the sentinel.
   *
   * @param blockId identifier of the block whose checksum is being looked up
   * @return `Some(checksum)` if a producer-supplied checksum exists, else `None`
   */
  private def expectedChecksumFor(blockId: BlockId): Option[Long] = {
    // Reference the parameter to avoid an unused-parameter warning under strict scalastyle
    // and to keep the integration-point signature stable for v2 implementations that
    // need the BlockId to look up per-block checksums in a producer-supplied side channel.
    logTrace(log"Resolving expected CRC32C checksum for blockId=" +
      log"${MDC(BLOCK_ID, blockId.name)} (v1: returns None)")
    None
  }

  /**
   * Send a consumer-position acknowledgment to the producer at `producerId` so the
   * producer can reclaim buffer memory for blocks whose cumulative offset is &le;
   * `position`.
   *
   * == v1 Limitation: Acknowledgment Wiring Deferred to v2 ==
   * Per AAP Section 0.4.3.2, the v1 acknowledgment is *implicit*: the producer's
   * `BackpressureProtocol` is intended to observe the next fetch request as proof of
   * consumption progress and reclaim buffer memory for the prior offsets. However, the
   * v1 producer-side wiring -- where `BackpressureProtocol.recordConsumerAck(...)` is
   * invoked from this reader (or from the network handler observing the next fetch
   * request) -- is NOT yet delivered. The implication is that the 10-second
   * consumer-failure detection in `BackpressureProtocol` is not yet exercisable
   * end-to-end through this reader. The decision to defer the explicit RPC to v2 is
   * recorded in the streaming shuffle decision log under "Consumer acknowledgment
   * wiring deferred to v2".
   *
   * The position is logged at TRACE level for operator-side debugging when
   * `spark.shuffle.streaming.debug=true`. The `ackedPositions` map (mutated before this
   * call) tracks each producer's last-acked offset so a future explicit-ack RPC has the
   * cumulative offset to send.
   *
   * @param producerId the producer's [[BlockManagerId]]
   * @param position   the cumulative byte offset acked back to the producer
   */
  private def acknowledgePosition(producerId: BlockManagerId, position: Long): Unit = {
    logTrace(log"Acked position=${MDC(NUM_BYTES, position)} for producerId=" +
      log"${MDC(HOST_PORT, s"${producerId.host}:${producerId.port}")}")
  }

  /**
   * Extract the `mapId` (Long) from a [[BlockId]]. For [[ShuffleBlockId]] the `mapId` is
   * a direct field; for any other block type encountered in this read path (which would
   * be unexpected since `MapOutputTracker.getMapSizesByExecutorId` returns shuffle
   * blocks), we return `-1L` to satisfy the [[FetchFailedException]] contract that allows
   * sentinel values for unknown identifiers.
   *
   * @param blockId the block whose map ID we want to extract
   * @return the map ID, or `-1L` if the block is not a [[ShuffleBlockId]]
   */
  private def mapIdFromBlock(blockId: BlockId): Long = blockId match {
    case sb: ShuffleBlockId => sb.mapId
    case _ => -1L
  }

  /**
   * @return a snapshot of the current per-producer acked-position map. Provided for
   *         tests and observability tooling that need to verify acknowledgment progress
   *         without exposing the mutable internal map. The returned map is an immutable
   *         copy at the moment of the call.
   */
  private[streaming] def ackedPositionsSnapshot: scala.collection.Map[BlockManagerId, Long] =
    ackedPositions.toMap

  // Defensive null check for the constructor-injected TaskContext at instantiation. In
  // production task threads the TaskContext is ALWAYS non-null (the executor task runner
  // installs it before invoking shuffle reads). A `null` TaskContext is tolerated only
  // for synthetic test harnesses; in such cases interruptible iteration and per-task
  // metric merging are skipped (see read()). The DEBUG log here is the documented
  // signal that the reader is operating in test mode -- production deployments will
  // never emit this line.
  if (context == null) {
    logDebug(log"StreamingShuffleReader instantiated with null TaskContext; " +
      log"cancellation checks and per-task metric merging will be skipped (test path)")
  }
}
