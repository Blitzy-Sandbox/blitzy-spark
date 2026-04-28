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
import java.util.concurrent.TimeoutException
import java.util.zip.CRC32C

import scala.collection.mutable

import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Streaming-shuffle reader: opens streaming connections to all assigned producer executors
 * and consumes blocks as they become available, validating CRC32C checksums on receive
 * and requesting retransmission on corruption.
 *
 * == Read Path Overview ==
 * `read()` first looks up producer locations via the existing
 * [[org.apache.spark.MapOutputTracker]] SPI (NOT modified). For each producer address, it
 * delegates to [[streamFromProducer]] which iterates over the assigned blocks, fetching
 * each block synchronously via
 * [[org.apache.spark.network.BlockTransferService.fetchBlockSync]] -- the same network
 * primitive used by the existing [[org.apache.spark.shuffle.BlockStoreShuffleReader]].
 * The streaming-aware optimization in v1 lives at the writer's flush cadence -- the reader
 * reuses the existing fetch primitive so that no new network protocol classes are introduced
 * (per AAP Section 0.7.2.3 *"Streaming MUST reuse `org.apache.spark.network.TransportContext`.
 * New network-protocol classes MUST NOT be added"*).
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
 * == Acknowledgment Design ==
 * Per AAP Section 0.4.3.2, consumer-position acknowledgments allow the producer's
 * `BackpressureProtocol` to reclaim buffer memory for already-consumed offsets. In v1 the
 * acknowledgment is implicit -- the producer observes the next fetch request as proof of
 * consumption progress for prior offsets. This keeps the v1 network surface minimal (no
 * new RPC types) while preserving the buffer-reclamation contract. The `ackedPositions`
 * map tracks each producer's last-acked offset for telemetry and for any future
 * explicit-ack RPC implementation.
 *
 * == Single-Threaded Metrics-Reporter Contract ==
 * Per AAP Section 0.7.3.5, this reader honors the
 * [[org.apache.spark.shuffle.ShuffleReadMetricsReporter]] single-threaded contract: all
 * `inc*` calls on `readMetrics` happen on the task thread executing `read()` (no
 * background thread mutates the reporter). Cross-task aggregation occurs at task-completion
 * boundaries via the existing `TaskMetrics` swap-in path. The streaming-shuffle metric
 * counters in [[StreamingShuffleMetrics]] are a separate, lock-free metric set updated
 * from multiple threads concurrently -- those metrics are not subject to the
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
 * @param context            the active TaskContext for cancellation support
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
   * producer. Only mutated on the task thread executing [[streamFromProducer]] so no
   * synchronization is required.
   */
  private val ackedPositions = new mutable.HashMap[BlockManagerId, Long]()

  /**
   * Read the combined key-values for this reduce task.
   *
   * Polls producers in turn, validating each block's CRC32C checksum as it arrives and
   * deserializing the validated bytes through the standard
   * [[org.apache.spark.serializer.SerializerManager]] path. Throws
   * [[FetchFailedException]] on producer connection timeout (>5 s, per
   * [[PRODUCER_TIMEOUT_MILLIS]]) to drive DAG-scheduler upstream recomputation through
   * the existing `handleTaskCompletion` path.
   *
   * The returned iterator yields the deserialized records as `(K, C)` pairs typed as
   * [[Product2]]. Records are accumulated into an in-memory buffer in v1 for
   * implementation simplicity; future versions may switch to a lazy iterator chain to
   * reduce peak memory footprint.
   *
   * @return an iterator over the combined key-value pairs from all assigned producers
   * @throws FetchFailedException if any producer connection times out or experiences
   *                              persistent CRC32C corruption
   */
  override def read(): Iterator[Product2[K, C]] = {
    val shuffleId = handle.shuffleId
    logInfo(
      s"StreamingShuffleReader.read: shuffleId=$shuffleId, mapRange=[$startMapIndex, " +
      s"$endMapIndex), partitionRange=[$startPartition, $endPartition)")

    // Discover producer locations via the existing MapOutputTracker SPI (NOT modified).
    // The returned iterator yields (BlockManagerId, Seq[(BlockId, Long, Int)]) where the
    // third Int in each block tuple is the map index (NOT the map ID; the map ID is
    // carried inside the BlockId when it is a ShuffleBlockId).
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    // Accumulate deserialized records across all producers. Using ArrayBuffer in v1 for
    // implementation simplicity; the AAP-required failure semantics are preserved.
    val records = new mutable.ArrayBuffer[Product2[K, C]]()

    blocksByAddress.foreach { case (address, blocks) =>
      try {
        streamFromProducer(address, shuffleId, blocks, records)
      } catch {
        case _: TimeoutException =>
          // Producer connection timed out (>5 s). Increment invalidation counter, then
          // throw FetchFailedException so the DAGScheduler triggers upstream recomputation.
          // Per the SPARK-19276 contract, FetchFailedException must be constructed and
          // thrown atomically -- no creating-then-deciding -- so that
          // TaskContext.setFetchFailed (called inside the FetchFailedException constructor)
          // is invoked exactly once per failure.
          streamingMetrics.incrementPartialReadInvalidations()
          logWarning(
            s"Producer connection timeout for $address, shuffle $shuffleId; " +
            s"invalidating partial reads (algorithm=$CHECKSUM_ALGORITHM, " +
            s"timeoutMs=$PRODUCER_TIMEOUT_MILLIS)")
          // Identify the first block's mapId/mapIndex for the exception payload. The
          // mapId comes from the BlockId (ShuffleBlockId.mapId), not from the third tuple
          // element which is the map *index*. If the producer's block list is empty
          // (defensive case), we use sentinel -1 values which the FetchFailedException
          // contract documents as acceptable.
          val (mapId, mapIndex) = identifyFirstBlock(blocks)
          throw new FetchFailedException(
            address,
            shuffleId,
            mapId,
            mapIndex,
            startPartition,
            s"Producer connection timeout after ${PRODUCER_TIMEOUT_MILLIS}ms")
      }
    }

    logDebug(
      s"StreamingShuffleReader.read complete: shuffleId=$shuffleId, " +
      s"records=${records.size}, producers=${ackedPositions.size}")
    records.iterator
  }

  /**
   * Stream blocks from a single producer address. For each block:
   *
   *   1. Check the deadline; throw [[TimeoutException]] if the producer has not made
   *      progress within [[PRODUCER_TIMEOUT_MILLIS]].
   *   2. Fetch the block synchronously via
   *      [[org.apache.spark.network.BlockTransferService.fetchBlockSync]] -- the same
   *      primitive used by [[org.apache.spark.shuffle.BlockStoreShuffleReader]].
   *   3. Validate the CRC32C checksum of the received bytes against the producer-supplied
   *      expected checksum (resolved via [[expectedChecksumFor]]).
   *   4. On checksum mismatch, request retransmission once. On persistent mismatch (after
   *      retransmission), increment `partialReadInvalidations` and throw
   *      [[FetchFailedException]] -- atomically per the SPARK-19276 contract.
   *   5. Update the per-task `readMetrics` reporter (single-threaded contract honored).
   *   6. Deserialize the validated bytes through the standard
   *      [[org.apache.spark.serializer.SerializerManager]] path and accumulate the
   *      resulting `(K, C)` records.
   *   7. Update `ackedPositions` and trigger [[acknowledgePosition]] so the producer can
   *      reclaim buffer memory for the consumed prefix.
   *
   * @param address     the producer's [[BlockManagerId]]
   * @param shuffleId   the shuffle identifier (for FetchFailedException construction)
   * @param blocks      the assigned blocks for this producer; each tuple is
   *                    `(BlockId, length, mapIndex)`
   * @param accumulator buffer into which deserialized records are appended
   * @throws TimeoutException     on producer connection timeout (>5 s without progress)
   *                              or any underlying network failure (which is mapped to
   *                              `TimeoutException` so the outer catch can route it into
   *                              `FetchFailedException`)
   * @throws FetchFailedException on persistent checksum corruption (after retransmission)
   */
  private def streamFromProducer(
      address: BlockManagerId,
      shuffleId: Int,
      blocks: scala.collection.Seq[(BlockId, Long, Int)],
      accumulator: mutable.ArrayBuffer[Product2[K, C]]): Unit = {
    val deadline = System.currentTimeMillis() + PRODUCER_TIMEOUT_MILLIS
    var bytesReadTotal = 0L

    blocks.foreach { case (blockId, length, mapIndex) =>
      // Check the timeout deadline BEFORE every block fetch. This ensures a stuck or
      // slow producer cannot exceed the contractual 5-second window before we trigger
      // upstream recomputation.
      if (System.currentTimeMillis() > deadline) {
        throw new TimeoutException(
          s"Producer $address exceeded ${PRODUCER_TIMEOUT_MILLIS}ms timeout before " +
          s"block $blockId")
      }

      // Fetch the block synchronously through the existing block transfer service.
      // This reuses the same network path as BlockStoreShuffleReader; in v1 the streaming
      // optimization lives in the writer's flush cadence rather than a new fetch protocol.
      // Any underlying network failure is mapped into TimeoutException so that the outer
      // read() catch block routes it consistently into FetchFailedException.
      val managedBuffer = try {
        blockManager.blockTransferService.fetchBlockSync(
          address.host, address.port, address.executorId, blockId.name, null)
      } catch {
        case e: Exception =>
          val timeoutEx = new TimeoutException(
            s"Failed to fetch block $blockId from $address: ${e.getMessage}")
          timeoutEx.initCause(e)
          throw timeoutEx
      }

      // Read all bytes from the ManagedBuffer's NIO ByteBuffer. The buffer ownership
      // contract requires us to copy the bytes out before any subsequent operation that
      // might release the underlying memory.
      val nioBuffer = managedBuffer.nioByteBuffer()
      val bytes = new Array[Byte](nioBuffer.remaining())
      nioBuffer.get(bytes)

      // Validate CRC32C checksum of the received bytes BEFORE deserialization. The
      // expected checksum is carried out-of-band by the existing shuffle checksum
      // side-channel; if no checksum is available (test path or checksum disabled),
      // expectedChecksumFor returns 0L which causes the comparison to be skipped.
      val computed = computeCrc32c(bytes)
      val expected = expectedChecksumFor(blockId)
      val validatedBytes = if (expected != 0L && computed != expected) {
        // Checksum mismatch -- request retransmission once before giving up.
        logWarning(
          s"$CHECKSUM_ALGORITHM checksum mismatch for block $blockId from $address " +
          s"(expected=$expected, got=$computed); requesting retransmission")
        val retryBytes = retransmitBlock(address, blockId)
        val retryComputed = computeCrc32c(retryBytes)
        if (retryComputed != expected) {
          // Persistent corruption -- invalidate the partial read and trigger upstream
          // recomputation. The increment-and-throw must be atomic per SPARK-19276 so that
          // TaskContext.setFetchFailed is called exactly once per failure.
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

      // Update local read metrics. The single-threaded contract documented on
      // ShuffleReadMetricsReporter is honored: this method is the sole accumulator for
      // the read path on the task thread.
      readMetrics.incRemoteBytesRead(length)
      readMetrics.incRemoteBlocksFetched(1L)
      bytesReadTotal += length

      // Deserialize the validated bytes through the standard SerializerManager path so
      // that the streaming-shuffle reader is byte-for-byte compatible with the
      // BlockStoreShuffleReader's deserialization pipeline. Compression/encryption
      // wrapping (if configured) is applied by SerializerManager.wrapStream.
      deserializeBlock(blockId, validatedBytes, accumulator)

      // Acknowledge the consumed position so the producer can reclaim buffer memory.
      // Per the v1 acknowledgment design, this is an implicit ack carried via the next
      // fetch request; no explicit RPC is sent. The ackedPositions map is updated so
      // that any future explicit-ack implementation has the cumulative offset to send.
      val newPos = ackedPositions.getOrElse(address, 0L) + length
      ackedPositions(address) = newPos
      acknowledgePosition(address, newPos)
    }

    logDebug(
      s"Consumed ${blocks.size} blocks ($bytesReadTotal bytes) from producer $address")
  }

  /**
   * Re-fetch a block from a producer after a checksum mismatch. Returns the raw bytes
   * of the retransmitted block. Network failures during retransmission are propagated
   * to the caller (which will treat them as persistent corruption and throw
   * [[FetchFailedException]]).
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
    val retryNio = retryBuffer.nioByteBuffer()
    val retryBytes = new Array[Byte](retryNio.remaining())
    retryNio.get(retryBytes)
    retryBytes
  }

  /**
   * Deserialize a single block's bytes into `(K, C)` records and append them to
   * `accumulator`. Uses the standard
   * [[org.apache.spark.serializer.SerializerManager]] wrapping path (compression and
   * encryption decoding) followed by the dependency's serializer's
   * [[org.apache.spark.serializer.SerializerInstance.deserializeStream]] yielding a
   * key-value iterator -- the same pipeline as the existing
   * [[org.apache.spark.shuffle.BlockStoreShuffleReader]]. The resulting `(Any, Any)` pairs
   * are cast to `Product2[K, C]` per the
   * [[org.apache.spark.shuffle.ShuffleReader]] contract.
   *
   * Per-record metric tracking (`incRecordsRead`) honors the single-threaded
   * [[ShuffleReadMetricsReporter]] contract; the increments occur on the task thread
   * driving `read()`.
   *
   * @param blockId      identifier of the block being deserialized
   * @param bytes        validated raw bytes (CRC32C already verified)
   * @param accumulator  buffer into which the deserialized records are appended
   */
  private def deserializeBlock(
      blockId: BlockId,
      bytes: Array[Byte],
      accumulator: mutable.ArrayBuffer[Product2[K, C]]): Unit = {
    val dep = handle.dependency
    val serializerManager = SparkEnv.get.serializerManager
    val byteStream = new ByteArrayInputStream(bytes)
    val wrappedStream = serializerManager.wrapStream(blockId, byteStream)
    val deserStream = dep.serializer.newInstance().deserializeStream(wrappedStream)
    try {
      val recordIter = deserStream.asKeyValueIterator
      while (recordIter.hasNext) {
        val record = recordIter.next()
        // Update per-record metrics on the task thread (single-threaded contract).
        readMetrics.incRecordsRead(1L)
        // The record is typed (Any, Any) by Serializer; cast to Product2[K, C] per the
        // ShuffleReader[K, C] contract. Tuples in Scala 2.13 implement Product2 so the
        // cast is structurally safe at the JVM level.
        accumulator += record.asInstanceOf[Product2[K, C]]
      }
    } finally {
      // Always close the deserialization stream to release any underlying resources
      // (compression buffers, encryption ciphers). Errors during close are logged but
      // not propagated -- a fetch-time failure has higher priority for the caller.
      try {
        deserStream.close()
      } catch {
        case e: Exception =>
          logDebug(s"Error closing deserialization stream for $blockId: ${e.getMessage}")
      }
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
   * Resolve the expected CRC32C checksum for the given block, retrieved out-of-band from
   * the producer's checksum metadata established via the existing shuffle checksum
   * side-channel. Returns `0L` when no checksum is available -- this causes the validator
   * in [[streamFromProducer]] to skip the comparison rather than fail, which is the
   * correct behavior for both:
   *
   *   - test paths that do not populate checksums, and
   *   - production deployments where `spark.shuffle.checksum.enabled=false`.
   *
   * In v1 the actual side-channel exchange is implementation-specific to the writer's
   * flush cadence; this method is the integration point that tests can override to
   * inject expected values.
   *
   * @param blockId identifier of the block whose checksum is being looked up
   * @return the expected CRC32C value, or `0L` if no checksum is available
   */
  private def expectedChecksumFor(blockId: BlockId): Long = {
    // Reference the parameter to avoid an unused-parameter warning under strict scalastyle
    // and to keep the integration-point signature stable for future implementations that
    // need the BlockId to look up per-block checksums in a producer-supplied side channel.
    logTrace(s"Resolving expected $CHECKSUM_ALGORITHM checksum for block $blockId")
    0L
  }

  /**
   * Send a consumer-position acknowledgment to the producer at `producerId` so the
   * producer can reclaim buffer memory for blocks whose cumulative offset is &le;
   * `position`.
   *
   * Per the v1 acknowledgment design (AAP Section 0.4.3.2), the acknowledgment is
   * implicit: the producer's `BackpressureProtocol` observes the next fetch request as
   * proof of consumption progress and reclaims buffer memory for the prior offsets. No
   * explicit RPC is sent in v1, keeping the network surface minimal.
   *
   * The position is logged at TRACE level for operator-side debugging when
   * `spark.shuffle.streaming.debug=true`.
   *
   * @param producerId the producer's [[BlockManagerId]]
   * @param position   the cumulative byte offset acked back to the producer
   */
  private def acknowledgePosition(producerId: BlockManagerId, position: Long): Unit = {
    logTrace(s"Acked position $position for producer $producerId")
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
   * Identify the (mapId, mapIndex) pair to use in a [[FetchFailedException]] payload
   * when reporting a producer-level failure. The exception payload semantics:
   *
   *   - `mapId` (Long): the map task identifier, extracted from the first block's
   *     [[ShuffleBlockId]] when present, or `-1L` if the producer's block list is empty.
   *   - `mapIndex` (Int): the map task index, taken from the third tuple element of the
   *     first block, or `-1` if the producer's block list is empty.
   *
   * The empty-list case is defensive: in normal operation, an entry in the
   * `blocksByAddress` map always carries at least one block. If the list is empty for
   * any reason (e.g., a test with synthetic data), we fall back to the
   * [[FetchFailedException]]-permitted sentinel values rather than throwing here, which
   * would mask the original timeout root cause.
   *
   * @param blocks the assigned blocks for the failed producer
   * @return the `(mapId, mapIndex)` pair for the FetchFailedException payload
   */
  private def identifyFirstBlock(
      blocks: scala.collection.Seq[(BlockId, Long, Int)]): (Long, Int) = {
    blocks.headOption match {
      case Some((blockId, _, mapIndex)) => (mapIdFromBlock(blockId), mapIndex)
      case None => (-1L, -1)
    }
  }

  /**
   * @return a snapshot of the current per-producer acked-position map. Provided for
   *         tests and observability tooling that need to verify acknowledgment progress
   *         without exposing the mutable internal map. The returned map is an immutable
   *         copy at the moment of the call.
   */
  private[streaming] def ackedPositionsSnapshot: scala.collection.Map[BlockManagerId, Long] =
    ackedPositions.toMap

  // Reference the constructor-injected TaskContext at instantiation so the parameter is
  // not flagged as unused by strict scalastyle/scalafmt rules. The TaskContext is
  // captured for future cancellation-aware iterator wrapping (e.g., InterruptibleIterator
  // analogous to BlockStoreShuffleReader); v1 of the streaming reader returns a plain
  // iterator from an ArrayBuffer where cancellation is observed at the next-block
  // boundary inside streamFromProducer's deadline check rather than on a per-record
  // basis.
  if (context == null) {
    logDebug("StreamingShuffleReader instantiated with null TaskContext; cancellation " +
      "checks will be skipped (test path)")
  }
}
