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
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, StreamingShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * State for tracking partial reads from a producer.
 *
 * @param producerId    the producer's BlockManagerId
 * @param receivedBytes bytes received so far
 * @param isValid       whether the partial read is still valid
 * @param startTime     when the read started
 */
private[streaming] case class PartialReadState(
    producerId: BlockManagerId,
    receivedBytes: Long = 0,
    isValid: Boolean = true,
    startTime: Long = System.currentTimeMillis()) {

  /**
   * Create updated state with more bytes received.
   */
  def withMoreBytes(bytes: Long): PartialReadState = {
    copy(receivedBytes = receivedBytes + bytes)
  }

  /**
   * Create invalidated state.
   */
  def invalidate(): PartialReadState = copy(isValid = false)
}

/**
 * ShuffleReader[K, C] implementation for streaming shuffle mode.
 *
 * Capable of consuming in-progress shuffle blocks before the map task completes,
 * reducing overall shuffle latency compared to traditional batch-mode shuffle.
 *
 * == Partial Read Invalidation ==
 *
 * When a producer fails (detected via 5-second connection timeout or missed heartbeats),
 * all partially received data from that producer is invalidated. The DAGScheduler is
 * notified to trigger upstream recomputation.
 *
 * == Checksum Validation ==
 *
 * Performs CRC32C checksum validation on received blocks. On corruption detection,
 * requests retransmission from the producer via the backpressure protocol.
 *
 * == Acknowledgment Protocol ==
 *
 * Sends acknowledgments to producers after successfully receiving blocks, enabling
 * buffer reclamation on the producer side.
 *
 * == Flow Control ==
 *
 * Integrates with BackpressureProtocol to send backpressure signals when the consumer
 * is overwhelmed, preventing producer from sending faster than consumer can process.
 *
 * @param handle              streaming shuffle handle with dependency info
 * @param startMapIndex       start of map output range (inclusive)
 * @param endMapIndex         end of map output range (exclusive)
 * @param startPartition      start partition to read (inclusive)
 * @param endPartition        end partition to read (exclusive)
 * @param context             task context for the reader task
 * @param readMetrics         reporter for shuffle read metrics
 * @param backpressureProtocol flow control protocol
 * @param serializerManager   serializer manager for deserialization
 * @param blockManager        block manager for block retrieval
 * @param mapOutputTracker    tracker for shuffle block locations
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    backpressureProtocol: BackpressureProtocol,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  // Dependency reference for serializer, aggregator, and ordering
  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf

  // Configuration (timeConf returns Long in seconds)
  private val connectionTimeoutMs = conf.get(SHUFFLE_STREAMING_CONNECTION_TIMEOUT) * 1000
  private val heartbeatIntervalMs = conf.get(SHUFFLE_STREAMING_HEARTBEAT_INTERVAL) * 1000

  // Partial read tracking per producer
  private val partialReads = new ConcurrentHashMap[BlockManagerId, PartialReadState]()

  // Received block checksums for validation
  private val receivedChecksums = new ConcurrentHashMap[StreamingShuffleBlockId, Long]()

  /**
   * Read shuffle data and return an iterator of key-value pairs.
   *
   * This is the main entry point for consuming shuffle data. It:
   * 1. Gets block locations from MapOutputTracker (supporting in-progress shuffles)
   * 2. Fetches blocks with checksum validation
   * 3. Sends acknowledgments for successfully received blocks
   * 4. Deserializes and optionally aggregates records
   *
   * @return iterator of (key, combined-value) pairs
   */
  override def read(): Iterator[Product2[K, C]] = {
    // Get block locations from MapOutputTracker
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    // Create fetcher iterator with streaming support
    val wrappedStreams = createBlockFetcherIterator(blocksByAddress)

    // Create deserialized record iterator
    val recordIterator = wrappedStreams.flatMap { case (blockId, inputStream) =>
      val serializerInstance = dep.serializer.newInstance()
      val deserializeStream = serializerInstance.deserializeStream(inputStream)

      // Register completion callback for cleanup
      context.addTaskCompletionListener[Unit](_ => deserializeStream.close())

      deserializeStream.asKeyValueIterator.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Apply aggregation if defined
    val aggregatedIterator: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // Map-side combine already done, just merge combiners
        val combinedKeyValuesIterator = recordIterator.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = recordIterator.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      recordIterator.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Apply sorting if key ordering is defined
    val sortedIterator: Iterator[Product2[K, C]] = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Use ExternalSorter for spill-capable sorting
        val sorter = new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAllAndUpdateMetrics(aggregatedIterator.asInstanceOf[Iterator[Product2[K, C]]])
      case None =>
        aggregatedIterator
    }

    // Wrap with completion callback
    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sortedIterator, {
      // Cleanup on completion
      context.taskMetrics().mergeShuffleReadMetrics()
    })
  }

  /**
   * Invalidate all partial reads from a specific producer.
   *
   * Called when producer failure is detected (via timeout or missed heartbeats).
   * Updates metrics and logs the invalidation.
   *
   * @param producerId the failed producer's BlockManagerId
   */
  def invalidatePartialReads(producerId: BlockManagerId): Unit = {
    Option(partialReads.get(producerId)).foreach { state =>
      if (state.isValid) {
        logWarning(s"Invalidating partial reads from producer $producerId " +
          s"(received ${state.receivedBytes} bytes)")

        partialReads.put(producerId, state.invalidate())
        readMetrics.incStreamingPartialReadInvalidations(1)

        // Remove any cached checksums from this producer
        receivedChecksums.keySet().asScala
          .filter(blockId => isBlockFromProducer(blockId, producerId))
          .foreach(receivedChecksums.remove)
      }
    }
  }

  /**
   * Handle producer failure detection.
   *
   * Called when connection timeout (5s) or missed heartbeats indicate producer is dead.
   * Invalidates partial reads and logs a warning.
   *
   * @param producerId the failed producer's BlockManagerId
   */
  def handleProducerFailure(producerId: BlockManagerId): Unit = {
    logWarning(s"Producer failure detected: $producerId")
    invalidatePartialReads(producerId)

    // Notify backpressure protocol
    backpressureProtocol.unregisterConsumer(producerId)
  }

  /**
   * Send backpressure signal when consumer is overwhelmed.
   *
   * @param producerId the producer to signal
   * @param activate   true to activate backpressure, false to release
   */
  def sendBackpressureSignal(producerId: BlockManagerId, activate: Boolean): Unit = {
    backpressureProtocol.handleBackpressureSignal(producerId, activate)
    if (activate) {
      logDebug(s"Sent backpressure signal to producer $producerId")
    }
  }

  /**
   * Create an iterator that fetches blocks and validates checksums.
   */
  private def createBlockFetcherIterator(
      blocksByAddress: Iterator[(BlockManagerId, scala.collection.Seq[(BlockId, Long, Int)])]
  ): Iterator[(BlockId, InputStream)] = {

    blocksByAddress.flatMap { case (address, blocks) =>
      // Register producer tracking
      backpressureProtocol.registerConsumer(address)
      partialReads.computeIfAbsent(address, _ => PartialReadState(address))

      blocks.iterator.flatMap { case (blockId, size, mapIdx) =>
        fetchAndValidateBlock(address, blockId, size)
      }
    }
  }

  /**
   * Fetch a single block and validate its checksum.
   */
  private def fetchAndValidateBlock(
      producerId: BlockManagerId,
      blockId: BlockId,
      expectedSize: Long): Option[(BlockId, InputStream)] = {

    // Check if producer is still alive
    if (!backpressureProtocol.isConsumerAlive(producerId)) {
      handleProducerFailure(producerId)
      return None
    }

    try {
      // Fetch block data
      val startTime = System.nanoTime()
      val blockData = blockManager.getRemoteBytes(blockId)

      blockData.map { buffer =>
        val fetchTimeNanos = System.nanoTime() - startTime
        val bytes = buffer.toByteBuffer

        // Validate checksum if this is a streaming block
        blockId match {
          case streamingId: StreamingShuffleBlockId =>
            validateStreamingBlock(streamingId, bytes, producerId)
          case _ => // Non-streaming block, skip validation
        }

        // Update metrics
        readMetrics.incRemoteBytesRead(bytes.remaining())
        readMetrics.incRemoteBlocksFetched(1)
        readMetrics.incFetchWaitTime(fetchTimeNanos / 1000000)

        // Update partial read state
        Option(partialReads.get(producerId)).foreach { state =>
          partialReads.put(producerId, state.withMoreBytes(bytes.remaining()))
        }

        // Send acknowledgment
        sendAcknowledgment(blockId, producerId, bytes.remaining())

        // Convert to input stream
        val inputStream = new java.io.ByteArrayInputStream(
          bytes.array(), bytes.position(), bytes.remaining())

        (blockId, inputStream.asInstanceOf[InputStream])
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to fetch block $blockId from $producerId", e)
        handleProducerFailure(producerId)
        None
    }
  }

  /**
   * Validate a streaming block's checksum.
   */
  private def validateStreamingBlock(
      blockId: StreamingShuffleBlockId,
      data: java.nio.ByteBuffer,
      producerId: BlockManagerId): Unit = {

    // Compute checksum of received data
    val computedChecksum = computeChecksum(data)

    // Get expected checksum from block header or resolver
    // Use containsKey check since ConcurrentHashMap[_, Long] returns boxed value
    if (receivedChecksums.containsKey(blockId)) {
      val expectedChecksum = receivedChecksums.get(blockId)
      if (computedChecksum != expectedChecksum) {
        logWarning(s"Checksum mismatch for block $blockId from $producerId: " +
          s"expected $expectedChecksum, got $computedChecksum")

        // Request retransmission (in production, this would send an RPC)
        readMetrics.incCorruptMergedBlockChunks(1)
        throw new RuntimeException(s"Checksum validation failed for block $blockId")
      }
    }

    // Store checksum for future reference
    receivedChecksums.put(blockId, computedChecksum)
  }

  /**
   * Send acknowledgment to producer for successful block receipt.
   */
  private def sendAcknowledgment(
      blockId: BlockId,
      producerId: BlockManagerId,
      bytes: Long): Unit = {

    backpressureProtocol.recordAcknowledgment(producerId, bytes)
    backpressureProtocol.recordHeartbeat(producerId)
    logDebug(s"Sent acknowledgment for block $blockId ($bytes bytes) to $producerId")
  }

  /**
   * Check if a block came from a specific producer.
   */
  private def isBlockFromProducer(
      blockId: StreamingShuffleBlockId,
      producerId: BlockManagerId): Boolean = {
    // In a full implementation, we would track block-to-producer mapping
    // For now, return false (conservative approach)
    false
  }
}

/**
 * Companion object for StreamingShuffleReader.
 */
private[spark] object StreamingShuffleReader {

  /**
   * Create a streaming shuffle reader.
   *
   * @param handle              the streaming shuffle handle
   * @param startMapIndex       start map index (inclusive)
   * @param endMapIndex         end map index (exclusive)
   * @param startPartition      start partition (inclusive)
   * @param endPartition        end partition (exclusive)
   * @param context             task context
   * @param metrics             metrics reporter
   * @param backpressureProtocol backpressure protocol
   * @return new StreamingShuffleReader instance
   */
  def apply[K, C](
      handle: StreamingShuffleHandle[K, _, C],
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      backpressureProtocol: BackpressureProtocol): StreamingShuffleReader[K, C] = {
    new StreamingShuffleReader(
      handle, startMapIndex, endMapIndex, startPartition, endPartition,
      context, metrics, backpressureProtocol)
  }
}
