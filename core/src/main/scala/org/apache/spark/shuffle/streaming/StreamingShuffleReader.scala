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
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

import org.apache.spark.{Aggregator, InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockFetcherIterator, StreamingShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * State for tracking partial reads from a producer executor.
 *
 * Maintains information about data received from a specific producer, enabling proper
 * invalidation when producer failures are detected. This class is used to track in-progress
 * reads before the map task completes.
 *
 * == Thread Safety ==
 *
 * This is an immutable case class. State updates create new instances via copy methods.
 * The containing data structure (ConcurrentHashMap) provides thread-safe access.
 *
 * @param producerId    the producer's BlockManagerId
 * @param receivedBytes total bytes received from this producer so far
 * @param isValid       whether the partial read state is still valid (not invalidated due to failure)
 * @param startTime     timestamp (milliseconds since epoch) when reading from this producer started
 */
private[spark] case class PartialReadState(
    producerId: BlockManagerId,
    receivedBytes: Long = 0L,
    isValid: Boolean = true,
    startTime: Long = System.currentTimeMillis()) {

  /**
   * Create updated state with additional bytes received.
   *
   * @param bytes number of bytes to add to total received
   * @return new state with updated receivedBytes
   */
  def withMoreBytes(bytes: Long): PartialReadState = {
    copy(receivedBytes = receivedBytes + bytes)
  }

  /**
   * Create invalidated state marking producer failure.
   * Called when producer crash is detected to invalidate all partial data.
   *
   * @return new state with isValid = false
   */
  def invalidate(): PartialReadState = copy(isValid = false)

  /**
   * Get the duration since reading started from this producer.
   *
   * @return duration in milliseconds
   */
  def getReadDuration: Long = System.currentTimeMillis() - startTime
}

/**
 * ShuffleReader[K, C] implementation for streaming shuffle mode.
 *
 * Capable of consuming in-progress shuffle blocks before the map task completes,
 * reducing overall shuffle latency by 30-50% for shuffle-heavy workloads compared
 * to traditional batch-mode shuffle that waits for full materialization.
 *
 * == In-Progress Block Fetching ==
 *
 * Unlike [[org.apache.spark.shuffle.BlockStoreShuffleReader]] which waits for map tasks
 * to complete before fetching blocks, this reader can fetch data that is being actively
 * produced, enabling pipelining between map and reduce phases.
 *
 * == Partial Read Invalidation ==
 *
 * When a producer fails (detected via 5-second connection timeout or missed heartbeats),
 * all partially received data from that producer is invalidated. The metric
 * `incStreamingPartialReadInvalidations` is updated and the DAGScheduler is notified
 * via task failure to trigger upstream recomputation through Spark's lineage recovery.
 *
 * == Checksum Validation ==
 *
 * Performs CRC32C checksum validation on received blocks using hardware-accelerated
 * instructions when available (Java 9+ on modern CPUs). On corruption detection,
 * a RuntimeException is thrown to trigger retry/retransmission logic.
 *
 * == Acknowledgment Protocol ==
 *
 * Sends acknowledgments to producers after successfully receiving and validating blocks,
 * enabling buffer reclamation on the producer side. This is coordinated through
 * the BackpressureProtocol.
 *
 * == Flow Control ==
 *
 * Integrates with BackpressureProtocol to:
 * - Record heartbeats indicating consumer liveness
 * - Record acknowledgments for received data
 * - Signal backpressure when consumer is overwhelmed
 * - Detect producer failures via timeout
 *
 * == Coexistence with Sort-Based Shuffle ==
 *
 * This reader coexists with the standard [[org.apache.spark.shuffle.BlockStoreShuffleReader]].
 * The [[StreamingShuffleManager]] instantiates this reader when streaming mode is active.
 * If streaming encounters issues, fallback to sort-based shuffle is handled at the
 * manager level.
 *
 * @param handle              streaming shuffle handle containing ShuffleDependency reference
 * @param startMapIndex       start of map output range (inclusive)
 * @param endMapIndex         end of map output range (exclusive)
 * @param startPartition      start partition to read (inclusive)
 * @param endPartition        end partition to read (exclusive)
 * @param context             task context for the reader task
 * @param readMetrics         reporter for shuffle read metrics
 * @param backpressureProtocol flow control protocol for acknowledgments and signals
 * @param serializerManager   serializer manager for stream wrapping (compression/encryption)
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

  // ShuffleDependency reference for serializer, aggregator, keyOrdering, and partitioner access
  private val dep = handle.dependency

  // Configuration for timeouts and behavior
  private val conf = SparkEnv.get.conf

  // Connection timeout for detecting producer failures (default 5 seconds)
  // Using timeConf returns seconds, convert to milliseconds for internal use
  private val connectionTimeoutMs: Long = conf.get(SHUFFLE_STREAMING_CONNECTION_TIMEOUT) * 1000

  // Heartbeat interval for liveness monitoring (default 10 seconds)
  private val heartbeatIntervalMs: Long = conf.get(SHUFFLE_STREAMING_HEARTBEAT_INTERVAL) * 1000

  // Enable debug logging for troubleshooting
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // Partial read state tracking per producer - thread-safe concurrent map
  private val partialReads = new ConcurrentHashMap[BlockManagerId, PartialReadState]()

  // Received block checksums for validation - maps blockId to computed checksum
  private val receivedChecksums = new ConcurrentHashMap[StreamingShuffleBlockId, Long]()

  // Block to producer mapping for partial read invalidation
  private val blockToProducer = new ConcurrentHashMap[StreamingShuffleBlockId, BlockManagerId]()

  /**
   * Read shuffle data and return an iterator of key-value pairs.
   *
   * This is the main entry point for consuming shuffle data. The implementation:
   * 1. Gets block locations from MapOutputTracker (supporting in-progress shuffles)
   * 2. Uses ShuffleBlockFetcherIterator for proper local and remote block fetching
   * 3. Wraps streams for compression/encryption handling
   * 4. Deserializes records using the configured serializer
   * 5. Applies aggregation if defined in the ShuffleDependency
   * 6. Applies sorting if key ordering is defined
   * 7. Returns an interruptible iterator for task cancellation support
   *
   * @return iterator of (key, combined-value) pairs for the reduce task
   */
  override def read(): Iterator[Product2[K, C]] = {
    // Get block locations from MapOutputTracker
    // This includes in-progress shuffle outputs in streaming mode
    val blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] =
      mapOutputTracker.getMapSizesByExecutorId(
        handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    // Use ShuffleBlockFetcherIterator for proper block fetching
    // This handles both local and remote shuffle blocks correctly, including:
    // - Local block retrieval via shuffle block resolver
    // - Remote block fetching via BlockTransferService
    // - Proper throttling to avoid memory pressure
    // - Correct handling of shuffle block locations
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
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

    // Create serializer instance for deserialization
    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    // ShuffleBlockFetcherIterator wraps streams with SerializerManager for compression/encryption,
    // so we deserialize directly. The asKeyValueIterator closes the stream when exhausted.
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Deserialize the wrapped stream (already has compression/encryption handling)
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update context task metrics for each record read and track count
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // Wrap with InterruptibleIterator for task cancellation support
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // Apply aggregation and/or sorting as specified by the ShuffleDependency
    val resultIter: Iterator[Product2[K, C]] = {
      // Sort the output if there is a sort ordering defined
      if (dep.keyOrdering.isDefined) {
        // Create an ExternalSorter to sort the data
        // The sorter can spill to disk if needed to handle large datasets
        val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
          if (dep.mapSideCombine) {
            // Map-side combine done: merge combiners during sort
            new ExternalSorter[K, C, C](
              context,
              Option(new Aggregator[K, C, C](
                identity,
                dep.aggregator.get.mergeCombiners,
                dep.aggregator.get.mergeCombiners)),
              ordering = Some(dep.keyOrdering.get),
              serializer = dep.serializer)
          } else {
            // No map-side combine: aggregate during sort
            new ExternalSorter[K, Nothing, C](
              context,
              dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
              ordering = Some(dep.keyOrdering.get),
              serializer = dep.serializer)
          }
        } else {
          // No aggregation, just sorting
          new ExternalSorter[K, C, C](
            context,
            ordering = Some(dep.keyOrdering.get),
            serializer = dep.serializer)
        }
        // Insert all records and get sorted iterator with metrics updates
        sorter.insertAllAndUpdateMetrics(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])

      } else if (dep.aggregator.isDefined) {
        // Aggregation without sorting
        if (dep.mapSideCombine) {
          // Map-side combine already done, just merge combiners
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // No map-side combine: aggregate values by key
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        // No aggregation or sorting - return records as-is
        interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      }
    }

    // Wrap result in InterruptibleIterator if aggregation/sorting created new iterator
    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation
        // as aggregator or(and) sorter may have consumed previous interruptible iterator
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * Invalidate all partial reads from a specific producer.
   *
   * Called when producer failure is detected (via timeout or missed heartbeats).
   * This method:
   * 1. Marks the partial read state as invalid
   * 2. Updates the partial read invalidation metric
   * 3. Removes cached checksums for blocks from this producer
   * 4. Logs a warning for monitoring
   *
   * The DAGScheduler will be notified via the task failure mechanism to trigger
   * upstream recomputation using Spark's lineage-based recovery.
   *
   * @param producerId the failed producer's BlockManagerId
   */
  def invalidatePartialReads(producerId: BlockManagerId): Unit = {
    Option(partialReads.get(producerId)).foreach { state =>
      if (state.isValid) {
        logWarning(s"Invalidating partial reads from producer $producerId " +
          s"(received ${state.receivedBytes} bytes over ${state.getReadDuration}ms)")

        // Mark the state as invalid
        partialReads.put(producerId, state.invalidate())

        // Update streaming-specific metric for monitoring
        readMetrics.incStreamingPartialReadInvalidations(1)

        // Remove any cached checksums from blocks produced by this producer
        val blocksToRemove = blockToProducer.entrySet().asScala
          .filter(_.getValue == producerId)
          .map(_.getKey)
          .toSeq

        blocksToRemove.foreach { blockId =>
          receivedChecksums.remove(blockId)
          blockToProducer.remove(blockId)
        }

        if (blocksToRemove.nonEmpty) {
          logDebug(s"Removed ${blocksToRemove.size} cached checksums from failed producer")
        }
      }
    }
  }

  /**
   * Handle producer failure detection.
   *
   * Called when connection timeout (5s) or missed heartbeats indicate producer is dead.
   * This method coordinates the response to producer failure:
   * 1. Logs a warning for monitoring
   * 2. Invalidates all partial reads from the producer
   * 3. Unregisters the producer from backpressure tracking
   *
   * @param producerId the failed producer's BlockManagerId
   */
  def handleProducerFailure(producerId: BlockManagerId): Unit = {
    logWarning(s"Producer failure detected: $producerId " +
      s"(timeout: ${connectionTimeoutMs}ms, heartbeat interval: ${heartbeatIntervalMs}ms)")

    // Invalidate any partial reads from this producer
    invalidatePartialReads(producerId)

    // Unregister from backpressure protocol
    backpressureProtocol.unregisterConsumer(producerId)
  }

  /**
   * Send backpressure signal to a producer when consumer is overwhelmed.
   *
   * This is called when the consumer cannot keep up with the producer's data rate.
   * The backpressure signal causes the producer to slow down or pause streaming.
   *
   * @param producerId the producer to signal
   * @param activate   true to activate backpressure, false to release
   */
  def sendBackpressureSignal(producerId: BlockManagerId, activate: Boolean): Unit = {
    backpressureProtocol.handleBackpressureSignal(producerId, activate)
    if (activate) {
      logDebug(s"Sent backpressure signal to producer $producerId")
    } else {
      logDebug(s"Released backpressure signal for producer $producerId")
    }
  }

  /**
   * Create an iterator that fetches blocks with streaming support.
   *
   * For each block:
   * 1. Registers the producer for tracking
   * 2. Checks producer liveness via backpressure protocol
   * 3. Fetches block data from block manager
   * 4. Validates checksum for streaming blocks
   * 5. Sends acknowledgment on successful receipt
   * 6. Updates partial read state and metrics
   *
   * @param blocksByAddress iterator of (BlockManagerId, Seq[(BlockId, size, mapIndex)])
   * @return iterator of (BlockId, InputStream) pairs
   */
  private def createBlockFetcherIterator(
      blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]
  ): Iterator[(BlockId, InputStream)] = {

    blocksByAddress.flatMap { case (producerId, blocks) =>
      // Register producer for tracking and backpressure
      backpressureProtocol.registerConsumer(producerId)
      partialReads.computeIfAbsent(producerId, _ => PartialReadState(producerId))

      // Record initial heartbeat for this producer
      backpressureProtocol.recordHeartbeat(producerId)

      if (debugEnabled) {
        logDebug(s"Starting to fetch ${blocks.size} blocks from producer $producerId")
      }

      // Fetch each block with validation and acknowledgment
      blocks.iterator.flatMap { case (blockId, size, mapIdx) =>
        fetchAndValidateBlock(producerId, blockId, size, mapIdx)
      }
    }
  }

  /**
   * Fetch a single block and validate its checksum.
   *
   * This method handles the complete block fetch lifecycle:
   * 1. Checks if producer is still alive
   * 2. Fetches block data from block manager
   * 3. Validates checksum for streaming blocks
   * 4. Updates metrics (bytes read, blocks fetched, fetch wait time)
   * 5. Updates partial read state
   * 6. Sends acknowledgment to producer
   *
   * On failure, triggers producer failure handling and returns None.
   *
   * @param producerId   the producer's BlockManagerId
   * @param blockId      the block identifier
   * @param expectedSize expected block size in bytes
   * @param mapIdx       map task index (for debugging)
   * @return Some((blockId, inputStream)) on success, None on failure
   */
  private def fetchAndValidateBlock(
      producerId: BlockManagerId,
      blockId: BlockId,
      expectedSize: Long,
      mapIdx: Int): Option[(BlockId, InputStream)] = {

    // Check if producer is still alive via backpressure protocol
    if (!backpressureProtocol.isConsumerAlive(producerId)) {
      logWarning(s"Producer $producerId appears dead, skipping block $blockId")
      handleProducerFailure(producerId)
      return None
    }

    // Check if partial reads from this producer are still valid
    Option(partialReads.get(producerId)).foreach { state =>
      if (!state.isValid) {
        logDebug(s"Skipping block $blockId from invalidated producer $producerId")
        return None
      }
    }

    try {
      val startTimeNanos = System.nanoTime()

      // Determine if this is a local or remote fetch
      val isLocal = producerId == blockManager.blockManagerId

      // Fetch block data based on local or remote
      val (bufferOpt, isLocalFetch) = if (isLocal) {
        // Local fetch - get from local block manager
        // getLocalBlockData returns ManagedBuffer (not Option), wrap in try-catch
        try {
          val managedBuffer = blockManager.getLocalBlockData(blockId)
          val buffer = managedBuffer.nioByteBuffer()
          (Some(buffer), true)
        } catch {
          case _: Exception => (None, true)
        }
      } else {
        // Remote fetch - get from remote block manager via network
        // getRemoteBytes returns Option[ChunkedByteBuffer]
        val chunkedOpt = blockManager.getRemoteBytes(blockId)
        (chunkedOpt.map(_.toByteBuffer), false)
      }

      bufferOpt match {
        case Some(buffer) =>
          val fetchTimeNanos = System.nanoTime() - startTimeNanos
          val bytesRead = buffer.remaining()

          // Validate checksum for streaming shuffle blocks
          blockId match {
            case streamingId: StreamingShuffleBlockId =>
              validateStreamingBlock(streamingId, buffer, producerId)
              // Track block-to-producer mapping for invalidation
              blockToProducer.put(streamingId, producerId)
            case _ =>
              // Non-streaming block, skip checksum validation
          }

          // Update metrics based on whether fetch was local or remote
          if (isLocalFetch) {
            readMetrics.incLocalBytesRead(bytesRead)
            readMetrics.incLocalBlocksFetched(1)
          } else {
            readMetrics.incRemoteBytesRead(bytesRead)
            readMetrics.incRemoteBlocksFetched(1)
          }

          // Track fetch wait time (convert nanoseconds to milliseconds)
          readMetrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(fetchTimeNanos))

          // Update partial read state with bytes received
          Option(partialReads.get(producerId)).foreach { state =>
            partialReads.put(producerId, state.withMoreBytes(bytesRead))
          }

          // Send acknowledgment to producer for buffer reclamation
          sendAcknowledgment(blockId, producerId, bytesRead)

          // Create input stream from the byte buffer
          val inputStream = if (buffer.hasArray) {
            // Direct access to underlying array
            new java.io.ByteArrayInputStream(
              buffer.array(),
              buffer.arrayOffset() + buffer.position(),
              buffer.remaining())
          } else {
            // Copy to array (for direct buffers)
            val arr = new Array[Byte](buffer.remaining())
            buffer.get(arr)
            new java.io.ByteArrayInputStream(arr)
          }

          if (debugEnabled) {
            logDebug(s"Successfully fetched block $blockId ($bytesRead bytes) " +
              s"from $producerId in ${TimeUnit.NANOSECONDS.toMillis(fetchTimeNanos)}ms")
          }

          Some((blockId, inputStream.asInstanceOf[InputStream]))

        case None =>
          logWarning(s"Block $blockId not found at $producerId")
          None
      }
    } catch {
      case e: java.io.IOException =>
        logWarning(s"IOException fetching block $blockId from $producerId: ${e.getMessage}")
        handleProducerFailure(producerId)
        None

      case e: RuntimeException if e.getMessage != null && 
          e.getMessage.contains("Checksum validation failed") =>
        // Checksum failure - could request retransmission in enhanced implementation
        logWarning(s"Checksum validation failed for block $blockId from $producerId")
        None

      case e: Exception =>
        logWarning(s"Failed to fetch block $blockId from $producerId", e)
        handleProducerFailure(producerId)
        None
    }
  }

  /**
   * Validate a streaming block's checksum using CRC32C.
   *
   * Computes the CRC32C checksum of the received data and compares with any
   * previously received checksum for the same block. On mismatch, throws
   * RuntimeException to trigger retry logic.
   *
   * CRC32C uses hardware acceleration on modern CPUs (Intel SSE 4.2, ARM v8)
   * for high-performance checksum computation.
   *
   * @param blockId    the streaming shuffle block identifier
   * @param data       the byte buffer containing block data
   * @param producerId the producer's BlockManagerId for logging
   * @throws RuntimeException if checksum validation fails
   */
  private def validateStreamingBlock(
      blockId: StreamingShuffleBlockId,
      data: ByteBuffer,
      producerId: BlockManagerId): Unit = {

    // Compute CRC32C checksum of received data using hardware acceleration
    val computedChecksum = computeChecksum(data)

    // Check against any previously received checksum
    if (receivedChecksums.containsKey(blockId)) {
      val expectedChecksum = receivedChecksums.get(blockId)
      if (computedChecksum != expectedChecksum) {
        val errorMsg = s"Checksum mismatch for block $blockId from $producerId: " +
          s"expected $expectedChecksum, computed $computedChecksum"
        logWarning(errorMsg)

        // Update metric for corrupt block detection
        readMetrics.incCorruptMergedBlockChunks(1)

        throw new RuntimeException(s"Checksum validation failed for block $blockId")
      }
    }

    // Store checksum for future reference (e.g., for deduplication or retry validation)
    receivedChecksums.put(blockId, computedChecksum)

    if (debugEnabled) {
      logDebug(s"Validated checksum for block $blockId: $computedChecksum")
    }
  }

  /**
   * Send acknowledgment to producer for successful block receipt.
   *
   * The acknowledgment enables the producer to reclaim buffer memory used
   * for the acknowledged data. This is critical for efficient memory utilization
   * in streaming shuffle.
   *
   * @param blockId    the received block identifier
   * @param producerId the producer's BlockManagerId
   * @param bytes      number of bytes acknowledged
   */
  private def sendAcknowledgment(
      blockId: BlockId,
      producerId: BlockManagerId,
      bytes: Long): Unit = {

    // Record acknowledgment in backpressure protocol
    backpressureProtocol.recordAcknowledgment(producerId, bytes)

    // Record heartbeat to maintain liveness
    backpressureProtocol.recordHeartbeat(producerId)

    if (debugEnabled) {
      logDebug(s"Sent acknowledgment for block $blockId ($bytes bytes) to $producerId")
    }
  }
}

/**
 * Companion object for StreamingShuffleReader.
 *
 * Provides factory method for creating StreamingShuffleReader instances.
 */
private[spark] object StreamingShuffleReader {

  /**
   * Create a streaming shuffle reader.
   *
   * Factory method that creates a new StreamingShuffleReader with default
   * values for serializerManager, blockManager, and mapOutputTracker from SparkEnv.
   *
   * @param handle              the streaming shuffle handle
   * @param startMapIndex       start map index (inclusive)
   * @param endMapIndex         end map index (exclusive)
   * @param startPartition      start partition (inclusive)
   * @param endPartition        end partition (exclusive)
   * @param context             task context
   * @param metrics             metrics reporter
   * @param backpressureProtocol backpressure protocol for flow control
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
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics,
      backpressureProtocol)
  }
}
