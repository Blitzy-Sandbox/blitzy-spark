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

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32C

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{
  BLOCK_ID, BYTE_SIZE, CONSUMER, ELAPSED_TIME, ERROR, MAP_ID, NUM_BLOCKS,
  PARTITION_ID, SHUFFLE_ID, TIMEOUT
}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{
  BaseShuffleHandle, FetchFailedException, ShuffleHandle, ShuffleReader,
  ShuffleReadMetricsReporter
}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

/**
 * Reduce-side shuffle reader implementation for streaming shuffle mode with in-progress
 * block support.
 *
 * This reader implements the [[ShuffleReader]] trait to provide streaming shuffle functionality:
 *   - Polls producers for available data before shuffle completion
 *   - Validates block integrity via CRC32C checksums
 *   - Detects producer failure via 5-second connection timeout
 *   - Triggers upstream recomputation on producer failure detection
 *   - Discards buffered data from failed shuffle attempts
 *   - Coordinates with BackpressureProtocol for flow control signaling
 *
 * == Streaming Protocol ==
 *
 * Unlike traditional shuffle readers that wait for complete shuffle data materialization,
 * StreamingShuffleReader actively polls producer executors for available data. This enables
 * pipelined execution where consumers can begin processing data before producers complete.
 *
 * Data flow:
 * 1. Reader registers with BackpressureProtocol for flow control
 * 2. Reader polls producer executors for available data via StreamingShuffleBlockResolver
 * 3. Received blocks are validated via CRC32C checksum
 * 4. Reader sends acknowledgments via BackpressureProtocol
 * 5. On producer failure, partial reads are atomically invalidated
 *
 * == Failure Handling ==
 *
 * Producer failure is detected via heartbeat timeout (default 5 seconds). When failure
 * is detected:
 * 1. All partial reads from the failed producer are atomically invalidated
 * 2. FetchFailedException is thrown to trigger upstream recomputation
 * 3. Buffered data from the failed attempt is discarded
 *
 * Retry logic uses exponential backoff starting at 1 second with maximum 5 attempts.
 *
 * == Coexistence Strategy ==
 *
 * This reader coexists with BlockStoreShuffleReader which handles traditional sort-based
 * shuffles. When streaming conditions are not met (e.g., protocol version mismatch,
 * sustained consumer slowdown), the shuffle manager falls back to BlockStoreShuffleReader.
 *
 * @param handle The shuffle handle containing shuffle dependency information
 * @param startMapIndex Starting map index (inclusive) to read from
 * @param endMapIndex Ending map index (exclusive) to read from
 * @param startPartition Starting partition index (inclusive) to read
 * @param endPartition Ending partition index (exclusive) to read
 * @param context Task context for task lifecycle management
 * @param readMetrics Metrics reporter for shuffle read statistics
 * @param config Configuration wrapper for streaming shuffle parameters
 * @param blockResolver Block resolver for accessing in-flight and spilled data
 * @param backpressureProtocol Protocol handler for flow control signaling
 * @param metrics Telemetry component for streaming shuffle metrics
 * @since 4.2.0
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: ShuffleHandle,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    config: StreamingShuffleConfig,
    blockResolver: StreamingShuffleBlockResolver,
    backpressureProtocol: BackpressureProtocol,
    metrics: StreamingShuffleMetrics,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager)
  extends ShuffleReader[K, C]
  with Logging {

  // ============================================================================
  // Configuration and Constants
  // ============================================================================

  /**
   * Heartbeat timeout for producer failure detection in milliseconds.
   * Uses configured value (default 5 seconds).
   */
  private val heartbeatTimeoutMs: Int = config.heartbeatTimeoutMs

  /**
   * Debug logging flag from configuration.
   */
  private val isDebug: Boolean = config.isDebug

  /**
   * Initial delay for exponential backoff retry.
   */
  private val retryInitialDelayMs: Int = DEFAULT_RETRY_INITIAL_DELAY_MS

  /**
   * Maximum number of retry attempts.
   */
  private val maxRetryAttempts: Int = DEFAULT_MAX_RETRY_ATTEMPTS

  // ============================================================================
  // Shuffle State
  // ============================================================================

  /**
   * Shuffle ID extracted from the handle.
   */
  private val shuffleId: Int = handle.shuffleId

  /**
   * Unique consumer ID for backpressure tracking.
   * Format: "reader-{shuffleId}-{partitionId}-{taskAttemptId}"
   */
  private val consumerId: String =
    s"reader-$shuffleId-$startPartition-${context.taskAttemptId()}"

  // ============================================================================
  // Partial Read Tracking
  // ============================================================================

  /**
   * Thread-safe map tracking partial reads from each producer (map task).
   * Key: mapId, Value: List of received block data and checksums.
   */
  private val partialReads = new ConcurrentHashMap[Long, ArrayBuffer[PartialBlockData]]()

  /**
   * Atomic flag indicating whether partial reads have been invalidated.
   */
  private val partialReadsInvalidated = new AtomicBoolean(false)

  /**
   * Set of producer IDs (map IDs) that have been detected as failed.
   */
  private val failedProducers = new ConcurrentHashMap[Long, java.lang.Boolean]()

  /**
   * Last heartbeat timestamp per producer for timeout detection.
   */
  private val producerLastHeartbeat = new ConcurrentHashMap[Long, java.lang.Long]()

  // ============================================================================
  // Initialization
  // ============================================================================

  // Register with backpressure protocol for flow control
  backpressureProtocol.registerConsumer(consumerId)

  // Initialize producer heartbeat tracking for all map tasks in range
  for (mapId <- startMapIndex until endMapIndex) {
    producerLastHeartbeat.put(mapId.toLong, System.currentTimeMillis())
    partialReads.put(mapId.toLong, new ArrayBuffer[PartialBlockData]())
  }

  logInfo(log"StreamingShuffleReader initialized for shuffle ${MDC(SHUFFLE_ID, shuffleId)}, " +
    log"partitions ${MDC(PARTITION_ID, s"$startPartition-$endPartition")}, " +
    log"maps ${MDC(MAP_ID, s"$startMapIndex-$endMapIndex")}")

  // ============================================================================
  // ShuffleReader Implementation
  // ============================================================================

  /**
   * Reads the combined key-values for this reduce task by streaming from producers.
   *
   * This method implements the main shuffle read operation for streaming shuffle:
   * 1. Polls producers for available data before shuffle completion
   * 2. Validates received blocks via CRC32C checksum
   * 3. Deserializes and combines records
   * 4. Falls back to completed blocks on producer failure
   *
   * Thread Safety: This method is called from a single task thread, but internal
   * state may be accessed by backpressure protocol callbacks.
   *
   * @return Iterator over key-value pairs from the shuffle
   * @throws FetchFailedException if producer failure is detected and unrecoverable
   */
  override def read(): Iterator[Product2[K, C]] = {
    if (isDebug) {
      logDebug(s"Starting streaming shuffle read for consumer $consumerId")
    }

    // Check if partial reads have already been invalidated
    if (partialReadsInvalidated.get()) {
      throw createFetchFailedException(
        "Partial reads already invalidated before read started")
    }

    // Create iterator that streams from producers
    val streamingIterator = new StreamingRecordIterator()

    // Wrap with completion handler for cleanup
    val completionIterator = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      streamingIterator,
      cleanup()
    )

    // Wrap with interruptible iterator for task cancellation support
    new InterruptibleIterator[Product2[K, C]](context, completionIterator)
  }

  // ============================================================================
  // Producer Polling
  // ============================================================================

  /**
   * Polls producers for available data before shuffle completion.
   *
   * This method actively requests data from producer executors that is available
   * in their streaming buffers. It enables pipelined execution where consumers
   * can begin processing before producers finish.
   *
   * Poll Strategy:
   * 1. Query StreamingShuffleBlockResolver for available in-flight blocks
   * 2. Fetch blocks that are in InFlight or Spilled state
   * 3. Validate checksums and add to partial reads
   * 4. Send acknowledgments via BackpressureProtocol
   *
   * @return Iterator over key-value pairs from available data
   */
  def pollProducerForAvailableData(): Iterator[(K, C)] = {
    if (partialReadsInvalidated.get()) {
      logWarning(s"Attempted to poll after partial reads invalidated for $consumerId")
      return Iterator.empty
    }

    val availableRecords = new ArrayBuffer[(K, C)]()

    // Iterate through map tasks to poll for available data
    for (mapId <- startMapIndex until endMapIndex) {
      if (!failedProducers.containsKey(mapId.toLong)) {
        try {
          // Check for producer failure before polling
          val producerFailed = detectProducerFailureForMap(mapId.toLong)
          if (producerFailed) {
            logWarning(log"Producer failure detected for map ${MDC(MAP_ID, mapId)} " +
              log"during polling for shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
          }

          // Only poll if producer hasn't failed
          if (!producerFailed) {
            // Poll for available blocks for each partition
            for (partitionId <- startPartition until endPartition) {
              val blockId = ShuffleBlockId(shuffleId, mapId.toLong, partitionId)

              // Check block state in resolver
              val blockState = blockResolver.getBlockState(blockId)

              blockState match {
                case Some(InFlight) | Some(Spilled) =>
                  // Block is available - fetch and process
                  try {
                    val managedBuffer = blockResolver.getBlockData(blockId)
                    val buffer = managedBuffer.nioByteBuffer()

                    // Get expected checksum from block info
                    val blockInfo = blockResolver.getBlockInfo(blockId)
                    val expectedChecksum = blockInfo.map(_.checksum).getOrElse(0L)

                    // Validate checksum
                    if (validateChecksum(buffer, expectedChecksum)) {
                      // Record partial read
                      recordPartialRead(mapId.toLong, blockId, buffer, expectedChecksum)

                      // Send acknowledgment via backpressure protocol
                      backpressureProtocol.processAck(consumerId, blockId)

                      // Send heartbeat to indicate consumer is alive and processing
                      backpressureProtocol.sendHeartbeat(consumerId)

                      // Update metrics
                      val bytesRead = buffer.remaining()
                      metrics.incBytesStreamed(bytesRead)
                      readMetrics.incRemoteBytesRead(bytesRead)

                      // Update producer heartbeat
                      producerLastHeartbeat.put(mapId.toLong, System.currentTimeMillis())

                      if (isDebug) {
                        logDebug(log"Successfully polled block ${MDC(BLOCK_ID, blockId)}")
                      }
                    } else {
                      logWarning(log"Checksum validation failed: " +
                        log"block ${MDC(BLOCK_ID, blockId)}")
                      // Request retransmission by not acknowledging
                    }
                  } catch {
                    case e: IOException =>
                      logWarning(log"Failed to fetch block ${MDC(BLOCK_ID, blockId)}: " +
                        log"${MDC(ERROR, e.getMessage)}")
                      // Will retry on next poll
                  }

                case Some(Completed) =>
                  if (isDebug) {
                    logDebug(log"Block ${MDC(BLOCK_ID, blockId)} already completed")
                  }

                case None =>
                  // Block not yet available
                  if (isDebug) {
                    logDebug(log"Block ${MDC(BLOCK_ID, blockId)} not yet available")
                  }
              }
            }
          }
        } catch {
          case e: Exception =>
            logWarning(s"Error polling producer $mapId: ${e.getMessage}")
        }
      }
    }

    availableRecords.iterator
  }

  // ============================================================================
  // Producer Failure Detection
  // ============================================================================

  /**
   * Detects if any producer has failed based on heartbeat timeout.
   *
   * A producer is considered failed if no heartbeat (data or acknowledgment)
   * has been received within the configured heartbeatTimeoutMs interval
   * (default 5 seconds).
   *
   * @return true if any producer failure is detected, false otherwise
   */
  def detectProducerFailure(): Boolean = {
    val currentTime = System.currentTimeMillis()
    var failureDetected = false

    for (mapId <- startMapIndex until endMapIndex) {
      if (detectProducerFailureForMap(mapId.toLong)) {
        failureDetected = true
      }
    }

    if (failureDetected && isDebug) {
      logDebug(s"Producer failure detected for consumer $consumerId")
    }

    failureDetected
  }

  /**
   * Detects if a specific producer has failed based on heartbeat timeout.
   *
   * @param mapId The map task ID to check
   * @return true if producer failure is detected, false otherwise
   */
  private def detectProducerFailureForMap(mapId: Long): Boolean = {
    // Skip if already marked as failed
    if (failedProducers.containsKey(mapId)) {
      return true
    }

    val currentTime = System.currentTimeMillis()
    val lastHeartbeat = producerLastHeartbeat.getOrDefault(mapId, currentTime)
    val elapsed = currentTime - lastHeartbeat

    if (elapsed > heartbeatTimeoutMs) {
      // Mark producer as failed
      failedProducers.put(mapId, java.lang.Boolean.TRUE)

      logWarning(log"Producer failure detected for map ${MDC(MAP_ID, mapId)} " +
        log"in shuffle ${MDC(SHUFFLE_ID, shuffleId)}. " +
        log"Last heartbeat: ${MDC(ELAPSED_TIME, elapsed)}ms ago, " +
        log"timeout: ${MDC(TIMEOUT, heartbeatTimeoutMs)}ms")

      // Check if we should use backpressure protocol for heartbeat check
      if (backpressureProtocol.checkHeartbeatTimeout(consumerId)) {
        logWarning(s"Backpressure protocol also detected timeout for $consumerId")
      }

      true
    } else {
      false
    }
  }

  // ============================================================================
  // Checksum Validation
  // ============================================================================

  /**
   * Validates block data integrity using CRC32C checksum.
   *
   * This method uses Java 9+ CRC32C implementation with hardware-accelerated
   * intrinsics for high-throughput validation. If the checksum doesn't match,
   * the block data is considered corrupted and retransmission should be requested.
   *
   * @param block ByteBuffer containing the block data to validate
   * @param expected Expected CRC32C checksum value
   * @return true if checksum matches, false if data is corrupted
   */
  def validateChecksum(block: ByteBuffer, expected: Long): Boolean = {
    if (expected == 0L) {
      // No checksum available - accept data but log warning
      logWarning("Block received without checksum - accepting data without validation")
      return true
    }

    try {
      val crc = new CRC32C()

      // Create a duplicate to avoid modifying position
      val duplicate = block.duplicate()

      if (duplicate.hasArray) {
        // Use array-based update for efficiency
        crc.update(duplicate.array(), duplicate.arrayOffset() + duplicate.position(),
                   duplicate.remaining())
      } else {
        // Direct buffer - need to copy to array
        val bytes = new Array[Byte](duplicate.remaining())
        duplicate.get(bytes)
        crc.update(bytes, 0, bytes.length)
      }

      val computed = crc.getValue
      val matches = computed == expected

      if (!matches) {
        logWarning(s"Checksum mismatch: expected=$expected, computed=$computed")
      } else if (isDebug) {
        logDebug(s"Checksum validated successfully: $computed")
      }

      matches
    } catch {
      case e: Exception =>
        logError(s"Checksum validation error: ${e.getMessage}", e)
        false
    }
  }

  // ============================================================================
  // Partial Read Invalidation
  // ============================================================================

  /**
   * Atomically discards all blocks from failed producers.
   *
   * This method performs atomic invalidation of all partial reads when producer
   * failure is detected. After invalidation:
   * 1. All buffered data from failed producers is discarded
   * 2. Metrics are updated to reflect invalidation
   * 3. FetchFailedException is prepared for DAG scheduler notification
   *
   * Thread Safety: Uses AtomicBoolean for compareAndSet to ensure exactly-once
   * invalidation even under concurrent access.
   */
  def invalidatePartialReads(): Unit = {
    // Ensure atomic invalidation - only one thread should do this
    if (!partialReadsInvalidated.compareAndSet(false, true)) {
      logWarning(s"Partial reads already invalidated for $consumerId")
      return
    }

    logWarning(log"Invalidating all partial reads for consumer ${MDC(CONSUMER, consumerId)} " +
      log"in shuffle ${MDC(SHUFFLE_ID, shuffleId)}")

    // Count invalidated blocks for metrics
    var invalidatedBlockCount = 0
    var invalidatedBytes = 0L

    // Iterate through all partial reads and discard
    import scala.jdk.CollectionConverters._
    partialReads.asScala.foreach { case (mapId, blocks) =>
      blocks.synchronized {
        invalidatedBlockCount += blocks.size
        blocks.foreach { block =>
          invalidatedBytes += block.size
        }
        blocks.clear()
      }
    }

    // Update metrics
    metrics.incPartialReadInvalidations()

    logInfo(log"Invalidated ${MDC(NUM_BLOCKS, invalidatedBlockCount)} blocks " +
      log"(${MDC(BYTE_SIZE, invalidatedBytes)} bytes) for consumer ${MDC(CONSUMER, consumerId)}")

    // Unregister from backpressure protocol
    backpressureProtocol.unregisterConsumer(consumerId)
  }

  // ============================================================================
  // Retry Logic
  // ============================================================================

  /**
   * Retries an operation with exponential backoff.
   *
   * Implements exponential backoff starting at retryInitialDelayMs (1 second)
   * with maximum maxRetryAttempts (5) attempts.
   *
   * Retry sequence: 1s, 2s, 4s, 8s, 16s
   *
   * @param operation Description of the operation for logging
   * @param action The action to retry
   * @tparam T Return type of the action
   * @return Result of successful action
   * @throws Exception if all retries are exhausted
   */
  private def withExponentialBackoff[T](operation: String)(action: => T): T = {
    var attempt = 0
    var lastException: Exception = null
    var delayMs = retryInitialDelayMs

    while (attempt < maxRetryAttempts) {
      try {
        return action
      } catch {
        case e: Exception =>
          lastException = e
          attempt += 1

          if (attempt < maxRetryAttempts) {
            logWarning(s"$operation failed (attempt $attempt/$maxRetryAttempts), " +
              s"retrying in ${delayMs}ms: ${e.getMessage}")

            try {
              Thread.sleep(delayMs)
            } catch {
              case _: InterruptedException =>
                Thread.currentThread().interrupt()
                throw new IOException(s"$operation interrupted during retry", e)
            }

            // Exponential backoff
            delayMs = delayMs * 2
          }
      }
    }

    logError(s"$operation failed after $maxRetryAttempts attempts")
    throw new IOException(s"$operation failed after $maxRetryAttempts attempts", lastException)
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Records a partial read from a producer.
   */
  private def recordPartialRead(
      mapId: Long,
      blockId: BlockId,
      buffer: ByteBuffer,
      checksum: Long): Unit = {

    val blocks = partialReads.get(mapId)
    if (blocks != null) {
      blocks.synchronized {
        // Create a copy of the buffer data for storage
        val data = new Array[Byte](buffer.remaining())
        buffer.duplicate().get(data)

        blocks += PartialBlockData(
          blockId = blockId,
          data = data,
          checksum = checksum,
          timestamp = System.currentTimeMillis()
        )
      }
    }
  }

  /**
   * Creates a FetchFailedException for DAG scheduler notification.
   */
  private def createFetchFailedException(message: String): FetchFailedException = {
    // Find a failed producer to report
    val failedMapId = if (failedProducers.isEmpty) {
      startMapIndex.toLong
    } else {
      import scala.jdk.CollectionConverters._
      failedProducers.keys().asScala.nextOption().getOrElse(startMapIndex.toLong)
    }

    new FetchFailedException(
      blockManager.blockManagerId,
      shuffleId,
      failedMapId,
      failedMapId.toInt,
      startPartition,
      message
    )
  }

  /**
   * Cleanup resources after read completion or failure.
   */
  private def cleanup(): Unit = {
    logInfo(log"Cleaning up StreamingShuffleReader for consumer ${MDC(CONSUMER, consumerId)}")

    // Unregister from backpressure protocol
    try {
      backpressureProtocol.unregisterConsumer(consumerId)
    } catch {
      case e: Exception =>
        logWarning(s"Error unregistering from backpressure protocol: ${e.getMessage}")
    }

    // Clear partial reads
    partialReads.clear()
    failedProducers.clear()
    producerLastHeartbeat.clear()

    // Merge shuffle read metrics
    context.taskMetrics().mergeShuffleReadMetrics()
  }

  // ============================================================================
  // Streaming Record Iterator
  // ============================================================================

  /**
   * Internal iterator that streams records from producers with failure handling.
   *
   * This iterator implements the core streaming logic:
   * 1. Polls producers for available data
   * 2. Deserializes and yields records
   * 3. Handles producer failures with invalidation
   * 4. Falls back to completed blocks when streaming is exhausted
   */
  private class StreamingRecordIterator extends Iterator[Product2[K, C]] {

    /**
     * Buffer holding deserialized records ready for consumption.
     */
    private val recordBuffer = new mutable.Queue[Product2[K, C]]()

    /**
     * Set of map tasks that have been fully processed.
     */
    private val completedMaps = new mutable.HashSet[Long]()

    /**
     * Flag indicating whether initial polling has been done.
     */
    private var pollingStarted = false

    /**
     * Counter for records read.
     */
    private var recordsRead = 0L

    /**
     * Checks if more records are available.
     */
    override def hasNext: Boolean = {
      // Check task interruption
      context.killTaskIfInterrupted()

      // Check if invalidated
      if (partialReadsInvalidated.get()) {
        throw createFetchFailedException("Partial reads invalidated during iteration")
      }

      // If buffer has records, return true
      if (recordBuffer.nonEmpty) {
        return true
      }

      // Try to fetch more records
      fetchMoreRecords()

      recordBuffer.nonEmpty
    }

    /**
     * Returns the next record.
     */
    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException("No more records available")
      }

      val record = recordBuffer.dequeue()
      recordsRead += 1
      readMetrics.incRecordsRead(1)
      record
    }

    /**
     * Attempts to fetch more records from producers or completed blocks.
     */
    private def fetchMoreRecords(): Unit = {
      // Start polling if not started
      if (!pollingStarted) {
        pollingStarted = true
        if (isDebug) {
          logDebug(s"Starting record polling for $consumerId")
        }
      }

      // Check for producer failures
      if (detectProducerFailure()) {
        // Handle failed producers - check if we can recover
        val allFailed = (startMapIndex until endMapIndex).forall { mapId =>
          failedProducers.containsKey(mapId.toLong)
        }

        if (allFailed) {
          // All producers failed - trigger recomputation
          invalidatePartialReads()
          throw createFetchFailedException(
            "All producers failed - triggering upstream recomputation")
        }
      }

      // Process partial reads that have accumulated
      processPartialReads()

      // Poll for new data
      withExponentialBackoff("Poll producers") {
        pollProducerForAvailableData()
      }

      // Process any newly polled data
      processPartialReads()

      // If still no records and all maps are complete or failed, we're done
      val allMapsProcessed = (startMapIndex until endMapIndex).forall { mapId =>
        completedMaps.contains(mapId.toLong) || failedProducers.containsKey(mapId.toLong)
      }

      if (allMapsProcessed && recordBuffer.isEmpty) {
        // Try one more time to process any remaining partial reads
        processPartialReads()
      }
    }

    /**
     * Processes accumulated partial reads and deserializes into records.
     */
    private def processPartialReads(): Unit = {
      import scala.jdk.CollectionConverters._

      partialReads.asScala.foreach { case (mapId, blocks) =>
        if (!failedProducers.containsKey(mapId)) {
          blocks.synchronized {
            val processedIndices = new ArrayBuffer[Int]()

            blocks.zipWithIndex.foreach { case (blockData, index) =>
              try {
                // Deserialize records from block data
                val records = deserializeBlock(blockData.data)
                records.foreach { record =>
                  recordBuffer.enqueue(record.asInstanceOf[Product2[K, C]])
                }
                processedIndices += index

                // Check if this is a completed block
                val blockState = blockResolver.getBlockState(blockData.blockId)
                if (blockState.contains(Completed)) {
                  if (isDebug) {
                    logDebug(log"Block ${MDC(BLOCK_ID, blockData.blockId)} completed")
                  }
                }
              } catch {
                case e: Exception =>
                  logWarning(s"Failed to deserialize block ${blockData.blockId}: ${e.getMessage}")
              }
            }

            // Remove processed blocks (in reverse order to maintain indices)
            processedIndices.reverse.foreach(blocks.remove)
          }
        }
      }
    }

    /**
     * Deserializes a block of bytes into key-value records.
     */
    private def deserializeBlock(data: Array[Byte]): Iterator[(Any, Any)] = {
      if (data.isEmpty) {
        return Iterator.empty
      }

      try {
        // Get serializer instance from the handle's dependency
        val dep = handle.asInstanceOf[BaseShuffleHandle[K, _, C]].dependency
        val serializerInstance = dep.serializer.newInstance()

        // Create input stream from data
        val inputStream = new java.io.ByteArrayInputStream(data)

        // Wrap with serializer manager for potential decompression
        val wrappedStream = serializerManager.wrapStream(
          ShuffleBlockId(shuffleId, 0, 0), // BlockId for compression detection
          inputStream
        )

        // Deserialize records
        serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
      } catch {
        case e: Exception =>
          logWarning(s"Deserialization error: ${e.getMessage}")
          Iterator.empty
      }
    }
  }
}

// ============================================================================
// Supporting Data Classes
// ============================================================================

/**
 * Container for partial block data received from a producer.
 *
 * @param blockId The block identifier
 * @param data The raw byte data
 * @param checksum CRC32C checksum for validation
 * @param timestamp Timestamp when data was received
 */
private[streaming] case class PartialBlockData(
    blockId: BlockId,
    data: Array[Byte],
    checksum: Long,
    timestamp: Long) {

  /**
   * Returns the size of the data in bytes.
   */
  def size: Long = data.length
}

/**
 * Companion object for StreamingShuffleReader providing factory methods.
 */
private[spark] object StreamingShuffleReader {

  /**
   * Creates a new StreamingShuffleReader with default dependencies from SparkEnv.
   *
   * This factory method is useful for production use where dependencies
   * are obtained from the active SparkEnv.
   *
   * @param handle The shuffle handle
   * @param mapRange Tuple of (startMapIndex, endMapIndex)
   * @param partitionRange Tuple of (startPartition, endPartition)
   * @param context Task context
   * @param readMetrics Metrics reporter
   * @param config Streaming shuffle configuration
   * @param blockResolver Block resolver for streaming shuffle
   * @param backpressureProtocol Backpressure protocol handler
   * @param metrics Streaming shuffle metrics
   * @return A new StreamingShuffleReader instance
   */
  def apply[K, C](
      handle: ShuffleHandle,
      mapRange: (Int, Int),
      partitionRange: (Int, Int),
      context: TaskContext,
      readMetrics: ShuffleReadMetricsReporter,
      config: StreamingShuffleConfig,
      blockResolver: StreamingShuffleBlockResolver,
      backpressureProtocol: BackpressureProtocol,
      metrics: StreamingShuffleMetrics): StreamingShuffleReader[K, C] = {

    new StreamingShuffleReader[K, C](
      handle,
      mapRange._1,
      mapRange._2,
      partitionRange._1,
      partitionRange._2,
      context,
      readMetrics,
      config,
      blockResolver,
      backpressureProtocol,
      metrics
    )
  }
}
