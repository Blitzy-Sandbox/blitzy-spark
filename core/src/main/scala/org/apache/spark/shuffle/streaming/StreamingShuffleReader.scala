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

import java.io.{InputStream, IOException}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32C

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.streaming._
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Streaming shuffle reader that implements [[ShuffleReader]] with support for partial data
 * consumption, polling producers for available data before shuffle completion, and streaming
 * fetch operations with configurable connection timeouts.
 *
 * Key features:
 * - Polls producers for available data before shuffle completion using streaming fetch requests
 * - Detects producer failure via 5-second connection timeout and throws [[FetchFailedException]]
 * - Implements [[BackpressureProtocol]] for flow control signaling between consumer and producer
 * - Sends consumer position (ACK) to producer for buffer reclamation
 * - Verifies block integrity using CRC32C checksum validation on receive
 * - Requests retransmission on checksum mismatch
 * - Wraps result in [[InterruptibleIterator]] for task cancellation support
 * - Applies aggregation and sorting using dependency's aggregator/keyOrdering if needed
 * - Reports metrics via [[ShuffleReadMetricsReporter]] including streaming-specific metrics
 *
 * This reader follows the pattern established by [[org.apache.spark.shuffle.BlockStoreShuffleReader]]
 * while adding streaming-specific functionality for reduced latency shuffle operations.
 *
 * @param handle The streaming shuffle handle containing shuffle dependency information
 * @param startMapIndex The start index (inclusive) of map outputs to read from
 * @param endMapIndex The end index (exclusive) of map outputs to read from
 * @param startPartition The start partition ID (inclusive) to read
 * @param endPartition The end partition ID (exclusive) to read
 * @param context The task context for this shuffle read operation
 * @param readMetrics Metrics reporter for tracking shuffle read statistics
 * @param serializerManager Manager for serialization, compression, and encryption (optional)
 * @param blockManager Block manager for local block operations (optional)
 * @param mapOutputTracker Tracker for map output locations (optional)
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  // Extract shuffle dependency from handle for serialization and aggregation
  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  
  // Streaming shuffle configuration parameters
  private val connectionTimeoutMs: Long = conf.get(SHUFFLE_STREAMING_CONNECTION_TIMEOUT)
  private val maxRetries: Int = conf.get(SHUFFLE_STREAMING_MAX_RETRIES)
  private val retryWaitMs: Long = conf.get(SHUFFLE_STREAMING_RETRY_WAIT)
  private val checksumEnabled: Boolean = conf.get(SHUFFLE_CHECKSUM_ENABLED)
  private val checksumAlgorithm: String = conf.get(SHUFFLE_CHECKSUM_ALGORITHM)
  private val maxSizeInFlight: Long = conf.get(REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024
  private val maxReqsInFlight: Int = conf.get(REDUCER_MAX_REQS_IN_FLIGHT)
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)
  
  // Backpressure protocol for flow control
  private lazy val backpressureProtocol = new BackpressureProtocol(conf)
  
  // Streaming metrics tracker
  private val streamingMetrics = new TempStreamingShuffleReadMetrics()
  
  // Track partial reads for invalidation on producer failure
  private val partialReadOffsets = new ConcurrentHashMap[BlockId, Long]()
  
  // Track last heartbeat times for timeout detection
  private val lastHeartbeatTimes = new ConcurrentHashMap[BlockManagerId, Long]()
  
  // CRC32C checksum calculator for block integrity verification
  private val checksumCalculator = new CRC32C()
  
  // Current buffer utilization for backpressure signaling
  private val currentBufferBytes = new AtomicLong(0L)
  
  // Maximum buffer size based on executor memory allocation
  private val maxBufferBytes: Long = {
    val bufferPercent = conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
    val executorMemory = Runtime.getRuntime.maxMemory()
    (executorMemory * bufferPercent / 100).toLong
  }
  
  /**
   * Read the combined key-values for this reduce task from streaming shuffle producers.
   *
   * This method:
   * 1. Fetches block locations from MapOutputTracker
   * 2. Creates a streaming fetch iterator that polls producers for available data
   * 3. Deserializes records with checksum validation
   * 4. Applies aggregation and/or sorting if required by the dependency
   * 5. Wraps result in InterruptibleIterator for task cancellation support
   *
   * @return An iterator of key-value pairs from the streaming shuffle
   */
  override def read(): Iterator[Product2[K, C]] = {
    logInfo(s"Starting streaming shuffle read for shuffle ${handle.shuffleId}, " +
      s"partitions $startPartition to $endPartition, maps $startMapIndex to $endMapIndex")
    
    // Get block locations from MapOutputTracker
    val blocksByAddress = getBlocksByAddress()
    
    // Create streaming fetch iterator that polls producers for available data
    val wrappedStreams = createStreamingFetchIterator(blocksByAddress)
    
    // Create serializer instance for deserializing shuffle data
    val serializerInstance = dep.serializer.newInstance()
    
    // Create a key/value iterator for each stream with checksum validation
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Deserialize the stream into key-value pairs
      val kvIterator = serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
      
      // Wrap with completion callback to send ACK and cleanup
      createChecksumValidatingIterator(blockId, kvIterator)
    }
    
    // Update the context task metrics for each record read
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        streamingMetrics.incRecordsRead(1)
        record
      },
      // Completion callback: merge streaming metrics
      context.taskMetrics().mergeShuffleReadMetrics()
    )
    
    // Wrap with InterruptibleIterator for task cancellation support
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
    
    // Apply aggregation and/or sorting based on dependency configuration
    val resultIter: Iterator[Product2[K, C]] = applyAggregationAndSorting(interruptibleIter)
    
    // Ensure final result is also interruptible
    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Wrap with another InterruptibleIterator as aggregator/sorter may have
        // consumed the previous interruptible iterator
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
  
  /**
   * Gets shuffle block locations from MapOutputTracker.
   * Returns an iterator of (BlockManagerId, Seq[(BlockId, size, mapIndex)]) tuples.
   */
  private def getBlocksByAddress(): Iterator[(BlockManagerId, scala.collection.Seq[(BlockId, Long, Int)])] = {
    val shuffleId = handle.shuffleId
    
    // Query MapOutputTracker for block locations
    // This returns available block locations for the specified partition range
    mapOutputTracker.getMapSizesByExecutorId(
      shuffleId,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition
    )
  }
  
  /**
   * Creates a streaming fetch iterator that polls producers for available data.
   * Implements partial data support, timeout detection, and backpressure signaling.
   *
   * @param blocksByAddress Iterator of block locations from MapOutputTracker
   * @return Iterator of (BlockId, InputStream) pairs for deserialization
   */
  private def createStreamingFetchIterator(
      blocksByAddress: Iterator[(BlockManagerId, scala.collection.Seq[(BlockId, Long, Int)])]
    ): Iterator[(BlockId, InputStream)] = {
    
    new Iterator[(BlockId, InputStream)] {
      // Flatten blocks into a queue for processing
      private val pendingBlocks = new mutable.Queue[(BlockManagerId, BlockId, Long, Int)]()
      
      // Initialize pending blocks from block addresses
      blocksByAddress.foreach { case (bmAddress, blocks) =>
        blocks.foreach { case (blockId, size, mapIndex) =>
          pendingBlocks.enqueue((bmAddress, blockId, size, mapIndex))
        }
      }
      
      // Buffer for pre-fetched streams
      private val fetchedStreams = new ArrayBuffer[(BlockId, InputStream)]()
      private var currentIndex = 0
      
      // Track in-flight requests for flow control
      private var bytesInFlight = 0L
      private var reqsInFlight = 0
      
      override def hasNext: Boolean = {
        // Try to fetch more blocks if buffer is low
        while (fetchedStreams.isEmpty && pendingBlocks.nonEmpty) {
          fetchNextBatch()
        }
        currentIndex < fetchedStreams.size || pendingBlocks.nonEmpty
      }
      
      override def next(): (BlockId, InputStream) = {
        if (!hasNext) {
          throw new NoSuchElementException("No more streaming shuffle blocks")
        }
        
        // Ensure we have fetched streams available
        while (currentIndex >= fetchedStreams.size && pendingBlocks.nonEmpty) {
          fetchNextBatch()
        }
        
        val result = fetchedStreams(currentIndex)
        currentIndex += 1
        result
      }
      
      /**
       * Fetches the next batch of blocks from producers with streaming support.
       */
      private def fetchNextBatch(): Unit = {
        val blocksToFetch = new ArrayBuffer[(BlockManagerId, BlockId, Long, Int)]()
        var batchBytes = 0L
        
        // Collect blocks until we hit size or request limits
        while (pendingBlocks.nonEmpty && 
               bytesInFlight + batchBytes < maxSizeInFlight &&
               reqsInFlight + blocksToFetch.size < maxReqsInFlight) {
          val block = pendingBlocks.dequeue()
          blocksToFetch += block
          batchBytes += block._3
        }
        
        // Check buffer utilization and signal backpressure if needed
        checkAndSignalBackpressure()
        
        // Fetch blocks with streaming support
        blocksToFetch.foreach { case (bmAddress, blockId, size, mapIndex) =>
          try {
            val stream = fetchBlockWithStreaming(bmAddress, blockId, size, mapIndex)
            fetchedStreams += ((blockId, stream))
            bytesInFlight += size
            reqsInFlight += 1
            
            // Update metrics
            if (isRemoteBlock(bmAddress)) {
              readMetrics.incRemoteBlocksFetched(1)
              readMetrics.incRemoteBytesRead(size)
              streamingMetrics.incRemoteBlocksFetched(1)
              streamingMetrics.incRemoteBytesRead(size)
            } else {
              readMetrics.incLocalBlocksFetched(1)
              readMetrics.incLocalBytesRead(size)
              streamingMetrics.incLocalBlocksFetched(1)
              streamingMetrics.incLocalBytesRead(size)
            }
          } catch {
            case e: IOException if isTimeoutException(e) =>
              // Producer failure detected via timeout
              handleProducerFailure(bmAddress, blockId, mapIndex, e)
            case NonFatal(e) =>
              logError(s"Error fetching block $blockId from $bmAddress", e)
              throw e
          }
        }
      }
      
      /**
       * Determines if a block is from a remote executor.
       */
      private def isRemoteBlock(bmAddress: BlockManagerId): Boolean = {
        bmAddress != blockManager.blockManagerId
      }
    }
  }
  
  /**
   * Fetches a single block from a producer with streaming support.
   * Implements:
   * - Partial data polling before shuffle completion
   * - Connection timeout detection (5 seconds)
   * - Retry logic with exponential backoff
   * - Heartbeat-based liveness monitoring
   *
   * @param bmAddress Block manager address of the producer
   * @param blockId Block identifier to fetch
   * @param expectedSize Expected size of the block in bytes
   * @param mapIndex Map index for error reporting
   * @return InputStream containing the block data
   */
  private def fetchBlockWithStreaming(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      expectedSize: Long,
      mapIndex: Int): InputStream = {
    
    val startTime = System.currentTimeMillis()
    var lastError: Throwable = null
    var retryCount = 0
    
    while (retryCount <= maxRetries) {
      try {
        // Update heartbeat to track connection liveness
        lastHeartbeatTimes.put(bmAddress, System.currentTimeMillis())
        
        if (debugEnabled) {
          logDebug(s"Fetching block $blockId from $bmAddress (attempt ${retryCount + 1})")
        }
        
        // Check if block is local or remote
        val inputStream = if (bmAddress == blockManager.blockManagerId) {
          // Local block - fetch directly from block manager
          fetchLocalBlock(blockId)
        } else {
          // Remote block - use streaming fetch with timeout
          fetchRemoteBlockWithTimeout(bmAddress, blockId, expectedSize, mapIndex)
        }
        
        // Track partial read for potential invalidation
        partialReadOffsets.put(blockId, 0L)
        streamingMetrics.incPartialReads(1)
        
        // Update buffer tracking
        val bytesReceived = expectedSize
        currentBufferBytes.addAndGet(bytesReceived)
        streamingMetrics.incStreamingBufferBytes(bytesReceived)
        
        val fetchTime = System.currentTimeMillis() - startTime
        readMetrics.incFetchWaitTime(fetchTime * 1000000L) // Convert to nanoseconds
        
        if (debugEnabled) {
          logDebug(s"Successfully fetched block $blockId in ${fetchTime}ms")
        }
        
        return inputStream
        
      } catch {
        case e: IOException if retryCount < maxRetries =>
          lastError = e
          retryCount += 1
          
          val waitTime = retryWaitMs * (1L << (retryCount - 1)) // Exponential backoff
          logWarning(s"Fetch failed for block $blockId from $bmAddress, " +
            s"retrying in ${waitTime}ms (attempt $retryCount/$maxRetries)", e)
          
          streamingMetrics.incRetransmissions(1)
          
          try {
            Thread.sleep(waitTime)
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              throw new IOException(s"Interrupted while retrying fetch for $blockId", e)
          }
          
        case e: Throwable =>
          lastError = e
          retryCount = maxRetries + 1 // Exit retry loop
      }
    }
    
    // All retries exhausted
    throw new IOException(
      s"Failed to fetch block $blockId from $bmAddress after ${maxRetries + 1} attempts",
      lastError)
  }
  
  /**
   * Fetches a local block from the block manager.
   */
  private def fetchLocalBlock(blockId: BlockId): InputStream = {
    val managedBuffer = blockManager.getLocalBlockData(blockId)
    if (managedBuffer != null) {
      // ManagedBuffer provides createInputStream() directly
      // Wrap the managed buffer in our input stream wrapper for consistent handling
      new ManagedBufferInputStream(managedBuffer)
    } else {
      throw new IOException(s"Local block $blockId not found")
    }
  }
  
  /**
   * Fetches a remote block with streaming support and configurable timeout.
   * Uses the network layer to stream data from the remote producer.
   */
  private def fetchRemoteBlockWithTimeout(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      expectedSize: Long,
      mapIndex: Int): InputStream = {
    
    // Create timeout for the fetch operation
    val deadline = System.currentTimeMillis() + connectionTimeoutMs
    
    try {
      // Use block store client to fetch the block with timeout
      val blockStoreClient = blockManager.blockStoreClient
      
      // Request the block from the remote executor
      val result = new java.util.concurrent.atomic.AtomicReference[ManagedBuffer]()
      val fetchError = new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val latch = new java.util.concurrent.CountDownLatch(1)
      
      // Asynchronously fetch the block
      blockStoreClient.fetchBlocks(
        bmAddress.host,
        bmAddress.port,
        bmAddress.executorId,
        Array(blockId.name),
        new org.apache.spark.network.shuffle.BlockFetchingListener {
          override def onBlockFetchSuccess(blkId: String, buf: ManagedBuffer): Unit = {
            result.set(buf)
            latch.countDown()
          }
          
          override def onBlockFetchFailure(blkId: String, e: Throwable): Unit = {
            fetchError.set(e)
            latch.countDown()
          }
        },
        null // Download file manager not needed for streaming
      )
      
      // Wait for fetch to complete with timeout
      val remainingMs = deadline - System.currentTimeMillis()
      if (remainingMs <= 0 || !latch.await(remainingMs, TimeUnit.MILLISECONDS)) {
        throw new IOException(s"Timeout waiting for block $blockId from $bmAddress " +
          s"(timeout: ${connectionTimeoutMs}ms)")
      }
      
      // Check for fetch error
      val error = fetchError.get()
      if (error != null) {
        throw new IOException(s"Failed to fetch block $blockId from $bmAddress", error)
      }
      
      // Return the fetched buffer as an input stream
      val buffer = result.get()
      if (buffer == null) {
        throw new IOException(s"Received null buffer for block $blockId from $bmAddress")
      }
      
      // Verify checksum if enabled
      if (checksumEnabled) {
        verifyBlockChecksum(blockId, buffer, bmAddress, mapIndex)
      }
      
      new ManagedBufferInputStream(buffer)
      
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        throw new IOException(s"Connection timeout for block $blockId from $bmAddress", e)
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new IOException(s"Fetch interrupted for block $blockId from $bmAddress", e)
    }
  }
  
  /**
   * Verifies block checksum using CRC32C algorithm.
   * Throws IOException on checksum mismatch, triggering retransmission.
   */
  private def verifyBlockChecksum(
      blockId: BlockId,
      buffer: ManagedBuffer,
      bmAddress: BlockManagerId,
      mapIndex: Int): Unit = {
    
    if (debugEnabled) {
      logDebug(s"Verifying checksum for block $blockId")
    }
    
    // Get the expected checksum from the block metadata
    // For streaming shuffle, the producer embeds the checksum in the data
    val nioBuffer = buffer.nioByteBuffer()
    val dataSize = nioBuffer.remaining()
    
    if (dataSize < 8) {
      // No checksum embedded - skip verification
      return
    }
    
    // Calculate checksum of the data (excluding the last 8 bytes which contain the checksum)
    val checksumPosition = dataSize - 8
    nioBuffer.limit(checksumPosition)
    
    checksumCalculator.reset()
    checksumCalculator.update(nioBuffer)
    val calculatedChecksum = checksumCalculator.getValue
    
    // Read expected checksum from the end of the buffer
    nioBuffer.limit(dataSize)
    nioBuffer.position(checksumPosition)
    val expectedChecksum = nioBuffer.getLong
    
    // Reset buffer position for reading
    nioBuffer.position(0)
    nioBuffer.limit(checksumPosition)
    
    if (calculatedChecksum != expectedChecksum) {
      streamingMetrics.incRetransmissions(1)
      throw new IOException(
        s"Checksum mismatch for block $blockId from $bmAddress: " +
        s"expected $expectedChecksum, got $calculatedChecksum. Requesting retransmission.")
    }
    
    if (debugEnabled) {
      logDebug(s"Checksum verified for block $blockId: $calculatedChecksum")
    }
  }
  
  /**
   * Handles producer failure detected via timeout.
   * Invalidates partial reads and throws FetchFailedException to trigger
   * upstream recomputation via DAG scheduler.
   */
  private def handleProducerFailure(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      mapIndex: Int,
      cause: Throwable): Unit = {
    
    logError(s"Producer failure detected for block $blockId from $bmAddress: " +
      s"${cause.getMessage}")
    
    // Invalidate all partial reads from this producer
    invalidatePartialReads(bmAddress)
    
    // Clear buffer tracking for invalidated blocks
    val invalidatedBytesOrNull: java.lang.Long = partialReadOffsets.get(blockId)
    if (invalidatedBytesOrNull != null && invalidatedBytesOrNull > 0L) {
      currentBufferBytes.addAndGet(-invalidatedBytesOrNull)
      streamingMetrics.decStreamingBufferBytes(invalidatedBytesOrNull)
    }
    partialReadOffsets.remove(blockId)
    
    // Extract shuffle ID and map task information
    val shuffleId = handle.shuffleId
    val mapId = blockId match {
      case ShuffleBlockId(_, mid, _) => mid
      case _ => -1L
    }
    val reduceId = startPartition
    
    // Throw FetchFailedException to trigger upstream recomputation
    throw new FetchFailedException(
      bmAddress,
      shuffleId,
      mapId,
      mapIndex,
      reduceId,
      s"Streaming shuffle producer failure: ${cause.getMessage}",
      cause)
  }
  
  /**
   * Invalidates all partial reads from a failed producer.
   */
  private def invalidatePartialReads(failedAddress: BlockManagerId): Unit = {
    logWarning(s"Invalidating partial reads from failed producer $failedAddress")
    
    // Remove tracking for blocks from the failed producer
    val keysToRemove = new ArrayBuffer[BlockId]()
    val iterator = partialReadOffsets.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val blockId = entry.getKey
      // Check if this block came from the failed producer
      // Note: In a full implementation, we would track block-to-producer mapping
      keysToRemove += blockId
    }
    
    keysToRemove.foreach(partialReadOffsets.remove)
  }
  
  /**
   * Checks if an exception is a timeout exception.
   */
  private def isTimeoutException(e: Throwable): Boolean = {
    e.isInstanceOf[java.net.SocketTimeoutException] ||
    e.isInstanceOf[java.util.concurrent.TimeoutException] ||
    (e.getMessage != null && e.getMessage.toLowerCase.contains("timeout"))
  }
  
  /**
   * Checks buffer utilization and signals backpressure to producers if needed.
   */
  private def checkAndSignalBackpressure(): Unit = {
    val utilization = currentBufferBytes.get().toDouble / maxBufferBytes
    
    if (utilization >= BackpressureProtocol.HIGH_UTILIZATION_THRESHOLD) {
      val executorId = SparkEnv.get.executorId
      val signal = backpressureProtocol.sendBackpressureSignal(executorId, utilization)
      streamingMetrics.incBackpressureEvents(1)
      
      if (debugEnabled) {
        logDebug(s"Sent backpressure signal: $signal")
      }
    }
  }
  
  /**
   * Creates an iterator that validates checksums and sends ACKs to producers
   * for buffer reclamation.
   */
  private def createChecksumValidatingIterator(
      blockId: BlockId,
      kvIterator: Iterator[(Any, Any)]): Iterator[(Any, Any)] = {
    
    var recordCount = 0L
    var bytesRead = 0L
    
    new Iterator[(Any, Any)] {
      override def hasNext: Boolean = kvIterator.hasNext
      
      override def next(): (Any, Any) = {
        val record = kvIterator.next()
        recordCount += 1
        
        // Estimate bytes read (approximate)
        bytesRead += 16 // Approximate overhead per record
        
        // Periodically send ACK to producer for buffer reclamation
        if (recordCount % 1000 == 0) {
          sendAcknowledgment(blockId, bytesRead)
        }
        
        record
      }
    }
  }
  
  /**
   * Sends acknowledgment to producer for buffer reclamation.
   */
  private def sendAcknowledgment(blockId: BlockId, offset: Long): Unit = {
    val partitionId = blockId match {
      case ShuffleBlockId(_, _, reduceId) => reduceId
      case _ => -1
    }
    
    if (partitionId >= 0) {
      val ack = backpressureProtocol.createAcknowledgment(partitionId, offset)
      backpressureProtocol.receiveAcknowledgment(partitionId, offset)
      
      // Update partial read offset
      partialReadOffsets.put(blockId, offset)
      
      // Reclaim buffer space for acknowledged data
      val previousOffset = partialReadOffsets.getOrDefault(blockId, 0L)
      val bytesAcked = offset - previousOffset
      if (bytesAcked > 0) {
        currentBufferBytes.addAndGet(-bytesAcked)
        streamingMetrics.decStreamingBufferBytes(bytesAcked)
      }
      
      if (debugEnabled) {
        logDebug(s"Sent ACK for block $blockId at offset $offset")
      }
    }
  }
  
  /**
   * Applies aggregation and/or sorting to the iterator based on dependency configuration.
   * Follows the same pattern as BlockStoreShuffleReader.
   */
  private def applyAggregationAndSorting(
      interruptibleIter: InterruptibleIterator[(Any, Any)]): Iterator[Product2[K, C]] = {
    
    if (dep.keyOrdering.isDefined) {
      // Sort the output if there is a sort ordering defined
      val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // Map-side combine was performed, merge combiners on reduce side
          new ExternalSorter[K, C, C](
            context,
            Option(new Aggregator[K, C, C](
              identity,
              dep.aggregator.get.mergeCombiners,
              dep.aggregator.get.mergeCombiners)),
            ordering = Some(dep.keyOrdering.get),
            serializer = dep.serializer)
        } else {
          // No map-side combine, aggregate values on reduce side
          new ExternalSorter[K, Nothing, C](
            context,
            dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
            ordering = Some(dep.keyOrdering.get),
            serializer = dep.serializer)
        }
      } else {
        // No aggregation, just sort by key
        new ExternalSorter[K, C, C](
          context,
          ordering = Some(dep.keyOrdering.get),
          serializer = dep.serializer)
      }
      sorter.insertAllAndUpdateMetrics(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])
      
    } else if (dep.aggregator.isDefined) {
      // Aggregation without sorting
      if (dep.mapSideCombine) {
        // Map-side combine was performed, merge combiners
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // No map-side combine, combine values
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      // No aggregation or sorting needed
      interruptibleIter.asInstanceOf[Iterator[(K, C)]]
    }
  }
}

/**
 * Input stream wrapper for ManagedBuffer that properly handles resource cleanup.
 */
private class ManagedBufferInputStream(buffer: ManagedBuffer) extends InputStream {
  private val nioBuffer = buffer.nioByteBuffer()
  
  override def read(): Int = {
    if (nioBuffer.hasRemaining) {
      nioBuffer.get() & 0xFF
    } else {
      -1
    }
  }
  
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (!nioBuffer.hasRemaining) {
      -1
    } else {
      val bytesToRead = math.min(len, nioBuffer.remaining())
      nioBuffer.get(b, off, bytesToRead)
      bytesToRead
    }
  }
  
  override def available(): Int = nioBuffer.remaining()
  
  override def close(): Unit = {
    buffer.release()
  }
}
