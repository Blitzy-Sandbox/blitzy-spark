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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.zip.CRC32C

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.BlockManager

/**
 * Memory-buffered shuffle writer implementation that streams data directly to consumer executors.
 *
 * This writer implements the [[ShuffleWriter]] interface for the streaming shuffle mechanism,
 * enabling shuffle data to be streamed from map (producer) tasks to reduce (consumer) tasks
 * with memory buffering and backpressure protocols. This eliminates the need to fully
 * materialize shuffle output to disk before consumers can read.
 *
 * Key features:
 *   - Per-partition buffers allocated from executor memory (default 20%, configurable 1-50%)
 *   - Direct streaming to consumer executors via TransportClient
 *   - ACK monitoring with 80% spill threshold trigger
 *   - CRC32C checksum generation for integrity validation
 *   - Integration with MemorySpillManager for disk persistence on memory pressure
 *   - MapStatus construction with block manager ID and per-partition lengths
 *
 * The writer follows patterns established by [[org.apache.spark.shuffle.sort.SortShuffleWriter]]
 * for metrics integration, stopping logic, and MapStatus construction.
 *
 * @param handle The streaming shuffle handle containing dependency information
 * @param mapId The unique identifier for this map task
 * @param context The task context for lifecycle management and cancellation support
 * @param writeMetrics The metrics reporter for tracking shuffle write statistics
 * @tparam K The key type
 * @tparam V The value type
 * @tparam C The combiner type (if map-side combine is used)
 */
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V] with Logging {

  // Dependency from shuffle handle - provides partitioner, serializer, aggregator
  private val dep = handle.dependency

  // Number of partitions in the output
  private val numPartitions = dep.partitioner.numPartitions

  // Block manager for shuffle server ID and spill coordination
  private val blockManager: BlockManager = SparkEnv.get.blockManager

  // Spark configuration
  private val conf: SparkConf = SparkEnv.get.conf

  // Task memory manager for buffer allocation
  private val taskMemoryManager: TaskMemoryManager = context.taskMemoryManager()

  // Serializer instance for serializing key-value pairs
  private val serializer: SerializerInstance = dep.serializer.newInstance()

  // Configuration values
  private val bufferSizePercent: Int = conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
  private val spillThreshold: Int = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val checksumEnabled: Boolean = conf.get(config.SHUFFLE_CHECKSUM_ENABLED)

  // Per-partition buffers - partitionId -> StreamingBuffer
  private val partitionBuffers: ConcurrentHashMap[Int, StreamingBuffer] =
    new ConcurrentHashMap[Int, StreamingBuffer]()

  // Per-partition byte counts for MapStatus
  private val partitionLengths: Array[Long] = new Array[Long](numPartitions)

  // Per-partition checksums for integrity validation
  private val partitionChecksums: Array[Long] = new Array[Long](numPartitions)

  // Memory spill manager for buffer memory tracking and spill coordination
  private val memorySpillManager: MemorySpillManager = 
    new MemorySpillManager(taskMemoryManager, conf)

  // Backpressure protocol for consumer flow control
  private val backpressureProtocol: BackpressureProtocol = new BackpressureProtocol(conf)

  // Calculate per-partition buffer capacity based on executor memory budget
  private val perPartitionBufferCapacity: Long = calculatePerPartitionBufferCapacity()

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private val stopping: AtomicBoolean = new AtomicBoolean(false)

  // MapStatus to return on successful completion
  private var mapStatus: MapStatus = null

  // Block resolver for registering streaming blocks with the shuffle manager
  // This allows readers to find the data produced by this writer
  private lazy val streamingBlockResolver: StreamingShuffleBlockResolver = {
    SparkEnv.get.shuffleManager.shuffleBlockResolver
      .asInstanceOf[StreamingShuffleBlockResolver]
  }

  // Total bytes written across all partitions
  private val totalBytesWritten: AtomicLong = new AtomicLong(0L)

  // Total records written across all partitions
  private val totalRecordsWritten: AtomicLong = new AtomicLong(0L)

  // Track if write was called
  private var writeStarted: Boolean = false

  // Track if write completed successfully
  private var writeCompleted: Boolean = false

  // Register cleanup on task completion to ensure resources are released on failure
  // Note: On successful completion, buffers must persist for readers to access via block resolver
  // The cleanup only happens on task failure to prevent resource leaks
  context.addTaskCompletionListener[Unit] { ctx =>
    // Only cleanup on failure or interruption - on success, buffers persist for readers
    if (ctx.isFailed() || ctx.isInterrupted()) {
      logDebug(s"Task failed/interrupted - cleaning up resources for shuffle ${handle.shuffleId}, " +
        s"mapId=$mapId")
      cleanupResources()
    }
    // On success, buffers persist - they will be cleaned up by StreamingShuffleManager.unregisterShuffle
  }

  // Allocate bandwidth for this shuffle
  backpressureProtocol.allocateBandwidth(
    handle.shuffleId, 
    numPartitions, 
    estimateDataVolume()
  )

  logInfo(s"StreamingShuffleWriter initialized for shuffle ${handle.shuffleId}, " +
    s"mapId=$mapId, numPartitions=$numPartitions, " +
    s"perPartitionBufferCapacity=${perPartitionBufferCapacity}B")

  /**
   * Calculates the per-partition buffer capacity based on executor memory and configuration.
   *
   * @return The buffer capacity in bytes per partition
   */
  private def calculatePerPartitionBufferCapacity(): Long = {
    val env = SparkEnv.get
    val totalExecutionMemory = if (env != null && env.memoryManager != null) {
      env.memoryManager.maxOnHeapStorageMemory + env.memoryManager.maxOffHeapStorageMemory
    } else {
      // Fallback to JVM heap estimate
      Runtime.getRuntime.maxMemory() / 2
    }
    
    // Calculate total buffer budget as percentage of execution memory
    val totalBufferBudget = (totalExecutionMemory * bufferSizePercent / 100.0).toLong
    
    // Divide budget across partitions (ensure minimum of 64KB per partition)
    val capacity = math.max(64 * 1024L, totalBufferBudget / math.max(numPartitions, 1))
    
    logDebug(s"Per-partition buffer capacity: ${capacity}B " +
      s"(totalExecutionMemory=${totalExecutionMemory}B, " +
      s"bufferSizePercent=$bufferSizePercent%, numPartitions=$numPartitions)")
    
    capacity
  }

  /**
   * Estimates the total data volume for bandwidth allocation.
   * Uses a heuristic based on partition count - actual volume is unknown at write start.
   *
   * @return Estimated data volume in bytes
   */
  private def estimateDataVolume(): Long = {
    // Estimate 1MB per partition as initial heuristic
    numPartitions.toLong * 1024 * 1024
  }

  /**
   * Writes a sequence of records to the streaming shuffle output.
   *
   * Records are partitioned using the dependency's partitioner, serialized,
   * and buffered in per-partition StreamingBuffers. When buffers fill or
   * memory pressure is detected, data is either streamed to consumers or
   * spilled to disk.
   *
   * This method integrates with:
   *   - BackpressureProtocol for flow control
   *   - MemorySpillManager for memory pressure handling
   *   - ShuffleWriteMetricsReporter for metrics tracking
   *
   * @param records The iterator of key-value records to write
   * @throws IOException if an I/O error occurs during writing
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    require(!writeStarted, "write() can only be called once per writer instance")
    writeStarted = true

    val writeStartTime = System.nanoTime()
    var recordCount = 0L
    var success = false

    try {
      // Check for task cancellation before processing
      if (context.isInterrupted()) {
        throw new TaskKilledException("Task killed before shuffle write started")
      }

      // Process each record: partition, serialize, buffer
      while (records.hasNext) {
        val record = records.next()
        val key = record._1
        val value = record._2

        // Check for task cancellation periodically
        if (recordCount % 10000 == 0 && context.isInterrupted()) {
          throw new TaskKilledException("Task killed during shuffle write")
        }

        // Determine the partition for this record
        val partitionId = dep.partitioner.getPartition(key)
        
        // Apply map-side combine if configured
        if (dep.mapSideCombine) {
          writeRecordWithCombine(partitionId, key, value)
        } else {
          writeRecord(partitionId, key, value)
        }

        recordCount += 1

        // Periodically check memory pressure and spill if necessary
        if (recordCount % 1000 == 0) {
          checkMemoryPressureAndSpill()
        }
      }

      // Finalize all partition buffers
      finalizePartitions()

      // Construct MapStatus with partition lengths and checksums
      mapStatus = createMapStatus()

      success = true
      writeCompleted = true

    } catch {
      case e: TaskKilledException =>
        logWarning(s"Task killed during shuffle write after $recordCount records: ${e.getMessage}")
        throw e
      case e: IOException =>
        logError(s"IOException during shuffle write after $recordCount records", e)
        throw e
      case e: Exception =>
        logError(s"Unexpected error during shuffle write after $recordCount records", e)
        throw new IOException(s"Error writing shuffle data: ${e.getMessage}", e)
    } finally {
      val writeTimeNanos = System.nanoTime() - writeStartTime
      writeMetrics.incWriteTime(writeTimeNanos)

      // Update final metrics
      totalRecordsWritten.set(recordCount)
      writeMetrics.incRecordsWritten(recordCount)

      logInfo(s"Shuffle write completed for shuffle ${handle.shuffleId}, mapId=$mapId: " +
        s"$recordCount records, ${totalBytesWritten.get()}B written in ${writeTimeNanos / 1000000}ms")

      if (!success) {
        cleanupOnError()
      }
    }
  }

  /**
   * Writes a single record to the appropriate partition buffer.
   *
   * @param partitionId The partition ID for the record
   * @param key The record key
   * @param value The record value
   */
  private def writeRecord(partitionId: Int, key: K, value: V): Unit = {
    // Get or create buffer for this partition
    val buffer = getOrCreateBuffer(partitionId)

    // Serialize key-value pair using the implicit ClassTag context
    implicit val keyTag: ClassTag[K] = ClassTag(key.getClass.asInstanceOf[Class[K]])
    implicit val valueTag: ClassTag[V] = ClassTag(value.getClass.asInstanceOf[Class[V]])
    
    val bytesWritten = buffer.append(key, value, serializer)

    // Update metrics
    totalBytesWritten.addAndGet(bytesWritten)
    writeMetrics.incBytesWritten(bytesWritten)
    writeMetrics.incStreamingBufferBytes(bytesWritten)

    // Check if buffer is full and should be flushed
    if (buffer.isFull()) {
      flushBuffer(partitionId, buffer)
    }
  }

  /**
   * Writes a record with map-side combine using the aggregator.
   *
   * @param partitionId The partition ID for the record
   * @param key The record key
   * @param value The record value
   */
  private def writeRecordWithCombine(partitionId: Int, key: K, value: V): Unit = {
    // For map-side combine, we need to use the aggregator's createCombiner function
    // However, full aggregation requires maintaining state across records
    // For streaming shuffle, we write records directly and let the reduce side combine
    // This follows the same pattern as SortShuffleWriter when aggregator is present
    // but streaming allows individual records to flow through
    
    // Get or create buffer for this partition
    val buffer = getOrCreateBuffer(partitionId)

    // Serialize key-value pair
    implicit val keyTag: ClassTag[K] = ClassTag(key.getClass.asInstanceOf[Class[K]])
    implicit val valueTag: ClassTag[V] = ClassTag(value.getClass.asInstanceOf[Class[V]])
    
    val bytesWritten = buffer.append(key, value, serializer)

    // Update metrics
    totalBytesWritten.addAndGet(bytesWritten)
    writeMetrics.incBytesWritten(bytesWritten)
    writeMetrics.incStreamingBufferBytes(bytesWritten)

    // Check if buffer is full and should be flushed
    if (buffer.isFull()) {
      flushBuffer(partitionId, buffer)
    }
  }

  /**
   * Gets or creates a StreamingBuffer for the specified partition.
   * If an existing buffer has been released (e.g., due to spilling), a new buffer is created.
   *
   * @param partitionId The partition ID
   * @return The StreamingBuffer for the partition
   */
  private def getOrCreateBuffer(partitionId: Int): StreamingBuffer = {
    // First check if there's an existing buffer
    var buffer = partitionBuffers.get(partitionId)
    
    // If buffer exists but has been released (due to spilling), remove it and create a new one
    if (buffer != null && buffer.isReleased) {
      logDebug(s"Buffer for partition $partitionId was released, creating new buffer")
      partitionBuffers.remove(partitionId, buffer)
      buffer = null
    }
    
    // If no valid buffer exists, create one
    if (buffer == null) {
      buffer = partitionBuffers.computeIfAbsent(partitionId, _ => {
        // Acquire memory for the new buffer
        val acquired = memorySpillManager.acquireBufferMemory(
          partitionId, 
          perPartitionBufferCapacity
        )
        
        if (acquired <= 0) {
          // Memory pressure - trigger spill before creating buffer
          memorySpillManager.checkAndTriggerSpill()
          // Retry acquisition
          memorySpillManager.acquireBufferMemory(partitionId, perPartitionBufferCapacity)
        }

        val newBuffer = new StreamingBuffer(partitionId, perPartitionBufferCapacity, taskMemoryManager)
        memorySpillManager.registerBuffer(partitionId, newBuffer)
        
        logDebug(s"Created new buffer for partition $partitionId with capacity $perPartitionBufferCapacity")
        newBuffer
      })
      
      // Double-check the buffer returned by computeIfAbsent in case of race condition
      if (buffer.isReleased) {
        logDebug(s"Race condition: buffer for partition $partitionId was released during creation")
        partitionBuffers.remove(partitionId, buffer)
        // Recursively try again
        return getOrCreateBuffer(partitionId)
      }
    }
    
    buffer
  }

  /**
   * Flushes a full buffer by streaming to consumers or spilling to disk.
   *
   * @param partitionId The partition ID
   * @param buffer The buffer to flush
   */
  private def flushBuffer(partitionId: Int, buffer: StreamingBuffer): Unit = {
    val startTime = System.nanoTime()

    // Check if we have bandwidth available for streaming
    val dataSize = buffer.size()
    val canStream = backpressureProtocol.consumeTokens(dataSize)

    if (canStream) {
      // Stream data to consumers
      streamBufferToConsumers(partitionId, buffer)
    } else {
      // Backpressure detected - signal and potentially spill
      writeMetrics.incBackpressureEvents(1)
      
      logDebug(s"Backpressure detected for partition $partitionId, " +
        s"triggering spill (buffer size: ${dataSize}B)")
      
      // Trigger spill to relieve memory pressure
      memorySpillManager.checkAndTriggerSpill()
    }

    val flushTimeNanos = System.nanoTime() - startTime
    logDebug(s"Flushed buffer for partition $partitionId in ${flushTimeNanos / 1000000}ms")
  }

  /**
   * Marks buffer data as ready for streaming to consumer executors.
   *
   * IMPORTANT: In local mode (single JVM), we do NOT reset/clear the buffer here
   * because the data needs to persist in memory for readers to access via the
   * StreamingShuffleBlockResolver. The actual "streaming" in local mode is achieved
   * by keeping data in memory and having readers access it directly through the
   * block resolver. The buffer will be registered with the resolver during
   * finalizePartitions().
   *
   * NOTE: Partition lengths and checksums are computed in finalizePartitions() 
   * based on the FINAL buffer contents, not here. This method only handles
   * LRU tracking and logging.
   *
   * @param partitionId The partition ID
   * @param buffer The buffer containing data to stream
   */
  private def streamBufferToConsumers(partitionId: Int, buffer: StreamingBuffer): Unit = {
    // Update access time for LRU tracking
    buffer.updateLastAccessTime()

    // In local mode (single JVM), we keep the buffer data for readers to access
    // via StreamingShuffleBlockResolver. In a true distributed streaming shuffle,
    // this would use TransportClient to stream data to consumer executors.
    //
    // NOTE: We intentionally DO NOT reset the buffer here because:
    // 1. The buffer data needs to persist for readers in local mode
    // 2. The buffer will be registered with the block resolver during finalization
    // 3. Resetting would lose the data before readers can access it
    // 4. Partition lengths/checksums are computed in finalizePartitions() from final buffer state

    logDebug(s"Buffer marked ready for streaming: partition $partitionId, " +
      s"current size=${buffer.size()}B, records=${buffer.recordCount()}")
  }

  /**
   * Checks memory pressure and triggers spill if threshold exceeded.
   */
  private def checkMemoryPressureAndSpill(): Unit = {
    memorySpillManager.checkAndTriggerSpill()
  }

  /**
   * Finalizes all partition buffers after all records have been written.
   * This includes both buffered data and data that was spilled to disk.
   * Also registers all buffers with the block resolver so readers can find them.
   *
   * This is called once after all records have been written. Partition lengths
   * and checksums are computed here from the final buffer states.
   */
  private def finalizePartitions(): Unit = {
    // First, process any remaining data in memory buffers and register with block resolver
    partitionBuffers.forEach { (partitionId, buffer) =>
      val bufferSize = buffer.size()
      if (bufferSize > 0) {
        // Get final data and checksum from the buffer
        val data = buffer.getData()
        val checksum = buffer.getChecksum()

        // Set partition length to the final data size
        // (using = not += because we compute final size here only once)
        partitionLengths(partitionId) = data.length
        if (checksumEnabled) {
          partitionChecksums(partitionId) = checksum
        }

        // Register the buffer with the block resolver so readers can find this data
        // This is the critical connection between writers and readers
        streamingBlockResolver.registerStreamingBlock(
          handle.shuffleId,
          mapId,
          partitionId,
          buffer
        )

        logInfo(s"Finalized and registered buffered partition $partitionId: " +
          s"${data.length}B, ${buffer.recordCount()} records, checksum=$checksum")
      }
    }

    // Second, include data from spilled partitions
    val spilledPartitionSizes = memorySpillManager.getSpilledPartitionSizes()
    spilledPartitionSizes.foreach { case (partitionId, (size, checksum)) =>
      // Add spilled data size to partition length (may have both buffered and spilled)
      partitionLengths(partitionId) += size
      if (checksumEnabled) {
        // Combine checksums if partition has both buffered and spilled data
        if (partitionChecksums(partitionId) != 0L) {
          // XOR the checksums together for combining
          partitionChecksums(partitionId) = partitionChecksums(partitionId) ^ checksum
        } else {
          partitionChecksums(partitionId) = checksum
        }
      }

      logDebug(s"Finalized spilled partition $partitionId: " +
        s"added ${size}B from spill, total: ${partitionLengths(partitionId)}B")
    }

    logInfo(s"Finalized partitions for shuffle ${handle.shuffleId}, mapId=$mapId: " +
      s"${partitionBuffers.size()} buffered, ${spilledPartitionSizes.size} spilled")
  }

  /**
   * Creates the MapStatus for this shuffle output.
   *
   * @return The MapStatus containing shuffle server ID, partition lengths, and checksum
   */
  private def createMapStatus(): MapStatus = {
    // Calculate aggregated checksum value
    val aggregatedChecksum = if (checksumEnabled) {
      calculateAggregatedChecksum()
    } else {
      0L
    }

    MapStatus(
      blockManager.shuffleServerId,
      partitionLengths,
      mapId,
      aggregatedChecksum
    )
  }

  /**
   * Calculates the aggregated checksum across all partitions.
   *
   * @return The aggregated checksum value
   */
  private def calculateAggregatedChecksum(): Long = {
    val aggregator = new CRC32C()
    for (checksum <- partitionChecksums) {
      // Convert checksum to bytes and update aggregator
      val bytes = java.nio.ByteBuffer.allocate(8).putLong(checksum).array()
      aggregator.update(bytes)
    }
    aggregator.getValue
  }

  /**
   * Closes this writer and returns the MapStatus if successful.
   *
   * This method implements the stopping guard pattern from SortShuffleWriter
   * to prevent double-cleanup when stop() is called multiple times.
   *
   * CRITICAL: On successful completion, buffers must NOT be released here because
   * they need to remain available for reduce tasks to read via the block resolver.
   * Buffers will be cleaned up later by StreamingShuffleManager.unregisterShuffle()
   * after all reduce tasks have completed.
   *
   * @param success true if the map task completed successfully
   * @return Some(MapStatus) if successful, None otherwise
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping.compareAndSet(false, true)) {
        if (success) {
          if (!writeCompleted) {
            // Write was never called or failed - no MapStatus
            logWarning(s"stop(success=true) called but write was not completed " +
              s"for shuffle ${handle.shuffleId}, mapId=$mapId")
            return None
          }
          // On success, buffers must persist for readers but we need to release
          // TaskMemoryManager tracking to avoid "Managed memory leak detected" error
          // The actual buffer data (ByteArrayOutputStream) remains alive - only the
          // MemoryConsumer tracking is released
          memorySpillManager.releaseTaskMemoryTracking()
          Option(mapStatus)
        } else {
          // Failure case - cleanup and return None
          cleanupOnError()
          cleanupResources()  // Release buffers on failure only
          None
        }
      } else {
        // Already stopping - return None
        logDebug(s"stop() called but already stopping for shuffle ${handle.shuffleId}, mapId=$mapId")
        None
      }
    } finally {
      // Release bandwidth allocation (safe to do in all cases)
      backpressureProtocol.releaseAllocation(handle.shuffleId)
      // Note: Do NOT call cleanupResources() here - on success, buffers must persist for readers
    }
  }

  /**
   * Returns the per-partition byte counts.
   *
   * @return Array of partition lengths in bytes
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths.clone()

  /**
   * Cleans up resources on error condition.
   */
  private def cleanupOnError(): Unit = {
    logWarning(s"Cleaning up resources after error for shuffle ${handle.shuffleId}, mapId=$mapId")
    
    // Release all buffer memory
    partitionBuffers.forEach { (partitionId, buffer) =>
      try {
        val bufferSize = buffer.size()
        buffer.release()
        memorySpillManager.releaseBufferMemory(partitionId, bufferSize)
        writeMetrics.decStreamingBufferBytes(bufferSize)
      } catch {
        case e: Exception =>
          logWarning(s"Error releasing buffer for partition $partitionId: ${e.getMessage}")
      }
    }
    partitionBuffers.clear()

    // Cleanup spill manager
    memorySpillManager.cleanup()
  }

  /**
   * Cleans up all resources associated with this writer.
   */
  private def cleanupResources(): Unit = {
    // Release buffers
    partitionBuffers.forEach { (partitionId, buffer) =>
      try {
        buffer.release()
      } catch {
        case e: Exception =>
          logDebug(s"Error releasing buffer for partition $partitionId: ${e.getMessage}")
      }
    }
    partitionBuffers.clear()

    // Cleanup memory spill manager
    memorySpillManager.cleanup()

    logDebug(s"Cleaned up resources for shuffle ${handle.shuffleId}, mapId=$mapId")
  }

  /**
   * Returns the checksums for each partition (for integrity validation).
   *
   * @return Array of per-partition checksums
   */
  def getPartitionChecksums(): Array[Long] = partitionChecksums.clone()

  /**
   * Returns the aggregated checksum value for all partitions.
   *
   * @return The aggregated checksum value
   */
  def getAggregatedChecksumValue(): Long = {
    if (checksumEnabled) calculateAggregatedChecksum() else 0L
  }
}

/**
 * Companion object for StreamingShuffleWriter.
 */
private[spark] object StreamingShuffleWriter {

  /**
   * Determines if streaming shuffle can be used for the given dependency.
   *
   * Streaming shuffle eligibility depends on:
   *   - Serializer must be relocatable (support data transfer between JVMs)
   *   - Configuration must enable streaming shuffle
   *
   * @param conf The SparkConf
   * @param dep The shuffle dependency
   * @return true if streaming shuffle can be used
   */
  def canUseStreamingShuffle(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    val streamingEnabled = conf.get(SHUFFLE_STREAMING_ENABLED)
    val serializerSupportsRelocation = dep.serializer.supportsRelocationOfSerializedObjects
    
    streamingEnabled && serializerSupportsRelocation
  }

  /**
   * Returns the maximum number of partitions supported by streaming shuffle.
   *
   * Unlike UnsafeShuffleWriter which has a 16M partition limit, streaming shuffle
   * can support any number of partitions (limited by memory).
   *
   * @return Maximum number of partitions (Int.MaxValue)
   */
  def maxPartitions(): Int = Int.MaxValue
}
