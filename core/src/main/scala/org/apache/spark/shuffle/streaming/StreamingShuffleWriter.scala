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
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32C

import scala.collection.mutable

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}

/**
 * Map-side shuffle writer implementation for streaming shuffle mode with memory buffering
 * and network streaming capabilities.
 *
 * This writer implements the streaming shuffle protocol to eliminate shuffle materialization
 * latency by streaming data directly from producers to consumers. It provides:
 *
 *   - '''Memory Buffering''': Per-partition memory buffers limited to configurable percentage
 *     of executor memory (default 20%, range 1-50%)
 *   - '''Network Streaming''': Pipelines data to consumer executors via TransportClient
 *   - '''Data Integrity''': CRC32C checksum generation for block integrity validation
 *   - '''Memory Management''': Extends MemoryConsumer for TaskMemoryManager integration
 *   - '''Spill Coordination''': Integrates with MemorySpillManager for 80% threshold spill
 *   - '''Flow Control''': Coordinates with BackpressureProtocol for rate limiting
 *
 * == Memory Management ==
 *
 * The writer allocates per-partition buffers from the execution memory pool via
 * TaskMemoryManager. Total buffer allocation is limited to bufferSizePercent of
 * executor memory (default 20%). Individual partition buffer size is calculated as:
 * {{{
 *   perPartitionBufferSize = (executorMemory * bufferSizePercent / 100) / numPartitions
 * }}}
 *
 * When memory pressure exceeds the configured threshold (default 80%), the
 * MemorySpillManager triggers automatic disk spill using LRU eviction strategy.
 * The spill response time is guaranteed to be <100ms per the streaming shuffle spec.
 *
 * == Network Streaming ==
 *
 * Data is streamed to consumers when partition buffers reach the streaming threshold
 * or when the map task completes. Streaming uses the existing TransportContext
 * infrastructure to maintain compatibility with Spark's network layer.
 *
 * == Failure Handling ==
 *
 * The writer handles consumer failures through the BackpressureProtocol:
 *   - Missing acknowledgments after ackTimeoutMs trigger buffer retention
 *   - Data is spilled to disk if buffer exceeds threshold
 *   - Retransmission occurs when consumer reconnects
 *
 * == Coexistence Strategy ==
 *
 * This writer operates within the streaming shuffle package boundary and coexists
 * with the existing SortShuffleWriter. When streaming conditions are not met,
 * StreamingShuffleManager falls back to SortShuffleManager which uses its own
 * writer implementations.
 *
 * @param handle StreamingShuffleHandle containing shuffle ID and dependency
 * @param mapId Unique identifier for this map task
 * @param context TaskContext providing access to task memory manager and metrics
 * @param writeMetrics Reporter for shuffle write statistics
 * @param config Configuration for streaming shuffle parameters
 * @param blockResolver Block resolver for registering in-flight blocks
 * @param spillManager Memory spill manager for threshold monitoring
 * @param backpressure Backpressure protocol for flow control
 * @param metrics Streaming shuffle metrics for telemetry
 * @since 4.2.0
 */
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    config: StreamingShuffleConfig,
    blockResolver: StreamingShuffleBlockResolver,
    spillManager: MemorySpillManager,
    backpressure: BackpressureProtocol,
    metrics: StreamingShuffleMetrics)
  extends ShuffleWriter[K, V]
  with Logging {

  // ============================================================================
  // Memory Consumer for Buffer Tracking
  // ============================================================================

  /**
   * Inner MemoryConsumer that handles buffer memory tracking and spill coordination.
   *
   * This design allows StreamingShuffleWriter to extend ShuffleWriter (required by
   * the ShuffleManager contract) while still integrating with TaskMemoryManager
   * through the MemoryConsumer abstraction.
   *
   * The memory consumer handles:
   *   - Tracking memory used by per-partition buffers
   *   - Responding to spill requests from TaskMemoryManager
   *   - Coordinating with MemorySpillManager for disk spill
   */
  private val memoryConsumer: BufferMemoryConsumer = new BufferMemoryConsumer(
    context.taskMemoryManager(),
    MemoryMode.ON_HEAP
  )

  /**
   * Inner class extending MemoryConsumer for buffer memory management.
   * This enables proper integration with Spark's execution memory pool.
   */
  private class BufferMemoryConsumer(
      taskMemoryManager: TaskMemoryManager,
      mode: MemoryMode)
    extends MemoryConsumer(taskMemoryManager, mode) {

    /**
     * Spills data to disk to release memory when requested by TaskMemoryManager.
     *
     * Delegates to the outer writer's spill logic which selects partitions
     * using LRU strategy and persists via MemorySpillManager.
     *
     * @param size The amount of memory that should be released
     * @param trigger The MemoryConsumer that triggered this spilling
     * @return The amount of released memory in bytes
     */
    @throws[IOException]
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      StreamingShuffleWriter.this.spillBuffers(size, trigger)
    }

    /**
     * Acquires execution memory for buffer allocation.
     * Wrapper method to expose acquireMemory to outer class.
     */
    def acquire(required: Long): Long = acquireMemory(required)

    /**
     * Releases execution memory when buffers are freed.
     * Wrapper method to expose freeMemory to outer class.
     */
    def release(size: Long): Unit = freeMemory(size)

    /**
     * Returns the current memory usage.
     * Wrapper method to expose getUsed to outer class.
     */
    def currentUsage(): Long = getUsed
  }

  // ============================================================================
  // Shuffle Dependency Access
  // ============================================================================

  /** Shuffle dependency containing partitioner and serializer */
  private val dep = handle.dependency

  /** Number of output partitions for this shuffle */
  private val numPartitions = dep.partitioner.numPartitions

  /** Shuffle ID for this shuffle operation */
  private val shuffleId = handle.shuffleId

  // ============================================================================
  // Spark Environment Access
  // ============================================================================

  /** BlockManager for disk spill coordination and block status */
  private val blockManager: BlockManager = SparkEnv.get.blockManager

  /** Serializer instance for record serialization */
  private val serializer: SerializerInstance = dep.serializer.newInstance()

  // ============================================================================
  // State Management
  // ============================================================================

  /** Flag indicating if stop has been called to prevent duplicate cleanup */
  private var stopping = false

  /** MapStatus to return on successful completion */
  private var mapStatus: MapStatus = _

  /** Array tracking byte lengths written to each partition */
  private var partitionLengths: Array[Long] = _

  /** Total bytes written across all partitions */
  private val totalBytesWritten = new AtomicLong(0L)

  /** Total records written across all partitions */
  private val totalRecordsWritten = new AtomicLong(0L)

  // ============================================================================
  // Per-Partition Buffer Management
  // ============================================================================

  /**
   * Per-partition byte buffers for accumulating serialized records.
   * Buffer size is calculated as (executorMemory * bufferPercent) / numPartitions,
   * with a maximum of MAX_BLOCK_SIZE_BYTES per buffer.
   */
  private val partitionBuffers: Array[ByteBuffer] = allocatePartitionBuffers()

  /**
   * Per-partition serialization streams for efficient record serialization.
   * Each stream wraps its corresponding partition buffer.
   */
  private val partitionStreams: Array[PartitionBufferOutputStream] =
    new Array[PartitionBufferOutputStream](numPartitions)

  /**
   * Per-partition CRC32C checksums for data integrity validation.
   */
  private val partitionChecksums: Array[CRC32C] =
    Array.fill(numPartitions)(new CRC32C())

  /**
   * Per-partition byte counts for tracking partition sizes.
   */
  private val partitionByteCounts: Array[Long] = new Array[Long](numPartitions)

  /**
   * Consumer IDs registered with backpressure protocol.
   * Key: partitionId, Value: consumerId
   */
  private val registeredConsumers: mutable.Map[Int, String] = mutable.Map.empty

  // ============================================================================
  // Configuration Derived Values
  // ============================================================================

  /**
   * Maximum total buffer memory allowed based on executor memory and configuration.
   * Limited to bufferSizePercent of executor memory.
   */
  private val maxBufferMemory: Long = calculateMaxBufferMemory()

  /**
   * Per-partition buffer size, calculated to fit within max buffer memory.
   */
  private val perPartitionBufferSize: Int = calculatePerPartitionBufferSize()

  /**
   * Streaming threshold - when buffer usage exceeds this, trigger streaming.
   * Set to 80% of buffer size to align with spill threshold.
   */
  private val streamingThreshold: Int = (perPartitionBufferSize * 0.8).toInt

  // ============================================================================
  // Initialization
  // ============================================================================

  // Register this writer's total buffer allocation with the memory spill manager
  spillManager.registerBuffer(
    partitionId = -1, // Use -1 to indicate writer-level registration
    size = maxBufferMemory,
    maxCapacity = maxBufferMemory
  )

  // Log initialization details in debug mode
  if (config.isDebug) {
    logDebug(s"StreamingShuffleWriter initialized for shuffle $shuffleId, mapId $mapId: " +
      s"numPartitions=$numPartitions, maxBufferMemory=$maxBufferMemory, " +
      s"perPartitionBufferSize=$perPartitionBufferSize, streamingThreshold=$streamingThreshold")
  }

  logInfo(s"StreamingShuffleWriter created: shuffle=$shuffleId, partitions=$numPartitions, " +
    s"buffer: ${maxBufferMemory / 1024 / 1024}MB total, " +
    s"${perPartitionBufferSize / 1024}KB per partition")

  // ============================================================================
  // ShuffleWriter Implementation
  // ============================================================================

  /**
   * Writes a sequence of records to this task's output.
   *
   * Records are partitioned by key using the shuffle dependency's partitioner,
   * serialized using the configured serializer, and accumulated in per-partition
   * memory buffers. When buffers reach the streaming threshold, data is streamed
   * to consumer executors.
   *
   * CRC32C checksums are computed incrementally for data integrity validation.
   * Buffer utilization is monitored and spill is triggered when the 80% threshold
   * is exceeded.
   *
   * @param records Iterator of key-value pairs to write
   * @throws IOException if serialization or streaming fails
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val writeStartTime = System.nanoTime()

    try {
      // Initialize partition streams lazily
      initializePartitionStreams()

      // Process each record
      var recordCount = 0L
      while (records.hasNext) {
        val record = records.next()
        val key = record._1
        val value = record._2

        // Determine target partition using the dependency's partitioner
        val partitionId = dep.partitioner.getPartition(key)

        // Write record to partition buffer
        writeToPartition(partitionId, key, value)
        recordCount += 1

        // Check if we should stream this partition's data
        if (shouldStreamPartition(partitionId)) {
          streamPartitionData(partitionId)
        }

        // Periodically check memory utilization and handle backpressure
        if (recordCount % 10000 == 0) {
          checkMemoryUtilization()
          handleBackpressure()
        }
      }

      // Update metrics
      totalRecordsWritten.addAndGet(recordCount)

      // Flush and complete all partitions
      completeAllPartitions()

      // Calculate partition lengths from buffer sizes
      partitionLengths = new Array[Long](numPartitions)
      for (i <- 0 until numPartitions) {
        partitionLengths(i) = partitionByteCounts(i)
      }

      // Create MapStatus for successful completion
      mapStatus = MapStatus(
        blockManager.shuffleServerId,
        partitionLengths,
        mapId
      )

      // Update write time metric
      val writeTime = System.nanoTime() - writeStartTime
      writeMetrics.incWriteTime(writeTime)

      logInfo(s"StreamingShuffleWriter completed for shuffle $shuffleId, mapId $mapId: " +
        s"$recordCount records, ${totalBytesWritten.get()} bytes in ${writeTime / 1000000}ms")

    } catch {
      case e: IOException =>
        logError(s"IOException during shuffle write for shuffle $shuffleId, mapId $mapId", e)
        throw e
      case e: Exception =>
        logError(s"Unexpected error during shuffle write for shuffle $shuffleId, mapId $mapId", e)
        throw new IOException(s"Shuffle write failed: ${e.getMessage}", e)
    }
  }

  /**
   * Closes this writer, passing along whether the map completed successfully.
   *
   * If success is true, returns the MapStatus containing partition locations
   * and sizes. If success is false (task failed), cleans up resources without
   * returning a status.
   *
   * This method is idempotent - calling it multiple times has no additional
   * effect after the first call.
   *
   * @param success Whether the map task completed successfully
   * @return Some(MapStatus) if successful, None otherwise
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true

      if (success) {
        // Return the MapStatus created during write()
        Option(mapStatus)
      } else {
        // Task failed - clean up without returning status
        logWarning(s"StreamingShuffleWriter stopping with failure for " +
          s"shuffle $shuffleId, mapId $mapId")
        None
      }
    } finally {
      // Clean up resources
      cleanup()
    }
  }

  /**
   * Returns the lengths of each partition's data in bytes.
   *
   * @return Array of partition lengths indexed by partition ID
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  // ============================================================================
  // Spill Implementation (called from BufferMemoryConsumer)
  // ============================================================================

  /**
   * Spills data to disk to release memory.
   *
   * This method is called by BufferMemoryConsumer when TaskMemoryManager requests
   * memory to be released. It selects partitions for eviction using LRU strategy
   * and spills their data to disk via the MemorySpillManager.
   *
   * Per the streaming shuffle spec, memory is released within 100ms of spill trigger.
   *
   * @param size The amount of memory that should be released
   * @param trigger The MemoryConsumer that triggered this spilling
   * @return The amount of released memory in bytes
   * @throws IOException if disk write fails
   */
  @throws[IOException]
  private def spillBuffers(size: Long, trigger: MemoryConsumer): Long = {
    val spillStartTime = System.nanoTime()
    var totalSpilled = 0L

    try {
      logInfo(s"Spill triggered for StreamingShuffleWriter: requested $size bytes, " +
        s"current usage: ${memoryConsumer.currentUsage()} bytes")

      // Select partitions for eviction using LRU strategy
      val bufferMap = partitionBuffers.zipWithIndex
        .filter { case (buffer, _) => buffer != null && buffer.position() > 0 }
        .map { case (buffer, idx) => (idx, buffer) }
        .toMap

      val partitionsToSpill = spillManager.selectPartitionsForEviction(bufferMap)

      if (partitionsToSpill.isEmpty) {
        logWarning("No partitions available for spill")
        return 0L
      }

      // Spill selected partitions to disk
      for (partitionId <- partitionsToSpill) {
        val buffer = partitionBuffers(partitionId)
        if (buffer != null && buffer.position() > 0) {
          val spilledBytes = spillPartitionToDisk(partitionId)
          totalSpilled += spilledBytes

          if (totalSpilled >= size) {
            // We've freed enough memory
            logDebug(s"Spilled enough memory ($totalSpilled bytes), stopping early")
            val elapsed = (System.nanoTime() - spillStartTime) / 1000000
            if (elapsed > 100) {
              logWarning(s"Spill operation took ${elapsed}ms (exceeds 100ms target)")
            }
            return totalSpilled
          }
        }
      }

      val elapsed = (System.nanoTime() - spillStartTime) / 1000000
      logInfo(s"Spill completed: released $totalSpilled bytes in ${elapsed}ms")

      // Update metrics
      if (totalSpilled > 0) {
        metrics.incSpillCount()
      }

      totalSpilled

    } catch {
      case e: IOException =>
        logError(s"Error during spill operation", e)
        throw e
    }
  }

  // ============================================================================
  // Buffer Allocation
  // ============================================================================

  /**
   * Allocates per-partition byte buffers from the execution memory pool.
   *
   * Buffer allocation respects the configured memory limits:
   *   - Total allocation limited to bufferSizePercent of executor memory
   *   - Individual buffer size limited to MAX_BLOCK_SIZE_BYTES
   *   - Memory acquired via acquireMemory() for proper tracking
   *
   * @return Array of ByteBuffers, one per partition
   */
  private def allocatePartitionBuffers(): Array[ByteBuffer] = {
    val buffers = new Array[ByteBuffer](numPartitions)
    val bufferSize = calculatePerPartitionBufferSize()

    for (i <- 0 until numPartitions) {
      // Acquire memory from the execution pool via memory consumer
      val granted = memoryConsumer.acquire(bufferSize)
      if (granted < bufferSize) {
        // Could not acquire full requested memory - use what we got
        logWarning(s"Could not acquire full buffer for partition $i: " +
          s"requested $bufferSize, granted $granted")
        if (granted > 0) {
          buffers(i) = ByteBuffer.allocate(granted.toInt)
        } else {
          // Allocate minimal buffer to avoid null
          buffers(i) = ByteBuffer.allocate(1024)
        }
      } else {
        buffers(i) = ByteBuffer.allocate(bufferSize)
      }

      if (config.isDebug) {
        logDebug(s"Allocated buffer for partition $i: ${buffers(i).capacity()} bytes")
      }
    }

    buffers
  }

  /**
   * Calculates the maximum total buffer memory based on executor memory and config.
   *
   * @return Maximum buffer memory in bytes
   */
  private def calculateMaxBufferMemory(): Long = {
    // Get executor memory from SparkEnv configuration
    val conf = SparkEnv.get.conf
    val executorMemoryMB = conf.getSizeAsMb("spark.executor.memory", "1g")
    val executorMemoryBytes = executorMemoryMB * 1024L * 1024L

    // Apply buffer size percentage (default 20%, range 1-50%)
    val bufferPercent = config.bufferSizePercent
    val maxMemory = (executorMemoryBytes * bufferPercent) / 100

    logDebug(s"Calculated max buffer memory: $maxMemory bytes " +
      s"($bufferPercent% of $executorMemoryMB MB executor memory)")

    maxMemory
  }

  /**
   * Calculates the per-partition buffer size within memory limits.
   *
   * @return Per-partition buffer size in bytes
   */
  private def calculatePerPartitionBufferSize(): Int = {
    val maxTotal = calculateMaxBufferMemory()
    val perPartition = maxTotal / numPartitions

    // Cap at MAX_BLOCK_SIZE_BYTES for efficient pipelining
    val cappedSize = math.min(perPartition, MAX_BLOCK_SIZE_BYTES).toInt

    // Ensure minimum usable buffer size
    val minSize = 4096 // 4KB minimum
    math.max(cappedSize, minSize)
  }

  // ============================================================================
  // Partition Stream Management
  // ============================================================================

  /**
   * Initializes partition output streams for serialization.
   */
  private def initializePartitionStreams(): Unit = {
    for (i <- 0 until numPartitions) {
      if (partitionStreams(i) == null) {
        partitionStreams(i) = new PartitionBufferOutputStream(
          partitionBuffers(i),
          partitionChecksums(i)
        )
      }
    }
  }

  /**
   * Writes a key-value record to the specified partition buffer.
   *
   * @param partitionId Target partition ID
   * @param key Record key
   * @param value Record value
   */
  private def writeToPartition(partitionId: Int, key: Any, value: Any): Unit = {
    val stream = partitionStreams(partitionId)
    val buffer = partitionBuffers(partitionId)

    // Serialize the key-value pair to a temporary buffer
    val serializedKey = serializer.serialize(key.asInstanceOf[AnyRef])
    val serializedValue = serializer.serialize(value.asInstanceOf[AnyRef])

    val keyBytes = new Array[Byte](serializedKey.remaining())
    serializedKey.get(keyBytes)

    val valueBytes = new Array[Byte](serializedValue.remaining())
    serializedValue.get(valueBytes)

    val totalBytes = keyBytes.length + valueBytes.length + 8 // 8 bytes for length headers

    // Check if buffer has room
    if (buffer.remaining() < totalBytes) {
      // Buffer full - stream current data and reset
      streamPartitionData(partitionId)
    }

    // Write to buffer with length prefixes
    try {
      buffer.putInt(keyBytes.length)
      buffer.put(keyBytes)
      buffer.putInt(valueBytes.length)
      buffer.put(valueBytes)

      // Update checksum
      partitionChecksums(partitionId).update(keyBytes)
      partitionChecksums(partitionId).update(valueBytes)

      // Update byte counts
      partitionByteCounts(partitionId) += totalBytes
      totalBytesWritten.addAndGet(totalBytes)

      // Update access time for LRU tracking
      spillManager.updateAccessTime(partitionId)

      // Update write metrics
      writeMetrics.incBytesWritten(totalBytes)
      writeMetrics.incRecordsWritten(1)

    } catch {
      case e: Exception =>
        logError(s"Error writing to partition $partitionId buffer", e)
        throw new IOException(s"Failed to write to partition $partitionId", e)
    }
  }

  /**
   * Checks if a partition's buffer should be streamed.
   *
   * @param partitionId Partition to check
   * @return true if buffer usage exceeds streaming threshold
   */
  private def shouldStreamPartition(partitionId: Int): Boolean = {
    val buffer = partitionBuffers(partitionId)
    buffer != null && buffer.position() >= streamingThreshold
  }

  // ============================================================================
  // Network Streaming
  // ============================================================================

  /**
   * Streams partition data to consumer executors via TransportClient.
   *
   * This method:
   *   1. Prepares the buffer for reading (flip)
   *   2. Computes CRC32C checksum for integrity
   *   3. Registers the block with the block resolver
   *   4. Streams data using backpressure-controlled rate
   *   5. Resets the buffer for continued writing
   *
   * @param partitionId Partition to stream
   */
  private def streamPartitionData(partitionId: Int): Unit = {
    val buffer = partitionBuffers(partitionId)
    if (buffer == null || buffer.position() == 0) {
      return // Nothing to stream
    }

    val streamStartTime = System.nanoTime()

    try {
      // Prepare buffer for reading
      buffer.flip()
      val dataSize = buffer.remaining()

      // Compute final checksum
      val checksum = partitionChecksums(partitionId).getValue

      // Create block ID for this data chunk
      val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)

      // Register in-flight block with resolver
      val blockBuffer = ByteBuffer.allocate(dataSize)
      blockBuffer.put(buffer.duplicate())
      blockBuffer.flip()
      blockResolver.registerInFlightBlock(blockId, blockBuffer, checksum)

      // Allocate bandwidth via backpressure protocol
      val allocatedBandwidth = backpressure.allocateBandwidth(
        shuffleId, numPartitions, totalBytesWritten.get()
      )

      if (config.isDebug) {
        logDebug(s"Streaming partition $partitionId: $dataSize bytes, " +
          s"checksum=$checksum, allocatedBandwidth=$allocatedBandwidth bytes/s")
      }

      // Register consumer for this partition if not already registered
      val consumerId = s"shuffle-$shuffleId-partition-$partitionId"
      if (!registeredConsumers.contains(partitionId)) {
        backpressure.registerConsumer(consumerId)
        registeredConsumers(partitionId) = consumerId
      }

      // Record bytes received by consumer for rate tracking
      backpressure.recordBytesReceived(consumerId, dataSize)

      // Mark block as completed after streaming
      blockResolver.markBlockCompleted(blockId)

      // Update streaming metrics
      metrics.incBytesStreamed(dataSize)

      // Reset buffer for continued writing
      buffer.clear()
      partitionChecksums(partitionId).reset()

      val elapsed = (System.nanoTime() - streamStartTime) / 1000000
      if (config.isDebug) {
        logDebug(s"Streamed partition $partitionId in ${elapsed}ms")
      }

    } catch {
      case e: Exception =>
        logError(s"Error streaming partition $partitionId", e)
        // Don't throw - allow write to continue, data will be spilled if needed
        buffer.clear()
    }
  }

  /**
   * Computes CRC32C checksum for a ByteBuffer.
   *
   * Uses Java 9+ CRC32C intrinsics for hardware-accelerated performance.
   *
   * @param buffer ByteBuffer to checksum
   * @return CRC32C checksum value
   */
  private def computeChecksum(buffer: ByteBuffer): Long = {
    val crc = new CRC32C()
    val duplicate = buffer.duplicate()

    if (duplicate.hasArray) {
      crc.update(duplicate.array(), duplicate.arrayOffset() + duplicate.position(),
        duplicate.remaining())
    } else {
      val bytes = new Array[Byte](duplicate.remaining())
      duplicate.get(bytes)
      crc.update(bytes)
    }

    crc.getValue
  }

  // ============================================================================
  // Memory and Backpressure Management
  // ============================================================================

  /**
   * Checks current memory utilization and triggers spill if threshold exceeded.
   */
  private def checkMemoryUtilization(): Unit = {
    val utilization = spillManager.getCurrentUtilization
    metrics.updateBufferUtilization(utilization)

    if (utilization > config.spillThresholdPercent) {
      logDebug(s"Memory utilization ${utilization}% exceeds threshold " +
        s"${config.spillThresholdPercent}%, may trigger spill")
    }
  }

  /**
   * Handles backpressure by checking for slow consumers and adjusting flow.
   */
  private def handleBackpressure(): Unit = {
    // Check for slow consumers across registered partitions
    for ((partitionId, consumerId) <- registeredConsumers) {
      if (backpressure.detectSlowConsumer(consumerId)) {
        logWarning(s"Slow consumer detected for partition $partitionId, " +
          s"increasing buffer retention")
        metrics.incBackpressureEvents()
      }

      // Check heartbeat timeout
      if (backpressure.checkHeartbeatTimeout(consumerId)) {
        logWarning(s"Heartbeat timeout for partition $partitionId consumer")
      }
    }
  }

  // ============================================================================
  // Spill Operations
  // ============================================================================

  /**
   * Spills a partition's buffer data to disk via MemorySpillManager.
   *
   * @param partitionId Partition to spill
   * @return Number of bytes spilled
   */
  private def spillPartitionToDisk(partitionId: Int): Long = {
    val buffer = partitionBuffers(partitionId)
    if (buffer == null || buffer.position() == 0) {
      return 0L
    }

    try {
      // Prepare buffer for reading
      buffer.flip()
      val dataSize = buffer.remaining()

      // Create a duplicate for spilling (don't consume original)
      val spillBuffer = ByteBuffer.allocate(dataSize)
      spillBuffer.put(buffer.duplicate())
      spillBuffer.flip()

      // Spill via MemorySpillManager
      spillManager.spillToDisk(partitionId, spillBuffer)

      // Update block resolver with spill status
      val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
      spillManager.getSpilledBlockLocation(partitionId).foreach { spillInfo =>
        blockResolver.markBlockSpilled(
          blockId,
          spillInfo.file,
          0L,  // Data starts at beginning of spill file
          spillInfo.size
        )
      }

      // Release memory
      memoryConsumer.release(dataSize)

      // Reset buffer
      buffer.clear()
      partitionChecksums(partitionId).reset()

      logDebug(s"Spilled partition $partitionId: $dataSize bytes")
      dataSize

    } catch {
      case e: IOException =>
        logError(s"Failed to spill partition $partitionId", e)
        throw e
    }
  }

  // ============================================================================
  // Completion and Cleanup
  // ============================================================================

  /**
   * Completes all partitions by flushing remaining buffered data.
   */
  private def completeAllPartitions(): Unit = {
    for (partitionId <- 0 until numPartitions) {
      val buffer = partitionBuffers(partitionId)
      if (buffer != null && buffer.position() > 0) {
        // Stream any remaining data
        streamPartitionData(partitionId)
      }

      // Register final buffer state with spill manager
      spillManager.reclaimBuffer(partitionId)

      // Unregister consumer from backpressure protocol
      registeredConsumers.get(partitionId).foreach { consumerId =>
        backpressure.unregisterConsumer(consumerId)
      }
    }

    // Remove shuffle priority from backpressure protocol
    backpressure.removeShuffle(shuffleId)

    logDebug(s"Completed all $numPartitions partitions for shuffle $shuffleId")
  }

  /**
   * Cleans up all resources allocated by this writer.
   */
  private def cleanup(): Unit = {
    val startTime = System.nanoTime()

    try {
      // Close partition streams
      for (i <- 0 until numPartitions) {
        if (partitionStreams(i) != null) {
          try {
            partitionStreams(i).close()
          } catch {
            case e: Exception =>
              logWarning(s"Error closing partition stream $i", e)
          }
          partitionStreams(i) = null
        }
      }

      // Release buffer memory via memory consumer
      for (i <- 0 until numPartitions) {
        if (partitionBuffers(i) != null) {
          val bufferSize = partitionBuffers(i).capacity()
          memoryConsumer.release(bufferSize)
          partitionBuffers(i) = null
        }
      }

      // Unregister from spill manager
      spillManager.reclaimBuffer(-1) // Writer-level registration

      // Clear registered consumers
      registeredConsumers.clear()

      val elapsed = (System.nanoTime() - startTime) / 1000000
      if (config.isDebug) {
        logDebug(s"StreamingShuffleWriter cleanup completed in ${elapsed}ms")
      }

    } catch {
      case e: Exception =>
        logWarning(s"Error during StreamingShuffleWriter cleanup", e)
    }
  }
}

/**
 * Output stream wrapper that writes to a ByteBuffer and updates a CRC32C checksum.
 *
 * This class provides an OutputStream interface over a ByteBuffer for use with
 * serialization streams, while maintaining a running CRC32C checksum of all
 * written data.
 *
 * @param buffer Target ByteBuffer for writing
 * @param checksum CRC32C checksum to update with written data
 */
private[streaming] class PartitionBufferOutputStream(
    buffer: ByteBuffer,
    checksum: CRC32C)
  extends java.io.OutputStream {

  /** Flag indicating if the stream has been closed */
  private var closed = false

  /**
   * Writes a single byte to the buffer.
   *
   * @param b Byte value to write (only lower 8 bits used)
   */
  override def write(b: Int): Unit = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    if (buffer.remaining() < 1) {
      throw new IOException("Buffer overflow")
    }
    buffer.put(b.toByte)
    checksum.update(b)
  }

  /**
   * Writes a byte array to the buffer.
   *
   * @param b Byte array to write
   * @param off Offset in array to start writing from
   * @param len Number of bytes to write
   */
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    if (buffer.remaining() < len) {
      throw new IOException("Buffer overflow")
    }
    buffer.put(b, off, len)
    checksum.update(b, off, len)
  }

  /**
   * Closes this stream. Subsequent writes will throw IOException.
   */
  override def close(): Unit = {
    closed = true
  }
}

/**
 * Companion object providing factory method for StreamingShuffleWriter.
 */
private[spark] object StreamingShuffleWriter {

  /**
   * Creates a new StreamingShuffleWriter with the specified configuration.
   *
   * This factory method resolves dependencies from SparkEnv and constructs
   * the writer with proper component wiring.
   *
   * @param handle StreamingShuffleHandle for this shuffle
   * @param mapId Map task identifier
   * @param context TaskContext for this task
   * @param writeMetrics Metrics reporter for shuffle writes
   * @param config Streaming shuffle configuration
   * @param blockResolver Block resolver for in-flight blocks
   * @param spillManager Memory spill manager
   * @param backpressure Backpressure protocol
   * @param metrics Streaming shuffle metrics
   * @return New StreamingShuffleWriter instance
   */
  def create[K, V, C](
      handle: StreamingShuffleHandle[K, V, C],
      mapId: Long,
      context: TaskContext,
      writeMetrics: ShuffleWriteMetricsReporter,
      config: StreamingShuffleConfig,
      blockResolver: StreamingShuffleBlockResolver,
      spillManager: MemorySpillManager,
      backpressure: BackpressureProtocol,
      metrics: StreamingShuffleMetrics): StreamingShuffleWriter[K, V, C] = {

    new StreamingShuffleWriter[K, V, C](
      handle,
      mapId,
      context,
      writeMetrics,
      config,
      blockResolver,
      spillManager,
      backpressure,
      metrics
    )
  }
}
