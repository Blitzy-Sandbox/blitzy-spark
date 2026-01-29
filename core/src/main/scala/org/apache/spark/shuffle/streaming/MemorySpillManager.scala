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

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.storage.{TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * Memory threshold monitoring and spill coordination component for streaming shuffle.
 *
 * This class extends MemoryConsumer for memory allocation tracking via TaskMemoryManager.
 * It monitors memory threshold at a configurable percentage (default 80%, range 50-95%
 * via spark.shuffle.streaming.spillThreshold) and implements LRU-based partition selection
 * for eviction when memory pressure is detected.
 *
 * Key features:
 *   - Less than 100ms response time target for spill trigger to disk
 *   - Coordination with BlockManager for disk persistence of spilled data
 *   - Buffer utilization tracking across concurrent shuffles
 *   - Memory reclamation on consumer acknowledgment receipt
 *
 * The spill manager follows the UnsafeExternalSorter pattern for memory consumer
 * integration and uses ON_HEAP memory mode for streaming shuffle buffer allocation.
 *
 * @param taskMemoryManager Per-task memory manager for tracking memory allocation
 * @param conf SparkConf for reading configuration values
 */
private[spark] class MemorySpillManager(
    taskMemoryManager: TaskMemoryManager,
    conf: SparkConf)
  extends MemoryConsumer(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP)
  with Logging {

  /**
   * Memory utilization threshold (as a fraction between 0.0 and 1.0) at which
   * spilling will be triggered. Configured via spark.shuffle.streaming.spillThreshold
   * with default of 80% (valid range: 50-95%).
   */
  private val spillThreshold: Double = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) / 100.0

  /**
   * Thread-safe map for tracking partition ID to StreamingBuffer associations.
   * Enables concurrent access from multiple threads during streaming shuffle operations.
   */
  private val partitionBuffers: ConcurrentHashMap[Int, StreamingBuffer] = 
    new ConcurrentHashMap[Int, StreamingBuffer]()

  /**
   * Thread-safe map for tracking partition ID to spill file associations.
   * Maintains references to disk files where evicted partition data is stored.
   */
  private val spilledPartitions: ConcurrentHashMap[Int, SpillInfo] = 
    new ConcurrentHashMap[Int, SpillInfo]()

  /**
   * Total memory currently allocated for streaming buffers.
   * Uses AtomicLong for thread-safe concurrent accounting.
   */
  private val totalAllocatedMemory: AtomicLong = new AtomicLong(0L)

  /**
   * Total bytes spilled to disk across all spill operations.
   * Uses AtomicLong for thread-safe concurrent accounting.
   */
  private val totalBytesSpilled: AtomicLong = new AtomicLong(0L)

  /**
   * Total count of spill operations performed.
   * Uses AtomicLong for thread-safe concurrent accounting.
   */
  private val spillCount: AtomicLong = new AtomicLong(0L)

  /**
   * Maximum memory budget for this spill manager.
   * Calculated based on executor memory and buffer size percentage configuration.
   */
  private val memoryBudget: Long = calculateMemoryBudget()

  /**
   * Flag indicating if cleanup has been performed.
   */
  @volatile private var cleanedUp: Boolean = false

  // Register cleanup on task completion to prevent memory leaks
  Option(TaskContext.get()).foreach { context =>
    context.addTaskCompletionListener[Unit] { _ =>
      cleanup()
    }
  }

  /**
   * Calculates the memory budget for streaming buffers based on executor configuration.
   *
   * @return The maximum memory budget in bytes
   */
  private def calculateMemoryBudget(): Long = {
    val bufferSizePercent = conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
    // Get total execution memory from memory manager
    val env = SparkEnv.get
    val totalExecutionMemory = if (env != null && env.memoryManager != null) {
      env.memoryManager.maxOnHeapStorageMemory + env.memoryManager.maxOffHeapStorageMemory
    } else {
      // Fallback: use a reasonable default based on JVM heap
      Runtime.getRuntime.maxMemory() / 2
    }
    val budget = (totalExecutionMemory * bufferSizePercent / 100.0).toLong
    logInfo(log"MemorySpillManager initialized with budget: ${MDC(MEMORY_SIZE, 
      Utils.bytesToString(budget))}, spillThreshold: ${MDC(COUNT, 
      (spillThreshold * 100).toInt)}%")
    budget
  }

  /**
   * Spills partition data to disk to release memory when triggered by TaskMemoryManager.
   *
   * This method is called by TaskMemoryManager when there is not enough memory for the task.
   * It implements LRU-based partition selection for eviction and targets less than 100ms
   * response time for the spill operation.
   *
   * Following the MemoryConsumer contract, this method:
   *   - Should not call acquireMemory() to avoid deadlock
   *   - Returns the amount of memory freed in bytes
   *   - Selects partitions using LRU policy for fair eviction
   *
   * @param size The amount of memory requested to be released
   * @param trigger The MemoryConsumer that triggered this spill
   * @return The amount of memory actually released in bytes
   */
  @throws[IOException]
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this || partitionBuffers.isEmpty) {
      // Only handle spill requests from self, or skip if no buffers to spill
      return 0L
    }

    val spillStartTime = System.nanoTime()
    var freedBytes: Long = 0L
    var spilledPartitionCount: Int = 0

    logInfo(log"Thread ${MDC(THREAD_ID, Thread.currentThread().getId)} " +
      log"starting spill operation, requested: ${MDC(MEMORY_SIZE, 
        Utils.bytesToString(size))}")

    try {
      // Select partitions for eviction using LRU policy
      val partitionsToSpill = selectPartitionsForSpill(size)

      for ((partitionId, buffer) <- partitionsToSpill if freedBytes < size) {
        val bytesSpilled = spillPartitionToDisk(partitionId, buffer)
        if (bytesSpilled > 0) {
          freedBytes += bytesSpilled
          spilledPartitionCount += 1
          
          // Release memory from the buffer
          buffer.release()
          partitionBuffers.remove(partitionId)
          totalAllocatedMemory.addAndGet(-bytesSpilled)
        }
      }

      // Update metrics
      totalBytesSpilled.addAndGet(freedBytes)
      spillCount.incrementAndGet()

      // Release memory to the TaskMemoryManager
      if (freedBytes > 0) {
        freeMemory(freedBytes)
      }

    } catch {
      case e: IOException =>
        logError(log"Error during spill operation: ${MDC(ERROR, e.getMessage)}")
        throw e
    }

    val spillDurationMs = (System.nanoTime() - spillStartTime) / 1000000
    logInfo(log"Thread ${MDC(THREAD_ID, Thread.currentThread().getId)} " +
      log"spilled ${MDC(NUM_PARTITIONS, spilledPartitionCount)} partitions, " +
      log"freed ${MDC(MEMORY_SIZE, Utils.bytesToString(freedBytes))} " +
      log"in ${MDC(COUNT, spillDurationMs)}ms " +
      log"(${MDC(NUM_SPILLS, spillCount.get())} spills total)")

    // Log warning if spill took longer than target 100ms
    if (spillDurationMs > 100) {
      logWarning(log"Spill operation exceeded 100ms target: ${MDC(COUNT, spillDurationMs)}ms")
    }

    freedBytes
  }

  /**
   * Selects partitions for spilling using LRU (Least Recently Used) policy.
   *
   * Partitions are sorted by their last access time (oldest first) and selected
   * until enough memory can be freed to satisfy the requested size.
   *
   * @param requestedSize The amount of memory requested to be freed
   * @return A sequence of (partitionId, StreamingBuffer) tuples to be spilled
   */
  private def selectPartitionsForSpill(requestedSize: Long): Seq[(Int, StreamingBuffer)] = {
    // Get all buffers and sort by last access time (LRU - oldest first)
    val buffersByLRU = partitionBuffers.asScala.toSeq
      .sortBy { case (_, buffer) => buffer.getLastAccessTime() }

    var accumulatedSize: Long = 0L
    val selectedPartitions = buffersByLRU.takeWhile { case (_, buffer) =>
      val shouldTake = accumulatedSize < requestedSize
      if (shouldTake) {
        accumulatedSize += buffer.size()
      }
      shouldTake
    }

    logDebug(s"Selected ${selectedPartitions.size} partitions for spill " +
      s"(accumulated size: ${Utils.bytesToString(accumulatedSize)})")

    selectedPartitions
  }

  /**
   * Spills a single partition's data to disk.
   *
   * Creates a temporary shuffle block file via DiskBlockManager and writes the
   * buffer contents. Tracks the spill file location for later retrieval.
   *
   * @param partitionId The partition ID being spilled
   * @param buffer The StreamingBuffer containing the partition data
   * @return The number of bytes written to disk
   */
  private def spillPartitionToDisk(partitionId: Int, buffer: StreamingBuffer): Long = {
    val env = SparkEnv.get
    if (env == null || env.blockManager == null) {
      logWarning("SparkEnv or BlockManager not available, cannot spill to disk")
      return 0L
    }

    val blockManager = env.blockManager
    val diskBlockManager = blockManager.diskBlockManager

    // Create a temporary shuffle block for the spilled data
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()
    
    var bytesWritten: Long = 0L
    var outputStream: FileOutputStream = null
    
    try {
      // Get the buffer data and write to disk
      val data = buffer.getData()
      outputStream = new FileOutputStream(file)
      outputStream.write(data)
      outputStream.flush()
      bytesWritten = data.length.toLong

      // Record spill information for later retrieval
      val spillInfo = SpillInfo(
        partitionId = partitionId,
        blockId = blockId,
        file = file,
        size = bytesWritten,
        checksum = buffer.getChecksum()
      )
      spilledPartitions.put(partitionId, spillInfo)

      logDebug(s"Spilled partition $partitionId to ${file.getAbsolutePath}, " +
        s"size: ${Utils.bytesToString(bytesWritten)}")

    } catch {
      case e: IOException =>
        logError(log"Failed to spill partition ${MDC(PARTITION_ID, partitionId)}: " +
          log"${MDC(ERROR, e.getMessage)}")
        // Clean up partial file on failure
        if (file.exists()) {
          file.delete()
        }
        throw e
    } finally {
      if (outputStream != null) {
        try {
          outputStream.close()
        } catch {
          case e: IOException =>
            logWarning(log"Error closing output stream: ${MDC(ERROR, e.getMessage)}")
        }
      }
    }

    bytesWritten
  }

  /**
   * Acquires memory for a partition buffer via TaskMemoryManager.
   *
   * This method allocates execution memory for a streaming buffer and updates
   * internal accounting. If memory cannot be acquired, it may trigger spilling
   * of other buffers.
   *
   * @param partitionId The partition ID for which memory is being acquired
   * @param size The number of bytes to acquire
   * @return The actual number of bytes acquired (may be less than requested)
   */
  def acquireBufferMemory(partitionId: Int, size: Long): Long = {
    if (size <= 0) {
      return 0L
    }

    // Check if we're within the memory budget
    val currentUsage = totalAllocatedMemory.get()
    val maxAllowed = memoryBudget - currentUsage
    val requestSize = math.min(size, maxAllowed)

    if (requestSize <= 0) {
      logDebug(s"Memory budget exhausted for partition $partitionId, " +
        s"current: ${Utils.bytesToString(currentUsage)}, " +
        s"budget: ${Utils.bytesToString(memoryBudget)}")
      // Trigger spill to free up memory
      checkAndTriggerSpill()
      return 0L
    }

    // Acquire memory from TaskMemoryManager
    val granted = acquireMemory(requestSize)
    
    if (granted > 0) {
      totalAllocatedMemory.addAndGet(granted)
      logDebug(s"Acquired ${Utils.bytesToString(granted)} for partition $partitionId, " +
        s"total allocated: ${Utils.bytesToString(totalAllocatedMemory.get())}")
    }

    granted
  }

  /**
   * Releases memory for a partition buffer on consumer acknowledgment.
   *
   * Called when a consumer acknowledges receipt of partition data, allowing
   * the memory to be reclaimed and made available for other operations.
   *
   * @param partitionId The partition ID whose memory is being released
   * @param size The number of bytes to release
   */
  def releaseBufferMemory(partitionId: Int, size: Long): Unit = {
    if (size <= 0) {
      return
    }

    val actualRelease = math.min(size, totalAllocatedMemory.get())
    if (actualRelease > 0) {
      freeMemory(actualRelease)
      totalAllocatedMemory.addAndGet(-actualRelease)
      
      logDebug(s"Released ${Utils.bytesToString(actualRelease)} for partition $partitionId, " +
        s"total allocated: ${Utils.bytesToString(totalAllocatedMemory.get())}")
    }
  }

  /**
   * Monitors memory threshold and triggers spill if utilization exceeds the configured threshold.
   *
   * This method should be called periodically (e.g., after buffer writes) to proactively
   * manage memory pressure and avoid OOM conditions. If memory utilization exceeds the
   * spillThreshold, it triggers a spill operation to free up memory.
   *
   * @return true if spill was triggered, false otherwise
   */
  def checkAndTriggerSpill(): Boolean = {
    val currentUsage = totalAllocatedMemory.get()
    val usageRatio = if (memoryBudget > 0) currentUsage.toDouble / memoryBudget else 0.0

    if (usageRatio >= spillThreshold) {
      logInfo(log"Memory usage (${MDC(COUNT, (usageRatio * 100).toInt)}%) " +
        log"exceeds threshold (${MDC(COUNT, (spillThreshold * 100).toInt)}%), " +
        log"triggering spill")

      // Calculate how much memory to free to get below threshold
      val targetUsage = (memoryBudget * (spillThreshold - 0.1)).toLong // Target 10% below threshold
      val bytesToFree = currentUsage - targetUsage

      try {
        val freedBytes = spill(bytesToFree, this)
        logInfo(log"Proactive spill freed ${MDC(MEMORY_SIZE, 
          Utils.bytesToString(freedBytes))}")
        return freedBytes > 0
      } catch {
        case e: IOException =>
          logError(log"Failed to trigger proactive spill: ${MDC(ERROR, e.getMessage)}")
      }
    }

    false
  }

  /**
   * Registers a StreamingBuffer for management by this MemorySpillManager.
   *
   * The registered buffer will be tracked for LRU-based eviction and included
   * in memory utilization calculations.
   *
   * @param partitionId The partition ID associated with the buffer
   * @param buffer The StreamingBuffer to register
   */
  def registerBuffer(partitionId: Int, buffer: StreamingBuffer): Unit = {
    if (buffer == null) {
      throw new IllegalArgumentException(s"Cannot register null buffer for partition $partitionId")
    }

    // Remove any existing buffer for this partition
    val existingBuffer = partitionBuffers.put(partitionId, buffer)
    if (existingBuffer != null) {
      logDebug(s"Replaced existing buffer for partition $partitionId")
      // Note: The caller is responsible for releasing the old buffer's memory
    }

    buffer.updateLastAccessTime()
    
    logDebug(s"Registered buffer for partition $partitionId, " +
      s"size: ${Utils.bytesToString(buffer.size())}, " +
      s"total buffers: ${partitionBuffers.size()}")
  }

  /**
   * Retrieves spilled data for a partition from disk.
   *
   * If the partition was previously spilled to disk, this method reads the data
   * back into memory. Returns null if the partition was not spilled.
   *
   * @param partitionId The partition ID to retrieve
   * @return The partition data as a byte array, or null if not found
   */
  def getSpilledData(partitionId: Int): Array[Byte] = {
    val spillInfo = spilledPartitions.get(partitionId)
    if (spillInfo == null) {
      logDebug(s"No spilled data found for partition $partitionId")
      return null
    }

    val file = spillInfo.file
    if (!file.exists()) {
      logWarning(log"Spill file for partition ${MDC(PARTITION_ID, partitionId)} " +
        log"not found: ${MDC(PATH, file.getAbsolutePath)}")
      spilledPartitions.remove(partitionId)
      return null
    }

    var inputStream: FileInputStream = null
    try {
      inputStream = new FileInputStream(file)
      val data = new Array[Byte](spillInfo.size.toInt)
      var bytesRead = 0
      var offset = 0
      
      while (offset < data.length && {
        bytesRead = inputStream.read(data, offset, data.length - offset)
        bytesRead != -1
      }) {
        offset += bytesRead
      }

      if (offset != data.length) {
        logWarning(log"Incomplete read from spill file for partition " +
          log"${MDC(PARTITION_ID, partitionId)}: " +
          log"expected ${MDC(BYTE_SIZE, spillInfo.size)}, " +
          log"got ${MDC(BYTE_SIZE, offset)}")
        return null
      }

      logDebug(s"Retrieved ${Utils.bytesToString(data.length)} of spilled data " +
        s"for partition $partitionId")

      data
    } catch {
      case e: IOException =>
        logError(log"Failed to read spilled data for partition " +
          log"${MDC(PARTITION_ID, partitionId)}: ${MDC(ERROR, e.getMessage)}")
        null
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: IOException =>
            logWarning(log"Error closing input stream: ${MDC(ERROR, e.getMessage)}")
        }
      }
    }
  }

  /**
   * Releases all resources and deletes spill files managed by this MemorySpillManager.
   *
   * This method should be called when the task completes (success or failure) to
   * ensure proper cleanup of memory and disk resources. It is idempotent and can
   * be called multiple times safely.
   */
  def cleanup(): Unit = {
    if (cleanedUp) {
      return
    }

    synchronized {
      if (cleanedUp) {
        return
      }
      cleanedUp = true
    }

    logInfo(log"Cleaning up MemorySpillManager: ${MDC(NUM_PARTITIONS, 
      partitionBuffers.size())} buffers, ${MDC(NUM_PARTITIONS, 
      spilledPartitions.size())} spill files")

    // Release all registered buffers
    partitionBuffers.forEach { (partitionId, buffer) =>
      try {
        val bufferSize = buffer.size()
        buffer.release()
        logDebug(s"Released buffer for partition $partitionId, " +
          s"size: ${Utils.bytesToString(bufferSize)}")
      } catch {
        case e: Exception =>
          logWarning(log"Error releasing buffer for partition " +
            log"${MDC(PARTITION_ID, partitionId)}: ${MDC(ERROR, e.getMessage)}")
      }
    }
    partitionBuffers.clear()

    // Delete all spill files
    spilledPartitions.forEach { (partitionId, spillInfo) =>
      try {
        if (spillInfo.file.exists()) {
          if (spillInfo.file.delete()) {
            logDebug(s"Deleted spill file for partition $partitionId: " +
              s"${spillInfo.file.getAbsolutePath}")
          } else {
            logWarning(log"Failed to delete spill file for partition " +
              log"${MDC(PARTITION_ID, partitionId)}: " +
              log"${MDC(PATH, spillInfo.file.getAbsolutePath)}")
          }
        }
      } catch {
        case e: Exception =>
          logWarning(log"Error deleting spill file for partition " +
            log"${MDC(PARTITION_ID, partitionId)}: ${MDC(ERROR, e.getMessage)}")
      }
    }
    spilledPartitions.clear()

    // Release remaining memory
    val remainingMemory = totalAllocatedMemory.get()
    if (remainingMemory > 0) {
      freeMemory(remainingMemory)
      totalAllocatedMemory.set(0L)
    }

    logInfo(log"MemorySpillManager cleanup complete. Total spilled: " +
      log"${MDC(MEMORY_SIZE, Utils.bytesToString(totalBytesSpilled.get()))}, " +
      log"spill count: ${MDC(NUM_SPILLS, spillCount.get())}")
  }

  /**
   * Returns the amount of memory currently used by this MemorySpillManager.
   *
   * @return The current memory usage in bytes
   */
  override def getUsed(): Long = totalAllocatedMemory.get()

  /**
   * Returns the total number of spill operations performed.
   *
   * @return The spill count
   */
  def getSpillCount(): Long = spillCount.get()

  /**
   * Returns the total number of bytes spilled to disk.
   *
   * @return The total bytes spilled
   */
  def getTotalSpillBytes(): Long = totalBytesSpilled.get()

  /**
   * Returns the number of partitions currently buffered in memory.
   *
   * @return The number of buffered partitions
   */
  def getBufferedPartitionCount(): Int = partitionBuffers.size()

  /**
   * Returns the number of partitions currently spilled to disk.
   *
   * @return The number of spilled partitions
   */
  def getSpilledPartitionCount(): Int = spilledPartitions.size()

  /**
   * Returns the memory budget for this MemorySpillManager.
   *
   * @return The memory budget in bytes
   */
  def getMemoryBudget(): Long = memoryBudget

  /**
   * Returns the configured spill threshold as a fraction (0.0 to 1.0).
   *
   * @return The spill threshold
   */
  def getSpillThreshold(): Double = spillThreshold

  /**
   * Checks if a partition is currently spilled to disk.
   *
   * @param partitionId The partition ID to check
   * @return true if the partition is spilled, false otherwise
   */
  def isSpilled(partitionId: Int): Boolean = spilledPartitions.containsKey(partitionId)

  /**
   * Checks if a partition is currently buffered in memory.
   *
   * @param partitionId The partition ID to check
   * @return true if the partition is buffered, false otherwise
   */
  def isBuffered(partitionId: Int): Boolean = partitionBuffers.containsKey(partitionId)

  /**
   * Gets the buffer for a specific partition if it exists.
   *
   * @param partitionId The partition ID to look up
   * @return The StreamingBuffer for the partition, or null if not found
   */
  def getBuffer(partitionId: Int): StreamingBuffer = partitionBuffers.get(partitionId)

  /**
   * Removes a buffer from management without releasing its memory.
   *
   * This is useful when the caller wants to take ownership of the buffer
   * and handle its lifecycle independently.
   *
   * @param partitionId The partition ID to unregister
   * @return The removed StreamingBuffer, or null if not found
   */
  def unregisterBuffer(partitionId: Int): StreamingBuffer = {
    val buffer = partitionBuffers.remove(partitionId)
    if (buffer != null) {
      logDebug(s"Unregistered buffer for partition $partitionId")
    }
    buffer
  }

  /**
   * Removes a spilled partition's tracking information and deletes its file.
   *
   * @param partitionId The partition ID to remove
   * @return true if the spill info was removed, false if not found
   */
  def removeSpilledPartition(partitionId: Int): Boolean = {
    val spillInfo = spilledPartitions.remove(partitionId)
    if (spillInfo != null) {
      try {
        if (spillInfo.file.exists()) {
          spillInfo.file.delete()
        }
        logDebug(s"Removed spilled partition $partitionId")
        true
      } catch {
        case e: Exception =>
          logWarning(log"Error removing spilled partition " +
            log"${MDC(PARTITION_ID, partitionId)}: ${MDC(ERROR, e.getMessage)}")
          false
      }
    } else {
      false
    }
  }

  override def toString: String = {
    s"MemorySpillManager(used=${Utils.bytesToString(getUsed())}, " +
      s"budget=${Utils.bytesToString(memoryBudget)}, " +
      s"threshold=${(spillThreshold * 100).toInt}%, " +
      s"buffered=${partitionBuffers.size()}, " +
      s"spilled=${spilledPartitions.size()}, " +
      s"spillCount=${spillCount.get()})"
  }
}

/**
 * Metadata about a partition that has been spilled to disk.
 *
 * @param partitionId The partition ID that was spilled
 * @param blockId The TempShuffleBlockId for the spill file
 * @param file The file where the data was written
 * @param size The size of the spilled data in bytes
 * @param checksum The CRC32C checksum of the spilled data for integrity validation
 */
private[spark] case class SpillInfo(
    partitionId: Int,
    blockId: TempShuffleBlockId,
    file: File,
    size: Long,
    checksum: Long)

/**
 * Companion object for MemorySpillManager providing factory methods and utilities.
 */
private[spark] object MemorySpillManager extends Logging {

  /**
   * Creates a new MemorySpillManager with the given TaskMemoryManager and configuration.
   *
   * @param taskMemoryManager Per-task memory manager
   * @param conf SparkConf for reading configuration
   * @return A new MemorySpillManager instance
   */
  def create(taskMemoryManager: TaskMemoryManager, conf: SparkConf): MemorySpillManager = {
    new MemorySpillManager(taskMemoryManager, conf)
  }

  /**
   * Creates a new MemorySpillManager using the SparkEnv's configuration.
   *
   * @param taskMemoryManager Per-task memory manager
   * @return A new MemorySpillManager instance
   * @throws IllegalStateException if SparkEnv is not initialized
   */
  def create(taskMemoryManager: TaskMemoryManager): MemorySpillManager = {
    val env = SparkEnv.get
    if (env == null) {
      throw new IllegalStateException("SparkEnv not initialized")
    }
    new MemorySpillManager(taskMemoryManager, env.conf)
  }
}
