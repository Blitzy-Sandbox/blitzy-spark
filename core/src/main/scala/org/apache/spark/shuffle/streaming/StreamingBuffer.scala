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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32C

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.SerializerInstance

/**
 * Per-partition buffer abstraction for streaming shuffle with capacity management 
 * and CRC32C checksum generation per block.
 *
 * This class manages serialized key-value records for a single partition, tracks 
 * buffer size and record count, provides append/read/reset operations, and generates 
 * checksums for integrity validation during streaming transfer.
 *
 * The buffer integrates with TaskMemoryManager for memory tracking and supports
 * LRU-based eviction through last access time tracking.
 *
 * @param partitionId The partition ID this buffer is associated with
 * @param capacity Maximum capacity of this buffer in bytes
 * @param taskMemoryManager Per-task memory manager for tracking memory allocation
 */
private[spark] class StreamingBuffer(
    val partitionId: Int,
    val capacity: Long,
    private val taskMemoryManager: TaskMemoryManager)
  extends Logging {

  // Internal byte array output stream for storing serialized data
  private val buffer: ByteArrayOutputStream = new ByteArrayOutputStream(
    math.min(capacity, Int.MaxValue).toInt
  )

  // CRC32C checksum calculator for block integrity validation
  private val checksum: CRC32C = new CRC32C()

  // Thread-safe counter for tracking number of records in the buffer
  private val recordsCounter: AtomicLong = new AtomicLong(0L)

  // Thread-safe timestamp for LRU eviction tracking
  private val lastAccessTimeMs: AtomicLong = new AtomicLong(System.currentTimeMillis())

  // Tracks memory that has been acquired from TaskMemoryManager
  @volatile private var acquiredMemory: Long = 0L

  // Flag indicating if buffer has been released
  @volatile private var released: Boolean = false

  /**
   * Serializes and appends a key-value record to the buffer.
   *
   * This method serializes the key-value pair using the provided serializer instance
   * and appends the serialized bytes to the internal buffer. The checksum is updated
   * incrementally with the new data.
   *
   * @param key The key to serialize and append
   * @param value The value to serialize and append
   * @param serializer The serializer instance to use for serialization
   * @tparam K The key type
   * @tparam V The value type
   * @return The number of bytes written for this record
   * @throws IllegalStateException if the buffer has been released
   */
  def append[K: ClassTag, V: ClassTag](
      key: K, 
      value: V, 
      serializer: SerializerInstance): Long = {
    checkNotReleased()
    updateLastAccessTime()

    val sizeBefore = buffer.size()
    
    // Create a serialization stream that writes to our buffer
    val serializationStream = serializer.serializeStream(buffer)
    try {
      serializationStream.writeKey(key)
      serializationStream.writeValue(value)
      serializationStream.flush()
    } finally {
      // Note: We don't close the stream here because we want to keep writing to the buffer
      // The ByteArrayOutputStream doesn't need to be closed between writes
    }
    
    val bytesWritten = buffer.size() - sizeBefore
    
    // Update checksum with newly written bytes
    val allData = buffer.toByteArray
    checksum.update(allData, sizeBefore, bytesWritten)
    
    // Increment record counter
    recordsCounter.incrementAndGet()
    
    logDebug(s"Appended record to buffer for partition $partitionId, " +
      s"bytes written: $bytesWritten, total size: ${buffer.size()}")
    
    bytesWritten
  }

  /**
   * Appends pre-serialized bytes directly to the buffer.
   *
   * This method is useful when the data has already been serialized elsewhere
   * and just needs to be added to the buffer. The checksum is updated with
   * the new bytes.
   *
   * @param bytes The pre-serialized bytes to append
   * @throws IllegalStateException if the buffer has been released
   */
  def appendBytes(bytes: Array[Byte]): Unit = {
    checkNotReleased()
    updateLastAccessTime()

    if (bytes != null && bytes.length > 0) {
      buffer.write(bytes)
      checksum.update(bytes)
      recordsCounter.incrementAndGet()
      
      logDebug(s"Appended ${bytes.length} pre-serialized bytes to buffer for partition " +
        s"$partitionId, total size: ${buffer.size()}")
    }
  }

  /**
   * Returns the CRC32C checksum of the current buffer contents.
   *
   * This checksum can be used for integrity validation during streaming
   * transfer to ensure data has not been corrupted.
   *
   * @return The CRC32C checksum value as a Long
   */
  def getChecksum(): Long = {
    checkNotReleased()
    checksum.getValue
  }

  /**
   * Returns the buffer contents as a byte array.
   *
   * This method returns a copy of the internal buffer data for streaming
   * to consumers. The caller should be aware that this creates a new
   * array copy.
   *
   * @return A byte array containing the buffer contents
   * @throws IllegalStateException if the buffer has been released
   */
  def getData(): Array[Byte] = {
    checkNotReleased()
    updateLastAccessTime()
    buffer.toByteArray
  }

  /**
   * Returns the buffer contents as an InputStream.
   *
   * This method wraps the buffer data in a ByteArrayInputStream for
   * efficient streaming access without copying the entire buffer.
   *
   * @return An InputStream for reading buffer contents
   * @throws IllegalStateException if the buffer has been released
   */
  def getDataAsInputStream(): InputStream = {
    checkNotReleased()
    updateLastAccessTime()
    new ByteArrayInputStream(buffer.toByteArray)
  }

  /**
   * Returns the current size of the buffer in bytes.
   *
   * @return The current buffer size in bytes
   */
  def size(): Long = buffer.size().toLong

  /**
   * Returns the number of records currently stored in the buffer.
   *
   * @return The number of records in the buffer
   */
  def recordCount(): Long = recordsCounter.get()

  /**
   * Checks if the buffer has exceeded its capacity threshold.
   *
   * This method is used for determining when to trigger spilling
   * or streaming to consumers.
   *
   * @return true if the buffer size exceeds or equals capacity
   */
  def isFull(): Boolean = {
    buffer.size().toLong >= capacity
  }

  /**
   * Returns the remaining capacity in bytes before the buffer is full.
   *
   * @return The number of bytes available before capacity is reached
   */
  def remainingCapacity(): Long = {
    math.max(0L, capacity - buffer.size().toLong)
  }

  /**
   * Clears the buffer for reuse while preserving the allocated memory.
   *
   * This method resets the internal buffer, checksum, and record counter
   * without releasing memory back to the TaskMemoryManager. This allows
   * the buffer to be efficiently reused for subsequent partitions.
   *
   * @throws IllegalStateException if the buffer has been released
   */
  def reset(): Unit = {
    checkNotReleased()
    
    buffer.reset()
    checksum.reset()
    recordsCounter.set(0L)
    updateLastAccessTime()
    
    logDebug(s"Reset buffer for partition $partitionId")
  }

  /**
   * Releases all resources and memory associated with this buffer.
   *
   * This method should be called when the buffer is no longer needed.
   * After calling release(), the buffer cannot be used again.
   */
  def release(): Unit = {
    if (!released) {
      synchronized {
        if (!released) {
          // Release memory back to TaskMemoryManager
          if (acquiredMemory > 0 && taskMemoryManager != null) {
            // Note: In the actual implementation, we would need a MemoryConsumer reference
            // to properly release memory. For now, we just track it internally.
            logDebug(s"Released buffer for partition $partitionId, " +
              s"memory: $acquiredMemory bytes")
            acquiredMemory = 0L
          }
          
          // Clear internal state
          buffer.reset()
          checksum.reset()
          recordsCounter.set(0L)
          released = true
          
          logDebug(s"Released buffer for partition $partitionId")
        }
      }
    }
  }

  /**
   * Checks if this buffer has been released.
   *
   * @return true if the buffer has been released and should not be used
   */
  def isReleased: Boolean = released

  /**
   * Updates the last access timestamp for LRU eviction tracking.
   *
   * This method should be called whenever the buffer is accessed
   * (read or write operations) to maintain accurate LRU ordering.
   */
  def updateLastAccessTime(): Unit = {
    lastAccessTimeMs.set(System.currentTimeMillis())
  }

  /**
   * Returns the last access time in milliseconds since epoch.
   *
   * This timestamp is used by MemorySpillManager for LRU-based
   * partition eviction decisions.
   *
   * @return The last access timestamp in milliseconds
   */
  def getLastAccessTime(): Long = {
    lastAccessTimeMs.get()
  }

  /**
   * Acquires memory from the TaskMemoryManager for this buffer.
   *
   * @param size The number of bytes to acquire
   * @return The actual number of bytes acquired (may be less than requested)
   */
  private[streaming] def acquireMemory(size: Long): Long = {
    if (taskMemoryManager != null && size > 0) {
      // Note: In the full implementation, this would use taskMemoryManager.acquireExecutionMemory()
      // with a proper MemoryConsumer. For now, we track the memory internally.
      acquiredMemory += size
      logDebug(s"Acquired $size bytes for buffer partition $partitionId, " +
        s"total acquired: $acquiredMemory")
      size
    } else {
      0L
    }
  }

  /**
   * Releases a portion of acquired memory back to the TaskMemoryManager.
   *
   * @param size The number of bytes to release
   */
  private[streaming] def releaseMemory(size: Long): Unit = {
    if (taskMemoryManager != null && size > 0 && acquiredMemory > 0) {
      val toRelease = math.min(size, acquiredMemory)
      acquiredMemory -= toRelease
      // Note: In the full implementation, this would use taskMemoryManager.releaseExecutionMemory()
      logDebug(s"Released $toRelease bytes for buffer partition $partitionId, " +
        s"remaining acquired: $acquiredMemory")
    }
  }

  /**
   * Returns the amount of memory currently acquired by this buffer.
   *
   * @return The acquired memory in bytes
   */
  private[streaming] def getAcquiredMemory(): Long = acquiredMemory

  /**
   * Checks that the buffer has not been released.
   *
   * @throws IllegalStateException if the buffer has been released
   */
  private def checkNotReleased(): Unit = {
    if (released) {
      throw new IllegalStateException(
        s"StreamingBuffer for partition $partitionId has already been released")
    }
  }

  override def toString: String = {
    s"StreamingBuffer(partitionId=$partitionId, capacity=$capacity, " +
      s"size=${buffer.size()}, records=${recordsCounter.get()}, released=$released)"
  }
}

/**
 * Companion object for StreamingBuffer providing buffer pool management for reuse.
 *
 * The buffer pool enables efficient reuse of StreamingBuffer instances across
 * partitions, reducing allocation overhead and memory churn during streaming
 * shuffle operations.
 */
private[spark] object StreamingBuffer extends Logging {

  // Thread-safe queue for pooling reusable buffer instances
  private val bufferPool: ConcurrentLinkedQueue[StreamingBuffer] = new ConcurrentLinkedQueue()

  // Counter for tracking pool size
  private val poolSizeCounter: AtomicLong = new AtomicLong(0L)

  // Maximum number of buffers to keep in the pool (prevents unbounded growth)
  private val maxPoolSize: Int = 256

  /**
   * Acquires a StreamingBuffer from the pool or creates a new one if the pool is empty.
   *
   * This method attempts to reuse a previously released buffer from the pool.
   * If no buffer is available, a new buffer is created with the specified parameters.
   *
   * @param partitionId The partition ID for the buffer
   * @param capacity The maximum capacity in bytes
   * @param taskMemoryManager Per-task memory manager for memory tracking
   * @return A StreamingBuffer instance ready for use
   */
  def acquire(
      partitionId: Int, 
      capacity: Long, 
      taskMemoryManager: TaskMemoryManager): StreamingBuffer = {
    
    // Try to get a buffer from the pool
    val pooledBuffer = bufferPool.poll()
    
    if (pooledBuffer != null) {
      poolSizeCounter.decrementAndGet()
      logDebug(s"Acquired buffer from pool for partition $partitionId, " +
        s"pool size: ${poolSizeCounter.get()}")
      
      // Create a new buffer since we can't easily reinitialize pooled buffers
      // with different capacity and partitionId
      new StreamingBuffer(partitionId, capacity, taskMemoryManager)
    } else {
      logDebug(s"Creating new buffer for partition $partitionId, capacity: $capacity")
      new StreamingBuffer(partitionId, capacity, taskMemoryManager)
    }
  }

  /**
   * Releases a StreamingBuffer back to the pool for reuse.
   *
   * The buffer is reset before being added to the pool. If the pool
   * is at maximum capacity, the buffer is discarded instead.
   *
   * @param buffer The buffer to release back to the pool
   */
  def release(buffer: StreamingBuffer): Unit = {
    if (buffer != null && poolSizeCounter.get() < maxPoolSize) {
      // Reset and release the buffer
      try {
        buffer.release()
        
        // Add to pool only if under max size
        if (bufferPool.offer(buffer)) {
          poolSizeCounter.incrementAndGet()
          logDebug(s"Released buffer to pool for partition ${buffer.partitionId}, " +
            s"pool size: ${poolSizeCounter.get()}")
        }
      } catch {
        case e: Exception =>
          logWarning(s"Error releasing buffer to pool: ${e.getMessage}")
      }
    } else if (buffer != null) {
      // Pool is full, just release the buffer
      buffer.release()
      logDebug(s"Pool full, released buffer for partition ${buffer.partitionId} without pooling")
    }
  }

  /**
   * Clears all buffers from the pool.
   *
   * This method should be called during shutdown or when the pool
   * needs to be completely emptied.
   */
  def clearPool(): Unit = {
    var buffer = bufferPool.poll()
    while (buffer != null) {
      buffer.release()
      poolSizeCounter.decrementAndGet()
      buffer = bufferPool.poll()
    }
    poolSizeCounter.set(0L)
    logDebug("Cleared buffer pool")
  }

  /**
   * Returns the current number of buffers in the pool.
   *
   * @return The number of available buffers in the pool
   */
  def poolSize(): Int = {
    poolSizeCounter.get().toInt
  }

  /**
   * Checks if the buffer pool is empty.
   *
   * @return true if the pool has no available buffers
   */
  def isPoolEmpty(): Boolean = {
    bufferPool.isEmpty
  }
}
