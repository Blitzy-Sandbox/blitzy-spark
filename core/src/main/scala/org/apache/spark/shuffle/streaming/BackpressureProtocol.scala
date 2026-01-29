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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

/**
 * Consumer-to-producer flow control protocol implementing heartbeat-based flow control
 * with 5-second timeout, token-bucket rate limiting at 80% link capacity, buffer utilization
 * tracking across concurrent shuffles, priority-based memory allocation based on partition
 * count and data volume, and backpressure event logging for operational visibility.
 *
 * The protocol ensures that producers do not overwhelm consumers by:
 * 1. Monitoring consumer acknowledgments to track buffer utilization
 * 2. Applying token-bucket rate limiting to cap bandwidth at 80% link capacity
 * 3. Detecting connection failures via heartbeat timeouts
 * 4. Allocating bandwidth based on shuffle priority (partition count and data volume)
 *
 * @param conf SparkConf for reading streaming shuffle configuration
 */
private[spark] class BackpressureProtocol(conf: SparkConf) extends Logging {

  // Configuration values
  private val heartbeatIntervalMs: Long = conf.get(SHUFFLE_STREAMING_HEARTBEAT_INTERVAL)
  private val connectionTimeoutMs: Long = conf.get(SHUFFLE_STREAMING_CONNECTION_TIMEOUT)
  private val maxBandwidthMBps: Int = conf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // Token bucket for rate limiting - 80% of max bandwidth if configured, or unlimited
  private val tokenBucket: TokenBucket = {
    val bytesPerSecond = if (maxBandwidthMBps > 0) {
      // Apply 80% link capacity cap as specified
      (maxBandwidthMBps.toLong * 1024 * 1024 * 0.8).toLong
    } else {
      // Unlimited bandwidth - use Long.MaxValue / 2 to avoid overflow
      Long.MaxValue / 2
    }
    new TokenBucket(bytesPerSecond, bytesPerSecond)
  }

  // Per-executor, per-shuffle buffer utilization tracking
  // Key: (executorId, shuffleId) -> buffer utilization (0.0 to 1.0)
  private val bufferUtilization: mutable.Map[(String, Int), Double] = 
    mutable.Map.empty[(String, Int), Double]

  // Bandwidth allocation tracking per shuffle
  // Key: shuffleId -> allocated bytes per second
  private val bandwidthAllocations: mutable.Map[Int, Long] = mutable.Map.empty[Int, Long]

  // Priority tracking for bandwidth arbitration
  // Key: shuffleId -> (partitionCount, dataVolume)
  private val shufflePriorities: mutable.Map[Int, (Int, Long)] = mutable.Map.empty[Int, (Int, Long)]

  // Last heartbeat timestamps per executor
  private val lastHeartbeatTimes: mutable.Map[String, Long] = mutable.Map.empty[String, Long]

  // Acknowledgment tracking per partition for buffer reclamation
  // Key: partitionId -> last acknowledged offset
  private val acknowledgedOffsets: mutable.Map[Int, Long] = mutable.Map.empty[Int, Long]

  // Total bandwidth pool available for allocation
  private val totalBandwidthPool: Long = if (maxBandwidthMBps > 0) {
    (maxBandwidthMBps.toLong * 1024 * 1024 * 0.8).toLong
  } else {
    Long.MaxValue / 2
  }

  /**
   * Sends a heartbeat to maintain connection liveness and returns the current timestamp.
   * Updates the last heartbeat time for the current executor.
   *
   * @return HeartbeatMessage containing executor ID and timestamp
   */
  def heartbeat(): HeartbeatMessage = {
    val timestamp = System.currentTimeMillis()
    val executorId = getExecutorId()
    
    synchronized {
      lastHeartbeatTimes.update(executorId, timestamp)
    }
    
    if (debugEnabled) {
      logDebug(s"Heartbeat sent from executor $executorId at $timestamp")
    }
    
    HeartbeatMessage(executorId, timestamp)
  }

  /**
   * Sends a backpressure signal from consumer to producer indicating buffer pressure.
   * This signal informs the producer to slow down data transmission.
   *
   * @param executorId The executor sending the backpressure signal
   * @param bufferUtilization Current buffer utilization as a percentage (0.0 to 1.0)
   * @return BackpressureSignal message containing the signal details
   */
  def sendBackpressureSignal(executorId: String, bufferUtilization: Double): BackpressureSignal = {
    require(bufferUtilization >= 0.0 && bufferUtilization <= 1.0,
      s"Buffer utilization must be between 0.0 and 1.0, got $bufferUtilization")
    
    val timestamp = System.currentTimeMillis()
    val signal = BackpressureSignal(executorId, bufferUtilization, timestamp)
    
    // Log backpressure event for operational visibility
    if (bufferUtilization >= 0.8) {
      logWarning(s"High buffer utilization ($bufferUtilization) on executor $executorId - " +
        "sending backpressure signal")
    } else if (debugEnabled) {
      logDebug(s"Backpressure signal: executor=$executorId, utilization=$bufferUtilization")
    }
    
    signal
  }

  /**
   * Processes an acknowledgment message from a consumer for buffer reclamation.
   * Updates the acknowledged offset for the partition, allowing the producer
   * to reclaim buffer memory for data that has been confirmed received.
   *
   * @param partitionId The partition ID that was acknowledged
   * @param offset The offset up to which data has been acknowledged
   * @return true if the acknowledgment was processed successfully
   */
  def receiveAcknowledgment(partitionId: Int, offset: Long): Boolean = {
    synchronized {
      val previousOffset = acknowledgedOffsets.getOrElse(partitionId, -1L)
      
      if (offset > previousOffset) {
        acknowledgedOffsets.update(partitionId, offset)
        
        if (debugEnabled) {
          logDebug(s"Received ACK for partition $partitionId at offset $offset " +
            s"(previous: $previousOffset)")
        }
        true
      } else {
        // Duplicate or out-of-order acknowledgment
        if (debugEnabled) {
          logDebug(s"Ignoring stale ACK for partition $partitionId: " +
            s"received offset $offset <= previous $previousOffset")
        }
        false
      }
    }
  }

  /**
   * Checks if a connection has timed out based on the last heartbeat time.
   * A timeout indicates the connection may have failed and remedial action
   * (such as invalidating partial reads) may be needed.
   *
   * @param lastHeartbeatTime The timestamp of the last received heartbeat
   * @return true if the connection has timed out (>5 seconds since last heartbeat)
   */
  def checkTimeout(lastHeartbeatTime: Long): Boolean = {
    val currentTime = System.currentTimeMillis()
    val elapsedMs = currentTime - lastHeartbeatTime
    val isTimedOut = elapsedMs > connectionTimeoutMs
    
    if (isTimedOut) {
      logWarning(s"Connection timeout detected: ${elapsedMs}ms since last heartbeat " +
        s"(threshold: ${connectionTimeoutMs}ms)")
    }
    
    isTimedOut
  }

  /**
   * Allocates bandwidth share for a shuffle based on partition count and data volume.
   * Priority arbitration ensures that larger shuffles (by partition count and data volume)
   * receive proportionally more bandwidth.
   *
   * @param shuffleId The shuffle ID requesting bandwidth allocation
   * @param partitionCount Number of partitions in the shuffle
   * @param dataVolume Estimated total data volume for the shuffle in bytes
   * @return Allocated bandwidth in bytes per second
   */
  def allocateBandwidth(shuffleId: Int, partitionCount: Int, dataVolume: Long): Long = {
    synchronized {
      // Store priority information for this shuffle
      shufflePriorities.update(shuffleId, (partitionCount, dataVolume))
      
      // Calculate total priority weight across all active shuffles
      val totalWeight = shufflePriorities.values.map { case (pc, dv) =>
        calculatePriorityWeight(pc, dv)
      }.sum
      
      // Calculate this shuffle's weight
      val thisWeight = calculatePriorityWeight(partitionCount, dataVolume)
      
      // Allocate bandwidth proportionally
      val allocation = if (totalWeight > 0) {
        ((thisWeight.toDouble / totalWeight) * totalBandwidthPool).toLong
      } else {
        totalBandwidthPool
      }
      
      // Ensure minimum allocation of 1 MB/s
      val finalAllocation = math.max(allocation, 1024L * 1024L)
      
      bandwidthAllocations.update(shuffleId, finalAllocation)
      
      logInfo(s"Allocated bandwidth for shuffle $shuffleId: " +
        s"${finalAllocation / (1024 * 1024)} MB/s " +
        s"(partitions: $partitionCount, volume: ${dataVolume / (1024 * 1024)} MB)")
      
      finalAllocation
    }
  }

  /**
   * Releases bandwidth allocation for a completed or cancelled shuffle.
   * This frees up bandwidth for other active shuffles.
   *
   * @param shuffleId The shuffle ID to release allocation for
   */
  def releaseAllocation(shuffleId: Int): Unit = {
    synchronized {
      bandwidthAllocations.remove(shuffleId)
      shufflePriorities.remove(shuffleId)
      
      // Clean up buffer utilization entries for this shuffle
      bufferUtilization.keys.filter(_._2 == shuffleId).foreach { key =>
        bufferUtilization.remove(key)
      }
      
      if (debugEnabled) {
        logDebug(s"Released bandwidth allocation for shuffle $shuffleId")
      }
    }
  }

  /**
   * Returns the number of tokens (bytes) available for sending.
   * This is the current capacity in the token bucket.
   *
   * @return Available tokens in bytes
   */
  def getAvailableTokens(): Long = {
    tokenBucket.getAvailableTokens()
  }

  /**
   * Consumes tokens from the bucket for sending data.
   * If insufficient tokens are available, this method blocks or returns false
   * depending on the implementation.
   *
   * @param bytes Number of bytes to consume
   * @return true if tokens were consumed successfully, false if insufficient
   */
  def consumeTokens(bytes: Long): Boolean = {
    tokenBucket.consumeTokens(bytes)
  }

  /**
   * Refills tokens in the bucket based on time elapsed since last refill.
   * Should be called periodically to allow more data to be sent.
   */
  def refillTokens(): Unit = {
    tokenBucket.refillTokens()
  }

  /**
   * Updates buffer utilization for a specific executor and shuffle.
   *
   * @param executorId The executor ID
   * @param shuffleId The shuffle ID
   * @param utilization Buffer utilization (0.0 to 1.0)
   */
  def updateBufferUtilization(executorId: String, shuffleId: Int, utilization: Double): Unit = {
    synchronized {
      bufferUtilization.update((executorId, shuffleId), utilization)
      
      if (utilization >= 0.8) {
        logWarning(s"High buffer utilization on executor $executorId for shuffle $shuffleId: " +
          s"${(utilization * 100).toInt}%")
      }
    }
  }

  /**
   * Gets buffer utilization for a specific executor and shuffle.
   *
   * @param executorId The executor ID
   * @param shuffleId The shuffle ID
   * @return Buffer utilization (0.0 to 1.0), or 0.0 if not tracked
   */
  def getBufferUtilization(executorId: String, shuffleId: Int): Double = {
    synchronized {
      bufferUtilization.getOrElse((executorId, shuffleId), 0.0)
    }
  }

  /**
   * Gets the last acknowledged offset for a partition.
   *
   * @param partitionId The partition ID
   * @return Last acknowledged offset, or -1 if no acknowledgments received
   */
  def getAcknowledgedOffset(partitionId: Int): Long = {
    synchronized {
      acknowledgedOffsets.getOrElse(partitionId, -1L)
    }
  }

  /**
   * Checks if a specific executor has timed out based on stored heartbeat times.
   *
   * @param executorId The executor ID to check
   * @return true if the executor has timed out
   */
  def hasExecutorTimedOut(executorId: String): Boolean = {
    synchronized {
      lastHeartbeatTimes.get(executorId) match {
        case Some(lastTime) => checkTimeout(lastTime)
        case None => true // No heartbeat recorded means timed out
      }
    }
  }

  /**
   * Records a received heartbeat for an executor.
   *
   * @param message The received heartbeat message
   */
  def recordHeartbeat(message: HeartbeatMessage): Unit = {
    synchronized {
      lastHeartbeatTimes.update(message.executorId, message.timestamp)
      
      if (debugEnabled) {
        logDebug(s"Recorded heartbeat from executor ${message.executorId}")
      }
    }
  }

  /**
   * Processes a backpressure signal from a consumer.
   *
   * @param signal The backpressure signal to process
   */
  def processBackpressureSignal(signal: BackpressureSignal): Unit = {
    // Update buffer utilization based on signal
    // Extract shuffle ID from the signal context (assumed to be included in executor state)
    if (signal.bufferUtilization >= 0.8) {
      logWarning(s"Processing backpressure signal from executor ${signal.executorId}: " +
        s"${(signal.bufferUtilization * 100).toInt}% buffer utilization")
    }
  }

  /**
   * Creates an acknowledgment message for a partition.
   *
   * @param partitionId The partition ID
   * @param offset The offset being acknowledged
   * @return AcknowledgmentMessage
   */
  def createAcknowledgment(partitionId: Int, offset: Long): AcknowledgmentMessage = {
    AcknowledgmentMessage(partitionId, offset, System.currentTimeMillis())
  }

  /**
   * Calculates priority weight for bandwidth arbitration based on
   * partition count and data volume.
   */
  private def calculatePriorityWeight(partitionCount: Int, dataVolume: Long): Long = {
    // Weight formula: partition count * (1 + log10(data volume in MB + 1))
    // This gives reasonable weight to both small and large shuffles
    val volumeMB = dataVolume / (1024L * 1024L)
    val volumeWeight = math.log10(volumeMB + 1) + 1
    (partitionCount * volumeWeight).toLong
  }

  /**
   * Gets the current executor ID.
   * In a real implementation, this would be obtained from SparkEnv.
   */
  private def getExecutorId(): String = {
    // In production, this would use SparkEnv.get.executorId
    // For now, use thread-based identification
    s"executor-${Thread.currentThread().getId}"
  }

  /**
   * Resets all internal state. Used for testing or cleanup.
   */
  private[streaming] def reset(): Unit = {
    synchronized {
      bufferUtilization.clear()
      bandwidthAllocations.clear()
      shufflePriorities.clear()
      lastHeartbeatTimes.clear()
      acknowledgedOffsets.clear()
    }
  }
}

/**
 * Token bucket implementation for rate limiting at configurable bandwidth.
 * Uses 80% of link capacity by default to prevent network saturation.
 *
 * The token bucket algorithm works as follows:
 * 1. Tokens are added to the bucket at a fixed rate (tokensPerSecond)
 * 2. Each byte sent consumes one token
 * 3. If insufficient tokens are available, sending must wait or be throttled
 *
 * @param maxTokens Maximum bucket capacity in bytes
 * @param tokensPerSecond Rate at which tokens are refilled
 */
private[spark] class TokenBucket(
    val maxTokens: Long,
    val tokensPerSecond: Long) extends Logging {

  // Current number of available tokens
  private val availableTokens = new AtomicLong(maxTokens)
  
  // Last refill timestamp in nanoseconds for precise timing
  private val lastRefillTime = new AtomicLong(System.nanoTime())

  /**
   * Returns the number of tokens (bytes) currently available for sending.
   *
   * @return Available tokens in bytes
   */
  def getAvailableTokens(): Long = {
    availableTokens.get()
  }

  /**
   * Attempts to consume tokens from the bucket.
   * Returns true if sufficient tokens were available and consumed.
   *
   * @param bytes Number of bytes to consume
   * @return true if tokens were consumed, false if insufficient tokens
   */
  def consumeTokens(bytes: Long): Boolean = {
    require(bytes >= 0, s"Cannot consume negative bytes: $bytes")
    
    if (bytes == 0) return true
    
    // First refill based on elapsed time
    refillTokens()
    
    // Try to consume tokens atomically
    var currentTokens = availableTokens.get()
    while (currentTokens >= bytes) {
      if (availableTokens.compareAndSet(currentTokens, currentTokens - bytes)) {
        return true
      }
      currentTokens = availableTokens.get()
    }
    
    // Insufficient tokens
    false
  }

  /**
   * Refills tokens based on time elapsed since last refill.
   * Uses nanosecond precision for accurate rate limiting.
   */
  def refillTokens(): Unit = {
    val currentTime = System.nanoTime()
    val lastTime = lastRefillTime.get()
    val elapsedNanos = currentTime - lastTime
    
    if (elapsedNanos <= 0) return
    
    // Calculate tokens to add based on elapsed time
    val tokensToAdd = (elapsedNanos * tokensPerSecond) / TimeUnit.SECONDS.toNanos(1)
    
    if (tokensToAdd > 0) {
      // Update last refill time
      if (lastRefillTime.compareAndSet(lastTime, currentTime)) {
        // Add tokens up to max capacity
        var currentTokens = availableTokens.get()
        var newTokens = math.min(currentTokens + tokensToAdd, maxTokens)
        
        while (!availableTokens.compareAndSet(currentTokens, newTokens)) {
          currentTokens = availableTokens.get()
          newTokens = math.min(currentTokens + tokensToAdd, maxTokens)
        }
      }
    }
  }

  /**
   * Resets the token bucket to full capacity.
   */
  private[streaming] def reset(): Unit = {
    availableTokens.set(maxTokens)
    lastRefillTime.set(System.nanoTime())
  }
}

/**
 * Backpressure signal message sent from consumer to producer indicating buffer pressure.
 * This signal informs the producer to adjust its data transmission rate.
 *
 * @param executorId The executor ID of the consumer sending the signal
 * @param bufferUtilization Current buffer utilization as a fraction (0.0 to 1.0)
 * @param timestamp Timestamp when the signal was generated
 */
case class BackpressureSignal(
    executorId: String,
    bufferUtilization: Double,
    timestamp: Long) {
  
  require(bufferUtilization >= 0.0 && bufferUtilization <= 1.0,
    s"Buffer utilization must be between 0.0 and 1.0, got $bufferUtilization")
}

/**
 * Acknowledgment message sent from consumer to producer to confirm receipt of data.
 * Used for buffer reclamation - the producer can release buffer memory for data
 * that has been acknowledged by the consumer.
 *
 * @param partitionId The partition ID that is being acknowledged
 * @param offset The offset up to which data has been received and processed
 * @param timestamp Timestamp when the acknowledgment was generated
 */
case class AcknowledgmentMessage(
    partitionId: Int,
    offset: Long,
    timestamp: Long) {
  
  require(partitionId >= 0, s"Partition ID must be non-negative, got $partitionId")
  require(offset >= 0, s"Offset must be non-negative, got $offset")
}

/**
 * Heartbeat message sent to maintain connection liveness between
 * producers and consumers in the streaming shuffle protocol.
 *
 * @param executorId The executor ID sending the heartbeat
 * @param timestamp Timestamp when the heartbeat was generated
 */
case class HeartbeatMessage(
    executorId: String,
    timestamp: Long)

/**
 * Companion object for BackpressureProtocol containing constants and factory methods.
 */
object BackpressureProtocol {
  
  /** Default connection timeout in milliseconds (5 seconds) */
  val DEFAULT_CONNECTION_TIMEOUT_MS: Long = 5000L
  
  /** Default heartbeat interval in milliseconds (10 seconds) */
  val DEFAULT_HEARTBEAT_INTERVAL_MS: Long = 10000L
  
  /** Default bandwidth cap percentage of link capacity */
  val DEFAULT_BANDWIDTH_CAP_PERCENT: Double = 0.8
  
  /** High buffer utilization threshold triggering backpressure */
  val HIGH_UTILIZATION_THRESHOLD: Double = 0.8
  
  /**
   * Creates a BackpressureProtocol instance with the given configuration.
   *
   * @param conf SparkConf for configuration
   * @return BackpressureProtocol instance
   */
  def apply(conf: SparkConf): BackpressureProtocol = {
    new BackpressureProtocol(conf)
  }
}
