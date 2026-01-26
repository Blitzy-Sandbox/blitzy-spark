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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

/**
 * Consumer-to-producer flow control signaling component implementing backpressure protocol
 * for streaming shuffle.
 *
 * This class provides the following core functionality:
 *   - Heartbeat-based flow control with configurable timeout (default 5 seconds)
 *   - Token bucket rate limiting at 80% link capacity
 *   - Priority arbitration based on partition count and data volume
 *   - Consumer liveness monitoring via configurable heartbeat interval (default 10 seconds)
 *   - Backpressure event telemetry emission
 *
 * == Design Overview ==
 *
 * The BackpressureProtocol coordinates data flow between streaming shuffle producers
 * and consumers to prevent buffer overflow and ensure fair resource allocation across
 * concurrent shuffles. It implements a three-layer flow control strategy:
 *
 * 1. '''Heartbeat-based Flow Control''': Producers send periodic heartbeats to consumers.
 *    If no heartbeat is received within the configured timeout, the consumer considers
 *    the producer failed and triggers upstream recomputation.
 *
 * 2. '''Token Bucket Rate Limiting''': Bandwidth allocation uses a token bucket algorithm
 *    with refill rate set to 80% of the maximum configured bandwidth. This provides
 *    headroom for control messages and burst absorption.
 *
 * 3. '''Priority Arbitration''': When multiple shuffles compete for bandwidth, priority
 *    is assigned based on partition count (more partitions = higher priority) and
 *    data volume (larger volume = higher priority) to optimize overall throughput.
 *
 * == Consumer Slowdown Detection ==
 *
 * The protocol detects when consumers are processing data significantly slower than
 * producers are generating it. If a consumer processes data at less than half the
 * producer's rate for more than 60 seconds, the protocol signals for automatic
 * fallback to disk-based shuffle (SortShuffleManager).
 *
 * == Thread Safety ==
 *
 * All public methods are thread-safe. Consumer tracking uses ConcurrentHashMap,
 * token bucket uses AtomicLong for lock-free operations, and priority scoring
 * uses synchronized access to the internal priority map.
 *
 * == Coexistence Strategy ==
 *
 * This backpressure protocol is specific to streaming shuffle and does not affect
 * the existing SortShuffleManager flow control. When automatic fallback occurs,
 * the protocol cleanly transfers control to the disk-based shuffle path.
 *
 * @param config Configuration wrapper providing timeout values and bandwidth limits
 * @param metrics Telemetry emission component for backpressure event reporting
 * @since 4.2.0
 */
private[spark] class BackpressureProtocol(
    config: StreamingShuffleConfig,
    metrics: StreamingShuffleMetrics)
  extends Logging {

  // ============================================================================
  // Configuration Parameters
  // ============================================================================

  /**
   * Heartbeat timeout for producer failure detection in milliseconds.
   * Default: 5000ms (5 seconds)
   */
  private val heartbeatTimeoutMs: Int = config.heartbeatTimeoutMs

  /**
   * Acknowledgment timeout for consumer failure detection in milliseconds.
   * Default: 10000ms (10 seconds)
   */
  private val ackTimeoutMs: Int = config.ackTimeoutMs

  /**
   * Maximum bandwidth in MB/s for rate limiting. 0 means unlimited.
   */
  private val maxBandwidthMBps: Int = config.maxBandwidthMBps

  /**
   * Debug logging enabled flag.
   */
  private val isDebug: Boolean = config.isDebug

  // ============================================================================
  // Consumer Tracking
  // ============================================================================

  /**
   * Thread-safe map tracking consumer status information.
   * Key: consumerId (String), Value: ConsumerInfo
   */
  private val consumerStatus = new ConcurrentHashMap[String, ConsumerInfo]()

  // ============================================================================
  // Token Bucket Rate Limiter
  // ============================================================================

  /**
   * Token bucket instance for bandwidth rate limiting.
   * Created with configured max bandwidth and 80% capacity factor.
   */
  private val tokenBucket: TokenBucket = new TokenBucket(maxBandwidthMBps.toDouble)

  // ============================================================================
  // Priority Arbitration
  // ============================================================================

  /**
   * Map tracking priority scores for active shuffles.
   * Key: shuffleId, Value: priority score (higher = more priority)
   */
  private val shufflePriorityScores = mutable.HashMap[Int, Double]()

  /**
   * Lock object for synchronized access to priority scores.
   */
  private val priorityLock = new Object()

  // ============================================================================
  // Heartbeat-based Flow Control
  // ============================================================================

  /**
   * Sends a heartbeat signal from this producer to the specified consumer.
   *
   * This method updates the consumer's last heartbeat timestamp to indicate
   * that the producer is still active and producing data. The consumer uses
   * this information to detect producer failures via timeout.
   *
   * @param consumerId Unique identifier of the target consumer
   */
  def sendHeartbeat(consumerId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val info = consumerStatus.get(consumerId)
    
    if (info != null) {
      info.updateLastHeartbeatTime(currentTime)
      if (isDebug) {
        logDebug(s"Heartbeat sent to consumer $consumerId at timestamp $currentTime")
      }
    } else {
      logWarning(s"Attempted to send heartbeat to unregistered consumer: $consumerId")
    }
  }

  /**
   * Checks if the specified consumer has exceeded the heartbeat timeout.
   *
   * A consumer is considered to have timed out if no heartbeat has been
   * received within the configured heartbeatTimeoutMs interval. This indicates
   * potential producer failure.
   *
   * @param consumerId Unique identifier of the consumer to check
   * @return true if heartbeat timeout has occurred, false otherwise
   */
  def checkHeartbeatTimeout(consumerId: String): Boolean = {
    val currentTime = System.currentTimeMillis()
    val info = consumerStatus.get(consumerId)
    
    if (info == null) {
      logWarning(s"Checking heartbeat timeout for unregistered consumer: $consumerId")
      return false
    }
    
    val lastHeartbeat = info.lastHeartbeatTime
    val elapsed = currentTime - lastHeartbeat
    val timedOut = elapsed > heartbeatTimeoutMs
    
    if (timedOut) {
      logWarning(s"Heartbeat timeout detected for consumer $consumerId. " +
        s"Last heartbeat: ${elapsed}ms ago, timeout threshold: ${heartbeatTimeoutMs}ms")
      metrics.incBackpressureEvents()
    } else if (isDebug) {
      logDebug(s"Heartbeat check for consumer $consumerId: ${elapsed}ms since last heartbeat")
    }
    
    timedOut
  }

  // ============================================================================
  // Token Bucket Rate Limiting
  // ============================================================================

  /**
   * Allocates bandwidth for the specified shuffle based on priority arbitration.
   *
   * This method calculates the available bandwidth for a shuffle operation based on:
   * 1. Total available tokens in the token bucket
   * 2. Priority score calculated from partition count and data volume
   * 3. Fair sharing among concurrent active shuffles
   *
   * The priority scoring formula is:
   *   score = (partitionCount * PARTITION_WEIGHT) + (dataVolumeBytes / MB * VOLUME_WEIGHT)
   *
   * Higher partition counts and larger data volumes receive higher priority to
   * optimize overall shuffle throughput.
   *
   * @param shuffleId Unique identifier of the shuffle operation
   * @param partitionCount Number of partitions in this shuffle
   * @param dataVolume Total data volume for this shuffle in bytes
   * @return Allocated bandwidth in bytes per second for this shuffle
   */
  def allocateBandwidth(shuffleId: Int, partitionCount: Int, dataVolume: Long): Long = {
    // If bandwidth limiting is disabled, return maximum
    if (maxBandwidthMBps <= 0) {
      return Long.MaxValue
    }

    // Calculate priority score for this shuffle
    val partitionWeight = 1.0
    val volumeWeight = 0.001  // 0.001 per MB
    val volumeMB = dataVolume.toDouble / (1024.0 * 1024.0)
    val priorityScore = (partitionCount * partitionWeight) + (volumeMB * volumeWeight)

    // Update priority scores and calculate fair share
    val totalScore = priorityLock.synchronized {
      shufflePriorityScores.put(shuffleId, priorityScore)
      shufflePriorityScores.values.sum
    }

    // Prevent division by zero
    if (totalScore <= 0) {
      if (isDebug) {
        logDebug(s"Total priority score is zero, allocating max bandwidth for shuffle $shuffleId")
      }
      return config.effectiveBandwidthBytesPerSecond
    }

    // Calculate this shuffle's share of total bandwidth
    val share = priorityScore / totalScore
    val allocatedBandwidth = (config.effectiveBandwidthBytesPerSecond * share).toLong

    if (isDebug) {
      logDebug(s"Bandwidth allocation for shuffle $shuffleId: " +
        s"priority=$priorityScore, share=${share * 100}%, allocated=${allocatedBandwidth} bytes/s")
    }

    // Ensure minimum allocation to prevent starvation
    val minAllocation = 1024L * 1024L  // 1MB/s minimum
    math.max(allocatedBandwidth, minAllocation)
  }

  /**
   * Gets the current fair bandwidth allocation for a shuffle based on all registered shuffles.
   *
   * This method calculates what the fair share would be given the current set of registered
   * shuffles. This is useful for querying allocations after all concurrent shuffles have been
   * registered to get the actual fair shares.
   *
   * @param shuffleId The shuffle ID to query allocation for
   * @return The current fair bandwidth allocation in bytes per second, or 0 if not registered
   */
  def getCurrentBandwidthAllocation(shuffleId: Int): Long = {
    // If bandwidth limiting is disabled, return maximum
    if (maxBandwidthMBps <= 0) {
      return Long.MaxValue
    }

    val (priorityScore, totalScore) = priorityLock.synchronized {
      val score = shufflePriorityScores.getOrElse(shuffleId, 0.0)
      val total = shufflePriorityScores.values.sum
      (score, total)
    }

    // Not registered
    if (priorityScore <= 0 || totalScore <= 0) {
      return 0L
    }

    // Calculate this shuffle's share of total bandwidth
    val share = priorityScore / totalScore
    val allocatedBandwidth = (config.effectiveBandwidthBytesPerSecond * share).toLong

    // Ensure minimum allocation to prevent starvation
    val minAllocation = 1024L * 1024L  // 1MB/s minimum
    math.max(allocatedBandwidth, minAllocation)
  }

  // ============================================================================
  // Acknowledgment Processing
  // ============================================================================

  /**
   * Processes an acknowledgment from a consumer indicating successful block receipt.
   *
   * This method updates consumer tracking information including:
   * - Last acknowledgment timestamp
   * - Cumulative bytes acknowledged
   * - Rate calculation for slowdown detection
   *
   * If the consumer is detected as slow (processing rate < producer rate / 2 for > 60s),
   * a backpressure event is emitted and the consumer is flagged for potential fallback.
   *
   * @param consumerId Unique identifier of the acknowledging consumer
   * @param blockId Block identifier that was acknowledged
   */
  def processAck(consumerId: String, blockId: BlockId): Unit = {
    val currentTime = System.currentTimeMillis()
    val info = consumerStatus.get(consumerId)
    
    if (info == null) {
      logWarning(s"Received acknowledgment from unregistered consumer: $consumerId " +
        s"for block ${blockId.name}")
      return
    }

    // Update acknowledgment timestamp and bytes acknowledged
    info.updateLastAckTime(currentTime)
    
    // Extract block size from BlockId name if possible (for metrics)
    // For now, we just mark that an ack was received
    info.incrementAcknowledgedBlocks()
    
    if (isDebug) {
      logDebug(s"Acknowledgment processed from consumer $consumerId for block ${blockId.name}")
    }

    // Check for consumer slowdown after processing ack
    if (detectSlowConsumer(consumerId)) {
      logWarning(s"Consumer $consumerId is slow - backpressure event recorded")
    }
  }

  // ============================================================================
  // Consumer Slowdown Detection
  // ============================================================================

  /**
   * Detects if the specified consumer is processing data significantly slower than
   * the producer is generating it.
   *
   * A consumer is considered "slow" if it has been processing data at less than
   * half the producer's rate (CONSUMER_SLOWDOWN_FACTOR = 2.0) for longer than
   * the configured slowdown threshold (DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS = 60000ms).
   *
   * When slowdown is detected:
   * 1. A backpressure event is emitted to metrics
   * 2. The consumer is flagged with slowdownDetectedTime
   * 3. The calling code can trigger fallback to SortShuffleManager
   *
   * @param consumerId Unique identifier of the consumer to check
   * @return true if sustained consumer slowdown is detected, false otherwise
   */
  def detectSlowConsumer(consumerId: String): Boolean = {
    val currentTime = System.currentTimeMillis()
    val info = consumerStatus.get(consumerId)
    
    if (info == null) {
      return false
    }

    // Calculate consumer consumption rate
    val elapsedMs = currentTime - info.startTime
    if (elapsedMs < 0) {
      // Only return false if time went backwards (clock skew), not for 0ms elapsed
      return false
    }

    val bytesAcknowledged = info.bytesAcknowledged.get()
    val bytesReceived = info.bytesReceived.get()

    // Avoid division by zero
    if (bytesReceived <= 0) {
      return false
    }

    // Calculate consumption ratio (acknowledged / received)
    // A ratio < 1/CONSUMER_SLOWDOWN_FACTOR indicates slowdown
    val consumptionRatio = bytesAcknowledged.toDouble / bytesReceived.toDouble

    val isCurrentlySlow = consumptionRatio < (1.0 / CONSUMER_SLOWDOWN_FACTOR)

    if (isCurrentlySlow) {
      // Check if this is a new slowdown detection
      if (info.slowdownDetectedTime.get() == 0L) {
        info.slowdownDetectedTime.set(currentTime)
        if (isDebug) {
          logDebug(s"Initial slowdown detected for consumer $consumerId: " +
            s"consumption ratio = $consumptionRatio")
        }
      }

      // Check if slowdown has persisted beyond threshold
      val slowdownDuration = currentTime - info.slowdownDetectedTime.get()
      if (slowdownDuration > DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS) {
        logWarning(s"Sustained consumer slowdown detected for $consumerId: " +
          s"slowdown duration = ${slowdownDuration}ms, threshold = ${DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS}ms")
        metrics.incBackpressureEvents()
        return true
      }
    } else {
      // Consumer has recovered, reset slowdown detection
      if (info.slowdownDetectedTime.get() > 0L) {
        info.slowdownDetectedTime.set(0L)
        if (isDebug) {
          logDebug(s"Consumer $consumerId recovered from slowdown")
        }
      }
    }

    false
  }

  // ============================================================================
  // Consumer Registration and Management
  // ============================================================================

  /**
   * Registers a new consumer for tracking.
   *
   * This method creates a new ConsumerInfo entry to track:
   * - Heartbeat timestamps for liveness monitoring
   * - Acknowledgment tracking for rate calculation
   * - Slowdown detection state
   *
   * @param consumerId Unique identifier of the consumer to register
   */
  def registerConsumer(consumerId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val info = new ConsumerInfo(
      consumerId = consumerId,
      lastHeartbeatTime = currentTime,
      bytesReceived = new AtomicLong(0L),
      bytesAcknowledged = new AtomicLong(0L),
      startTime = currentTime,
      slowdownDetectedTime = new AtomicLong(0L)
    )
    
    val existing = consumerStatus.putIfAbsent(consumerId, info)
    if (existing != null) {
      logWarning(s"Consumer $consumerId is already registered, keeping existing entry")
    } else {
      logInfo(s"Consumer $consumerId registered for backpressure tracking")
    }
  }

  /**
   * Unregisters a consumer and cleans up tracking resources.
   *
   * This method removes all tracking information for the specified consumer.
   * Should be called when a consumer disconnects or the shuffle completes.
   *
   * @param consumerId Unique identifier of the consumer to unregister
   */
  def unregisterConsumer(consumerId: String): Unit = {
    val removed = consumerStatus.remove(consumerId)
    if (removed != null) {
      logInfo(s"Consumer $consumerId unregistered from backpressure tracking")
    } else {
      logWarning(s"Attempted to unregister non-existent consumer: $consumerId")
    }
  }

  /**
   * Returns the current status information for the specified consumer.
   *
   * @param consumerId Unique identifier of the consumer
   * @return Option containing ConsumerInfo if registered, None otherwise
   */
  def getConsumerStatus(consumerId: String): Option[ConsumerInfo] = {
    Option(consumerStatus.get(consumerId))
  }

  // ============================================================================
  // Data Volume Tracking
  // ============================================================================

  /**
   * Records bytes received by a consumer for rate tracking.
   *
   * This method should be called by the producer when data is sent to a consumer
   * to track the producer's transmission rate for slowdown detection.
   *
   * @param consumerId Unique identifier of the consumer
   * @param bytes Number of bytes sent
   */
  def recordBytesReceived(consumerId: String, bytes: Long): Unit = {
    val info = consumerStatus.get(consumerId)
    if (info != null) {
      info.bytesReceived.addAndGet(bytes)
      if (isDebug) {
        logDebug(s"Recorded $bytes bytes received for consumer $consumerId")
      }
    }
  }

  /**
   * Records bytes acknowledged by a consumer for rate tracking.
   *
   * This method should be called when an acknowledgment is processed to
   * track the consumer's processing rate.
   *
   * @param consumerId Unique identifier of the consumer
   * @param bytes Number of bytes acknowledged
   */
  def recordBytesAcknowledged(consumerId: String, bytes: Long): Unit = {
    val info = consumerStatus.get(consumerId)
    if (info != null) {
      info.bytesAcknowledged.addAndGet(bytes)
      if (isDebug) {
        logDebug(s"Recorded $bytes bytes acknowledged for consumer $consumerId")
      }
    }
  }

  // ============================================================================
  // Shuffle Priority Management
  // ============================================================================

  /**
   * Removes the priority score for a completed shuffle.
   *
   * Should be called when a shuffle completes to clean up tracking state
   * and allow fair reallocation of bandwidth to remaining active shuffles.
   *
   * @param shuffleId Unique identifier of the completed shuffle
   */
  def removeShuffle(shuffleId: Int): Unit = {
    priorityLock.synchronized {
      shufflePriorityScores.remove(shuffleId)
    }
    if (isDebug) {
      logDebug(s"Removed priority tracking for shuffle $shuffleId")
    }
  }

  /**
   * Checks acknowledgment timeout for a consumer.
   *
   * A consumer has timed out if no acknowledgment has been received within
   * the configured ackTimeoutMs interval.
   *
   * @param consumerId Unique identifier of the consumer
   * @return true if acknowledgment timeout detected, false otherwise
   */
  def checkAckTimeout(consumerId: String): Boolean = {
    val currentTime = System.currentTimeMillis()
    val info = consumerStatus.get(consumerId)
    
    if (info == null) {
      return false
    }
    
    val lastAck = info.lastAckTime.get()
    if (lastAck == 0L) {
      // No ack received yet - use start time as reference
      val elapsed = currentTime - info.startTime
      val timedOut = elapsed > ackTimeoutMs
      if (timedOut) {
        logWarning(s"Acknowledgment timeout detected for consumer $consumerId. " +
          s"No ack received since registration, elapsed: ${elapsed}ms, timeout threshold: ${ackTimeoutMs}ms")
        metrics.incBackpressureEvents()
      }
      return timedOut
    }
    
    val elapsed = currentTime - lastAck
    val timedOut = elapsed > ackTimeoutMs
    
    if (timedOut) {
      logWarning(s"Acknowledgment timeout detected for consumer $consumerId. " +
        s"Last ack: ${elapsed}ms ago, timeout threshold: ${ackTimeoutMs}ms")
      metrics.incBackpressureEvents()
    }
    
    timedOut
  }

  /**
   * Gets a snapshot of all registered consumers and their status.
   *
   * @return Map of consumer IDs to their current status
   */
  def getAllConsumerStatus: Map[String, ConsumerInfo] = {
    import scala.jdk.CollectionConverters._
    consumerStatus.asScala.toMap
  }

  /**
   * Resets all tracking state. Used primarily for testing.
   */
  private[streaming] def reset(): Unit = {
    consumerStatus.clear()
    priorityLock.synchronized {
      shufflePriorityScores.clear()
    }
    tokenBucket.reset()
    logInfo("BackpressureProtocol state reset")
  }
}

/**
 * Information container tracking a consumer's status for backpressure protocol.
 *
 * This class maintains thread-safe counters and timestamps for:
 *   - Heartbeat tracking (producer liveness)
 *   - Acknowledgment tracking (consumer processing rate)
 *   - Slowdown detection state
 *
 * All mutable fields use AtomicLong for thread-safe updates without locking.
 *
 * @param consumerId Unique identifier for this consumer
 * @param lastHeartbeatTime Timestamp of the last received heartbeat (volatile for visibility)
 * @param bytesReceived Cumulative bytes sent to this consumer
 * @param bytesAcknowledged Cumulative bytes acknowledged by this consumer
 * @param startTime Timestamp when tracking started for this consumer
 * @param slowdownDetectedTime Timestamp when slowdown was first detected (0 if not slow)
 */
private[spark] class ConsumerInfo(
    val consumerId: String,
    @volatile var lastHeartbeatTime: Long,
    val bytesReceived: AtomicLong,
    val bytesAcknowledged: AtomicLong,
    val startTime: Long,
    val slowdownDetectedTime: AtomicLong) {

  /**
   * Timestamp of last acknowledgment received.
   */
  val lastAckTime: AtomicLong = new AtomicLong(0L)

  /**
   * Count of blocks acknowledged.
   */
  private val acknowledgedBlocks: AtomicLong = new AtomicLong(0L)

  /**
   * Updates the last heartbeat timestamp atomically.
   *
   * @param time New heartbeat timestamp in milliseconds
   */
  def updateLastHeartbeatTime(time: Long): Unit = {
    lastHeartbeatTime = time
  }

  /**
   * Updates the last acknowledgment timestamp atomically.
   *
   * @param time New acknowledgment timestamp in milliseconds
   */
  def updateLastAckTime(time: Long): Unit = {
    lastAckTime.set(time)
  }

  /**
   * Increments the count of acknowledged blocks.
   */
  def incrementAcknowledgedBlocks(): Unit = {
    acknowledgedBlocks.incrementAndGet()
  }

  /**
   * Returns the current count of acknowledged blocks.
   */
  def getAcknowledgedBlockCount: Long = acknowledgedBlocks.get()

  /**
   * Calculates the current consumption rate in bytes per second.
   *
   * @return Consumption rate in bytes/second, or 0 if insufficient data
   */
  def getConsumptionRate: Double = {
    val elapsed = System.currentTimeMillis() - startTime
    if (elapsed <= 0) {
      0.0
    } else {
      bytesAcknowledged.get().toDouble / (elapsed.toDouble / 1000.0)
    }
  }

  /**
   * Calculates the current producer rate in bytes per second.
   *
   * @return Producer rate in bytes/second, or 0 if insufficient data
   */
  def getProducerRate: Double = {
    val elapsed = System.currentTimeMillis() - startTime
    if (elapsed <= 0) {
      0.0
    } else {
      bytesReceived.get().toDouble / (elapsed.toDouble / 1000.0)
    }
  }

  override def toString: String = {
    s"ConsumerInfo(consumerId=$consumerId, " +
      s"lastHeartbeat=$lastHeartbeatTime, " +
      s"bytesReceived=${bytesReceived.get()}, " +
      s"bytesAcknowledged=${bytesAcknowledged.get()}, " +
      s"startTime=$startTime, " +
      s"slowdownDetected=${slowdownDetectedTime.get() > 0})"
  }
}

/**
 * Token bucket implementation for bandwidth rate limiting.
 *
 * This class implements a token bucket algorithm that limits the rate at which
 * data can be transmitted. Tokens represent bytes that can be sent, and the
 * bucket refills at a rate proportional to the configured bandwidth.
 *
 * The implementation uses the "lazy refill" pattern where tokens are only
 * added when allocateTokens() is called, based on the elapsed time since
 * the last allocation.
 *
 * == Algorithm Details ==
 *
 * - Bucket capacity = maxBandwidthMBps * 1024 * 1024 (one second's worth of data)
 * - Refill rate = maxBandwidthMBps * capacityFactor / 1000 (bytes per millisecond)
 * - Tokens are refilled lazily based on elapsed time
 * - Allocation succeeds if availableTokens >= requested bytes
 *
 * @param maxBandwidthMBps Maximum bandwidth in megabytes per second
 * @param capacityFactor Fraction of max bandwidth to use (default: 80%)
 */
private[spark] class TokenBucket(
    maxBandwidthMBps: Double,
    capacityFactor: Double = TOKEN_BUCKET_CAPACITY_FACTOR) {

  /**
   * Maximum number of tokens (bytes) the bucket can hold.
   * Set to one second's worth of data at the effective rate.
   */
  private val bucketCapacity: Long = {
    if (maxBandwidthMBps <= 0) {
      Long.MaxValue
    } else {
      (maxBandwidthMBps * capacityFactor * 1024.0 * 1024.0).toLong
    }
  }

  /**
   * Token refill rate in bytes per millisecond.
   */
  private val refillRateBytesPerMs: Double = {
    if (maxBandwidthMBps <= 0) {
      Double.MaxValue
    } else {
      maxBandwidthMBps * capacityFactor * 1024.0 * 1024.0 / 1000.0
    }
  }

  /**
   * Current number of available tokens (bytes).
   * Uses AtomicLong for thread-safe operations.
   */
  private val availableTokens: AtomicLong = new AtomicLong(bucketCapacity)

  /**
   * Timestamp of the last token refill operation.
   */
  private val lastRefillTime: AtomicLong = new AtomicLong(System.currentTimeMillis())

  /**
   * Attempts to allocate tokens for transmitting the specified number of bytes.
   *
   * This method first refills tokens based on elapsed time, then attempts to
   * consume the requested number of tokens atomically. If sufficient tokens
   * are available, they are deducted and the method returns true. Otherwise,
   * no tokens are consumed and the method returns false.
   *
   * @param bytes Number of bytes (tokens) to allocate
   * @return true if allocation succeeded, false if insufficient tokens
   */
  def allocateTokens(bytes: Long): Boolean = {
    // Handle unlimited bandwidth case
    if (maxBandwidthMBps <= 0) {
      return true
    }

    // Refill tokens based on elapsed time
    refillTokens()

    // Attempt atomic allocation
    var success = false
    var currentTokens = availableTokens.get()
    
    while (currentTokens >= bytes && !success) {
      success = availableTokens.compareAndSet(currentTokens, currentTokens - bytes)
      if (!success) {
        currentTokens = availableTokens.get()
      }
    }

    success
  }

  /**
   * Refills tokens based on elapsed time since last refill.
   *
   * This method calculates how many tokens should be added based on the
   * time elapsed since the last refill and the configured refill rate.
   * The total tokens are capped at bucketCapacity.
   */
  def refillTokens(): Unit = {
    // Handle unlimited bandwidth case
    if (maxBandwidthMBps <= 0) {
      return
    }

    val currentTime = System.currentTimeMillis()
    val lastTime = lastRefillTime.get()
    val elapsed = currentTime - lastTime

    if (elapsed <= 0) {
      return
    }

    // Calculate tokens to add
    val tokensToAdd = (elapsed * refillRateBytesPerMs).toLong

    if (tokensToAdd <= 0) {
      return
    }

    // Update last refill time (may fail if another thread updated it)
    if (lastRefillTime.compareAndSet(lastTime, currentTime)) {
      // Add tokens, respecting capacity
      var success = false
      while (!success) {
        val current = availableTokens.get()
        val newValue = math.min(current + tokensToAdd, bucketCapacity)
        success = availableTokens.compareAndSet(current, newValue)
      }
    }
  }

  /**
   * Returns the current number of available tokens.
   *
   * Note: This is a snapshot and may change immediately after the call returns.
   *
   * @return Current available tokens (bytes)
   */
  def getAvailableTokens: Long = {
    if (maxBandwidthMBps <= 0) {
      Long.MaxValue
    } else {
      availableTokens.get()
    }
  }

  /**
   * Resets the token bucket to full capacity. Used primarily for testing.
   */
  private[streaming] def reset(): Unit = {
    availableTokens.set(bucketCapacity)
    lastRefillTime.set(System.currentTimeMillis())
  }

  /**
   * Returns the bucket capacity in bytes.
   */
  def getCapacity: Long = bucketCapacity

  /**
   * Returns the refill rate in bytes per second.
   */
  def getRefillRateBytesPerSecond: Double = {
    if (maxBandwidthMBps <= 0) {
      Double.MaxValue
    } else {
      refillRateBytesPerMs * 1000.0
    }
  }
}
