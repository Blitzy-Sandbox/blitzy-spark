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

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.storage.BlockManagerId

/**
 * Token bucket implementation for rate limiting streaming shuffle traffic.
 *
 * Implements a time-based token bucket algorithm that allows burst traffic up to
 * the bucket capacity while limiting sustained throughput to the configured rate.
 * Tokens are refilled based on elapsed time, enabling smooth rate limiting without
 * blocking threads.
 *
 * @param maxBandwidthMBps optional maximum bandwidth in MB/s; None means unlimited
 * @param capacityFactor   fraction of bandwidth allocated as bucket capacity (default 80%)
 */
private[streaming] class TokenBucket(
    maxBandwidthMBps: Option[Int],
    capacityFactor: Double = 0.8) {

  // Capacity in bytes per second, with 80% headroom for burst
  private val capacityBytes: Long = maxBandwidthMBps.map(mbps =>
    (mbps.toLong * 1024L * 1024L * capacityFactor).toLong
  ).getOrElse(Long.MaxValue)

  // Tokens replenish per millisecond
  private val tokensPerMs: Double = capacityBytes / 1000.0

  // Current available tokens (thread-safe)
  private val tokens = new AtomicLong(capacityBytes)

  // Last time tokens were refilled
  private val lastRefillTime = new AtomicLong(System.currentTimeMillis())

  /**
   * Attempt to acquire tokens for sending the specified number of bytes.
   * Non-blocking - returns immediately with success/failure.
   *
   * @param bytes number of bytes to send
   * @return true if tokens were acquired, false if rate limited
   */
  def tryAcquire(bytes: Long): Boolean = {
    if (capacityBytes == Long.MaxValue) return true // Unlimited bandwidth

    refill()
    var acquired = false
    var currentTokens = tokens.get()
    while (currentTokens >= bytes && !acquired) {
      acquired = tokens.compareAndSet(currentTokens, currentTokens - bytes)
      if (!acquired) {
        currentTokens = tokens.get()
      }
    }
    acquired
  }

  /**
   * Release tokens back to the bucket (no-op for time-based token bucket).
   * Kept for API symmetry with other rate limiting implementations.
   *
   * @param bytes number of bytes to release
   */
  def release(bytes: Long): Unit = {
    // No-op: tokens are refilled based on time, not explicit release
  }

  /**
   * Get current token count for monitoring purposes.
   *
   * @return current available tokens
   */
  def availableTokens: Long = {
    refill()
    tokens.get()
  }

  /**
   * Refill tokens based on elapsed time since last refill.
   */
  private def refill(): Unit = {
    val now = System.currentTimeMillis()
    var lastTime = lastRefillTime.get()
    val elapsed = now - lastTime

    if (elapsed > 0) {
      // Try to update the last refill time atomically
      while (!lastRefillTime.compareAndSet(lastTime, now)) {
        lastTime = lastRefillTime.get()
        if (now <= lastTime) return // Another thread already updated
      }

      val newTokens = (elapsed * tokensPerMs).toLong
      tokens.updateAndGet(current => math.min(capacityBytes, current + newTokens))
    }
  }
}

/**
 * State tracking for a single consumer executor in the streaming shuffle protocol.
 *
 * Maintains timestamps and counters for flow control decisions including:
 * - Last heartbeat time for liveness detection
 * - Last acknowledgment time for throughput tracking
 * - Bytes acknowledged and pending for lag calculation
 * - Backpressure signal state
 *
 * @param consumerId the BlockManagerId identifying this consumer
 */
private[streaming] case class ConsumerState(
    consumerId: BlockManagerId,
    lastHeartbeat: AtomicLong = new AtomicLong(System.currentTimeMillis()),
    lastAckTime: AtomicLong = new AtomicLong(System.currentTimeMillis()),
    acknowledgedBytes: AtomicLong = new AtomicLong(0),
    pendingBytes: AtomicLong = new AtomicLong(0),
    backpressureActive: AtomicBoolean = new AtomicBoolean(false),
    lagStartTime: AtomicLong = new AtomicLong(0)) {

  /**
   * Reset lag tracking when consumer catches up.
   */
  def resetLagTracking(): Unit = {
    lagStartTime.set(0)
  }

  /**
   * Mark the start of sustained lag condition.
   */
  def startLagTracking(): Unit = {
    lagStartTime.compareAndSet(0, System.currentTimeMillis())
  }

  /**
   * Get duration of sustained lag, or 0 if not lagging.
   *
   * @return lag duration in milliseconds
   */
  def getLagDuration: Long = {
    val startTime = lagStartTime.get()
    if (startTime > 0) System.currentTimeMillis() - startTime else 0
  }
}

/**
 * Priority information for shuffle ordering during concurrent shuffle execution.
 *
 * @param shuffleId      the shuffle identifier
 * @param partitionCount number of partitions (higher = higher priority)
 * @param dataVolume     estimated data volume in bytes (higher = higher priority)
 */
private[streaming] case class ShufflePriority(
    shuffleId: Int,
    partitionCount: Int,
    dataVolume: Long) extends Comparable[ShufflePriority] {

  /**
   * Compare priorities: higher partition count first, then higher data volume.
   */
  override def compareTo(other: ShufflePriority): Int = {
    val partitionCompare = other.partitionCount.compareTo(this.partitionCount)
    if (partitionCompare != 0) partitionCompare
    else other.dataVolume.compareTo(this.dataVolume)
  }
}

/**
 * Flow control mechanism for streaming shuffle using consumer-to-producer signaling.
 *
 * Implements a comprehensive flow control protocol including:
 * - Token bucket rate limiting at configurable bandwidth (default 80% link capacity)
 * - Heartbeat-based liveness detection with configurable timeout (default 5s)
 * - Per-consumer backpressure signal handling
 * - Priority arbitration for concurrent shuffles based on partition count and data volume
 * - Acknowledgment tracking for buffer reclamation coordination
 *
 * == Thread Safety ==
 *
 * All public methods are thread-safe and can be called concurrently from multiple
 * streaming shuffle writers and readers.
 *
 * == Fallback Detection ==
 *
 * The protocol tracks consumer lag ratio (pending/acknowledged bytes) to detect
 * when a consumer is consistently slower than the producer. If lag ratio exceeds
 * [[FALLBACK_LAG_RATIO_THRESHOLD]] (2x) for more than [[FALLBACK_LAG_DURATION_MS]]
 * (60 seconds), the caller should trigger fallback to sort-based shuffle.
 *
 * @param conf SparkConf containing streaming shuffle configuration
 */
private[spark] class BackpressureProtocol(conf: SparkConf) extends Logging {

  // Configuration values (timeConf returns Long in seconds)
  private val connectionTimeoutMs: Long =
    conf.get(SHUFFLE_STREAMING_CONNECTION_TIMEOUT) * 1000
  private val heartbeatIntervalMs: Long =
    conf.get(SHUFFLE_STREAMING_HEARTBEAT_INTERVAL) * 1000
  private val maxBandwidthMBps: Option[Int] =
    conf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)

  // Rate limiting via token bucket
  private val tokenBucket = new TokenBucket(maxBandwidthMBps)

  // Per-consumer state tracking
  private val consumers = new ConcurrentHashMap[BlockManagerId, ConsumerState]()

  // Per-shuffle priority information
  private val shufflePriorities = new ConcurrentHashMap[Int, ShufflePriority]()

  // Background heartbeat monitoring thread
  @volatile private var heartbeatThread: ScheduledExecutorService = _
  @volatile private var stopped = false

  // Listeners for consumer status changes (optional callbacks)
  @volatile private var consumerDeadCallback: BlockManagerId => Unit = _

  /**
   * Check if sending data to the specified consumer should be throttled.
   *
   * Throttling occurs when:
   * - The consumer has an active backpressure signal
   * - Rate limiting via token bucket denies the request
   * - The consumer's heartbeat is stale (may be dead)
   * - The protocol has been stopped
   *
   * @param consumerId  the target consumer's BlockManagerId
   * @param bytesToSend number of bytes to send
   * @return true if should throttle, false if can proceed
   */
  def shouldThrottle(consumerId: BlockManagerId, bytesToSend: Long): Boolean = {
    if (stopped) return true

    val state = consumers.computeIfAbsent(consumerId, _ => ConsumerState(consumerId))

    // Check if consumer has active backpressure signal
    if (state.backpressureActive.get()) {
      logDebug(s"Throttling due to backpressure signal from $consumerId")
      return true
    }

    // Check token bucket for rate limiting
    if (!tokenBucket.tryAcquire(bytesToSend)) {
      logDebug(s"Throttling due to rate limit for $consumerId (requested $bytesToSend bytes)")
      return true
    }

    // Check for stale heartbeat (consumer may be dead)
    val heartbeatAge = System.currentTimeMillis() - state.lastHeartbeat.get()
    if (heartbeatAge > heartbeatIntervalMs * 2) {
      logWarning(s"Consumer $consumerId heartbeat stale (${heartbeatAge}ms), may be dead")
      return true
    }

    false
  }

  /**
   * Record that bytes have been sent to a consumer (pending acknowledgment).
   *
   * @param consumerId the consumer's BlockManagerId
   * @param bytes      number of bytes sent
   */
  def recordSentBytes(consumerId: BlockManagerId, bytes: Long): Unit = {
    Option(consumers.get(consumerId)).foreach { state =>
      state.pendingBytes.addAndGet(bytes)
      updateLagTracking(state)
    }
  }

  /**
   * Record acknowledgment from a consumer, allowing buffer reclamation.
   *
   * @param consumerId the consumer's BlockManagerId
   * @param bytes      number of bytes acknowledged
   */
  def recordAcknowledgment(consumerId: BlockManagerId, bytes: Long): Unit = {
    Option(consumers.get(consumerId)).foreach { state =>
      state.acknowledgedBytes.addAndGet(bytes)
      state.pendingBytes.addAndGet(-bytes)
      state.lastAckTime.set(System.currentTimeMillis())
      state.backpressureActive.set(false) // Acknowledgment clears backpressure
      updateLagTracking(state)
    }
  }

  /**
   * Handle backpressure signal from a consumer.
   *
   * @param consumerId the consumer's BlockManagerId
   * @param activate   true to activate backpressure, false to deactivate
   */
  def handleBackpressureSignal(consumerId: BlockManagerId, activate: Boolean): Unit = {
    Option(consumers.get(consumerId)).foreach { state =>
      val wasActive = state.backpressureActive.getAndSet(activate)
      if (activate && !wasActive) {
        logInfo(s"Backpressure activated for consumer $consumerId")
      } else if (!activate && wasActive) {
        logDebug(s"Backpressure deactivated for consumer $consumerId")
      }
    }
  }

  /**
   * Record heartbeat from a consumer, indicating it is still alive.
   *
   * @param consumerId the consumer's BlockManagerId
   */
  def recordHeartbeat(consumerId: BlockManagerId): Unit = {
    val state = consumers.computeIfAbsent(consumerId, _ => ConsumerState(consumerId))
    state.lastHeartbeat.set(System.currentTimeMillis())
  }

  /**
   * Check if a consumer is alive based on recent heartbeat.
   *
   * @param consumerId the consumer's BlockManagerId
   * @return true if consumer is alive (recent heartbeat), false otherwise
   */
  def isConsumerAlive(consumerId: BlockManagerId): Boolean = {
    Option(consumers.get(consumerId)).exists { state =>
      val heartbeatAge = System.currentTimeMillis() - state.lastHeartbeat.get()
      heartbeatAge < connectionTimeoutMs
    }
  }

  /**
   * Get the lag ratio for a consumer (pending bytes / acknowledged bytes).
   * Used for fallback detection: ratio > 2.0 indicates consumer is 2x slower.
   *
   * @param consumerId the consumer's BlockManagerId
   * @return lag ratio, or 0.0 if no acknowledgments yet
   */
  def getConsumerLagRatio(consumerId: BlockManagerId): Double = {
    Option(consumers.get(consumerId)).map { state =>
      val acked = state.acknowledgedBytes.get()
      val pending = state.pendingBytes.get()
      if (acked > 0) pending.toDouble / acked else 0.0
    }.getOrElse(0.0)
  }

  /**
   * Get the duration of sustained lag for a consumer.
   *
   * @param consumerId the consumer's BlockManagerId
   * @return lag duration in milliseconds, or 0 if not lagging
   */
  def getConsumerLagDuration(consumerId: BlockManagerId): Long = {
    Option(consumers.get(consumerId)).map(_.getLagDuration).getOrElse(0L)
  }

  /**
   * Check if fallback should be triggered for a consumer.
   * Fallback is recommended when lag ratio exceeds threshold for sustained duration.
   *
   * @param consumerId the consumer's BlockManagerId
   * @return true if fallback to sort-based shuffle is recommended
   */
  def shouldFallback(consumerId: BlockManagerId): Boolean = {
    val lagRatio = getConsumerLagRatio(consumerId)
    val lagDuration = getConsumerLagDuration(consumerId)
    lagRatio >= FALLBACK_LAG_RATIO_THRESHOLD && lagDuration >= FALLBACK_LAG_DURATION_MS
  }

  /**
   * Check if fallback should be triggered for any registered consumer.
   * Returns true if ANY consumer has sustained lag that exceeds thresholds.
   *
   * Used by StreamingShuffleManager.shouldFallbackToSort() to determine
   * if the shuffle should fall back to sort-based shuffle mode.
   *
   * @return true if fallback is recommended due to consumer lag
   */
  def shouldFallbackAny(): Boolean = {
    consumers.values().asScala.exists { state =>
      val lagRatio = getConsumerLagRatio(state.consumerId)
      val lagDuration = getConsumerLagDuration(state.consumerId)
      lagRatio >= FALLBACK_LAG_RATIO_THRESHOLD && lagDuration >= FALLBACK_LAG_DURATION_MS
    }
  }

  /**
   * Get all registered consumer IDs.
   *
   * @return iterable of registered consumer BlockManagerIds
   */
  def getRegisteredConsumerIds: Iterable[BlockManagerId] = {
    consumers.keySet().asScala
  }

  /**
   * Register a shuffle for priority arbitration.
   *
   * @param shuffleId           the shuffle identifier
   * @param partitionCount      number of partitions in this shuffle
   * @param estimatedDataVolume estimated total data volume in bytes
   */
  def registerShuffle(shuffleId: Int, partitionCount: Int, estimatedDataVolume: Long): Unit = {
    shufflePriorities.put(shuffleId, ShufflePriority(shuffleId, partitionCount, estimatedDataVolume))
  }

  /**
   * Unregister a shuffle from priority tracking.
   *
   * @param shuffleId the shuffle identifier
   */
  def unregisterShuffle(shuffleId: Int): Unit = {
    shufflePriorities.remove(shuffleId)
  }

  /**
   * Get the priority rank for a shuffle (lower = higher priority).
   *
   * @param shuffleId the shuffle identifier
   * @return priority rank (0 = highest priority), or Int.MaxValue if not registered
   */
  def getShufflePriority(shuffleId: Int): Int = {
    val priorities = shufflePriorities.values().asScala.toSeq.sorted
    val index = priorities.indexWhere(_.shuffleId == shuffleId)
    if (index >= 0) index else Int.MaxValue
  }

  /**
   * Register a consumer for tracking.
   *
   * @param consumerId the consumer's BlockManagerId
   */
  def registerConsumer(consumerId: BlockManagerId): Unit = {
    consumers.computeIfAbsent(consumerId, _ => ConsumerState(consumerId))
  }

  /**
   * Unregister a consumer (e.g., when it disconnects or the shuffle completes).
   *
   * @param consumerId the consumer's BlockManagerId
   */
  def unregisterConsumer(consumerId: BlockManagerId): Unit = {
    consumers.remove(consumerId)
  }

  /**
   * Set callback for consumer death notification.
   *
   * @param callback function called when a consumer is detected as dead
   */
  def setConsumerDeadCallback(callback: BlockManagerId => Unit): Unit = {
    consumerDeadCallback = callback
  }

  /**
   * Start the background heartbeat monitoring thread.
   * Should be called once when the protocol is initialized.
   */
  def startHeartbeatMonitor(): Unit = {
    if (heartbeatThread == null) {
      heartbeatThread = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactory {
          override def newThread(r: Runnable): Thread = {
            val t = new Thread(r, "streaming-shuffle-heartbeat-monitor")
            t.setDaemon(true)
            t
          }
        })
      heartbeatThread.scheduleAtFixedRate(
        () => checkConsumerLiveness(),
        heartbeatIntervalMs,
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS)
      logInfo(s"Started heartbeat monitor with ${heartbeatIntervalMs}ms interval")
    }
  }

  /**
   * Stop the backpressure protocol and release resources.
   */
  def stop(): Unit = {
    stopped = true
    if (heartbeatThread != null) {
      heartbeatThread.shutdown()
      try {
        if (!heartbeatThread.awaitTermination(5, TimeUnit.SECONDS)) {
          heartbeatThread.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          heartbeatThread.shutdownNow()
          Thread.currentThread().interrupt()
      }
      heartbeatThread = null
    }
    consumers.clear()
    shufflePriorities.clear()
    logInfo("BackpressureProtocol stopped")
  }

  /**
   * Check liveness of all registered consumers and notify callback for dead ones.
   */
  private def checkConsumerLiveness(): Unit = {
    if (stopped) return

    try {
      val now = System.currentTimeMillis()
      consumers.values().asScala.foreach { state =>
        val heartbeatAge = now - state.lastHeartbeat.get()
        if (heartbeatAge > connectionTimeoutMs) {
          logWarning(s"Consumer ${state.consumerId} appears dead " +
            s"(no heartbeat for ${heartbeatAge}ms)")
          Option(consumerDeadCallback).foreach(_(state.consumerId))
        }
      }
    } catch {
      case e: Exception =>
        logWarning("Error during consumer liveness check", e)
    }
  }

  /**
   * Update lag tracking for a consumer based on current state.
   */
  private def updateLagTracking(state: ConsumerState): Unit = {
    val acked = state.acknowledgedBytes.get()
    val pending = state.pendingBytes.get()

    if (acked > 0) {
      val lagRatio = pending.toDouble / acked
      if (lagRatio >= FALLBACK_LAG_RATIO_THRESHOLD) {
        state.startLagTracking()
      } else {
        state.resetLagTracking()
      }
    }
  }
}
