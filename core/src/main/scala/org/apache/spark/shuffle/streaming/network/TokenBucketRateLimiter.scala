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

package org.apache.spark.shuffle.streaming.network

import com.google.common.util.concurrent.RateLimiter

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._

/**
 * Thin, thread-safe wrapper around Guava's
 * [[com.google.common.util.concurrent.RateLimiter]] that implements a token-bucket rate
 * limiter for the streaming shuffle feature (F-001).
 *
 * Coexistence strategy: this class is only instantiated when the streaming shuffle manager is
 * active (`spark.shuffle.manager=streaming`) and is loaded lazily on executors. It does not
 * participate in any sort-path control flow; sort-based shuffle keeps its existing per-node
 * throughput characteristics unchanged.
 *
 * Dependency posture: Guava (`com.google.common:guava:33.4.8-jre`) is already a transitive
 * dependency of Spark Core via multiple existing consumers (`MapMaker`, `CacheBuilder`,
 * `ThreadFactoryBuilder`, `Preconditions`, ...). Introducing
 * [[com.google.common.util.concurrent.RateLimiter]] here adds ZERO new Maven coordinates, per
 * AAP section 0.3.1. The Guava package is shaded to `org.sparkproject.guava` at assembly time,
 * so the source-level import remains `com.google.common.util.concurrent.RateLimiter`.
 *
 * Rate semantics:
 *   - [[acquire]] blocks until `permits` tokens are available. One permit == one byte in the
 *     streaming shuffle usage (see
 *     [[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport#sendBlock]]).
 *   - [[setRate]] resets the refill rate at runtime. Guava's
 *     [[com.google.common.util.concurrent.RateLimiter#setRate]] is documented as safe to call
 *     concurrently with [[acquire]].
 *   - [[updateRate]] applies the user-specified 80% link-capacity cap and divides the result
 *     by the concurrent shuffle count, per the formula
 *     `refillRate = maxBandwidthMBps * 1_048_576 / max(1, numConcurrentShuffles) * 0.80`.
 *   - A non-positive `maxBandwidthMBps` (`<= 0`) is interpreted as UNLIMITED; the underlying
 *     limiter is pinned to [[scala.Double#MaxValue]] so that [[acquire]] returns essentially
 *     instantly.
 *
 * Thread safety: Guava's [[com.google.common.util.concurrent.RateLimiter]] is documented as
 * thread-safe. Instances of this class may be shared across map tasks on the same executor
 * without external synchronization.
 *
 * @param initialRateBytesPerSec The initial refill rate in bytes-per-second. A value
 *                               `<= 0.0` is treated as unlimited (pinned to
 *                               [[scala.Double#MaxValue]]).
 */
private[spark] class TokenBucketRateLimiter(initialRateBytesPerSec: Double) extends Logging {

  import TokenBucketRateLimiter._

  /**
   * The underlying Guava limiter. Guava requires the creation rate to be strictly positive;
   * we pass [[scala.Double#MaxValue]] as the sentinel when unlimited was requested.
   */
  private val limiter: RateLimiter =
    RateLimiter.create(effectiveRate(initialRateBytesPerSec))

  /**
   * Blocks the current thread until `permits` tokens are available, then returns.
   *
   * Throws [[IllegalArgumentException]] if `permits <= 0` (mirroring Guava's contract).
   * Logs at DEBUG when a non-trivial wait occurred, honoring the user-specified log-volume
   * cap of 10 MB/hour per executor by avoiding INFO-level emission on the hot path.
   *
   * @param permits The number of tokens to acquire (strictly positive).
   */
  def acquire(permits: Int): Unit = {
    require(permits > 0, s"TokenBucketRateLimiter.acquire requires permits > 0, got $permits.")
    val waitedSec = limiter.acquire(permits)
    if (waitedSec > 0.0) {
      // Convert seconds to milliseconds for structured logging consistency.
      val waitedMs = (waitedSec * 1000.0).toLong
      logDebug(log"TokenBucketRateLimiter waited " +
        log"${MDC(DURATION, waitedMs)} ms for ${MDC(COUNT, permits)} permits.")
    }
  }

  /**
   * Overwrites the refill rate at runtime. A value `<= 0.0` is treated as unlimited
   * ([[scala.Double#MaxValue]] is installed as the sentinel).
   *
   * @param newRate The new refill rate in bytes-per-second.
   */
  def setRate(newRate: Double): Unit = {
    limiter.setRate(effectiveRate(newRate))
  }

  /**
   * Recomputes the refill rate from the user-provided `maxBandwidthMBps` budget, divided
   * across the current count of concurrent shuffles, and capped at the 80% link-capacity
   * factor.
   *
   * Formula (per AAP folder specification):
   * {{{
   *   bytesPerSec = (maxBandwidthMBps.toLong * 1024L * 1024L).toDouble /
   *                   math.max(1, numConcurrentShuffles) * 0.80
   * }}}
   *
   * When `maxBandwidthMBps <= 0`, the limiter is switched to unlimited.
   *
   * @param maxBandwidthMBps      The configured per-executor bandwidth budget in MB/s.
   *                              A value `<= 0` means unlimited.
   * @param numConcurrentShuffles The number of shuffles currently competing for bandwidth.
   *                              Values `<= 0` are clamped to 1 to avoid division-by-zero.
   */
  def updateRate(maxBandwidthMBps: Int, numConcurrentShuffles: Int): Unit = {
    if (maxBandwidthMBps <= 0) {
      setRate(UNLIMITED_RATE)
    } else {
      val shares = math.max(1, numConcurrentShuffles)
      val totalBytesPerSec = maxBandwidthMBps.toLong * BYTES_PER_MB
      val perShuffleBytesPerSec = totalBytesPerSec.toDouble / shares
      val cappedBytesPerSec = perShuffleBytesPerSec * LINK_CAPACITY_FACTOR
      setRate(cappedBytesPerSec)
    }
  }

  /**
   * Returns the current refill rate in bytes-per-second, as reported by the underlying Guava
   * limiter. Useful for telemetry and tests.
   */
  def getRate: Double = limiter.getRate
}

private[spark] object TokenBucketRateLimiter {
  /** Sentinel representing an unlimited rate. Guava requires strictly positive rates, so
   * [[scala.Double#MaxValue]] acts as a proximate "no throttling" value. */
  val UNLIMITED_RATE: Double = Double.MaxValue

  /** Bytes per MB (1024 * 1024). */
  val BYTES_PER_MB: Long = 1024L * 1024L

  /** Link-capacity factor (80%), per user specification: "Transfer rate dynamically adjusted,
   * capped at 80% link capacity". */
  val LINK_CAPACITY_FACTOR: Double = 0.80

  /** Normalizes a user-provided rate into a strictly-positive value Guava will accept. A
   * value `<= 0.0` (e.g., a zero or negative config) is treated as UNLIMITED. */
  @inline private def effectiveRate(rateBytesPerSec: Double): Double =
    if (rateBytesPerSec <= 0.0) UNLIMITED_RATE else rateBytesPerSec
}
