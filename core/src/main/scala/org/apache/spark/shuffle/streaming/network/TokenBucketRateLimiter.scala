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

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging

/**
 * Per-executor, byte-granular rate limiter for streaming-shuffle transfers.
 *
 * This is a thin wrapper over Guava's `com.google.common.util.concurrent.RateLimiter` (Guava is
 * already a Spark Core dependency, so no new library is introduced -- Architectural Decision Log
 * #4). The wrapper fixes the token semantics as **one permit equals one byte** and derives the
 * refill rate from the operator-configured bandwidth budget:
 *
 * {{{
 *   effective bytes/sec = (maxBandwidthMBps * 0.80 / numConcurrentShuffles) * 1024 * 1024
 * }}}
 *
 * The 0.80 factor keeps the executor below ~80% of the link capacity (leaving headroom that the
 * fallback policy treats as saturation), and dividing by the number of concurrent shuffles fairly
 * shares the budget across shuffles running on the same executor.
 *
 * When `maxBandwidthMBps == 0` (the default, meaning "unlimited"), the limiter becomes a
 * pass-through: [[acquire]] returns immediately and [[tryAcquire]] always succeeds. A defensive
 * treatment of any non-positive value as unlimited avoids a Guava `IllegalArgumentException` from
 * a zero/negative rate.
 *
 * Isolation: this class lives entirely in the streaming `network` subpackage. It is constructed by
 * `StreamingShuffleManager` and consumed by `BackpressureProtocol`, which calls [[setRate]] to
 * apply a dynamic throttle in response to consumer acknowledgment pressure. It has no coupling to,
 * and no effect on, the existing sort-based shuffle code path.
 *
 * @param maxBandwidthMBps    per-executor bandwidth budget in MB/s; 0 (or any non-positive value)
 *                            means unlimited (pass-through)
 * @param numConcurrentShuffles number of shuffles sharing the executor budget (floored at 1)
 */
@Since("4.2.0")
private[spark] class TokenBucketRateLimiter(
    maxBandwidthMBps: Int,
    numConcurrentShuffles: Int) extends Logging {

  // Unlimited (pass-through) when no positive bandwidth budget is configured.
  private val unlimited: Boolean = maxBandwidthMBps <= 0

  // Guava RateLimiter with 1 permit == 1 byte. Absent (None) in unlimited pass-through mode so we
  // never throttle and never hand Guava a non-positive rate.
  private val limiter: Option[GuavaRateLimiter] =
    if (unlimited) {
      None
    } else {
      val bytesPerSecond =
        TokenBucketRateLimiter.effectiveBytesPerSecond(maxBandwidthMBps, numConcurrentShuffles)
      Some(GuavaRateLimiter.create(bytesPerSecond))
    }

  /**
   * Acquire `numBytes` permits, blocking until they are available under the current rate. Returns
   * the time spent sleeping in seconds (0.0 when nothing was throttled, including unlimited mode
   * and non-positive requests).
   */
  def acquire(numBytes: Int): Double = {
    if (numBytes <= 0 || limiter.isEmpty) {
      0.0
    } else {
      limiter.get.acquire(numBytes)
    }
  }

  /**
   * Try to acquire `numBytes` permits without blocking. Returns true if the permits were granted
   * immediately (always true in unlimited mode or for a non-positive request), false otherwise.
   */
  def tryAcquire(numBytes: Int): Boolean = {
    if (numBytes <= 0 || limiter.isEmpty) {
      true
    } else {
      limiter.get.tryAcquire(numBytes)
    }
  }

  /**
   * Update the throttle to `bytesPerSecond`. Invoked by `BackpressureProtocol` when a consumer
   * requests a dynamic throttle. In unlimited pass-through mode there is no limiter to adjust, so
   * the request is ignored (logged at debug); throttling remains off until the executor restarts
   * with a bandwidth budget, matching the v1 "no dynamic reconfiguration" constraint.
   */
  def setRate(bytesPerSecond: Double): Unit = {
    require(bytesPerSecond > 0,
      s"bytesPerSecond must be positive but was $bytesPerSecond")
    limiter match {
      case Some(l) =>
        l.setRate(bytesPerSecond)
        logDebug(s"Streaming shuffle rate limiter updated to $bytesPerSecond bytes/sec")
      case None =>
        logDebug("Ignoring setRate: rate limiter is in unlimited pass-through mode " +
          "(spark.shuffle.streaming.maxBandwidthMBps=0)")
    }
  }
}

/**
 * Companion object holding the link-capacity factor and the refill-rate computation shared by the
 * limiter constructor and any caller that needs to reason about the effective byte rate.
 */
@Since("4.2.0")
private[spark] object TokenBucketRateLimiter {

  /** Fraction of the configured link capacity the limiter is allowed to use (leaves headroom). */
  val LINK_CAPACITY_FACTOR: Double = 0.80

  // 1 MB expressed in bytes; permits are counted in bytes (1 permit == 1 byte).
  private val BYTES_PER_MB: Long = 1024L * 1024L

  /**
   * Compute the effective refill rate in bytes/second from the configured bandwidth budget:
   * `(maxBandwidthMBps * LINK_CAPACITY_FACTOR / numConcurrentShuffles) * 1024 * 1024`. The shuffle
   * count is floored at 1 so a zero/negative caller value cannot divide by zero.
   */
  def effectiveBytesPerSecond(maxBandwidthMBps: Int, numConcurrentShuffles: Int): Double = {
    val shuffles = math.max(1, numConcurrentShuffles)
    (maxBandwidthMBps * LINK_CAPACITY_FACTOR / shuffles) * BYTES_PER_MB
  }
}
