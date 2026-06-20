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

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.streaming.StreamingShuffleConfig

/**
 * A token-bucket rate limiter that throttles the streaming-shuffle producer send path so that a
 * fast producer cannot overwhelm a slower consumer. It is the byte-budget enforcement primitive
 * of the backpressure subsystem: `BackpressureProtocol` calls [[acquire]] on the hot send path
 * before transmitting each block, and the limiter blocks the calling producer thread until enough
 * permits have accrued.
 *
 * ==Token semantics: one permit equals one byte==
 *
 * The bucket is denominated in bytes: requesting `n` permits reserves the right to send `n` bytes.
 * This keeps the limit expressed in the same unit as the configured per-executor bandwidth cap
 * (`spark.shuffle.streaming.maxBandwidthMBps`, converted to bytes/second by
 * [[StreamingShuffleConfig.effectiveBandwidthBytesPerSec]]), so the call site needs no unit
 * conversion.
 *
 * ==Unlimited (zero-overhead) mode==
 *
 * When the configured rate is non-positive or `Long.MaxValue` -- the default, because
 * `maxBandwidthMBps` defaults to `-1` (unlimited) -- the limiter enters "unlimited" mode: it
 * allocates NO underlying Guava limiter, every [[acquire]] is a no-op, and [[tryAcquire]] always
 * succeeds. This keeps the common, untuned cluster path allocating nothing and blocking nothing,
 * so the gate's overhead is effectively zero.
 *
 * ==Bandwidth math lives in one place==
 *
 * The 80% safety factor that leaves headroom for protocol overhead is applied exactly once, inside
 * [[StreamingShuffleConfig.effectiveBandwidthBytesPerSec]]. This class never re-applies it; the
 * companion [[TokenBucketRateLimiter.apply]] factory only divides that already-factored budget
 * across the number of concurrent shuffles sharing the executor's link.
 *
 * ==Thread-safety==
 *
 * [[acquire]] and [[tryAcquire]] may be invoked concurrently from many producer threads. Guava's
 * `RateLimiter` is itself thread-safe, so the steady-state hot path is lock-free: it reads the
 * `@volatile` unlimited flag and a stable `@volatile` limiter reference. The rare reconfiguration
 * performed by [[setBytesPerSecond]] is guarded by `synchronized`. Note that v1 streaming
 * configuration is immutable for the application lifetime (an executor restart is required to
 * change it), so such transitions are not expected at runtime; they are implemented correctly
 * regardless.
 *
 * @param initialBytesPerSecond the initial byte/second ceiling; a non-positive value or
 *                              `Long.MaxValue` selects unlimited mode (no limiter is allocated)
 */
private[spark] class TokenBucketRateLimiter(initialBytesPerSecond: Long) extends Logging {

  // Whether throttling is disabled. Read on the hot path, so kept @volatile; it is mutated only
  // under this-monitor synchronization in setBytesPerSecond.
  @volatile private var unlimited: Boolean =
    TokenBucketRateLimiter.isUnlimitedRate(initialBytesPerSecond)

  // The backing Guava limiter, allocated at construction only when a finite rate applies. In
  // unlimited mode this stays None and no permits machinery is created. Once a limiter has been
  // allocated it is retained (its rate is updated in place), so the hot path always observes a
  // stable reference.
  @volatile private var limiterOpt: Option[GuavaRateLimiter] =
    if (unlimited) None else Some(GuavaRateLimiter.create(initialBytesPerSecond.toDouble))

  // One-time, low-volume construction log; never log per acquire on the hot path (this respects
  // the < 10 MB/hour/executor log budget).
  if (unlimited) {
    logDebug("TokenBucketRateLimiter created in unlimited mode (no throttling, no allocation).")
  } else {
    logDebug(s"TokenBucketRateLimiter created with rate=$initialBytesPerSecond bytes/sec.")
  }

  /**
   * @return `true` when this limiter performs no throttling (unlimited mode), `false` otherwise
   */
  def isUnlimited: Boolean = unlimited

  /**
   * Blocks the calling thread until `bytes` permits (one permit per byte) are available, then
   * consumes them. A non-positive `bytes` request and unlimited mode are both no-ops that return
   * immediately without blocking.
   *
   * @param bytes the number of bytes (permits) to reserve before sending
   */
  def acquire(bytes: Int): Unit = {
    if (bytes > 0 && !unlimited) {
      limiterOpt.foreach(_.acquire(bytes))
    }
  }

  /**
   * Attempts to acquire `bytes` permits without blocking.
   *
   * @param bytes the number of bytes (permits) to reserve before sending
   * @return `true` if the permits were granted immediately (always `true` for a non-positive
   *         request or in unlimited mode); `false` if they could not be granted without waiting
   */
  def tryAcquire(bytes: Int): Boolean = {
    if (bytes <= 0 || unlimited) {
      true
    } else {
      limiterOpt.exists(_.tryAcquire(bytes))
    }
  }

  /**
   * Reconfigures the byte/second ceiling. A non-positive `rate` or `Long.MaxValue` switches the
   * limiter into unlimited mode; any other value installs (or updates in place) the backing Guava
   * limiter. Guarded by `synchronized` because it mutates the unlimited/limiterOpt pair that the
   * lock-free hot path reads.
   *
   * @param rate the new ceiling in bytes/second
   */
  def setBytesPerSecond(rate: Long): Unit = synchronized {
    if (TokenBucketRateLimiter.isUnlimitedRate(rate)) {
      unlimited = true
    } else {
      limiterOpt match {
        case Some(limiter) => limiter.setRate(rate.toDouble)
        case None => limiterOpt = Some(GuavaRateLimiter.create(rate.toDouble))
      }
      unlimited = false
    }
  }

  /**
   * @return the currently configured ceiling in bytes/second, or `Long.MaxValue` when unlimited
   */
  def currentBytesPerSecond: Long = {
    if (unlimited) {
      Long.MaxValue
    } else {
      limiterOpt.map(_.getRate.toLong).getOrElse(Long.MaxValue)
    }
  }
}

/**
 * Factory and predicates for [[TokenBucketRateLimiter]].
 */
private[spark] object TokenBucketRateLimiter {

  /**
   * Classifies a byte/second rate as "no throttling". A non-positive rate (where the unlimited
   * default of `maxBandwidthMBps = -1` resolves) or the `Long.MaxValue` sentinel returned by
   * [[StreamingShuffleConfig.effectiveBandwidthBytesPerSec]] both mean unlimited.
   *
   * @param bytesPerSecond the candidate ceiling in bytes/second
   * @return `true` if the rate should disable throttling entirely
   */
  def isUnlimitedRate(bytesPerSecond: Long): Boolean =
    bytesPerSecond <= 0L || bytesPerSecond == Long.MaxValue

  /**
   * Builds a limiter from the typed streaming configuration, dividing the executor's
   * already-80%-factored bandwidth budget
   * ([[StreamingShuffleConfig.effectiveBandwidthBytesPerSec]]) evenly across the number of
   * concurrent shuffles sharing the link. An unlimited budget stays unlimited regardless of the
   * divisor, and the divisor is clamped to at least one to avoid division by zero.
   *
   * @param config the typed streaming-shuffle configuration providing the bandwidth budget
   * @param numConcurrentShuffles the number of active shuffles sharing the executor bandwidth;
   *                              values below one are treated as one
   * @return a [[TokenBucketRateLimiter]] enforcing the per-shuffle share of the budget
   */
  def apply(
      config: StreamingShuffleConfig,
      numConcurrentShuffles: Int = 1): TokenBucketRateLimiter = {
    val budget = config.effectiveBandwidthBytesPerSec
    val perShuffle =
      if (budget == Long.MaxValue) {
        Long.MaxValue
      } else {
        budget / math.max(1, numConcurrentShuffles)
      }
    new TokenBucketRateLimiter(perShuffle)
  }
}
