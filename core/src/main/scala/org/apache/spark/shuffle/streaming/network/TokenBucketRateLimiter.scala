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
 * Token-bucket rate limiter for the streaming-shuffle producer send path.
 *
 * This is a thin, near-zero-overhead wrapper around Guava's
 * [[com.google.common.util.concurrent.RateLimiter]] that throttles how fast a producer
 * (map-side) executor pushes shuffle bytes toward consumers, so reduce-side consumers are
 * not overwhelmed (the token-bucket half of the streaming-shuffle backpressure protocol).
 * It is constructed by `StreamingShuffleManager` and handed to `BackpressureProtocol`,
 * which calls [[acquire]] on the hot send path; the protocol (not this class) owns the
 * backpressure-event metric, so this type performs no metric accounting and avoids double
 * counting.
 *
 * Semantics:
 *  - '''1 permit = 1 byte.''' [[acquire]] and [[tryAcquire]] request `bytes` permits, so the
 *    configured rate is expressed directly in bytes per second.
 *  - '''Unlimited fast path.''' A non-positive rate (`maxBandwidthMBps <= 0`) or
 *    [[scala.Long.MaxValue]] means "no throttling". In unlimited mode '''no Guava limiter is
 *    allocated''', [[acquire]] is a no-op, and [[tryAcquire]] always succeeds. Because the
 *    default `spark.shuffle.streaming.maxBandwidthMBps` is `-1`, this is the default path and
 *    it allocates nothing and blocks nothing.
 *  - '''Single source of bandwidth math.''' The per-concurrent-shuffle division is applied by
 *    the companion [[TokenBucketRateLimiter.apply]] factory against the already-80%-factored
 *    [[org.apache.spark.shuffle.streaming.StreamingShuffleConfig.effectiveBandwidthBytesPerSec]];
 *    the 80% safety factor lives in `StreamingShuffleConfig` and is never recomputed here.
 *
 * Thread-safety: [[acquire]]/[[tryAcquire]] may be called concurrently from many producer
 * threads. Guava's `RateLimiter` is itself thread-safe; the fast-path reads of [[unlimited]]
 * and `limiterOpt` use `@volatile`, and the rare rate transitions in [[setBytesPerSecond]]
 * are guarded by `synchronized`. The streaming configuration is immutable for the lifetime of
 * the application (no dynamic reconfiguration in v1), so transitions are exceptional rather
 * than routine, but they remain correct under concurrency.
 *
 * @param initialBytesPerSecond the initial throttle in bytes per second; a value `<= 0` or
 *                              [[scala.Long.MaxValue]] selects the unlimited fast path
 */
private[spark] class TokenBucketRateLimiter(initialBytesPerSecond: Long) extends Logging {

  // Whether throttling is currently disabled. Read on the hot path, mutated only under lock.
  @volatile private var unlimited: Boolean =
    TokenBucketRateLimiter.isUnlimitedRate(initialBytesPerSecond)

  // The backing Guava limiter, or None while unlimited. No limiter is allocated in unlimited
  // mode so the default (no-cap) configuration imposes zero allocation and zero blocking.
  @volatile private var limiterOpt: Option[GuavaRateLimiter] =
    if (unlimited) None else Some(GuavaRateLimiter.create(initialBytesPerSecond.toDouble))

  // One construction-time line keeps the configured cap observable without per-acquire
  // logging, which would breach the < 10 MB/hour/executor streaming-shuffle log budget.
  if (unlimited) {
    logDebug("TokenBucketRateLimiter created in unlimited mode; no throttling applied.")
  } else {
    logDebug(s"TokenBucketRateLimiter created with rate=$initialBytesPerSecond bytes/sec.")
  }

  /**
   * @return `true` when no throttling is applied and [[acquire]]/[[tryAcquire]] are no-ops.
   */
  def isUnlimited: Boolean = unlimited

  /**
   * Blocks until `bytes` permits (1 permit = 1 byte) become available, throttling the caller
   * to the configured rate. A non-positive `bytes` or the unlimited fast path returns
   * immediately without contacting the Guava limiter.
   *
   * @param bytes the number of bytes about to be sent; values `<= 0` are ignored
   */
  def acquire(bytes: Int): Unit = {
    if (bytes > 0 && !unlimited) {
      // foreach discards Guava's "seconds slept" Double return without a value-discard warning.
      limiterOpt.foreach(_.acquire(bytes))
    }
  }

  /**
   * Non-blocking attempt to reserve `bytes` permits (1 permit = 1 byte) without sleeping.
   *
   * @param bytes the number of bytes about to be sent; values `<= 0` are treated as always
   *              grantable
   * @return `true` if the permits were granted immediately or throttling is disabled, `false`
   *         if the limiter could not grant them without waiting
   */
  def tryAcquire(bytes: Int): Boolean = {
    if (bytes <= 0 || unlimited) {
      true
    } else {
      limiterOpt.exists(_.tryAcquire(bytes))
    }
  }

  /**
   * Updates the throttle to `rate` bytes per second. A `rate <= 0` or [[scala.Long.MaxValue]]
   * switches to the unlimited fast path; a positive rate updates the existing Guava limiter
   * in place, or lazily allocates one if the limiter was previously unlimited.
   *
   * Guarded by `synchronized` so the [[unlimited]]/`limiterOpt` transition is atomic with
   * respect to concurrent readers. Note v1 configuration is immutable, so this is rarely
   * invoked, but it remains correct if a caller wires dynamic updates in a later version.
   *
   * @param rate the new throttle in bytes per second
   */
  def setBytesPerSecond(rate: Long): Unit = synchronized {
    if (TokenBucketRateLimiter.isUnlimitedRate(rate)) {
      // Leave any existing limiter in place; the volatile flag alone disables the hot path.
      unlimited = true
    } else {
      limiterOpt match {
        case Some(limiter) => limiter.setRate(rate.toDouble)
        case None => limiterOpt = Some(GuavaRateLimiter.create(rate.toDouble))
      }
      // Publish limiterOpt before clearing the flag so readers never see a missing limiter.
      unlimited = false
    }
  }

  /**
   * @return the currently configured throttle in bytes per second, or [[scala.Long.MaxValue]]
   *         when throttling is disabled
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
 *
 * The companion centralizes the "what counts as unlimited" rule and the per-concurrent-shuffle
 * division of the executor bandwidth budget, keeping callers (`StreamingShuffleManager`,
 * `BackpressureProtocol`) free of bandwidth arithmetic.
 */
private[spark] object TokenBucketRateLimiter {

  /**
   * @param bytesPerSecond a candidate throttle in bytes per second
   * @return `true` when the rate represents "no throttling": a non-positive value or
   *         [[scala.Long.MaxValue]] (the sentinel returned by
   *         [[StreamingShuffleConfig.effectiveBandwidthBytesPerSec]] when the cap is unlimited)
   */
  def isUnlimitedRate(bytesPerSecond: Long): Boolean =
    bytesPerSecond <= 0L || bytesPerSecond == Long.MaxValue

  /**
   * Builds a limiter from the typed streaming configuration, dividing the executor's
   * (already 80%-factored) bandwidth budget evenly across the active shuffles.
   *
   * The 80% safety factor is applied once inside
   * [[org.apache.spark.shuffle.streaming.StreamingShuffleConfig.effectiveBandwidthBytesPerSec]]
   * and is deliberately not recomputed here, so the bandwidth math stays in a single place. An
   * unlimited budget ([[scala.Long.MaxValue]]) stays unlimited regardless of the shuffle count.
   *
   * @param config the typed streaming-shuffle configuration accessor
   * @param numConcurrentShuffles the number of concurrent shuffles sharing the budget; clamped
   *                              to at least `1` to avoid division by zero
   * @return a limiter capped at `effectiveBandwidthBytesPerSec / max(1, numConcurrentShuffles)`
   */
  def apply(
      config: StreamingShuffleConfig,
      numConcurrentShuffles: Int = 1): TokenBucketRateLimiter = {
    val budget = config.effectiveBandwidthBytesPerSec
    val perShuffle = if (budget == Long.MaxValue) {
      Long.MaxValue
    } else {
      budget / math.max(1, numConcurrentShuffles)
    }
    new TokenBucketRateLimiter(perShuffle)
  }
}
