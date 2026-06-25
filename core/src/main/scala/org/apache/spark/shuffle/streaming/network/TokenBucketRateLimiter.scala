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

/**
 * A thin, thread-safe token-bucket rate limiter for the streaming shuffle network path where
 * '''one permit == one byte'''. It wraps Guava's `RateLimiter` so that producer-to-consumer
 * throughput can be throttled at byte granularity by the backpressure protocol (F-107).
 *
 * The supplied `maxBytesPerSec` is the ''already-effective'' byte/second cap: the 80%
 * link-capacity factor (see [[TokenBucketRateLimiter.BANDWIDTH_CAP_FACTOR]]) is applied by the
 * companion factory [[TokenBucketRateLimiter.fromMaxBandwidthMBps]], not here. When the cap is
 * non-positive the limiter is a no-op pass-through: no Guava limiter is constructed (a zero-rate
 * limiter would block forever) and every acquisition succeeds immediately.
 *
 * Guava's `RateLimiter` is thread-safe and `limiterOpt` is an immutable `val`, so instances are
 * safe to share across concurrent writer threads without external synchronization.
 *
 * @param maxBytesPerSec the effective permitted rate in bytes/second; `<= 0` means unlimited
 */
private[spark] class TokenBucketRateLimiter(val maxBytesPerSec: Long) extends Logging {

  /**
   * The backing Guava limiter, or `None` when unlimited. `None` is the no-op pass-through mode; a
   * zero-rate Guava limiter is intentionally never constructed because it would block forever.
   */
  private val limiterOpt: Option[GuavaRateLimiter] =
    if (maxBytesPerSec > 0L) {
      Some(GuavaRateLimiter.create(maxBytesPerSec.toDouble))
    } else {
      None
    }

  logDebug(s"TokenBucketRateLimiter created: maxBytesPerSec=$maxBytesPerSec, " +
    s"limited=${limiterOpt.isDefined}")

  /**
   * Whether a real rate cap is in effect (`true`) or the limiter is an unlimited pass-through.
   */
  def isLimited: Boolean = limiterOpt.isDefined

  /**
   * Blocks until `numBytes` permits (one permit per byte) are available.
   *
   * @param numBytes the number of bytes to acquire permits for
   * @return the time spent sleeping in seconds; `0.0` when unlimited or when `numBytes <= 0`
   */
  def acquire(numBytes: Int): Double = {
    if (numBytes <= 0) {
      0.0
    } else {
      limiterOpt match {
        case Some(limiter) => limiter.acquire(numBytes)
        case None => 0.0
      }
    }
  }

  /**
   * Attempts to acquire `numBytes` permits (one permit per byte) without blocking.
   *
   * @param numBytes the number of bytes to acquire permits for
   * @return `true` if the permits were acquired immediately (always `true` when unlimited or when
   *         `numBytes <= 0`); `false` if they could not be acquired without waiting
   */
  def tryAcquire(numBytes: Int): Boolean = {
    if (numBytes <= 0) {
      true
    } else {
      limiterOpt match {
        case Some(limiter) => limiter.tryAcquire(numBytes)
        case None => true
      }
    }
  }

  /**
   * The currently configured stable rate in bytes/second, or `Double.PositiveInfinity` when the
   * limiter is an unlimited pass-through. Intended for diagnostics and tests.
   */
  def currentRate: Double = limiterOpt.map(_.getRate).getOrElse(Double.PositiveInfinity)
}

/**
 * Factories for [[TokenBucketRateLimiter]]. Construction either accepts a precomputed effective
 * bytes/second cap ([[apply]]) or a per-executor bandwidth in MB/s ([[fromMaxBandwidthMBps]]) to
 * which the 80% link-capacity cap and the MB-to-byte conversion are applied.
 */
private[spark] object TokenBucketRateLimiter {

  /** Number of bytes in one mebibyte, used for the MB/s to bytes/s conversion. */
  val BYTES_PER_MB: Long = 1024L * 1024L

  /** Fraction of raw link capacity the limiter is allowed to use (cap at 80%). */
  val BANDWIDTH_CAP_FACTOR: Double = 0.8

  /**
   * Constructs a limiter from a raw effective bytes/second cap.
   *
   * @param maxBytesPerSec the effective permitted rate in bytes/second; `<= 0` means unlimited
   * @return a configured [[TokenBucketRateLimiter]]
   */
  def apply(maxBytesPerSec: Long): TokenBucketRateLimiter =
    new TokenBucketRateLimiter(maxBytesPerSec)

  /**
   * Constructs a limiter from a per-executor maximum bandwidth in MB/s, applying the 80%
   * link-capacity cap and converting MB/s to bytes/s. A non-positive `maxBandwidthMBps` yields an
   * unlimited (no-op) limiter. This mirrors `StreamingShuffleConfig.effectiveBandwidthMBps`
   * (`maxBandwidthMBps * 0.8`).
   *
   * @param maxBandwidthMBps the per-executor bandwidth ceiling in MB/s; `<= 0` means unlimited
   * @return a configured [[TokenBucketRateLimiter]]
   */
  def fromMaxBandwidthMBps(maxBandwidthMBps: Int): TokenBucketRateLimiter = {
    if (maxBandwidthMBps <= 0) {
      new TokenBucketRateLimiter(0L)
    } else {
      val effectiveBytesPerSec =
        (maxBandwidthMBps.toDouble * BANDWIDTH_CAP_FACTOR * BYTES_PER_MB).toLong
      new TokenBucketRateLimiter(effectiveBytesPerSec)
    }
  }
}
