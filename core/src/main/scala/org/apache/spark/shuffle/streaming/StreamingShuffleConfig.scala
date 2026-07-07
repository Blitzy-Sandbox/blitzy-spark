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

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Since
import org.apache.spark.internal.config

/**
 * Typed, read-only accessor for the `spark.shuffle.streaming.*` configuration surface.
 *
 * This class is the single source of truth that every streaming-shuffle component
 * (`StreamingShuffleManager`, `StreamingShuffleWriter`, `StreamingShuffleReader`,
 * `StreamingShuffleFallbackPolicy`, `MemorySpillManager`, `BackpressureProtocol`, and
 * `network.TokenBucketRateLimiter`) consults to resolve its runtime limits. It merely ''reads''
 * the typed [[org.apache.spark.internal.config.ConfigEntry]] definitions registered in the
 * `org.apache.spark.internal.config` package object -- it never redefines a key -- so the
 * defaults and range guards declared there (for example the `[1, 50]` bound on
 * `bufferSizePercent`) are already enforced at read time.
 *
 * '''Immutability.''' Streaming-shuffle configuration is immutable for the lifetime of the
 * application. Version 1 deliberately supports no dynamic reconfiguration: changing any
 * `spark.shuffle.streaming.*` value requires an executor restart. Reading through this accessor
 * (rather than re-parsing the [[SparkConf]] ad hoc) keeps every writer, reader, and buffer
 * created for a shuffle observing one consistent set of limits.
 *
 * '''Sentinels.''' `maxBandwidthMBps == 0` means ''unlimited'' bandwidth (no per-executor rate
 * limit); see [[effectiveBandwidthMBps]] for how that sentinel propagates through the token-bucket
 * refill computation.
 *
 * @param conf the active [[SparkConf]] carrying the resolved streaming-shuffle keys
 */
@Since("4.2.0")
private[spark] class StreamingShuffleConfig(conf: SparkConf) {

  // -- Typed getters -----------------------------------------------------------------------------
  // Each getter delegates to `conf.get(config.<ENTRY>)`, returning the strongly-typed value and
  // relying on the ConfigEntry's own default / checkValue enforcement rather than re-parsing.

  /** Whether the streaming shuffle feature is opted in (`spark.shuffle.streaming.enabled`). */
  def enabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)

  /**
   * Percent of executor memory (1-50, default 20) reserved for per-partition streaming buffers
   * (`spark.shuffle.streaming.bufferSizePercent`). The per-partition cap is later derived by the
   * writer as `(executorMemory * bufferSizePercent / 100) / numPartitions`.
   */
  def bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  /**
   * Buffer-utilization percent (50-95, default 80) at which the [[MemorySpillManager]] spills the
   * largest / least-recently-used partitions to disk (`spark.shuffle.streaming.spillThreshold`).
   */
  def spillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  /**
   * Per-executor streaming rate limit in MB/s (`spark.shuffle.streaming.maxBandwidthMBps`).
   * A value of `0` (the default) is the ''unlimited'' sentinel and disables rate limiting.
   */
  def maxBandwidthMBps: Int = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)

  /** Whether the streaming logger is elevated to DEBUG (`spark.shuffle.streaming.debug`). */
  def debug: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /** The configured shuffle manager (`spark.shuffle.manager`, default `sort`). */
  def shuffleManager: String = conf.get(config.SHUFFLE_MANAGER)

  /**
   * The dual activation gate for the streaming shuffle backend.
   *
   * Streaming shuffle is active if and only if BOTH configuration surfaces agree: the manager is
   * selected via `spark.shuffle.manager=streaming` (matched case-insensitively) AND the feature is
   * explicitly opted in via `spark.shuffle.streaming.enabled=true`. Requiring both is a
   * defense-in-depth opt-in that prevents accidental enablement -- selecting the manager alone, or
   * flipping the `enabled` flag under the default `sort` manager, leaves the production-stable sort
   * path active.
   *
   * @return `true` only when the manager is `streaming` and the feature flag is set
   */
  def isStreamingActive: Boolean =
    shuffleManager.equalsIgnoreCase("streaming") && enabled

  /**
   * Validates the streaming-shuffle configuration invariants.
   *
   * The bounds asserted here intentionally duplicate the `checkValue` guards on the corresponding
   * [[org.apache.spark.internal.config.ConfigEntry]] definitions: they act as a defensive,
   * post-construction cross-check that yields a readable failure (naming the offending key) for
   * tests and for any code path that constructs values outside the normal `conf.get` flow. Note
   * that `maxBandwidthMBps` intentionally has no upper bound and permits `0`, which is the
   * ''unlimited'' sentinel, so the guard here only asserts non-negativity.
   *
   * @throws IllegalArgumentException if any value falls outside its permitted range
   */
  def validate(): Unit = {
    require(bufferSizePercent >= 1 && bufferSizePercent <= 50,
      s"spark.shuffle.streaming.bufferSizePercent must be in [1, 50], but was $bufferSizePercent")
    require(spillThreshold >= 50 && spillThreshold <= 95,
      s"spark.shuffle.streaming.spillThreshold must be in [50, 95], but was $spillThreshold")
    require(maxBandwidthMBps >= 0,
      s"spark.shuffle.streaming.maxBandwidthMBps must be >= 0 (0 means unlimited), " +
        s"but was $maxBandwidthMBps")
  }

  /**
   * Computes the effective per-shuffle bandwidth budget from the preserved user formula
   * `Refill rate = maxBandwidthMBps / numConcurrentShuffles`.
   *
   * The single global `maxBandwidthMBps` link budget is divided evenly across the shuffles that are
   * streaming concurrently on this executor, so that N concurrent shuffles each receive an equal
   * `1/N` share of the token-bucket refill rate consumed by
   * [[org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter]].
   *
   * Special cases:
   *  - If `maxBandwidthMBps == 0` the result is `0`, propagating the ''unlimited'' sentinel so the
   *    rate limiter applies no throttling regardless of concurrency.
   *  - If `numConcurrentShuffles <= 0` the divisor is treated as `1` to avoid division by zero,
   *    yielding the full budget for the single (or as-yet-unknown) shuffle.
   *
   * Integer division is intentional: the refill rate is expressed in whole MB/s and any fractional
   * remainder is discarded, which keeps the aggregate share at or under the configured link budget.
   *
   * @param numConcurrentShuffles the number of shuffles currently streaming on this executor
   * @return the per-shuffle bandwidth budget in MB/s, or `0` when bandwidth is unlimited
   */
  def effectiveBandwidthMBps(numConcurrentShuffles: Int): Int = {
    if (maxBandwidthMBps == 0) {
      // 0 is the unlimited sentinel: no per-executor rate limit is applied.
      0
    } else {
      // Guard against a non-positive concurrency count to avoid division by zero.
      val divisor = if (numConcurrentShuffles <= 0) 1 else numConcurrentShuffles
      maxBandwidthMBps / divisor
    }
  }
}
