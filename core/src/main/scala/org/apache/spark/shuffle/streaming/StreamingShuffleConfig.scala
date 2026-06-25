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
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

/**
 * Typed, immutable accessor over the five `spark.shuffle.streaming.*` configuration entries.
 *
 * This class centralizes every read of the streaming shuffle tuning knobs, the defensive range
 * validation, and the effective-bandwidth computation, so that the rest of the subsystem (the
 * manager, writer, spill manager, and fallback policy) consumes a single, strongly typed view of
 * the configuration rather than threading the raw `org.apache.spark.internal.config` entries
 * through every collaborator.
 *
 * Every value is read once from the supplied [[SparkConf]] at construction time and then held
 * immutably: streaming shuffle configuration is fixed for the lifetime of the application and is
 * never reconfigured dynamically in this version. Because each backing
 * [[org.apache.spark.internal.config.ConfigEntry]] declares a default, the accessors always
 * resolve to a concrete value (the configured value when present, otherwise the entry default).
 *
 * The dual-flag activation contract for streaming shuffle is
 * `spark.shuffle.manager=streaming` (resolved by the shuffle-manager alias map) together
 * with `spark.shuffle.streaming.enabled=true` (exposed here as [[enabled]]); the manager
 * consults [[enabled]] before routing a shuffle onto the streaming data path.
 *
 * @param conf the application [[SparkConf]] from which the streaming shuffle settings are read
 */
private[spark] class StreamingShuffleConfig(conf: SparkConf) extends Logging {

  /**
   * Whether the opt-in streaming shuffle data path is enabled
   * (`spark.shuffle.streaming.enabled`, default `false`). This is one half of the dual-flag
   * activation contract; streaming engages only when this flag is `true` '''and''' the streaming
   * manager is selected ([[managerSelected]]). See [[active]].
   */
  val enabled: Boolean = conf.get(STREAMING_SHUFFLE_ENABLED)

  /**
   * Whether the streaming shuffle manager is the selected shuffle manager, i.e. whether
   * `spark.shuffle.manager` resolves to the streaming short-name alias
   * ([[StreamingShuffleConfig.STREAMING_MANAGER_ALIAS]], `"streaming"`). This is the second half
   * of the dual-flag activation contract; see [[active]]. Selection by fully-qualified class name
   * leaves this `false`, so the streaming data path stays disengaged and every shuffle is
   * delegated to the inner sort-based manager (see decision log ADR-02).
   */
  val managerSelected: Boolean =
    conf.get(SHUFFLE_MANAGER) == StreamingShuffleConfig.STREAMING_MANAGER_ALIAS

  /**
   * Whether the streaming shuffle data path is active under the dual-flag activation contract:
   * the streaming manager must be selected via the `"streaming"` alias ([[managerSelected]])
   * '''and''' the opt-in flag must be enabled ([[enabled]]). When either half is absent the
   * streaming manager delegates every shuffle to the inner sort-based manager, so the default
   * behavior is provably identical to plain sort-based shuffle.
   *
   * @return `true` iff both `spark.shuffle.manager=streaming` and
   *         `spark.shuffle.streaming.enabled=true`
   */
  def active: Boolean = managerSelected && enabled

  /**
   * Percentage of executor memory budgeted for streaming shuffle per-partition buffers
   * (`spark.shuffle.streaming.bufferSizePercent`, default `20`, valid range [1, 50]).
   */
  val bufferSizePercent: Int = conf.get(STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)

  /**
   * Buffer-utilization percentage at which the spill-to-disk path is triggered
   * (`spark.shuffle.streaming.spillThreshold`, default `80`, valid range [50, 95]).
   */
  val spillThreshold: Int = conf.get(STREAMING_SHUFFLE_SPILL_THRESHOLD)

  /**
   * Per-executor streaming shuffle bandwidth ceiling in MB/s
   * (`spark.shuffle.streaming.maxBandwidthMBps`, default `0`). A value of `0` (or any
   * non-positive value) denotes unlimited bandwidth.
   */
  val maxBandwidthMBps: Int = conf.get(STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS)

  /**
   * Whether verbose debug logging for the streaming shuffle subsystem is enabled
   * (`spark.shuffle.streaming.debug`, default `false`).
   */
  val debug: Boolean = conf.get(STREAMING_SHUFFLE_DEBUG)

  /**
   * The effective streaming bandwidth ceiling in MB/s after applying the 80%-of-link-capacity
   * cap (see [[StreamingShuffleConfig.BANDWIDTH_CAP_FACTOR]]).
   *
   * Returns `maxBandwidthMBps * 0.8` when a positive limit is configured. When
   * [[maxBandwidthMBps]] is `0` (the default) or any non-positive value, bandwidth is unlimited
   * and this method returns `0.0` as the sentinel for "unlimited", mirroring the no-op behavior
   * of the streaming token-bucket rate limiter.
   *
   * @return the capped effective bandwidth in MB/s, or `0.0` to denote unlimited bandwidth
   */
  def effectiveBandwidthMBps: Double = {
    if (maxBandwidthMBps <= 0) {
      0.0
    } else {
      maxBandwidthMBps * StreamingShuffleConfig.BANDWIDTH_CAP_FACTOR
    }
  }

  /**
   * Defensively re-validates the configured ranges and logs the resolved configuration.
   *
   * The backing [[org.apache.spark.internal.config.ConfigEntry]] definitions already enforce
   * these ranges via `checkValue`, so this method is a belt-and-suspenders guard for callers that
   * construct or mutate a [[SparkConf]] programmatically (bypassing the typed builders). It
   * throws `IllegalArgumentException` when:
   *  - [[bufferSizePercent]] is outside [1, 50], or
   *  - [[spillThreshold]] is outside [50, 95].
   *
   * On success, and only when [[debug]] is enabled, the resolved configuration is logged at debug
   * level.
   *
   * @throws IllegalArgumentException if any tuning value falls outside its permitted range
   */
  def validate(): Unit = {
    require(
      bufferSizePercent >= StreamingShuffleConfig.MIN_BUFFER_SIZE_PERCENT &&
        bufferSizePercent <= StreamingShuffleConfig.MAX_BUFFER_SIZE_PERCENT,
      s"${STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT.key} must be in " +
        s"[${StreamingShuffleConfig.MIN_BUFFER_SIZE_PERCENT}, " +
        s"${StreamingShuffleConfig.MAX_BUFFER_SIZE_PERCENT}] but was $bufferSizePercent")
    require(
      spillThreshold >= StreamingShuffleConfig.MIN_SPILL_THRESHOLD &&
        spillThreshold <= StreamingShuffleConfig.MAX_SPILL_THRESHOLD,
      s"${STREAMING_SHUFFLE_SPILL_THRESHOLD.key} must be in " +
        s"[${StreamingShuffleConfig.MIN_SPILL_THRESHOLD}, " +
        s"${StreamingShuffleConfig.MAX_SPILL_THRESHOLD}] but was $spillThreshold")
    if (debug) {
      logDebug(
        "Streaming shuffle configuration resolved: " +
          s"enabled=$enabled, bufferSizePercent=$bufferSizePercent, " +
          s"spillThreshold=$spillThreshold, maxBandwidthMBps=$maxBandwidthMBps " +
          s"(effectiveBandwidthMBps=$effectiveBandwidthMBps), debug=$debug")
    }
  }
}

/**
 * Factory and shared range constants for [[StreamingShuffleConfig]].
 */
private[spark] object StreamingShuffleConfig {

  /**
   * The `spark.shuffle.manager` short-name alias that selects the streaming shuffle manager. This
   * is the value the shuffle-manager alias map resolves to
   * `org.apache.spark.shuffle.streaming.StreamingShuffleManager`, and the only selection form
   * that engages the streaming data path (see [[StreamingShuffleConfig.managerSelected]]).
   */
  val STREAMING_MANAGER_ALIAS: String = "streaming"

  /** Fraction of raw link capacity the streaming data path is permitted to use (80%). */
  val BANDWIDTH_CAP_FACTOR: Double = 0.8

  /** Minimum permitted value for `spark.shuffle.streaming.bufferSizePercent`. */
  val MIN_BUFFER_SIZE_PERCENT: Int = 1

  /** Maximum permitted value for `spark.shuffle.streaming.bufferSizePercent`. */
  val MAX_BUFFER_SIZE_PERCENT: Int = 50

  /** Minimum permitted value for `spark.shuffle.streaming.spillThreshold`. */
  val MIN_SPILL_THRESHOLD: Int = 50

  /** Maximum permitted value for `spark.shuffle.streaming.spillThreshold`. */
  val MAX_SPILL_THRESHOLD: Int = 95

  /**
   * Constructs a [[StreamingShuffleConfig]] from the supplied [[SparkConf]].
   *
   * @param conf the application [[SparkConf]] from which the streaming shuffle settings are read
   * @return a new, immutable [[StreamingShuffleConfig]]
   */
  def apply(conf: SparkConf): StreamingShuffleConfig = new StreamingShuffleConfig(conf)
}
