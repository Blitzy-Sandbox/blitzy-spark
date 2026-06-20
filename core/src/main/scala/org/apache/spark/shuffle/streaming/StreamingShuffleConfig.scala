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
import org.apache.spark.internal.config

/**
 * Typed, validated accessor for the five `spark.shuffle.streaming.*` configuration keys, and the
 * single source of truth for the streaming shuffle backend's tuning values and operational
 * invariants.
 *
 * Every other class in the `org.apache.spark.shuffle.streaming` package obtains its configuration
 * and constants through this type rather than reading [[org.apache.spark.internal.config]]
 * `ConfigEntry`s ad hoc or hard-coding magic numbers. Centralizing access here keeps the rest of
 * the package free of duplicated keys and literals and guarantees one consistent reading of each
 * setting.
 *
 * ==Configuration immutability==
 *
 * Each of the five settings is read exactly once, at construction time, into a typed `val`. The
 * streaming backend does not support dynamic reconfiguration in v1: changing any
 * `spark.shuffle.streaming.*` value requires an executor restart, which naturally yields a fresh
 * instance of this class. Holding the values in immutable `val`s makes that contract explicit.
 *
 * ==Activation contract==
 *
 * The [[enabled]] flag is necessary but not sufficient to engage the streaming path. The backend
 * activates only when BOTH `spark.shuffle.manager=streaming` (selecting this manager through the
 * factory alias) AND `spark.shuffle.streaming.enabled=true` hold. This class reports only the
 * feature flag, via [[streamingActive]]; the manager-alias half of the contract is evaluated by
 * `StreamingShuffleManager`. Both signals default to off, so the default behavior of every
 * existing Spark deployment is unchanged.
 *
 * ==Validation==
 *
 * Range validation is primarily enforced by the `ConfigEntry.checkValue` predicates declared in
 * [[org.apache.spark.internal.config]] (bufferSizePercent in 1..50, spillThreshold in 50..95), so
 * an out-of-range value fails fast when the configuration is first read. [[validate]] re-checks the
 * same ranges as a belt-and-suspenders guard for callers that assemble a [[SparkConf]]
 * programmatically; it is not a substitute for the primary `ConfigEntry` validation.
 *
 * ==Purity==
 *
 * This class is pure and deterministic: it never touches `SparkEnv` or any other global state.
 * Callers that need executor-memory-derived sizing pass the executor memory budget and the
 * partition count into [[perPartitionBufferBytes]] explicitly, which keeps the type trivially
 * testable.
 *
 * @param conf the [[SparkConf]] from which the streaming settings are read once at construction
 */
private[spark] class StreamingShuffleConfig(conf: SparkConf) {

  /** Master opt-in flag (`spark.shuffle.streaming.enabled`); default false. */
  val enabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)

  /** Percent of executor memory for per-partition buffers; 1..50, default 20. */
  val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  /** Buffer-utilization percent that triggers a disk spill; 50..95, default 80. */
  val spillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  /** Per-executor bandwidth cap in MB/s; non-positive (default -1) means unlimited. */
  val maxBandwidthMBps: Int = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)

  /** Verbose debug-logging toggle (`spark.shuffle.streaming.debug`); default false. */
  val debug: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * Defensively re-validates the two ranged settings and throws [[IllegalArgumentException]] on a
   * violation. This duplicates the primary `ConfigEntry.checkValue` enforcement (which remains the
   * authoritative gate) and exists for callers that construct a [[SparkConf]] programmatically and
   * may bypass the `ConfigEntry` predicates.
   *
   * @throws IllegalArgumentException if `bufferSizePercent` is outside 1..50 or `spillThreshold` is
   *                                  outside 50..95
   */
  def validate(): Unit = {
    require(bufferSizePercent >= 1 && bufferSizePercent <= 50,
      "spark.shuffle.streaming.bufferSizePercent must be between 1 and 50, but was " +
        bufferSizePercent)
    require(spillThreshold >= 50 && spillThreshold <= 95,
      "spark.shuffle.streaming.spillThreshold must be between 50 and 95, but was " +
        spillThreshold)
  }

  /**
   * Computes the per-partition in-memory buffer size in bytes, applying the canonical streaming
   * buffer formula `(executorMemoryBytes * bufferSizePercent / 100) / numPartitions` with a hard
   * 2 MB floor ([[StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES]]). A non-positive partition count
   * is clamped to one to avoid division by zero.
   *
   * @param executorMemoryBytes the executor memory budget, in bytes, to apportion across buffers
   * @param numPartitions the number of shuffle partitions sharing the budget
   * @return the per-partition buffer size in bytes, never below the 2 MB floor
   */
  def perPartitionBufferBytes(executorMemoryBytes: Long, numPartitions: Int): Long = {
    val safePartitions = math.max(1, numPartitions)
    val sized = (executorMemoryBytes * bufferSizePercent / 100) / safePartitions
    math.max(StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES, sized)
  }

  /** True when no bandwidth cap applies, i.e. `maxBandwidthMBps <= 0` (the default of -1). */
  def isBandwidthUnlimited: Boolean = maxBandwidthMBps <= 0

  /**
   * The effective per-executor bandwidth ceiling for the token-bucket rate limiter, in bytes per
   * second. When [[isBandwidthUnlimited]] the limiter is effectively disabled and this returns
   * `Long.MaxValue`. Otherwise the configured `maxBandwidthMBps` is converted to bytes per second
   * and scaled by the 80% safety factor ([[StreamingShuffleConfig.BANDWIDTH_SAFETY_FACTOR]]) so the
   * cap leaves headroom for protocol overhead and bursts.
   *
   * @return the 80%-factored bandwidth ceiling in bytes/second, or `Long.MaxValue` if unlimited
   */
  def effectiveBandwidthBytesPerSec: Long = {
    if (isBandwidthUnlimited) {
      Long.MaxValue
    } else {
      val rawBytesPerSec = maxBandwidthMBps.toLong * 1024 * 1024
      (rawBytesPerSec * StreamingShuffleConfig.BANDWIDTH_SAFETY_FACTOR).toLong
    }
  }

  /** The spill threshold expressed as a fraction in `[0.0, 1.0]`; e.g. 80 becomes 0.8. */
  def spillThresholdFraction: Double = spillThreshold / 100.0

  /**
   * Reports whether the streaming feature flag is armed. This is the feature-flag half of the
   * activation contract only; final activation additionally requires `spark.shuffle.manager` to
   * resolve to the streaming manager, which is evaluated by `StreamingShuffleManager`.
   *
   * @return the value of [[enabled]]
   */
  def streamingActive: Boolean = enabled

  override def toString: String =
    s"StreamingShuffleConfig(enabled=$enabled, bufferSizePercent=$bufferSizePercent, " +
      s"spillThreshold=$spillThreshold, maxBandwidthMBps=$maxBandwidthMBps, debug=$debug)"
}

/**
 * Companion object holding the streaming shuffle backend's compile-time invariants. These are the
 * fixed protocol, sizing, timeout, retry, bandwidth, and fallback constants mandated by the design;
 * collaborating classes reference them here so the package contains no duplicated magic numbers.
 */
private[spark] object StreamingShuffleConfig {

  // == Wire and block framing invariants ==

  /** Fixed streaming block size: 2 MB. Map output is framed into blocks of at most this size. */
  val BLOCK_SIZE_BYTES: Int = 2 * 1024 * 1024

  /** Size, in bytes, of the fixed big-endian on-the-wire block header. */
  val ENVELOPE_HEADER_BYTES: Int = 32

  // == Buffering and spill invariants ==

  /** Hard floor for a per-partition in-memory buffer: 2 MB. */
  val MIN_BUFFER_SIZE_BYTES: Long = 2L * 1024 * 1024

  /** Spill manager poll cadence: every 100 ms. */
  val SPILL_POLL_INTERVAL_MS: Long = 100

  /** Memory-reclamation SLA after a spill is triggered: 100 ms. */
  val SPILL_RECLAIM_SLA_MS: Long = 100

  // == Timeout, heartbeat, and retry invariants ==

  /** Connection timeout for a streaming fetch: 5 s. */
  val CONNECTION_TIMEOUT_MS: Long = 5000

  /** Heartbeat interval between consumer and producer: 10 s. */
  val HEARTBEAT_INTERVAL_MS: Long = 10000

  /** Producer-side missing-ack timeout: 5 s. */
  val PRODUCER_TIMEOUT_MS: Long = 5000

  /** Consumer-side missing-data timeout: 10 s. */
  val CONSUMER_TIMEOUT_MS: Long = 10000

  /** Backpressure state-machine scan cadence: every 1 s. */
  val SCAN_INTERVAL_MS: Long = 1000

  /** Initial backoff for retrying a failed streaming operation: 1 s. */
  val RETRY_INITIAL_BACKOFF_MS: Long = 1000

  /** Maximum number of retry attempts under exponential backoff. */
  val RETRY_MAX_ATTEMPTS: Int = 5

  // == Bandwidth and fallback thresholds ==

  /** Safety factor applied to the configured bandwidth cap to leave protocol headroom (80%). */
  val BANDWIDTH_SAFETY_FACTOR: Double = 0.8

  /** A consumer sustained this many seconds slower than its producer trips the fallback. */
  val SLOW_CONSUMER_THRESHOLD_SECONDS: Long = 60

  /** Producer/consumer throughput ratio above which the consumer is deemed too slow. */
  val SLOW_CONSUMER_RATIO: Double = 2.0

  /** Buffer-allocation memory-pressure percentage above which fallback trips (OOM risk). */
  val MEMORY_PRESSURE_PERCENT: Int = 95

  /** Network-link utilization percentage above which fallback trips. */
  val NETWORK_SATURATION_PERCENT: Int = 90

  // == RPC ==

  /** Name under which the executor-only backpressure endpoint registers on the `RpcEnv`. */
  val BACKPRESSURE_ENDPOINT_NAME: String = "streaming-shuffle-backpressure"
}
