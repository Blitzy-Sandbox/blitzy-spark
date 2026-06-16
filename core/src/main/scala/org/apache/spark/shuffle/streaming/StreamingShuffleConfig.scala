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
 * Typed, immutable configuration accessor for the opt-in streaming shuffle backend.
 *
 * This class is the single source of truth for every `spark.shuffle.streaming.*` tuning
 * value and for the streaming-shuffle protocol invariants (block size, timeouts, heartbeat
 * cadence, spill SLA, retry/backoff, bandwidth safety factor, and fallback thresholds).
 * Every other class in the `org.apache.spark.shuffle.streaming` package reads configuration
 * THROUGH this accessor rather than re-reading the underlying
 * [[org.apache.spark.internal.config]] `ConfigEntry` values ad hoc, so the rest of the
 * package stays free of magic numbers and the tuning surface stays consistent.
 *
 * The five user-facing keys are declared once in
 * `core/src/main/scala/org/apache/spark/internal/config/package.scala` and are read here
 * exactly once each into typed `val`s at construction time. Because the configuration is
 * resolved eagerly and never re-read, it is effectively immutable for the lifetime of the
 * application: there is no dynamic reconfiguration in v1, and an executor restart is
 * required to change any streaming-shuffle setting.
 *
 * The accessor is intentionally pure and deterministic. It performs no
 * [[org.apache.spark.SparkEnv]] access and reads nothing beyond the supplied
 * [[org.apache.spark.SparkConf]]; callers that need executor memory or the partition count
 * (for example to size per-partition buffers) pass those values in explicitly. This keeps
 * the type trivially unit-testable without a live Spark runtime.
 *
 * This type coexists with the sort-based shuffle path. Reading these values never engages
 * the streaming backend on its own: the streaming path is active only when
 * `spark.shuffle.manager=streaming` (evaluated by `StreamingShuffleManager`) AND
 * [[enabled]] is `true`. Both signals default to off, so the default behavior of every
 * existing Spark deployment is byte-for-byte unchanged.
 *
 * @param conf the [[org.apache.spark.SparkConf]] to read the streaming-shuffle settings from
 */
private[spark] class StreamingShuffleConfig(conf: SparkConf) {

  /**
   * Opt-in master flag for the streaming shuffle backend (`spark.shuffle.streaming.enabled`,
   * default `false`). When `false`, the streaming manager delegates entirely to the
   * sort-based shuffle. Engaging the streaming path additionally requires
   * `spark.shuffle.manager=streaming`.
   */
  val enabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)

  /**
   * Percentage of executor memory budgeted for per-partition streaming buffers
   * (`spark.shuffle.streaming.bufferSizePercent`, default `20`, valid range `1..50`). The
   * effective per-partition buffer size is derived in [[perPartitionBufferBytes]].
   */
  val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  /**
   * Buffer-utilization percentage that triggers spilling the largest buffered partitions to
   * disk (`spark.shuffle.streaming.spillThreshold`, default `80`, valid range `50..95`). The
   * fractional form is exposed by [[spillThresholdFraction]].
   */
  val spillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  /**
   * Per-executor streaming bandwidth cap in MB/s for the token-bucket rate limiter
   * (`spark.shuffle.streaming.maxBandwidthMBps`, default `-1`). A value of `0` or less means
   * unlimited; see [[isBandwidthUnlimited]] and [[effectiveBandwidthBytesPerSec]].
   */
  val maxBandwidthMBps: Int = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)

  /**
   * Whether verbose streaming-shuffle debug logging is enabled
   * (`spark.shuffle.streaming.debug`, default `false`).
   */
  val debug: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * Defensively re-validates the numeric tuning ranges and throws
   * [[IllegalArgumentException]] with a descriptive message if any value is out of bounds.
   *
   * The primary validation is performed by the `ConfigEntry.checkValue` guards declared in
   * the internal config registry, which reject out-of-range values when the configuration is
   * first read. This method is a belt-and-suspenders check that callers may invoke after
   * construction (for example, immediately before sizing buffers) to fail fast with a clear
   * message rather than propagating a nonsensical derived value.
   */
  def validate(): Unit = {
    require(bufferSizePercent >= 1 && bufferSizePercent <= 50,
      s"spark.shuffle.streaming.bufferSizePercent must be in [1, 50], got $bufferSizePercent")
    require(spillThreshold >= 50 && spillThreshold <= 95,
      s"spark.shuffle.streaming.spillThreshold must be in [50, 95], got $spillThreshold")
  }

  /**
   * Computes the per-partition in-memory buffer size in bytes, applying the canonical
   * streaming-shuffle sizing formula with a hard 2 MB floor.
   *
   * The size is `(executorMemoryBytes * bufferSizePercent / 100) / numPartitions`, floored at
   * [[StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES]] (2 MB) so that even a large partition
   * count or a small memory budget still yields a usable buffer. The partition count is
   * clamped to at least `1` to avoid division by zero, which also makes a non-positive
   * partition count degrade gracefully to the single-buffer sizing.
   *
   * @param executorMemoryBytes total executor memory available for buffering, in bytes
   * @param numPartitions the number of reduce partitions sharing the budget
   * @return the per-partition buffer size in bytes, never below the 2 MB floor
   */
  def perPartitionBufferBytes(executorMemoryBytes: Long, numPartitions: Int): Long = {
    val budget = (executorMemoryBytes * bufferSizePercent / 100) / math.max(1, numPartitions)
    math.max(StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES, budget)
  }

  /**
   * @return `true` when the configured bandwidth cap is non-positive, meaning the streaming
   *         backend imposes no per-executor rate limit.
   */
  def isBandwidthUnlimited: Boolean = maxBandwidthMBps <= 0

  /**
   * Computes the effective per-executor streaming bandwidth in bytes per second.
   *
   * When the cap is unlimited (see [[isBandwidthUnlimited]]) this returns
   * [[scala.Long.MaxValue]] so callers can treat the limiter as effectively uncapped.
   * Otherwise the configured MB/s cap is converted to bytes per second and scaled by
   * [[StreamingShuffleConfig.BANDWIDTH_SAFETY_FACTOR]] (the 80% safety factor), leaving
   * headroom so that bursts do not saturate the link.
   *
   * @return the effective rate in bytes per second, or [[scala.Long.MaxValue]] if unlimited
   */
  def effectiveBandwidthBytesPerSec: Long = {
    if (isBandwidthUnlimited) {
      Long.MaxValue
    } else {
      val bytesPerSec = maxBandwidthMBps.toLong * 1024L * 1024L
      (bytesPerSec * StreamingShuffleConfig.BANDWIDTH_SAFETY_FACTOR).toLong
    }
  }

  /**
   * @return the spill threshold expressed as a fraction in `(0, 1]` (for example `0.8` for
   *         the default `80`), suitable for comparing against a buffer-utilization ratio.
   */
  def spillThresholdFraction: Double = spillThreshold / 100.0

  /**
   * @return whether the streaming feature flag is set. This is a convenience alias for
   *         [[enabled]]; the final activation decision also depends on the
   *         `spark.shuffle.manager=streaming` alias and is made by `StreamingShuffleManager`.
   */
  def streamingActive: Boolean = enabled
}

/**
 * Companion object holding the streaming-shuffle protocol invariants and tuning constants.
 *
 * Centralizing these values here keeps the rest of the `org.apache.spark.shuffle.streaming`
 * package free of magic numbers and guarantees that the writer, reader, buffer, spill
 * manager, backpressure protocol, rate limiter, and wire envelope all agree on the same
 * block size, timeouts, intervals, SLAs, retry policy, bandwidth safety factor, fallback
 * thresholds, RPC endpoint name, and wire-header size.
 */
private[spark] object StreamingShuffleConfig {

  // ---------------------------------------------------------------------------------------
  // Block and buffer sizing.
  // ---------------------------------------------------------------------------------------

  /** Fixed framing block size for streamed and spilled data: 2 MB. */
  val BLOCK_SIZE_BYTES: Int = 2 * 1024 * 1024

  /** Hard floor for a per-partition in-memory buffer: 2 MB. */
  val MIN_BUFFER_SIZE_BYTES: Long = 2L * 1024 * 1024

  // ---------------------------------------------------------------------------------------
  // Reduce-side fetch memory safety (bounds the aggregate payload a single fetched block may
  // assemble, defending against reduce-side memory exhaustion from an oversized/malicious block).
  // ---------------------------------------------------------------------------------------

  /**
   * Multiplicative tolerance applied to a block's advertised (MapStatus) size when bounding the
   * aggregate de-enveloped payload a single fetched block may assemble. MapStatus stores sizes in
   * a lossy compressed form and the de-enveloped payload is always `<=` the enveloped block size,
   * so a modest 1.5x margin absorbs that imprecision while still rejecting a block that streams
   * materially more than advertised (a producer-corruption / memory-exhaustion vector).
   */
  val AGGREGATE_SIZE_TOLERANCE: Double = 1.5

  /**
   * Absolute memory-safety ceiling for a single fetched block's assembled payload, expressed as a
   * fraction of the executor on-heap storage memory. Even when a block's advertised size is bogus
   * or unavailable, the reducer never allocates more than this fraction for one block, bounding the
   * reduce-side memory-exhaustion blast radius; the effective per-block cap is the tighter of this
   * and the tolerance-inflated advertised size.
   */
  val MAX_FETCH_MEMORY_FRACTION: Double = 0.5

  // ---------------------------------------------------------------------------------------
  // Timeouts, heartbeat, and scan intervals (milliseconds).
  // ---------------------------------------------------------------------------------------

  /** Connection timeout bounding producer-liveness detection: 5 s. */
  val CONNECTION_TIMEOUT_MS: Long = 5000

  /** Heartbeat interval driving the backpressure liveness protocol: 10 s. */
  val HEARTBEAT_INTERVAL_MS: Long = 10000

  /** Producer-side timeout for detecting an unresponsive consumer side: 5 s. */
  val PRODUCER_TIMEOUT_MS: Long = 5000

  /** Consumer-side timeout for detecting missing acknowledgements: 10 s. */
  val CONSUMER_TIMEOUT_MS: Long = 10000

  /** Cadence at which the backpressure state machine scans for timeouts: 1 s. */
  val SCAN_INTERVAL_MS: Long = 1000

  // ---------------------------------------------------------------------------------------
  // Spill polling and reclamation SLA (milliseconds).
  // ---------------------------------------------------------------------------------------

  /** Interval at which the spill manager polls buffer utilization: 100 ms. */
  val SPILL_POLL_INTERVAL_MS: Long = 100

  /** Target SLA for reclaiming memory once a spill is triggered: 100 ms. */
  val SPILL_RECLAIM_SLA_MS: Long = 100

  // ---------------------------------------------------------------------------------------
  // Retry policy (exponential backoff).
  // ---------------------------------------------------------------------------------------

  /** Initial backoff for retrying a failed transfer: 1 s, doubled per attempt. */
  val RETRY_INITIAL_BACKOFF_MS: Long = 1000

  /** Maximum number of retry attempts before surfacing a failure: 5. */
  val RETRY_MAX_ATTEMPTS: Int = 5

  // ---------------------------------------------------------------------------------------
  // Bandwidth shaping.
  // ---------------------------------------------------------------------------------------

  /**
   * Fraction of the configured bandwidth actually used by the rate limiter (0.8). The
   * effective bandwidth is 80%-factored to leave headroom for bursts; see
   * [[StreamingShuffleConfig#effectiveBandwidthBytesPerSec]].
   */
  val BANDWIDTH_SAFETY_FACTOR: Double = 0.8

  // ---------------------------------------------------------------------------------------
  // Automatic-fallback thresholds.
  // ---------------------------------------------------------------------------------------

  /** Sustained-slow-consumer window before tripping fallback: 60 s. */
  val SLOW_CONSUMER_THRESHOLD_SECONDS: Long = 60

  /** Consumer-to-producer slowness ratio that counts as "too slow": 2x. */
  val SLOW_CONSUMER_RATIO: Double = 2.0

  /** Memory-utilization percentage that signals OOM risk for fallback: 95%. */
  val MEMORY_PRESSURE_PERCENT: Int = 95

  /** Network-link-utilization percentage that signals saturation for fallback: 90%. */
  val NETWORK_SATURATION_PERCENT: Int = 90

  // ---------------------------------------------------------------------------------------
  // Streaming-protocol version (peer-compatibility / version-mismatch fallback).
  // ---------------------------------------------------------------------------------------

  /**
   * The streaming-shuffle wire/control protocol version this build speaks. It is the single
   * source of truth that `BackpressureProtocol.recordPeerProtocolVersion` compares an observed
   * peer version against: a peer reporting any other value trips the version-mismatch revert
   * condition in `StreamingShuffleFallbackPolicy`, so a mixed-version (for example, mid-rolling-
   * upgrade) cluster automatically reverts to the sort-based path rather than risking an
   * incompatible exchange. It is `1` for this first release and is bumped only when an
   * incompatible change is made to the streaming framing or control messages.
   */
  val STREAMING_PROTOCOL_VERSION: Int = 1

  // ---------------------------------------------------------------------------------------
  // RPC endpoint and wire framing.
  // ---------------------------------------------------------------------------------------

  /**
   * Name under which the executor-only backpressure endpoint is registered on the
   * [[org.apache.spark.rpc.RpcEnv]]. Shared by `BackpressureRpcEndpoint` (registration) and
   * `StreamingShuffleManager` / readers (lookup).
   */
  val BACKPRESSURE_ENDPOINT_NAME: String = "streaming-shuffle-backpressure"

  /** Size of the big-endian streaming block envelope header: 32 bytes. */
  val ENVELOPE_HEADER_BYTES: Int = 32
}
