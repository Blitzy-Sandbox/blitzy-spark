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
import org.apache.spark.internal.config.ConfigBuilder

/**
 * Configuration wrapper class that parses and validates all spark.shuffle.streaming.* parameters.
 * 
 * This class provides typed accessors for streaming shuffle configuration parameters,
 * integrating with Spark's ConfigEntry system for proper validation and documentation.
 * All configuration changes require executor restart (no dynamic reconfiguration in v1).
 *
 * Configuration Parameters:
 * - spark.shuffle.streaming.enabled: Enable streaming shuffle mode (default: false)
 * - spark.shuffle.streaming.bufferSizePercent: Buffer size as percentage of executor memory (1-50, default: 20)
 * - spark.shuffle.streaming.spillThreshold: Buffer utilization percentage to trigger spill (50-95, default: 80)
 * - spark.shuffle.streaming.maxBandwidthMBps: Maximum bandwidth in MB/s (0 = unlimited, default: 0)
 * - spark.shuffle.streaming.heartbeatTimeoutMs: Heartbeat timeout for producer failure detection (default: 5000)
 * - spark.shuffle.streaming.ackTimeoutMs: Acknowledgment timeout for consumer failure detection (default: 10000)
 * - spark.shuffle.streaming.debug: Enable debug logging for streaming shuffle events (default: false)
 *
 * Coexistence Strategy:
 * The streaming shuffle configuration coexists with existing SortShuffleManager configuration.
 * When spark.shuffle.streaming.enabled is false (default), the streaming shuffle feature is
 * completely disabled and has zero impact on existing deployments. This ensures backward
 * compatibility and allows opt-in activation.
 *
 * @see [[StreamingShuffleManager]] for usage of these configuration parameters
 * @since 4.2.0
 */
private[spark] class StreamingShuffleConfig(conf: SparkConf) {

  /**
   * Returns whether streaming shuffle mode is enabled.
   * 
   * When enabled, eligible shuffles will use the streaming shuffle path for reduced latency.
   * Default is false (opt-in) to ensure zero impact on existing deployments.
   *
   * @return true if streaming shuffle is enabled, false otherwise
   */
  def isEnabled: Boolean = conf.get(StreamingShuffleConfig.STREAMING_ENABLED)

  /**
   * Returns the percentage of executor memory allocated for streaming buffers.
   * 
   * Per-partition buffer size is calculated as:
   * (executorMemory * bufferSizePercent / 100) / numPartitions
   * 
   * Valid range: 1-50 (percent)
   * Default: 20 (percent)
   *
   * @return buffer size as percentage of executor memory
   */
  def bufferSizePercent: Int = conf.get(StreamingShuffleConfig.BUFFER_SIZE_PERCENT)

  /**
   * Returns the buffer utilization threshold percentage that triggers disk spill.
   * 
   * When buffer utilization exceeds this threshold, the MemorySpillManager will
   * trigger automatic disk spill using LRU eviction to prevent memory exhaustion.
   * Response time is guaranteed to be <100ms from threshold trigger.
   * 
   * Valid range: 50-95 (percent)
   * Default: 80 (percent)
   *
   * @return spill threshold as percentage of buffer utilization
   */
  def spillThresholdPercent: Int = conf.get(StreamingShuffleConfig.SPILL_THRESHOLD)

  /**
   * Returns the maximum bandwidth in MB/s for streaming shuffle transfers.
   * 
   * Token bucket rate limiting is applied at 80% of this value to prevent
   * network saturation. A value of 0 means unlimited bandwidth.
   * 
   * Valid range: >= 0
   * Default: 0 (unlimited)
   *
   * @return maximum bandwidth in MB/s, or 0 for unlimited
   */
  def maxBandwidthMBps: Int = conf.get(StreamingShuffleConfig.MAX_BANDWIDTH_MBPS)

  /**
   * Returns the heartbeat timeout in milliseconds for producer failure detection.
   * 
   * If no heartbeat is received from a producer within this timeout,
   * the consumer will detect the producer as failed and trigger upstream
   * recomputation via FetchFailedException.
   * 
   * Valid range: > 0
   * Default: 5000 (5 seconds)
   *
   * @return heartbeat timeout in milliseconds
   */
  def heartbeatTimeoutMs: Int = conf.get(StreamingShuffleConfig.HEARTBEAT_TIMEOUT_MS)

  /**
   * Returns the acknowledgment timeout in milliseconds for consumer failure detection.
   * 
   * If no acknowledgment is received from a consumer within this timeout,
   * the producer will buffer unacknowledged data and potentially trigger
   * disk spill if memory pressure increases.
   * 
   * Valid range: > 0
   * Default: 10000 (10 seconds)
   *
   * @return acknowledgment timeout in milliseconds
   */
  def ackTimeoutMs: Int = conf.get(StreamingShuffleConfig.ACK_TIMEOUT_MS)

  /**
   * Returns whether debug logging is enabled for streaming shuffle events.
   * 
   * When enabled, additional debug-level logs are emitted for streaming shuffle
   * operations including buffer allocation, spill events, and backpressure signals.
   * Note: Log volume is capped at <10MB/hour per executor when debug is enabled.
   * 
   * Default: false
   *
   * @return true if debug logging is enabled, false otherwise
   */
  def isDebug: Boolean = conf.get(StreamingShuffleConfig.DEBUG)

  /**
   * Validates that the configuration is internally consistent.
   * 
   * This method performs additional cross-parameter validation beyond
   * the individual ConfigEntry validators.
   *
   * @throws IllegalArgumentException if configuration is invalid
   */
  def validate(): Unit = {
    // Ensure ack timeout is greater than heartbeat timeout for proper failure detection
    require(ackTimeoutMs >= heartbeatTimeoutMs,
      s"spark.shuffle.streaming.ackTimeoutMs ($ackTimeoutMs) must be >= " +
      s"spark.shuffle.streaming.heartbeatTimeoutMs ($heartbeatTimeoutMs)")
  }

  /**
   * Returns whether bandwidth limiting is enabled.
   *
   * @return true if maxBandwidthMBps > 0, false otherwise
   */
  def isBandwidthLimitingEnabled: Boolean = maxBandwidthMBps > 0

  /**
   * Calculates the effective bandwidth limit for token bucket rate limiting.
   * 
   * The effective bandwidth is 80% of the configured maximum to prevent
   * network saturation while still allowing efficient streaming.
   *
   * @return effective bandwidth in bytes per second, or Long.MaxValue if unlimited
   */
  def effectiveBandwidthBytesPerSecond: Long = {
    if (maxBandwidthMBps <= 0) {
      Long.MaxValue
    } else {
      // Apply 80% capacity factor for token bucket rate limiting
      (maxBandwidthMBps.toLong * 1024 * 1024 * 0.8).toLong
    }
  }

  override def toString: String = {
    s"StreamingShuffleConfig(" +
      s"enabled=$isEnabled, " +
      s"bufferSizePercent=$bufferSizePercent, " +
      s"spillThresholdPercent=$spillThresholdPercent, " +
      s"maxBandwidthMBps=$maxBandwidthMBps, " +
      s"heartbeatTimeoutMs=$heartbeatTimeoutMs, " +
      s"ackTimeoutMs=$ackTimeoutMs, " +
      s"debug=$isDebug)"
  }
}

/**
 * Companion object containing ConfigEntry definitions for streaming shuffle configuration.
 * 
 * All configuration entries follow Apache Spark's ConfigBuilder pattern with proper
 * validation, documentation, and version tracking. These entries integrate with
 * Spark's configuration system for consistent parameter handling.
 *
 * Configuration Naming Convention:
 * All streaming shuffle configuration parameters use the prefix "spark.shuffle.streaming."
 * to distinguish them from existing shuffle configuration parameters.
 *
 * Integration Points:
 * - StreamingShuffleManager uses these configs to determine streaming behavior
 * - StreamingShuffleWriter uses buffer size and spill threshold for memory management
 * - StreamingShuffleReader uses timeout configs for failure detection
 * - BackpressureProtocol uses bandwidth and timeout configs for flow control
 * - MemorySpillManager uses spill threshold for automatic disk spill trigger
 *
 * @since 4.2.0
 */
private[spark] object StreamingShuffleConfig {

  /**
   * Main feature flag for streaming shuffle mode.
   * 
   * When set to true, eligible shuffles will use the streaming shuffle path
   * for reduced latency. When false (default), the existing SortShuffleManager
   * behavior is preserved with zero impact.
   *
   * Target Improvement:
   * - 30-50% end-to-end latency reduction for shuffle-heavy workloads (10GB+ data, 100+ partitions)
   * - 5-10% improvement for CPU-bound workloads through reduced scheduler overhead
   */
  val STREAMING_ENABLED = ConfigBuilder("spark.shuffle.streaming.enabled")
    .doc("Enable streaming shuffle mode for reduced latency. " +
      "When enabled, eligible shuffles will stream data directly from producers to consumers " +
      "with memory buffering and backpressure protocols, eliminating shuffle materialization latency. " +
      "Default is false (opt-in) to ensure zero impact on existing deployments.")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(false)

  /**
   * Percentage of executor memory allocated for streaming shuffle buffers.
   * 
   * This determines the total memory budget for all streaming shuffle buffers
   * within an executor. Per-partition buffer size is calculated as:
   * (executorMemory * bufferSizePercent / 100) / numPartitions
   *
   * Lower values reduce memory pressure but may trigger more frequent disk spills.
   * Higher values improve streaming throughput but increase memory footprint.
   */
  val BUFFER_SIZE_PERCENT = ConfigBuilder("spark.shuffle.streaming.bufferSizePercent")
    .doc("Percentage of executor memory allocated for streaming shuffle buffers. " +
      "Per-partition buffer size is calculated as (executorMemory * bufferSizePercent) / numPartitions. " +
      "Valid range: 1-50. Default: 20.")
    .version("4.2.0")
    .intConf
    .checkValue(v => v >= 1 && v <= 50, "Must be between 1 and 50 percent")
    .createWithDefault(20)

  /**
   * Buffer utilization threshold percentage that triggers disk spill.
   * 
   * When buffer utilization exceeds this threshold, MemorySpillManager will
   * automatically spill the largest buffered partitions to disk using LRU eviction.
   * The spill response time is guaranteed to be <100ms from threshold trigger.
   *
   * Lower values trigger spills earlier, reducing memory pressure but increasing I/O.
   * Higher values maximize memory usage but risk memory exhaustion under pressure.
   */
  val SPILL_THRESHOLD = ConfigBuilder("spark.shuffle.streaming.spillThreshold")
    .doc("Buffer utilization percentage threshold that triggers automatic disk spill. " +
      "When buffer utilization exceeds this threshold, the largest buffered partitions " +
      "are spilled to disk using LRU eviction with <100ms response time. " +
      "Valid range: 50-95. Default: 80.")
    .version("4.2.0")
    .intConf
    .checkValue(v => v >= 50 && v <= 95, "Must be between 50 and 95 percent")
    .createWithDefault(80)

  /**
   * Maximum bandwidth in MB/s for streaming shuffle transfers.
   * 
   * Token bucket rate limiting is applied at 80% of this value to prevent
   * network saturation while ensuring fair sharing among concurrent shuffles.
   * A value of 0 disables bandwidth limiting (unlimited).
   *
   * Use this to prevent streaming shuffle from saturating network links
   * that are shared with other applications or Spark components.
   */
  val MAX_BANDWIDTH_MBPS = ConfigBuilder("spark.shuffle.streaming.maxBandwidthMBps")
    .doc("Maximum bandwidth in MB/s for streaming shuffle transfers. " +
      "Token bucket rate limiting is applied at 80% of this value to prevent network saturation. " +
      "Set to 0 for unlimited bandwidth. Default: 0 (unlimited).")
    .version("4.2.0")
    .intConf
    .checkValue(v => v >= 0, "Must be non-negative (0 for unlimited)")
    .createWithDefault(0)

  /**
   * Heartbeat timeout in milliseconds for producer failure detection.
   * 
   * StreamingShuffleReader uses this timeout to detect when a producer has failed.
   * If no heartbeat is received within this timeout, the consumer invalidates
   * all partial reads from the failed producer and triggers upstream recomputation
   * via FetchFailedException.
   *
   * Shorter values enable faster failure detection but increase false positives
   * during GC pauses or temporary network issues.
   */
  val HEARTBEAT_TIMEOUT_MS = ConfigBuilder("spark.shuffle.streaming.heartbeatTimeoutMs")
    .doc("Heartbeat timeout in milliseconds for producer failure detection. " +
      "If no heartbeat is received from a producer within this timeout, " +
      "the consumer will detect the producer as failed and trigger upstream recomputation. " +
      "Default: 5000 (5 seconds).")
    .version("4.2.0")
    .intConf
    .checkValue(v => v > 0, "Must be positive")
    .createWithDefault(5000)

  /**
   * Acknowledgment timeout in milliseconds for consumer failure detection.
   * 
   * StreamingShuffleWriter uses this timeout to detect when a consumer has failed
   * or is significantly slower than expected. If no acknowledgment is received
   * within this timeout, the producer buffers unacknowledged data and may trigger
   * disk spill if memory pressure increases.
   *
   * Consumer slowdown (2x slower than producer for >60s) triggers automatic
   * fallback to SortShuffleManager for the affected shuffle.
   */
  val ACK_TIMEOUT_MS = ConfigBuilder("spark.shuffle.streaming.ackTimeoutMs")
    .doc("Acknowledgment timeout in milliseconds for consumer failure detection. " +
      "If no acknowledgment is received from a consumer within this timeout, " +
      "the producer will buffer unacknowledged data and potentially trigger disk spill. " +
      "Default: 10000 (10 seconds).")
    .version("4.2.0")
    .intConf
    .checkValue(v => v > 0, "Must be positive")
    .createWithDefault(10000)

  /**
   * Enable debug logging for streaming shuffle events.
   * 
   * When enabled, additional debug-level logs are emitted including:
   * - Buffer allocation and deallocation events
   * - Spill trigger and completion events
   * - Backpressure signal events
   * - Heartbeat and acknowledgment events
   * - Producer/consumer failure detection events
   *
   * Note: Log volume is capped at <10MB/hour per executor to prevent log exhaustion.
   * Telemetry overhead must be limited to <1% CPU utilization.
   */
  val DEBUG = ConfigBuilder("spark.shuffle.streaming.debug")
    .doc("Enable debug logging for streaming shuffle events. " +
      "When enabled, additional debug-level logs are emitted for buffer allocation, " +
      "spill events, backpressure signals, and failure detection. " +
      "Log volume is capped at <10MB/hour per executor. Default: false.")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(false)

  /**
   * Creates a StreamingShuffleConfig instance from the given SparkConf.
   *
   * @param conf the SparkConf containing streaming shuffle configuration
   * @return a new StreamingShuffleConfig instance
   */
  def apply(conf: SparkConf): StreamingShuffleConfig = new StreamingShuffleConfig(conf)
}
