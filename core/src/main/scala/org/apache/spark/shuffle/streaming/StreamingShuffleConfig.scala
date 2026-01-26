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
import org.apache.spark.internal.config._

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
  def isEnabled: Boolean = conf.get(SHUFFLE_STREAMING_ENABLED)

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
  def bufferSizePercent: Int = conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

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
  def spillThresholdPercent: Int = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)

  /**
   * Returns the maximum bandwidth in MB/s for streaming shuffle transfers.
   * 
   * Token bucket rate limiting is applied at 80% of this value to prevent
   * network saturation. A value of 0 means unlimited bandwidth (config not set).
   * 
   * Valid range: > 0 when set, 0 means unlimited
   * Default: 0 (unlimited - config not set)
   *
   * @return maximum bandwidth in MB/s, or 0 for unlimited
   */
  def maxBandwidthMBps: Int = conf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).getOrElse(0)

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
  def heartbeatTimeoutMs: Int = conf.get(SHUFFLE_STREAMING_HEARTBEAT_TIMEOUT_MS)

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
  def ackTimeoutMs: Int = conf.get(SHUFFLE_STREAMING_ACK_TIMEOUT_MS)

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
  def isDebug: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

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
 * Companion object providing aliases to config entries and factory method.
 * 
 * The actual ConfigEntry definitions are in org.apache.spark.internal.config package
 * to follow Spark's convention of centralizing all configuration entries.
 * This object provides convenient aliases for use within the streaming shuffle package.
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
   * Alias for SHUFFLE_STREAMING_ENABLED config entry.
   * Main feature flag for streaming shuffle mode.
   */
  val STREAMING_ENABLED = SHUFFLE_STREAMING_ENABLED

  /**
   * Alias for SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT config entry.
   * Percentage of executor memory allocated for streaming shuffle buffers.
   */
  val BUFFER_SIZE_PERCENT = SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT

  /**
   * Alias for SHUFFLE_STREAMING_SPILL_THRESHOLD config entry.
   * Buffer utilization threshold percentage that triggers disk spill.
   */
  val SPILL_THRESHOLD = SHUFFLE_STREAMING_SPILL_THRESHOLD

  /**
   * Alias for SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS config entry.
   * Maximum bandwidth in MB/s for streaming shuffle transfers.
   */
  val MAX_BANDWIDTH_MBPS = SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS

  /**
   * Alias for SHUFFLE_STREAMING_HEARTBEAT_TIMEOUT_MS config entry.
   * Heartbeat timeout in milliseconds for producer failure detection.
   */
  val HEARTBEAT_TIMEOUT_MS = SHUFFLE_STREAMING_HEARTBEAT_TIMEOUT_MS

  /**
   * Alias for SHUFFLE_STREAMING_ACK_TIMEOUT_MS config entry.
   * Acknowledgment timeout in milliseconds for consumer failure detection.
   */
  val ACK_TIMEOUT_MS = SHUFFLE_STREAMING_ACK_TIMEOUT_MS

  /**
   * Alias for SHUFFLE_STREAMING_DEBUG config entry.
   * Enable debug logging for streaming shuffle events.
   */
  val DEBUG = SHUFFLE_STREAMING_DEBUG

  /**
   * Creates a StreamingShuffleConfig instance from the given SparkConf.
   *
   * @param conf the SparkConf containing streaming shuffle configuration
   * @return a new StreamingShuffleConfig instance
   */
  def apply(conf: SparkConf): StreamingShuffleConfig = new StreamingShuffleConfig(conf)
}
