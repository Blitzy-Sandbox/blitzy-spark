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

package org.apache.spark.shuffle

/**
 * Streaming shuffle implementation package that eliminates shuffle materialization latency
 * by streaming data directly from producers to consumers with memory buffering and
 * backpressure protocols.
 *
 * This package implements the streaming shuffle feature targeting:
 * - 30-50% end-to-end latency reduction for shuffle-heavy workloads (10GB+ data, 100+ partitions)
 * - 5-10% improvement for CPU-bound workloads through reduced scheduler overhead
 * - Zero performance regression for memory-bound workloads through automatic fallback
 *
 * The streaming shuffle coexists with the existing SortShuffleManager implementation,
 * which remains the production-stable fallback. Activation is opt-in via
 * spark.shuffle.streaming.enabled=true configuration.
 *
 * == Architecture Overview ==
 *
 * The streaming shuffle implementation consists of the following core components:
 *
 * '''StreamingShuffleManager''': Main ShuffleManager implementation that coordinates
 * writer/reader creation and manages fallback to SortShuffleManager when necessary.
 *
 * '''StreamingShuffleWriter''': Map-side writer that buffers records in per-partition
 * memory buffers and streams data directly to consumer executors via the network
 * transport layer. Integrates with MemorySpillManager for disk spill coordination.
 *
 * '''StreamingShuffleReader''': Reduce-side reader that polls producers for available
 * data before shuffle completion, validates block integrity via CRC32C checksums,
 * and triggers upstream recomputation on producer failure detection.
 *
 * '''BackpressureProtocol''': Implements consumer-to-producer flow control using
 * heartbeat-based signaling and token bucket rate limiting to prevent buffer overflow
 * and ensure fair bandwidth allocation across concurrent shuffles.
 *
 * '''MemorySpillManager''': Monitors memory utilization at configurable thresholds
 * and triggers automatic disk spill using LRU eviction strategy when memory pressure
 * is detected. Ensures memory release within 100ms of spill trigger.
 *
 * == Configuration ==
 *
 * Streaming shuffle is configured via the following Spark configuration properties:
 *
 * - `spark.shuffle.streaming.enabled`: Enable/disable streaming shuffle (default: false)
 * - `spark.shuffle.streaming.bufferSizePercent`: Percentage of executor memory for buffers (default: 20)
 * - `spark.shuffle.streaming.spillThreshold`: Memory utilization threshold for spill (default: 80%)
 * - `spark.shuffle.streaming.heartbeatTimeoutMs`: Heartbeat timeout for failure detection (default: 5000)
 * - `spark.shuffle.streaming.ackTimeoutMs`: Acknowledgment timeout (default: 10000)
 *
 * == Failure Handling ==
 *
 * The streaming shuffle implementation provides robust failure handling:
 *
 * - '''Producer Failure''': Detected via heartbeat timeout (5 seconds). All partial reads
 *   from the failed producer are atomically invalidated, and upstream recomputation is
 *   triggered via FetchFailedException.
 *
 * - '''Consumer Failure''': Detected via missing acknowledgments (10 seconds). Unacknowledged
 *   data is buffered in memory or spilled to disk, and streaming resumes when the consumer
 *   reconnects.
 *
 * - '''Memory Exhaustion''': Prevented via 80% threshold spill trigger with <100ms response
 *   time. Largest buffered partitions are selected for LRU eviction.
 *
 * - '''Automatic Fallback''': When consumer is 2x slower than producer for >60 seconds,
 *   or network saturation exceeds 90%, automatic fallback to SortShuffleManager occurs.
 *
 * == Coexistence Strategy ==
 *
 * The streaming shuffle is designed to coexist with the existing SortShuffleManager:
 *
 * - Both shuffle managers can be active in the same Spark deployment
 * - SortShuffleManager remains the production-stable fallback
 * - Graceful degradation occurs automatically based on runtime conditions
 * - Zero modifications to existing shuffle code paths
 *
 * @since 4.2.0
 */
package object streaming {

  // ============================================================================
  // Protocol Version
  // ============================================================================

  /**
   * Protocol version for streaming shuffle compatibility checks.
   *
   * This version identifier is used during handshake between producers and consumers
   * to ensure protocol compatibility. Mismatched versions trigger automatic fallback
   * to SortShuffleManager.
   *
   * Version history:
   * - v1: Initial streaming shuffle implementation with backpressure protocol
   */
  val STREAMING_PROTOCOL_VERSION: Int = 1

  // ============================================================================
  // Block Size Configuration
  // ============================================================================

  /**
   * Maximum block size for efficient pipelining (2MB).
   *
   * This limit ensures efficient network transfer by balancing:
   * - Network packet fragmentation overhead (smaller blocks = more overhead)
   * - Memory consumption during buffering (larger blocks = more memory)
   * - Pipeline granularity for backpressure response (smaller blocks = faster response)
   *
   * The 2MB size was chosen based on:
   * - Typical network MTU sizes and TCP buffer configurations
   * - Memory page alignment for efficient buffer management
   * - Empirical testing showing optimal throughput/latency tradeoff
   */
  val MAX_BLOCK_SIZE_BYTES: Long = 2L * 1024L * 1024L

  // ============================================================================
  // Buffer Size Configuration
  // ============================================================================

  /**
   * Default percentage of executor memory allocated for streaming buffers (20%).
   *
   * This value represents the portion of executor memory that can be used for
   * per-partition streaming buffers. The actual buffer size per partition is
   * calculated as: (executorMemory * bufferPercent) / numPartitions
   *
   * This default balances:
   * - Sufficient buffer capacity for network efficiency
   * - Available memory for other task execution needs
   * - Memory pressure avoidance during shuffle operations
   */
  val DEFAULT_BUFFER_SIZE_PERCENT: Int = 20

  /**
   * Minimum allowed buffer size percentage (1%).
   *
   * This floor prevents configuration errors that would result in
   * insufficient buffer capacity for meaningful streaming operations.
   */
  val MIN_BUFFER_SIZE_PERCENT: Int = 1

  /**
   * Maximum allowed buffer size percentage (50%).
   *
   * This ceiling prevents streaming buffers from consuming too much
   * executor memory, which could starve other task execution needs
   * and trigger excessive garbage collection.
   */
  val MAX_BUFFER_SIZE_PERCENT: Int = 50

  // ============================================================================
  // Spill Threshold Configuration
  // ============================================================================

  /**
   * Default memory utilization threshold for triggering disk spill (80%).
   *
   * When streaming buffer memory utilization exceeds this threshold,
   * the MemorySpillManager triggers automatic disk spill using LRU
   * eviction to select partitions for eviction.
   *
   * This threshold provides:
   * - Buffer headroom for incoming data during spill operation
   * - Time margin for spill completion before memory exhaustion
   * - Balance between memory efficiency and spill frequency
   */
  val DEFAULT_SPILL_THRESHOLD_PERCENT: Int = 80

  /**
   * Minimum allowed spill threshold percentage (50%).
   *
   * This floor prevents overly aggressive spilling that would
   * negate the latency benefits of streaming shuffle.
   */
  val MIN_SPILL_THRESHOLD_PERCENT: Int = 50

  /**
   * Maximum allowed spill threshold percentage (95%).
   *
   * This ceiling ensures sufficient headroom for spill operations
   * to complete before memory exhaustion occurs.
   */
  val MAX_SPILL_THRESHOLD_PERCENT: Int = 95

  // ============================================================================
  // Timeout Configuration
  // ============================================================================

  /**
   * Default heartbeat timeout for producer failure detection (5 seconds).
   *
   * If no heartbeat is received from a producer within this interval,
   * the consumer considers the producer failed and:
   * 1. Invalidates all partial reads from the failed producer
   * 2. Triggers FetchFailedException for upstream recomputation
   * 3. Cleans up associated buffer resources
   *
   * This timeout balances:
   * - Quick failure detection for fast recovery
   * - Tolerance for temporary network delays or GC pauses
   */
  val DEFAULT_HEARTBEAT_TIMEOUT_MS: Int = 5000

  /**
   * Default acknowledgment timeout for consumer liveness monitoring (10 seconds).
   *
   * If no acknowledgment is received from a consumer within this interval,
   * the producer considers the consumer potentially failed and:
   * 1. Buffers unacknowledged data in memory
   * 2. Triggers disk spill if buffer exceeds threshold
   * 3. Prepares for data retransmission on reconnect
   *
   * This timeout is longer than heartbeat timeout to allow for
   * consumer processing latency and batch acknowledgment delays.
   */
  val DEFAULT_ACK_TIMEOUT_MS: Int = 10000

  /**
   * Default threshold for detecting sustained consumer slowdown (60 seconds).
   *
   * If a consumer remains slower than the producer by CONSUMER_SLOWDOWN_FACTOR
   * for longer than this duration, automatic fallback to SortShuffleManager
   * is triggered for the affected shuffle.
   *
   * This duration provides:
   * - Tolerance for temporary consumer slowdowns (e.g., GC pauses)
   * - Sufficient observation window for accurate slowdown detection
   * - Timely intervention before buffer exhaustion
   */
  val DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS: Long = 60000L

  // ============================================================================
  // Retry Configuration
  // ============================================================================

  /**
   * Default initial delay for exponential backoff retry (1 second).
   *
   * When network operations fail, retries are attempted with exponential
   * backoff starting from this initial delay. Subsequent retries double
   * the delay until DEFAULT_MAX_RETRY_ATTEMPTS is reached.
   *
   * Retry sequence: 1s, 2s, 4s, 8s, 16s (for 5 attempts)
   */
  val DEFAULT_RETRY_INITIAL_DELAY_MS: Int = 1000

  /**
   * Default maximum number of retry attempts (5).
   *
   * After this many failed attempts, the operation is considered failed
   * and appropriate failure handling is triggered (e.g., FetchFailedException
   * for producer failures, buffer spill for consumer failures).
   */
  val DEFAULT_MAX_RETRY_ATTEMPTS: Int = 5

  // ============================================================================
  // Rate Limiting and Backpressure Configuration
  // ============================================================================

  /**
   * Consumer slowdown detection factor (2x slower than producer).
   *
   * If the consumer's data consumption rate falls below (producer rate / this factor)
   * for longer than DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS, automatic fallback
   * to SortShuffleManager is triggered.
   *
   * A factor of 2.0 means the consumer must be at least half as fast as the producer
   * to avoid triggering fallback.
   */
  val CONSUMER_SLOWDOWN_FACTOR: Double = 2.0

  /**
   * Network saturation threshold for fallback trigger (90% link capacity).
   *
   * When network utilization exceeds this threshold, the streaming shuffle
   * reduces transmission rate and may trigger fallback to disk-based shuffle
   * to prevent network congestion from affecting other Spark operations.
   */
  val NETWORK_SATURATION_THRESHOLD: Double = 0.90

  /**
   * Token bucket capacity factor for bandwidth rate limiting (80% of max bandwidth).
   *
   * The token bucket algorithm limits streaming bandwidth to this fraction of
   * the maximum available bandwidth. This provides headroom for:
   * - Other network operations (heartbeats, control messages)
   * - Bandwidth measurement inaccuracies
   * - Burst absorption during backpressure events
   *
   * Token bucket refill rate is calculated as:
   * refillRate = maxBandwidthMBps * TOKEN_BUCKET_CAPACITY_FACTOR / numConcurrentShuffles
   */
  val TOKEN_BUCKET_CAPACITY_FACTOR: Double = 0.80

  // ============================================================================
  // Monitoring and Telemetry Constants
  // ============================================================================

  /**
   * Interval for memory utilization monitoring in milliseconds.
   *
   * The MemorySpillManager checks buffer utilization at this interval
   * to detect when the spill threshold is exceeded. A 100ms interval
   * ensures quick response (<100ms) to memory pressure while minimizing
   * monitoring overhead.
   */
  val MEMORY_MONITORING_INTERVAL_MS: Int = 100

  /**
   * Maximum telemetry overhead as a fraction of CPU utilization.
   *
   * Streaming shuffle telemetry must not consume more than this fraction
   * of CPU resources. Metrics collection is throttled if this limit is approached.
   */
  val MAX_TELEMETRY_OVERHEAD_PERCENT: Double = 0.01 // 1%

  /**
   * Maximum log volume per hour per executor in bytes (10MB).
   *
   * Log rate limiting is applied to prevent streaming shuffle events
   * from overwhelming log storage. Debug logging is disabled by default
   * and can be enabled via spark.shuffle.streaming.debug configuration.
   */
  val MAX_LOG_VOLUME_BYTES_PER_HOUR: Long = 10L * 1024L * 1024L

  // ============================================================================
  // Checksum and Data Integrity Constants
  // ============================================================================

  /**
   * Checksum algorithm identifier for block integrity validation.
   *
   * CRC32C is used for its hardware acceleration support in modern CPUs
   * (via Java 9+ intrinsics) and good error detection properties.
   */
  val CHECKSUM_ALGORITHM: String = "CRC32C"

  /**
   * Maximum acceptable checksum failure rate before triggering emergency fallback.
   *
   * If checksum failures exceed 0.1% of total blocks, streaming shuffle is
   * disabled for the affected job and SortShuffleManager is used as fallback.
   */
  val MAX_CHECKSUM_FAILURE_RATE: Double = 0.001 // 0.1%

  // ============================================================================
  // Internal Constants (Package-Private)
  // ============================================================================

  /**
   * Size of the header prepended to each streaming block in bytes.
   *
   * Header format:
   * - 4 bytes: Block size
   * - 4 bytes: Partition ID
   * - 8 bytes: CRC32C checksum
   * - 4 bytes: Flags (compression, last block, etc.)
   */
  private[streaming] val BLOCK_HEADER_SIZE_BYTES: Int = 20

  /**
   * Magic number for streaming shuffle block identification.
   *
   * Used to identify valid streaming shuffle blocks and detect corruption
   * or protocol mismatch.
   */
  private[streaming] val STREAMING_BLOCK_MAGIC: Int = 0x53535348 // "SSSH" in ASCII

  /**
   * Flag indicating the block is the last block for a partition.
   */
  private[streaming] val FLAG_LAST_BLOCK: Int = 0x01

  /**
   * Flag indicating the block data is compressed.
   */
  private[streaming] val FLAG_COMPRESSED: Int = 0x02

  /**
   * Flag indicating the block requires acknowledgment.
   */
  private[streaming] val FLAG_REQUIRES_ACK: Int = 0x04
}
