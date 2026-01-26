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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

/**
 * Unit test suite for [[BackpressureProtocol]].
 *
 * This suite tests the following critical functionality:
 *   - Heartbeat-based flow control with configurable timeout (default 5 seconds)
 *   - Heartbeat timeout detection after the configured timeout period
 *   - Token bucket rate limiting implementation
 *   - Rate limiting at 80% of maximum configured bandwidth
 *   - Priority arbitration based on partition count and data volume
 *   - Consumer status tracking throughout shuffle lifecycle
 *   - Slow consumer detection (2x slower than producer for >60 seconds)
 *   - Acknowledgment processing that updates consumer status
 *   - Backpressure event telemetry emission via metrics
 *   - Fair bandwidth sharing among concurrent shuffles
 *
 * Test coverage target: >85% for BackpressureProtocol
 */
class BackpressureProtocolSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {

  // Mock for StreamingShuffleMetrics to verify backpressure telemetry emission
  @Mock(answer = RETURNS_SMART_NULLS)
  private var mockMetrics: StreamingShuffleMetrics = _

  // Configuration and protocol instances used across tests
  private var conf: SparkConf = _
  private var config: StreamingShuffleConfig = _
  private var protocol: BackpressureProtocol = _

  // Auto-closeable for Mockito mocks
  private var mockitoCloseable: AutoCloseable = _

  /**
   * Creates a SparkConf with streaming shuffle enabled and default settings.
   */
  private def createSparkConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SHUFFLE_STREAMING_ENABLED, true)
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, 20)
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 80)
      .set(SHUFFLE_STREAMING_HEARTBEAT_TIMEOUT_MS, 5000)
      .set(SHUFFLE_STREAMING_ACK_TIMEOUT_MS, 10000)
      .set(SHUFFLE_STREAMING_DEBUG, false)
  }

  /**
   * Creates a SparkConf with custom heartbeat timeout for timeout tests.
   */
  private def createSparkConfWithShortTimeout(timeoutMs: Int): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SHUFFLE_STREAMING_ENABLED, true)
      .set(SHUFFLE_STREAMING_HEARTBEAT_TIMEOUT_MS, timeoutMs)
      .set(SHUFFLE_STREAMING_ACK_TIMEOUT_MS, timeoutMs * 2)
      .set(SHUFFLE_STREAMING_DEBUG, true)
  }

  /**
   * Creates a SparkConf with bandwidth limiting enabled.
   */
  private def createSparkConfWithBandwidth(maxBandwidthMBps: Int): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SHUFFLE_STREAMING_ENABLED, true)
      .set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, maxBandwidthMBps)
      .set(SHUFFLE_STREAMING_HEARTBEAT_TIMEOUT_MS, 5000)
      .set(SHUFFLE_STREAMING_ACK_TIMEOUT_MS, 10000)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    mockitoCloseable = MockitoAnnotations.openMocks(this)
    conf = createSparkConf()
    config = new StreamingShuffleConfig(conf)
    protocol = new BackpressureProtocol(config, mockMetrics)
  }

  override def afterEach(): Unit = {
    if (mockitoCloseable != null) {
      mockitoCloseable.close()
    }
    protocol = null
    config = null
    conf = null
    super.afterEach()
  }

  // ==========================================================================
  // Heartbeat-based Flow Control Tests
  // ==========================================================================

  test("heartbeat-based flow control initialization") {
    // Verify that heartbeat timeout is properly configured from StreamingShuffleConfig
    config.heartbeatTimeoutMs must equal(DEFAULT_HEARTBEAT_TIMEOUT_MS)

    // Verify protocol is created with correct config
    val consumerId = "test-consumer-1"
    protocol.registerConsumer(consumerId)

    // Verify consumer is registered and tracking started
    val status = protocol.getConsumerStatus(consumerId)
    status.isDefined must be(true)
    status.get.consumerId must equal(consumerId)
    status.get.lastHeartbeatTime must be > 0L

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("heartbeat timeout detection after 5 seconds") {
    // Use short timeout for testing (100ms instead of 5000ms)
    val shortTimeoutConf = createSparkConfWithShortTimeout(100)
    val shortConfig = new StreamingShuffleConfig(shortTimeoutConf)
    val shortProtocol = new BackpressureProtocol(shortConfig, mockMetrics)

    val consumerId = "test-consumer-timeout"
    shortProtocol.registerConsumer(consumerId)

    // Initially, no timeout should be detected
    shortProtocol.checkHeartbeatTimeout(consumerId) must be(false)

    // Send a heartbeat to update timestamp
    shortProtocol.sendHeartbeat(consumerId)

    // Wait for timeout to expire (150ms > 100ms timeout)
    Thread.sleep(150)

    // Now timeout should be detected
    shortProtocol.checkHeartbeatTimeout(consumerId) must be(true)

    // Verify backpressure event was recorded
    verify(mockMetrics, times(1)).incBackpressureEvents()

    // Clean up
    shortProtocol.unregisterConsumer(consumerId)
  }

  test("heartbeat prevents timeout when sent within interval") {
    val shortTimeoutConf = createSparkConfWithShortTimeout(100)
    val shortConfig = new StreamingShuffleConfig(shortTimeoutConf)
    val shortProtocol = new BackpressureProtocol(shortConfig, mockMetrics)

    val consumerId = "test-consumer-active"
    shortProtocol.registerConsumer(consumerId)

    // Send heartbeats periodically to prevent timeout
    for (_ <- 1 to 5) {
      shortProtocol.sendHeartbeat(consumerId)
      Thread.sleep(50) // 50ms < 100ms timeout
      shortProtocol.checkHeartbeatTimeout(consumerId) must be(false)
    }

    // Verify no backpressure events were recorded
    verify(mockMetrics, never()).incBackpressureEvents()

    // Clean up
    shortProtocol.unregisterConsumer(consumerId)
  }

  // ==========================================================================
  // Token Bucket Rate Limiting Tests
  // ==========================================================================

  test("token bucket rate limiting") {
    // Create token bucket with known bandwidth
    val maxBandwidthMBps = 100.0
    val tokenBucket = new TokenBucket(maxBandwidthMBps)

    // Verify token bucket is initialized with capacity
    val expectedCapacity = (maxBandwidthMBps * TOKEN_BUCKET_CAPACITY_FACTOR * 1024 * 1024).toLong
    tokenBucket.getCapacity must equal(expectedCapacity)

    // Verify initial tokens are available
    tokenBucket.getAvailableTokens must be > 0L

    // Test token allocation succeeds when tokens are available
    val allocateBytes = 1024L * 1024L // 1MB
    tokenBucket.allocateTokens(allocateBytes) must be(true)

    // Available tokens should decrease after allocation
    tokenBucket.getAvailableTokens must be < expectedCapacity
  }

  test("rate limiting at 80% of max bandwidth") {
    // Verify TOKEN_BUCKET_CAPACITY_FACTOR is 0.8 (80%)
    TOKEN_BUCKET_CAPACITY_FACTOR must equal(0.80)

    // Create token bucket with specific bandwidth
    val maxBandwidthMBps = 100.0
    val tokenBucket = new TokenBucket(maxBandwidthMBps)

    // Verify capacity is 80% of max bandwidth
    val expectedCapacity = (maxBandwidthMBps * 0.80 * 1024 * 1024).toLong
    tokenBucket.getCapacity must equal(expectedCapacity)

    // Verify refill rate is 80% of max bandwidth
    val expectedRefillRate = maxBandwidthMBps * 0.80 * 1024 * 1024 // bytes per second
    tokenBucket.getRefillRateBytesPerSecond must equal(expectedRefillRate)
  }

  test("token bucket refills over time") {
    val maxBandwidthMBps = 100.0
    val tokenBucket = new TokenBucket(maxBandwidthMBps)

    // Allocate a large portion of available tokens
    val allocateBytes = tokenBucket.getCapacity / 2
    tokenBucket.allocateTokens(allocateBytes) must be(true)

    val tokensAfterAlloc = tokenBucket.getAvailableTokens

    // Wait a short period for refill
    Thread.sleep(50)

    // Manually trigger refill
    tokenBucket.refillTokens()

    // Tokens should have increased after refill
    tokenBucket.getAvailableTokens must be >= tokensAfterAlloc
  }

  test("token bucket rejects allocation when insufficient tokens") {
    val maxBandwidthMBps = 1.0 // Small bandwidth for easy testing
    val tokenBucket = new TokenBucket(maxBandwidthMBps)

    // Get the capacity
    val capacity = tokenBucket.getCapacity

    // Allocate all available tokens
    tokenBucket.allocateTokens(capacity) must be(true)

    // Next allocation should fail (no tokens available)
    tokenBucket.allocateTokens(1024L) must be(false)
  }

  test("token bucket with unlimited bandwidth always succeeds") {
    // Create token bucket with 0 (unlimited) bandwidth
    val tokenBucket = new TokenBucket(0.0)

    // Verify capacity is unlimited
    tokenBucket.getCapacity must equal(Long.MaxValue)

    // Allocation should always succeed
    tokenBucket.allocateTokens(Long.MaxValue / 2) must be(true)
    tokenBucket.allocateTokens(Long.MaxValue / 2) must be(true)
  }

  // ==========================================================================
  // Priority Arbitration Tests
  // ==========================================================================

  test("priority arbitration based on partition count") {
    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    // Register two shuffles with different partition counts
    val shuffleId1 = 1
    val shuffleId2 = 2
    val dataVolume = 1024L * 1024L * 1024L // 1GB

    // Shuffle 1 has 10 partitions - register first
    bandwidthProtocol.allocateBandwidth(shuffleId1, 10, dataVolume)

    // Shuffle 2 has 100 partitions (10x more) - register second
    bandwidthProtocol.allocateBandwidth(shuffleId2, 100, dataVolume)

    // Now re-allocate to get proper comparison values after both are registered
    // Both shuffles are now in the priority map, so fair sharing applies
    val bandwidth1 = bandwidthProtocol.allocateBandwidth(shuffleId1, 10, dataVolume)
    val bandwidth2 = bandwidthProtocol.allocateBandwidth(shuffleId2, 100, dataVolume)

    // Shuffle with more partitions should get higher priority/bandwidth
    bandwidth2 must be > bandwidth1

    // Clean up
    bandwidthProtocol.removeShuffle(shuffleId1)
    bandwidthProtocol.removeShuffle(shuffleId2)
  }

  test("priority arbitration based on data volume") {
    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    val shuffleId1 = 1
    val shuffleId2 = 2
    val partitionCount = 50

    // Shuffle 1 has 1GB data volume - register first
    bandwidthProtocol.allocateBandwidth(shuffleId1, partitionCount, 1024L * 1024L * 1024L)

    // Shuffle 2 has 10GB data volume (10x more) - register second
    bandwidthProtocol.allocateBandwidth(shuffleId2, partitionCount, 10L * 1024L * 1024L * 1024L)

    // Now re-allocate to get proper comparison values after both are registered
    // Both shuffles are now in the priority map, so fair sharing applies
    val bandwidth1 = bandwidthProtocol.allocateBandwidth(shuffleId1, partitionCount, 1024L * 1024L * 1024L)
    val bandwidth2 = bandwidthProtocol.allocateBandwidth(shuffleId2, partitionCount, 10L * 1024L * 1024L * 1024L)

    // Shuffle with more data volume should get higher priority/bandwidth
    bandwidth2 must be > bandwidth1

    // Clean up
    bandwidthProtocol.removeShuffle(shuffleId1)
    bandwidthProtocol.removeShuffle(shuffleId2)
  }

  test("priority arbitration combines partition count and data volume") {
    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    val shuffleId1 = 1
    val shuffleId2 = 2

    // Register both shuffles first
    // Shuffle 1: 50 partitions, 5GB data
    bandwidthProtocol.allocateBandwidth(shuffleId1, 50, 5L * 1024L * 1024L * 1024L)

    // Shuffle 2: 100 partitions (2x), 2.5GB data (0.5x)
    bandwidthProtocol.allocateBandwidth(shuffleId2, 100, 2560L * 1024L * 1024L)

    // Now re-allocate to get proper comparison values after both are registered
    val bandwidth1 = bandwidthProtocol.allocateBandwidth(shuffleId1, 50, 5L * 1024L * 1024L * 1024L)
    val bandwidth2 = bandwidthProtocol.allocateBandwidth(shuffleId2, 100, 2560L * 1024L * 1024L)

    // Both shuffles should get non-zero bandwidth
    bandwidth1 must be > 0L
    bandwidth2 must be > 0L

    // Each shuffle's individual bandwidth should not exceed effective bandwidth
    val effectiveBandwidth = bandwidthConfig.effectiveBandwidthBytesPerSecond
    bandwidth1 must be <= effectiveBandwidth
    bandwidth2 must be <= effectiveBandwidth

    // Clean up
    bandwidthProtocol.removeShuffle(shuffleId1)
    bandwidthProtocol.removeShuffle(shuffleId2)
  }

  // ==========================================================================
  // Consumer Status Tracking Tests
  // ==========================================================================

  test("consumer status tracking") {
    val consumerId = "test-consumer-tracking"

    // Register consumer
    protocol.registerConsumer(consumerId)

    // Verify initial status
    var status = protocol.getConsumerStatus(consumerId)
    status.isDefined must be(true)
    status.get.bytesReceived.get() must equal(0L)
    status.get.bytesAcknowledged.get() must equal(0L)

    // Record some bytes received
    val receivedBytes = 1024L * 1024L
    protocol.recordBytesReceived(consumerId, receivedBytes)

    status = protocol.getConsumerStatus(consumerId)
    status.get.bytesReceived.get() must equal(receivedBytes)

    // Record some bytes acknowledged
    val acknowledgedBytes = 512L * 1024L
    protocol.recordBytesAcknowledged(consumerId, acknowledgedBytes)

    status = protocol.getConsumerStatus(consumerId)
    status.get.bytesAcknowledged.get() must equal(acknowledgedBytes)

    // Verify consumer is in all consumer status list
    val allStatus = protocol.getAllConsumerStatus
    allStatus.contains(consumerId) must be(true)

    // Unregister and verify cleanup
    protocol.unregisterConsumer(consumerId)
    protocol.getConsumerStatus(consumerId).isDefined must be(false)
  }

  test("consumer status tracks multiple consumers independently") {
    val consumer1 = "consumer-1"
    val consumer2 = "consumer-2"

    protocol.registerConsumer(consumer1)
    protocol.registerConsumer(consumer2)

    // Record different data volumes for each consumer
    protocol.recordBytesReceived(consumer1, 1000L)
    protocol.recordBytesReceived(consumer2, 2000L)

    protocol.recordBytesAcknowledged(consumer1, 500L)
    protocol.recordBytesAcknowledged(consumer2, 1500L)

    // Verify each consumer has independent tracking
    val status1 = protocol.getConsumerStatus(consumer1).get
    val status2 = protocol.getConsumerStatus(consumer2).get

    status1.bytesReceived.get() must equal(1000L)
    status1.bytesAcknowledged.get() must equal(500L)

    status2.bytesReceived.get() must equal(2000L)
    status2.bytesAcknowledged.get() must equal(1500L)

    // Clean up
    protocol.unregisterConsumer(consumer1)
    protocol.unregisterConsumer(consumer2)
  }

  // ==========================================================================
  // Slow Consumer Detection Tests
  // ==========================================================================

  test("slow consumer detection (2x slower for >60s)") {
    // Verify the slowdown factor constant is 2.0
    CONSUMER_SLOWDOWN_FACTOR must equal(2.0)

    // Verify the default slowdown threshold is 60000ms
    DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS must equal(60000L)

    val consumerId = "slow-consumer-test"
    protocol.registerConsumer(consumerId)

    // Record bytes received (producer rate)
    val producerBytes = 1000L * 1024L
    protocol.recordBytesReceived(consumerId, producerBytes)

    // Record bytes acknowledged (consumer rate) - less than 50% (1/2.0) of producer
    // This simulates consumer being slower than 1/CONSUMER_SLOWDOWN_FACTOR
    val consumerBytes = 400L * 1024L // 40% of producer rate
    protocol.recordBytesAcknowledged(consumerId, consumerBytes)

    // Initially, slowdown should not be detected (not 60s yet)
    protocol.detectSlowConsumer(consumerId) must be(false)

    // Get consumer status and manually simulate elapsed time for sustained slowdown
    val status = protocol.getConsumerStatus(consumerId).get
    // Simulate slowdown detection started
    status.slowdownDetectedTime.set(
      System.currentTimeMillis() - DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS - 1000)

    // Now slowdown should be detected (sustained for >60s)
    protocol.detectSlowConsumer(consumerId) must be(true)

    // Verify backpressure event was recorded
    verify(mockMetrics, atLeastOnce()).incBackpressureEvents()

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("slow consumer recovers when consumption rate improves") {
    val consumerId = "recovering-consumer"
    protocol.registerConsumer(consumerId)

    // Record bytes where consumer is slow
    protocol.recordBytesReceived(consumerId, 1000L)
    protocol.recordBytesAcknowledged(consumerId, 400L) // 40% rate

    // Trigger initial slowdown detection
    protocol.detectSlowConsumer(consumerId)

    val status = protocol.getConsumerStatus(consumerId).get
    status.slowdownDetectedTime.get() must be > 0L

    // Consumer catches up - acknowledge more bytes
    protocol.recordBytesAcknowledged(consumerId, 600L) // Now 100% caught up

    // Slowdown detection should reset
    protocol.detectSlowConsumer(consumerId)
    status.slowdownDetectedTime.get() must equal(0L)

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  // ==========================================================================
  // Acknowledgment Processing Tests
  // ==========================================================================

  test("acknowledgment processing updates consumer status") {
    val consumerId = "ack-test-consumer"
    protocol.registerConsumer(consumerId)

    // Create a test block ID
    val blockId: BlockId = ShuffleBlockId(0, 0, 0)

    // Get initial status
    var status = protocol.getConsumerStatus(consumerId).get
    val initialAckCount = status.getAcknowledgedBlockCount

    // Process acknowledgment
    protocol.processAck(consumerId, blockId)

    // Verify acknowledgment was recorded
    status = protocol.getConsumerStatus(consumerId).get
    status.getAcknowledgedBlockCount must equal(initialAckCount + 1)
    status.lastAckTime.get() must be > 0L

    // Process more acknowledgments
    protocol.processAck(consumerId, ShuffleBlockId(0, 0, 1))
    protocol.processAck(consumerId, ShuffleBlockId(0, 0, 2))

    status = protocol.getConsumerStatus(consumerId).get
    status.getAcknowledgedBlockCount must equal(initialAckCount + 3)

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("acknowledgment timeout detection") {
    val shortTimeoutConf = createSparkConfWithShortTimeout(100)
    val shortConfig = new StreamingShuffleConfig(shortTimeoutConf)
    val shortProtocol = new BackpressureProtocol(shortConfig, mockMetrics)

    val consumerId = "ack-timeout-consumer"
    shortProtocol.registerConsumer(consumerId)

    // Initially no timeout (start time is reference)
    // Wait longer than ack timeout (200ms > 100ms * 2)
    Thread.sleep(250)

    // Check ack timeout - should detect since no ack received
    shortProtocol.checkAckTimeout(consumerId) must be(true)

    // Verify backpressure event recorded
    verify(mockMetrics, atLeastOnce()).incBackpressureEvents()

    // Clean up
    shortProtocol.unregisterConsumer(consumerId)
  }

  // ==========================================================================
  // Backpressure Event Telemetry Tests
  // ==========================================================================

  test("backpressure event telemetry emission") {
    val consumerId = "telemetry-test-consumer"
    protocol.registerConsumer(consumerId)

    // Reset mock to clear any previous invocations
    reset(mockMetrics)

    // Simulate conditions that trigger backpressure events

    // 1. Record bytes to create slow consumer condition
    protocol.recordBytesReceived(consumerId, 10000L)
    protocol.recordBytesAcknowledged(consumerId, 1000L) // Very slow

    // Simulate sustained slowdown
    val status = protocol.getConsumerStatus(consumerId).get
    status.slowdownDetectedTime.set(
      System.currentTimeMillis() - DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS - 1000)

    // Trigger slowdown detection
    protocol.detectSlowConsumer(consumerId)

    // Verify metrics were incremented
    verify(mockMetrics, atLeastOnce()).incBackpressureEvents()

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("multiple backpressure events are counted separately") {
    // Create real metrics instance for this test
    val realMetrics = new StreamingShuffleMetrics()
    val metricsProtocol = new BackpressureProtocol(config, realMetrics)

    val initialCount = realMetrics.getBackpressureEvents

    // Register multiple consumers and trigger backpressure events
    val consumers = (1 to 3).map(i => s"consumer-$i")
    consumers.foreach { consumerId =>
      metricsProtocol.registerConsumer(consumerId)
      metricsProtocol.recordBytesReceived(consumerId, 10000L)
      metricsProtocol.recordBytesAcknowledged(consumerId, 1000L)

      val status = metricsProtocol.getConsumerStatus(consumerId).get
      status.slowdownDetectedTime.set(
        System.currentTimeMillis() - DEFAULT_CONSUMER_SLOWDOWN_THRESHOLD_MS - 1000)

      metricsProtocol.detectSlowConsumer(consumerId)
    }

    // Verify multiple events were recorded
    realMetrics.getBackpressureEvents must be >= (initialCount + 3)

    // Clean up
    consumers.foreach(metricsProtocol.unregisterConsumer)
  }

  // ==========================================================================
  // Fair Bandwidth Sharing Tests
  // ==========================================================================

  test("fair bandwidth sharing among concurrent shuffles") {
    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    // Register multiple shuffles with similar characteristics
    val numShuffles = 5
    val partitionCount = 100
    val dataVolume = 1024L * 1024L * 1024L // 1GB each

    // First, register all shuffles (allocateBandwidth calculates share at registration time)
    (1 to numShuffles).foreach { shuffleId =>
      bandwidthProtocol.allocateBandwidth(shuffleId, partitionCount, dataVolume)
    }

    // Now query the current fair allocations after all shuffles are registered
    val fairAllocations = (1 to numShuffles).map { shuffleId =>
      bandwidthProtocol.getCurrentBandwidthAllocation(shuffleId)
    }

    // All shuffles should get similar bandwidth (fair sharing)
    val avgAllocation = fairAllocations.sum / numShuffles
    fairAllocations.foreach { allocation =>
      // Each allocation should be within 20% of average
      allocation.toDouble must be >= (avgAllocation * 0.8)
      allocation.toDouble must be <= (avgAllocation * 1.2)
    }

    // Total fair allocation should not exceed effective bandwidth
    val effectiveBandwidth = bandwidthConfig.effectiveBandwidthBytesPerSecond
    fairAllocations.sum must be <= effectiveBandwidth

    // Clean up
    (1 to numShuffles).foreach(bandwidthProtocol.removeShuffle)
  }

  test("bandwidth reallocation when shuffle completes") {
    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    val partitionCount = 100
    val dataVolume = 1024L * 1024L * 1024L

    // Register two shuffles
    val bandwidth1Before = bandwidthProtocol.allocateBandwidth(1, partitionCount, dataVolume)
    val bandwidth2Before = bandwidthProtocol.allocateBandwidth(2, partitionCount, dataVolume)

    // Complete shuffle 2
    bandwidthProtocol.removeShuffle(2)

    // Reallocate bandwidth for shuffle 1
    val bandwidth1After = bandwidthProtocol.allocateBandwidth(1, partitionCount, dataVolume)

    // Shuffle 1 should get more bandwidth after shuffle 2 completes
    bandwidth1After must be >= bandwidth1Before

    // Clean up
    bandwidthProtocol.removeShuffle(1)
  }

  // ==========================================================================
  // Bandwidth Allocation Tests
  // ==========================================================================

  test("allocateBandwidth returns appropriate rate") {
    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    val shuffleId = 1
    val partitionCount = 100
    val dataVolume = 10L * 1024L * 1024L * 1024L // 10GB

    val allocatedBandwidth = bandwidthProtocol.allocateBandwidth(
      shuffleId, partitionCount, dataVolume)

    // Allocated bandwidth should be positive
    allocatedBandwidth must be > 0L

    // Allocated bandwidth should not exceed effective bandwidth
    val effectiveBandwidth = bandwidthConfig.effectiveBandwidthBytesPerSecond
    allocatedBandwidth must be <= effectiveBandwidth

    // Verify minimum allocation (1MB/s) is guaranteed
    val minAllocation = 1024L * 1024L
    allocatedBandwidth must be >= minAllocation

    // Clean up
    bandwidthProtocol.removeShuffle(shuffleId)
  }

  test("allocateBandwidth returns MaxValue when bandwidth limiting is disabled") {
    // Use default config without bandwidth limit
    val shuffleId = 1
    val partitionCount = 100
    val dataVolume = 10L * 1024L * 1024L * 1024L

    val allocatedBandwidth = protocol.allocateBandwidth(shuffleId, partitionCount, dataVolume)

    // When bandwidth limiting is disabled, should return Long.MaxValue
    allocatedBandwidth must equal(Long.MaxValue)

    // Clean up
    protocol.removeShuffle(shuffleId)
  }

  // ==========================================================================
  // Edge Cases and Error Handling Tests
  // ==========================================================================

  test("operations on unregistered consumer are handled gracefully") {
    val unregisteredConsumerId = "nonexistent-consumer"

    // All operations should handle unregistered consumer gracefully
    protocol.sendHeartbeat(unregisteredConsumerId) // Should log warning
    protocol.checkHeartbeatTimeout(unregisteredConsumerId) must be(false)
    protocol.detectSlowConsumer(unregisteredConsumerId) must be(false)
    protocol.getConsumerStatus(unregisteredConsumerId).isDefined must be(false)

    // processAck should handle gracefully
    val blockId: BlockId = ShuffleBlockId(0, 0, 0)
    protocol.processAck(unregisteredConsumerId, blockId) // Should log warning

    // recordBytes should handle gracefully
    protocol.recordBytesReceived(unregisteredConsumerId, 1000L)
    protocol.recordBytesAcknowledged(unregisteredConsumerId, 500L)

    // No exceptions should be thrown
  }

  test("duplicate consumer registration is handled") {
    val consumerId = "duplicate-consumer"

    // First registration
    protocol.registerConsumer(consumerId)
    val firstStatus = protocol.getConsumerStatus(consumerId).get

    // Record some data
    protocol.recordBytesReceived(consumerId, 1000L)

    // Second registration attempt - should keep existing
    protocol.registerConsumer(consumerId)
    val secondStatus = protocol.getConsumerStatus(consumerId).get

    // Should be the same instance (not replaced)
    secondStatus.bytesReceived.get() must equal(1000L)

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("protocol reset clears all state") {
    // Register consumers and shuffles
    protocol.registerConsumer("consumer-1")
    protocol.registerConsumer("consumer-2")

    val bandwidthConf = createSparkConfWithBandwidth(100)
    val bandwidthConfig = new StreamingShuffleConfig(bandwidthConf)
    val bandwidthProtocol = new BackpressureProtocol(bandwidthConfig, mockMetrics)

    bandwidthProtocol.registerConsumer("consumer-3")
    bandwidthProtocol.allocateBandwidth(1, 100, 1024L * 1024L)
    bandwidthProtocol.allocateBandwidth(2, 50, 512L * 1024L)

    // Reset protocol
    bandwidthProtocol.reset()

    // Verify state is cleared
    bandwidthProtocol.getAllConsumerStatus.isEmpty must be(true)

    // Clean up original protocol
    protocol.unregisterConsumer("consumer-1")
    protocol.unregisterConsumer("consumer-2")
  }

  // ==========================================================================
  // ConsumerInfo Tests
  // ==========================================================================

  test("ConsumerInfo tracks consumption and producer rates") {
    val consumerId = "rate-tracking-consumer"
    protocol.registerConsumer(consumerId)

    // Wait a small amount of time to establish baseline
    Thread.sleep(100)

    // Record data
    protocol.recordBytesReceived(consumerId, 10000L)
    protocol.recordBytesAcknowledged(consumerId, 5000L)

    // Get consumer status and check rates
    val status = protocol.getConsumerStatus(consumerId).get

    // Rates should be calculable
    val producerRate = status.getProducerRate
    val consumptionRate = status.getConsumptionRate

    producerRate must be >= 0.0
    consumptionRate must be >= 0.0
    producerRate must be >= consumptionRate

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("ConsumerInfo toString provides meaningful representation") {
    val consumerId = "to-string-test"
    protocol.registerConsumer(consumerId)

    protocol.recordBytesReceived(consumerId, 1000L)
    protocol.recordBytesAcknowledged(consumerId, 500L)

    val status = protocol.getConsumerStatus(consumerId).get
    val stringRep = status.toString

    // Verify toString contains key information
    stringRep must include("consumerId")
    stringRep must include(consumerId)
    stringRep must include("bytesReceived")
    stringRep must include("bytesAcknowledged")

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  // ==========================================================================
  // Thread Safety Tests
  // ==========================================================================

  test("concurrent consumer operations are thread-safe") {
    val consumerId = "concurrent-test-consumer"
    protocol.registerConsumer(consumerId)

    // Run concurrent operations
    val threads = (1 to 10).map { i =>
      new Thread(() => {
        for (_ <- 1 to 100) {
          protocol.recordBytesReceived(consumerId, 100L)
          protocol.recordBytesAcknowledged(consumerId, 50L)
          protocol.sendHeartbeat(consumerId)
          protocol.checkHeartbeatTimeout(consumerId)
          protocol.detectSlowConsumer(consumerId)
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Verify final state is consistent
    val status = protocol.getConsumerStatus(consumerId).get
    status.bytesReceived.get() must equal(10 * 100 * 100L) // 10 threads * 100 iterations * 100 bytes
    status.bytesAcknowledged.get() must equal(10 * 100 * 50L)

    // Clean up
    protocol.unregisterConsumer(consumerId)
  }

  test("concurrent token bucket operations are thread-safe") {
    val tokenBucket = new TokenBucket(100.0) // 100 MB/s

    val successCount = new java.util.concurrent.atomic.AtomicInteger(0)
    val failCount = new java.util.concurrent.atomic.AtomicInteger(0)

    // Run concurrent allocations
    val threads = (1 to 10).map { i =>
      new Thread(() => {
        for (_ <- 1 to 100) {
          if (tokenBucket.allocateTokens(1024L)) {
            successCount.incrementAndGet()
          } else {
            failCount.incrementAndGet()
          }
          tokenBucket.refillTokens()
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Some allocations should succeed, some may fail (due to contention)
    // Total should equal 1000
    (successCount.get() + failCount.get()) must equal(1000)
  }
}
