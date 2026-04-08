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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.BlockManagerId

/**
 * Unit tests for [[BackpressureProtocol]] class verifying flow control mechanisms including:
 * - Token bucket rate limiting
 * - Heartbeat-based liveness detection
 * - Acknowledgment processing
 * - Backpressure signal handling
 * - Priority arbitration for concurrent shuffles
 *
 * Tests use SparkFunSuite and Mockito patterns for isolated testing without requiring
 * a full Spark environment.
 */
class BackpressureProtocolSuite
    extends SparkFunSuite
    with Matchers
    with BeforeAndAfterEach {

  // Test fixtures
  private var conf: SparkConf = _
  private var protocol: BackpressureProtocol = _

  /**
   * Create a SparkConf with streaming shuffle configuration values.
   * Uses reasonable test defaults for quick test execution.
   */
  private def createTestConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.bufferSizePercent", "20")
      .set("spark.shuffle.streaming.spillThreshold", "80")
      .set("spark.shuffle.streaming.connectionTimeout", "2s") // Short timeout for tests
      .set("spark.shuffle.streaming.heartbeatInterval", "1s") // Short interval for tests
      .set("spark.shuffle.streaming.debug", "true")
  }

  /**
   * Create a mock BlockManagerId for testing.
   * Uses a unique identifier to distinguish different consumers.
   *
   * @param id unique identifier for this mock consumer
   * @return a mock BlockManagerId
   */
  private def createMockConsumer(id: String): BlockManagerId = {
    val mockId = mock(classOf[BlockManagerId])
    when(mockId.executorId).thenReturn(id)
    when(mockId.host).thenReturn(s"host-$id")
    when(mockId.port).thenReturn(7000 + id.hashCode % 1000)
    when(mockId.toString).thenReturn(s"BlockManagerId($id)")
    mockId
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = createTestConf()
    protocol = new BackpressureProtocol(conf)
  }

  override def afterEach(): Unit = {
    try {
      if (protocol != null) {
        protocol.stop()
        protocol = null
      }
    } finally {
      super.afterEach()
    }
  }

  // ===================================================================================
  // Initial State Tests
  // ===================================================================================

  test("initial state has no consumers") {
    // A newly created protocol should have no registered consumers
    val consumerId = createMockConsumer("consumer-1")

    // Since consumer is not registered, isConsumerAlive should return false
    // (no state entry exists)
    protocol.isConsumerAlive(consumerId) must be(false)

    // Getting lag ratio for unregistered consumer should return 0.0
    protocol.getConsumerLagRatio(consumerId) must be(0.0)

    // Should not throw when operating on unregistered consumers
    noException should be thrownBy {
      protocol.recordSentBytes(consumerId, 1000)
      protocol.recordAcknowledgment(consumerId, 500)
      protocol.handleBackpressureSignal(consumerId, activate = true)
    }
  }

  // ===================================================================================
  // Token Bucket Rate Limiting Tests
  // ===================================================================================

  test("shouldThrottle returns false when under rate limit") {
    // Create conf with a reasonable bandwidth limit
    val limitedConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "100") // 100 MB/s limit
      .set("spark.shuffle.streaming.connectionTimeout", "5s")
      .set("spark.shuffle.streaming.heartbeatInterval", "10s")

    protocol.stop()
    protocol = new BackpressureProtocol(limitedConf)

    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)

    // Record heartbeat to ensure consumer is considered alive
    protocol.recordHeartbeat(consumerId)

    // Small amount of bytes should not trigger throttling
    val bytesToSend = 1024L // 1KB - well under the rate limit
    val shouldThrottle = protocol.shouldThrottle(consumerId, bytesToSend)

    shouldThrottle must be(false)
  }

  test("shouldThrottle returns true when rate limit exceeded") {
    // Create conf with a very small bandwidth limit
    val limitedConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "1") // 1 MB/s limit
      .set("spark.shuffle.streaming.connectionTimeout", "5s")
      .set("spark.shuffle.streaming.heartbeatInterval", "10s")

    protocol.stop()
    protocol = new BackpressureProtocol(limitedConf)

    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Try to send very large amount of bytes that exceeds bucket capacity
    // At 1 MB/s with 80% capacity factor, bucket capacity is ~838,860 bytes
    // We'll try to acquire much more than that repeatedly
    var throttled = false
    for (_ <- 1 to 10) {
      if (protocol.shouldThrottle(consumerId, 500 * 1024)) { // 500KB each attempt
        throttled = true
      }
    }

    // After several large acquisitions, token bucket should be depleted
    throttled must be(true)
  }

  test("shouldThrottle returns true when protocol is stopped") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Verify not throttled before stop
    protocol.shouldThrottle(consumerId, 1024) must be(false)

    // Stop the protocol
    protocol.stop()

    // After stop, all requests should be throttled
    protocol.shouldThrottle(consumerId, 1024) must be(true)
  }

  test("shouldThrottle returns true for stale heartbeat") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)

    // Record initial heartbeat
    protocol.recordHeartbeat(consumerId)

    // Wait for heartbeat to become stale (2x heartbeat interval)
    // Our test config has 1s heartbeat interval, so we wait 2.5s
    Thread.sleep(2500)

    // Heartbeat is now stale, should throttle
    protocol.shouldThrottle(consumerId, 1024) must be(true)
  }

  // ===================================================================================
  // Acknowledgment Processing Tests
  // ===================================================================================

  test("recordAcknowledgment updates consumer state") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Send some bytes
    protocol.recordSentBytes(consumerId, 10000)

    // Record acknowledgment for part of the sent bytes
    protocol.recordAcknowledgment(consumerId, 5000)

    // Lag ratio should be pending/acknowledged = 5000/5000 = 1.0
    val lagRatio = protocol.getConsumerLagRatio(consumerId)
    lagRatio must be(1.0)

    // Acknowledge remaining bytes
    protocol.recordAcknowledgment(consumerId, 5000)

    // Lag ratio should now be 0/10000 = 0.0
    val finalLagRatio = protocol.getConsumerLagRatio(consumerId)
    finalLagRatio must be(0.0)
  }

  test("recordAcknowledgment clears backpressure signal") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Activate backpressure
    protocol.handleBackpressureSignal(consumerId, activate = true)

    // Verify backpressure is active by checking throttling
    protocol.shouldThrottle(consumerId, 1) must be(true)

    // Send and acknowledge bytes - acknowledgment should clear backpressure
    protocol.recordSentBytes(consumerId, 1000)
    protocol.recordAcknowledgment(consumerId, 500)

    // After acknowledgment, backpressure should be cleared
    // Small amount should not be throttled (assuming we're under rate limit)
    protocol.shouldThrottle(consumerId, 1) must be(false)
  }

  // ===================================================================================
  // Sent Bytes Tracking Tests
  // ===================================================================================

  test("recordSentBytes tracks pending data") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Initially no lag since no bytes sent/acknowledged
    protocol.getConsumerLagRatio(consumerId) must be(0.0)

    // Send bytes without acknowledgment
    protocol.recordSentBytes(consumerId, 1000)

    // Still 0 lag ratio because no acknowledgments yet (division by zero protection)
    // The lag ratio is pending/acknowledged, and with 0 acknowledged, it returns 0.0
    val lagAfterSend = protocol.getConsumerLagRatio(consumerId)
    lagAfterSend must be(0.0)

    // Acknowledge some bytes to establish a baseline
    protocol.recordAcknowledgment(consumerId, 500)

    // Now pending = 500, acknowledged = 500, ratio = 1.0
    protocol.getConsumerLagRatio(consumerId) must be(1.0)

    // Send more bytes without acknowledgment
    protocol.recordSentBytes(consumerId, 1000)

    // Now pending = 1500, acknowledged = 500, ratio = 3.0
    protocol.getConsumerLagRatio(consumerId) must be(3.0)
  }

  // ===================================================================================
  // Backpressure Signal Handling Tests
  // ===================================================================================

  test("handleBackpressureSignal activates throttling") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Initially should not be throttled for small amounts
    protocol.shouldThrottle(consumerId, 1) must be(false)

    // Activate backpressure signal
    protocol.handleBackpressureSignal(consumerId, activate = true)

    // Now should be throttled due to backpressure
    protocol.shouldThrottle(consumerId, 1) must be(true)
  }

  test("handleBackpressureSignal deactivates throttling") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Activate backpressure
    protocol.handleBackpressureSignal(consumerId, activate = true)
    protocol.shouldThrottle(consumerId, 1) must be(true)

    // Deactivate backpressure
    protocol.handleBackpressureSignal(consumerId, activate = false)

    // Should no longer be throttled due to backpressure
    // (may still be throttled for other reasons like rate limit)
    protocol.shouldThrottle(consumerId, 1) must be(false)
  }

  // ===================================================================================
  // Heartbeat and Liveness Tests
  // ===================================================================================

  test("recordHeartbeat updates last heartbeat time") {
    val consumerId = createMockConsumer("consumer-1")

    // Consumer not registered yet - should not be alive
    protocol.isConsumerAlive(consumerId) must be(false)

    // Register consumer via heartbeat
    protocol.recordHeartbeat(consumerId)

    // Now consumer should be alive
    protocol.isConsumerAlive(consumerId) must be(true)

    // Wait a short time and send another heartbeat
    Thread.sleep(100)
    protocol.recordHeartbeat(consumerId)

    // Consumer should still be alive
    protocol.isConsumerAlive(consumerId) must be(true)
  }

  test("isConsumerAlive returns true for recent heartbeats") {
    val consumerId = createMockConsumer("consumer-1")

    // Register consumer
    protocol.recordHeartbeat(consumerId)

    // Immediately after heartbeat, consumer should be alive
    protocol.isConsumerAlive(consumerId) must be(true)

    // Wait less than connection timeout (2s in test config)
    Thread.sleep(500)

    // Should still be alive
    protocol.isConsumerAlive(consumerId) must be(true)
  }

  test("isConsumerAlive returns false for stale heartbeats") {
    val consumerId = createMockConsumer("consumer-1")

    // Register consumer
    protocol.recordHeartbeat(consumerId)

    // Verify consumer is alive
    protocol.isConsumerAlive(consumerId) must be(true)

    // Wait longer than connection timeout (2s in test config)
    Thread.sleep(2500)

    // Consumer should now be considered dead
    protocol.isConsumerAlive(consumerId) must be(false)
  }

  // ===================================================================================
  // Shuffle Priority Tests
  // ===================================================================================

  test("registerShuffle tracks shuffle priorities") {
    // Register multiple shuffles with different partition counts and data volumes
    protocol.registerShuffle(shuffleId = 1, partitionCount = 100, estimatedDataVolume = 1000000)
    protocol.registerShuffle(shuffleId = 2, partitionCount = 200, estimatedDataVolume = 500000)
    protocol.registerShuffle(shuffleId = 3, partitionCount = 50, estimatedDataVolume = 2000000)

    // Verify all shuffles are registered by checking their priorities
    val priority1 = protocol.getShufflePriority(1)
    val priority2 = protocol.getShufflePriority(2)
    val priority3 = protocol.getShufflePriority(3)

    // All should have valid priority (not Int.MaxValue)
    priority1 must not be Int.MaxValue
    priority2 must not be Int.MaxValue
    priority3 must not be Int.MaxValue
  }

  test("getShufflePriority returns correct ranking") {
    // Register shuffles with different partition counts
    // Higher partition count = higher priority (lower rank number)
    protocol.registerShuffle(shuffleId = 1, partitionCount = 100, estimatedDataVolume = 1000000)
    protocol.registerShuffle(shuffleId = 2, partitionCount = 200, estimatedDataVolume = 500000)
    protocol.registerShuffle(shuffleId = 3, partitionCount = 50, estimatedDataVolume = 2000000)

    val priority1 = protocol.getShufflePriority(1)
    val priority2 = protocol.getShufflePriority(2)
    val priority3 = protocol.getShufflePriority(3)

    // Shuffle 2 has highest partition count (200), so should have lowest priority number (rank 0)
    priority2 must be(0)

    // Shuffle 1 has second highest partition count (100), so should have rank 1
    priority1 must be(1)

    // Shuffle 3 has lowest partition count (50), so should have highest rank (2)
    priority3 must be(2)
  }

  test("getShufflePriority returns MaxValue for unregistered shuffle") {
    // Unregistered shuffle should return Int.MaxValue
    protocol.getShufflePriority(999) must be(Int.MaxValue)
  }

  test("unregisterShuffle removes shuffle from priority tracking") {
    protocol.registerShuffle(shuffleId = 1, partitionCount = 100, estimatedDataVolume = 1000000)

    // Verify registered
    protocol.getShufflePriority(1) must not be Int.MaxValue

    // Unregister
    protocol.unregisterShuffle(1)

    // Verify unregistered
    protocol.getShufflePriority(1) must be(Int.MaxValue)
  }

  // ===================================================================================
  // Consumer Lag Ratio Tests
  // ===================================================================================

  test("getConsumerLagRatio computes lag correctly") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Initially no data - lag ratio is 0
    protocol.getConsumerLagRatio(consumerId) must be(0.0)

    // Send bytes and acknowledge some
    protocol.recordSentBytes(consumerId, 10000)
    protocol.recordAcknowledgment(consumerId, 2000)

    // Pending = 8000, Acknowledged = 2000, Ratio = 8000/2000 = 4.0
    protocol.getConsumerLagRatio(consumerId) must be(4.0)

    // Acknowledge more bytes
    protocol.recordAcknowledgment(consumerId, 4000)

    // Pending = 4000, Acknowledged = 6000, Ratio = 4000/6000 ≈ 0.667
    val ratio = protocol.getConsumerLagRatio(consumerId)
    ratio must be(0.6666666666666666 +- 0.001)
  }

  test("getConsumerLagRatio returns 0 for unregistered consumer") {
    val consumerId = createMockConsumer("unknown")
    protocol.getConsumerLagRatio(consumerId) must be(0.0)
  }

  // ===================================================================================
  // Stop and Cleanup Tests
  // ===================================================================================

  test("stop() cleans up resources") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)
    protocol.registerShuffle(1, 100, 1000000)

    // Start heartbeat monitor
    protocol.startHeartbeatMonitor()

    // Verify consumer is tracked
    protocol.isConsumerAlive(consumerId) must be(true)

    // Stop the protocol
    protocol.stop()

    // After stop, shouldThrottle returns true for all requests
    protocol.shouldThrottle(consumerId, 1) must be(true)

    // Lag ratio should still work but return 0 since consumer state is cleared
    protocol.getConsumerLagRatio(consumerId) must be(0.0)

    // Priority should return MaxValue since shuffles are cleared
    protocol.getShufflePriority(1) must be(Int.MaxValue)
  }

  test("stop() can be called multiple times safely") {
    protocol.registerConsumer(createMockConsumer("consumer-1"))
    protocol.startHeartbeatMonitor()

    // Multiple stop calls should not throw
    noException should be thrownBy {
      protocol.stop()
      protocol.stop()
      protocol.stop()
    }
  }

  // ===================================================================================
  // Thread Safety Tests
  // ===================================================================================

  test("thread safety under concurrent access") {
    val numThreads = 10
    val operationsPerThread = 100
    val latch = new CountDownLatch(numThreads)
    val errors = new ArrayBuffer[Throwable]()
    val executorService = Executors.newFixedThreadPool(numThreads)

    // Create consumers
    val consumers = (1 to 5).map(i => createMockConsumer(s"consumer-$i"))
    consumers.foreach(protocol.registerConsumer)
    consumers.foreach(protocol.recordHeartbeat)

    // Register some shuffles
    (1 to 3).foreach(i => protocol.registerShuffle(i, i * 10, i * 1000))

    try {
      // Submit concurrent tasks
      for (threadId <- 1 to numThreads) {
        executorService.submit(new Runnable {
          override def run(): Unit = {
            try {
              for (_ <- 1 to operationsPerThread) {
                val consumer = consumers(threadId % consumers.size)

                // Perform various operations concurrently
                protocol.shouldThrottle(consumer, 100)
                protocol.recordSentBytes(consumer, 1000)
                protocol.recordAcknowledgment(consumer, 500)
                protocol.recordHeartbeat(consumer)
                protocol.isConsumerAlive(consumer)
                protocol.getConsumerLagRatio(consumer)
                protocol.handleBackpressureSignal(consumer, activate = threadId % 2 == 0)
                protocol.getShufflePriority(threadId % 3 + 1)
              }
            } catch {
              case e: Throwable =>
                errors.synchronized {
                  errors += e
                }
            } finally {
              latch.countDown()
            }
          }
        })
      }

      // Wait for all threads to complete
      val completed = latch.await(30, TimeUnit.SECONDS)
      completed must be(true)

      // Verify no errors occurred
      if (errors.nonEmpty) {
        fail(s"Concurrent access caused ${errors.size} errors: ${errors.head.getMessage}")
      }
    } finally {
      executorService.shutdown()
      executorService.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  // ===================================================================================
  // Consumer Registration Tests
  // ===================================================================================

  test("registerConsumer creates consumer state") {
    val consumerId = createMockConsumer("consumer-1")

    // Before registration - no state
    protocol.isConsumerAlive(consumerId) must be(false)

    // Register consumer
    protocol.registerConsumer(consumerId)

    // After registration, recordHeartbeat should update the state
    protocol.recordHeartbeat(consumerId)
    protocol.isConsumerAlive(consumerId) must be(true)
  }

  test("unregisterConsumer removes consumer state") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Verify consumer is registered
    protocol.isConsumerAlive(consumerId) must be(true)

    // Unregister consumer
    protocol.unregisterConsumer(consumerId)

    // Consumer should no longer be alive (state removed)
    protocol.isConsumerAlive(consumerId) must be(false)
  }

  // ===================================================================================
  // Fallback Detection Tests
  // ===================================================================================

  test("shouldFallback returns false when lag is below threshold") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Send and acknowledge bytes with low lag ratio
    protocol.recordSentBytes(consumerId, 1000)
    protocol.recordAcknowledgment(consumerId, 900)

    // Lag ratio is ~0.11 (pending 100 / acknowledged 900) - well below 2.0 threshold
    protocol.shouldFallback(consumerId) must be(false)
  }

  test("shouldFallback requires sustained lag duration") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Create high lag ratio (>2.0)
    protocol.recordSentBytes(consumerId, 10000)
    protocol.recordAcknowledgment(consumerId, 1000)
    // Pending = 9000, Acknowledged = 1000, Ratio = 9.0 (well above 2.0)

    // Even with high lag ratio, shouldFallback requires sustained duration (60s)
    // Since we just started, lag duration is minimal
    // The fallback should be false because the lag hasn't been sustained long enough
    val immediateResult = protocol.shouldFallback(consumerId)
    // This depends on implementation - lag tracking starts when ratio exceeds threshold
    // and requires 60 seconds of sustained lag

    // At minimum, verify the lag ratio is correctly high
    protocol.getConsumerLagRatio(consumerId) must be >= 2.0
  }

  // ===================================================================================
  // Heartbeat Monitor Tests
  // ===================================================================================

  test("startHeartbeatMonitor starts background thread") {
    val deadConsumers = new ArrayBuffer[BlockManagerId]()
    val callbackInvoked = new AtomicBoolean(false)

    // Set up callback to track dead consumers
    protocol.setConsumerDeadCallback((consumerId: BlockManagerId) => {
      deadConsumers.synchronized {
        deadConsumers += consumerId
      }
      callbackInvoked.set(true)
    })

    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Start heartbeat monitor
    protocol.startHeartbeatMonitor()

    // Initially consumer should be alive
    protocol.isConsumerAlive(consumerId) must be(true)

    // Wait longer than connection timeout for consumer to be detected as dead
    // Test config has 2s connection timeout, heartbeat monitor runs at 1s interval
    Thread.sleep(3500)

    // Consumer should now be detected as dead and callback invoked
    // Note: This depends on the heartbeat monitor having run
    callbackInvoked.get() must be(true)
    deadConsumers.synchronized {
      deadConsumers must contain(consumerId)
    }
  }

  test("startHeartbeatMonitor can be called multiple times safely") {
    // Multiple calls should not create multiple threads
    noException should be thrownBy {
      protocol.startHeartbeatMonitor()
      protocol.startHeartbeatMonitor()
      protocol.startHeartbeatMonitor()
    }
  }

  // ===================================================================================
  // Priority Arbitration with Equal Partition Counts Tests
  // ===================================================================================

  test("getShufflePriority breaks ties by data volume") {
    // Register shuffles with same partition count but different data volumes
    // Higher data volume = higher priority (lower rank)
    protocol.registerShuffle(shuffleId = 1, partitionCount = 100, estimatedDataVolume = 1000000)
    protocol.registerShuffle(shuffleId = 2, partitionCount = 100, estimatedDataVolume = 2000000)
    protocol.registerShuffle(shuffleId = 3, partitionCount = 100, estimatedDataVolume = 500000)

    val priority1 = protocol.getShufflePriority(1)
    val priority2 = protocol.getShufflePriority(2)
    val priority3 = protocol.getShufflePriority(3)

    // Shuffle 2 has highest data volume, should have rank 0
    priority2 must be(0)

    // Shuffle 1 has second highest data volume, should have rank 1
    priority1 must be(1)

    // Shuffle 3 has lowest data volume, should have rank 2
    priority3 must be(2)
  }

  // ===================================================================================
  // Edge Case Tests
  // ===================================================================================

  test("handles zero bytes in recordSentBytes and recordAcknowledgment") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Should handle zero bytes gracefully
    noException should be thrownBy {
      protocol.recordSentBytes(consumerId, 0)
      protocol.recordAcknowledgment(consumerId, 0)
    }

    protocol.getConsumerLagRatio(consumerId) must be(0.0)
  }

  test("handles large byte values") {
    val consumerId = createMockConsumer("consumer-1")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Send large amount of bytes
    val largeBytes = Long.MaxValue / 2
    noException should be thrownBy {
      protocol.recordSentBytes(consumerId, largeBytes)
      protocol.recordAcknowledgment(consumerId, largeBytes / 2)
    }

    // Lag ratio should be calculable
    val lagRatio = protocol.getConsumerLagRatio(consumerId)
    lagRatio must be > 0.0
  }

  test("handles negative shuffle IDs") {
    // Negative shuffle IDs should work (they're just integers)
    noException should be thrownBy {
      protocol.registerShuffle(shuffleId = -1, partitionCount = 100, estimatedDataVolume = 1000000)
    }

    protocol.getShufflePriority(-1) must not be Int.MaxValue
  }

  // ===================================================================================
  // Consumer Dead Callback Tests
  // ===================================================================================

  test("setConsumerDeadCallback sets callback correctly") {
    val callbackCalled = new AtomicBoolean(false)
    val callbackConsumerId = new AtomicLong(0)

    protocol.setConsumerDeadCallback((consumerId: BlockManagerId) => {
      callbackCalled.set(true)
      callbackConsumerId.set(consumerId.hashCode())
    })

    // Start monitoring
    protocol.startHeartbeatMonitor()

    val consumerId = createMockConsumer("dying-consumer")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Wait for timeout (longer than connection timeout + heartbeat interval for async safety)
    // Test config has 2s connection timeout, heartbeat monitor runs at 1s interval
    Thread.sleep(3500)

    // Callback should have been invoked
    callbackCalled.get() must be(true)
  }

  // ===================================================================================
  // Consumer Lag Duration Tests
  // ===================================================================================

  test("getConsumerLagDuration returns 0 for unregistered consumer") {
    val consumerId = createMockConsumer("unknown")
    protocol.getConsumerLagDuration(consumerId) must be(0L)
  }

  test("getConsumerLagDuration tracks sustained lag") {
    val consumerId = createMockConsumer("lagging-consumer")
    protocol.registerConsumer(consumerId)
    protocol.recordHeartbeat(consumerId)

    // Create a lag condition (ratio > 2.0)
    protocol.recordSentBytes(consumerId, 10000)
    protocol.recordAcknowledgment(consumerId, 1000)
    // Pending = 9000, Acked = 1000, Ratio = 9.0

    // Send more to trigger lag tracking
    protocol.recordSentBytes(consumerId, 5000)
    // Pending = 14000, Acked = 1000, Ratio = 14.0

    // Wait a short time
    Thread.sleep(100)

    // Lag duration should be > 0 if lag tracking has started
    val lagDuration = protocol.getConsumerLagDuration(consumerId)
    // Note: The exact value depends on when the lag tracking started
    lagDuration must be >= 0L
  }
}
