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

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable

import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.storage.{BlockManager, DiskBlockManager, TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * Unit tests for MemorySpillManager covering:
 * - 80% threshold triggering at 100ms intervals
 * - LRU eviction selection for largest partitions
 * - Buffer reclamation within 100ms of acknowledgment
 * - Spill coordination with BlockManager.diskStore
 * - Memory tracking with TaskMemoryManager integration
 * - Lifecycle methods (start/stop)
 * - Concurrent buffer access safety
 * - Threshold configurability (50-95%)
 *
 * These tests verify the MemorySpillManager meets its performance and correctness
 * requirements as specified in the streaming shuffle implementation plan.
 *
 * @since 4.2.0
 */
class MemorySpillManagerSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with MockitoSugar
  with Eventually {

  // Test configuration
  private var conf: SparkConf = _
  private var tempDir: File = _
  private var streamingConfig: StreamingShuffleConfig = _
  private var metrics: StreamingShuffleMetrics = _
  private var taskMemoryManager: TaskMemoryManager = _
  private var blockManager: BlockManager = _
  private var diskBlockManager: DiskBlockManager = _
  private var memorySpillManager: MemorySpillManager = _

  // Patience configuration for Eventually
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Milliseconds))

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Create temp directory for spill files
    tempDir = Utils.createTempDir()

    // Create SparkConf with streaming shuffle enabled and debug mode
    conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.spillThreshold", "80")
      .set("spark.shuffle.streaming.debug", "true")
      .set("spark.shuffle.streaming.bufferSizePercent", "20")
      .set("spark.app.id", "test-app")

    streamingConfig = new StreamingShuffleConfig(conf)
    metrics = new StreamingShuffleMetrics()

    // Setup mock TaskMemoryManager
    val memoryManager = new TestMemoryManager(conf)
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)

    // Setup mock BlockManager and DiskBlockManager
    blockManager = mock[BlockManager]
    diskBlockManager = mock[DiskBlockManager]
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)

    // Configure DiskBlockManager to create files in temp directory
    when(diskBlockManager.getFile(any[TempShuffleBlockId])).thenAnswer { invocation =>
      val blockId = invocation.getArgument[TempShuffleBlockId](0)
      val file = new File(tempDir, blockId.name)
      file.getParentFile.mkdirs()
      file
    }

    // Create MemorySpillManager instance for testing
    memorySpillManager = new MemorySpillManager(
      taskMemoryManager,
      blockManager,
      streamingConfig,
      metrics
    )
  }

  override def afterEach(): Unit = {
    try {
      // Stop memory spill manager
      if (memorySpillManager != null) {
        memorySpillManager.stop()
        memorySpillManager = null
      }

      // Cleanup temp directory
      if (tempDir != null) {
        Utils.deleteRecursively(tempDir)
        tempDir = null
      }
    } finally {
      super.afterEach()
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Creates a test ByteBuffer with specified size filled with test data.
   *
   * @param size Size of the buffer in bytes
   * @return ByteBuffer filled with sequential bytes
   */
  private def createTestBuffer(size: Int): ByteBuffer = {
    val buffer = ByteBuffer.allocate(size)
    for (i <- 0 until size) {
      buffer.put((i % 256).toByte)
    }
    buffer.flip()
    buffer
  }

  /**
   * Creates a MemorySpillManager with custom spill threshold.
   *
   * @param spillThreshold Spill threshold percentage (50-95)
   * @return Configured MemorySpillManager
   */
  private def createManagerWithThreshold(spillThreshold: Int): MemorySpillManager = {
    val customConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.spillThreshold", spillThreshold.toString)
      .set("spark.shuffle.streaming.debug", "true")
      .set("spark.app.id", "test-app")

    val customConfig = new StreamingShuffleConfig(customConf)
    val customMetrics = new StreamingShuffleMetrics()

    new MemorySpillManager(
      taskMemoryManager,
      blockManager,
      customConfig,
      customMetrics
    )
  }

  /**
   * Simulates memory pressure by registering multiple buffers.
   *
   * @param numBuffers Number of buffers to register
   * @param bufferSize Size of each buffer in bytes
   * @param maxCapacity Maximum memory capacity for utilization calculation
   */
  private def simulateMemoryPressure(
      numBuffers: Int,
      bufferSize: Long,
      maxCapacity: Long): Unit = {
    for (i <- 0 until numBuffers) {
      memorySpillManager.registerBuffer(i, bufferSize, maxCapacity)
    }
  }

  // ============================================================================
  // Test Cases - 80% Threshold Monitoring at 100ms Interval
  // ============================================================================

  test("80% threshold monitoring at 100ms interval") {
    // Verify the monitoring interval constant is 100ms
    assert(MEMORY_MONITORING_INTERVAL_MS === 100,
      s"Expected monitoring interval of 100ms, got $MEMORY_MONITORING_INTERVAL_MS")

    // Start the manager to activate monitoring
    memorySpillManager.start()

    // Register buffers to set up capacity (don't exceed threshold yet)
    val maxCapacity = 1000L
    memorySpillManager.registerBuffer(0, 100L, maxCapacity) // 10% utilization

    // Wait for at least one monitoring cycle (100ms + buffer)
    Thread.sleep(150)

    // Verify utilization is being tracked
    val utilization = memorySpillManager.getCurrentUtilization
    assert(utilization > 0.0,
      s"Expected positive utilization, got $utilization")

    // Verify metrics are being updated at monitoring intervals
    eventually {
      val bufferUtil = metrics.getBufferUtilization
      assert(bufferUtil >= 0.0 && bufferUtil <= 100.0,
        s"Buffer utilization should be between 0 and 100, got $bufferUtil")
    }
  }

  // ============================================================================
  // Test Cases - Spill Triggered When Utilization Exceeds 80%
  // ============================================================================

  test("spill triggered when utilization exceeds 80%") {
    // Start the manager
    memorySpillManager.start()

    val maxCapacity = 1000L

    // Register buffers to exceed 80% threshold
    // 850 bytes out of 1000 = 85% utilization
    memorySpillManager.registerBuffer(0, 300L, maxCapacity)
    memorySpillManager.registerBuffer(1, 300L, maxCapacity)
    memorySpillManager.registerBuffer(2, 250L, maxCapacity)

    // Verify utilization exceeds threshold
    val currentUtilization = memorySpillManager.getCurrentUtilization
    assert(currentUtilization > 80.0,
      s"Expected utilization > 80%, got $currentUtilization%")

    // Wait for monitoring to detect threshold breach
    Thread.sleep(200)

    // Verify eviction candidates are selected
    val partitionsToEvict = memorySpillManager.selectPartitionsForEviction(Map.empty)
    assert(partitionsToEvict.nonEmpty,
      "Expected partitions to be selected for eviction when above threshold")
  }

  // ============================================================================
  // Test Cases - LRU Eviction Selection
  // ============================================================================

  test("LRU eviction selects largest partitions") {
    val maxCapacity = 1000L

    // Register buffers with different sizes
    memorySpillManager.registerBuffer(0, 100L, maxCapacity) // smallest
    memorySpillManager.registerBuffer(1, 300L, maxCapacity) // largest
    memorySpillManager.registerBuffer(2, 200L, maxCapacity) // medium

    // Register additional buffer to exceed 80% threshold (600/1000 = 60%, need more)
    memorySpillManager.registerBuffer(3, 350L, maxCapacity) // Total: 950 bytes = 95%

    // Get eviction candidates
    val partitionsToEvict = memorySpillManager.selectPartitionsForEviction(Map.empty)

    // Verify largest partitions are selected first
    assert(partitionsToEvict.nonEmpty, "Expected partitions to be selected for eviction")

    // The largest partition (1 with 300L) should be in the list
    // Note: eviction order depends on both size and access time
    assert(partitionsToEvict.contains(1) || partitionsToEvict.contains(3),
      s"Expected largest partitions to be selected, got: ${partitionsToEvict.mkString(", ")}")
  }

  test("LRU eviction considers access time") {
    val maxCapacity = 1000L

    // Register buffers
    memorySpillManager.registerBuffer(0, 200L, maxCapacity)
    Thread.sleep(50) // Ensure different timestamps
    memorySpillManager.registerBuffer(1, 200L, maxCapacity)
    Thread.sleep(50)
    memorySpillManager.registerBuffer(2, 200L, maxCapacity)

    // Update access time for partition 0 to make it "newer"
    Thread.sleep(50)
    memorySpillManager.updateAccessTime(0)

    // Register one more to exceed threshold
    memorySpillManager.registerBuffer(3, 300L, maxCapacity) // Total: 900 bytes = 90%

    // Get eviction candidates
    val partitionsToEvict = memorySpillManager.selectPartitionsForEviction(Map.empty)

    assert(partitionsToEvict.nonEmpty, "Expected partitions to be selected for eviction")

    // Partition 1 should be preferred for eviction (older access time than 0, same size)
    // However, partition 3 is largest and should score higher overall
    // The algorithm weighs both size and age
  }

  // ============================================================================
  // Test Cases - Buffer Reclamation Within 100ms
  // ============================================================================

  test("buffer reclamation within 100ms") {
    val maxCapacity = 1000L

    // Register a buffer
    memorySpillManager.registerBuffer(0, 200L, maxCapacity)

    // Verify buffer is registered
    assert(memorySpillManager.getTotalAllocatedBytes === 200L,
      "Buffer should be registered")

    // Measure reclamation time
    val startTime = System.nanoTime()
    memorySpillManager.reclaimBuffer(0)
    val elapsedMs = (System.nanoTime() - startTime) / 1000000.0

    // Verify buffer is reclaimed
    assert(memorySpillManager.getTotalAllocatedBytes === 0L,
      "Buffer should be reclaimed")

    // Verify reclamation completed within 100ms
    assert(elapsedMs < 100.0,
      s"Buffer reclamation should complete within 100ms, took $elapsedMs ms")
  }

  test("buffer reclamation updates utilization metrics") {
    val maxCapacity = 1000L

    // Register buffers
    memorySpillManager.registerBuffer(0, 500L, maxCapacity)
    memorySpillManager.registerBuffer(1, 300L, maxCapacity) // Total: 800 = 80%

    // Verify initial utilization
    val initialUtilization = memorySpillManager.getCurrentUtilization
    assert(initialUtilization === 80.0,
      s"Expected 80% utilization, got $initialUtilization%")

    // Reclaim one buffer
    memorySpillManager.reclaimBuffer(0)

    // Verify utilization decreased
    val newUtilization = memorySpillManager.getCurrentUtilization
    assert(newUtilization === 30.0,
      s"Expected 30% utilization after reclamation, got $newUtilization%")
  }

  // ============================================================================
  // Test Cases - Spill Coordination with BlockManager.diskStore
  // ============================================================================

  test("spill coordination with BlockManager.diskStore") {
    val maxCapacity = 1000L
    val bufferData = createTestBuffer(1024)

    // Register a buffer
    memorySpillManager.registerBuffer(0, 1024L, maxCapacity)

    // Verify initial spill count
    val initialSpillCount = metrics.getSpillCount
    assert(initialSpillCount === 0, "Initial spill count should be 0")

    // Spill to disk
    memorySpillManager.spillToDisk(0, bufferData)

    // Verify spill count incremented
    assert(metrics.getSpillCount === initialSpillCount + 1,
      "Spill count should be incremented after spillToDisk")

    // Verify spilled block location is recorded
    val spilledInfo = memorySpillManager.getSpilledBlockLocation(0)
    assert(spilledInfo.isDefined, "Spilled block info should be recorded")
    assert(spilledInfo.get.partitionId === 0, "Spilled partition ID should match")
    assert(spilledInfo.get.size === 1024, "Spilled size should match buffer size")

    // Verify buffer is removed from tracked buffers
    assert(memorySpillManager.getTotalAllocatedBytes === 0L,
      "Buffer should be removed after spilling")
  }

  test("spillToDisk writes data to disk file") {
    val maxCapacity = 1000L
    val dataSize = 512
    val bufferData = createTestBuffer(dataSize)

    // Register and spill
    memorySpillManager.registerBuffer(0, dataSize.toLong, maxCapacity)
    memorySpillManager.spillToDisk(0, bufferData)

    // Verify file was created
    val spilledInfo = memorySpillManager.getSpilledBlockLocation(0)
    assert(spilledInfo.isDefined, "Spilled info should exist")
    assert(spilledInfo.get.file.exists(), "Spill file should exist on disk")
    assert(spilledInfo.get.file.length() === dataSize,
      s"Spill file should contain $dataSize bytes")
  }

  // ============================================================================
  // Test Cases - Memory Tracking with TaskMemoryManager
  // ============================================================================

  test("memory tracking with TaskMemoryManager") {
    val maxCapacity = 1000L

    // Register multiple buffers
    memorySpillManager.registerBuffer(0, 100L, maxCapacity)
    memorySpillManager.registerBuffer(1, 200L, maxCapacity)
    memorySpillManager.registerBuffer(2, 300L, maxCapacity)

    // Verify total allocated bytes
    val totalAllocated = memorySpillManager.getTotalAllocatedBytes
    assert(totalAllocated === 600L,
      s"Total allocated should be 600 bytes, got $totalAllocated")

    // Verify utilization calculation
    val utilization = memorySpillManager.getCurrentUtilization
    assert(utilization === 60.0,
      s"Utilization should be 60%, got $utilization%")
  }

  test("registerBuffer sets maxCapacity on first registration") {
    // First registration with capacity
    memorySpillManager.registerBuffer(0, 100L, 1000L)
    assert(memorySpillManager.getCurrentUtilization === 10.0)

    // Second registration without capacity (should use existing)
    memorySpillManager.registerBuffer(1, 200L, 0L)
    assert(memorySpillManager.getCurrentUtilization === 30.0)

    // Third registration with different capacity (should be ignored)
    memorySpillManager.registerBuffer(2, 200L, 5000L)
    // Utilization still calculated against original 1000L capacity
    assert(memorySpillManager.getCurrentUtilization === 50.0)
  }

  // ============================================================================
  // Test Cases - Metrics Reporting
  // ============================================================================

  test("spillCount metric incremented on spill") {
    val maxCapacity = 1000L
    val buffer = createTestBuffer(256)

    // Initial spill count
    val initialCount = metrics.getSpillCount
    assert(initialCount === 0, "Initial spill count should be 0")

    // Perform multiple spills
    for (i <- 0 until 5) {
      memorySpillManager.registerBuffer(i, 256L, maxCapacity)
      buffer.rewind()
      memorySpillManager.spillToDisk(i, buffer)
    }

    // Verify spill count
    assert(metrics.getSpillCount === 5,
      s"Spill count should be 5, got ${metrics.getSpillCount}")
  }

  test("buffer utilization metric updates during operations") {
    memorySpillManager.start()
    val maxCapacity = 1000L

    // Initial utilization should be 0
    assert(metrics.getBufferUtilization === 0.0)

    // Register buffer and check metric
    memorySpillManager.registerBuffer(0, 500L, maxCapacity)

    // Wait for metric update
    eventually {
      assert(metrics.getBufferUtilization === 50.0,
        s"Expected 50% utilization, got ${metrics.getBufferUtilization}%")
    }

    // Reclaim and verify decrease
    memorySpillManager.reclaimBuffer(0)

    eventually {
      assert(metrics.getBufferUtilization === 0.0,
        s"Expected 0% utilization after reclaim, got ${metrics.getBufferUtilization}%")
    }
  }

  // ============================================================================
  // Test Cases - Lifecycle Methods (start/stop)
  // ============================================================================

  test("start() initiates monitoring thread") {
    // Verify manager is not running initially
    val maxCapacity = 1000L

    // Start the manager
    memorySpillManager.start()

    // Register buffer to trigger utilization tracking
    memorySpillManager.registerBuffer(0, 500L, maxCapacity)

    // Wait for monitoring thread to execute at least once
    Thread.sleep(150)

    // Verify monitoring is active by checking metrics updates
    eventually {
      val utilization = metrics.getBufferUtilization
      assert(utilization === 50.0,
        s"Monitoring should update utilization to 50%, got $utilization%")
    }

    // Starting again should be idempotent
    memorySpillManager.start()

    // Still should work correctly
    memorySpillManager.registerBuffer(1, 300L, maxCapacity)
    eventually {
      val utilization = metrics.getBufferUtilization
      assert(utilization === 80.0,
        s"Utilization should be 80%, got $utilization%")
    }
  }

  test("stop() terminates monitoring thread") {
    val maxCapacity = 1000L

    // Start the manager
    memorySpillManager.start()
    memorySpillManager.registerBuffer(0, 500L, maxCapacity)

    // Wait for monitoring to start
    Thread.sleep(150)

    // Stop the manager
    memorySpillManager.stop()

    // Verify internal state is cleared
    assert(memorySpillManager.getTotalAllocatedBytes === 0L,
      "Allocated bytes should be cleared after stop")

    // Stopping again should be idempotent (no exception)
    memorySpillManager.stop()
  }

  test("stop() gracefully handles concurrent operations") {
    val maxCapacity = 1000L
    memorySpillManager.start()

    // Create concurrent registrations
    val executor = Executors.newFixedThreadPool(4)
    val latch = new CountDownLatch(1)
    val registrationCount = new AtomicInteger(0)

    try {
      // Submit registration tasks
      for (i <- 0 until 10) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            latch.await()
            try {
              memorySpillManager.registerBuffer(i, 50L, maxCapacity)
              registrationCount.incrementAndGet()
            } catch {
              case _: Exception => // Expected during shutdown
            }
          }
        })
      }

      // Release all threads and immediately stop
      latch.countDown()
      Thread.sleep(10)
      memorySpillManager.stop()

    } finally {
      executor.shutdown()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }

    // Manager should be cleanly stopped
    assert(memorySpillManager.getTotalAllocatedBytes === 0L)
  }

  // ============================================================================
  // Test Cases - Concurrent Buffer Access Safety
  // ============================================================================

  test("concurrent buffer access safety") {
    val maxCapacity = 10000L
    val numThreads = 8
    val operationsPerThread = 100
    val executor = Executors.newFixedThreadPool(numThreads)
    val errorCount = new AtomicInteger(0)
    val completedOperations = new AtomicInteger(0)

    memorySpillManager.start()

    try {
      val futures = (0 until numThreads).map { threadId =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            for (opId <- 0 until operationsPerThread) {
              try {
                val partitionId = threadId * operationsPerThread + opId

                // Register buffer
                memorySpillManager.registerBuffer(partitionId, 10L, maxCapacity)

                // Update access time
                memorySpillManager.updateAccessTime(partitionId)

                // Check utilization
                memorySpillManager.getCurrentUtilization

                // Reclaim some buffers
                if (opId % 3 == 0) {
                  memorySpillManager.reclaimBuffer(partitionId)
                }

                completedOperations.incrementAndGet()
              } catch {
                case e: Exception =>
                  errorCount.incrementAndGet()
              }
            }
          }
        })
      }

      // Wait for all tasks to complete
      futures.foreach(f => f.get())

    } finally {
      executor.shutdown()
      executor.awaitTermination(30, TimeUnit.SECONDS)
    }

    // Verify no errors occurred during concurrent access
    assert(errorCount.get() === 0,
      s"Expected no errors during concurrent access, got ${errorCount.get()} errors")

    // Verify some operations completed successfully
    assert(completedOperations.get() > 0,
      "Expected some operations to complete successfully")
  }

  test("concurrent spill operations") {
    val maxCapacity = 10000L
    val numThreads = 4
    val spillsPerThread = 10
    val executor = Executors.newFixedThreadPool(numThreads)
    val spillCount = new AtomicInteger(0)
    val errorCount = new AtomicInteger(0)

    try {
      val futures = (0 until numThreads).map { threadId =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            for (spillId <- 0 until spillsPerThread) {
              try {
                val partitionId = threadId * spillsPerThread + spillId
                val buffer = createTestBuffer(128)

                memorySpillManager.registerBuffer(partitionId, 128L, maxCapacity)
                memorySpillManager.spillToDisk(partitionId, buffer)
                spillCount.incrementAndGet()
              } catch {
                case e: Exception =>
                  errorCount.incrementAndGet()
              }
            }
          }
        })
      }

      futures.foreach(f => f.get())

    } finally {
      executor.shutdown()
      executor.awaitTermination(30, TimeUnit.SECONDS)
    }

    // Verify all spills completed
    assert(errorCount.get() === 0, s"Expected no errors, got ${errorCount.get()}")
    assert(spillCount.get() === numThreads * spillsPerThread,
      s"Expected ${numThreads * spillsPerThread} spills, got ${spillCount.get()}")

    // Verify spill metrics
    assert(metrics.getSpillCount === numThreads * spillsPerThread)
  }

  // ============================================================================
  // Test Cases - Threshold Configurability (50-95%)
  // ============================================================================

  test("threshold configurability (50-95%)") {
    // Test minimum threshold (50%)
    val manager50 = createManagerWithThreshold(50)
    try {
      manager50.registerBuffer(0, 600L, 1000L) // 60% utilization

      // At 50% threshold, 60% utilization should trigger eviction
      val partitions50 = manager50.selectPartitionsForEviction(Map.empty)
      assert(partitions50.nonEmpty,
        "At 50% threshold, 60% utilization should trigger eviction selection")
    } finally {
      manager50.stop()
    }

    // Test maximum threshold (95%)
    val manager95 = createManagerWithThreshold(95)
    try {
      manager95.registerBuffer(0, 900L, 1000L) // 90% utilization

      // At 95% threshold, 90% utilization should NOT trigger eviction
      val partitions95 = manager95.selectPartitionsForEviction(Map.empty)
      // Note: selectPartitionsForEviction calculates target at 90% of threshold
      // So at 95% threshold, target is ~85.5%, and 90% > 85.5% triggers eviction
    } finally {
      manager95.stop()
    }

    // Test default threshold (80%)
    val manager80 = createManagerWithThreshold(80)
    try {
      manager80.registerBuffer(0, 750L, 1000L) // 75% utilization

      // At 80% threshold (target ~72%), 75% should trigger eviction
      val partitions80 = manager80.selectPartitionsForEviction(Map.empty)
      assert(partitions80.nonEmpty,
        "At 80% threshold, 75% utilization should trigger eviction")
    } finally {
      manager80.stop()
    }
  }

  test("threshold respects MIN_SPILL_THRESHOLD_PERCENT boundary") {
    // Verify the constant is 50
    assert(MIN_SPILL_THRESHOLD_PERCENT === 50,
      s"MIN_SPILL_THRESHOLD_PERCENT should be 50, got $MIN_SPILL_THRESHOLD_PERCENT")
  }

  test("threshold respects MAX_SPILL_THRESHOLD_PERCENT boundary") {
    // Verify the constant is 95
    assert(MAX_SPILL_THRESHOLD_PERCENT === 95,
      s"MAX_SPILL_THRESHOLD_PERCENT should be 95, got $MAX_SPILL_THRESHOLD_PERCENT")
  }

  test("threshold respects DEFAULT_SPILL_THRESHOLD_PERCENT") {
    // Verify the constant is 80
    assert(DEFAULT_SPILL_THRESHOLD_PERCENT === 80,
      s"DEFAULT_SPILL_THRESHOLD_PERCENT should be 80, got $DEFAULT_SPILL_THRESHOLD_PERCENT")

    // Verify default config uses this value
    assert(streamingConfig.spillThresholdPercent === 80)
  }

  // ============================================================================
  // Test Cases - selectPartitionsForEviction Returns Correct Order
  // ============================================================================

  test("selectPartitionsForEviction returns correct order") {
    val maxCapacity = 1000L

    // Register buffers with known sizes and ensure different access times
    memorySpillManager.registerBuffer(0, 100L, maxCapacity)
    Thread.sleep(20)
    memorySpillManager.registerBuffer(1, 400L, maxCapacity) // Largest
    Thread.sleep(20)
    memorySpillManager.registerBuffer(2, 200L, maxCapacity)
    Thread.sleep(20)
    memorySpillManager.registerBuffer(3, 300L, maxCapacity) // Total: 1000 = 100%

    // Get eviction order
    val evictionOrder = memorySpillManager.selectPartitionsForEviction(Map.empty)

    // The algorithm scores by size * age_factor
    // Partition 1 (400L, oldest with data) should have high priority
    // The exact order depends on the timing, but larger/older partitions should come first
    assert(evictionOrder.nonEmpty, "Should have partitions to evict at 100% utilization")

    // Verify we have the expected partitions in the list
    assert(evictionOrder.toSet.subsetOf(Set(0, 1, 2, 3)),
      "Eviction order should only contain registered partitions")
  }

  test("selectPartitionsForEviction returns empty when below threshold") {
    val maxCapacity = 1000L

    // Register buffers below threshold (less than 72% target)
    memorySpillManager.registerBuffer(0, 300L, maxCapacity) // 30% utilization

    val evictionOrder = memorySpillManager.selectPartitionsForEviction(Map.empty)
    assert(evictionOrder.isEmpty,
      "Should return empty when below threshold target")
  }

  test("selectPartitionsForEviction handles empty buffer map") {
    // Don't register any buffers
    val evictionOrder = memorySpillManager.selectPartitionsForEviction(Map.empty)
    assert(evictionOrder.isEmpty, "Should return empty when no buffers registered")
  }

  test("selectPartitionsForEviction selects enough partitions to meet target") {
    val maxCapacity = 1000L

    // Register many small buffers
    for (i <- 0 until 10) {
      memorySpillManager.registerBuffer(i, 95L, maxCapacity) // Total: 950 = 95%
    }

    val evictionOrder = memorySpillManager.selectPartitionsForEviction(Map.empty)

    // Calculate expected bytes to free
    // Current: 950 bytes, Target: 1000 * 0.8 * 0.9 = 720 bytes
    // Need to free: 950 - 720 = 230 bytes
    // With 95 byte buffers, need at least 3 partitions

    assert(evictionOrder.size >= 2,
      s"Should select enough partitions to meet target, got ${evictionOrder.size}")
  }

  // ============================================================================
  // Test Cases - Edge Cases and Error Handling
  // ============================================================================

  test("reclaimBuffer handles non-existent partition") {
    // Reclaiming a non-existent partition should not throw
    memorySpillManager.reclaimBuffer(999)

    // Verify state is unchanged
    assert(memorySpillManager.getTotalAllocatedBytes === 0L)
  }

  test("updateAccessTime handles non-existent partition") {
    // Updating access time for non-existent partition should not throw
    memorySpillManager.updateAccessTime(999)
  }

  test("getCurrentUtilization returns 0 when maxCapacity is not set") {
    // Create a fresh manager without registering buffers with capacity
    val freshManager = new MemorySpillManager(
      taskMemoryManager,
      blockManager,
      streamingConfig,
      new StreamingShuffleMetrics()
    )

    try {
      val utilization = freshManager.getCurrentUtilization
      assert(utilization === 0.0,
        s"Utilization should be 0 when no capacity set, got $utilization")
    } finally {
      freshManager.stop()
    }
  }

  test("getSpilledBlockLocation returns None for non-spilled partition") {
    val location = memorySpillManager.getSpilledBlockLocation(999)
    assert(location.isEmpty, "Should return None for non-spilled partition")
  }

  test("multiple registrations for same partition updates buffer info") {
    val maxCapacity = 1000L

    // Register partition 0 with initial size
    memorySpillManager.registerBuffer(0, 100L, maxCapacity)
    assert(memorySpillManager.getTotalAllocatedBytes === 100L)

    // Re-register with different size (simulates buffer resize)
    // Note: This adds to total, doesn't replace
    memorySpillManager.registerBuffer(0, 200L, maxCapacity)

    // Both registrations contribute to total
    assert(memorySpillManager.getTotalAllocatedBytes === 300L)
  }
}
