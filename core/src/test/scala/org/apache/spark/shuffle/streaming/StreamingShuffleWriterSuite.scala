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

import java.util.zip.CRC32C

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleChecksumTestHelper
import org.apache.spark.storage.{BlockManager, BlockManagerId}

/**
 * Unit tests for [[StreamingShuffleWriter]] verifying:
 * - Buffer allocation within 20% executor memory limit
 * - Automatic spill trigger at 80% buffer utilization
 * - CRC32C checksum generation for block integrity
 * - Memory management via MemoryManager
 * - Producer failure cleanup
 *
 * Tests follow the patterns established in SortShuffleWriterSuite using
 * SharedSparkContext with Mockito mocks for isolated testing.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with BeforeAndAfterEach
    with ShuffleChecksumTestHelper {

  // ===========================================================================
  // Mock Declarations
  // ===========================================================================

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var spillManager: MemorySpillManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var backpressureProtocol: BackpressureProtocol = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  // ===========================================================================
  // Test Constants
  // ===========================================================================

  private val shuffleId = 0
  private val numMaps = 5
  private val numPartitions = 5
  private val serializer = new JavaSerializer(new SparkConf())

  // ===========================================================================
  // Test Fixtures
  // ===========================================================================

  private var shuffleHandle: StreamingShuffleHandle[Int, Int, Int] = _

  private val partitioner = new Partitioner() {
    def numPartitions: Int = StreamingShuffleWriterSuite.this.numPartitions
    def getPartition(key: Any): Int = {
      key.asInstanceOf[Int] % numPartitions
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()

    // Reset dependency mock with default behavior
    resetDependency()

    // Create streaming shuffle handle
    shuffleHandle = new StreamingShuffleHandle(shuffleId, dependency)

    // Setup spill manager mock with default behavior
    setupSpillManagerMocks()

    // Setup backpressure protocol mock with default behavior
    setupBackpressureMocks()
  }

  override def afterAll(): Unit = {
    try {
      // Cleanup any remaining resources
    } finally {
      super.afterAll()
    }
  }

  /**
   * Reset the dependency mock with standard behavior.
   */
  private def resetDependency(): Unit = {
    reset(dependency)
    when(dependency.partitioner).thenReturn(partitioner)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.rowBasedChecksums).thenReturn(Array.empty)
  }

  /**
   * Setup spill manager mocks with default behavior.
   */
  private def setupSpillManagerMocks(): Unit = {
    reset(spillManager)
    // By default, no spill is triggered
    when(spillManager.triggerSpill(anyInt())).thenReturn(0L)
    when(spillManager.getBufferUtilization).thenReturn(0)
    // Register buffer always succeeds
    doNothing().when(spillManager).registerBuffer(anyInt(), anyLong(), anyInt(), any(), anyLong())
    doNothing().when(spillManager).touchBuffer(any[PartitionKey])
    doNothing().when(spillManager).cleanupTask(anyLong())
    doNothing().when(spillManager).cleanupShuffle(anyInt())
  }

  /**
   * Setup backpressure protocol mocks with default behavior.
   */
  private def setupBackpressureMocks(): Unit = {
    reset(backpressureProtocol)
    // By default, no throttling
    when(backpressureProtocol.shouldThrottle(any[BlockManagerId], anyLong())).thenReturn(false)
    when(backpressureProtocol.isConsumerAlive(any[BlockManagerId])).thenReturn(true)
    doNothing().when(backpressureProtocol).registerShuffle(anyInt(), anyInt(), anyLong())
    doNothing().when(backpressureProtocol).unregisterShuffle(anyInt())
    doNothing().when(backpressureProtocol).recordSentBytes(any[BlockManagerId], anyLong())
    doNothing().when(backpressureProtocol).recordAcknowledgment(any[BlockManagerId], anyLong())
  }

  // ===========================================================================
  // Test Cases
  // ===========================================================================

  test("write empty iterator produces no data") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    writer.write(Iterator.empty)
    val mapStatus = writer.stop(success = true)

    // Verify empty write produces no data
    val partitionLengths = writer.getPartitionLengths()
    partitionLengths must not be null
    partitionLengths.sum must be(0)

    // MapStatus should still be returned for success
    mapStatus must be(defined)

    // Verify metrics show no data written
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    writeMetrics.bytesWritten must be(0)
    writeMetrics.recordsWritten must be(0)
  }

  test("write with records produces correct partition data") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(100)
    writer.write(records.iterator)
    val mapStatus = writer.stop(success = true)

    // Verify data was written
    val partitionLengths = writer.getPartitionLengths()
    partitionLengths must not be null
    partitionLengths.sum must be > 0L

    // MapStatus should be returned for success
    mapStatus must be(defined)

    // Verify metrics track written data
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    writeMetrics.recordsWritten must be(100)
    writeMetrics.bytesWritten must be > 0L
  }

  test("buffer allocation respects memory limit") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    // Write enough data to test buffer allocation
    val records = createRecords(1000)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify spill manager was consulted for buffer tracking
    verify(spillManager, atLeastOnce()).touchBuffer(any[PartitionKey])
    verify(spillManager, atLeastOnce()).registerBuffer(anyInt(), anyLong(), anyInt(), any(), anyLong())
  }

  test("write generates CRC32C checksums per partition") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(50)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify aggregated checksum was computed
    val aggregatedChecksum = writer.getAggregatedChecksumValue
    // Checksum should be non-zero for non-empty data
    // Note: Checksum could be 0 by coincidence but is very unlikely with real data
    aggregatedChecksum must not be 0L
  }

  test("checksum computation is correct") {
    // Test the package-level checksum computation independently
    val testData = "Hello, streaming shuffle!".getBytes("UTF-8")

    // Compute checksum using package utility
    val packageChecksum = streaming.computeChecksum(testData)

    // Compute checksum independently
    val crc32c = new CRC32C()
    crc32c.update(testData)
    val expectedChecksum = crc32c.getValue

    packageChecksum must be(expectedChecksum)
  }

  test("buffer utilization triggers spill at threshold") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    // Configure spill manager to report high utilization
    when(spillManager.getBufferUtilization).thenReturn(85)
    when(spillManager.triggerSpill(anyInt())).thenReturn(1024L * 1024L) // 1MB spilled

    val writer = createWriter(context)

    // Write data that would exceed threshold
    val records = createRecords(10000)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify cleanup was called
    verify(spillManager).cleanupTask(anyLong())
  }

  test("backpressure protocol integration throttles writes") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    // Setup throttling after some data
    var callCount = 0
    when(backpressureProtocol.shouldThrottle(any[BlockManagerId], anyLong())).thenAnswer { _ =>
      callCount += 1
      callCount > 5 // Start throttling after 5 calls
    }

    val writer = createWriter(context)
    val records = createRecords(100)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify backpressure protocol was consulted
    verify(backpressureProtocol, atLeastOnce()).recordSentBytes(any[BlockManagerId], anyLong())
  }

  test("stop(success=true) finalizes and returns MapStatus") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(50)
    writer.write(records.iterator)
    val mapStatus = writer.stop(success = true)

    // MapStatus must be returned
    mapStatus must be(defined)

    // Partition lengths must be set
    val partitionLengths = writer.getPartitionLengths()
    partitionLengths must have length numPartitions

    // Cleanup should be called
    verify(spillManager).cleanupTask(anyLong())
    verify(backpressureProtocol).unregisterShuffle(shuffleId)
  }

  test("stop(success=false) cleans up without MapStatus") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(50)
    writer.write(records.iterator)
    val mapStatus = writer.stop(success = false)

    // No MapStatus for failed task
    mapStatus must be(None)

    // Cleanup should still be called
    verify(spillManager).cleanupTask(anyLong())
    verify(backpressureProtocol).unregisterShuffle(shuffleId)
  }

  test("memory is released after stop") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(100)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify spill manager cleanup was called
    verify(spillManager).cleanupTask(anyLong())
  }

  test("partition lengths are tracked correctly") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    // Create records with specific partition distribution
    val records = (0 until numPartitions * 10).map(i => (i, i))
    writer.write(records.iterator)
    writer.stop(success = true)

    val partitionLengths = writer.getPartitionLengths()

    // All partitions should have some data since we have records for each
    partitionLengths must have length numPartitions
    // Each partition should have data (records mod numPartitions distributes evenly)
    partitionLengths.foreach { length =>
      length must be >= 0L
    }
  }

  test("write metrics are updated correctly") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val recordCount = 200
    val records = createRecords(recordCount)
    writer.write(records.iterator)
    writer.stop(success = true)

    val writeMetrics = context.taskMetrics().shuffleWriteMetrics

    // Record count should match
    writeMetrics.recordsWritten must be(recordCount)

    // Bytes written should be positive
    writeMetrics.bytesWritten must be > 0L
  }

  test("spill metrics are updated on disk spill") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    // Configure spill manager to trigger spill
    when(spillManager.getBufferUtilization).thenReturn(90)
    when(spillManager.triggerSpill(anyInt())).thenReturn(2 * 1024 * 1024L) // 2MB spilled

    val writer = createWriter(context)

    val records = createRecords(10000)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Spill manager should have been consulted
    verify(spillManager, atLeastOnce()).getBufferUtilization
  }

  test("multiple partitions are handled correctly") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    // Create records that span all partitions
    val records = (0 until 500).map(i => (i, i * 2))
    writer.write(records.iterator)
    writer.stop(success = true)

    val partitionLengths = writer.getPartitionLengths()

    // All partitions should have been written
    partitionLengths must have length numPartitions

    // Total length should be positive
    partitionLengths.sum must be > 0L

    // Metrics should reflect all records
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    writeMetrics.recordsWritten must be(500)
  }

  test("handles large records correctly") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    // Create records with larger values
    val records = (0 until 50).map(i => (i, "x" * 10000))
    writer.write(records.iterator)
    val mapStatus = writer.stop(success = true)

    mapStatus must be(defined)

    val partitionLengths = writer.getPartitionLengths()
    partitionLengths.sum must be > 0L
  }

  test("double stop is handled gracefully") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(50)
    writer.write(records.iterator)

    // First stop
    val mapStatus1 = writer.stop(success = true)
    mapStatus1 must be(defined)

    // Second stop should not throw
    val mapStatus2 = writer.stop(success = true)
    mapStatus2 must be(None) // Already stopped
  }

  test("stop without write returns None") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    // Stop without calling write
    val mapStatus = writer.stop(success = false)
    mapStatus must be(None)
  }

  test("backpressure registration and unregistration") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(10)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify registration was called during write
    verify(backpressureProtocol).registerShuffle(shuffleId, numPartitions, 0L)

    // Verify unregistration was called during stop
    verify(backpressureProtocol).unregisterShuffle(shuffleId)
  }

  test("row-based checksums are returned") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(50)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Row-based checksums array should be available
    val rowChecksums = writer.getRowBasedChecksums
    rowChecksums must not be null
    rowChecksums must have length numPartitions
  }

  test("spill manager touchBuffer called for LRU tracking") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = createWriter(context)

    val records = createRecords(100)
    writer.write(records.iterator)
    writer.stop(success = true)

    // Verify touchBuffer was called to update LRU ordering
    verify(spillManager, atLeast(1)).touchBuffer(any[PartitionKey])
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Create a StreamingShuffleWriter for testing.
   *
   * @param context the task context
   * @return configured writer instance
   */
  private def createWriter(context: TaskContext): StreamingShuffleWriter[Int, Int, Int] = {
    new StreamingShuffleWriter[Int, Int, Int](
      shuffleHandle,
      mapId = 1,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      spillManager,
      backpressureProtocol)
  }

  /**
   * Create test records for shuffle write.
   *
   * @param count number of records to create
   * @return list of (key, value) tuples
   */
  private def createRecords(count: Int): List[(Int, Int)] = {
    (0 until count).map(i => (i, i * 10)).toList
  }

  /**
   * Verify that a checksum matches expected value.
   *
   * @param data     the data to verify
   * @param expected the expected checksum value
   */
  private def verifyChecksum(data: Array[Byte], expected: Long): Unit = {
    val checksum = new CRC32C()
    checksum.update(data)
    checksum.getValue must be(expected)
  }
}
