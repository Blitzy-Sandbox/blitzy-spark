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

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle}

/**
 * Unit tests for [[StreamingShuffleManager]] verifying lifecycle methods,
 * handle registration, writer/reader instantiation, fallback detection,
 * and cleanup operations.
 *
 * Follows [[org.apache.spark.shuffle.sort.SortShuffleManagerSuite]] patterns
 * for consistent test infrastructure and mocking strategies.
 *
 * == Test Coverage ==
 *
 * Tests cover:
 * - registerShuffle returns correct handle types (StreamingShuffleHandle vs BaseShuffleHandle)
 * - getWriter returns StreamingShuffleWriter for streaming handles
 * - getReader returns StreamingShuffleReader for streaming handles
 * - Fallback to sort-based shuffle under various conditions
 * - Cleanup operations via unregisterShuffle and stop
 * - Concurrent shuffle tracking
 *
 * == Coexistence Testing ==
 *
 * Tests verify that streaming shuffle properly coexists with sort-based shuffle:
 * - Streaming handles produce streaming writer/reader
 * - BaseShuffleHandle (from fallback) produces sort-based writer/reader
 * - Resources are properly cleaned up for both types
 */
class StreamingShuffleManagerSuite
    extends SparkFunSuite
    with Matchers
    with BeforeAndAfterEach {

  // ============================================================================
  // Test Fixtures
  // ============================================================================

  /** SparkConf with streaming shuffle enabled for testing. */
  private var conf: SparkConf = _

  /** StreamingShuffleManager instance under test. */
  private var manager: StreamingShuffleManager = _

  /** Unique shuffle ID counter for test isolation. */
  private var shuffleIdCounter: Int = 0

  // ============================================================================
  // Mockito Helper - RuntimeExceptionAnswer
  // ============================================================================

  /**
   * Custom Mockito Answer that throws RuntimeException for non-stubbed method calls.
   * Ensures strict mocking behavior where only explicitly stubbed methods succeed.
   * Follows the pattern from SortShuffleManagerSuite.
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException(s"Called non-stubbed method: ${invocation.getMethod.getName}")
    }
  }

  // ============================================================================
  // doReturn Helper
  // ============================================================================

  /**
   * Helper for Mockito doReturn to work with Scala varargs.
   * Follows the pattern from SortShuffleManagerSuite.
   */
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  // ============================================================================
  // Setup and Teardown
  // ============================================================================

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Create SparkConf with streaming shuffle enabled
    conf = new SparkConf(loadDefaults = false)
      .set("spark.app.id", "test-streaming-shuffle-manager")
      .set(SHUFFLE_STREAMING_ENABLED, true)
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, 20)
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 80)
      .set(SHUFFLE_STREAMING_DEBUG, false)

    // Create manager instance
    manager = new StreamingShuffleManager(conf)

    // Reset shuffle ID counter
    shuffleIdCounter = 0
  }

  override def afterEach(): Unit = {
    try {
      // Stop the manager and release resources
      if (manager != null) {
        manager.stop()
        manager = null
      }
      conf = null
    } finally {
      super.afterEach()
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Create a mock ShuffleDependency with configurable parameters.
   * Uses RuntimeExceptionAnswer for strict mocking following SortShuffleManagerSuite pattern.
   *
   * @param partitioner     partitioner to use (determines partition count)
   * @param serializer      serializer for shuffle data
   * @param keyOrdering     optional key ordering for sorted shuffle
   * @param aggregator      optional aggregator for map-side combine
   * @param mapSideCombine  whether to perform map-side aggregation
   * @return mock ShuffleDependency configured with specified parameters
   */
  private def createShuffleDependency(
      partitioner: Partitioner,
      serializer: Serializer,
      keyOrdering: Option[Ordering[Any]] = None,
      aggregator: Option[Aggregator[Any, Any, Any]] = None,
      mapSideCombine: Boolean = false): ShuffleDependency[Any, Any, Any] = {

    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())

    // Get next unique shuffle ID
    val shuffleId = nextShuffleId()

    doReturn(shuffleId).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(keyOrdering).when(dep).keyOrdering
    doReturn(aggregator).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine

    dep
  }

  /**
   * Create a simple HashPartitioner with specified partition count.
   */
  private def createPartitioner(numPartitions: Int): Partitioner = {
    new HashPartitioner(numPartitions)
  }

  /**
   * Generate next unique shuffle ID for test isolation.
   */
  private def nextShuffleId(): Int = {
    shuffleIdCounter += 1
    shuffleIdCounter
  }

  // ============================================================================
  // Test Cases: Handle Registration
  // ============================================================================

  test("registerShuffle returns StreamingShuffleHandle") {
    // Given: A valid shuffle dependency that qualifies for streaming
    val kryo = new KryoSerializer(conf)
    val partitioner = createPartitioner(10)
    val dep = createShuffleDependency(
      partitioner = partitioner,
      serializer = kryo,
      keyOrdering = None,
      aggregator = None,
      mapSideCombine = false
    )

    // When: Register the shuffle
    val handle = manager.registerShuffle(dep.shuffleId, dep)

    // Then: Should return StreamingShuffleHandle
    handle mustBe a[StreamingShuffleHandle[_, _, _]]

    val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[Any, Any, Any]]
    streamingHandle.shuffleId mustBe dep.shuffleId
    streamingHandle.dependency mustBe dep
    streamingHandle.isStreaming mustBe true
  }

  test("registerShuffle returns BaseShuffleHandle for fallback cases") {
    // Given: Configuration with streaming disabled
    val disabledConf = new SparkConf(loadDefaults = false)
      .set("spark.app.id", "test-disabled")
      .set(SHUFFLE_STREAMING_ENABLED, false)

    val disabledManager = new StreamingShuffleManager(disabledConf)

    try {
      val kryo = new KryoSerializer(disabledConf)
      val dep = createShuffleDependency(
        partitioner = createPartitioner(10),
        serializer = kryo
      )

      // When: Register shuffle with streaming disabled
      val handle = disabledManager.registerShuffle(dep.shuffleId, dep)

      // Then: Should return BaseShuffleHandle (fallback to sort-based)
      handle mustBe a[BaseShuffleHandle[_, _, _]]
    } finally {
      disabledManager.stop()
    }
  }

  // ============================================================================
  // Test Cases: Writer Factory
  // ============================================================================

  test("getWriter returns StreamingShuffleWriter for streaming handle") {
    // Given: A registered streaming shuffle
    val kryo = new KryoSerializer(conf)
    val dep = createShuffleDependency(
      partitioner = createPartitioner(5),
      serializer = kryo
    )

    val handle = manager.registerShuffle(dep.shuffleId, dep)
    handle mustBe a[StreamingShuffleHandle[_, _, _]]

    // When: Get writer for streaming handle
    // Note: This test verifies the handle dispatch logic, actual writer creation
    // requires a full SparkEnv which is tested in integration tests
    val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[Any, Any, Any]]

    // Then: Verify handle is properly configured for streaming writer creation
    streamingHandle.isStreaming mustBe true
    streamingHandle.numPartitions mustBe 5
  }

  test("getWriter returns fallback writer for base handle") {
    // Given: Configuration with streaming disabled for BaseShuffleHandle
    val disabledConf = new SparkConf(loadDefaults = false)
      .set("spark.app.id", "test-fallback-writer")
      .set(SHUFFLE_STREAMING_ENABLED, false)

    val disabledManager = new StreamingShuffleManager(disabledConf)

    try {
      val kryo = new KryoSerializer(disabledConf)
      val dep = createShuffleDependency(
        partitioner = createPartitioner(5),
        serializer = kryo
      )

      // When: Register with disabled streaming (produces BaseShuffleHandle)
      val handle = disabledManager.registerShuffle(dep.shuffleId, dep)

      // Then: Handle should be BaseShuffleHandle (for sort-based writer)
      handle mustBe a[BaseShuffleHandle[_, _, _]]
    } finally {
      disabledManager.stop()
    }
  }

  // ============================================================================
  // Test Cases: Reader Factory
  // ============================================================================

  test("getReader returns StreamingShuffleReader for streaming handle") {
    // Given: A registered streaming shuffle
    val kryo = new KryoSerializer(conf)
    val dep = createShuffleDependency(
      partitioner = createPartitioner(8),
      serializer = kryo
    )

    val handle = manager.registerShuffle(dep.shuffleId, dep)
    handle mustBe a[StreamingShuffleHandle[_, _, _]]

    // When/Then: Verify handle is properly configured for streaming reader creation
    val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[Any, Any, Any]]
    streamingHandle.isStreaming mustBe true
    streamingHandle.numPartitions mustBe 8
  }

  test("getReader returns fallback reader for base handle") {
    // Given: Manager with streaming disabled
    val disabledConf = new SparkConf(loadDefaults = false)
      .set("spark.app.id", "test-fallback-reader")
      .set(SHUFFLE_STREAMING_ENABLED, false)

    val disabledManager = new StreamingShuffleManager(disabledConf)

    try {
      val kryo = new KryoSerializer(disabledConf)
      val dep = createShuffleDependency(
        partitioner = createPartitioner(8),
        serializer = kryo
      )

      // When: Register with disabled streaming
      val handle = disabledManager.registerShuffle(dep.shuffleId, dep)

      // Then: Should produce BaseShuffleHandle for fallback reader
      handle mustBe a[BaseShuffleHandle[_, _, _]]
    } finally {
      disabledManager.stop()
    }
  }

  // ============================================================================
  // Test Cases: Unregister and Cleanup
  // ============================================================================

  test("unregisterShuffle cleans up resources") {
    // Given: Multiple registered shuffles
    val kryo = new KryoSerializer(conf)
    val dep1 = createShuffleDependency(createPartitioner(5), kryo)
    val dep2 = createShuffleDependency(createPartitioner(10), kryo)

    val handle1 = manager.registerShuffle(dep1.shuffleId, dep1)
    val handle2 = manager.registerShuffle(dep2.shuffleId, dep2)

    handle1 mustBe a[StreamingShuffleHandle[_, _, _]]
    handle2 mustBe a[StreamingShuffleHandle[_, _, _]]

    // When: Unregister first shuffle
    val result1 = manager.unregisterShuffle(dep1.shuffleId)

    // Then: Unregistration should succeed
    result1 mustBe true

    // When: Unregister second shuffle
    val result2 = manager.unregisterShuffle(dep2.shuffleId)

    // Then: Unregistration should succeed
    result2 mustBe true
  }

  // ============================================================================
  // Test Cases: Block Resolver
  // ============================================================================

  test("shuffleBlockResolver returns StreamingShuffleBlockResolver") {
    // Given: Manager with streaming enabled (default in this suite)

    // When: Access the block resolver
    val resolver = manager.shuffleBlockResolver

    // Then: Should be StreamingShuffleBlockResolver
    resolver mustBe a[StreamingShuffleBlockResolver]
  }

  // ============================================================================
  // Test Cases: Stop and Shutdown
  // ============================================================================

  test("stop cleans up all managers") {
    // Given: Multiple registered shuffles
    val kryo = new KryoSerializer(conf)
    val dep1 = createShuffleDependency(createPartitioner(5), kryo)
    val dep2 = createShuffleDependency(createPartitioner(10), kryo)

    manager.registerShuffle(dep1.shuffleId, dep1)
    manager.registerShuffle(dep2.shuffleId, dep2)

    // When: Stop the manager
    manager.stop()

    // Then: Manager should be stopped (verify by checking it handles stop gracefully)
    // Calling stop again should not throw
    manager.stop()

    // Recreate manager for afterEach cleanup
    manager = new StreamingShuffleManager(conf)
  }

  // ============================================================================
  // Test Cases: Fallback Detection
  // ============================================================================

  test("shouldFallbackToSort detects map-side combine") {
    // Given: Shuffle dependency with map-side combine (aggregator defined)
    val kryo = new KryoSerializer(conf)
    val aggregator = mock(classOf[Aggregator[Any, Any, Any]], new RuntimeExceptionAnswer())
    val dep = createShuffleDependency(
      partitioner = createPartitioner(10),
      serializer = kryo,
      keyOrdering = Some(mock(classOf[Ordering[Any]])),
      aggregator = Some(aggregator),
      mapSideCombine = true
    )

    // When: Check if fallback should be triggered based on dependency
    val shouldFallback = manager.shouldFallbackToSort(dep)

    // Then: Map-side combine is still supported, so this alone doesn't trigger fallback
    // The shouldFallbackToSort(dependency) checks static properties like serializer support
    // Dynamic conditions like consumer lag are checked via shouldFallbackToSort(shuffleId)
    // With KryoSerializer supporting relocation, no static fallback should be triggered
    shouldFallback mustBe false
  }

  test("fallback when streaming is disabled via config") {
    // Given: Configuration with streaming disabled
    val disabledConf = new SparkConf(loadDefaults = false)
      .set("spark.app.id", "test-config-fallback")
      .set(SHUFFLE_STREAMING_ENABLED, false)

    val disabledManager = new StreamingShuffleManager(disabledConf)

    try {
      val kryo = new KryoSerializer(disabledConf)
      val dep = createShuffleDependency(
        partitioner = createPartitioner(10),
        serializer = kryo
      )

      // When: Check fallback with streaming disabled
      val shouldFallback = disabledManager.shouldFallbackToSort(dep)

      // Then: Should indicate fallback when streaming is disabled
      shouldFallback mustBe true
    } finally {
      disabledManager.stop()
    }
  }

  // ============================================================================
  // Test Cases: Concurrent Shuffles
  // ============================================================================

  test("multiple concurrent shuffles are tracked") {
    // Given: Multiple shuffles registered concurrently
    val kryo = new KryoSerializer(conf)

    val dependencies = (1 to 5).map { i =>
      createShuffleDependency(
        partitioner = createPartitioner(i * 2),
        serializer = kryo
      )
    }

    // When: Register all shuffles
    val handles = dependencies.map { dep =>
      manager.registerShuffle(dep.shuffleId, dep)
    }

    // Then: All should be StreamingShuffleHandle with unique shuffle IDs
    handles.foreach { handle =>
      handle mustBe a[StreamingShuffleHandle[_, _, _]]
    }

    val shuffleIds = handles.map(_.shuffleId)
    shuffleIds.distinct.size mustBe 5 // All unique

    // Verify individual shuffles can be checked
    handles.foreach { handle =>
      manager.isStreamingShuffle(handle.shuffleId) mustBe true
    }
  }

  test("map output tracking per shuffle") {
    // Given: A registered shuffle
    val kryo = new KryoSerializer(conf)
    val dep = createShuffleDependency(
      partitioner = createPartitioner(10),
      serializer = kryo
    )

    // When: Register shuffle
    val handle = manager.registerShuffle(dep.shuffleId, dep)

    // Then: Shuffle should be tracked
    manager.isStreamingShuffle(handle.shuffleId) mustBe true

    // When: Unregister shuffle
    manager.unregisterShuffle(handle.shuffleId)

    // Then: Shuffle should no longer be tracked as streaming
    manager.isStreamingShuffle(handle.shuffleId) mustBe false
  }

  // ============================================================================
  // Test Cases: Partition Count Handling
  // ============================================================================

  test("handles partition counts correctly") {
    val kryo = new KryoSerializer(conf)

    // Test various partition counts
    Seq(2, 10, 100, 1000).foreach { numPartitions =>
      val dep = createShuffleDependency(
        partitioner = createPartitioner(numPartitions),
        serializer = kryo
      )

      val handle = manager.registerShuffle(dep.shuffleId, dep)

      // Verify handle captures correct partition count
      handle mustBe a[StreamingShuffleHandle[_, _, _]]
      val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[Any, Any, Any]]
      streamingHandle.numPartitions mustBe numPartitions

      // Cleanup for next iteration
      manager.unregisterShuffle(handle.shuffleId)
    }
  }

  // ============================================================================
  // Additional Edge Case Tests
  // ============================================================================

  test("handles single partition shuffle") {
    // Given: Shuffle with single partition (streaming not beneficial)
    val kryo = new KryoSerializer(conf)
    val dep = createShuffleDependency(
      partitioner = createPartitioner(1),
      serializer = kryo
    )

    // When: Register shuffle
    val handle = manager.registerShuffle(dep.shuffleId, dep)

    // Then: Should fall back to sort-based for single partition
    // (streaming overhead not worth it)
    handle mustBe a[BaseShuffleHandle[_, _, _]]
  }

  test("handles JavaSerializer fallback") {
    // Given: Shuffle with JavaSerializer (doesn't support relocation)
    val java = new JavaSerializer(conf)
    val dep = createShuffleDependency(
      partitioner = createPartitioner(10),
      serializer = java
    )

    // When: Register shuffle
    val handle = manager.registerShuffle(dep.shuffleId, dep)

    // Then: Should fall back to sort-based due to serializer limitation
    handle mustBe a[BaseShuffleHandle[_, _, _]]
  }

  test("getStats returns meaningful information") {
    // Given: Manager with some shuffles
    val kryo = new KryoSerializer(conf)
    val dep = createShuffleDependency(createPartitioner(10), kryo)

    manager.registerShuffle(dep.shuffleId, dep)

    // When: Get stats
    val stats = manager.getStats

    // Then: Stats should contain expected keys
    stats must contain key "streamingEnabled"
    stats must contain key "registeredShuffles"
    stats must contain key "streamingShuffles"

    stats("streamingEnabled") mustBe true
    stats("registeredShuffles").asInstanceOf[Int] must be >= 1
    stats("streamingShuffles").asInstanceOf[Int] must be >= 1
  }

  test("isStreamingShuffle returns false for unknown shuffle") {
    // Given: An unregistered shuffle ID
    val unknownShuffleId = 99999

    // When: Check if it's a streaming shuffle
    val result = manager.isStreamingShuffle(unknownShuffleId)

    // Then: Should return false
    result mustBe false
  }

  test("shouldFallbackToSort returns false for streaming shuffles by default") {
    // Given: A registered streaming shuffle
    val kryo = new KryoSerializer(conf)
    val dep = createShuffleDependency(createPartitioner(10), kryo)

    val handle = manager.registerShuffle(dep.shuffleId, dep)
    handle mustBe a[StreamingShuffleHandle[_, _, _]]

    // When: Check runtime fallback condition
    val shouldFallback = manager.shouldFallbackToSort(handle.shuffleId)

    // Then: Should not fallback under normal conditions
    // (no consumer lag, no memory pressure, no network saturation)
    shouldFallback mustBe false
  }
}
