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

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkConf,
  SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * Unit tests for [[StreamingShuffleManager]] (feature F-101), the SPI entry point of the
 * streaming shuffle data path. These tests focus on the two behaviors that the CP2 review
 * flagged as missing:
 *
 *  - '''Dual-flag activation (M9).''' The streaming path engages only when '''both'''
 *    `spark.shuffle.manager` selects the streaming manager through the `"streaming"` alias and
 *    `spark.shuffle.streaming.enabled=true`. Selecting the manager by fully-qualified class name,
 *    or leaving either flag unset, must keep the path disengaged and delegate to the inner
 *    sort-based manager.
 *  - '''Automatic, per-shuffle fallback (M10).''' When a degradation condition holds at
 *    registration time the whole shuffle is registered on the inner
 *    [[org.apache.spark.shuffle.sort.SortShuffleManager]] (a non-streaming handle) and is not
 *    tracked as streaming, so the writer and reader consistently route to the sort path.
 *
 * The suite extends [[SharedSparkContext]] so that a real [[org.apache.spark.SparkEnv]] is
 * available (the manager's fallback policy, backpressure protocol, and metrics source are all
 * gated on an active `SparkEnv`). Each manager under test is constructed from a fresh, isolated
 * [[SparkConf]] so the activation matrix can be exercised independently of the shared context's
 * own configuration; `SparkEnv.get` is the shared driver environment regardless.
 *
 * Managers are constructed with `isDriver = true` so no executor-only collaborators (the spill
 * monitor's polling thread, the backpressure RPC endpoint) are started, and every constructed
 * manager is stopped to release its inner sort manager's resolver.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with SharedSparkContext {

  /** The `spark.shuffle.manager` short-name alias that selects the streaming manager. */
  private val streamingAlias = StreamingShuffleConfig.STREAMING_MANAGER_ALIAS

  /** The fully-qualified class name of the streaming manager (a non-alias selection form). */
  private val streamingFqcn = classOf[StreamingShuffleManager].getName

  /**
   * Builds an isolated [[SparkConf]] selecting the given shuffle manager and toggling the
   * streaming opt-in flag. `SparkConf(false)` skips loading system properties so the activation
   * matrix is fully determined by the two keys set here; the numeric `spark.shuffle.streaming.*`
   * tuning entries fall back to their (valid) defaults.
   */
  private def confWith(manager: String, enabled: Boolean): SparkConf = {
    new SparkConf(false)
      .set("spark.shuffle.manager", manager)
      .set("spark.shuffle.streaming.enabled", enabled.toString)
  }

  /** Runs `body` with a freshly constructed driver-side manager and always stops it afterward. */
  private def withManager(conf: SparkConf)(body: StreamingShuffleManager => Unit): Unit = {
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      body(manager)
    } finally {
      manager.stop()
    }
  }

  /** A hash partitioner over `numParts` partitions, as the sort delegation path expects. */
  private def newPartitioner(numParts: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = 0
  }

  /**
   * A mocked [[ShuffleDependency]] stubbed with exactly the members read on the registration
   * paths: the streaming handle constructor stores `shuffleId` and `dependency`, while the inner
   * sort manager's `registerShuffle` inspects the partitioner, serializer, aggregator, key
   * ordering, and map-side-combine flag to choose its handle subtype.
   */
  private def newDependency(shuffleId: Int, numParts: Int): ShuffleDependency[Int, Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.shuffleId).thenReturn(shuffleId)
    when(dependency.partitioner).thenReturn(newPartitioner(numParts))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.mapSideCombine).thenReturn(false)
    dependency
  }

  private def isStreamingHandle(handle: ShuffleHandle): Boolean =
    handle.isInstanceOf[StreamingShuffleHandle[_, _, _]]

  // ------------------------------------------------------------------------------------------
  // M9 -- dual-flag activation
  // ------------------------------------------------------------------------------------------

  test("streaming activates only under the dual flag: alias AND enabled (M9)") {
    // Active: the streaming alias together with the opt-in flag.
    withManager(confWith(streamingAlias, enabled = true)) { manager =>
      assert(manager.isStreamingActive)
      assert(manager.streamingShuffleConfig.managerSelected)
      assert(manager.streamingShuffleConfig.enabled)
      assert(manager.streamingShuffleConfig.active)
    }

    // Inactive: alias selected but the opt-in flag is off.
    withManager(confWith(streamingAlias, enabled = false)) { manager =>
      assert(!manager.isStreamingActive)
      assert(manager.streamingShuffleConfig.managerSelected)
      assert(!manager.streamingShuffleConfig.enabled)
      assert(!manager.streamingShuffleConfig.active)
    }

    // Inactive: opt-in flag on but a different manager is selected.
    withManager(confWith("sort", enabled = true)) { manager =>
      assert(!manager.isStreamingActive)
      assert(!manager.streamingShuffleConfig.managerSelected)
      assert(manager.streamingShuffleConfig.enabled)
      assert(!manager.streamingShuffleConfig.active)
    }

    // Inactive: neither half of the contract holds.
    withManager(confWith("sort", enabled = false)) { manager =>
      assert(!manager.isStreamingActive)
      assert(!manager.streamingShuffleConfig.active)
    }
  }

  test("selecting the manager by class name does NOT engage streaming (alias-only, M9)") {
    // The published activation contract recognizes only the "streaming" alias; an FQCN selection
    // still instantiates this manager but must delegate every shuffle to the inner sort manager.
    withManager(confWith(streamingFqcn, enabled = true)) { manager =>
      assert(!manager.streamingShuffleConfig.managerSelected)
      assert(!manager.isStreamingActive)

      val handle = manager.registerShuffle(0, newDependency(0, 4))
      assert(!isStreamingHandle(handle),
        "FQCN selection must delegate registration to the inner sort manager")
      assert(manager.registeredStreamingShuffleCount === 0)
    }
  }

  // ------------------------------------------------------------------------------------------
  // registerShuffle dispatch
  // ------------------------------------------------------------------------------------------

  test("registerShuffle returns a StreamingShuffleHandle when active and no fallback holds") {
    withManager(confWith(streamingAlias, enabled = true)) { manager =>
      val handle = manager.registerShuffle(7, newDependency(7, 4))
      assert(isStreamingHandle(handle))
      val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[_, _, _]]
      assert(streamingHandle.shuffleId === 7)
      // The per-shuffle tuning values are captured from the resolved configuration.
      val cfg = manager.streamingShuffleConfig
      assert(streamingHandle.bufferSizePercent === cfg.bufferSizePercent)
      assert(streamingHandle.spillThreshold === cfg.spillThreshold)
      assert(manager.registeredStreamingShuffleCount === 1)
    }
  }

  test("registerShuffle delegates to the inner sort manager when streaming is inactive") {
    withManager(confWith(streamingAlias, enabled = false)) { manager =>
      val handle = manager.registerShuffle(3, newDependency(3, 4))
      assert(!isStreamingHandle(handle))
      assert(manager.registeredStreamingShuffleCount === 0)
    }
  }

  // ------------------------------------------------------------------------------------------
  // M10 -- automatic, per-shuffle fallback to sort
  // ------------------------------------------------------------------------------------------

  test("a registration-time fallback condition routes the whole shuffle to sort (M10)") {
    // Force the version-mismatch fallback condition by reporting a different consumer version.
    // The manager is otherwise active (alias + enabled), so the ONLY reason it falls back is the
    // policy decision -- proving the manager now acts on the fallback policy rather than only
    // logging it.
    val conf = confWith(streamingAlias, enabled = true)
    val manager = new StreamingShuffleManager(conf, isDriver = true) {
      override private[streaming] def consumerStreamingVersion: String = "9.9.9-mismatch"
    }
    try {
      assert(manager.isStreamingActive, "the dual flag is satisfied; fallback must be the cause")
      // The policy reports the version mismatch as the triggered reason.
      assert(manager.registrationFallbackReason().contains(
        StreamingShuffleFallbackPolicy.VersionMismatch))

      val handle = manager.registerShuffle(11, newDependency(11, 4))
      assert(!isStreamingHandle(handle),
        "a triggered fallback condition must register the shuffle on the sort path")
      assert(manager.registeredStreamingShuffleCount === 0,
        "a fell-back shuffle must not be tracked as streaming")
    } finally {
      manager.stop()
    }
  }

  test("no fallback condition holds under a healthy driver environment (M10)") {
    // With matching versions, available memory, and no timed-out consumer streams, the policy
    // returns no reason and the streaming path proceeds.
    withManager(confWith(streamingAlias, enabled = true)) { manager =>
      assert(manager.registrationFallbackReason().isEmpty)
    }
  }

  // ------------------------------------------------------------------------------------------
  // Collaborator gating and lifecycle
  // ------------------------------------------------------------------------------------------

  test("SparkEnv-gated collaborators are present on the driver; executor-only ones are not") {
    withManager(confWith(streamingAlias, enabled = true)) { manager =>
      // Present wherever a SparkEnv exists (including the driver in local mode).
      assert(manager.backpressureProtocol.isDefined)
      assert(manager.fallbackPolicy.isDefined)
      assert(manager.streamingMetricsHolder != null)
      // Executor-only: never created on the driver.
      assert(manager.memorySpillManager.isEmpty)
      assert(manager.backpressureEndpointRef.isEmpty)
      // The exposed block resolver is the shared sort IndexShuffleBlockResolver.
      assert(manager.shuffleBlockResolver eq manager.innerSortShuffleManager.shuffleBlockResolver)
    }
  }

  test("stop() tears the manager down deterministically and clears streaming registrations") {
    val manager = new StreamingShuffleManager(confWith(streamingAlias, enabled = true),
      isDriver = true)
    manager.registerShuffle(21, newDependency(21, 4))
    assert(manager.registeredStreamingShuffleCount === 1)
    // stop() must not throw and must clear the streaming registration bookkeeping.
    manager.stop()
    assert(manager.registeredStreamingShuffleCount === 0)
  }
}
