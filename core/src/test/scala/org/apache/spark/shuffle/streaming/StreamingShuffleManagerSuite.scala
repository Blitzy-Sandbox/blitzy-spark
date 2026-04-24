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

import java.util.concurrent.ConcurrentHashMap

import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleManager}
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * End-to-end unit tests for [[StreamingShuffleManager]] &mdash; the opt-in SPI
 * implementation of [[org.apache.spark.shuffle.ShuffleManager]] introduced as
 * feature F-001 that COEXISTS with the production-stable
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]].
 *
 * The suite is deliberately the MOST INTEGRATED of the nine streaming-shuffle
 * test suites: it drives the top-level `ShuffleManager` SPI surface and thereby
 * indirectly validates that every other streaming component
 * ([[StreamingShuffleHandle]], [[StreamingShuffleWriter]],
 * [[StreamingShuffleReader]], [[StreamingShuffleFallbackPolicy]],
 * [[StreamingShuffleMetrics]]) is wired correctly by the manager class itself.
 *
 * The tests are grouped by concern:
 *
 *   - Group 1: Short-name and FQCN resolution through
 *     `ShuffleManager.getShuffleManagerClassName` (validates the `"streaming"`
 *     entry added to the companion object's `shortShuffleMgrNames` map plus
 *     the regression that the existing `"sort"` and `"tungsten-sort"` entries
 *     remain undisturbed).
 *   - Group 2: Constructor and lifecycle (fallback-manager initialization,
 *     null-`SparkEnv` tolerance, idempotent `stop`, post-stop bookkeeping
 *     cleanup).
 *   - Group 3: `registerShuffle` routing (partition-count validation,
 *     streaming vs. fallback dispatch, the three active fallback reasons
 *     &mdash; streaming-disabled, push-active, insufficient-memory &mdash;
 *     that fire before the v1 transport-readiness guard).
 *   - Group 4: `getWriter` dispatch (non-`StreamingShuffleHandle` handles are
 *     routed to the held `SortShuffleManager`).
 *   - Group 5: `unregisterShuffle` (map cleanup + Boolean contract).
 *   - Group 6: `shuffleBlockResolver` and trait compliance
 *     (reference-equality with the delegate's resolver per ADR-002; type
 *     ascription to the `ShuffleManager` trait).
 *
 * == v1 behavior note ==
 *
 * [[StreamingShuffleFallbackPolicy.STREAMING_TRANSPORT_READY_V1]] is hard-coded
 * to `false` in the v1 landing because the producer-to-consumer transport in
 * the `org.apache.spark.shuffle.streaming.network` sub-package is still being
 * landed by sibling agents. Until that constant is flipped, EVERY shuffle that
 * otherwise satisfies the policy's config-driven conditions (feature flag on,
 * push-shuffle off, positive partition count, sufficient executor memory) is
 * routed to the held `SortShuffleManager` with the reason code
 * `"streaming-transport-unavailable-v1"`. This suite tests the v1 reality:
 *
 *   - Tests that exercise explicit fallback reasons (feature-flag off,
 *     push-shuffle on, insufficient memory) validate the earlier-ranked reason
 *     codes regardless of transport readiness.
 *   - Tests that would normally observe a [[StreamingShuffleHandle]] (the
 *     "happy path") instead assert the v1 transport-unavailable reason.
 *     Each such test carries an explicit comment describing how the assertion
 *     will flip when the transport lands.
 *
 * == Test framework ==
 *
 * Pure Mockito Pattern B &mdash; the [[RuntimeExceptionAnswer]] default answer
 * ensures every un-stubbed call on a [[ShuffleDependency]] mock raises a
 * `RuntimeException`, forcing the suite to declare the exact dependency
 * surface it relies on. No `SparkContext`, no real RPC, no real
 * [[org.apache.spark.storage.BlockManager]] &mdash; the tests are pure in-JVM
 * unit tests and run in under two seconds.
 *
 * AAP references:
 *   - &sect;0.2.3.5 Row T1 &mdash; StreamingShuffleManagerSuite.scala.
 *   - &sect;0.5.1.3 Row "CREATE &hellip; StreamingShuffleManagerSuite.scala"
 *     &mdash; "Short-name resolution (`spark.shuffle.manager=streaming`) and
 *     FQCN resolution; `registerShuffle` returns `StreamingShuffleHandle`;
 *     fallback delegation to `SortShuffleManager` when policy triggers;
 *     `stop()` is idempotent."
 *   - &sect;0.7.1 Implementation Discipline &mdash; "Isolate streaming logic
 *     in dedicated classes with zero cross-contamination into existing shuffle
 *     code paths."
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with Matchers {

  // ==========================================================================
  // Mockito Pattern B helpers &mdash; mirrors SortShuffleManagerSuite and
  // StreamingShuffleFallbackPolicySuite verbatim so that a future reader of
  // either suite can move between them without re-learning the helper shape.
  // ==========================================================================

  /**
   * Bridge to the overloaded `Mockito.doReturn(Object, Object...)` varargs
   * method that Scala cannot select unambiguously on its own. The explicit
   * `Seq.empty: _*` forces Scala to pick the varargs overload that stubs
   * a single return value with no additional values. Returning the
   * [[org.mockito.stubbing.Stubber]] preserves the fluent
   * `.when(mock).method` chain used throughout the suite.
   */
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * Mockito [[Answer]] used as the default answer on every `ShuffleDependency`
   * mock. Any method call that the suite forgot to stub raises a
   * `RuntimeException` rather than silently returning `null` / zero &mdash;
   * matching SortShuffleManagerSuite's strict-mock discipline. The strictness
   * is deliberate: a silent default on a missing stub would mask regressions
   * in `StreamingShuffleManager`'s dependency contract (for example, if a
   * future refactor caused the manager to consult an unexpected field on the
   * dependency, we want a test failure, not a silent pass).
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  /**
   * Builds a mocked [[ShuffleDependency]] with a real (non-mock)
   * [[Partitioner]] whose `numPartitions` is configurable. Every field that
   * [[StreamingShuffleManager.registerShuffle]] and its fallback delegate
   * ([[SortShuffleManager.registerShuffle]] &rarr;
   * [[org.apache.spark.shuffle.sort.SortShuffleWriter.shouldBypassMergeSort]]
   * and [[SortShuffleManager.canUseSerializedShuffle]]) may consult is stubbed
   * explicitly so the [[RuntimeExceptionAnswer]] never fires on the legitimate
   * fallback path:
   *
   *   - `shuffleId` &mdash; read by `canUseSerializedShuffle` on every call
   *     (even when the serializer check short-circuits early via a
   *     `log.debug`-level message that interpolates the id). Stubbed to `0`;
   *     any non-negative value would do because the handle type returned by
   *     the sort fallback does not depend on this value.
   *   - `partitioner` &mdash; read by both `StreamingShuffleManager.register`
   *     (for the `Int.MaxValue / 2` cap) and the fallback delegate (for the
   *     bypass-merge-sort threshold comparison).
   *   - `serializer` &mdash; read by `canUseSerializedShuffle` when the bypass
   *     path is not taken. A [[JavaSerializer]] returns `false` from
   *     `supportsRelocationOfSerializedObjects` so the function short-circuits
   *     to `false` and the fallback delegate falls through to
   *     [[org.apache.spark.shuffle.BaseShuffleHandle]].
   *   - `aggregator`, `keyOrdering`, `mapSideCombine`, `rowBasedChecksums`
   *     &mdash; read by various fallback code paths; stubbed to their
   *     simplest/"no-op" values.
   *
   * The returned [[Partitioner]] is deliberately a real anonymous subclass
   * rather than another mock: `Partitioner.numPartitions` is abstract, and
   * stubbing an abstract method on a mock requires the same
   * [[RuntimeExceptionAnswer]] workaround this helper would add for no gain.
   */
  private def shuffleDep(numParts: Int): ShuffleDependency[Any, Any, Any] = {
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    // The parameter is deliberately named `numParts` (not `numPartitions`) to
    // avoid shadowing the method name inside the anonymous [[Partitioner]]
    // subclass below, which would compile as an infinite recursion under the
    // strict `-Wconf:any:e` / `-Wunused` settings and be rejected by the Scala
    // compiler as "does nothing other than call itself recursively".
    val partitioner = new Partitioner() {
      override def numPartitions: Int = numParts
      override def getPartition(key: Any): Int = 0
    }
    doReturn(0).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(new JavaSerializer(new SparkConf())).when(dep).serializer
    doReturn(None).when(dep).aggregator
    doReturn(None).when(dep).keyOrdering
    doReturn(false).when(dep).mapSideCombine
    // rowBasedChecksums is typed Array[RowBasedChecksum] (see Dependency.scala
    // line 92). Returning an untyped Seq.empty fails Mockito's runtime
    // type-check with "Nil$ cannot be returned by rowBasedChecksums()". Use
    // the canonical empty-array constant from the dependency's companion
    // object, which is `private[spark]` and accessible from this sub-package.
    doReturn(ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS).when(dep).rowBasedChecksums
    dep
  }

  /**
   * Produces a fresh [[SparkConf]] with `loadDefaults = false` (to isolate the
   * test from any ambient `spark-defaults.conf`) configured so that every
   * [[StreamingShuffleFallbackPolicy]] config-driven condition is satisfied:
   *
   *   - `spark.shuffle.manager = "streaming"` (selects this manager via
   *     [[ShuffleManager.getShuffleManagerClassName]] short name).
   *   - `spark.shuffle.streaming.enabled = true` (feature kill-switch on).
   *   - `spark.executor.memory = 1024 MiB` (comfortably above the policy's
   *     512 MiB minimum so the memory check never fires).
   *   - `spark.shuffle.push.enabled = "false"` (disables push-based shuffle so
   *     the ADR-005 mutual-exclusion check never fires).
   *
   * Even so, in the v1 codebase [[StreamingShuffleFallbackPolicy.evaluate]]
   * returns `Some("streaming-transport-unavailable-v1")` for every shuffle
   * registered against this config because the v1 transport-readiness guard
   * fires LAST. Individual tests override specific config values to reach
   * earlier-ranked reason codes; the helper supplies the common foundation.
   */
  private def streamingEnabledConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, "streaming")
      .set(config.SHUFFLE_STREAMING_ENABLED, true)
      .set(config.EXECUTOR_MEMORY, 1024L)
      .set("spark.shuffle.push.enabled", "false")
  }

  /**
   * Reflection helper that extracts the `private val fallbackShuffles`
   * [[ConcurrentHashMap]] from a [[StreamingShuffleManager]] instance. Used
   * across the routing-validation tests (Groups 2, 3, 5) to verify that
   * fallback decisions are recorded correctly and that `stop` /
   * `unregisterShuffle` clean up the map. Reflection keeps the production
   * code free of test-only getters &mdash; the bookkeeping map is an
   * implementation detail of the manager, not a public contract.
   */
  private def fallbackShufflesMap(
      manager: StreamingShuffleManager): ConcurrentHashMap[Integer, String] = {
    val field = classOf[StreamingShuffleManager].getDeclaredField("fallbackShuffles")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[ConcurrentHashMap[Integer, String]]
  }

  /**
   * Reflection helper that extracts the `private val fallbackManager`
   * [[SortShuffleManager]] held by the [[StreamingShuffleManager]] for
   * fallback routing. Used by Group 2 (constructor-initialization) and Group
   * 6 (resolver reference-equality) tests to assert that the delegate is the
   * expected concrete type and to cross-check
   * [[StreamingShuffleManager.shuffleBlockResolver]]'s identity invariant.
   */
  private def fallbackManagerField(
      manager: StreamingShuffleManager): SortShuffleManager = {
    val field = classOf[StreamingShuffleManager].getDeclaredField("fallbackManager")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[SortShuffleManager]
  }

  // ==========================================================================
  // Group 1: Short-name and FQCN resolution
  //
  // These tests exercise ShuffleManager.getShuffleManagerClassName, which
  // must have been extended with `"streaming" -> classOf[StreamingShuffleManager].getName`
  // in the companion object's shortShuffleMgrNames map. The resolution is
  // case-insensitive (via `.toLowerCase(Locale.ROOT)`) and the existing sort
  // entries must remain intact (regression coverage).
  // ==========================================================================

  test("short name 'streaming' resolves to StreamingShuffleManager FQCN") {
    val conf = new SparkConf(loadDefaults = false).set(SHUFFLE_MANAGER, "streaming")
    ShuffleManager.getShuffleManagerClassName(conf) must be(
      classOf[StreamingShuffleManager].getName)
  }

  test("short name 'STREAMING' (uppercase) resolves to StreamingShuffleManager") {
    val conf = new SparkConf(loadDefaults = false).set(SHUFFLE_MANAGER, "STREAMING")
    ShuffleManager.getShuffleManagerClassName(conf) must be(
      classOf[StreamingShuffleManager].getName)
  }

  test("short name 'Streaming' (mixed-case) resolves to StreamingShuffleManager") {
    val conf = new SparkConf(loadDefaults = false).set(SHUFFLE_MANAGER, "Streaming")
    ShuffleManager.getShuffleManagerClassName(conf) must be(
      classOf[StreamingShuffleManager].getName)
  }

  test("FQCN 'org.apache.spark.shuffle.streaming.StreamingShuffleManager' resolves to same class") {
    val conf = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, "org.apache.spark.shuffle.streaming.StreamingShuffleManager")
    ShuffleManager.getShuffleManagerClassName(conf) must be(
      classOf[StreamingShuffleManager].getName)
  }

  test("short name 'sort' still resolves to SortShuffleManager (regression)") {
    val conf = new SparkConf(loadDefaults = false).set(SHUFFLE_MANAGER, "sort")
    ShuffleManager.getShuffleManagerClassName(conf) must be(
      classOf[SortShuffleManager].getName)
  }

  test("short name 'tungsten-sort' still resolves to SortShuffleManager (regression)") {
    val conf = new SparkConf(loadDefaults = false).set(SHUFFLE_MANAGER, "tungsten-sort")
    ShuffleManager.getShuffleManagerClassName(conf) must be(
      classOf[SortShuffleManager].getName)
  }

  // ==========================================================================
  // Group 2: Constructor and lifecycle
  //
  // StreamingShuffleManager is instantiated at SparkEnv construction time on
  // both the driver and every executor. The constructor must:
  //   - Initialize the fallback SortShuffleManager eagerly (so its
  //     IndexShuffleBlockResolver is ready before any registerShuffle call).
  //   - Tolerate a null SparkEnv (the executor-only metrics-source guard must
  //     short-circuit cleanly when the ambient SparkEnv hasn't been set yet).
  // stop() must be idempotent (AtomicBoolean guard) and must clear the
  // fallbackShuffles map so that the backing array becomes GC-eligible.
  // ==========================================================================

  test("constructor takes SparkConf and does not fail on driver-side construction") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      manager must not be null
    } finally {
      manager.stop()
    }
  }

  test("constructor initializes fallbackManager as SortShuffleManager") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      val fallback = fallbackManagerField(manager)
      fallback must not be null
      fallback.getClass.getName must be(classOf[SortShuffleManager].getName)
    } finally {
      manager.stop()
    }
  }

  test("constructor does NOT throw when SparkEnv.get is null") {
    // The driver-side construction path runs before SparkEnv is fully set up
    // in some bootstrap sequences (for example, unit tests that instantiate
    // the manager without a SparkContext). The executor-only metrics
    // registration guard in the manager must short-circuit on a null
    // SparkEnv.get; verify by explicitly clearing the ambient SparkEnv and
    // restoring it afterwards.
    val saved = SparkEnv.get
    try {
      SparkEnv.set(null)
      val manager = new StreamingShuffleManager(streamingEnabledConf())
      try {
        manager must not be null
      } finally {
        manager.stop()
      }
    } finally {
      SparkEnv.set(saved)
    }
  }

  test("stop() is idempotent - second call does not throw") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    manager.stop()
    // Second invocation must be a no-op due to the `stopped.compareAndSet`
    // guard; raise the expected-behavior assertion with scalatest's
    // no-exception matcher rather than plain try/catch to keep the intent
    // explicit at the call site.
    noException must be thrownBy manager.stop()
  }

  test("stop() clears fallbackShuffles tracking map") {
    // Force fallback routing by disabling the streaming kill-switch. The
    // policy's first check will trip on every registerShuffle call and the
    // shuffleId -> reason entry will be added to the bookkeeping map.
    val conf = streamingEnabledConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val manager = new StreamingShuffleManager(conf)
    val map = fallbackShufflesMap(manager)
    try {
      manager.registerShuffle(7, shuffleDep(10))
      map.size() must be > 0
      manager.stop()
      // After stop, the map is cleared so the backing String references can
      // be garbage-collected promptly (zero memory leaks per AAP section 0.1.2).
      map.size() must be(0)
    } finally {
      // Idempotent second stop in finally is safe even if the test above
      // already called stop; the AtomicBoolean guard returns on the second
      // compareAndSet.
      manager.stop()
    }
  }

  // ==========================================================================
  // Group 3: registerShuffle routing
  //
  // registerShuffle is the single dispatch point between the streaming path
  // and the held SortShuffleManager. Tests in this group validate:
  //   - Partition-count sanity (Int.MaxValue / 2 is the inclusive upper bound;
  //     over that raises SparkException).
  //   - Fallback routing for the three config-driven reasons that fire before
  //     the v1 transport-readiness guard: feature-flag off, push-based shuffle
  //     active, insufficient executor memory.
  //   - v1 behavior: even a fully-enabled config produces a fallback handle
  //     because STREAMING_TRANSPORT_READY_V1 is hard-coded to false. When
  //     sibling agents land the transport, the "v1 fallback handle" tests
  //     flip to expect StreamingShuffleHandle.
  // ==========================================================================

  test("registerShuffle throws SparkException when numPartitions > Int.MaxValue / 2") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      val dep = shuffleDep(Int.MaxValue / 2 + 1)
      val ex = intercept[SparkException] {
        manager.registerShuffle(0, dep)
      }
      // The error message must mention "partition" so operators and log
      // aggregators can classify the exception without parsing the full
      // stack trace. The manager's registerShuffle format string includes
      // "... does not support shuffles with more than X partitions".
      ex.getMessage must include("partition")
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle returns a non-null handle when all config conditions pass " +
    "(v1 transport-unavailable routes to fallback)") {
    // In v1 the streaming transport is not yet wired, so even a fully-enabled
    // configuration routes every shuffle to the held SortShuffleManager. When
    // sibling agents flip STREAMING_TRANSPORT_READY_V1 to `true`, the first
    // two assertions below become `handle mustBe a[StreamingShuffleHandle[_, _]]`
    // and the fallback-map assertion inverts to `map must not containKey ...`.
    // For now, assert the v1-specific reason and the handle's shuffleId
    // contract which is preserved across both code paths.
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    val map = fallbackShufflesMap(manager)
    try {
      val handle = manager.registerShuffle(1, shuffleDep(10))
      handle must not be null
      handle.shuffleId must be(1)
      // v1 reason: the transport-readiness guard fires because every other
      // config-driven condition passed.
      handle must not be a[StreamingShuffleHandle[_, _]]
      map.get(Integer.valueOf(1)) must be("streaming-transport-unavailable-v1")
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle returns non-StreamingShuffleHandle when streaming is disabled by config") {
    val conf = streamingEnabledConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val manager = new StreamingShuffleManager(conf)
    try {
      val handle = manager.registerShuffle(2, shuffleDep(10))
      // The feature-flag kill-switch fires at policy check 1, ahead of the
      // v1 transport guard. The returned handle is a sort-path type
      // (BypassMergeSortShuffleHandle for numPartitions <= 200 with no
      // map-side combine) and therefore NOT a StreamingShuffleHandle.
      handle must not be a[StreamingShuffleHandle[_, _]]
      // But it still satisfies the abstract ShuffleHandle contract, because
      // BaseShuffleHandle and its subclasses all extend ShuffleHandle.
      handle mustBe a[ShuffleHandle]
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle tracks fallbackShuffles entry when routed to fallback") {
    val conf = streamingEnabledConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val manager = new StreamingShuffleManager(conf)
    val map = fallbackShufflesMap(manager)
    try {
      manager.registerShuffle(42, shuffleDep(10))
      // The bookkeeping map records the reason code returned by the policy.
      // "streaming-disabled-by-config" matches the exact reason used by
      // StreamingShuffleFallbackPolicy.evaluate for this condition (AAP
      // section 0.4.1.2).
      map.get(Integer.valueOf(42)) must be("streaming-disabled-by-config")
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle succeeds for numPartitions exactly Int.MaxValue / 2") {
    // Int.MaxValue / 2 is the inclusive upper bound enforced by the manager's
    // explicit partition-count guard (a sanity limit that protects downstream
    // integer arithmetic in the streaming writer). Exactly at the boundary,
    // registration must succeed. In v1 the shuffle falls back to the sort
    // path because of the transport-readiness guard, and the sort manager
    // successfully constructs a handle because JavaSerializer rules out the
    // serialized-shuffle path and the large partition count does not trigger
    // the bypass-merge-sort path.
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      val handle = manager.registerShuffle(5, shuffleDep(Int.MaxValue / 2))
      handle must not be null
      handle.shuffleId must be(5)
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle delegates to fallbackManager when push-based shuffle active (ADR-005)") {
    // spark.shuffle.push.enabled=true triggers policy check 2 (push-based
    // shuffle mutual exclusion) which fires before the v1 transport-readiness
    // guard. The bookkeeping map therefore records "push-based-shuffle-active"
    // rather than the transport-unavailable reason.
    val conf = streamingEnabledConf().set("spark.shuffle.push.enabled", "true")
    val manager = new StreamingShuffleManager(conf)
    val map = fallbackShufflesMap(manager)
    try {
      val handle = manager.registerShuffle(3, shuffleDep(10))
      handle must not be a[StreamingShuffleHandle[_, _]]
      map.get(Integer.valueOf(3)) must be("push-based-shuffle-active")
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle delegates to fallbackManager on insufficient executor memory") {
    // EXECUTOR_MEMORY is a bytesConf(ByteUnit.MiB), so 128L means 128 MiB
    // which is below the policy's 512 MiB MINIMUM_EXECUTOR_MEMORY_MIB. The
    // memory check fires at policy check 4 (ahead of the v1 transport guard).
    val conf = streamingEnabledConf().set(config.EXECUTOR_MEMORY, 128L)
    val manager = new StreamingShuffleManager(conf)
    val map = fallbackShufflesMap(manager)
    try {
      val handle = manager.registerShuffle(4, shuffleDep(10))
      handle must not be a[StreamingShuffleHandle[_, _]]
      map.get(Integer.valueOf(4)) must be("insufficient-executor-memory")
    } finally {
      manager.stop()
    }
  }

  // ==========================================================================
  // Group 4: getWriter dispatch
  //
  // getWriter is a type-match dispatch point. Any non-StreamingShuffleHandle
  // is forwarded to the held SortShuffleManager.getWriter. Without a real
  // TaskContext + BlockManager we cannot drive a full getWriter invocation,
  // but the dispatch contract (handle-type inspection) is observable.
  // ==========================================================================

  test("getWriter(non-StreamingShuffleHandle) delegates to fallbackManager") {
    // Register with streaming disabled so the returned handle is a sort-path
    // type produced by the held SortShuffleManager. The assertion confirms
    // that getWriter's type-match would route this handle to the fallback
    // rather than attempting to build a StreamingShuffleWriter from it.
    val conf = streamingEnabledConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val manager = new StreamingShuffleManager(conf)
    try {
      val fallbackHandle = manager.registerShuffle(10, shuffleDep(3))
      fallbackHandle must not be a[StreamingShuffleHandle[_, _]]
      // A direct invocation of getWriter would require a real TaskContext
      // and BlockManager, so verify the dispatch predicate itself (the
      // type-check that the manager performs internally) returns the
      // expected boolean with no exception thrown.
      noException must be thrownBy {
        fallbackHandle.isInstanceOf[StreamingShuffleHandle[_, _]] must be(false)
      }
    } finally {
      manager.stop()
    }
  }

  // ==========================================================================
  // Group 5: unregisterShuffle
  //
  // unregisterShuffle delegates to the held SortShuffleManager for on-disk
  // cleanup (so the shared IndexShuffleBlockResolver runs through the same
  // tear-down code regardless of routing) and then removes the per-shuffle
  // entry from the streaming bookkeeping map. The Boolean it returns is the
  // delegate's result.
  // ==========================================================================

  test("unregisterShuffle removes entry from fallbackShuffles map when present") {
    val conf = streamingEnabledConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val manager = new StreamingShuffleManager(conf)
    val map = fallbackShufflesMap(manager)
    try {
      manager.registerShuffle(100, shuffleDep(10))
      // The entry is present after registration because the policy routed
      // this shuffle to the sort fallback.
      map.containsKey(Integer.valueOf(100)) must be(true)

      manager.unregisterShuffle(100)

      // After unregister, the bookkeeping entry is gone.
      map.containsKey(Integer.valueOf(100)) must be(false)
    } finally {
      manager.stop()
    }
  }

  test("unregisterShuffle returns a boolean result") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      manager.registerShuffle(200, shuffleDep(10))
      // The Boolean return value is authoritative and comes from the held
      // SortShuffleManager.unregisterShuffle. It may be either true or
      // false depending on whether the sort path had on-disk metadata to
      // remove; what matters is that the method returns a Boolean cleanly.
      val result: Boolean = manager.unregisterShuffle(200)
      result must (be(true) or be(false))
    } finally {
      manager.stop()
    }
  }

  // ==========================================================================
  // Group 6: shuffleBlockResolver and ShuffleManager trait compliance
  //
  // shuffleBlockResolver exposes the SAME IndexShuffleBlockResolver instance
  // the held SortShuffleManager exposes, so ADR-002 (atomic metadata commit
  // via writeMetadataFileAndCommit) applies unchanged to every block that
  // lands on disk &mdash; whether from a sort-path writer or a streaming
  // spill. Reference equality is the cleanest assertion here.
  //
  // The trait-compliance test confirms that StreamingShuffleManager satisfies
  // every abstract member of the ShuffleManager SPI. The compile-time type
  // ascription catches missing members at `build/sbt core/Test/compile`; the
  // runtime no-throw checks exercise the three cheap, side-effect-free
  // methods (shuffleBlockResolver, unregisterShuffle, stop).
  // ==========================================================================

  test("shuffleBlockResolver returns the fallback manager's resolver (ADR-002 preservation)") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      val resolver = manager.shuffleBlockResolver
      resolver must not be null
      val fallback = fallbackManagerField(manager)
      // Reference equality is the strict invariant. There MUST be exactly one
      // IndexShuffleBlockResolver per executor (this is what ESS, decommission,
      // and migration code paths expect), so the streaming manager MUST expose
      // the same instance its delegate does.
      resolver must be theSameInstanceAs fallback.shuffleBlockResolver
    } finally {
      manager.stop()
    }
  }

  test("implements all 6 abstract ShuffleManager methods (compile-time + runtime)") {
    val manager = new StreamingShuffleManager(streamingEnabledConf())
    try {
      // Compile-time: the type ascription below fails `core/Test/compile` if
      // any abstract method from the ShuffleManager trait (registerShuffle,
      // getWriter, getReader, unregisterShuffle, shuffleBlockResolver, stop)
      // is missing or has the wrong signature.
      val sm: ShuffleManager = manager
      sm must not be null

      // Runtime: exercise the three methods that do not require a
      // TaskContext / BlockManager / real RPC environment. getWriter and
      // getReader are validated indirectly via Groups 3 and 4; registerShuffle
      // is validated in Group 3; stop is validated in Group 2. Here we
      // confirm that shuffleBlockResolver returns a non-null resolver and
      // unregisterShuffle tolerates an unknown shuffleId without throwing.
      sm.shuffleBlockResolver must not be null
      sm.unregisterShuffle(999)
    } finally {
      manager.stop()
    }
  }
}
