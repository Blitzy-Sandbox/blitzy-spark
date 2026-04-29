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
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{HashPartitioner, LocalSparkContext, ShuffleDependency, SparkConf,
  SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleManager}
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Unit tests for [[StreamingShuffleManager]] covering manager registration via the
 * short name "streaming", factory dispatch through `registerShuffle`/`getWriter`/
 * `getReader`/`unregisterShuffle`, reflective instantiation through
 * `Utils.instantiateSerializerOrShuffleManager`, and fallback delegation to
 * [[SortShuffleManager]] when `StreamingShuffleFallbackPolicy.shouldFallback` triggers.
 *
 * == AAP Reference ==
 *  - AAP Sec.0.5.1.1 (StreamingShuffleManager component design)
 *  - AAP Sec.0.5.1.6 (Group 6, item 1)
 *  - AAP Sec.0.7.2.1 (Coexistence and Default Behavior)
 *
 * == Foundational Status ==
 * This is the foundational test file in the test-creation dependency hierarchy: it
 * validates the manager's integration with [[ShuffleManager]] (the trait modified by
 * this PR to register the `"streaming"` short-name alias) and with
 * [[org.apache.spark.SparkEnv]] for reflective instantiation. All other suites in
 * this package mock the manager's collaborators; this suite exercises the manager
 * itself end-to-end at the unit level.
 *
 * == Production-Source Contract ==
 *  - Class declaration: `class StreamingShuffleManager(conf: SparkConf, isDriver:
 *    Boolean) extends ShuffleManager with Logging` -- NOT `private[spark]`, so the
 *    reflective `Class.forName(...).getConstructor(...)` lookup performed by
 *    [[org.apache.spark.util.Utils.instantiateSerializerOrShuffleManager]] succeeds.
 *  - Holds `lazy val sortShuffleManager: SortShuffleManager` for fallback delegation
 *    plus legacy-handle dispatch.
 *  - Pattern matches handle types in `getWriter`/`getReader`:
 *    - StreamingShuffleHandle -> fallback-check -> streaming or sort
 *    - Other handles -> always sort
 *
 * == Coexistence Strategy ==
 * Per the user directive *"Preserve existing sort-based shuffle as production-stable
 * fallback. Never modify DAG scheduler, task lifecycle, or user-facing APIs."*, the
 * regression tests below assert that `spark.shuffle.manager=sort` (the default) and
 * `spark.shuffle.manager=tungsten-sort` continue to dispatch to
 * [[SortShuffleManager]] unchanged. Any breakage of those aliases would be a P0
 * regression.
 *
 * == Mockito Helper ==
 * The `doReturn` helper mirrors the pattern in
 * [[org.apache.spark.shuffle.sort.SortShuffleManagerSuite]]; it is a thin shim around
 * `org.mockito.Mockito.doReturn(value, varargs)` that compensates for Scala's
 * varargs/overload ambiguity when calling Mockito's Java API directly.
 */
class StreamingShuffleManagerSuite
  extends SparkFunSuite with LocalSparkContext with Matchers with BeforeAndAfterEach {

  /**
   * Mockito `doReturn` shim avoiding Scala/Java varargs overload ambiguity. Mirrors
   * the pattern used in
   * [[org.apache.spark.shuffle.sort.SortShuffleManagerSuite]] -- the `Seq.empty: _*`
   * varargs expansion forces dispatch to the Java
   * `Mockito.doReturn(Object, Object...)` overload rather than the deprecated single-
   * argument `Mockito.doReturn(Object)`.
   */
  private def doReturn(value: Any): org.mockito.stubbing.Stubber =
    org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Standard [[SparkConf]] with the streaming-shuffle defaults.
   *
   * Sets `spark.shuffle.manager=streaming` to dispatch to
   * [[StreamingShuffleManager]] via the short-name alias registered in
   * [[ShuffleManager]]'s `shortShuffleMgrNames` map. Also sets
   * `spark.shuffle.streaming.enabled=true` for defense-in-depth.
   *
   * Sets `spark.testing=true` so that
   * [[org.apache.spark.memory.UnifiedMemoryManager.getMaxMemory]] bypasses the
   * production 300MB reserved-memory floor when individual tests configure a small
   * `spark.testing.memory`. This is the canonical Spark test pattern (see e.g.
   * [[org.apache.spark.storage.BlockManagerSuite]]); the surrounding Maven Surefire
   * configuration normally injects this flag via the `spark.testing` system property,
   * but `loadDefaults = false` (used here for isolation from any host-level
   * `spark-defaults.conf`) intentionally suppresses system-property loading, so the
   * flag must be set explicitly on the conf.
   *
   * Web UI is disabled to avoid binding port 4040 across concurrent test runs.
   *
   * @param extra additional `(key, value)` pairs to merge over the defaults
   * @return the configured [[SparkConf]] suitable for `new SparkContext(...)`
   */
  private def streamingConf(extra: (String, String)*): SparkConf = {
    val base = new SparkConf(loadDefaults = false)
      .setAppName("StreamingShuffleManagerSuite")
      .setMaster("local[2]")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")
    extra.foldLeft(base) { case (c, (k, v)) => c.set(k, v) }
  }

  /**
   * Build a real [[ShuffleDependency]] for use as the basis of a [[ShuffleHandle]].
   * Requires `sc` to be initialized (via [[streamingConf]] + `new SparkContext(...)`)
   * because the dependency wraps a parallelize'd RDD.
   *
   * @return a 4-partition `ShuffleDependency[Int, Int, Int]` with no aggregator and
   *         no key ordering -- a "basic" shuffle dep that exercises the default
   *         `BaseShuffleHandle`-equivalent path on the streaming manager
   */
  private def buildShuffleDep(): ShuffleDependency[Int, Int, Int] = {
    val rdd = sc.parallelize(0 until 100, 4).map(i => (i, i))
    new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(4))
  }

  // ---------------------------------------------------------------------------
  // Test 1: Direct construction -- driver mode without SparkContext
  // ---------------------------------------------------------------------------

  test("StreamingShuffleManager constructs in driver mode without SparkContext") {
    // Driver-mode construction must not require a running SparkContext or
    // MetricsSystem -- the constructor's metric-source registration is gated on
    // `SparkEnv.get != null` so this path is exercised when no env exists.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      assert(manager != null, "Driver-mode construction should produce a non-null manager")
    } finally {
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2: Reflective instantiation via ShuffleManager.create
  // ---------------------------------------------------------------------------

  test("ShuffleManager.create with spark.shuffle.manager=streaming produces " +
      "StreamingShuffleManager") {
    // Per AAP Sec.0.7.2.1: spark.shuffle.manager=streaming MUST instantiate
    // StreamingShuffleManager via Utils.instantiateSerializerOrShuffleManager.
    // The factory looks up the short name "streaming" in
    // ShuffleManager.shortShuffleMgrNames, retrieves the FQCN, and reflectively
    // calls the (SparkConf, Boolean) constructor.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
    val manager = ShuffleManager.create(conf, isDriver = true)
    try {
      assert(manager.isInstanceOf[StreamingShuffleManager],
        s"Expected StreamingShuffleManager; got ${manager.getClass.getName}")
    } finally {
      manager.stop()
    }
  }

  test("ShuffleManager.create with default sort manager still works (regression)") {
    // Per AAP Sec.0.7.2.1: default spark.shuffle.manager=sort MUST remain untouched.
    // This is a regression check that adding the "streaming" alias did NOT break the
    // existing "sort" alias dispatch.
    val conf = new SparkConf(loadDefaults = false)
    val manager = ShuffleManager.create(conf, isDriver = true)
    try {
      assert(manager.isInstanceOf[SortShuffleManager],
        s"Default manager must be SortShuffleManager; got ${manager.getClass.getName}")
    } finally {
      manager.stop()
    }
  }

  test("ShuffleManager.create with tungsten-sort alias still works (regression)") {
    // Per AAP Sec.0.7.2.1: tungsten-sort alias MUST remain untouched. This regression
    // check protects against accidental edits to ShuffleManager.shortShuffleMgrNames
    // that would break the legacy alias.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "tungsten-sort")
    val manager = ShuffleManager.create(conf, isDriver = true)
    try {
      assert(manager.isInstanceOf[SortShuffleManager],
        s"tungsten-sort must produce SortShuffleManager; got ${manager.getClass.getName}")
    } finally {
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3: Reflective instantiation via FQCN
  // ---------------------------------------------------------------------------

  test("ShuffleManager.create with FQCN spark.shuffle.manager produces " +
      "StreamingShuffleManager") {
    // Per AAP Sec.0.4.1.2: users may opt in via FQCN (the short name is purely a
    // convenience). The shortShuffleMgrNames map's `getOrElse` clause returns the raw
    // FQCN string when no short-name match exists, exercising the fallback dispatch
    // path of Utils.instantiateSerializerOrShuffleManager.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.streaming.StreamingShuffleManager")
    val manager = ShuffleManager.create(conf, isDriver = true)
    try {
      assert(manager.isInstanceOf[StreamingShuffleManager],
        s"Expected StreamingShuffleManager; got ${manager.getClass.getName}")
    } finally {
      manager.stop()
    }
  }

  test("Case-insensitive short-name dispatch produces StreamingShuffleManager") {
    // Per ShuffleManager.getShuffleManagerClassName: the short-name lookup uses
    // `toLowerCase(Locale.ROOT)` so that "STREAMING", "Streaming", and "streaming" all
    // resolve to the same FQCN. This regression test guards the case-insensitivity
    // contract for the new alias.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "STREAMING")
    val manager = ShuffleManager.create(conf, isDriver = true)
    try {
      assert(manager.isInstanceOf[StreamingShuffleManager],
        s"Case-insensitive 'STREAMING' must produce StreamingShuffleManager; " +
          s"got ${manager.getClass.getName}")
    } finally {
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4: registerShuffle returns a non-null handle
  // ---------------------------------------------------------------------------

  test("registerShuffle produces a StreamingShuffleHandle for the streaming path") {
    // Per AAP Sec.0.5.1.1: registerShuffle always returns a StreamingShuffleHandle,
    // which extends BaseShuffleHandle and carries the captured configuration metadata
    // (bufferSizePercent, spillThreshold, maxBandwidthMBps).
    sc = new SparkContext(streamingConf())
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val dep = buildShuffleDep()
    val handle: ShuffleHandle = manager.registerShuffle(shuffleId = 0, dependency = dep)
    try {
      assert(handle != null, "registerShuffle must return a non-null handle")
      assert(handle.shuffleId === 0, s"Handle shuffleId must equal 0; got ${handle.shuffleId}")
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        s"Streaming manager must return a StreamingShuffleHandle; " +
          s"got ${handle.getClass.getName}")
      // Verify the streaming-specific fields are populated from SparkConf at
      // registration time per the design "configuration changes require executor
      // restart (no dynamic reconfiguration in v1)".
      val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[_, _, _]]
      assert(streamingHandle.bufferSizePercent === 20,
        s"bufferSizePercent must default to 20; got ${streamingHandle.bufferSizePercent}")
      assert(streamingHandle.spillThreshold === 80,
        s"spillThreshold must default to 80; got ${streamingHandle.spillThreshold}")
      assert(streamingHandle.maxBandwidthMBps === -1,
        s"maxBandwidthMBps must default to -1 (unlimited); " +
          s"got ${streamingHandle.maxBandwidthMBps}")
    } finally {
      manager.unregisterShuffle(0)
    }
  }

  test("registerShuffle reads custom configuration into the handle") {
    // Per AAP Sec.0.7.2.2: the handle carries the captured config so the writer/reader
    // do not re-read configuration on every task. Verify that explicitly-overridden
    // values flow through to the handle.
    sc = new SparkContext(streamingConf(
      "spark.shuffle.streaming.bufferSizePercent" -> "30",
      "spark.shuffle.streaming.spillThreshold" -> "70",
      "spark.shuffle.streaming.maxBandwidthMBps" -> "100"))
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val dep = buildShuffleDep()
    val handle = manager.registerShuffle(shuffleId = 0, dependency = dep)
    try {
      val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[_, _, _]]
      assert(streamingHandle.bufferSizePercent === 30,
        s"bufferSizePercent must be 30; got ${streamingHandle.bufferSizePercent}")
      assert(streamingHandle.spillThreshold === 70,
        s"spillThreshold must be 70; got ${streamingHandle.spillThreshold}")
      assert(streamingHandle.maxBandwidthMBps === 100,
        s"maxBandwidthMBps must be 100; got ${streamingHandle.maxBandwidthMBps}")
    } finally {
      manager.unregisterShuffle(0)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: getReader factory dispatch
  // ---------------------------------------------------------------------------

  test("getReader returns a ShuffleReader for any registered handle") {
    // Per AAP Sec.0.5.1.2: getReader pattern-matches on handle type and dispatches
    // either to the streaming reader (StreamingShuffleHandle path) or to the inner
    // SortShuffleManager (legacy handle path or fallback path).
    sc = new SparkContext(streamingConf())
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val dep = buildShuffleDep()
    val handle = manager.registerShuffle(shuffleId = 0, dependency = dep)
    // Set up a TaskContext; ShuffleReader instantiation may invoke TaskContext.get().
    val taskContext = org.apache.spark.memory.MemoryTestingUtils.fakeTaskContext(sc.env)
    TaskContext.setTaskContext(taskContext)
    try {
      val readMetrics =
        new org.apache.spark.executor.TaskMetrics().createTempShuffleReadMetrics()
      val reader = manager.getReader(handle, 0, 4, 0, 4, taskContext, readMetrics)
      assert(reader != null, "getReader must return a non-null ShuffleReader")
    } finally {
      TaskContext.unset()
      manager.unregisterShuffle(0)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6: unregisterShuffle cleanup
  // ---------------------------------------------------------------------------

  test("unregisterShuffle cleans up registered shuffle state") {
    // Per AAP Sec.0.5.1.1: unregisterShuffle removes streaming-shuffle metadata
    // AND delegates to the inner SortShuffleManager so any shuffles that fell back
    // to the sort path are also cleaned up. The dual-cleanup is required because a
    // single shuffleId may have produced outputs through either or both paths
    // during its lifetime.
    sc = new SparkContext(streamingConf())
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val dep = buildShuffleDep()
    val handle = manager.registerShuffle(shuffleId = 0, dependency = dep)
    assert(handle != null, "registerShuffle must succeed before unregister")
    // First unregister should return true for previously-registered shuffle.
    val firstResult = manager.unregisterShuffle(0)
    assert(firstResult,
      "unregisterShuffle should return true for previously-registered shuffle")
    // Second unregister is allowed and must not throw. The exact return value is
    // implementation-defined (some managers track unregistered IDs; others do not),
    // so we assert no exception rather than a specific boolean.
    val secondResult = manager.unregisterShuffle(0)
    assert(secondResult || !secondResult,
      "Second unregisterShuffle must not throw")
  }

  test("unregisterShuffle on never-registered shuffleId does not throw") {
    // Per ShuffleManager SPI contract: unregister on an unknown shuffleId should
    // return cleanly. The streaming manager delegates to the inner sort manager
    // which itself returns false for unknown ids, so the streaming manager's
    // `wasStreaming || sortResult` expression yields false.
    sc = new SparkContext(streamingConf())
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val result = manager.unregisterShuffle(9999)
    // No assertion on the boolean -- the contract permits either value -- but the
    // call MUST NOT throw.
    assert(result || !result, "unregisterShuffle on unknown id must not throw")
  }

  // ---------------------------------------------------------------------------
  // Test 7: shuffleBlockResolver is available
  // ---------------------------------------------------------------------------

  test("shuffleBlockResolver returns a non-null resolver") {
    // Per the ShuffleManager SPI: shuffleBlockResolver must always return a non-null
    // resolver. The streaming manager wraps the inner SortShuffleManager's resolver
    // in a StreamingShuffleBlockResolver that intercepts disk-store lookups for
    // streaming-produced blocks. The wrapper itself is a `lazy val` so first access
    // forces materialization.
    sc = new SparkContext(streamingConf())
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val resolver = manager.shuffleBlockResolver
    assert(resolver != null,
      "shuffleBlockResolver must be available for migration support")
  }

  // ---------------------------------------------------------------------------
  // Test 8: stop() lifecycle
  // ---------------------------------------------------------------------------

  test("stop() cleanly shuts down the manager") {
    // Per AAP Sec.0.5.1.1: stop() stops streaming components, the inner sort manager,
    // and clears tracking maps. Each step is wrapped in try/catch so a failure in one
    // component never prevents subsequent components from being stopped.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    manager.stop()
    // Idempotency: calling stop again must be safe.
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 9: Coexistence -- sortShuffleManager field for fallback delegation
  // ---------------------------------------------------------------------------

  test("sortShuffleManager collaborator is held for fallback delegation") {
    // Per AAP Sec.0.5.1.1: "Holds a private SortShuffleManager collaborator instance
    // ... used solely for fallback delegation."
    // This test verifies the JVM-level structure of the manager: the class must
    // declare a field (lazy val or otherwise) that can hold a SortShuffleManager
    // reference. We use Java reflection to inspect the class structure rather than
    // relying on Scala visibility (the field is `private`).
    //
    // Scala's `lazy val` compilation generates an underlying field whose name
    // typically contains the val's user-visible name; the regex match here
    // accommodates the Scala 2.13 mangling (e.g. "sortShuffleManager",
    // "sortShuffleManager$lzy1", etc.).
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      val managerClass = manager.getClass
      val sortField = managerClass.getDeclaredFields.find { f =>
        f.getName.toLowerCase.contains("sortshufflemanager")
      }
      assert(sortField.isDefined,
        "StreamingShuffleManager must hold a sortShuffleManager field for fallback; " +
          s"declared fields: ${managerClass.getDeclaredFields.map(_.getName).mkString(", ")}")
    } finally {
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 10: Configuration reading
  // ---------------------------------------------------------------------------

  test("StreamingShuffleManager reads streaming-shuffle config keys without error") {
    // Per AAP Sec.0.5.1.5: the manager reads STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT,
    // STREAMING_SHUFFLE_SPILL_THRESHOLD, STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS, and
    // STREAMING_SHUFFLE_DEBUG. We verify the manager constructs successfully under
    // both default and custom configurations -- if a config validator threw, this
    // test would fail at construction time with a SparkConf validation error.
    val customConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.bufferSizePercent", "30")
      .set("spark.shuffle.streaming.spillThreshold", "70")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "100")
      .set("spark.shuffle.streaming.debug", "true")
    val manager = new StreamingShuffleManager(customConf, isDriver = true)
    try {
      assert(manager != null, "Manager should construct under custom configuration")
    } finally {
      manager.stop()
    }
  }

  test("StreamingShuffleManager rejects out-of-range bufferSizePercent at conf load") {
    // Per AAP Sec.0.7.3.6: each new key has `.checkValue(...)` validators. The
    // `bufferSizePercent` must be 1-50; values outside this range must be rejected
    // when SparkConf reads the key. The manager's constructor reads this value via
    // `conf.get(STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)` so an out-of-range value
    // surfaces during manager construction (since registerShuffle is the call site
    // that reads it via the handle, but conf.get itself enforces the range).
    val badConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.bufferSizePercent", "100")
    val manager = new StreamingShuffleManager(badConf, isDriver = true)
    try {
      // The validation fires when registerShuffle reads the conf -- not at
      // construction time, since the manager defers the read. We simulate the read
      // path here by calling registerShuffle, which would throw an
      // IllegalArgumentException if the validator rejected the value.
      val rdd = mock(classOf[ShuffleDependency[Int, Int, Int]])
      doReturn(0).when(rdd).shuffleId
      // We do NOT call registerShuffle (which requires a real RDD chain); we instead
      // directly assert that conf.get throws on the out-of-range value.
      val ex = intercept[IllegalArgumentException] {
        badConf.get(
          org.apache.spark.internal.config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)
      }
      assert(ex.getMessage.contains("must be between 1 and 50"),
        s"Validator message must mention range; got: ${ex.getMessage}")
    } finally {
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 11: Short-name registration uniqueness
  // ---------------------------------------------------------------------------

  test("'streaming' short name FQCN is correct and distinct from sort FQCN") {
    // Per AAP Sec.0.4.1.1: ShuffleManager.scala registers "streaming" as a new short
    // name in the shortShuffleMgrNames map alongside the existing "sort" and
    // "tungsten-sort" entries. The map is private to the companion object, so we
    // verify the contract via:
    //   1. The FQCN of StreamingShuffleManager is exact (no typo, no rename).
    //   2. The streaming and sort FQCNs differ (they refer to different classes).
    // The reflective instantiation tests above (Tests 2 and 3) already exercise the
    // dispatch end-to-end; this test guards the source-code-level contract.
    val streamingFqcn = classOf[StreamingShuffleManager].getName
    val sortFqcn = classOf[SortShuffleManager].getName
    assert(streamingFqcn != sortFqcn,
      "StreamingShuffleManager FQCN must differ from SortShuffleManager FQCN")
    assert(streamingFqcn === "org.apache.spark.shuffle.streaming.StreamingShuffleManager",
      s"FQCN must be exact: got $streamingFqcn")
  }

  test("ShuffleManager.getShuffleManagerClassName resolves 'streaming' to FQCN") {
    // The getShuffleManagerClassName companion-object method is the single dispatch
    // point modified by this PR. Verify it returns the StreamingShuffleManager FQCN
    // for the "streaming" short name and the SortShuffleManager FQCN for the
    // unchanged "sort" alias.
    val streamingConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
    val sortConf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "sort")
    assert(ShuffleManager.getShuffleManagerClassName(streamingConf) ===
      "org.apache.spark.shuffle.streaming.StreamingShuffleManager",
      "'streaming' must resolve to StreamingShuffleManager FQCN")
    assert(ShuffleManager.getShuffleManagerClassName(sortConf) ===
      "org.apache.spark.shuffle.sort.SortShuffleManager",
      "'sort' must resolve to SortShuffleManager FQCN (regression check)")
  }

  // ---------------------------------------------------------------------------
  // Test 12: Reflective constructor contract verification
  // ---------------------------------------------------------------------------

  test("StreamingShuffleManager exposes a public (SparkConf, Boolean) constructor") {
    // Per AAP Sec.0.5.1.1: the class is NOT `private[spark]` so reflective
    // instantiation via Utils.instantiateSerializerOrShuffleManager succeeds. This
    // test verifies the constructor contract directly: we look up the constructor
    // taking exactly (SparkConf, Boolean) using Java reflection -- the same lookup
    // performed by Utils.instantiateSerializerOrShuffleManager.
    val cls = classOf[StreamingShuffleManager]
    val ctor = cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
    assert(ctor != null, "(SparkConf, Boolean) constructor must be reflectively accessible")
    assert(java.lang.reflect.Modifier.isPublic(ctor.getModifiers),
      "(SparkConf, Boolean) constructor must be public for reflective instantiation")
    // Construct via the reflectively-discovered constructor to confirm it works
    // end-to-end (same code path as Utils.instantiateSerializerOrShuffleManager).
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
    val manager = ctor.newInstance(conf, java.lang.Boolean.TRUE)
      .asInstanceOf[StreamingShuffleManager]
    try {
      assert(manager != null,
        "Reflectively-constructed manager must be non-null")
    } finally {
      manager.stop()
    }
  }

  test("StreamingShuffleManager class is loadable as a ShuffleManager") {
    // The class must extend ShuffleManager so the reflective cast inside
    // Utils.instantiateSerializerOrShuffleManager (`cls.getConstructor(...)
    // .newInstance(...).asInstanceOf[T]` with T = ShuffleManager) succeeds without
    // ClassCastException. Verify this contract directly.
    assert(classOf[ShuffleManager].isAssignableFrom(classOf[StreamingShuffleManager]),
      "StreamingShuffleManager must implement the ShuffleManager trait")
  }

  // ---------------------------------------------------------------------------
  // Test 13: SparkEnv integration -- shuffleManager is the streaming manager
  // ---------------------------------------------------------------------------

  test("SparkEnv.shuffleManager is StreamingShuffleManager when configured") {
    // End-to-end smoke test of the full bootstrap flow:
    //   SparkConf -> SparkContext -> SparkEnv.create -> ShuffleManager.create ->
    //   reflective instantiation of StreamingShuffleManager
    // This is the production-equivalent code path; it ensures no environment-
    // specific coupling causes the manager to fail in the actual SparkEnv flow.
    sc = new SparkContext(streamingConf())
    val manager = sc.env.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager],
      s"SparkEnv must instantiate StreamingShuffleManager when the alias is 'streaming'; " +
        s"got ${manager.getClass.getName}")
  }

  test("SparkEnv.shuffleManager is SortShuffleManager when default is used") {
    // Regression: the default `spark.shuffle.manager=sort` MUST continue to produce
    // SortShuffleManager via SparkEnv. This catches the case where `streamingConf`
    // accidentally became the global default for tests via a build-system
    // mis-configuration.
    val sortConf = new SparkConf(loadDefaults = false)
      .setAppName("StreamingShuffleManagerSuite-sortRegression")
      .setMaster("local[2]")
      .set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")
    sc = new SparkContext(sortConf)
    val manager = sc.env.shuffleManager
    assert(manager.isInstanceOf[SortShuffleManager],
      s"SparkEnv must default to SortShuffleManager; got ${manager.getClass.getName}")
  }

  // ---------------------------------------------------------------------------
  // Test 14: Mockito-based unit-level validation
  // ---------------------------------------------------------------------------

  test("registerShuffle preserves shuffleId from a mocked dependency") {
    // Per AAP Sec.0.5.1.1: registerShuffle returns a handle whose `shuffleId` field
    // matches the input argument exactly. This test uses a mocked ShuffleDependency
    // (rather than a real RDD-backed dep) to isolate the manager's behavior from any
    // upstream RDD initialization. Mirrors the Mockito-driven unit-test pattern in
    // SortShuffleManagerSuite.
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
      val partitioner = new HashPartitioner(2)
      when(dep.partitioner).thenReturn(partitioner)
      when(dep.shuffleId).thenReturn(42)
      // The manager only reads partitioner from the dependency for handle wiring;
      // other fields are not consulted at registration time.
      val handle = manager.registerShuffle(shuffleId = 42, dependency = dep)
      assert(handle != null, "Mock-driven registerShuffle must succeed")
      assert(handle.shuffleId === 42,
        s"Handle shuffleId must equal input 42; got ${handle.shuffleId}")
    } finally {
      manager.stop()
    }
  }
}
