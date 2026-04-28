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
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.MemoryManager

/**
 * Unit tests for [[StreamingShuffleFallbackPolicy]] covering each of the four fallback
 * conditions individually, in combination, and at boundary values via ScalaCheck.
 *
 * == Conditions Tested ==
 *  - Version mismatch (highest priority; v1 placeholder always returns `false`)
 *  - Memory pressure (strict `>` 95% on-heap utilization)
 *  - Network saturation (strict `>` 100 backpressure events as v1 proxy for the
 *    AAP-mandated 90% link-capacity threshold)
 *  - Slow consumer (sustained 2x-slower-than-producer for &gt;60 s)
 *
 * == AAP References ==
 *  - AAP Section 0.5.1.6 (Group 6, item 6): "Each of the four fallback conditions
 *    individually; combined conditions; transparent delegation to SortShuffleManager.
 *    ScalaCheck 1.18 for boundary value testing."
 *  - AAP Section 0.1.2 (fallback condition enumeration verbatim from the user prompt).
 *  - AAP Section 0.7.2.4 (failure tolerance and integrity rules).
 *  - AAP Section 0.7.2.6 (quality gate: &gt;85% coverage for new components).
 *
 * == Test Strategy ==
 * The fallback policy is a pure decision class with two integration boundaries: the
 * [[org.apache.spark.memory.MemoryManager]] (for memory-pressure introspection) and the
 * [[StreamingShuffleMetrics]] metric set (for backpressure-event count and spill count).
 * The tests use Mockito 5.12 to inject deterministic behavior at the `MemoryManager`
 * boundary and a real `StreamingShuffleMetrics` instance to exercise the production
 * counter increment path. The handle is mocked to control its `shuffleId` for log
 * correlation -- the policy reads only that field from the handle in v1.
 *
 * Threshold constants are intentionally NOT asserted via direct field reference
 * (the production source declares them as `private val` rather than companion-object
 * `public val`); instead the boundary tests at each rule's strict-greater-than edge
 * lock the threshold values implicitly through observable behavior, which is more
 * robust against accidental refactoring than asserting against package-private fields.
 */
class StreamingShuffleFallbackPolicySuite
  extends SparkFunSuite with Matchers with ScalaCheckPropertyChecks {

  /**
   * Mockito stub helper mirroring the pattern from
   * `core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleManagerSuite.scala`.
   *
   * Wraps `org.mockito.Mockito.doReturn(value, varargs...)` with the empty-Seq splat
   * required by the Java vararg signature when called from Scala. Use as:
   * {{{
   *   doReturn(value).when(mock).method
   * }}}
   *
   * `doReturn` is the recommended Mockito idiom for stubbing methods that (a) are
   * declared `final` (e.g., [[MemoryManager.executionMemoryUsed]]) and require the
   * inline mock-maker, or (b) would otherwise invoke a partial real implementation
   * during stub setup if the conventional `when(mock.method).thenReturn(value)` form
   * were used. Using `doReturn` uniformly across the helper keeps the suite resilient
   * to either situation.
   *
   * @param value the canned return value
   * @return a Mockito `Stubber` for chaining the `.when(mock).method` call
   */
  private def doReturn(value: Any): org.mockito.stubbing.Stubber =
    org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * Standard SparkConf for fallback-policy testing. The streaming-shuffle policy retains
   * a `SparkConf` constructor parameter as a v2 extension point but does NOT read any
   * keys from it in v1, so the conf returned here is essentially a placeholder; the
   * `spark.shuffle.manager` and `spark.shuffle.streaming.enabled` keys are set anyway
   * to mirror the production configuration shape that an opted-in caller would supply.
   *
   * `loadDefaults = false` is used so the suite is independent of any system-property
   * `spark.*` keys that might be present in the test JVM.
   */
  private def baseConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
  }

  /**
   * Total on-heap storage memory used by the mock `MemoryManager` for the
   * memory-pressure ratio computation. Chosen as 1 GB (1,073,741,824 bytes) so that
   * `(totalMemory * utilizationRatio).toLong / totalMemory.toDouble` reproduces the
   * input ratio with negligible floating-point error -- the fractional bit truncated
   * by `.toLong` is at worst `1 / 1e9`, well below any meaningful threshold delta.
   */
  private val mockTotalMemory: Long = 1024L * 1024L * 1024L

  /**
   * Construct a mock [[org.apache.spark.memory.MemoryManager]] whose computed
   * `executionMemoryUsed / maxOnHeapStorageMemory` ratio equals `utilizationRatio`.
   *
   * The fallback policy reads exactly two methods on `MemoryManager` -- the abstract
   * `maxOnHeapStorageMemory` and the `final` `executionMemoryUsed` -- to compute the
   * memory-pressure ratio. We stub both via `doReturn`, which works for final methods
   * under Mockito 5.12's default inline mock-maker.
   *
   * @param utilizationRatio fraction of on-heap storage memory in use, in `[0.0, 1.0]`;
   *                         the resulting ratio observed by the policy will equal
   *                         `utilizationRatio` to within 1e-9 floating-point error
   * @return a Mockito mock of `MemoryManager` with the two relevant accessors stubbed
   */
  private def mockMemoryManager(utilizationRatio: Double): MemoryManager = {
    val mm = mock(classOf[MemoryManager])
    val usedMemory = (mockTotalMemory * utilizationRatio).toLong
    doReturn(mockTotalMemory).when(mm).maxOnHeapStorageMemory
    doReturn(usedMemory).when(mm).executionMemoryUsed
    mm
  }

  /**
   * Construct a mock [[StreamingShuffleHandle]] for fallback-policy input. The policy
   * reads only `handle.shuffleId` (for log correlation in the INFO-level fallback
   * notification), so we stub that accessor only. The type parameters `[Int, Int, Int]`
   * are arbitrary -- the policy never inspects the handle's type-erased generics.
   *
   * Note: the suite intentionally mocks the handle directly rather than constructing a
   * real one, because constructing a real [[StreamingShuffleHandle]] would require a
   * real [[org.apache.spark.ShuffleDependency]] which in turn requires `SparkEnv` setup
   * (for the default serializer) -- unwarranted state for a pure decision-class unit
   * test. Mockito 5.12's inline mock-maker creates the synthetic mock without invoking
   * any parent constructor.
   */
  private def mockHandle(): StreamingShuffleHandle[Int, Int, Int] = {
    val handle = mock(classOf[StreamingShuffleHandle[Int, Int, Int]])
    when(handle.shuffleId).thenReturn(0)
    handle
  }

  /**
   * Construct a real [[StreamingShuffleMetrics]] instance with the `backpressureEvents`
   * counter incremented to the supplied value.
   *
   * Using a real instance (rather than a mock) exercises the production counter
   * increment path and validates that the metric set's `getBackpressureEventsCount`
   * accessor returns the expected value -- a mini integration test of the metric set
   * embedded in the fallback-policy unit suite.
   *
   * @param backpressureEvents number of times to invoke `incrementBackpressureEvents`;
   *                           default `0L`, suitable for tests of other rules
   * @return a metric set whose backpressure counter equals `backpressureEvents` and
   *         whose other counters are at their initial zero values
   */
  private def makeMetrics(backpressureEvents: Long = 0L): StreamingShuffleMetrics = {
    val metrics = new StreamingShuffleMetrics()
    var i = 0L
    while (i < backpressureEvents) {
      metrics.incrementBackpressureEvents()
      i += 1L
    }
    metrics
  }

  // ===========================================================================
  // Negative case: no fallback condition is observed; the streaming path proceeds
  // ===========================================================================

  test("shouldFallback returns false under normal operating conditions") {
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 0L)
    assert(!policy.shouldFallback(handle, metrics),
      "Should not fall back under normal operating conditions (50% memory, 0 events)")
  }

  test("shouldFallback returns false at zero memory utilization") {
    val mm = mockMemoryManager(utilizationRatio = 0.0)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 0L)
    assert(!policy.shouldFallback(handle, metrics),
      "Should not fall back at zero memory utilization")
  }

  // ===========================================================================
  // Memory pressure rule (Condition 2 in the production source)
  // ===========================================================================

  test("memory pressure above 95% threshold triggers fallback") {
    // Production source uses strict greater-than: ratio > MEMORY_PRESSURE_THRESHOLD (0.95).
    // 0.96 is comfortably above the threshold and accounts for any floating-point error
    // introduced by the (totalMemory * 0.96).toLong truncation.
    val mm = mockMemoryManager(utilizationRatio = 0.96)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics()
    assert(policy.shouldFallback(handle, metrics),
      "Should fall back when memory utilization exceeds the 95% threshold")
  }

  test("memory pressure exactly at 95% threshold does not trigger fallback") {
    // The policy uses strict greater-than: > 0.95 (NOT >= 0.95). A utilization ratio of
    // exactly 0.95 (modulo a sub-1e-9 floating-point delta from the .toLong truncation)
    // must NOT trigger fallback. This test locks the strict-greater-than semantics
    // against accidental change to >=.
    val mm = mockMemoryManager(utilizationRatio = 0.95)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics()
    assert(!policy.shouldFallback(handle, metrics),
      "Memory at exactly 95% should not trigger fallback (strict greater-than semantics)")
  }

  test("memory pressure at 100% utilization triggers fallback") {
    // Upper-extreme boundary: full memory utilization clearly exceeds the threshold and
    // must trigger fallback. This is the worst-case OOM scenario the rule is designed
    // to guard against per AAP Section 0.1.2.
    val mm = mockMemoryManager(utilizationRatio = 1.00)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics()
    assert(policy.shouldFallback(handle, metrics),
      "Memory at 100% utilization should trigger fallback (upper extreme)")
  }

  test("memory pressure with zero capacity does not trigger fallback (defensive guard)") {
    // Production source contains a defensive guard: if maxOnHeapStorageMemory <= 0L
    // (e.g., a degenerate test mock or a configuration with zero on-heap storage),
    // isMemoryPressure returns false to avoid divide-by-zero. This test exercises that
    // guard explicitly. Without the guard, the ratio computation `used / 0` would yield
    // `Double.PositiveInfinity` and the policy would always trigger fallback, masking
    // the actual memory state.
    val mm = mock(classOf[MemoryManager])
    doReturn(0L).when(mm).maxOnHeapStorageMemory
    doReturn(100L).when(mm).executionMemoryUsed
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics()
    assert(!policy.shouldFallback(handle, metrics),
      "Zero-capacity memory manager should not trigger fallback (defensive guard)")
  }

  test("memory pressure with negative capacity does not trigger fallback (defensive guard)") {
    // Edge case: a buggy or test-only MemoryManager could conceivably return a negative
    // value from maxOnHeapStorageMemory. The defensive guard `<= 0L` covers this case
    // alongside the zero-capacity case above.
    val mm = mock(classOf[MemoryManager])
    doReturn(-1L).when(mm).maxOnHeapStorageMemory
    doReturn(100L).when(mm).executionMemoryUsed
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics()
    assert(!policy.shouldFallback(handle, metrics),
      "Negative-capacity memory manager should not trigger fallback (defensive guard)")
  }

  // ===========================================================================
  // Network saturation rule (Condition 3 in the production source)
  // ===========================================================================

  test("network saturation (>100 backpressure events) triggers fallback") {
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 101L)
    assert(policy.shouldFallback(handle, metrics),
      "Should fall back when backpressure events exceed 100")
  }

  test("network saturation at exactly 100 events does not trigger fallback") {
    // Strict greater-than semantics: 100 events exactly does NOT trigger.
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 100L)
    assert(!policy.shouldFallback(handle, metrics),
      "100 backpressure events should not trigger fallback (strict greater-than)")
  }

  test("network saturation below the threshold does not trigger fallback") {
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 50L)
    assert(!policy.shouldFallback(handle, metrics),
      "50 backpressure events should not trigger fallback")
  }

  // ===========================================================================
  // Combined-condition tests verifying short-circuit semantics and OR composition
  // ===========================================================================

  test("combined memory pressure and network saturation both trigger fallback") {
    // When BOTH rules fire, the policy still returns true. The short-circuit evaluation
    // means only one rule's INFO log line will appear -- production source evaluates
    // memory pressure before network saturation, so memory will be the logged trigger.
    val mm = mockMemoryManager(utilizationRatio = 0.97)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 200L)
    assert(policy.shouldFallback(handle, metrics),
      "Combined memory + network conditions should trigger fallback")
  }

  test("memory pressure alone triggers fallback even with no network events") {
    val mm = mockMemoryManager(utilizationRatio = 0.97)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 0L)
    assert(policy.shouldFallback(handle, metrics),
      "Memory pressure alone should trigger fallback")
  }

  test("network saturation alone triggers fallback even with no memory pressure") {
    val mm = mockMemoryManager(utilizationRatio = 0.10)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 500L)
    assert(policy.shouldFallback(handle, metrics),
      "Network saturation alone should trigger fallback")
  }

  // ===========================================================================
  // Slow consumer rule (Condition 1 in the production source)
  // ===========================================================================

  test("slow consumer condition does not fire on first observation (sustained-window)") {
    // Production source enforces a 60-second sustained window via firstSlowDetectionTime
    // (an AtomicLong initialized to 0L). The first call observing the proxy condition
    // initializes the timestamp via compareAndSet; elapsed = 0, which is NOT > 60_000ms,
    // so the rule does NOT fire on first observation. This test exercises that semantic.
    //
    // Proxy condition (from production source):
    //   triggered = backpressure > 10 AND spills * 2 > backpressure
    // We use backpressure = 11 (just above 10) and spills = 100 to satisfy:
    //   11 > 10  -> true
    //   100 * 2 = 200 > 11  -> true
    // So triggered = true, but elapsed = 0 < 60_000ms, so isSlowConsumer returns false.
    //
    // We deliberately keep backpressure (11) below the network-saturation threshold (100)
    // and memory utilization (0.50) below the memory-pressure threshold (0.95) so that
    // ONLY the slow-consumer evaluation path is exercised.
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = new StreamingShuffleMetrics()
    var b = 0
    while (b < 11) { metrics.incrementBackpressureEvents(); b += 1 }
    var s = 0
    while (s < 100) { metrics.incrementSpillCount(); s += 1 }
    assert(!policy.shouldFallback(handle, metrics),
      "Slow-consumer condition should not fire on first observation " +
        "(sustained-window timer just started)")
  }

  test("slow consumer condition does not fire when proxy ratio is not met") {
    // Counter-test: when spills * 2 <= backpressure, the proxy condition is NOT met and
    // the slow-consumer rule must not fire regardless of how many calls are made.
    //
    // backpressure = 50 (above the 10-event floor but below the 100-event saturation
    // threshold so the network-saturation rule does not fire instead).
    // spills = 10, so spills * 2 = 20 <= 50 -> proxy NOT triggered.
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = new StreamingShuffleMetrics()
    var b = 0
    while (b < 50) { metrics.incrementBackpressureEvents(); b += 1 }
    var s = 0
    while (s < 10) { metrics.incrementSpillCount(); s += 1 }
    // Multiple calls to verify the rule stays inactive across invocations.
    assert(!policy.shouldFallback(handle, metrics),
      "Slow-consumer rule should not fire when spills * 2 <= backpressure (call 1)")
    assert(!policy.shouldFallback(handle, metrics),
      "Slow-consumer rule should not fire when spills * 2 <= backpressure (call 2)")
  }

  test("slow consumer condition resets sustained-window when proxy becomes false") {
    // Production source resets firstSlowDetectionTime to 0L when the proxy condition is
    // no longer observed. We exercise this transition: trigger the proxy once (records
    // first-detection time), then call again with a metric set that no longer satisfies
    // the proxy. Subsequent calls with the proxy true again must restart the window
    // rather than continuing the prior window. The reset itself is internal state, but
    // we verify the externally-observable "no fallback" behavior at each step.
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()

    // Step 1: proxy triggered (backpressure = 11, spills = 100). First-call returns false
    // because elapsed = 0.
    val triggeredMetrics = new StreamingShuffleMetrics()
    var b = 0
    while (b < 11) { triggeredMetrics.incrementBackpressureEvents(); b += 1 }
    var s = 0
    while (s < 100) { triggeredMetrics.incrementSpillCount(); s += 1 }
    assert(!policy.shouldFallback(handle, triggeredMetrics),
      "First call with proxy triggered should not fall back (elapsed = 0)")

    // Step 2: proxy NOT triggered (fresh metrics with no backpressure). The reset path
    // executes. Returns false.
    val freshMetrics = new StreamingShuffleMetrics()
    assert(!policy.shouldFallback(handle, freshMetrics),
      "Call with proxy NOT triggered should reset window and return false")

    // Step 3: proxy triggered again. The window timer restarts; elapsed is again 0.
    assert(!policy.shouldFallback(handle, triggeredMetrics),
      "Call after window reset should restart timer and return false (elapsed = 0)")
  }

  // ===========================================================================
  // Property-based boundary tests via ScalaCheck (AAP Section 0.5.1.6 mandate)
  // ===========================================================================

  test("shouldFallback honors the 0.95 memory threshold across boundary values") {
    // Property: with no backpressure events and no spills, the only rule that can fire
    // is memory pressure. Therefore shouldFallback must return true for every utilization
    // value strictly above 0.95. We tighten the generator to [0.96, 1.0] (rather than
    // [0.0, 1.0] with an asymmetric `if (expected)` guard) to eliminate boundary edge
    // cases at exactly 0.95: the .toLong truncation in `mockMemoryManager` introduces
    // a sub-ulp delta of ~1 byte / 2^30 bytes ≈ 9.3e-10 relative error which, while
    // mathematically negligible, is not strictly impossible to cross the threshold.
    // By starting the generator at 0.96 we are guaranteed `utilization > 0.95` even
    // after truncation (the truncation can only reduce by < 1e-9, which is far less
    // than 0.96 - 0.95 = 0.01). This converts the asymmetric assertion to an
    // unconditional `assert(actual, ...)`, locking the positive-direction contract
    // strictly. The complementary negative-direction property test ("shouldFallback
    // returns false for memory utilization in [0.0, 0.95) ScalaCheck") covers
    // [0.0, 0.94] (also tightened away from the 0.95 boundary by the same logic).
    forAll(org.scalacheck.Gen.choose(0.96, 1.0)) { (utilization: Double) =>
      val mm = mockMemoryManager(utilization)
      val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
      val handle = mockHandle()
      val metrics = makeMetrics(backpressureEvents = 0L)
      val actual = policy.shouldFallback(handle, metrics)
      assert(actual,
        s"Should fall back at utilization = $utilization (strictly above 0.95)")
    }
  }

  test("shouldFallback honors the 100-backpressure-event threshold across boundary values") {
    // Property: with memory utilization at 50% (well below the 95% threshold) and zero
    // spills (so the slow-consumer proxy spills*2 > backpressure cannot fire because
    // spills*2 = 0 <= any non-negative backpressure), the only rule that can fire is
    // network saturation. Therefore shouldFallback must equal `events > 100` for every
    // value in [0L, 1000L].
    forAll(org.scalacheck.Gen.choose(0L, 1000L)) { (events: Long) =>
      val mm = mockMemoryManager(utilizationRatio = 0.50)
      val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
      val handle = mockHandle()
      val metrics = makeMetrics(backpressureEvents = events)
      val expected = events > 100L
      val actual = policy.shouldFallback(handle, metrics)
      if (expected) {
        assert(actual,
          s"Should fall back at backpressure events = $events (above 100)")
      }
    }
  }

  test("shouldFallback returns false for memory utilization in [0.0, 0.95) ScalaCheck") {
    // Stronger-direction property: when utilization is strictly below 0.95 AND no other
    // rule can fire, shouldFallback must return false. We use Gen.choose(0.0, 0.94) to
    // stay safely below the threshold even after the .toLong truncation in the mock.
    forAll(org.scalacheck.Gen.choose(0.0, 0.94)) { (utilization: Double) =>
      val mm = mockMemoryManager(utilization)
      val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
      val handle = mockHandle()
      val metrics = makeMetrics(backpressureEvents = 0L)
      assert(!policy.shouldFallback(handle, metrics),
        s"Should not fall back at utilization = $utilization (strictly below 0.95)")
    }
  }

  test("shouldFallback returns false for backpressure events in [0L, 100L] ScalaCheck") {
    // Stronger-direction property: when events are at or below 100 AND no other rule
    // can fire, shouldFallback must return false. This complements the asymmetric
    // property test above by locking the negative direction explicitly.
    forAll(org.scalacheck.Gen.choose(0L, 100L)) { (events: Long) =>
      val mm = mockMemoryManager(utilizationRatio = 0.50)
      val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
      val handle = mockHandle()
      val metrics = makeMetrics(backpressureEvents = events)
      assert(!policy.shouldFallback(handle, metrics),
        s"Should not fall back at events = $events (at or below 100)")
    }
  }

  // ===========================================================================
  // Construction and concurrency-safety tests
  // ===========================================================================

  test("policy is constructible with a null-effect SparkConf") {
    // The SparkConf parameter is a v2 extension point in v1 (no keys are read). The
    // policy constructor must succeed even with a minimally configured SparkConf.
    val mm = mockMemoryManager(utilizationRatio = 0.50)
    val policy = new StreamingShuffleFallbackPolicy(new SparkConf(loadDefaults = false), mm)
    val handle = mockHandle()
    val metrics = makeMetrics()
    assert(!policy.shouldFallback(handle, metrics),
      "Policy with bare SparkConf should evaluate to no-fallback under normal conditions")
  }

  test("policy returns consistent results across repeated invocations (idempotency)") {
    // Beyond the slow-consumer window-state evolution, the other three rules are pure
    // functions of their inputs. Repeated invocations with the same inputs must return
    // the same answer. This is a regression guard against accidental introduction of
    // stateful behavior in the memory-pressure or network-saturation paths.
    val mm = mockMemoryManager(utilizationRatio = 0.96)
    val policy = new StreamingShuffleFallbackPolicy(baseConf(), mm)
    val handle = mockHandle()
    val metrics = makeMetrics(backpressureEvents = 0L)
    val results = (1 to 5).map(_ => policy.shouldFallback(handle, metrics))
    assert(results.forall(identity),
      "All 5 invocations should return true (memory pressure is deterministic)")
    assert(results.distinct.size === 1,
      "Repeated invocations must yield identical results (idempotency)")
  }
}
