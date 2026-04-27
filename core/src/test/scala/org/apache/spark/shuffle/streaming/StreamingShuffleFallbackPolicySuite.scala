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

import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config

/**
 * Unit tests for [[StreamingShuffleFallbackPolicy]] &mdash; the pure, stateless
 * decision-only routing oracle that tells `StreamingShuffleManager` whether a
 * newly registered shuffle should use the streaming path or fall back to the
 * held `SortShuffleManager`. The suite validates, in precedence order, each of
 * the five active pre-registration conditions plus the happy path:
 *
 *   1. `spark.shuffle.streaming.enabled = false` &rarr;
 *      `Some("streaming-disabled-by-config")`.
 *   2. `spark.shuffle.push.enabled = true` (ADR-005 mutual exclusion with
 *      push-based shuffle) &rarr; `Some("push-based-shuffle-active")`.
 *   3. `numPartitions <= 0` (defensive partitioner-sanity check) &rarr;
 *      `Some("invalid-partition-count")`.
 *   4. `spark.executor.memory < 512 MiB` (streaming-buffer-budget sanity) &rarr;
 *      `Some("insufficient-executor-memory")`.
 *   5. v1 transport-readiness safety guard: while the compile-time invariant
 *      `STREAMING_TRANSPORT_READY_V1 = false` &rarr;
 *      `Some("streaming-transport-unavailable-v1")`. This is evaluated LAST so
 *      the earlier, more specific reason codes keep their precedence.
 *   6. All conditions clear AND transport is ready &rarr; `None` (streaming
 *      path is selected). In the v1 codebase this outcome is unreachable
 *      because [[StreamingShuffleFallbackPolicy.STREAMING_TRANSPORT_READY_V1]]
 *      is hard-coded to `false`; the happy-path `None` assertion is deferred
 *      to the sibling agent that lands the transport and flips the constant.
 *
 * Test ownership and hygiene:
 *   - Every test is pure in-JVM logic: no `SparkContext`, no executor bootstrap,
 *     no RPC machinery, no file-system or network resources. Runtime is
 *     expected to be well under one second per test and fully deterministic
 *     (no sleeps, timing, async, or flakiness).
 *   - `ShuffleDependency` is mocked with a [[RuntimeExceptionAnswer]] trap
 *     &mdash; any non-stubbed method call on the mock fails the test
 *     immediately, ensuring we exercise exactly the surface the policy
 *     actually touches (`partitioner.numPartitions`).
 *   - `SparkConf` is constructed with `loadDefaults = false` to decouple
 *     tests from JVM-level `spark.*` system properties.
 *   - `StreamingShuffleMetrics` is instantiated as a real object in almost
 *     every test (v1 `evaluate` does not read from it, so the instance is
 *     functionally a witness parameter) and is additionally passed as `null`
 *     in the null-safety test to document the policy's defensive contract.
 *
 * Mirrors the Mockito Pattern B setup used by
 * `org.apache.spark.shuffle.sort.SortShuffleManagerSuite` &mdash; see the
 * `doReturn` helper and `RuntimeExceptionAnswer` nested class below.
 *
 * AAP references:
 *   - &sect;0.2.3.5 Row T6 &mdash; StreamingShuffleFallbackPolicySuite.scala.
 *   - &sect;0.5.1.3 &mdash; "Four fallback conditions validated independently
 *     with deterministic mocks."
 *   - &sect;0.7.2 ADR-005 &mdash; streaming shuffle and push-based shuffle are
 *     mutually exclusive per active shuffle.
 */
class StreamingShuffleFallbackPolicySuite extends SparkFunSuite with Matchers {

  // ==========================================================================
  // Mockito Pattern B helpers &mdash; mirrors SortShuffleManagerSuite.
  // ==========================================================================

  /**
   * Bridge to the overloaded `Mockito.doReturn(Object, Object...)` varargs
   * method that Scala cannot select unambiguously on its own. Returning the
   * result of `doReturn(value)` as `Stubber` preserves the fluent
   * `.when(mock).method` chain used throughout the suite.
   */
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * Mockito [[Answer]] that throws on every invocation. Used as the default
   * answer for `ShuffleDependency` mocks so that any method this suite forgot
   * to stub is reported as a test failure &mdash; the policy touches only
   * `partitioner.numPartitions`, so any other call on the mock is a symptom
   * of a regression in the policy's implementation.
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  /**
   * Produces a `ShuffleDependency[Any, Any, Any]` mock whose `partitioner`
   * returns a `Partitioner` with the requested `numPartitions`. The
   * partitioner is a real (non-mock) `Partitioner` subclass because
   * `StreamingShuffleFallbackPolicy.evaluate` calls `numPartitions` on it
   * and a `Partitioner` mock would require additional stubbing without any
   * benefit. `getPartition` is overridden to a constant `0` because the
   * policy never invokes it.
   */
  private def depWithPartitions(numParts: Int): ShuffleDependency[Any, Any, Any] = {
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    val part = new Partitioner {
      override def numPartitions: Int = numParts
      override def getPartition(key: Any): Int = 0
    }
    doReturn(part).when(dep).partitioner
    dep
  }

  /**
   * Base `SparkConf` where all four config-driven pre-registration conditions
   * are satisfied, so `StreamingShuffleFallbackPolicy.evaluate` would reach
   * the v1 transport-readiness safety guard (condition #5) and return
   * `Some("streaming-transport-unavailable-v1")` in this build:
   *
   *   - `spark.shuffle.streaming.enabled = true` (kill switch on).
   *   - `spark.shuffle.push.enabled = false` (ADR-005 mutually exclusive;
   *     push disabled so streaming can proceed).
   *   - `spark.executor.memory = 1024 MiB` (well above the 512 MiB
   *     `MINIMUM_EXECUTOR_MEMORY_MIB` threshold).
   *
   * The v1 transport-readiness guard is a compile-time constant that cannot
   * be overridden via SparkConf; tests therefore verify that `goodConf`
   * produces the `streaming-transport-unavailable-v1` fallback reason, and
   * defer the happy-path `None` assertion to the release that wires up the
   * transport.
   *
   * `loadDefaults = false` suppresses any `spark.*` system properties that
   * may be present on the host JVM, making the test hermetic.
   */
  private def goodConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_STREAMING_ENABLED, true)
      .set("spark.shuffle.push.enabled", "false")
      .set(config.EXECUTOR_MEMORY, 1024L)
  }

  // ==========================================================================
  // Group 1: Each condition triggers the expected fallback reason
  // ==========================================================================

  test("evaluate returns Some('streaming-disabled-by-config') when " +
    "SHUFFLE_STREAMING_ENABLED=false") {
    val conf = goodConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-disabled-by-config"))
  }

  test("evaluate returns Some('push-based-shuffle-active') when " +
    "spark.shuffle.push.enabled=true") {
    val conf = goodConf().set("spark.shuffle.push.enabled", "true")
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("push-based-shuffle-active"))
  }

  test("evaluate returns Some('invalid-partition-count') for zero partitions") {
    val conf = goodConf()
    val dep = depWithPartitions(0)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("invalid-partition-count"))
  }

  test("evaluate returns Some('invalid-partition-count') for negative partitions") {
    val conf = goodConf()
    val dep = depWithPartitions(-5)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("invalid-partition-count"))
  }

  test("evaluate returns Some('insufficient-executor-memory') when " +
    "EXECUTOR_MEMORY < 512 MiB") {
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 128L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("insufficient-executor-memory"))
  }

  test("evaluate returns Some('streaming-transport-unavailable-v1') when all " +
    "config conditions pass (v1 transport-readiness guard)") {
    // In v1 the streaming transport is not yet wired, so the policy returns
    // `Some("streaming-transport-unavailable-v1")` even when every config-
    // driven condition is satisfied. When sibling agents land the transport
    // and flip `STREAMING_TRANSPORT_READY_V1` to `true`, this assertion
    // becomes `result must be(None)`.
    val conf = goodConf()
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-transport-unavailable-v1"))
  }

  // ==========================================================================
  // Group 2: Boundary values for integer-ranged conditions
  // ==========================================================================

  test("evaluate returns Some('streaming-transport-unavailable-v1') for exactly " +
    "512 MiB executor memory (memory boundary pass; v1 transport still unwired)") {
    // 512 MiB is exactly on the `MINIMUM_EXECUTOR_MEMORY_MIB` boundary, so
    // the executor-memory check passes. The policy proceeds to the v1
    // transport-readiness guard, which fires in this build.
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 512L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-transport-unavailable-v1"))
  }

  test("evaluate returns Some('insufficient-executor-memory') for 511 MiB " +
    "executor memory (memory boundary fail)") {
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 511L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("insufficient-executor-memory"))
  }

  test("evaluate returns Some('streaming-transport-unavailable-v1') for " +
    "partitionCount = 1 (smallest valid; v1 transport still unwired)") {
    // partitionCount = 1 is the smallest valid partition count, so the
    // partition-count check passes. The policy proceeds to the v1
    // transport-readiness guard, which fires in this build.
    val conf = goodConf()
    val dep = depWithPartitions(1)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-transport-unavailable-v1"))
  }

  // ==========================================================================
  // Group 3: Precedence ordering (earlier conditions mask later ones)
  //
  // The policy's Scaladoc specifies evaluation order:
  //   1. Feature flag (SHUFFLE_STREAMING_ENABLED)
  //   2. Push-based shuffle (ADR-005 mutual exclusion)
  //   3. Partition-count sanity
  //   4. Executor-memory sanity
  //
  // Each test below constructs a conf where TWO conditions would trigger in
  // isolation; the expected result is the reason of the earlier-ranked
  // condition.
  // ==========================================================================

  test("streaming-disabled precedence over invalid-partition-count") {
    // Both conditions would trigger if evaluated in isolation.
    val conf = goodConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val dep = depWithPartitions(0) // invalid partition count
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-disabled-by-config"))
  }

  test("streaming-disabled precedence over push-enabled") {
    val conf = goodConf()
      .set(config.SHUFFLE_STREAMING_ENABLED, false)
      .set("spark.shuffle.push.enabled", "true")
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-disabled-by-config"))
  }

  test("streaming-disabled precedence over insufficient-memory") {
    val conf = goodConf()
      .set(config.SHUFFLE_STREAMING_ENABLED, false)
      .set(config.EXECUTOR_MEMORY, 128L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-disabled-by-config"))
  }

  test("push-enabled precedence over invalid-partition-count") {
    val conf = goodConf().set("spark.shuffle.push.enabled", "true")
    val dep = depWithPartitions(0)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("push-based-shuffle-active"))
  }

  test("push-enabled precedence over insufficient-memory") {
    val conf = goodConf()
      .set("spark.shuffle.push.enabled", "true")
      .set(config.EXECUTOR_MEMORY, 128L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("push-based-shuffle-active"))
  }

  test("invalid-partition-count precedence over insufficient-memory") {
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 128L)
    val dep = depWithPartitions(0)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("invalid-partition-count"))
  }

  test("insufficient-memory precedence over streaming-transport-unavailable-v1") {
    // With memory at 128 MiB (below the 512 MiB threshold) the insufficient-
    // memory check triggers BEFORE the v1 transport-readiness guard. This
    // confirms the v1 guard is evaluated LAST, preserving specificity of
    // the earlier reason codes.
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 128L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("insufficient-executor-memory"))
  }

  test("streaming-disabled precedence over streaming-transport-unavailable-v1") {
    val conf = goodConf().set(config.SHUFFLE_STREAMING_ENABLED, false)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("streaming-disabled-by-config"))
  }

  test("push-enabled precedence over streaming-transport-unavailable-v1") {
    val conf = goodConf().set("spark.shuffle.push.enabled", "true")
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("push-based-shuffle-active"))
  }

  test("invalid-partition-count precedence over streaming-transport-unavailable-v1") {
    val conf = goodConf()
    val dep = depWithPartitions(0)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("invalid-partition-count"))
  }

  // ==========================================================================
  // Group 4: Determinism and null safety
  // ==========================================================================

  test("evaluate is pure - multiple invocations with same inputs produce same output") {
    val conf = goodConf()
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    // Fifty invocations with byte-identical inputs. If the policy retained
    // any hidden state between calls (a counter, a `var`, a cached config
    // lookup), the results would diverge and `distinct.size` would be > 1.
    val results = (1 to 50).map(_ =>
      StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics))

    results.distinct.size must be(1)
    // `goodConf` produces the v1 transport-readiness fallback in the current
    // build. If/when sibling agents wire up the transport and flip
    // `STREAMING_TRANSPORT_READY_V1` to `true`, this assertion becomes `None`.
    results.head must be(Some("streaming-transport-unavailable-v1"))
  }

  test("evaluate with null metrics does not throw") {
    // The `metrics` parameter is a reserved extension point (v1 does not read
    // from it) and the policy MUST tolerate `null` so that callers can use
    // `evaluate` in deep unit-test harnesses that have no Dropwizard registry.
    val conf = goodConf()
    val dep = depWithPartitions(10)

    noException must be thrownBy {
      val r = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, null)
      // `goodConf` produces the v1 transport-readiness fallback in the
      // current build. If/when sibling agents wire up the transport and flip
      // `STREAMING_TRANSPORT_READY_V1` to `true`, this assertion becomes
      // `None`.
      r must be(Some("streaming-transport-unavailable-v1"))
    }
  }

  test("evaluate with various shuffleIds produces same result given same conf+dep") {
    // The `shuffleId` parameter is used only for the structured-log MDC;
    // it MUST NOT influence the routing decision. Varying the id across
    // 0, 1, and `Int.MaxValue` confirms the decision is shuffleId-agnostic.
    val conf = goodConf()
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val r0 = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)
    val r1 = StreamingShuffleFallbackPolicy.evaluate(1, dep, conf, metrics)
    val rLarge = StreamingShuffleFallbackPolicy.evaluate(Int.MaxValue, dep, conf, metrics)

    r0 must be(r1)
    r1 must be(rLarge)
  }

  // ==========================================================================
  // Group 5: Helper methods (isStreamingEnabled, isPushShuffleActive)
  //
  // These boolean accessors are public extension points exposed for use by
  // `StreamingShuffleManager` construction-time short-circuits; they mirror
  // the corresponding checks inside `evaluate` so the two cannot drift.
  // ==========================================================================

  test("isStreamingEnabled returns SHUFFLE_STREAMING_ENABLED config value (true)") {
    val conf = new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_STREAMING_ENABLED, true)
    StreamingShuffleFallbackPolicy.isStreamingEnabled(conf) must be(true)
  }

  test("isStreamingEnabled returns SHUFFLE_STREAMING_ENABLED config value (false)") {
    val conf = new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_STREAMING_ENABLED, false)
    StreamingShuffleFallbackPolicy.isStreamingEnabled(conf) must be(false)
  }

  test("isPushShuffleActive returns spark.shuffle.push.enabled (true)") {
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.shuffle.push.enabled", "true")
    StreamingShuffleFallbackPolicy.isPushShuffleActive(conf) must be(true)
  }

  test("isPushShuffleActive returns spark.shuffle.push.enabled (false default)") {
    // Key intentionally NOT set -> the untyped `getBoolean(..., false)` in
    // the helper should yield the `false` default.
    val conf = new SparkConf(loadDefaults = false)
    StreamingShuffleFallbackPolicy.isPushShuffleActive(conf) must be(false)
  }

  // ==========================================================================
  // Group: RW-7 runtime observer infrastructure
  // --------------------------------------------------------------------------
  // Tests for the consumer-lag, network-saturation, and version-mismatch
  // observer hooks added per Refine PR work item RW-7. These are scaffolding
  // for the v2 transport (RW-4/RW-5); v1 callsites do not invoke
  // `evaluateRuntime` because `evaluate` short-circuits to the
  // streaming-transport-unavailable-v1 reason ahead of any runtime
  // observation.
  //
  // Each test resets the singleton observer state via
  // `resetObserversForTesting()` to keep cases hermetic.
  // ==========================================================================

  override def beforeEach(): Unit = {
    super.beforeEach()
    StreamingShuffleFallbackPolicy.resetObserversForTesting()
  }

  // Group RW-7.1: recordConsumerLag / isConsumerLagging semantics

  test("isConsumerLagging returns false for an unobserved shuffle") {
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 1000L) must be(false)
  }

  test("isConsumerLagging returns false when ratio is below 2.0") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 1.5, 1000L)
    // A sub-threshold sample should NOT start a lag run; predicate is false.
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 999999L) must be(false)
  }

  test("isConsumerLagging returns false immediately after a >=2.0 sample " +
    "before sustained duration elapses") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.0, 1000L)
    // 30 seconds elapsed -> not yet >60s, so predicate is still false.
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 31000L) must be(false)
  }

  test("isConsumerLagging returns true after sustained 2.0+ lag exceeds 60s") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.0, 1000L)
    // 61 seconds elapsed -> beyond the 60_000 ms threshold.
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 62001L) must be(true)
  }

  test("isConsumerLagging at exactly the 60s boundary is false (strict >)") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.0, 1000L)
    // 60s exactly: 1000 + 60000 = 61000.
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 61000L) must be(false)
  }

  test("isConsumerLagging returns true at 60s + 1 ms (strict > boundary)") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.0, 1000L)
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 61001L) must be(true)
  }

  test("recordConsumerLag with ratio >= 2.0 preserves the original start " +
    "timestamp on contiguous threshold-met observations") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.5, 1000L)
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 3.0, 30000L)
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 4.0, 50000L)
    // start anchor is still 1000L; elapsed = 62001 - 1000 = 61001 > 60000.
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 62001L) must be(true)
  }

  test("recordConsumerLag with ratio < 2.0 resets the sustained-lag timer") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.5, 1000L)
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 1.0, 30000L)
    // The sub-threshold sample reset the timer; the next >=2.0 anchor is
    // 50000L; elapsed = 110000 - 50000 = 60000 (exactly threshold, not >).
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 2.5, 50000L)
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 110000L) must be(false)
    // 60001 ms after the new start IS strictly >60000.
    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 110001L) must be(true)
  }

  test("recordConsumerLag is per-shuffle: shuffleA lag does not affect " +
    "shuffleB") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(101, 3.0, 1000L)
    StreamingShuffleFallbackPolicy.isConsumerLagging(101, 100000L) must be(true)
    StreamingShuffleFallbackPolicy.isConsumerLagging(202, 100000L) must be(false)
  }

  // Group RW-7.2: recordNetworkUtilization / isNetworkSaturated semantics

  test("isNetworkSaturated returns false when no observation has been recorded") {
    StreamingShuffleFallbackPolicy.isNetworkSaturated(1000L) must be(false)
  }

  test("isNetworkSaturated returns false at exactly the 90% threshold " +
    "(strict > boundary)") {
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.90, 1000L)
    StreamingShuffleFallbackPolicy.isNetworkSaturated(1000L) must be(false)
  }

  test("isNetworkSaturated returns true above the 90% threshold") {
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.91, 1000L)
    StreamingShuffleFallbackPolicy.isNetworkSaturated(1000L) must be(true)
  }

  test("isNetworkSaturated reflects the most recent observation only") {
    // Initially saturated...
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.99, 1000L)
    StreamingShuffleFallbackPolicy.isNetworkSaturated(1000L) must be(true)
    // ...then desaturated by a fresh observation.
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.50, 2000L)
    StreamingShuffleFallbackPolicy.isNetworkSaturated(2000L) must be(false)
  }

  // Group RW-7.3: markVersionMismatch / clearVersionMismatch / isVersionMismatched

  test("isVersionMismatched returns false for an unflagged producer") {
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(false)
  }

  test("markVersionMismatch then isVersionMismatched returns true") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(true)
  }

  test("markVersionMismatch is idempotent") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(true)
  }

  test("markVersionMismatch is per-producer: exec-1 mismatch does not affect " +
    "exec-2") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(true)
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-2") must be(false)
  }

  test("clearVersionMismatch removes the flag") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(true)
    StreamingShuffleFallbackPolicy.clearVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(false)
  }

  test("clearVersionMismatch on an unflagged producer is a silent no-op") {
    noException must be thrownBy {
      StreamingShuffleFallbackPolicy.clearVersionMismatch("never-marked")
    }
    StreamingShuffleFallbackPolicy.isVersionMismatched("never-marked") must be(false)
  }

  // Group RW-7.4: evaluateRuntime composite predicate

  test("evaluateRuntime returns None when no runtime conditions trigger") {
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = Some("exec-1"), asOfMillis = 1000L
    ) must be(None)
  }

  test("evaluateRuntime returns Some('runtime-version-mismatch') when " +
    "producer is flagged") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = Some("exec-1"), asOfMillis = 1000L
    ) must be(Some("runtime-version-mismatch"))
  }

  test("evaluateRuntime ignores version mismatch when producerId is None") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = None, asOfMillis = 1000L
    ) must be(None)
  }

  test("evaluateRuntime returns Some('runtime-network-saturated') when " +
    "network exceeds 90%") {
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.95, 500L)
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = Some("exec-1"), asOfMillis = 1000L
    ) must be(Some("runtime-network-saturated"))
  }

  test("evaluateRuntime returns Some('runtime-consumer-lag') when sustained " +
    "lag exceeds 60s") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 3.0, 1000L)
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = Some("exec-1"), asOfMillis = 70000L
    ) must be(Some("runtime-consumer-lag"))
  }

  test("evaluateRuntime evaluates version-mismatch first when multiple " +
    "conditions are active") {
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.99, 500L)
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 3.0, 1000L)
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = Some("exec-1"), asOfMillis = 70000L
    ) must be(Some("runtime-version-mismatch"))
  }

  test("evaluateRuntime evaluates network saturation second (before " +
    "consumer-lag)") {
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.99, 500L)
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 3.0, 1000L)
    StreamingShuffleFallbackPolicy.evaluateRuntime(
      shuffleId = 42, producerId = Some("exec-1"), asOfMillis = 70000L
    ) must be(Some("runtime-network-saturated"))
  }

  test("resetObserversForTesting clears all three observer fields") {
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 3.0, 1000L)
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.99, 1000L)
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")

    StreamingShuffleFallbackPolicy.resetObserversForTesting()

    StreamingShuffleFallbackPolicy.isConsumerLagging(42, 100000L) must be(false)
    StreamingShuffleFallbackPolicy.isNetworkSaturated(100000L) must be(false)
    StreamingShuffleFallbackPolicy.isVersionMismatched("exec-1") must be(false)
  }

  test("evaluate is unaffected by observer state -- pre-registration policy " +
    "remains independent of runtime observers") {
    // Set runtime observers to "everything is broken"...
    StreamingShuffleFallbackPolicy.markVersionMismatch("exec-1")
    StreamingShuffleFallbackPolicy.recordNetworkUtilization(0.99, 1000L)
    StreamingShuffleFallbackPolicy.recordConsumerLag(42, 3.0, 1000L)
    // ...but evaluate (the pre-registration policy) does not consult observer
    // state. With goodConf() it still returns the v1 transport-unavailable
    // reason because the registration-time policy is observer-independent.
    val result = StreamingShuffleFallbackPolicy.evaluate(
      42, depWithPartitions(8), goodConf(), new StreamingShuffleMetrics())
    result must be(Some("streaming-transport-unavailable-v1"))
  }
}
