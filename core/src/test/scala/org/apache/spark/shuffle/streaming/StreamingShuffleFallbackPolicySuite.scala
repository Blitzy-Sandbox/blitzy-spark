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
 * the four active pre-registration conditions plus the happy path:
 *
 *   1. `spark.shuffle.streaming.enabled = false` &rarr;
 *      `Some("streaming-disabled-by-config")`.
 *   2. `spark.shuffle.push.enabled = true` (ADR-005 mutual exclusion with
 *      push-based shuffle) &rarr; `Some("push-based-shuffle-active")`.
 *   3. `numPartitions <= 0` (defensive partitioner-sanity check) &rarr;
 *      `Some("invalid-partition-count")`.
 *   4. `spark.executor.memory < 256 MiB` (streaming-buffer-budget sanity) &rarr;
 *      `Some("insufficient-executor-memory")`.
 *   5. All four conditions clear &rarr; `None` (streaming path is selected).
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
   * Base `SparkConf` where all four pre-registration conditions are
   * satisfied, so `StreamingShuffleFallbackPolicy.evaluate` would return
   * `None` (streaming path selected):
   *
   *   - `spark.shuffle.streaming.enabled = true` (kill switch on).
   *   - `spark.shuffle.push.enabled = false` (ADR-005 mutually exclusive;
   *     push disabled so streaming can proceed).
   *   - `spark.executor.memory = 1024 MiB` (well above the 256 MiB
   *     `MINIMUM_EXECUTOR_MEMORY_MIB` threshold).
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
    "EXECUTOR_MEMORY < 256 MiB") {
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 128L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("insufficient-executor-memory"))
  }

  test("evaluate returns None when all conditions pass (streaming proceeds)") {
    val conf = goodConf()
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(None)
  }

  // ==========================================================================
  // Group 2: Boundary values for integer-ranged conditions
  // ==========================================================================

  test("evaluate returns None for exactly 256 MiB executor memory (boundary pass)") {
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 256L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(None)
  }

  test("evaluate returns Some('insufficient-executor-memory') for 255 MiB " +
    "executor memory (boundary fail)") {
    val conf = goodConf().set(config.EXECUTOR_MEMORY, 255L)
    val dep = depWithPartitions(10)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(Some("insufficient-executor-memory"))
  }

  test("evaluate returns None for partitionCount = 1 (smallest valid)") {
    val conf = goodConf()
    val dep = depWithPartitions(1)
    val metrics = new StreamingShuffleMetrics()

    val result = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, metrics)

    result must be(None)
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
    results.head must be(None)
  }

  test("evaluate with null metrics does not throw") {
    // The `metrics` parameter is a reserved extension point (v1 does not read
    // from it) and the policy MUST tolerate `null` so that callers can use
    // `evaluate` in deep unit-test harnesses that have no Dropwizard registry.
    val conf = goodConf()
    val dep = depWithPartitions(10)

    noException must be thrownBy {
      val r = StreamingShuffleFallbackPolicy.evaluate(0, dep, conf, null)
      r must be(None)
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
}
