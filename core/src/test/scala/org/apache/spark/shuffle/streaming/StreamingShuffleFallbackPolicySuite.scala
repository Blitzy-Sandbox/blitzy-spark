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

import java.util.concurrent.TimeUnit

import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}

/**
 * Unit tests for [[StreamingShuffleFallbackPolicy]] -- the type that encodes the streaming
 * shuffle backend's zero-regression guarantee. The policy reverts ("falls back") to sort-based
 * shuffle the moment ANY of four revert conditions holds, so these tests validate each condition
 * independently, the [[StreamingShuffleFallbackPolicy.shouldFallback]] OR-composition, and the
 * human-readable [[StreamingShuffleFallbackPolicy.fallbackReason]].
 *
 * The four revert conditions (thresholds sourced from the [[StreamingShuffleConfig]] companion
 * constants, never hard-coded in the test) are:
 *
 *   1. Slow consumer -- the producer sustains more than
 *      [[StreamingShuffleConfig.SLOW_CONSUMER_RATIO]]x the consumer's throughput for longer than
 *      [[StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS]] seconds.
 *   2. Memory pressure -- buffer-allocation memory utilization exceeds
 *      [[StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT]]%.
 *   3. Network saturation -- link utilization exceeds
 *      [[StreamingShuffleConfig.NETWORK_SATURATION_PERCENT]]%.
 *   4. Version mismatch -- a producer/consumer streaming-protocol version mismatch was reported.
 *
 * ==Determinism==
 *
 * The slow-consumer condition is intrinsically time-based (it requires a sustained imbalance), but
 * the suite must run in milliseconds. [[StreamingShuffleFallbackPolicy.recordThroughput]] opens the
 * slow window using an internal `System.nanoTime()` reading, while
 * [[StreamingShuffleFallbackPolicy.isSlowConsumer]] accepts an explicit `nowNanos` -- the seam that
 * exists precisely so the 60s boundary can be crossed with an injected timestamp instead of a real
 * wall-clock sleep. The suite NEVER sleeps; it brackets `recordThroughput` with monotonic readings
 * to bound the window-open instant and then evaluates `isSlowConsumer(nowNanos)` on both sides of
 * the threshold. The remaining three conditions are time-independent and are exercised directly.
 *
 * The suite is pure and deterministic: it needs no `SparkContext`, constructs the policy with a
 * standalone [[SparkConf]], and leaves the optional metrics holder `null` (its default).
 */
class StreamingShuffleFallbackPolicySuite extends SparkFunSuite with Matchers {

  /**
   * Builds a fresh [[StreamingShuffleFallbackPolicy]] backed by a standalone [[SparkConf]]. The
   * optional `extra` key/value pairs override individual settings before the typed config is
   * derived; with no overrides the policy uses the registered `ConfigEntry` defaults. The metrics
   * holder is left at its `null` default, so the helper also doubles as the null-safety fixture.
   */
  private def newPolicy(extra: (String, String)*): StreamingShuffleFallbackPolicy = {
    val conf = new SparkConf(false)
    extra.foreach { case (k, v) => conf.set(k, v) }
    new StreamingShuffleFallbackPolicy(new StreamingShuffleConfig(conf))
  }

  test("fresh policy does not fall back") {
    val p = newPolicy()
    // A brand-new policy has recorded no throughput, memory, network, or version signal, so every
    // individual predicate -- and therefore the aggregate decision -- must report "stay streaming".
    p.isSlowConsumer() mustBe false
    p.isMemoryPressure mustBe false
    p.isNetworkSaturated mustBe false
    p.isVersionMismatch mustBe false
    p.shouldFallback mustBe false
    p.fallbackReason mustBe None
  }

  test("slow consumer trips only after the imbalance is sustained past 60s") {
    val p = newPolicy()
    // recordThroughput opens the slow-consumer window with an internal System.nanoTime() reading,
    // so we bracket the call to bound that instant: the window opens somewhere in [before, after].
    val before = System.nanoTime()
    p.recordThroughput(100L, 10L) // producer 10x the consumer: well past the 2x ratio
    val after = System.nanoTime()

    val oneSecond = TimeUnit.SECONDS.toNanos(1L)
    val pastThreshold =
      TimeUnit.SECONDS.toNanos(StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS + 1L)

    // Short window: at most 1s of simulated elapsed time (since >= before) must NOT trip -- a
    // transient imbalance is not a fallback condition; the slowness has to persist.
    p.isSlowConsumer(before + oneSecond) mustBe false
    // Long window: "after + 61s" guarantees elapsed >= 61s > 60s regardless of where the window
    // actually opened, so the sustained-slowness predicate must trip deterministically.
    p.isSlowConsumer(after + pastThreshold) mustBe true

    // Recovery: once the consumer catches up the window closes, so even a far-future timestamp no
    // longer trips -- a subsequent imbalance must restart the 60s timer from scratch.
    p.recordThroughput(100L, 100L) // 1x ratio: consumer keeping up
    p.isSlowConsumer(after + pastThreshold) mustBe false
  }

  test("memory pressure trips above the configured threshold") {
    // The production predicate is a strict integer comparison (> MEMORY_PRESSURE_PERCENT), so the
    // boundary is exercised at integer granularity around the canonical 95% constant.
    val threshold = StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT

    val tripped = newPolicy()
    tripped.updateMemoryUtilization(threshold + 1) // 96%: above the threshold
    tripped.isMemoryPressure mustBe true
    tripped.shouldFallback mustBe true

    val atBoundary = newPolicy()
    atBoundary.updateMemoryUtilization(threshold) // 95%: strictly-greater-than means no trip
    atBoundary.isMemoryPressure mustBe false
    atBoundary.shouldFallback mustBe false

    val below = newPolicy()
    below.updateMemoryUtilization(threshold - 1) // 94%: clearly under the threshold
    below.isMemoryPressure mustBe false
  }

  test("network saturation trips above the configured threshold") {
    // Same strict integer-boundary semantics as memory pressure, around the canonical 90% constant.
    val threshold = StreamingShuffleConfig.NETWORK_SATURATION_PERCENT

    val tripped = newPolicy()
    tripped.updateNetworkUtilization(threshold + 1) // 91%: above the threshold
    tripped.isNetworkSaturated mustBe true
    tripped.shouldFallback mustBe true

    val atBoundary = newPolicy()
    atBoundary.updateNetworkUtilization(threshold) // 90%: boundary value must not trip
    atBoundary.isNetworkSaturated mustBe false
    atBoundary.shouldFallback mustBe false

    val below = newPolicy()
    below.updateNetworkUtilization(threshold - 1) // 89%: clearly under the threshold
    below.isNetworkSaturated mustBe false
  }

  test("version mismatch trips immediately") {
    val p = newPolicy()
    // A protocol version mismatch cannot self-heal, so it trips the policy at once and stays set.
    p.markVersionMismatch()
    p.isVersionMismatch mustBe true
    p.shouldFallback mustBe true
    // The reason must name the active condition for the decision log and structured logs. The
    // production string is emitted in lower case, so the substring check is case-sensitive.
    p.fallbackReason mustBe defined
    p.fallbackReason.get must include("version")
  }

  test("shouldFallback is the OR of all conditions and the reason names the active condition") {
    // Baseline: with no condition tripped the aggregate decision is "stay streaming".
    newPolicy().shouldFallback mustBe false

    // Memory pressure alone trips the OR and the reason identifies memory.
    val mem = newPolicy()
    mem.updateMemoryUtilization(StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT + 1)
    mem.shouldFallback mustBe true
    mem.fallbackReason.get must include("memory")

    // Network saturation alone trips the OR and the reason identifies network.
    val net = newPolicy()
    net.updateNetworkUtilization(StreamingShuffleConfig.NETWORK_SATURATION_PERCENT + 1)
    net.shouldFallback mustBe true
    net.fallbackReason.get must include("network")

    // Version mismatch alone trips the OR and the reason identifies the version mismatch.
    val ver = newPolicy()
    ver.markVersionMismatch()
    ver.shouldFallback mustBe true
    ver.fallbackReason.get must include("version")

    // Two simultaneous conditions still yield a single, non-empty reason (first match wins).
    val both = newPolicy()
    both.updateMemoryUtilization(StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT + 1)
    both.markVersionMismatch()
    both.shouldFallback mustBe true
    both.fallbackReason mustBe defined
  }

  test("metrics holder is optional and the fallback path is null-safe") {
    // newPolicy leaves metrics at its null default; the fallback transition increments the metrics
    // holder only when it is non-null, so engaging fallback here must not raise an NPE.
    val p = newPolicy()
    p.markVersionMismatch()
    noException must be thrownBy {
      p.shouldFallback mustBe true
    }
  }
}
