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
 * Unit tests for [[StreamingShuffleFallbackPolicy]] -- the decision object that encodes the
 * streaming shuffle's zero-regression guarantee by reverting to the sort-based path whenever any
 * of the four revert conditions trips.
 *
 * The four conditions (slow consumer, memory pressure, network saturation, version mismatch) are
 * exercised independently together with their threshold boundaries, then the OR-composed
 * [[StreamingShuffleFallbackPolicy.shouldFallback]] decision and the human-readable
 * [[StreamingShuffleFallbackPolicy.fallbackReason]] are validated.
 *
 * The sustained slow-consumer window (>60s) is driven DETERMINISTICALLY by injecting an explicit
 * monotonic-clock reading through the time-injectable predicates
 * ([[StreamingShuffleFallbackPolicy.isSlowConsumer]] and
 * [[StreamingShuffleFallbackPolicy.shouldFallbackAt]]); the suite NEVER sleeps, so it completes in
 * milliseconds. Because `recordThroughput` stamps the window start from `System.nanoTime()`
 * internally, the slow-consumer tests bracket that call with before/after clock readings so the
 * injected timestamps remain robust regardless of scheduling jitter.
 */
class StreamingShuffleFallbackPolicySuite extends SparkFunSuite with Matchers {

  /**
   * Builds a fallback policy backed by a fresh, defaults-free [[SparkConf]]. Optional key/value
   * pairs allow a test to override individual streaming-shuffle settings; with no overrides the
   * configuration uses the production defaults. The metrics holder defaults to `null` (matching
   * the production signature), so policies built here exercise the null-safe path.
   */
  private def newPolicy(extra: (String, String)*): StreamingShuffleFallbackPolicy = {
    val conf = new SparkConf(false)
    extra.foreach { case (k, v) => conf.set(k, v) }
    new StreamingShuffleFallbackPolicy(new StreamingShuffleConfig(conf))
  }

  test("fresh policy does not fall back") {
    val p = newPolicy()
    assert(!p.shouldFallback)
    assert(p.fallbackReason.isEmpty)
    assert(!p.isSlowConsumer())
    assert(!p.isMemoryPressure)
    assert(!p.isNetworkSaturated)
    assert(!p.isVersionMismatch)
  }

  test("slow consumer trips after sustained > 60s at > 2x") {
    val p = newPolicy()
    // Capture the clock BEFORE recording: the internal slowSince stamp is >= t0, so a reading at
    // (t0 + 1s) is guaranteed to fall within the 60s window and must NOT trip.
    val t0 = System.nanoTime()
    // Consumer (10 B/s) is 10x slower than producer (100 B/s): well beyond the 2x ratio.
    p.recordThroughput(100L, 10L)
    // Capture the clock AFTER recording: slowSince <= tAfter, so (tAfter + 61s) is guaranteed to
    // exceed the 60s sustained window and must trip.
    val tAfter = System.nanoTime()
    val withinWindow = t0 + TimeUnit.SECONDS.toNanos(1)
    val pastWindow = tAfter + TimeUnit.SECONDS.toNanos(61)

    // Only microseconds of real time have elapsed, so the sustained window is NOT yet met.
    assert(!p.shouldFallback)
    assert(!p.isSlowConsumer(withinWindow))
    assert(!p.shouldFallbackAt(withinWindow))

    // Once the sustained 60s window elapses, the slow-consumer condition trips.
    assert(p.isSlowConsumer(pastWindow))
    assert(p.shouldFallbackAt(pastWindow))
    // The time-injectable reason accessor names the slow-consumer condition at the same instant
    // the decision trips; within the window no condition is active so the reason is empty.
    assert(p.fallbackReasonAt(pastWindow).exists(_.contains("slow consumer")))
    assert(p.fallbackReasonAt(withinWindow).isEmpty)
  }

  test("memory pressure trips above 95%") {
    val p = newPolicy()
    p.updateMemoryUtilization(96)
    assert(p.isMemoryPressure)
    assert(p.shouldFallback)
    assert(p.fallbackReason.exists(_.contains("memory")))

    // Boundary: the threshold is strictly greater-than, so exactly 95% must NOT trip.
    val atThreshold = newPolicy()
    atThreshold.updateMemoryUtilization(95)
    assert(!atThreshold.isMemoryPressure)
    assert(!atThreshold.shouldFallback)

    // Below the threshold must not trip either.
    val below = newPolicy()
    below.updateMemoryUtilization(94)
    assert(!below.isMemoryPressure)
  }

  test("network saturation trips above 90%") {
    val p = newPolicy()
    p.updateNetworkUtilization(91)
    assert(p.isNetworkSaturated)
    assert(p.shouldFallback)
    assert(p.fallbackReason.exists(_.contains("network")))

    // Boundary: exactly 90% must NOT trip (strictly greater-than semantics).
    val atThreshold = newPolicy()
    atThreshold.updateNetworkUtilization(90)
    assert(!atThreshold.isNetworkSaturated)
    assert(!atThreshold.shouldFallback)

    val below = newPolicy()
    below.updateNetworkUtilization(89)
    assert(!below.isNetworkSaturated)
  }

  test("memory pressure threshold is strict at the decimal boundary") {
    // Strictly greater-than semantics: a fractional value just above 95% must trip, while the
    // exact threshold and a fractional value just below must not. The double-typed setter makes
    // these decimal boundaries expressible (95.1 trips, 95.0 and 94.9 do not).
    val above = newPolicy()
    above.updateMemoryUtilization(95.1)
    assert(above.isMemoryPressure)
    assert(above.shouldFallback)
    assert(above.fallbackReason.exists(_.contains("memory")))

    val atThreshold = newPolicy()
    atThreshold.updateMemoryUtilization(95.0)
    assert(!atThreshold.isMemoryPressure)
    assert(!atThreshold.shouldFallback)

    val below = newPolicy()
    below.updateMemoryUtilization(94.9)
    assert(!below.isMemoryPressure)
    assert(!below.shouldFallback)
  }

  test("network saturation threshold is strict at the decimal boundary") {
    // Strictly greater-than semantics at the 90% network threshold, exercised at decimal offsets
    // (90.1 trips, 90.0 and 89.9 do not).
    val above = newPolicy()
    above.updateNetworkUtilization(90.1)
    assert(above.isNetworkSaturated)
    assert(above.shouldFallback)
    assert(above.fallbackReason.exists(_.contains("network")))

    val atThreshold = newPolicy()
    atThreshold.updateNetworkUtilization(90.0)
    assert(!atThreshold.isNetworkSaturated)
    assert(!atThreshold.shouldFallback)

    val below = newPolicy()
    below.updateNetworkUtilization(89.9)
    assert(!below.isNetworkSaturated)
    assert(!below.shouldFallback)
  }

  test("version mismatch trips immediately") {
    val p = newPolicy()
    p.markVersionMismatch()
    assert(p.isVersionMismatch)
    assert(p.shouldFallback)
    assert(p.fallbackReason.exists(_.contains("version")))
  }

  test("shouldFallback is the OR of all conditions and reason names the active condition") {
    // Each real-time condition independently drives the OR-composed decision on a fresh policy,
    // and the reason string names that specific condition.
    val mem = newPolicy()
    assert(!mem.shouldFallback)
    mem.updateMemoryUtilization(99)
    assert(mem.shouldFallback)
    assert(mem.fallbackReason.exists(_.contains("memory")))

    val net = newPolicy()
    net.updateNetworkUtilization(99)
    assert(net.shouldFallback)
    assert(net.fallbackReason.exists(_.contains("network")))

    val ver = newPolicy()
    ver.markVersionMismatch()
    assert(ver.shouldFallback)
    assert(ver.fallbackReason.exists(_.contains("version")))

    // With two conditions active simultaneously the decision still trips and reports a reason.
    val both = newPolicy()
    both.updateMemoryUtilization(99)
    both.markVersionMismatch()
    assert(both.shouldFallback)
    assert(both.fallbackReason.isDefined)
  }

  test("metrics is optional (null-safe)") {
    // The default metrics argument is null; a fallback transition must not dereference it.
    val p = newPolicy()
    p.markVersionMismatch()
    // Must not throw a NullPointerException despite the null metrics holder.
    assert(p.shouldFallback)
    assert(p.fallbackReason.isDefined)
  }

  test("fallback transition increments metrics exactly once") {
    val conf = new StreamingShuffleConfig(new SparkConf(false))
    val metrics = new StreamingShuffleMetrics
    val policy = new StreamingShuffleFallbackPolicy(conf, metrics)
    assert(metrics.backpressureEvents === 0L)

    policy.markVersionMismatch()
    // First evaluation flips streaming -> fallback and counts the transition exactly once.
    assert(policy.shouldFallback)
    // Re-evaluating while the condition persists is side-effect free (no double counting).
    assert(policy.shouldFallback)
    assert(metrics.backpressureEvents === 1L)
  }

  test("slow-consumer window resets when the consumer recovers") {
    val p = newPolicy()
    // Start a slow window, then record a healthy sample so the window timer is cleared.
    p.recordThroughput(100L, 10L)
    val tAfter = System.nanoTime()
    // 100 is not > 100 * 2.0, so this sample is NOT slow and clears the sustained window.
    p.recordThroughput(100L, 100L)

    // Even far past the 60s window, the cleared timer means no slow-consumer fallback.
    val wayPast = tAfter + TimeUnit.SECONDS.toNanos(120)
    assert(!p.isSlowConsumer(wayPast))
    assert(!p.shouldFallbackAt(wayPast))
  }

  test("reset clears every tripped condition") {
    val p = newPolicy()
    // Trip two independent conditions simultaneously so reset must clear all tracked state.
    p.updateMemoryUtilization(96.0)
    p.markVersionMismatch()
    assert(p.shouldFallback)
    assert(p.isMemoryPressure)
    assert(p.isVersionMismatch)

    // reset() is the test-isolation / stress-reuse hook: it returns the policy to its pristine,
    // no-fallback state with each field cleared independently.
    p.reset()
    assert(!p.shouldFallback)
    assert(p.fallbackReason.isEmpty)
    assert(!p.isMemoryPressure)
    assert(!p.isVersionMismatch)
    assert(!p.isNetworkSaturated)
    assert(!p.isSlowConsumer())
  }

  test("fallback recovery transition is taken when the last condition clears (debug on)") {
    // debug=true drives the recovery-log branch so the streaming -> fallback -> streaming
    // round trip is fully exercised, not just the forward transition.
    val p = newPolicy("spark.shuffle.streaming.debug" -> "true")

    // Trip memory pressure and evaluate once so the internal fallbackActive flag flips
    // false -> true (the forward transition).
    p.updateMemoryUtilization(96.0)
    assert(p.shouldFallback)
    assert(p.fallbackReason.exists(_.contains("memory")))

    // Clearing the condition makes the next evaluation flip fallbackActive true -> false, taking
    // the recovery path; the decision must report streaming is eligible again.
    p.updateMemoryUtilization(10.0)
    assert(!p.shouldFallback)
    assert(p.fallbackReason.isEmpty)
  }
}
