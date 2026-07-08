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

import org.apache.spark.{SparkConf, SparkFunSuite}

/**
 * Unit tests for [[StreamingShuffleFallbackPolicy]], the four-condition decision engine that
 * backs the streaming shuffle's "zero regression / automatic fallback" guarantee.
 *
 * Each fallback guard is exercised as a pure predicate so every threshold is verified in
 * isolation, with explicit coverage of the STRICT boundaries mandated by the specification:
 *
 *  - `isSlowConsumer`: fires only when the consumer is at least 2x slower AND the condition has
 *    been sustained for strictly more than 60000 ms (60000 ms itself must NOT trigger).
 *  - `isMemoryPressure`: fires only above 95% utilization (95 false, 96 true) - deliberately
 *    distinct from the 80% spill threshold, so 80 must NOT trigger.
 *  - `isNetworkSaturated`: fires only above 90% link utilization (90 false, 91 true).
 *  - `isVersionMismatch`: fires whenever the local and remote protocol versions differ.
 *
 * A final test drives the aggregating `shouldFallback` with an all-healthy snapshot (no guard
 * fires) and one snapshot per guard (each guard fires), covering every OR-branch. The predicates
 * are side-effect free, so no `SparkContext` or shuffle machinery is required; the policy is
 * built with a real [[StreamingShuffleConfig]] and [[StreamingShuffleMetrics]].
 */
class StreamingShuffleFallbackPolicySuite extends SparkFunSuite {

  /**
   * Builds a policy backed by a real (defaults-only) [[StreamingShuffleConfig]] and a fresh
   * [[StreamingShuffleMetrics]]. `SparkConf(false)` skips loading external system properties so the
   * suite is fully isolated. Mocks are unnecessary because the predicates under test are pure.
   */
  private def newPolicy(): StreamingShuffleFallbackPolicy = {
    val streamingConf = new StreamingShuffleConfig(new SparkConf(false))
    new StreamingShuffleFallbackPolicy(streamingConf, new StreamingShuffleMetrics())
  }

  /**
   * A baseline [[FallbackStats]] snapshot under which none of the four guards fires: the consumer
   * keeps pace with the producer, memory and network utilization sit well below their thresholds,
   * and the local and remote protocol versions match. Individual `shouldFallback` cases trip a
   * single field via `copy` so exactly one guard is responsible for the resulting fallback.
   */
  private def healthyStats(): FallbackStats = FallbackStats(
    consumerRateBytesPerSec = 20.0,
    producerRateBytesPerSec = 20.0,
    sustainedSlowMillis = 0L,
    memoryUtilizationPercent = 50,
    networkUtilizationPercent = 50,
    localProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION,
    remoteProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION)

  test("isSlowConsumer requires 2x slower AND >60s strictly") {
    val policy = newPolicy()
    // 10 * 2 == 20 <= 20 (2x slower) and 60001 > 60000 (sustained past the strict boundary).
    assert(policy.isSlowConsumer(10.0, 20.0, 60001L))
    // The duration guard is strict: exactly 60000 ms does NOT trigger fallback.
    assert(!policy.isSlowConsumer(10.0, 20.0, 60000L))
    // Not sustained long enough (below the 60000 ms window).
    assert(!policy.isSlowConsumer(10.0, 20.0, 59000L))
    // Rate condition fails: 10 * 2 == 20 is greater than 19, so the consumer is not 2x slower.
    assert(!policy.isSlowConsumer(10.0, 19.0, 120000L))
    // 9 * 2 == 18 <= 20 and sustained well beyond 60000 ms.
    assert(policy.isSlowConsumer(9.0, 20.0, 90000L))
  }

  test("isMemoryPressure fires above 95") {
    val policy = newPolicy()
    assert(policy.isMemoryPressure(96))
    // Strict boundary: exactly 95 does NOT trigger memory-pressure fallback.
    assert(!policy.isMemoryPressure(95))
    assert(policy.isMemoryPressure(100))
    // 80 is the spill threshold, deliberately distinct from the 95 fallback threshold.
    assert(!policy.isMemoryPressure(80))
  }

  test("isNetworkSaturated fires above 90") {
    val policy = newPolicy()
    assert(policy.isNetworkSaturated(91))
    // Strict boundary: exactly 90 does NOT trigger network-saturation fallback.
    assert(!policy.isNetworkSaturated(90))
    assert(policy.isNetworkSaturated(100))
  }

  test("isVersionMismatch on inequality") {
    val policy = newPolicy()
    assert(!policy.isVersionMismatch(1, 1))
    assert(policy.isVersionMismatch(1, 2))
    // Equal protocol versions (the canonical local version) never mismatch.
    assert(!policy.isVersionMismatch(
      StreamingShuffleFallbackPolicy.PROTOCOL_VERSION,
      StreamingShuffleFallbackPolicy.PROTOCOL_VERSION))
  }

  test("PROTOCOL_VERSION is 1") {
    assert(StreamingShuffleFallbackPolicy.PROTOCOL_VERSION == 1)
  }

  test("shouldFallback true when any condition fires; false when none") {
    val policy = newPolicy()
    // (a) All healthy: none of the four guards fires.
    assert(!policy.shouldFallback(healthyStats()))
    // (b) Slow consumer: 10 * 2 == 20 <= 20 sustained strictly beyond 60000 ms.
    assert(policy.shouldFallback(healthyStats().copy(
      consumerRateBytesPerSec = 10.0,
      producerRateBytesPerSec = 20.0,
      sustainedSlowMillis = 60001L)))
    // (c) Memory pressure: utilization above the 95% fallback threshold.
    assert(policy.shouldFallback(healthyStats().copy(memoryUtilizationPercent = 99)))
    // (d) Network saturation: link utilization above the 90% threshold.
    assert(policy.shouldFallback(healthyStats().copy(networkUtilizationPercent = 95)))
    // (e) Version mismatch: the remote protocol version differs from the local one.
    assert(policy.shouldFallback(healthyStats().copy(
      remoteProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION + 1)))
  }
}
