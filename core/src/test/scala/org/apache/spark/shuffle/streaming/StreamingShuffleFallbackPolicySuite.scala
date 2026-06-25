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
 * Unit tests for [[StreamingShuffleFallbackPolicy]], the decision-only safety envelope that
 * encapsulates the four automatic conditions under which the streaming shuffle data path reverts
 * ("falls back") to the built-in sort-based shuffle.
 *
 * The policy performs no I/O and switches no managers, so these tests need neither a
 * `SparkContext` nor any live Spark services: each `shouldFallbackFor*` predicate is exercised as
 * a pure function of its arguments, and [[StreamingShuffleFallbackPolicy.evaluate]] is exercised
 * for both its priority ordering and its `None` (no-fallback) result. The numeric thresholds (a
 * sustained 2x slowness over 60s and a 90% network-saturation ceiling) are contractual and are
 * pinned by the first test so an accidental change to the production constants fails fast.
 */
class StreamingShuffleFallbackPolicySuite extends SparkFunSuite {

  import StreamingShuffleFallbackPolicy._

  /**
   * Builds a policy backed by a default configuration and a fresh metrics holder.
   *
   * A live [[StreamingShuffleMetrics]] instance is required because
   * [[StreamingShuffleFallbackPolicy.evaluate]] reads the buffer-utilization gauge when it logs a
   * decided fallback; the default [[StreamingShuffleConfig]] (built from `new SparkConf(false)`)
   * resolves every tuning knob to its registered default, which is sufficient for the
   * decision-only predicates under test.
   */
  private def newPolicy(
      conf: SparkConf = new SparkConf(false)): StreamingShuffleFallbackPolicy = {
    val config = new StreamingShuffleConfig(conf)
    val metrics = new StreamingShuffleMetrics
    new StreamingShuffleFallbackPolicy(config, metrics)
  }

  test("fallback thresholds match the streaming shuffle specification") {
    // These three values are the contractual safety envelope and must never drift.
    assert(CONSUMER_SLOWNESS_FACTOR === 2.0)
    assert(CONSUMER_SLOWNESS_DURATION_MS === 60000L)
    assert(NETWORK_SATURATION_THRESHOLD === 0.90)
  }

  test("consumer-lag fallback triggers when consumer >= 2x slower AND sustained > 60s") {
    val policy = newPolicy()
    // TRUE: 50 * 2 == 100 <= 100 (consumer at least 2x slower) AND 60001 > 60000 (sustained).
    assert(
      policy.shouldFallbackForConsumerLag(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 60001L))
    // FALSE: consumer is not slow enough (60 * 2 == 120 > 100).
    assert(
      !policy.shouldFallbackForConsumerLag(
        producerRate = 100.0,
        consumerRate = 60.0,
        sustainedMs = 60001L))
    // FALSE: slowness has not been sustained strictly beyond 60000 ms (equal is not enough).
    assert(
      !policy.shouldFallbackForConsumerLag(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 60000L))
    // FALSE: well under the sustained-duration gate.
    assert(
      !policy.shouldFallbackForConsumerLag(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 30000L))
  }

  test("consumer-lag duration gate is strictly greater than 60000 ms") {
    val policy = newPolicy()
    // consumerRate is exactly 2x slower, so the rate side is satisfied via `<=` and only the
    // sustained-duration gate decides the outcome at the boundary.
    assert(
      !policy.shouldFallbackForConsumerLag(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 59999L))
    assert(
      policy.shouldFallbackForConsumerLag(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 60001L))
  }

  test("consumer-lag fallback requires a positive producer rate") {
    val policy = newPolicy()
    // A non-positive producer rate means there is no production to outpace, so the condition is
    // not met regardless of the consumer rate or how long the slowness has been observed.
    assert(
      !policy.shouldFallbackForConsumerLag(
        producerRate = 0.0,
        consumerRate = 0.0,
        sustainedMs = 120000L))
    assert(
      !policy.shouldFallbackForConsumerLag(
        producerRate = -1.0,
        consumerRate = -1.0,
        sustainedMs = 120000L))
  }

  test("memory-pressure fallback triggers when allocation is impossible") {
    val policy = newPolicy()
    assert(policy.shouldFallbackForMemoryPressure(canAllocate = false))
    assert(!policy.shouldFallbackForMemoryPressure(canAllocate = true))
  }

  test("network-saturation fallback triggers strictly above 0.90") {
    val policy = newPolicy()
    assert(policy.shouldFallbackForNetworkSaturation(0.91))
    assert(policy.shouldFallbackForNetworkSaturation(0.95))
    // Strictly greater: exactly at the 0.90 threshold does NOT trigger.
    assert(!policy.shouldFallbackForNetworkSaturation(0.90))
    assert(!policy.shouldFallbackForNetworkSaturation(0.50))
  }

  test("version-mismatch fallback triggers when versions differ") {
    val policy = newPolicy()
    assert(policy.shouldFallbackForVersionMismatch("1", "2"))
    assert(!policy.shouldFallbackForVersionMismatch("1", "1"))
  }

  test("evaluate returns the highest-priority triggered reason") {
    val policy = newPolicy()

    // All four conditions hold simultaneously => ConsumerTooSlow wins (highest priority).
    assert(
      policy.evaluate(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 60001L,
        canAllocate = false,
        networkUtilizationFraction = 0.95,
        producerVersion = "1",
        consumerVersion = "2") === Some(ConsumerTooSlow))

    // Memory pressure + network saturation + version mismatch, but the consumer keeps up,
    // so MemoryPressure wins over the two lower-priority conditions.
    assert(
      policy.evaluate(
        producerRate = 100.0,
        consumerRate = 100.0,
        sustainedMs = 60001L,
        canAllocate = false,
        networkUtilizationFraction = 0.95,
        producerVersion = "1",
        consumerVersion = "2") === Some(MemoryPressure))

    // Network saturation + version mismatch only => NetworkSaturation wins over VersionMismatch.
    assert(
      policy.evaluate(
        producerRate = 100.0,
        consumerRate = 100.0,
        sustainedMs = 0L,
        canAllocate = true,
        networkUtilizationFraction = 0.95,
        producerVersion = "1",
        consumerVersion = "2") === Some(NetworkSaturation))
  }

  test("evaluate returns the lowest-priority reason when only it is triggered") {
    val policy = newPolicy()
    // Only the version mismatch holds; every higher-priority condition is clear.
    assert(
      policy.evaluate(
        producerRate = 100.0,
        consumerRate = 100.0,
        sustainedMs = 0L,
        canAllocate = true,
        networkUtilizationFraction = 0.50,
        producerVersion = "1",
        consumerVersion = "2") === Some(VersionMismatch))
  }

  test("evaluate returns None and shouldFallback is false under nominal conditions") {
    val policy = newPolicy()
    // Every individual predicate is false under healthy, nominal operation.
    assert(!policy.shouldFallbackForConsumerLag(100.0, 100.0, 0L))
    assert(!policy.shouldFallbackForMemoryPressure(canAllocate = true))
    assert(!policy.shouldFallbackForNetworkSaturation(0.50))
    assert(!policy.shouldFallbackForVersionMismatch("1", "1"))
    assert(
      policy
        .evaluate(
          producerRate = 100.0,
          consumerRate = 100.0,
          sustainedMs = 0L,
          canAllocate = true,
          networkUtilizationFraction = 0.50,
          producerVersion = "1",
          consumerVersion = "1")
        .isEmpty)
    assert(
      !policy.shouldFallback(
        producerRate = 100.0,
        consumerRate = 100.0,
        sustainedMs = 0L,
        canAllocate = true,
        networkUtilizationFraction = 0.50,
        producerVersion = "1",
        consumerVersion = "1"))
  }

  test("shouldFallback agrees with evaluate when a condition is triggered") {
    val policy = newPolicy()
    // Sustained consumer lag alone is enough for shouldFallback to report true.
    assert(
      policy.shouldFallback(
        producerRate = 100.0,
        consumerRate = 50.0,
        sustainedMs = 60001L,
        canAllocate = true,
        networkUtilizationFraction = 0.50,
        producerVersion = "1",
        consumerVersion = "1"))
  }
}
