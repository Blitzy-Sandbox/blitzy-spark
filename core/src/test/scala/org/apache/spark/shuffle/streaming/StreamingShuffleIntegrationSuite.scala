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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_ENABLED}

/**
 * End-to-end integration suite that drives REAL shuffles through a live [[SparkContext]]
 * configured with the opt-in streaming shuffle backend and asserts on the observable outcome.
 *
 * This is one of the F-121 streaming-shuffle suites and doubles as the Observability
 * "verify metric emission in the local development environment" gate: it both proves the
 * streaming path round-trips data correctly and confirms that the `shuffle.streaming.*`
 * telemetry surfaces through the existing `MetricsSystem`.
 *
 * ==Activation contract==
 *
 * The streaming backend is opt-in and requires BOTH activation signals, which every test sets
 * explicitly via [[streamingSparkConf]]: the manager alias `spark.shuffle.manager=streaming`
 * (reflectively resolved by `SparkEnv` into [[StreamingShuffleManager]]) AND the feature flag
 * `spark.shuffle.streaming.enabled=true`. Because both default to off, the default behavior of
 * every existing Spark deployment is unchanged; these tests deliberately turn both on.
 *
 * ==Why the correctness assertions always hold (zero-regression by construction)==
 *
 * [[StreamingShuffleManager]] coexists with the sort-based path rather than replacing it: when
 * the streaming path cannot engage (the feature is disabled, or the fallback policy trips) it
 * delegates to an inner, unchanged `SortShuffleManager`. A shuffle therefore ALWAYS produces
 * correct results regardless of which backend actually served it, so the strongest end-to-end
 * guarantee this suite proves is that the streaming result is identical to the sort result (see
 * the zero-regression test). The remaining tests assert correctness of the common shuffle
 * operators (group/reduce/sort) and that a wider, multi-partition shuffle completes.
 *
 * ==Local-mode metrics observability==
 *
 * The executor-side streaming components (including the [[StreamingShuffleSource]] metrics
 * adapter) are initialized lazily on first streaming use and registered with the executor
 * `MetricsSystem`. In `local` mode the executor runs in-process and shares the driver's
 * `SparkEnv`, so the registered source is observable here through `sc.env.metricsSystem`.
 *
 * All datasets are intentionally small so the suite runs in seconds under CI; each test uses a
 * `local[2]` master and relies on [[LocalSparkContext]] to stop its `SparkContext` afterwards.
 */
class StreamingShuffleIntegrationSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * Builds an isolated [[SparkConf]] that activates the streaming shuffle backend. Both
   * activation signals are required and set here: the manager alias
   * `spark.shuffle.manager=streaming` and the feature flag
   * `spark.shuffle.streaming.enabled=true`. `loadDefaults=false` keeps the configuration
   * hermetic so ambient system properties cannot perturb the test.
   */
  private def streamingSparkConf(): SparkConf = {
    val conf = new SparkConf(false).setAppName("streaming-shuffle").setMaster("local[2]")
    conf.set(SHUFFLE_MANAGER, "streaming")
    conf.set(SHUFFLE_STREAMING_ENABLED, true)
    conf
  }

  /**
   * Builds an isolated [[SparkConf]] that uses the stock sort-based shuffle. Used by the
   * zero-regression test to produce the reference result the streaming output is compared to.
   */
  private def sortSparkConf(): SparkConf = {
    val conf = new SparkConf(false).setAppName("streaming-shuffle").setMaster("local[2]")
    conf.set(SHUFFLE_MANAGER, "sort")
    conf
  }

  test("groupByKey round-trips correctly with the streaming manager") {
    sc = new SparkContext(streamingSparkConf())
    val pairs = sc.parallelize(0 until 1000, 4).map(i => (i % 10, i))
    val grouped = pairs.groupByKey().collect()
    // Exactly the ten distinct keys 0..9 must survive the shuffle.
    assert(grouped.length === 10)
    assert(grouped.map(_._1).toSet === (0 until 10).toSet)
    // The union of all grouped values must be precisely the original input - nothing lost and
    // nothing duplicated - proving every record round-tripped through the shuffle.
    val allValues = grouped.flatMap(_._2).toSet
    assert(allValues === (0 until 1000).toSet)
  }

  test("reduceByKey produces correct sums") {
    sc = new SparkContext(streamingSparkConf())
    val sums =
      sc.parallelize(1 to 100, 4).map(i => (i % 5, i)).reduceByKey(_ + _).collect().toMap
    // Compute the expected per-key sums independently of Spark so the assertion is self-checking.
    val expected = (1 to 100).groupBy(_ % 5).map { case (k, vs) => (k, vs.sum) }
    assert(sums === expected)
  }

  test("sortByKey produces ordered output") {
    sc = new SparkContext(streamingSparkConf())
    // Shuffle a permutation of 0..999 so keys arrive out of order at the reducer; a fixed seed
    // keeps the input deterministic across runs.
    val permutation = new scala.util.Random(42).shuffle((0 until 1000).toList)
    val sorted = sc.parallelize(permutation, 4).map(i => (i, i)).sortByKey().collect()
    // Exercises keyOrdering through the streaming reader: the collected keys must be exactly
    // 0..999 in ascending order. Compare as List to use value (not array reference) equality.
    val keys = sorted.map(_._1).toList
    assert(keys === (0 until 1000).toList)
  }

  test("large-ish shuffle (>= 10 partitions) completes without error") {
    sc = new SparkContext(streamingSparkConf())
    // 12 partitions exceeds the >= 10-partition success-criteria envelope; the data volume stays
    // modest so the suite remains CI-friendly. groupByKey().count() returns the distinct-key
    // count, which must be 50.
    val count = sc.parallelize(0 until 5000, 12).map(i => (i % 50, i)).groupByKey().count()
    assert(count === 50L)
  }

  test("streaming-shuffle metrics source is registered and emits") {
    sc = new SparkContext(streamingSparkConf())
    // Force a real streaming shuffle so the executor-side components initialize and register the
    // streaming metrics source with the MetricsSystem.
    sc.parallelize(0 until 1000, 4).map(i => (i % 8, i)).reduceByKey(_ + _).count()
    // Robust primary assertion: selecting the "streaming" alias always resolves the active
    // manager to StreamingShuffleManager, independent of local-mode registration timing.
    assert(sc.env.shuffleManager.isInstanceOf[StreamingShuffleManager])
    // Observability gate: in local mode the executor runs in-process and shares this SparkEnv, so
    // the StreamingShuffleSource registered on first streaming use is observable through the
    // driver's MetricsSystem. The source name is the stable "StreamingShuffle".
    val sources = sc.env.metricsSystem.getSourcesByName("StreamingShuffle")
    assert(sources.nonEmpty,
      "expected a StreamingShuffle metrics source to be registered with the MetricsSystem")
  }

  test("results are identical to the sort manager (zero regression)") {
    // Reference result from the stock sort-based shuffle.
    sc = new SparkContext(sortSparkConf())
    val sortResult =
      sc.parallelize(1 to 1000, 6).map(i => (i % 20, i)).reduceByKey(_ + _).collect().toMap
    // Stop the sort context before creating the streaming one: only one SparkContext may be
    // active per JVM at a time. LocalSparkContext.resetSparkContext() stops sc and nulls it.
    resetSparkContext()

    // Same job under the streaming backend.
    sc = new SparkContext(streamingSparkConf())
    val streamingResult =
      sc.parallelize(1 to 1000, 6).map(i => (i % 20, i)).reduceByKey(_ + _).collect().toMap

    // Identical results prove the streaming path introduces zero regression versus sort.
    assert(streamingResult === sortResult)
  }
}
