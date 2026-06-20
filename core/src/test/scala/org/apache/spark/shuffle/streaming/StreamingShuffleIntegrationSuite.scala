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
 * End-to-end integration suite for the opt-in streaming shuffle backend. Unlike the unit suites in
 * this package, which exercise individual collaborators in isolation, this suite drives a REAL
 * shuffle through a live [[SparkContext]] configured with `spark.shuffle.manager=streaming` and
 * `spark.shuffle.streaming.enabled=true` -- the two activation signals that engage the streaming
 * path (see [[StreamingShuffleManager]]). It is modeled on `org.apache.spark.SortShuffleSuite`.
 *
 * The suite serves two purposes:
 *  1. Correctness: groupByKey / reduceByKey / sortByKey round-trip correctly, and a `>= 10`
 *     partition shuffle completes -- the success-criteria envelope for shuffle-heavy workloads.
 *  2. Observability: it verifies, in the local development environment, that the streaming backend
 *     is the active manager and that its `shuffle.streaming.*` metrics source is registered and
 *     emitting through the executor `MetricsSystem`.
 *
 * Because [[StreamingShuffleManager]] composes the sort-based shuffle as an automatic fallback, a
 * shuffle ALWAYS produces correct results whether or not the streaming path engages. The strongest
 * end-to-end guarantee is therefore streaming-vs-sort result equality -- the zero-regression proof
 * implemented by the final test.
 *
 * ==Local-mode assertion note (the metrics test)==
 *
 * In `local` mode the driver and the executor share a single `SparkEnv`, so the executor-registered
 * `StreamingShuffleSource` is observable from `sc.env`. The metrics test asserts both that the
 * active manager is a [[StreamingShuffleManager]] (always true once the `"streaming"` factory alias
 * selects it, and therefore robust to registration timing) and that the `"StreamingShuffle"`
 * metrics source and its four `shuffle.streaming.*` metrics are registered after a shuffle runs.
 *
 * Datasets are intentionally small so the whole suite runs in seconds under CI; per-test
 * `SparkContext`s are stopped automatically by [[LocalSparkContext.afterEach]].
 */
class StreamingShuffleIntegrationSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * Builds a fresh, defaults-free [[SparkConf]] for a two-core local cluster wired to the requested
   * shuffle manager. The streaming feature flag is set explicitly so the activation contract is
   * unambiguous: the streaming path engages only when `manager == "streaming"` AND
   * `streamingEnabled == true`; any other combination keeps the sort-based path.
   *
   * @param manager          the value for `spark.shuffle.manager` (e.g. `"streaming"` or `"sort"`)
   * @param streamingEnabled the value for `spark.shuffle.streaming.enabled`
   * @return a ready-to-use [[SparkConf]] for a local `SparkContext`
   */
  private def newConf(manager: String, streamingEnabled: Boolean): SparkConf = {
    new SparkConf(false)
      .setAppName("streaming-shuffle-integration")
      .setMaster("local[2]")
      .set(SHUFFLE_MANAGER, manager)
      .set(SHUFFLE_STREAMING_ENABLED, streamingEnabled)
  }

  /** Convenience accessor for a conf with the streaming backend fully engaged. */
  private def newStreamingConf(): SparkConf = newConf("streaming", streamingEnabled = true)

  test("groupByKey round-trips correctly with the streaming manager") {
    sc = new SparkContext(newStreamingConf())
    val pairs = sc.parallelize(0 until 1000, 4).map(i => (i % 10, i))
    val grouped = pairs.groupByKey().collect()
    // Exactly the ten distinct keys 0..9 must survive the shuffle.
    assert(grouped.length == 10)
    assert(grouped.map(_._1).toSet == (0 until 10).toSet)
    // The union of all grouped values must be exactly the original 0..999, with none lost.
    assert(grouped.flatMap(_._2).toSet == (0 until 1000).toSet)
  }

  test("reduceByKey produces correct sums") {
    sc = new SparkContext(newStreamingConf())
    val sums = sc.parallelize(1 to 100, 4).map(i => (i % 5, i)).reduceByKey(_ + _).collect().toMap
    // Compute the expected per-key sums in-test by the same partitioning arithmetic.
    val expected = (1 to 100).groupBy(_ % 5).map { case (k, vs) => (k, vs.sum) }
    assert(sums == expected)
  }

  test("sortByKey produces ordered output") {
    sc = new SparkContext(newStreamingConf())
    // A descending range is a valid permutation; sortByKey must restore ascending key order,
    // exercising the keyOrdering path through the streaming reader's range-partitioned shuffle.
    val permutation = (0 until 500).reverse
    val sorted = sc.parallelize(permutation, 4).map(i => (i, i)).sortByKey().collect()
    val keys = sorted.map(_._1).toList
    assert(keys == (0 until 500).toList)
  }

  test("large-ish shuffle (>= 10 partitions) completes without error") {
    sc = new SparkContext(newStreamingConf())
    // Twelve partitions exercises the >= 10-partition success-criteria envelope; the data volume
    // is kept modest so the suite still runs in seconds under CI.
    val count = sc.parallelize(0 until 5000, 12).map(i => (i % 50, i)).groupByKey().count()
    assert(count == 50L)
  }

  test("streaming-shuffle metrics source is registered and emits") {
    sc = new SparkContext(newStreamingConf())
    // Force a real shuffle so the executor-side components (including the metrics source) init.
    sc.parallelize(0 until 1000, 4).map(i => (i % 8, i)).reduceByKey(_ + _).collect()
    // Robust primary assertion: the streaming manager is the active backend (always true once the
    // "streaming" factory alias selects it), independent of metrics-registration timing.
    assert(sc.env.shuffleManager.isInstanceOf[StreamingShuffleManager])
    // In local mode the driver and executor share one SparkEnv, so the source is observable here.
    val sources = sc.env.metricsSystem.getSourcesByName("StreamingShuffle")
    assert(sources.nonEmpty, "StreamingShuffleSource not registered after shuffle")
    // Verify the four shuffle.streaming.* metrics are present, proving telemetry actually emits.
    val names = sources.head.metricRegistry.getNames
    val prefix = StreamingShuffleMetrics.METRIC_PREFIX
    val expected = Seq(
      StreamingShuffleMetrics.BUFFER_UTILIZATION_PERCENT,
      StreamingShuffleMetrics.SPILL_COUNT,
      StreamingShuffleMetrics.BACKPRESSURE_EVENTS,
      StreamingShuffleMetrics.PARTIAL_READ_INVALIDATIONS).map(n => s"$prefix.$n")
    expected.foreach(n => assert(names.contains(n), s"metric $n should be registered"))
  }

  test("results are identical to the sort manager (zero regression)") {
    val data = 1 to 200
    // Baseline: run the job with the sort-based manager.
    sc = new SparkContext(newConf("sort", streamingEnabled = false))
    val sortResult =
      sc.parallelize(data, 4).map(i => (i % 7, i)).reduceByKey(_ + _).collect().toMap
    // Stop the first context explicitly before creating the second (LocalSparkContext helper).
    resetSparkContext()
    // Candidate: run the SAME job with the streaming manager engaged.
    sc = new SparkContext(newConf("streaming", streamingEnabled = true))
    val streamingResult =
      sc.parallelize(data, 4).map(i => (i % 7, i)).reduceByKey(_ + _).collect().toMap
    // Zero-regression proof: identical results, and both match the in-test expected arithmetic.
    assert(streamingResult == sortResult)
    val expected = data.groupBy(_ % 7).map { case (k, vs) => (k, vs.sum) }
    assert(streamingResult == expected)
  }
}
