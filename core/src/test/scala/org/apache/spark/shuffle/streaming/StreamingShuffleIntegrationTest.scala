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
import org.apache.spark.internal.config.SHUFFLE_MANAGER

/**
 * Additional end-to-end integration coverage for the opt-in streaming shuffle backend,
 * complementary to `StreamingShuffleIntegrationSuite`.
 *
 * Where the companion integration suite exercises the core streaming read/write round-trip, this
 * suite focuses on the dimensions that distinguish the streaming manager from the sort-based
 * fallback path:
 *
 *   - the opt-in activation gate: selecting `spark.shuffle.manager=streaming` while leaving
 *     `spark.shuffle.streaming.enabled=false` must transparently delegate to the inner
 *     `SortShuffleManager` and still produce correct results;
 *   - configuration permutations across the five `spark.shuffle.streaming.*` keys (buffer sizing,
 *     spill threshold, finite and unlimited bandwidth caps, and the debug flag); and
 *   - multi-stage and join shuffles that drive several shuffle boundaries through the streaming
 *     manager within a single job.
 *
 * Each test builds its own [[SparkConf]] and runs a real local [[SparkContext]] (modeled on
 * `org.apache.spark.SortShuffleSuite`), forcing genuine shuffles and asserting the collected
 * results against values computed locally from the same input.
 *
 * The `*Test` filename suffix is intentional and matches the feature's file catalog: this is a
 * normal ScalaTest suite that merely preserves that name rather than the `*Suite` form.
 */
class StreamingShuffleIntegrationTest extends SparkFunSuite with LocalSparkContext {

  /**
   * Builds a base [[SparkConf]] that selects the streaming shuffle manager alias on a two-thread
   * local master. The streaming feature flag is intentionally NOT set here so callers can opt in
   * explicitly (or, for the fallback test, deliberately leave it off).
   *
   * The Web UI is disabled because these integration tests spin up several short-lived local
   * [[SparkContext]]s; disabling it avoids contending for a host-global UI port (which keeps the
   * suite robust when test JVMs run in parallel) and has no bearing on shuffle behavior.
   */
  private def baseStreamingConf(appName: String): SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .setAppName(appName)
      .set(SHUFFLE_MANAGER, "streaming")
      .set("spark.ui.enabled", "false")
  }

  /**
   * Builds a [[SparkConf]] that both selects the streaming manager alias AND turns on the opt-in
   * feature flag, so the streaming path is fully engaged.
   */
  private def enabledConf(appName: String): SparkConf = {
    baseStreamingConf(appName).set("spark.shuffle.streaming.enabled", "true")
  }

  /**
   * Runs a canonical keyed shuffle on the active [[SparkContext]] and asserts the result matches
   * the value computed locally from the same input. This proves a streaming shuffle round-trips
   * correctly under whatever configuration the calling test set.
   */
  private def reduceByKeyRoundTrip(): Unit = {
    val input = (1 to 100).map(i => (i % 5, i))
    val expected = input.groupBy(_._1).map { case (k, vs) => (k, vs.map(_._2).sum) }
    val result = sc.parallelize(input, 4).reduceByKey(_ + _).collect().toMap
    assert(result == expected)
  }

  test(
    "manager=streaming with enabled=false delegates to sort and still produces " +
      "correct results") {
    // The "streaming" alias is selected but the feature flag is OFF, so the streaming
    // manager must delegate to its inner SortShuffleManager. The shuffle therefore runs on
    // the unchanged sort path and the results must still be correct (the opt-in gate).
    val conf = baseStreamingConf("streaming-disabled-delegates-to-sort")
      .set("spark.shuffle.streaming.enabled", "false")
    sc = new SparkContext(conf)
    val input = Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5))
    val expected = input.groupBy(_._1).map { case (k, vs) => (k, vs.map(_._2).sum) }
    val result = sc.parallelize(input, 3).reduceByKey(_ + _).collect().toMap
    assert(result == expected)
  }

  test("custom bufferSizePercent and spillThreshold are accepted and shuffle succeeds") {
    // Exercise a config permutation well inside the documented ranges (bufferSizePercent
    // 1-50, spillThreshold 50-95). The shuffle must still complete with correct results.
    val conf = enabledConf("streaming-custom-buffer-spill")
      .set("spark.shuffle.streaming.bufferSizePercent", "10")
      .set("spark.shuffle.streaming.spillThreshold", "70")
    sc = new SparkContext(conf)
    reduceByKeyRoundTrip()
  }

  test("maxBandwidthMBps set to a finite cap still completes") {
    // A finite per-executor rate cap engages the token-bucket limiter. With tiny data the
    // limiter never blocks, so the shuffle completes without deadlock and stays correct.
    val conf = enabledConf("streaming-bandwidth-capped")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "64")
    sc = new SparkContext(conf)
    reduceByKeyRoundTrip()
  }

  test("maxBandwidthMBps unlimited (default / <= 0) completes") {
    // A non-positive cap selects the unlimited rate-limiting path. Results stay correct.
    val conf = enabledConf("streaming-bandwidth-unlimited")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "0")
    sc = new SparkContext(conf)
    reduceByKeyRoundTrip()
  }

  test("multi-stage shuffle (two consecutive shuffles) round-trips") {
    // Two shuffle boundaries flow through the streaming manager: reduceByKey then
    // groupByKey. Each key 0..9 appears exactly 10 times in 1..100, so every count is 10;
    // swapping and grouping by the count collapses all ten keys under the single value 10.
    val conf = enabledConf("streaming-multi-stage")
    sc = new SparkContext(conf)
    val grouped = sc
      .parallelize(1 to 100, 4)
      .map(i => (i % 10, 1))
      .reduceByKey(_ + _)
      .map { case (key, count) => (count, key) }
      .groupByKey()
      .collect()
    assert(grouped.length == 1)
    val (count, keys) = grouped.head
    assert(count == 10)
    assert(keys.toSet == (0 to 9).toSet)
  }

  test("join across two RDDs works through the streaming manager") {
    // A join introduces a co-group shuffle on both sides. Every key 1..50 is present in both
    // RDDs, so the join yields exactly 50 paired rows of the form (i, (i, i * 2)).
    val conf = enabledConf("streaming-join")
    sc = new SparkContext(conf)
    val a = sc.parallelize((1 to 50).map(i => (i, i)), 2)
    val b = sc.parallelize((1 to 50).map(i => (i, i * 2)), 2)
    val joined = a.join(b).collect()
    assert(joined.length == 50)
    val expected = (1 to 50).map(i => (i, (i, i * 2))).toMap
    assert(joined.toMap == expected)
  }

  test("debug flag enabled does not change results") {
    // The debug flag only adds verbose logging; it must never alter shuffle output. Run the
    // same reduceByKey once with debug=true and once with debug=false (each in its own
    // SparkContext) and assert both match the deterministic expected result and each other.
    val input = (1 to 100).map(i => (i % 8, i))
    val expected = input.groupBy(_._1).map { case (k, vs) => (k, vs.map(_._2).sum) }

    val debugConf = enabledConf("streaming-debug-on")
      .set("spark.shuffle.streaming.debug", "true")
    sc = new SparkContext(debugConf)
    val withDebug = sc.parallelize(input, 4).reduceByKey(_ + _).collect().toMap
    resetSparkContext()

    val plainConf = enabledConf("streaming-debug-off")
      .set("spark.shuffle.streaming.debug", "false")
    sc = new SparkContext(plainConf)
    val withoutDebug = sc.parallelize(input, 4).reduceByKey(_ + _).collect().toMap

    assert(withDebug == expected)
    assert(withoutDebug == expected)
    assert(withDebug == withoutDebug)
  }
}
