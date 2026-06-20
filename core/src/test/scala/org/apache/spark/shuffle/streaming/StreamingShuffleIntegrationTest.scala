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
 * End-to-end integration tests for the opt-in streaming shuffle backend that COMPLEMENT
 * `StreamingShuffleIntegrationSuite` rather than duplicate it. Where the companion suite exercises
 * the streaming happy path, this suite concentrates on three distinct concerns:
 *
 *   1. the opt-in activation gate -- selecting `spark.shuffle.manager=streaming` while leaving
 *      `spark.shuffle.streaming.enabled=false` must transparently delegate to the sort-based
 *      shuffle and still produce correct results (the zero-regression guarantee);
 *   2. configuration permutations across the five `spark.shuffle.streaming.*` keys
 *      (`bufferSizePercent`, `spillThreshold`, a finite and an unlimited `maxBandwidthMBps`, and
 *      `debug`), each of which must complete a real shuffle with byte-for-byte correct output; and
 *   3. multi-boundary topologies -- two consecutive shuffles and a two-RDD join routed entirely
 *      through `StreamingShuffleManager`.
 *
 * The suite follows the integration pattern of `org.apache.spark.SortShuffleSuite`: it drives a
 * real [[SparkContext]] in `local[2]` mode, forces genuine shuffle boundaries, and asserts on the
 * collected results. [[LocalSparkContext]] stops the `SparkContext` after every test, so each test
 * starts from a clean environment. All datasets are intentionally small (tens to hundreds of
 * records) to keep the suite CI-friendly.
 *
 * The filename and class name intentionally keep the `Test` suffix (rather than the more common
 * `Suite` suffix) because the streaming-shuffle test catalog enumerates this artifact as
 * `StreamingShuffleIntegrationTest`; it is nonetheless a standard ScalaTest [[SparkFunSuite]].
 */
class StreamingShuffleIntegrationTest extends SparkFunSuite with LocalSparkContext {

  /**
   * Builds a [[SparkConf]] that selects the streaming shuffle backend and arms its feature flag.
   * The streaming path engages only when BOTH `spark.shuffle.manager=streaming` AND
   * `spark.shuffle.streaming.enabled=true` hold, so both signals are set here; individual tests
   * layer additional `spark.shuffle.streaming.*` overrides (or flip `enabled` back off) on top of
   * the returned conf via further `set` calls.
   *
   * Shuffle compression is disabled because the streaming writer frames raw serialized bytes into
   * CRC32C-checked envelopes, whereas the reader mirrors `BlockStoreShuffleReader` and wraps each
   * payload through `SerializerManager`. Turning compression off keeps the two sides symmetric and
   * matches the configuration the committed `StreamingShuffleReaderSuite` validates against, so the
   * end-to-end streaming path round-trips records exactly.
   *
   * @param appName the application name, also used to disambiguate the per-test `SparkContext`
   * @return a streaming-enabled [[SparkConf]] bound to a two-slot local master
   */
  private def streamingConf(appName: String): SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .setAppName(appName)
      .set(SHUFFLE_MANAGER, "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.shuffle.spill.compress", "false")
  }

  /** The canonical small key/count expectation: keys `0 until n`, each observed `perKey` times. */
  private def countsByResidue(n: Int, perKey: Int): Map[Int, Int] =
    (0 until n).map(k => k -> perKey).toMap

  test("manager=streaming with enabled=false delegates to sort and still produces correct " +
    "results") {
    // Opt-in gate: the alias selects StreamingShuffleManager, but with the feature flag off the
    // manager delegates every operation to the inner SortShuffleManager, so results must be exact.
    val conf = streamingConf("streaming-disabled-delegates-to-sort")
      .set("spark.shuffle.streaming.enabled", "false")
    sc = new SparkContext(conf)
    val counts = sc.parallelize(1 to 100, 4).map(i => (i % 10, 1))
      .reduceByKey(_ + _).collect().toMap
    assert(counts === countsByResidue(10, 10))
  }

  test("custom bufferSizePercent and spillThreshold are accepted and shuffle succeeds") {
    // Configuration permutation inside the documented ranges (bufferSizePercent 1..50,
    // spillThreshold 50..95): the shuffle must still round-trip correctly.
    val conf = streamingConf("streaming-custom-buffer-and-spill")
      .set("spark.shuffle.streaming.bufferSizePercent", "10")
      .set("spark.shuffle.streaming.spillThreshold", "70")
    sc = new SparkContext(conf)
    val counts = sc.parallelize(1 to 100, 4).map(i => (i % 10, 1))
      .reduceByKey(_ + _).collect().toMap
    assert(counts === countsByResidue(10, 10))
  }

  test("maxBandwidthMBps set to a finite cap still completes") {
    // A finite per-executor rate cap engages the token-bucket limiter; the shuffle must complete
    // without deadlock and yield correct results.
    val conf = streamingConf("streaming-finite-bandwidth-cap")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "64")
    sc = new SparkContext(conf)
    val counts = sc.parallelize(1 to 50, 2).map(i => (i % 5, 1))
      .reduceByKey(_ + _).collect().toMap
    assert(counts === countsByResidue(5, 10))
  }

  test("maxBandwidthMBps unlimited (<= 0) completes") {
    // A non-positive cap (here 0; the production default is -1) takes the unlimited rate path. The
    // shuffle must complete and produce identical results to the rate-limited case above.
    val conf = streamingConf("streaming-unlimited-bandwidth")
      .set("spark.shuffle.streaming.maxBandwidthMBps", "0")
    sc = new SparkContext(conf)
    val counts = sc.parallelize(1 to 50, 2).map(i => (i % 5, 1))
      .reduceByKey(_ + _).collect().toMap
    assert(counts === countsByResidue(5, 10))
  }

  test("multi-stage shuffle (two consecutive shuffles) round-trips") {
    // Two shuffle boundaries through the streaming manager: a reduceByKey followed by a groupByKey
    // over the swapped (count, key) pairs. Every distinct key collapses to the same count (10), so
    // the second shuffle yields a single group whose values are exactly the original keys 0..9.
    val conf = streamingConf("streaming-multi-stage")
    sc = new SparkContext(conf)
    val counts = sc.parallelize(1 to 100, 4).map(i => (i % 10, 1)).reduceByKey(_ + _)
    val grouped = counts.map { case (key, count) => (count, key) }.groupByKey().collect()
    assert(grouped.length === 1)
    assert(grouped.head._1 === 10)
    assert(grouped.head._2.toSet === (0 until 10).toSet)
  }

  test("join across two RDDs works through the streaming manager") {
    // A co-partitioned join drives a shuffle on both sides; every key 1..50 must pair its identity
    // value with its doubled value.
    val conf = streamingConf("streaming-join")
    sc = new SparkContext(conf)
    val a = sc.parallelize((1 to 50).map(i => (i, i)))
    val b = sc.parallelize((1 to 50).map(i => (i, i * 2)))
    val joined = a.join(b).collect()
    assert(joined.length === 50)
    assert(joined.toMap === (1 to 50).map(i => i -> ((i, i * 2))).toMap)
  }

  test("debug flag enabled does not change results") {
    // The debug flag only adds verbose logging; it must never alter shuffle output. The result is
    // therefore identical to the (debug=false) correct expectation.
    val conf = streamingConf("streaming-debug-enabled")
      .set("spark.shuffle.streaming.debug", "true")
    sc = new SparkContext(conf)
    val counts = sc.parallelize(1 to 100, 4).map(i => (i % 10, 1))
      .reduceByKey(_ + _).collect().toMap
    assert(counts === countsByResidue(10, 10))
  }
}
