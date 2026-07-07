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

import org.apache.spark._

/**
 * End-to-end integration tests for the opt-in streaming shuffle backend.
 *
 * Unlike the component unit suites (which exercise a single collaborator in isolation), this suite
 * drives the FULL producer -> consumer shuffle path through a real, in-process [[SparkContext]]
 * with `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`. Everything is
 * exercised strictly through the public Spark API (configuration plus RDD transformations); no
 * production streaming class is stubbed, redefined, or reached into. The only streaming type this
 * suite names directly is [[StreamingShuffleManager]] -- and only to assert that the short-name
 * alias resolved and the manager was actually selected -- which is legal because the suite lives in
 * the same `org.apache.spark.shuffle.streaming` package.
 *
 * ==Activation contract under test==
 * The streaming backend is active for a shuffle iff BOTH of the following hold:
 *   1. `spark.shuffle.manager == "streaming"` (case-insensitive), which resolves through the
 *      `shortShuffleMgrNames` alias in the `ShuffleManager` factory to
 *      [[StreamingShuffleManager]]; and
 *   2. `spark.shuffle.streaming.enabled == "true"`.
 * When the manager is `streaming` but the feature is NOT enabled, [[StreamingShuffleManager]] is
 * still instantiated but delegates every shuffle to its inner
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] by composition. This suite verifies that
 * both modes produce identical, correct results -- the "zero regression / safe fallback" guarantee.
 *
 * ==Anti-flakiness==
 * All contexts use the small `local[2]` master and modest datasets (<= 100k records). Correctness
 * is asserted purely on shuffle OUTPUT (sums, counts, joined pairs, key ordering); no assertion is
 * made about timing, latency, or buffer/spill internals -- those belong to the benchmark and unit
 * suites. Because the v1 transport is a logging-only stub, correctness must (and does) hold through
 * the in-process path and/or the sort fallback, never relying on cross-executor network I/O. Each
 * test assigns [[LocalSparkContext.sc]], which is stopped automatically after every test.
 */
class StreamingShuffleIntegrationSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * Builds a streaming-enabled [[SparkContext]] on a small `local[2]` master. Callers may override
   * or add configuration entries (for example, disabling the feature to exercise the sort fallback)
   * via `extra`; later entries win because they are applied after the streaming defaults.
   */
  private def newStreamingContext(extra: (String, String)*): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingShuffleIntegrationSuite")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
    extra.foreach { case (k, v) => conf.set(k, v) }
    new SparkContext(conf)
  }

  /**
   * The expected `key -> sum` map for grouping `1 to 1000` by `i % 10`, computed independently of
   * Spark so the shuffle output can be checked against a trusted reference. Every residue class
   * `0..9` contains exactly 100 of the integers in `[1, 1000]`.
   */
  private def expectedResidueSums: Map[Int, Int] =
    (1 to 1000).groupBy(_ % 10).map { case (k, values) => (k, values.sum) }

  test("spark.shuffle.manager=streaming selects StreamingShuffleManager") {
    sc = newStreamingContext()
    // Proves the pending "streaming" short-name alias resolved and the streaming backend -- not the
    // default sort manager -- is the active ShuffleManager for this application.
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "spark.shuffle.manager=streaming must resolve to StreamingShuffleManager via the alias")
  }

  test("reduceByKey over a streaming shuffle yields correct results") {
    sc = newStreamingContext()
    // A 10-key reduction across 8 input partitions forces a real shuffle through the streaming
    // manager; the aggregated sums must match the independently computed reference exactly.
    val data = sc.parallelize(1 to 1000, 8).map(i => (i % 10, i))
    val result = data.reduceByKey(_ + _).collectAsMap()
    assert(result.size == 10, s"expected 10 distinct keys, got ${result.size}")
    assert(result.toMap == expectedResidueSums, "per-key sums must match the expected arithmetic")
  }

  test("groupByKey correctness across partitions") {
    sc = newStreamingContext()
    // groupByKey shuffles every value to its key's reduce partition; each residue class 0..9 must
    // receive exactly its 100 members with no loss or duplication across the 8 map partitions.
    val grouped = sc.parallelize(1 to 1000, 8).map(i => (i % 10, i)).groupByKey().collect()
    assert(grouped.length == 10, s"expected 10 grouped keys, got ${grouped.length}")
    assert(grouped.map(_._1).toSet == (0 to 9).toSet, "grouped keys must be exactly 0..9")
    grouped.foreach { case (key, values) =>
      assert(values.size == 100, s"key $key should have 100 elements, had ${values.size}")
    }
  }

  test("join correctness through streaming shuffle") {
    sc = newStreamingContext()
    // A co-partitioning join shuffles both sides on the key; only keys present in BOTH inputs
    // (1 and 3) survive, each paired with its matching value.
    val a = sc.parallelize(Seq((1, "a"), (2, "b"), (3, "c")))
    val b = sc.parallelize(Seq((1, 10), (3, 30)))
    assert(a.join(b).collect().toSet == Set((1, ("a", 10)), (3, ("c", 30))))
  }

  test("sortByKey (keyOrdering path) correctness") {
    sc = newStreamingContext()
    // sortByKey exercises the key-ordering shuffle path (range-partitioned); the collected keys
    // must come back in ascending order regardless of input ordering or partitioning.
    val sortedKeys =
      sc.parallelize(Seq(3, 1, 2, 5, 4), 2).map(x => (x, x)).sortByKey().keys.collect().toSeq
    assert(sortedKeys == Seq(1, 2, 3, 4, 5))
  }

  test("fallback-to-sort safety: manager=streaming but enabled=false runs identically") {
    // manager=streaming still instantiates StreamingShuffleManager, but with the feature disabled
    // it delegates every shuffle to the inner SortShuffleManager by composition. Correctness must
    // be indistinguishable from the enabled path -- the zero-regression fallback guarantee.
    sc = newStreamingContext("spark.shuffle.streaming.enabled" -> "false")
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "manager=streaming must still select StreamingShuffleManager even when disabled")
    val result =
      sc.parallelize(1 to 1000, 8).map(i => (i % 10, i)).reduceByKey(_ + _).collectAsMap()
    assert(result.size == 10, s"expected 10 distinct keys, got ${result.size}")
    assert(result.toMap == expectedResidueSums, "fallback results must match the streaming results")
  }

  test("empty shuffle completes without error") {
    sc = newStreamingContext()
    // A shuffle over an empty dataset must complete cleanly and yield no output -- exercising the
    // zero-record edge case of buffer allocation, transport, and read paths.
    val result = sc.parallelize(Seq.empty[(Int, Int)], 2).reduceByKey(_ + _).collect()
    assert(result.isEmpty, "reducing an empty dataset must produce an empty result")
  }

  test("larger partition count exercises buffering") {
    sc = newStreamingContext()
    // A wider fan-out (repartition to 16) over a larger record count exercises per-partition
    // buffering while staying modest enough to keep the suite fast and non-flaky. The record count
    // must be preserved exactly end to end through the shuffle.
    val numRecords = 100000
    val count = sc.parallelize(1 to numRecords, 4).map(i => (i % 100, i)).repartition(16).count()
    assert(count == numRecords, s"record count must be preserved end to end, got $count")
  }

}
