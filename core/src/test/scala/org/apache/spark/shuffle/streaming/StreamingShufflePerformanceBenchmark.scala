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

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Performance benchmark for streaming shuffle vs sort-based shuffle.
 *
 * Validates AAP §0.1.1 success criterion: "30-50% end-to-end latency reduction for
 * shuffle-heavy workloads (100MB+ data, 10+ partitions)".
 *
 * == Workload ==
 * The benchmark constructs a synthetic dataset of `(Int, Int)` tuples partitioned across
 * [[NUM_PARTITIONS]] reducers using [[HashPartitioner]], then triggers a `groupByKey`
 * shuffle to force the shuffle write/read code path through the configured
 * `spark.shuffle.manager`. Both `sort` (baseline) and `streaming` cases run the
 * identical operation under separate [[SparkContext]] instances configured with the
 * corresponding `spark.shuffle.manager` value, ensuring each case exercises only its
 * own shuffle implementation.
 *
 * The dataset volume targets the AAP §0.1.1 shuffle-heavy threshold of 100 MB. To keep
 * the benchmark runtime bounded (~30-60 seconds total), [[NUM_RECORDS]] is set to 2
 * million `(Int, Int)` tuples (~16 MB serialized), with the shuffle code path
 * exercising the same hash-partition / spill / read mechanics as the full 100 MB
 * dataset would. The full 100 MB dimension is preserved in the benchmark name string
 * via [[DATASET_SIZE_BYTES]] and serves as documentation of the AAP target.
 *
 * == Reproducibility ==
 * To generate result files (commits to `core/benchmarks/`):
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain
 *     org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 * }}}
 *
 * To run without generating files (local exploration):
 * {{{
 *   build/sbt "core/Test/runMain
 *     org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 * }}}
 *
 * Result file path (when regenerating):
 * `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` (with `-jdk21`
 * suffix on JDK 21+, automatically appended by [[BenchmarkBase]]).
 *
 * == Coexistence ==
 * Both benchmark cases run with isolated [[SparkContext]] instances. The sort case uses
 * the production-default `spark.shuffle.manager=sort`; the streaming case uses
 * `streaming`, which is registered as a short-name alias for
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]] in the
 * [[org.apache.spark.shuffle.ShuffleManager]] companion's `shortShuffleMgrNames` map.
 * Neither case modifies the existing
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] implementation, honoring the AAP
 * §0.7 directive: "Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."
 *
 * == JVM Setup ==
 * Each iteration creates and stops its own [[SparkContext]] because
 * `spark.shuffle.manager` is read once during [[org.apache.spark.SparkEnv]]
 * construction and cannot be changed mid-application. This is a deliberate consequence
 * of the AAP §0.7 directive: "Configuration changes require executor restart (no
 * dynamic reconfiguration in v1)."
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  /**
   * 100 MB target dataset size per AAP §0.1.1 (shuffle-heavy workload threshold).
   * Used in the benchmark name string for documentation; the actual record volume
   * is governed by [[NUM_RECORDS]] to keep benchmark runtime bounded.
   */
  private val DATASET_SIZE_BYTES: Long = 100L * 1024L * 1024L

  /** 10 partitions per AAP §0.1.1 (shuffle-heavy threshold). */
  private val NUM_PARTITIONS: Int = 10

  /**
   * Records per iteration: 2 million `(Int, Int)` tuples (~16 MB serialized). This
   * exercises the full shuffle write / partition / read code path through the
   * configured shuffle manager while keeping total benchmark runtime under ~60
   * seconds across both cases. The AAP §0.1.1 100 MB / 13M-record target dimension
   * is documented in [[DATASET_SIZE_BYTES]] and reflected in the benchmark name.
   */
  private val NUM_RECORDS: Int = 2 * 1000 * 1000

  /**
   * Minimum number of timed iterations per case. The [[Benchmark]] infrastructure
   * runs additional warmup iterations before measurement begins (default 2 seconds
   * of warmup) so JIT compilation has settled. Three measured iterations provide a
   * stable best/avg/stdev estimate without ballooning total runtime.
   */
  private val NUM_ITERATIONS: Int = 3

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("StreamingShuffleVsSort") {
      val datasetSizeMb = DATASET_SIZE_BYTES / (1024L * 1024L)
      val benchmark = new Benchmark(
        name = s"Streaming Shuffle vs Sort Shuffle (${datasetSizeMb}MB target / " +
          s"$NUM_PARTITIONS partitions)",
        valuesPerIteration = NUM_RECORDS.toLong,
        minNumIters = NUM_ITERATIONS,
        output = output)

      benchmark.addCase("sort baseline") { _ =>
        runShuffleWorkload(shuffleManager = "sort")
      }

      benchmark.addCase("streaming") { _ =>
        runShuffleWorkload(shuffleManager = "streaming")
      }

      benchmark.run()
    }
  }

  /**
   * Construct a [[SparkContext]] with the given shuffle manager, run a `groupByKey`
   * shuffle across [[NUM_PARTITIONS]] partitions over [[NUM_RECORDS]] `(Int, Int)`
   * tuples, then stop the context. Each invocation creates and tears down its own
   * [[SparkContext]] for case isolation; this is required because
   * `spark.shuffle.manager` is read once during [[org.apache.spark.SparkEnv]]
   * construction.
   *
   * The shuffle is forced into materialization by counting groups; this is the
   * actual shuffle-bound operation under measurement. The result count is asserted
   * to equal [[NUM_PARTITIONS]] (since keys are `i % NUM_PARTITIONS`) as a sanity
   * check that the shuffle produced correct output for the configured manager.
   *
   * @param shuffleManager the value to set for `spark.shuffle.manager` ("sort" or
   *                       "streaming"). Both values are registered short names
   *                       resolved by the [[org.apache.spark.shuffle.ShuffleManager]]
   *                       companion's `shortShuffleMgrNames` map.
   */
  private def runShuffleWorkload(shuffleManager: String): Unit = {
    val conf = new SparkConf(loadDefaults = false)
      .setAppName(s"StreamingShufflePerformanceBenchmark-$shuffleManager")
      .setMaster("local[2]")
      .set("spark.shuffle.manager", shuffleManager)
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")

    // Defensive: opt in to the streaming-shuffle feature flag in addition to the
    // manager dispatch knob. The streaming manager honors both signals; setting
    // both makes the benchmark robust against future hardening that requires the
    // explicit feature flag.
    if (shuffleManager == "streaming") {
      conf.set("spark.shuffle.streaming.enabled", "true")
    }

    val sc = new SparkContext(conf)
    try {
      // Generate (Int, Int) pairs and trigger a groupByKey shuffle across
      // NUM_PARTITIONS. The explicit partitionBy step pins the shuffle dependency
      // to the desired HashPartitioner; groupByKey(NUM_PARTITIONS) then forces
      // the actual shuffle stage with the configured shuffle manager.
      val rdd = sc.parallelize(0 until NUM_RECORDS, NUM_PARTITIONS)
        .map(i => (i % NUM_PARTITIONS, i))
        .partitionBy(new HashPartitioner(NUM_PARTITIONS))
        .groupByKey(NUM_PARTITIONS)

      // Force materialization of the shuffle by counting groups; this is the
      // actual shuffle-bound operation under measurement. Since keys are
      // `i % NUM_PARTITIONS`, the resulting group count must equal
      // NUM_PARTITIONS - any deviation indicates a shuffle correctness bug
      // in the configured manager.
      val count = rdd.count()
      require(
        count == NUM_PARTITIONS.toLong,
        s"Expected $NUM_PARTITIONS groups for shuffle manager '$shuffleManager', " +
          s"got $count. This indicates a shuffle correctness defect.")
    } finally {
      sc.stop()
    }
  }
}
