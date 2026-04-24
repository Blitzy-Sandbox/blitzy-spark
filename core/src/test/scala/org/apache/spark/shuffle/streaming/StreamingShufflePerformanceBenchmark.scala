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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark comparing the opt-in streaming shuffle path against the production-stable
 * sort-based shuffle path.
 *
 * ==Coexistence Strategy==
 *
 * This benchmark exercises the two shuffle manager implementations that coexist in
 * Apache Spark 4.2+ via the pluggable [[org.apache.spark.shuffle.ShuffleManager]] SPI:
 *
 *  - `spark.shuffle.manager=sort` -- the production-stable default that maps to
 *    [[org.apache.spark.shuffle.sort.SortShuffleManager]]. This path is unchanged by
 *    the streaming shuffle feature and continues to serve every deployment that does
 *    not explicitly opt in.
 *  - `spark.shuffle.manager=streaming` -- the opt-in streaming path that maps to
 *    `org.apache.spark.shuffle.streaming.StreamingShuffleManager`. This path pipelines
 *    map-output bytes directly from producer executors to consumer executors with
 *    in-memory buffering and consumer-driven backpressure, falling back to sort-based
 *    shuffle automatically when degradation is detected (consumer slowdown, memory
 *    pressure, network saturation, or version mismatch).
 *
 * The streaming path is fully isolated: it lives in a dedicated sub-package, adds a
 * single short-name entry to
 * [[org.apache.spark.shuffle.ShuffleManager.getShuffleManagerClassName]], and never
 * modifies the sort-path implementation. Consequently, a default `SparkContext` built
 * without setting `spark.shuffle.manager` executes exactly the same sort-path
 * codepaths as before the streaming feature was introduced.
 *
 * ==Success Criterion==
 *
 * This benchmark validates the user-specified success criterion:
 *
 *   "30-50% end-to-end latency reduction for shuffle-heavy workloads
 *    (100MB+ data, 10+ partitions)"
 *
 * Each comparison adds the `sort-based shuffle (baseline)` case first and the
 * `streaming shuffle (opt-in)` case second, so the Relative column of the output
 * table shows the streaming path's speedup factor directly (e.g., `1.5X` corresponds
 * to a ~33% latency reduction, `2.0X` to a 50% reduction).
 *
 * ==Running This Benchmark==
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to
 *      "benchmarks/StreamingShufflePerformanceBenchmark-results.txt" (JDK 17) or
 *      "benchmarks/StreamingShufflePerformanceBenchmark-jdk21-results.txt" (JDK 21+).
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  // ~100 bytes per (Int key, ~96-byte String value) record. Keeping this as a Long
  // avoids integer overflow when computing record counts for multi-hundred-MB payloads.
  private val BYTES_PER_RECORD: Long = 100L

  // Cardinality of the key space for groupByKey. Enough keys to produce realistic
  // partition-level fan-out without collapsing into a trivial hot-key workload.
  private val NUM_KEYS: Int = 1000

  // Pre-computed data-volume constants for the benchmark matrix. All three satisfy
  // the "100MB+ data" portion of the success criterion.
  private val DATA_100MB: Long = 100L * 1024L * 1024L
  private val DATA_200MB: Long = 200L * 1024L * 1024L
  private val DATA_500MB: Long = 500L * 1024L * 1024L

  // A single SparkContext is shared across cases that use the same shuffle manager.
  // Because SparkEnv.initializeShuffleManager() uses Preconditions.checkState to
  // enforce single initialization, the entire context is torn down and rebuilt when
  // switching between "sort" and "streaming". @volatile ensures visibility across
  // the benchmark driver thread and any background threads the benchmark spawns.
  @volatile private var sc: SparkContext = null
  @volatile private var currentShuffleManager: String = null

  /**
   * Returns a SparkContext bound to the requested shuffle manager, creating one if
   * necessary. If the cached context already uses the requested manager the same
   * instance is reused to minimize teardown overhead between iterations of the same
   * benchmark case. Switching managers always tears down the cached context first
   * because SparkEnv binds the shuffle manager exactly once per SparkEnv lifetime.
   */
  private def ensureSparkContext(shuffleManager: String): SparkContext = {
    if (sc == null || currentShuffleManager != shuffleManager) {
      if (sc != null) {
        sc.stop()
        sc = null
      }
      // loadDefaults=false ensures deterministic behavior regardless of any
      // spark-defaults.conf present on the host running the benchmark.
      val conf = new SparkConf(false)
        .setMaster("local[4]")
        .setAppName("StreamingShufflePerformanceBenchmark")
        .set("spark.shuffle.manager", shuffleManager)
        .set("spark.ui.enabled", "false")
      sc = new SparkContext(conf)
      currentShuffleManager = shuffleManager
    }
    sc
  }

  /**
   * Executes a shuffle-heavy workload that exercises the ShuffleWriter / ShuffleReader
   * code paths end-to-end. `groupByKey` is intentionally chosen because it performs no
   * map-side combining, so every input record crosses the shuffle boundary and the
   * full configured data volume traverses the network/BlockManager stack.
   */
  private def runShuffleWorkload(
      context: SparkContext,
      numPartitions: Int,
      dataBytes: Long): Unit = {
    val numRecords = (dataBytes / BYTES_PER_RECORD).toInt
    // 8 bytes reserved for the Int key + tuple object overhead; the remaining
    // ~92 bytes are consumed by the String payload to approximate 100 bytes per pair.
    val payloadSize = (BYTES_PER_RECORD - 8L).toInt
    val payload = "x" * payloadSize
    context.parallelize(0 until numRecords, numPartitions)
      .map(i => (i % NUM_KEYS, payload))
      .groupByKey(numPartitions)
      .count()
  }

  /**
   * Runs a single sort-vs-streaming comparison. The sort case is registered first so
   * that `Benchmark.run()` (which uses relativeTime=false by default) treats the sort
   * best-time as the 1.0X baseline and prints the streaming path's speedup factor in
   * the Relative column.
   */
  private def compareShuffleManagers(
      benchmarkName: String,
      numPartitions: Int,
      dataBytes: Long,
      numIters: Int = 3): Unit = {
    val numRecords = dataBytes / BYTES_PER_RECORD
    val benchmark = new Benchmark(benchmarkName, numRecords, numIters, output = output)
    benchmark.addCase("sort-based shuffle (baseline)") { _ =>
      val ctx = ensureSparkContext("sort")
      runShuffleWorkload(ctx, numPartitions, dataBytes)
    }
    benchmark.addCase("streaming shuffle (opt-in)") { _ =>
      val ctx = ensureSparkContext("streaming")
      runShuffleWorkload(ctx, numPartitions, dataBytes)
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Section 1: Primary success-criterion validation. The 100MB / 10-partition
    // configuration is the exact boundary called out by the user-specified success
    // criterion ("30-50% end-to-end latency reduction for shuffle-heavy workloads
    // (100MB+ data, 10+ partitions)"). Expected Relative for the streaming case: >=1.3X.
    runBenchmark("Shuffle Performance - 100MB / 10 partitions (primary success criterion)") {
      compareShuffleManagers(
        "groupByKey on 100MB across 10 partitions", 10, DATA_100MB)
    }

    // Section 2: Partition count sensitivity. Larger partition counts shrink the
    // per-partition buffer available to the streaming writer, stressing the
    // buffer-allocation discipline and the BackpressureProtocol's priority arbitration.
    runBenchmark("Shuffle Performance - Varying Partition Counts on 100MB") {
      Seq(10, 50, 200).foreach { numPartitions =>
        compareShuffleManagers(
          s"groupByKey on 100MB across $numPartitions partitions",
          numPartitions,
          DATA_100MB)
      }
    }

    // Section 3: Data volume sensitivity. Larger volumes increasingly benefit from
    // streaming's pipelining because the consumer can begin processing blocks before
    // the producer has materialized the full shuffle output, whereas the sort path
    // must complete the map-side write before any read can start.
    runBenchmark("Shuffle Performance - Varying Data Volumes on 10 partitions") {
      Seq(DATA_100MB, DATA_200MB, DATA_500MB).foreach { dataBytes =>
        val mb = dataBytes / (1024L * 1024L)
        compareShuffleManagers(
          s"groupByKey on ${mb}MB across 10 partitions", 10, dataBytes)
      }
    }
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }
}
