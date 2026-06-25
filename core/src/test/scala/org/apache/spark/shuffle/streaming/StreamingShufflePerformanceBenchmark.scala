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
 * Benchmark for streaming shuffle vs sort shuffle.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 *
 * This microbenchmark contrasts the end-to-end latency of the opt-in streaming shuffle
 * manager against the default sort-based shuffle. The streaming manager is selected through
 * the dual-flag activation contract (`spark.shuffle.manager=streaming` plus
 * `spark.shuffle.streaming.enabled=true`). Both cases run an identical `reduceByKey` workload,
 * so the only variable is the active `ShuffleManager`. Each case owns its `SparkContext`
 * lifecycle, which guarantees that exactly one context is active at a time.
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  /** Number of records shuffled per benchmark iteration (modest but representative). */
  private val N = 1000000

  /**
   * Number of shuffle partitions. Kept at or above the ten-partition shuffle-heavy threshold
   * documented for the streaming shuffle feature so that the workload exercises a real shuffle.
   */
  private val numPartitions = 16

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Streaming Shuffle Performance") {
      val benchmark =
        new Benchmark("shuffle latency: streaming vs sort", N, 3, output = output)
      benchmark.addCase("sort shuffle") { _ =>
        runSortShuffle(N)
      }
      benchmark.addCase("streaming shuffle") { _ =>
        runStreamingShuffle(N)
      }
      benchmark.run()
    }
  }

  /**
   * Executes `body` with a freshly created [[SparkContext]] configured for either the
   * streaming or the sort shuffle manager. The context is always stopped before returning,
   * which guarantees that exactly one `SparkContext` is active at any moment and avoids
   * cross-case port/JVM conflicts.
   *
   * @param streaming when `true`, selects the streaming shuffle manager via the dual-flag
   *                  activation contract; otherwise the default sort-based shuffle is used
   * @param body      the workload to execute against the configured context
   * @tparam T        the result type produced by `body`
   * @return the value produced by `body`
   */
  private def withSc[T](streaming: Boolean)(body: SparkContext => T): T = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("streaming-shuffle-benchmark")
    if (streaming) {
      conf
        .set("spark.shuffle.manager", "streaming")
        .set("spark.shuffle.streaming.enabled", "true")
    }
    val sc = new SparkContext(conf)
    try {
      body(sc)
    } finally {
      sc.stop()
    }
  }

  /** Runs the shared `reduceByKey` workload on the default sort-based shuffle manager. */
  private def runSortShuffle(n: Int): Unit = withSc(streaming = false) { sc =>
    sc.parallelize(1 to n, numPartitions).map(i => (i % 64, i)).reduceByKey(_ + _).count()
  }

  /** Runs the shared `reduceByKey` workload on the opt-in streaming shuffle manager. */
  private def runStreamingShuffle(n: Int): Unit = withSc(streaming = true) { sc =>
    sc.parallelize(1 to n, numPartitions).map(i => (i % 64, i)).reduceByKey(_ + _).count()
  }
}
