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
 * Component-level microbenchmark for the streaming shuffle write path and read path versus the
 * default sort-based shuffle.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StreamingShuffleBenchmark-results.txt".
 * }}}
 *
 * Where the sibling [[StreamingShufflePerformanceBenchmark]] measures the single end-to-end
 * latency headline, this benchmark splits the shuffle into its two halves so the producer-side
 * and consumer-side costs can be contrasted independently:
 *
 *  - '''Write path''' uses a non-aggregating `groupByKey`, which performs no map-side combine and
 *    therefore forces the producer to buffer, checksum (CRC32C), and write the full shuffle
 *    volume. This isolates the cost dominated by [[StreamingShuffleWriter]].
 *  - '''Read path''' uses an aggregating `reduceByKey`, whose map-side combine shrinks the bytes
 *    on the wire so the dominant remaining work is the consumer fetching, validating, and merging
 *    blocks. This isolates the cost dominated by [[StreamingShuffleReader]].
 *
 * In both halves the streaming manager is selected through the dual-flag activation contract
 * (`spark.shuffle.manager=streaming` plus `spark.shuffle.streaming.enabled=true`), and the sort
 * and streaming cases run the identical workload so the only variable is the active
 * `ShuffleManager`. Each case owns its `SparkContext` lifecycle, which guarantees that exactly
 * one context is active at a time and avoids cross-case port/JVM conflicts.
 */
object StreamingShuffleBenchmark extends BenchmarkBase {

  /**
   * Number of records shuffled per benchmark iteration. This value is also passed to `Benchmark`
   * as the per-iteration row count, so the reported `Rate(M/s)` and `Per Row(ns)` columns
   * describe the per-record cost of the shuffle.
   */
  private val N = 10000000

  /** Number of input partitions feeding the shuffle (the map-side task count). */
  private val numInputPartitions = 16

  /**
   * Number of shuffle (reduce) partitions. Kept well above the ten-partition shuffle-heavy
   * threshold documented for the streaming shuffle feature so the workload exercises a real,
   * fanned-out shuffle and matches the committed "200 partitions" baseline.
   */
  private val numShufflePartitions = 200

  /**
   * Key cardinality for the synthetic dataset. A large key space keeps the per-key value lists
   * short so the non-aggregating `groupByKey` write-path workload stays memory-safe while still
   * writing the full record volume across the shuffle.
   */
  private val keySpace = 1 << 20

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Streaming shuffle write path") {
      val benchmark =
        new Benchmark("Shuffle write (256MB, 200 partitions)", N, 3, output = output)
      benchmark.addCase("sort shuffle writer") { _ =>
        runWritePath(streaming = false)
      }
      benchmark.addCase("streaming shuffle writer") { _ =>
        runWritePath(streaming = true)
      }
      benchmark.run()
    }

    runBenchmark("Streaming shuffle read path") {
      val benchmark =
        new Benchmark("Shuffle read (256MB, 200 partitions)", N, 3, output = output)
      benchmark.addCase("sort shuffle reader") { _ =>
        runReadPath(streaming = false)
      }
      benchmark.addCase("streaming shuffle reader") { _ =>
        runReadPath(streaming = true)
      }
      benchmark.run()
    }
  }

  /**
   * Executes `body` with a freshly created [[SparkContext]] configured for either the streaming
   * or the sort shuffle manager. The context is always stopped before returning, which guarantees
   * that exactly one `SparkContext` is active at any moment and avoids cross-case port/JVM
   * conflicts.
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

  /**
   * Write-path workload: a non-aggregating `groupByKey` performs no map-side combine, so the
   * producer must buffer, checksum, and write the entire record volume across the shuffle. This
   * exercises the streaming writer (feature F-103) on the configured shuffle manager.
   */
  private def runWritePath(streaming: Boolean): Unit = withSc(streaming) { sc =>
    sc.parallelize(0 until N, numInputPartitions)
      .map(i => (i % keySpace, i))
      .groupByKey(numShufflePartitions)
      .count()
  }

  /**
   * Read-path workload: an aggregating `reduceByKey` applies a map-side combine, so the dominant
   * remaining work is the consumer fetching, validating, and merging shuffle blocks. This
   * exercises the streaming reader (feature F-104) on the configured shuffle manager.
   */
  private def runReadPath(streaming: Boolean): Unit = withSc(streaming) { sc =>
    sc.parallelize(0 until N, numInputPartitions)
      .map(i => (i % keySpace, i))
      .reduceByKey(_ + _, numShufflePartitions)
      .count()
  }
}
