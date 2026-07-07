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
 * Benchmark comparing baseline sort-based shuffle vs. streaming shuffle latency.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt \
 *      "core/Test/runMain org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *      "core/Test/runMain org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 *      Results will be written to
 *      "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  // Number of (key, value) records shuffled per benchmark iteration. Kept bounded so a run
  // finishes in reasonable time on a developer machine while still exercising a real shuffle.
  val numRecords: Long = 2L * 1000 * 1000

  // Number of map/reduce partitions. Ten-plus partitions keep the comparison in the regime where
  // streaming shuffle is designed to help (AAP success criteria: 100MB+ data, 10+ partitions).
  val numPartitions: Int = 16

  /**
   * Builds a fresh local-mode [[SparkContext]] for a single benchmark case. Every case owns its
   * own context so that at most one context is ever active in the JVM (Spark permits only one),
   * and the caller is responsible for stopping it (see the `finally` blocks below).
   *
   * @param extra shuffle-manager / streaming configuration entries layered on top of the defaults
   */
  private def newContext(extra: (String, String)*): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("StreamingShufflePerformanceBenchmark")
    extra.foreach { case (k, v) => conf.set(k, v) }
    new SparkContext(conf)
  }

  /**
   * Runs one shuffle-heavy job (`reduceByKey` over `numRecords` records) and forces materialization
   * with `count()`. The identical job body is used for both the sort and streaming cases so that
   * the only variable between cases is the configured shuffle manager.
   */
  private def runShuffleJob(sc: SparkContext): Unit = {
    sc.parallelize(0L until numRecords, numPartitions)
      .map(i => (i % 1000, i))
      .reduceByKey(_ + _)
      .count()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Streaming Shuffle vs Sort Shuffle Latency") {
      val benchmark =
        new Benchmark("shuffle latency (reduceByKey)", numRecords, 3, output = output)

      // Baseline: the production-stable sort-based shuffle manager.
      benchmark.addCase("sort shuffle (baseline)") { _ =>
        val sc = newContext("spark.shuffle.manager" -> "sort")
        try {
          runShuffleJob(sc)
        } finally {
          sc.stop()
        }
      }

      // Candidate: the opt-in streaming shuffle manager. Requires BOTH the manager alias and the
      // dual-activation flag (AAP: manager=streaming AND streaming.enabled=true).
      benchmark.addCase("streaming shuffle") { _ =>
        val sc = newContext(
          "spark.shuffle.manager" -> "streaming",
          "spark.shuffle.streaming.enabled" -> "true")
        try {
          runShuffleJob(sc)
        } finally {
          sc.stop()
        }
      }

      benchmark.run()
    }
  }
}
