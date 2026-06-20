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
import org.apache.spark.internal.config.SHUFFLE_MANAGER

/**
 * Benchmark comparing the opt-in streaming shuffle backend against the default sort-based shuffle.
 *
 * This object mirrors the canonical [[org.apache.spark.benchmark.BenchmarkBase]] pattern used by
 * `ChecksumBenchmark`: it is a benchmark entry point, NOT a ScalaTest suite, and therefore makes
 * no assertions. Benchmarks report timings; the success-criteria deltas (a 30-50% latency
 * reduction for shuffle-heavy workloads, a 5-10% improvement for CPU-bound workloads, and zero
 * regression for memory-bound workloads via automatic fallback) are demonstrated by the committed
 * result files, not asserted here.
 *
 * Each case selects its shuffle backend purely through configuration -- `spark.shuffle.manager`
 * plus, for the streaming cases, `spark.shuffle.streaming.enabled=true` -- so the comparison
 * exercises the exact production activation contract: streaming engages only when both signals
 * hold, and otherwise (or when the fallback policy trips) delegates to the sort-based path. Because
 * a JVM may host at most one active [[org.apache.spark.SparkContext]], every case builds its own
 * context with the chosen manager and stops it before the next case runs.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain
 *        org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 *      Results will be written to "benchmarks/StreamingShuffleBenchmark-results.txt"
 *      and "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 *
 * Those two `*-results.txt` files live in `core/benchmarks/` (a sibling of `core/src`) and are
 * produced and committed by the `core` module agent when this benchmark is run with
 * `SPARK_GENERATE_BENCHMARK_FILES=1`; they are outside this source folder's scope. This file
 * contributes only the benchmark source.
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  /** Local master with enough threads to force a real, multi-partition shuffle. */
  private val LOCAL_MASTER = "local[2]"

  /** `spark.shuffle.manager` alias for the default sort-based backend. */
  private val SORT_MANAGER = "sort"

  /** `spark.shuffle.manager` alias for the opt-in streaming backend. */
  private val STREAMING_MANAGER = "streaming"

  /** Number of distinct shuffle keys; bounds the reduce-side cardinality. */
  private val NUM_KEYS = 1024

  /** Shuffle-heavy workload: large record count spread across >= 10 partitions. */
  private val SHUFFLE_HEAVY_RECORDS = 500000
  private val SHUFFLE_HEAVY_PARTITIONS = 16

  /** CPU-bound workload: fewer records and a light shuffle, but heavier per-record compute. */
  private val CPU_BOUND_RECORDS = 100000
  private val CPU_BOUND_PARTITIONS = 8
  private val CPU_BOUND_WORK = 128

  /**
   * Tight-memory configuration for the memory-bound workload. A constrained memory budget combined
   * with the minimum streaming buffer percentage exercises the spill/fallback path so the streaming
   * case automatically reverts to the sort-based shuffle (the zero-regression guarantee). The
   * memory value stays above the memory manager's minimum so the context still initializes.
   */
  private val memoryBoundConf = Seq(
    "spark.testing.memory" -> "536870912",
    "spark.shuffle.streaming.bufferSizePercent" -> "1")

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Shuffle-heavy workload (>=100MB, >=10 partitions)") {
      val benchmark =
        new Benchmark("Shuffle-heavy workload", SHUFFLE_HEAVY_RECORDS.toLong, output = output)
      benchmark.addCase("sort shuffle") { _ =>
        withShuffleContext(SORT_MANAGER) { sc => runShuffleJob(SORT_MANAGER, sc) }
      }
      benchmark.addCase("streaming shuffle") { _ =>
        withShuffleContext(STREAMING_MANAGER) { sc => runShuffleJob(STREAMING_MANAGER, sc) }
      }
      benchmark.run()
    }

    runBenchmark("CPU-bound workload") {
      val benchmark =
        new Benchmark("CPU-bound workload", CPU_BOUND_RECORDS.toLong, output = output)
      benchmark.addCase("sort shuffle") { _ =>
        withShuffleContext(SORT_MANAGER) { sc => runCpuBoundJob(SORT_MANAGER, sc) }
      }
      benchmark.addCase("streaming shuffle") { _ =>
        withShuffleContext(STREAMING_MANAGER) { sc => runCpuBoundJob(STREAMING_MANAGER, sc) }
      }
      benchmark.run()
    }

    runBenchmark("Memory-bound workload (fallback)") {
      val benchmark =
        new Benchmark("Memory-bound workload", SHUFFLE_HEAVY_RECORDS.toLong, output = output)
      benchmark.addCase("sort shuffle") { _ =>
        withShuffleContext(SORT_MANAGER, memoryBoundConf) { sc =>
          runShuffleJob(SORT_MANAGER, sc)
        }
      }
      benchmark.addCase("streaming shuffle") { _ =>
        withShuffleContext(STREAMING_MANAGER, memoryBoundConf) { sc =>
          runShuffleJob(STREAMING_MANAGER, sc)
        }
      }
      benchmark.run()
    }
  }

  /**
   * Builds a [[SparkContext]] configured with the requested shuffle `manager`, runs `body` against
   * it, and stops it before returning so the next case can create its own context (a JVM may host
   * only one active context at a time). Streaming cases additionally arm the feature flag; both
   * signals together are what engage the streaming path, exactly as in production.
   *
   * Defaults are loaded so the `spark.testing` system property set by [[BenchmarkBase.main]] is
   * picked up, which relaxes the memory manager's minimum-system-memory check and keeps the
   * benchmark runnable regardless of the launching JVM's heap size.
   *
   * @param manager   the `spark.shuffle.manager` alias ("sort" or "streaming")
   * @param extraConf additional `(key, value)` settings (e.g. tight memory for the fallback case)
   * @param body      the workload to run against the freshly created context
   */
  private def withShuffleContext(
      manager: String,
      extraConf: Seq[(String, String)] = Seq.empty)(body: SparkContext => Unit): Unit = {
    val conf = new SparkConf()
      .setMaster(LOCAL_MASTER)
      .setAppName(s"streaming-shuffle-benchmark-$manager")
      .set("spark.ui.enabled", "false")
      .set(SHUFFLE_MANAGER, manager)
    if (manager == STREAMING_MANAGER) {
      // Streaming engages only when the manager alias AND this feature flag are both set; otherwise
      // StreamingShuffleManager delegates to its inner SortShuffleManager (sort-based fallback).
      conf.set("spark.shuffle.streaming.enabled", "true")
    }
    extraConf.foreach { case (key, value) => conf.set(key, value) }
    val sc = new SparkContext(conf)
    try {
      body(sc)
    } finally {
      sc.stop()
    }
  }

  /**
   * Shuffle-heavy job: parallelizes a large key/value range across many partitions and forces a
   * shuffle via `reduceByKey`, materialized with `count()`. Used by the shuffle-heavy and
   * memory-bound groups, which differ only in their [[SparkContext]] memory configuration.
   *
   * @param manager the active shuffle manager, recorded as the job description for traceability
   * @param sc      the context whose shuffle backend is exercised
   */
  private def runShuffleJob(manager: String, sc: SparkContext): Unit = {
    sc.setJobDescription(s"streaming-shuffle-benchmark [shuffle-heavy/$manager]")
    sc.parallelize(0 until SHUFFLE_HEAVY_RECORDS, SHUFFLE_HEAVY_PARTITIONS)
      .map(i => (i % NUM_KEYS, i.toLong))
      .reduceByKey(_ + _)
      .count()
  }

  /**
   * CPU-bound job: performs a bounded per-record compute loop before a comparatively light shuffle,
   * so scheduler and transport overhead -- rather than shuffle volume -- dominates the runtime.
   *
   * @param manager the active shuffle manager, recorded as the job description for traceability
   * @param sc      the context whose shuffle backend is exercised
   */
  private def runCpuBoundJob(manager: String, sc: SparkContext): Unit = {
    sc.setJobDescription(s"streaming-shuffle-benchmark [cpu-bound/$manager]")
    sc.parallelize(0 until CPU_BOUND_RECORDS, CPU_BOUND_PARTITIONS)
      .map { i =>
        var acc = i.toLong
        var j = 0
        while (j < CPU_BOUND_WORK) {
          acc = (acc * 31 + j) & 0xffffffffL
          j += 1
        }
        (i % NUM_KEYS, acc)
      }
      .reduceByKey(_ + _)
      .count()
  }
}
