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
 * Performance benchmark for the opt-in streaming shuffle backend across four representative
 * workload regimes, each comparing the production-stable sort-based shuffle (baseline) against the
 * streaming shuffle manager.
 *
 * ==What this proves (and what it deliberately does not)==
 * The streaming transport is a v1 logging-only stub
 * ([[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport.isWireTransferAvailable]]
 * `== false`), so `StreamingShuffleManager` routes every production shuffle through its inner
 * `SortShuffleManager`. Consequently the "streaming shuffle" case executes the identical sort code
 * path as the baseline, and the acceptance target for this benchmark is '''zero performance
 * regression via automatic fallback''' (streaming ~ 1.0X sort), not the 30-50% latency reduction
 * that the AAP earmarks for the v2 wire path. The four tables exercise genuinely different regimes
 * (two shuffle-heavy sizes, a CPU-bound job, and a memory-heavy `groupByKey`) so the no-regression
 * guarantee is demonstrated broadly rather than for a single shape.
 *
 * ==Timing methodology==
 * Each case uses [[Benchmark.addTimerCase]] to time ONLY the shuffle job; the per-iteration
 * `SparkContext` construction and teardown happen outside the timed region so the reported
 * milliseconds reflect the shuffle itself rather than driver startup. The framework's per-case
 * warmup drives each case to JIT/heap steady state before measurement, and the `Relative` column
 * is computed from best (minimum) times -- so identical code paths (the v1 sort fallback) converge
 * to ~1.0X rather than reporting a warm-ordering artifact.
 *
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

  // Fixed iteration count per case: bounded so a full four-table run completes in minutes on a
  // developer machine, while still averaging over multiple shuffles for a stable best/avg/stdev.
  private val NUM_ITERS: Int = 3

  // Record counts per regime. Kept bounded (millions, not billions) so the benchmark finishes
  // quickly, while the shuffle-heavy pair scales up (100MB/16 -> 512MB/64 regime labels) to
  // demonstrate the no-regression guarantee holds as data volume and partition count grow.
  private val SHUFFLE_HEAVY_SMALL_RECORDS: Long = 4L * 1000 * 1000
  private val SHUFFLE_HEAVY_LARGE_RECORDS: Long = 8L * 1000 * 1000
  private val CPU_BOUND_RECORDS: Long = 2L * 1000 * 1000
  private val MEMORY_BOUND_RECORDS: Long = 3L * 1000 * 1000

  // One-time global JVM warmup that complements the framework's per-case warmup: a short burst of
  // sort-path shuffles that stabilize JVM-wide state (heap sizing, GC ergonomics, C2 compilation)
  // before the first -- and coldest -- table is timed. See [[warmUpJvm]].
  private val WARMUP_JOBS: Int = 20
  private val WARMUP_RECORDS: Long = 4L * 1000 * 1000

  /**
   * Builds a fresh local-mode [[SparkContext]] for a single benchmark case invocation. Every
   * invocation owns its own context so that at most one context is ever active in the JVM (Spark
   * permits only one), and the caller is responsible for stopping it (see the `finally` blocks in
   * [[addSortAndStreamingCases]]).
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
   * Adds the two comparison cases -- "sort shuffle (baseline)" then "streaming shuffle" -- to
   * `benchmark`, running the SAME `job` under each so the only variable is the configured shuffle
   * manager. The streaming case sets BOTH the manager alias and the dual-activation flag (AAP:
   * `manager=streaming` AND `streaming.enabled=true`); in v1 it falls back to the sort path.
   *
   * Timing is manual: the `SparkContext` is created before `timer.startTiming()` and stopped in a
   * `finally` after `timer.stopTiming()`, so only the shuffle `job` is measured.
   */
  private def addSortAndStreamingCases(
      benchmark: Benchmark)(job: SparkContext => Unit): Unit = {
    benchmark.addTimerCase("sort shuffle (baseline)") { timer =>
      val sc = newContext("spark.shuffle.manager" -> "sort")
      try {
        timer.startTiming()
        job(sc)
        timer.stopTiming()
      } finally {
        sc.stop()
      }
    }
    benchmark.addTimerCase("streaming shuffle") { timer =>
      val sc = newContext(
        "spark.shuffle.manager" -> "streaming",
        "spark.shuffle.streaming.enabled" -> "true")
      try {
        timer.startTiming()
        job(sc)
        timer.stopTiming()
      } finally {
        sc.stop()
      }
    }
  }

  /**
   * Builds a [[Benchmark]] writing to `output`, relying on the framework's per-case warmup and
   * multi-iteration best-of-min timing (default `warmupTime`/`minTime`). Each case is thus driven
   * independently to JIT/heap steady state before it is measured. Because v1 streaming falls back
   * to the identical sort path, this per-case warmup is what keeps the comparison honest at ~1.0X:
   * without it the first timed case (sort) would pay cold-start JIT that the later, already-warm
   * streaming case avoids, manufacturing an apparent speedup that cannot exist under v1 fallback.
   */
  private def newBenchmark(title: String, numRecords: Long): Benchmark = {
    new Benchmark(title, numRecords, minNumIters = NUM_ITERS, output = output)
  }

  /**
   * Drives the JVM to steady state once, before the first table is timed, by running a few
   * throwaway sort-path shuffles. This complements the framework's per-case warmup: the framework
   * re-warms the code cache for each individual case, but only a global pre-warm stabilizes
   * JVM-wide state (heap sizing, GC ergonomics) that would otherwise make the very first timed
   * table -- the coldest -- report a warm-ordering artifact. HotSpot compilation and heap growth
   * are per-JVM and survive `SparkContext` restarts, so one global warmup benefits every table.
   * Because v1 streaming falls back to the identical sort path, this is purely about measurement
   * fidelity: it surfaces the expected ~1.0X (no regression) instead of a first-table illusion.
   */
  private def warmUpJvm(): Unit = {
    val sc = newContext("spark.shuffle.manager" -> "sort")
    try {
      var w = 0
      while (w < WARMUP_JOBS) {
        runShuffleHeavyJob(sc, WARMUP_RECORDS, 16, 1000L)
        w += 1
      }
    } finally {
      sc.stop()
    }
  }

  /**
   * Shuffle-heavy job: `reduceByKey` over `numRecords` records materialized with `count()`.
   * Dominated by shuffle write/read, this is the regime the AAP success criteria target
   * (100MB+ data, 10+ partitions).
   */
  private def runShuffleHeavyJob(
      sc: SparkContext, numRecords: Long, numPartitions: Int, numKeys: Long): Unit = {
    sc.parallelize(0L until numRecords, numPartitions)
      .map(i => (i % numKeys, i))
      .reduceByKey(_ + _)
      .count()
  }

  /**
   * CPU-bound job: a transcendental inner loop per record makes map-side computation dominate while
   * the shuffle footprint stays small (few keys). This is the regime where the AAP expects only a
   * modest 5-10% scheduler-overhead improvement, and where no regression must hold.
   */
  private def runCpuBoundJob(
      sc: SparkContext, numRecords: Long, numPartitions: Int, numKeys: Long): Unit = {
    sc.parallelize(0L until numRecords, numPartitions)
      .map { i =>
        var acc = 0.0d
        var k = 0
        while (k < 128) {
          acc += math.sqrt((i + k).toDouble) + math.log1p(math.abs(acc) + 1.0d)
          k += 1
        }
        (i % numKeys, acc)
      }
      .reduceByKey(_ + _)
      .count()
  }

  /**
   * Memory-heavy job: `groupByKey` forces per-key value materialization on the reduce side, the
   * classic memory-pressure shape that in a v2 deployment would exercise the spill path and, under
   * sustained pressure, the memory-pressure fallback condition. In v1 it runs entirely on the sort
   * path and must show no regression.
   */
  private def runMemoryBoundJob(
      sc: SparkContext, numRecords: Long, numPartitions: Int, numKeys: Long): Unit = {
    sc.parallelize(0L until numRecords, numPartitions)
      .map(i => (i % numKeys, i))
      .groupByKey()
      .mapValues(_.iterator.size.toLong)
      .count()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Stabilize the JVM once before any table is timed so the first (coldest) table is not skewed
    // by a warm-ordering artifact; each case is then additionally warmed by the framework.
    warmUpJvm()

    // A single runBenchmark block emits the shared banner ("Streaming Shuffle Performance
    // Benchmark") followed by the four workload tables, matching the committed results artifact.
    runBenchmark("Streaming Shuffle Performance Benchmark") {
      val shuffleHeavySmall = newBenchmark(
        "shuffle-heavy latency 100MB / 16 parts", SHUFFLE_HEAVY_SMALL_RECORDS)
      addSortAndStreamingCases(shuffleHeavySmall)(
        runShuffleHeavyJob(_, SHUFFLE_HEAVY_SMALL_RECORDS, 16, 1000L))
      shuffleHeavySmall.run()

      val shuffleHeavyLarge = newBenchmark(
        "shuffle-heavy latency 512MB / 64 parts", SHUFFLE_HEAVY_LARGE_RECORDS)
      addSortAndStreamingCases(shuffleHeavyLarge)(
        runShuffleHeavyJob(_, SHUFFLE_HEAVY_LARGE_RECORDS, 64, 4000L))
      shuffleHeavyLarge.run()

      val cpuBound = newBenchmark("CPU-bound workload latency", CPU_BOUND_RECORDS)
      addSortAndStreamingCases(cpuBound)(
        runCpuBoundJob(_, CPU_BOUND_RECORDS, 16, 100L))
      cpuBound.run()

      val memoryBound = newBenchmark("memory-bound workload (fallback)", MEMORY_BOUND_RECORDS)
      addSortAndStreamingCases(memoryBound)(
        runMemoryBoundJob(_, MEMORY_BOUND_RECORDS, 32, 1000L))
      memoryBound.run()
    }
  }
}
