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
 * Latency benchmark for the opt-in streaming shuffle backend, comparing the production-stable
 * sort-based shuffle (baseline) against the streaming shuffle manager across three shuffle sizes
 * (100MB/16, 256MB/32, 512MB/64 partition regimes).
 *
 * ==Relationship to [[StreamingShufflePerformanceBenchmark]]==
 * That sibling benchmark spans four heterogeneous workload regimes (two shuffle sizes plus a
 * CPU-bound and a memory-bound job). This one focuses narrowly on shuffle latency as a function of
 * data volume and partition count, holding the workload shape (`reduceByKey`) fixed. Both write
 * regenerable results artifacts under `core/benchmarks/`.
 *
 * ==What this proves (and what it deliberately does not)==
 * The streaming transport is a v1 logging-only stub
 * ([[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport.isWireTransferAvailable]]
 * `== false`), so `StreamingShuffleManager` routes every production shuffle through its inner
 * `SortShuffleManager`. The "streaming shuffle" case therefore executes the identical sort code
 * path as the baseline, and the acceptance target is '''zero performance regression via automatic
 * fallback''' (streaming ~ 1.0X sort) rather than the 30-50% latency reduction earmarked for the v2
 * wire path.
 *
 * ==Timing methodology==
 * Each case uses [[Benchmark.addTimerCase]] to time ONLY the shuffle job; the per-iteration
 * `SparkContext` construction and teardown happen outside the timed region. The framework's
 * per-case warmup drives each case to JIT/heap steady state before measurement, and the `Relative`
 * column is computed from best (minimum) times -- so identical code paths (the v1 sort fallback)
 * converge to ~1.0X rather than reporting a warm-ordering artifact.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt \
 *      "core/Test/runMain org.apache.spark.shuffle.streaming.StreamingShuffleBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *      "core/Test/runMain org.apache.spark.shuffle.streaming.StreamingShuffleBenchmark"
 *      Results will be written to "benchmarks/StreamingShuffleBenchmark-results.txt".
 * }}}
 */
object StreamingShuffleBenchmark extends BenchmarkBase {

  // Fixed iteration count per case: bounded so a full three-table run completes in a few minutes on
  // a developer machine, while still averaging over multiple shuffles for a stable best/avg/stdev.
  private val NUM_ITERS: Int = 3

  // Record counts per size regime, scaled with the 100MB/256MB/512MB labels but kept bounded
  // (millions, not billions) so the benchmark finishes quickly.
  private val SMALL_RECORDS: Long = 4L * 1000 * 1000
  private val MEDIUM_RECORDS: Long = 6L * 1000 * 1000
  private val LARGE_RECORDS: Long = 8L * 1000 * 1000

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
      .setAppName("StreamingShuffleBenchmark")
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
        runShuffleJob(sc, WARMUP_RECORDS, 16, 1000L)
        w += 1
      }
    } finally {
      sc.stop()
    }
  }

  /**
   * The fixed workload for every size regime: `reduceByKey` over `numRecords` records forced to
   * materialize with `count()`. The identical body runs for both the sort and streaming cases so
   * the only variable is the configured shuffle manager.
   */
  private def runShuffleJob(
      sc: SparkContext, numRecords: Long, numPartitions: Int, numKeys: Long): Unit = {
    sc.parallelize(0L until numRecords, numPartitions)
      .map(i => (i % numKeys, i))
      .reduceByKey(_ + _)
      .count()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Stabilize the JVM once before any table is timed so the first (coldest) table is not skewed
    // by a warm-ordering artifact; each case is then additionally warmed by the framework.
    warmUpJvm()

    // A single runBenchmark block emits the shared banner ("Streaming Shuffle Latency Benchmark")
    // followed by the three size tables, matching the committed results artifact.
    runBenchmark("Streaming Shuffle Latency Benchmark") {
      val small = newBenchmark("shuffle latency 100MB / 16 partitions", SMALL_RECORDS)
      addSortAndStreamingCases(small)(runShuffleJob(_, SMALL_RECORDS, 16, 1000L))
      small.run()

      val medium = newBenchmark("shuffle latency 256MB / 32 partitions", MEDIUM_RECORDS)
      addSortAndStreamingCases(medium)(runShuffleJob(_, MEDIUM_RECORDS, 32, 2000L))
      medium.run()

      val large = newBenchmark("shuffle latency 512MB / 64 partitions", LARGE_RECORDS)
      addSortAndStreamingCases(large)(runShuffleJob(_, LARGE_RECORDS, 64, 4000L))
      large.run()
    }
  }
}
