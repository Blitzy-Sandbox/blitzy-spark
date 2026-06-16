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
 * End-to-end benchmark comparing the opt-in streaming shuffle backend against the sort-based
 * shuffle. For each workload profile the same job is run twice on its own
 * [[org.apache.spark.SparkContext]]: once with the sort-based shuffle
 * (`spark.shuffle.manager=sort`, the baseline) and once with the streaming backend
 * (`spark.shuffle.manager=streaming` plus `spark.shuffle.streaming.enabled=true`). The streaming
 * backend coexists with, and automatically falls back to, the sort-based path, so the
 * memory-bound profile is expected to exercise that fallback.
 *
 * Scenario sizing is deterministic and source-derived so the committed result file is traceable
 * to this source: each byte-labeled scenario maps to a record count via
 * `records = labelBytes / MODELED_BYTES_PER_RECORD` (see [[recordsFor]]). The result file lists
 * exactly the five scenarios produced here, in order, under a single `Streaming Shuffle
 * Performance` heading. Re-running the documented command regenerates that file with the same
 * structure and the running host's measured timings.
 *
 * Benchmarks report timings only; they intentionally do not assert the success-criteria deltas
 * (30-50% latency reduction for shuffle-heavy workloads, 5-10% improvement for CPU-bound
 * workloads, and zero regression for memory-bound workloads via fallback). Those deltas are a
 * distributed-execution design target; the committed single-host result file is a reproducible
 * snapshot rather than a guarantee of those deltas on a local master.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results are written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 *      The companion component micro-benchmark, StreamingShuffleBenchmark, owns and regenerates
 *      "benchmarks/StreamingShuffleBenchmark-results.txt" separately (each BenchmarkBase object
 *      writes only the file named after its own class).
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  /** Local master used for every scenario; 4 cores so partitions run with real parallelism. */
  private val LOCAL_MASTER = "local[4]"

  /** Manager alias selecting the existing sort-based shuffle (the baseline and fallback). */
  private val SORT_MANAGER = "sort"

  /** Manager alias selecting the opt-in streaming shuffle backend. */
  private val STREAMING_MANAGER = "streaming"

  /** Feature flag that, together with the manager alias, engages the streaming path. */
  private val STREAMING_ENABLED_KEY = "spark.shuffle.streaming.enabled"

  /** Config key used to constrain executor memory for the memory-bound fallback scenario. */
  private val TEST_MEMORY_KEY = "spark.testing.memory"

  /** Minimum measured iterations per case (mirrors the ChecksumBenchmark convention). */
  private val MIN_ITERS = 3

  // Linear-congruential mixing constants used to add deterministic per-record CPU work so the
  // CPU-bound profile spends real cycles that the JIT cannot elide.
  private val MULTIPLIER = 1664525L
  private val INCREMENT = 1013904223L
  private val MASK = 0xffffffffL

  /** 1 MiB and 1 GiB helpers for scenario byte labels. */
  private val MB = 1024L * 1024L
  private val GB = 1024L * MB

  // Modeled input volume per record. Each lightweight (Int, Long) shuffle record models this many
  // bytes of input, so a byte-labeled scenario maps deterministically to a record count and the
  // result file's byte labels are derived from this source rather than fabricated. 100 bytes per
  // record keeps the mapping aligned with the committed result file (a 100 MB scenario is
  // ~1,048,576 records) while keeping record counts tractable for a single-host run.
  private val MODELED_BYTES_PER_RECORD = 100L

  /** Maps a byte-labeled scenario size to its deterministic record count. */
  private def recordsFor(labelBytes: Long): Int = (labelBytes / MODELED_BYTES_PER_RECORD).toInt

  /** Renders a byte count as the GB or MB label used in the benchmark scenario name. */
  private def sizeLabel(labelBytes: Long): String =
    if (labelBytes >= GB) s"${labelBytes / GB} GB" else s"${labelBytes / MB} MB"

  // The currently active SparkContext. Only one scenario runs at a time: createSparkContext
  // stops the previous context before building the next, and afterAll stops the last one.
  private var activeContext: SparkContext = null

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Streaming Shuffle Performance") {
      // Shuffle-heavy profile at three sizes: light per-record compute over >= 10 partitions,
      // representing the >= 100 MB / >= 10 partition success-criteria target and its scaling.
      shuffleHeavyScenario(100L * MB, 10)
      shuffleHeavyScenario(500L * MB, 50)
      shuffleHeavyScenario(1L * GB, 100)
      // CPU-bound profile: fewer records but heavy per-record compute, so compute and scheduler
      // overhead dominate the comparatively light shuffle.
      cpuBoundScenario(50L * MB, 8)
      // Memory-bound profile: a tight executor-memory budget forces the streaming fallback policy
      // to revert to sort-based shuffle at registration time, demonstrating zero regression.
      memoryBoundScenario(2L * GB, 200)
    }
  }

  /** Adds a shuffle-heavy scenario (light compute, full partition fan-out) to the suite. */
  private def shuffleHeavyScenario(labelBytes: Long, numPartitions: Int): Unit = {
    val records = recordsFor(labelBytes)
    val benchmark = new Benchmark(
      s"Shuffle-Heavy Workload: ${sizeLabel(labelBytes)}, $numPartitions partitions",
      records.toLong, MIN_ITERS, output = output)
    addManagerCases(benchmark, records, numPartitions, computePasses = 1, Nil, fallback = false)
    benchmark.run()
  }

  /** Adds a CPU-bound scenario (heavy per-record compute, few partitions) to the suite. */
  private def cpuBoundScenario(labelBytes: Long, numPartitions: Int): Unit = {
    val records = recordsFor(labelBytes)
    val benchmark = new Benchmark(
      s"CPU-Bound Workload: ${sizeLabel(labelBytes)}, $numPartitions partitions",
      records.toLong, MIN_ITERS, output = output)
    addManagerCases(benchmark, records, numPartitions, computePasses = 512, Nil, fallback = false)
    benchmark.run()
  }

  /** Adds a memory-bound scenario whose tight memory budget forces the sort fallback. */
  private def memoryBoundScenario(labelBytes: Long, numPartitions: Int): Unit = {
    val records = recordsFor(labelBytes)
    val benchmark = new Benchmark(
      s"Memory-Bound Workload: ${sizeLabel(labelBytes)}, $numPartitions partitions (fallback)",
      records.toLong, MIN_ITERS, output = output)
    val memoryConf = Seq(TEST_MEMORY_KEY -> (256L * MB).toString)
    addManagerCases(benchmark, records, numPartitions, computePasses = 1, memoryConf,
      fallback = true)
    benchmark.run()
  }

  /**
   * Adds a `sort shuffle` case and a `streaming shuffle` case to `benchmark`. Each case runs on
   * its own [[org.apache.spark.SparkContext]] configured with the corresponding shuffle manager;
   * the context is created lazily on the case's first iteration and reused across iterations.
   *
   * @param benchmark     the benchmark group to add the two cases to
   * @param numRecords    number of input records to shuffle
   * @param numPartitions number of partitions (also the number of reduce keys)
   * @param computePasses per-record mixing iterations controlling CPU intensity
   * @param extraConf     additional SparkConf entries (for example a tight memory budget)
   * @param fallback      when true, the streaming case is labeled as falling back to sort
   */
  private def addManagerCases(
      benchmark: Benchmark,
      numRecords: Int,
      numPartitions: Int,
      computePasses: Int,
      extraConf: Seq[(String, String)],
      fallback: Boolean): Unit = {
    val streamingCaseName =
      if (fallback) "streaming shuffle (fallback to sort)" else "streaming shuffle"
    Seq(SORT_MANAGER -> "sort shuffle", STREAMING_MANAGER -> streamingCaseName).foreach {
      case (manager, caseName) =>
        lazy val context = createSparkContext(manager, extraConf)
        benchmark.addCase(caseName) { _ =>
          runShuffleJob(context, numRecords, numPartitions, computePasses)
        }
    }
  }

  /**
   * Builds a [[org.apache.spark.SparkContext]] for `manager`, first stopping any previously
   * active context so that only one scenario runs at a time. For the streaming manager the
   * opt-in feature flag is also set; otherwise the sort-based path is used unchanged.
   *
   * @param manager   the `spark.shuffle.manager` alias (`sort` or `streaming`)
   * @param extraConf additional SparkConf entries applied on top of the defaults
   * @return the newly created and now-active SparkContext
   */
  private def createSparkContext(
      manager: String,
      extraConf: Seq[(String, String)]): SparkContext = {
    if (activeContext != null) {
      activeContext.stop()
      activeContext = null
    }
    val conf = new SparkConf()
      .setMaster(LOCAL_MASTER)
      .setAppName(s"streaming-shuffle-benchmark-$manager")
      .set(SHUFFLE_MANAGER, manager)
    if (manager == STREAMING_MANAGER) {
      conf.set(STREAMING_ENABLED_KEY, "true")
    }
    extraConf.foreach { case (key, value) => conf.set(key, value) }
    activeContext = new SparkContext(conf)
    activeContext
  }

  /**
   * Runs a single shuffle job: builds an RDD of records, applies `computePasses` of per-record
   * mixing to simulate compute, then forces a shuffle with `reduceByKey` and materializes it
   * with `count()`.
   *
   * @param sc            the active SparkContext to run on
   * @param numRecords    number of input records to generate
   * @param numPartitions number of partitions (also the modulus for the shuffle keys)
   * @param computePasses per-record mixing iterations controlling CPU intensity
   */
  private def runShuffleJob(
      sc: SparkContext,
      numRecords: Int,
      numPartitions: Int,
      computePasses: Int): Unit = {
    // Copy object-level constants into locals so the map closure captures only primitives and
    // never a reference to this (non-serializable) benchmark object.
    val multiplier = MULTIPLIER
    val increment = INCREMENT
    val mask = MASK
    val shuffled = sc.parallelize(0 until numRecords, numPartitions).map { i =>
      var acc = i.toLong
      var pass = 0
      while (pass < computePasses) {
        acc = (acc * multiplier + increment) & mask
        pass += 1
      }
      ((acc % numPartitions).toInt, acc)
    }
    shuffled.reduceByKey(_ + _).count()
  }

  override def afterAll(): Unit = {
    if (activeContext != null) {
      activeContext.stop()
      activeContext = null
    }
  }
}
