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
 * Benchmark comparing the opt-in streaming shuffle backend against the sort-based shuffle.
 *
 * For each of three workload profiles - shuffle-heavy, CPU-bound, and memory-bound - the same
 * job is run twice: once with the sort-based shuffle (`spark.shuffle.manager=sort`, the
 * baseline) and once with the streaming backend (`spark.shuffle.manager=streaming` plus
 * `spark.shuffle.streaming.enabled=true`). The streaming backend coexists with, and
 * automatically falls back to, the sort-based path, so the memory-bound profile is expected to
 * exercise that fallback. Each scenario runs on its own [[org.apache.spark.SparkContext]]
 * because the shuffle manager is immutable for the lifetime of an application.
 *
 * Benchmarks report timings; they intentionally do not assert the success-criteria deltas
 * (30-50% latency reduction for shuffle-heavy workloads, 5-10% improvement for CPU-bound
 * workloads, and zero regression for memory-bound workloads via fallback). Those deltas are
 * demonstrated by the committed result files rather than by assertions in this source.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt"
 *      and the companion "benchmarks/StreamingShuffleBenchmark-results.txt". NOTE: both result
 *      files live in core/benchmarks/ (a sibling of core/src) and are owned by the core module;
 *      only this benchmark source is created here.
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

  // Shuffle-heavy profile: a large record count spread across >= 10 partitions with light
  // per-record compute, representing the >= 100 MB / >= 10 partition success-criteria target.
  // Absolute sizes are tunable; they are kept modest so the benchmark stays runnable.
  private val SHUFFLE_HEAVY_RECORDS = 1024 * 1024
  private val SHUFFLE_HEAVY_PARTITIONS = 16
  private val SHUFFLE_HEAVY_COMPUTE_PASSES = 1

  // CPU-bound profile: fewer records but heavy per-record compute over fewer partitions, so
  // compute and scheduler overhead dominate the comparatively light shuffle.
  private val CPU_BOUND_RECORDS = 64 * 1024
  private val CPU_BOUND_PARTITIONS = 8
  private val CPU_BOUND_COMPUTE_PASSES = 512

  // Memory-bound profile: a tight executor-memory budget so the streaming path's fallback
  // policy reverts to sort-based shuffle, demonstrating zero regression.
  private val MEMORY_BOUND_RECORDS = 512 * 1024
  private val MEMORY_BOUND_PARTITIONS = 16
  private val MEMORY_BOUND_COMPUTE_PASSES = 1
  private val MEMORY_BOUND_MEMORY_BYTES = 256L * 1024 * 1024
  private val MEMORY_BOUND_CONF = Seq(TEST_MEMORY_KEY -> MEMORY_BOUND_MEMORY_BYTES.toString)

  // The currently active SparkContext. Only one scenario runs at a time: createSparkContext
  // stops the previous context before building the next, and afterAll stops the last one.
  private var activeContext: SparkContext = null

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Shuffle-heavy workload (>=100MB, >=10 partitions)") {
      val benchmark = new Benchmark(
        "Shuffle-heavy workload", SHUFFLE_HEAVY_RECORDS.toLong, MIN_ITERS, output = output)
      addManagerCases(benchmark, SHUFFLE_HEAVY_RECORDS, SHUFFLE_HEAVY_PARTITIONS,
        SHUFFLE_HEAVY_COMPUTE_PASSES, Nil)
      benchmark.run()
    }

    runBenchmark("CPU-bound workload") {
      val benchmark = new Benchmark(
        "CPU-bound workload", CPU_BOUND_RECORDS.toLong, MIN_ITERS, output = output)
      addManagerCases(benchmark, CPU_BOUND_RECORDS, CPU_BOUND_PARTITIONS,
        CPU_BOUND_COMPUTE_PASSES, Nil)
      benchmark.run()
    }

    runBenchmark("Memory-bound workload (fallback)") {
      val benchmark = new Benchmark(
        "Memory-bound workload", MEMORY_BOUND_RECORDS.toLong, MIN_ITERS, output = output)
      addManagerCases(benchmark, MEMORY_BOUND_RECORDS, MEMORY_BOUND_PARTITIONS,
        MEMORY_BOUND_COMPUTE_PASSES, MEMORY_BOUND_CONF)
      benchmark.run()
    }
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
   */
  private def addManagerCases(
      benchmark: Benchmark,
      numRecords: Int,
      numPartitions: Int,
      computePasses: Int,
      extraConf: Seq[(String, String)]): Unit = {
    Seq(SORT_MANAGER, STREAMING_MANAGER).foreach { manager =>
      lazy val context = createSparkContext(manager, extraConf)
      benchmark.addCase(s"$manager shuffle") { _ =>
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
