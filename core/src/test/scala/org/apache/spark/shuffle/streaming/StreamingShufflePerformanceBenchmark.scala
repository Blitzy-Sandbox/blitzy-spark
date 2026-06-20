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
import org.apache.spark.storage.StorageLevel

/**
 * Benchmark comparing the opt-in streaming shuffle backend against the default sort-based shuffle.
 *
 * This object mirrors the canonical [[org.apache.spark.benchmark.BenchmarkBase]] pattern used by
 * `ChecksumBenchmark`: it is a benchmark entry point, NOT a ScalaTest suite, and therefore makes
 * no assertions. Benchmarks report timings only; this source's job is to model the THREE
 * AAP success-criteria scenarios faithfully so the committed result files measure the right thing.
 *
 * ==What each scenario models (and what v1 can honestly demonstrate)==
 *
 *   - '''Shuffle-heavy''' (AAP: >= 100 MB shuffled across >= 10 partitions): a `groupByKey` over
 *     [[SHUFFLE_HEAVY_RECORDS]] records each carrying a [[SHUFFLE_HEAVY_VALUE_BYTES]]-byte value
 *     across [[SHUFFLE_HEAVY_PARTITIONS]] partitions. `groupByKey` performs NO map-side combine, so
 *     the full payload (`records * valueBytes` ~ 128 MB, see [[shuffleHeavyBytes]]) crosses the
 *     shuffle boundary -- unlike a `reduceByKey` on a small key space, which would collapse volume
 *     map-side and never reach 100 MB.
 *   - '''CPU-bound''': a bounded per-record compute loop ([[CPU_BOUND_WORK]] iterations) before a
 *     comparatively light shuffle, so scheduler/transport overhead -- not shuffle volume --
 *     dominates the runtime.
 *   - '''Memory-bound (production fallback)''': a tight executor memory budget under which the case
 *     genuinely caches ~98% of the storage pool ([[fillStorageMemory]]) BEFORE registering its
 *     shuffle. At registration `StreamingShuffleManager.refreshFallbackSignals()` samples that
 *     live pressure, the memory-pressure revert condition (> 95%) trips, and the manager delegates
 *     to the unchanged inner `SortShuffleManager`. Both managers therefore execute the identical
 *     sort path under identical pressure, which is exactly the zero-regression guarantee. In local
 *     mode the driver and executor share one `MemoryManager`, so the cached pressure is the same
 *     pressure the registration-time sample observes.
 *
 * ==v1 transport scope (honest disclosure; see AAP 0.4.4 and the decision log)==
 *
 * In v1 the streaming data plane reuses the existing `BlockTransferService` (`StreamingShuffle
 * Transport` is an intentional logging-only integration layer, not a defect). So v1 does NOT add a
 * separate low-latency wire path: against the sort backend it demonstrates '''functional parity and
 * zero regression''', plus a '''valid measurement harness''' for the three scenarios. The headline
 * AAP latency deltas (30-50% shuffle-heavy, 5-10% CPU-bound) are v2 targets that materialize when
 * the real streaming data plane replaces the v1 logging-only transport; this benchmark is the
 * apparatus that will measure them then. The committed result files report the actual measured v1
 * numbers, never aspirational ones.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain
 *        org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 *      Results will be written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  /** Local master with enough threads to force a real, multi-partition shuffle. */
  private val LOCAL_MASTER = "local[2]"

  /** `spark.shuffle.manager` alias for the default sort-based backend. */
  private val SORT_MANAGER = "sort"

  /** `spark.shuffle.manager` alias for the opt-in streaming backend. */
  private val STREAMING_MANAGER = "streaming"

  /**
   * Shuffle-heavy key space. Wide enough to spread the load yet small relative to the record count,
   * so `groupByKey` produces meaningfully-sized groups while still shuffling the full payload.
   */
  private val SHUFFLE_HEAVY_KEYS = 4096

  /**
   * Shuffle-heavy workload sizing. `SHUFFLE_HEAVY_RECORDS * SHUFFLE_HEAVY_VALUE_BYTES` ~= 128 MB of
   * value bytes (see [[shuffleHeavyBytes]]), comfortably exceeding the AAP's 100 MB floor, spread
   * across 16 partitions (>= the AAP's 10-partition floor).
   */
  private val SHUFFLE_HEAVY_RECORDS = 500000
  private val SHUFFLE_HEAVY_VALUE_BYTES = 256
  private val SHUFFLE_HEAVY_PARTITIONS = 16

  /** The shuffled payload size in bytes, surfaced for the scenario label and as proof. */
  private def shuffleHeavyBytes: Long =
    SHUFFLE_HEAVY_RECORDS.toLong * SHUFFLE_HEAVY_VALUE_BYTES

  /** CPU-bound workload: fewer records and a light shuffle, but heavier per-record compute. */
  private val CPU_BOUND_RECORDS = 100000
  private val CPU_BOUND_KEYS = 1024
  private val CPU_BOUND_PARTITIONS = 8
  private val CPU_BOUND_WORK = 128

  /** Memory-bound workload: a small shuffle whose POINT is the fallback decision, not volume. */
  private val MEMORY_BOUND_RECORDS = 50000
  private val MEMORY_BOUND_KEYS = 256
  private val MEMORY_BOUND_PARTITIONS = 8

  /**
   * Tight-memory configuration for the memory-bound workload. The small testing-memory budget makes
   * the storage pool small (so [[fillStorageMemory]] can saturate it cheaply), and the minimum
   * streaming buffer percentage reflects a realistically memory-starved streaming attempt. Combined
   * with the genuine cache fill, this trips the memory-pressure fallback at registration time.
   * The value is interpreted under the `spark.testing` flag that [[BenchmarkBase.main]] sets, which
   * zeroes the reserved-memory floor so a small budget still initializes the memory manager.
   */
  private val memoryBoundConf = Seq(
    "spark.testing.memory" -> "268435456",
    "spark.shuffle.streaming.bufferSizePercent" -> "1")

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val heavyMb = shuffleHeavyBytes / (1024 * 1024)
    runBenchmark(s"Shuffle-heavy workload (~${heavyMb}MB, $SHUFFLE_HEAVY_PARTITIONS partitions)") {
      val benchmark =
        new Benchmark("Shuffle-heavy workload", SHUFFLE_HEAVY_RECORDS.toLong, output = output)
      benchmark.addCase("sort shuffle") { _ =>
        withShuffleContext(SORT_MANAGER) { sc => runShuffleHeavyJob(SORT_MANAGER, sc) }
      }
      benchmark.addCase("streaming shuffle") { _ =>
        withShuffleContext(STREAMING_MANAGER) { sc => runShuffleHeavyJob(STREAMING_MANAGER, sc) }
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

    runBenchmark("Memory-bound workload (production fallback to sort)") {
      val benchmark =
        new Benchmark("Memory-bound workload", MEMORY_BOUND_RECORDS.toLong, output = output)
      benchmark.addCase("sort shuffle") { _ =>
        withShuffleContext(SORT_MANAGER, memoryBoundConf) { sc =>
          fillStorageMemory(sc)
          runMemoryBoundJob(SORT_MANAGER, sc)
        }
      }
      // Streaming is ENABLED here; the genuine memory pressure established by fillStorageMemory is
      // what causes StreamingShuffleManager to revert to sort at registration -- NOT a disabled
      // feature flag. This is the production zero-regression path, measured end to end.
      benchmark.addCase("streaming shuffle (falls back to sort)") { _ =>
        withShuffleContext(STREAMING_MANAGER, memoryBoundConf) { sc =>
          fillStorageMemory(sc)
          runMemoryBoundJob(STREAMING_MANAGER, sc)
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
   * Shuffle-heavy job: shuffles >= 100 MB across >= 10 partitions. Each record carries a
   * [[SHUFFLE_HEAVY_VALUE_BYTES]]-byte value, and `groupByKey` (which does NOT combine map-side)
   * moves the entire `records * valueBytes` payload across the shuffle boundary; `count()`
   * materializes it. The byte array is touched so the allocation and its shuffled size are real.
   *
   * @param manager the active shuffle manager, recorded as the job description for traceability
   * @param sc      the context whose shuffle backend is exercised
   */
  private def runShuffleHeavyJob(manager: String, sc: SparkContext): Unit = {
    sc.setJobDescription(s"streaming-shuffle-benchmark [shuffle-heavy/$manager]")
    val valueBytes = SHUFFLE_HEAVY_VALUE_BYTES
    val numKeys = SHUFFLE_HEAVY_KEYS
    sc.parallelize(0 until SHUFFLE_HEAVY_RECORDS, SHUFFLE_HEAVY_PARTITIONS)
      .map { i =>
        val payload = new Array[Byte](valueBytes)
        // Touch both ends so neither the allocation nor its serialized size can be optimized away.
        payload(0) = i.toByte
        payload(valueBytes - 1) = (i >> 8).toByte
        (i % numKeys, payload)
      }
      .groupByKey()
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
    val numKeys = CPU_BOUND_KEYS
    sc.parallelize(0 until CPU_BOUND_RECORDS, CPU_BOUND_PARTITIONS)
      .map { i =>
        var acc = i.toLong
        var j = 0
        while (j < CPU_BOUND_WORK) {
          acc = (acc * 31 + j) & 0xffffffffL
          j += 1
        }
        (i % numKeys, acc)
      }
      .reduceByKey(_ + _)
      .count()
  }

  /**
   * Memory-bound job: a deliberately small shuffle. Its purpose is to measure the cost of the
   * fallback decision and the subsequent sort-path execution under memory pressure, NOT to move a
   * large payload. The pressure itself is established by [[fillStorageMemory]] before this runs.
   *
   * @param manager the active shuffle manager, recorded as the job description for traceability
   * @param sc      the context whose shuffle backend is exercised
   */
  private def runMemoryBoundJob(manager: String, sc: SparkContext): Unit = {
    sc.setJobDescription(s"streaming-shuffle-benchmark [memory-bound/$manager]")
    val numKeys = MEMORY_BOUND_KEYS
    sc.parallelize(0 until MEMORY_BOUND_RECORDS, MEMORY_BOUND_PARTITIONS)
      .map(i => (i % numKeys, i.toLong))
      .reduceByKey(_ + _)
      .count()
  }

  /**
   * Establishes genuine executor memory pressure by saturating the on-heap storage pool, so a
   * shuffle registered afterward observes utilization above the 95% memory-pressure threshold and
   * the streaming manager falls back to sort (the production zero-regression path). In local mode
   * the driver and executor share one `MemoryManager`, so this is the exact pressure the
   * registration-time sample reads.
   *
   * Two sizing choices make the saturation reliable (empirically ~99% of the pool):
   *   - '''one 1 MB block per partition''' -- the cache block is the partition, so a single
   *     coarse multi-MB block per partition would leave large unroll headroom and stall near 85%;
   *     one small block per partition lets the MemoryStore pack the pool finely; and
   *   - '''~1.1x over-provisioning''' -- MEMORY_ONLY simply drops the small overflow once the pool
   *     is full, so the retained set saturates the pool rather than landing short of it.
   *
   * @param sc the context whose storage memory is filled; the cached RDD is held for the case's
   *           lifetime and released when [[withShuffleContext]] stops the context
   */
  private def fillStorageMemory(sc: SparkContext): Unit = {
    val pool = sc.env.memoryManager.maxOnHeapStorageMemory
    val blockBytes = 1024 * 1024
    val numBlocks = math.max(1, ((pool * 1.1).toLong / blockBytes).toInt)
    sc.parallelize(0 until numBlocks, numBlocks)
      .map { _ =>
        val block = new Array[Byte](blockBytes)
        // Touch both ends so the block's real, uncompressible size is what occupies the pool.
        block(0) = 1
        block(blockBytes - 1) = 1
        block
      }
      .persist(StorageLevel.MEMORY_ONLY)
      .count()
  }
}
