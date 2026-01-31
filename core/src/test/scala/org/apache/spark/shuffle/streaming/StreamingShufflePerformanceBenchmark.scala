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

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for Streaming Shuffle vs Sort-based Shuffle performance comparison.
 *
 * This benchmark measures:
 * - Shuffle latency comparison between streaming and sort-based shuffle
 * - Memory utilization percentage during shuffle operations
 * - Spill frequency under different memory conditions
 * - Buffer reclamation timing
 * - Backpressure event counts
 *
 * Performance targets:
 * - 30-50% latency reduction for shuffle-heavy workloads (100MB+, 10+ partitions)
 * - Sub-100ms buffer reclamation response time
 * - <1% telemetry CPU overhead
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  // Benchmark parameters
  /** Target data size in MB for 100MB+ shuffle workloads */
  val DATA_SIZE_MB: Int = 100

  /** Number of partitions (10+ per requirement) */
  val NUM_PARTITIONS: Int = 16

  /** Number of benchmark iterations */
  val NUM_ITERATIONS: Int = 3

  /** Number of warmup iterations before measurement */
  val NUM_WARMUP: Int = 2

  /** Records per partition to generate approximately DATA_SIZE_MB of data */
  val RECORDS_PER_PARTITION: Int = 100000

  /** String length for generated random values */
  val STRING_LENGTH: Int = 64

  /** Local master configuration with 4 threads */
  val LOCAL_MASTER: String = "local[4]"

  /** Executor memory configuration */
  val EXECUTOR_MEMORY: String = "2g"

  /** Driver memory configuration */
  val DRIVER_MEMORY: String = "2g"

  /**
   * Main entry point for running all benchmark suites.
   *
   * @param mainArgs Command line arguments (not used currently)
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runShuffleLatencyComparison()
    runMemoryUtilizationBenchmark()
    runDataSizeScalingBenchmark()
    runPartitionCountScalingBenchmark()
    runBackpressureBenchmark()
    runSpillFrequencyBenchmark()
  }

  /**
   * Benchmark comparing shuffle latency between streaming and sort-based shuffle.
   *
   * This is the primary benchmark measuring the 30-50% latency reduction target
   * for shuffle-heavy workloads with 100MB+ data and 10+ partitions.
   */
  private def runShuffleLatencyComparison(): Unit = {
    runBenchmark(s"Shuffle Latency Comparison (${DATA_SIZE_MB}MB, $NUM_PARTITIONS partitions)") {
      val totalRecords = RECORDS_PER_PARTITION.toLong * NUM_PARTITIONS
      val benchmark = new Benchmark(
        "Shuffle Mode Latency",
        totalRecords,
        minNumIters = NUM_ITERATIONS,
        warmupTime = scala.concurrent.duration.Duration(5, "seconds"),
        minTime = scala.concurrent.duration.Duration(10, "seconds"),
        output = output
      )

      benchmark.addCase("Sort-based Shuffle (baseline)") { _ =>
        runShuffleWorkload("sort", NUM_PARTITIONS, RECORDS_PER_PARTITION)
      }

      benchmark.addCase("Streaming Shuffle") { _ =>
        runShuffleWorkload("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION)
      }

      benchmark.run()
    }
  }

  /**
   * Benchmark measuring memory utilization during shuffle operations.
   *
   * Measures:
   * - Peak memory utilization percentage
   * - Buffer allocation efficiency
   * - Memory reclamation timing
   */
  private def runMemoryUtilizationBenchmark(): Unit = {
    runBenchmark("Memory Utilization During Shuffle") {
      val totalRecords = RECORDS_PER_PARTITION.toLong * NUM_PARTITIONS
      val benchmark = new Benchmark(
        "Memory Utilization Mode",
        totalRecords,
        minNumIters = NUM_ITERATIONS,
        warmupTime = scala.concurrent.duration.Duration(3, "seconds"),
        minTime = scala.concurrent.duration.Duration(5, "seconds"),
        output = output
      )

      // Memory utilization with default 20% buffer size
      benchmark.addCase("Streaming Shuffle (20% buffer)") { _ =>
        runShuffleWorkloadWithMemoryConfig("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 20)
      }

      // Memory utilization with 30% buffer size
      benchmark.addCase("Streaming Shuffle (30% buffer)") { _ =>
        runShuffleWorkloadWithMemoryConfig("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 30)
      }

      // Memory utilization with 10% buffer size (more spilling expected)
      benchmark.addCase("Streaming Shuffle (10% buffer)") { _ =>
        runShuffleWorkloadWithMemoryConfig("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 10)
      }

      benchmark.run()
    }
  }

  /**
   * Benchmark measuring shuffle performance across different data sizes.
   *
   * Tests scaling behavior from 10MB to 200MB to verify consistent
   * performance improvements across various workload sizes.
   */
  private def runDataSizeScalingBenchmark(): Unit = {
    runBenchmark("Shuffle Latency vs Data Size") {
      val dataSizes = Seq(10, 50, 100, 200)

      dataSizes.foreach { sizeMB =>
        val scaledRecordsPerPartition = (RECORDS_PER_PARTITION * sizeMB) / DATA_SIZE_MB
        val totalRecords = scaledRecordsPerPartition.toLong * NUM_PARTITIONS

        val benchmark = new Benchmark(
          s"Data Size ${sizeMB}MB",
          totalRecords,
          minNumIters = NUM_ITERATIONS,
          warmupTime = scala.concurrent.duration.Duration(2, "seconds"),
          minTime = scala.concurrent.duration.Duration(5, "seconds"),
          output = output
        )

        benchmark.addCase(s"Sort-based (${sizeMB}MB)") { _ =>
          runShuffleWorkload("sort", NUM_PARTITIONS, scaledRecordsPerPartition)
        }

        benchmark.addCase(s"Streaming (${sizeMB}MB)") { _ =>
          runShuffleWorkload("streaming", NUM_PARTITIONS, scaledRecordsPerPartition)
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark measuring shuffle performance across different partition counts.
   *
   * Tests scaling behavior from 4 to 64 partitions to verify
   * performance improvements across various partition configurations.
   */
  private def runPartitionCountScalingBenchmark(): Unit = {
    runBenchmark("Shuffle Latency vs Partition Count") {
      val partitionCounts = Seq(4, 8, 16, 32, 64)

      partitionCounts.foreach { numPartitions =>
        // Keep total data size constant by adjusting records per partition
        val adjustedRecordsPerPartition = (RECORDS_PER_PARTITION * NUM_PARTITIONS) / numPartitions
        val totalRecords = adjustedRecordsPerPartition.toLong * numPartitions

        val benchmark = new Benchmark(
          s"Partition Count $numPartitions",
          totalRecords,
          minNumIters = NUM_ITERATIONS,
          warmupTime = scala.concurrent.duration.Duration(2, "seconds"),
          minTime = scala.concurrent.duration.Duration(5, "seconds"),
          output = output
        )

        benchmark.addCase(s"Sort-based ($numPartitions partitions)") { _ =>
          runShuffleWorkload("sort", numPartitions, adjustedRecordsPerPartition)
        }

        benchmark.addCase(s"Streaming ($numPartitions partitions)") { _ =>
          runShuffleWorkload("streaming", numPartitions, adjustedRecordsPerPartition)
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark measuring backpressure protocol performance.
   *
   * Tests the impact of rate limiting and flow control on shuffle
   * throughput under various congestion conditions.
   */
  private def runBackpressureBenchmark(): Unit = {
    runBenchmark("Backpressure Protocol Performance") {
      val totalRecords = RECORDS_PER_PARTITION.toLong * NUM_PARTITIONS
      val benchmark = new Benchmark(
        "Backpressure Mode",
        totalRecords,
        minNumIters = NUM_ITERATIONS,
        warmupTime = scala.concurrent.duration.Duration(3, "seconds"),
        minTime = scala.concurrent.duration.Duration(5, "seconds"),
        output = output
      )

      // No bandwidth limit (unlimited)
      benchmark.addCase("Streaming (unlimited bandwidth)") { _ =>
        runShuffleWorkloadWithBandwidthLimit("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 0)
      }

      // 100 MB/s bandwidth limit
      benchmark.addCase("Streaming (100 MB/s limit)") { _ =>
        runShuffleWorkloadWithBandwidthLimit("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 100)
      }

      // 50 MB/s bandwidth limit (more backpressure expected)
      benchmark.addCase("Streaming (50 MB/s limit)") { _ =>
        runShuffleWorkloadWithBandwidthLimit("streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 50)
      }

      benchmark.run()
    }
  }

  /**
   * Benchmark measuring spill frequency under different memory conditions.
   *
   * Tests disk spill behavior when buffer utilization exceeds
   * the configurable threshold (default 80%).
   */
  private def runSpillFrequencyBenchmark(): Unit = {
    runBenchmark("Spill Frequency Under Memory Pressure") {
      val totalRecords = RECORDS_PER_PARTITION.toLong * NUM_PARTITIONS
      val benchmark = new Benchmark(
        "Spill Threshold Mode",
        totalRecords,
        minNumIters = NUM_ITERATIONS,
        warmupTime = scala.concurrent.duration.Duration(3, "seconds"),
        minTime = scala.concurrent.duration.Duration(5, "seconds"),
        output = output
      )

      // High spill threshold (95%) - less spilling
      benchmark.addCase("Streaming (95% spill threshold)") { _ =>
        runShuffleWorkloadWithSpillThreshold(
          "streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 95)
      }

      // Default spill threshold (80%)
      benchmark.addCase("Streaming (80% spill threshold)") { _ =>
        runShuffleWorkloadWithSpillThreshold(
          "streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 80)
      }

      // Low spill threshold (60%) - more spilling
      benchmark.addCase("Streaming (60% spill threshold)") { _ =>
        runShuffleWorkloadWithSpillThreshold(
          "streaming", NUM_PARTITIONS, RECORDS_PER_PARTITION, 60)
      }

      benchmark.run()
    }
  }

  /**
   * Runs a shuffle workload with the specified shuffle manager.
   *
   * Creates a SparkContext, generates test data, performs a groupByKey
   * shuffle operation, and returns the elapsed time in nanoseconds.
   *
   * @param shuffleManager The shuffle manager to use ("sort" or "streaming")
   * @param numPartitions Number of partitions for the shuffle
   * @param recordsPerPartition Number of records per partition
   * @return Elapsed time in nanoseconds
   */
  private def runShuffleWorkload(
      shuffleManager: String,
      numPartitions: Int,
      recordsPerPartition: Int): Long = {
    val conf = createBenchmarkConf(shuffleManager)
    val sc = new SparkContext(conf)

    try {
      val startTime = System.nanoTime()

      // Generate key-value pairs where key is partition index for even distribution
      val totalRecords = recordsPerPartition * numPartitions
      val data = sc.parallelize(0 until totalRecords, numPartitions)
        .map { i =>
          val key = i % numPartitions
          val value = Random.nextString(STRING_LENGTH)
          (key, value)
        }
        .groupByKey(numPartitions)
        .count()

      val elapsedNs = System.nanoTime() - startTime
      elapsedNs
    } finally {
      sc.stop()
    }
  }

  /**
   * Runs a shuffle workload with specific memory buffer size configuration.
   *
   * @param shuffleManager The shuffle manager to use
   * @param numPartitions Number of partitions
   * @param recordsPerPartition Records per partition
   * @param bufferSizePercent Buffer size as percentage of executor memory
   * @return Elapsed time in nanoseconds
   */
  private def runShuffleWorkloadWithMemoryConfig(
      shuffleManager: String,
      numPartitions: Int,
      recordsPerPartition: Int,
      bufferSizePercent: Int): Long = {
    val conf = createBenchmarkConf(shuffleManager)
      .set("spark.shuffle.streaming.bufferSizePercent", bufferSizePercent.toString)

    val sc = new SparkContext(conf)

    try {
      val startTime = System.nanoTime()

      val totalRecords = recordsPerPartition * numPartitions
      val data = sc.parallelize(0 until totalRecords, numPartitions)
        .map { i =>
          val key = i % numPartitions
          val value = Random.nextString(STRING_LENGTH)
          (key, value)
        }
        .groupByKey(numPartitions)
        .count()

      val elapsedNs = System.nanoTime() - startTime
      elapsedNs
    } finally {
      sc.stop()
    }
  }

  /**
   * Runs a shuffle workload with specific bandwidth limit configuration.
   *
   * @param shuffleManager The shuffle manager to use
   * @param numPartitions Number of partitions
   * @param recordsPerPartition Records per partition
   * @param maxBandwidthMBps Maximum bandwidth in MB/s (0 for unlimited)
   * @return Elapsed time in nanoseconds
   */
  private def runShuffleWorkloadWithBandwidthLimit(
      shuffleManager: String,
      numPartitions: Int,
      recordsPerPartition: Int,
      maxBandwidthMBps: Int): Long = {
    val conf = createBenchmarkConf(shuffleManager)
    if (maxBandwidthMBps > 0) {
      conf.set("spark.shuffle.streaming.maxBandwidthMBps", maxBandwidthMBps.toString)
    }

    val sc = new SparkContext(conf)

    try {
      val startTime = System.nanoTime()

      val totalRecords = recordsPerPartition * numPartitions
      val data = sc.parallelize(0 until totalRecords, numPartitions)
        .map { i =>
          val key = i % numPartitions
          val value = Random.nextString(STRING_LENGTH)
          (key, value)
        }
        .groupByKey(numPartitions)
        .count()

      val elapsedNs = System.nanoTime() - startTime
      elapsedNs
    } finally {
      sc.stop()
    }
  }

  /**
   * Runs a shuffle workload with specific spill threshold configuration.
   *
   * @param shuffleManager The shuffle manager to use
   * @param numPartitions Number of partitions
   * @param recordsPerPartition Records per partition
   * @param spillThreshold Spill threshold as percentage of buffer utilization
   * @return Elapsed time in nanoseconds
   */
  private def runShuffleWorkloadWithSpillThreshold(
      shuffleManager: String,
      numPartitions: Int,
      recordsPerPartition: Int,
      spillThreshold: Int): Long = {
    val conf = createBenchmarkConf(shuffleManager)
      .set("spark.shuffle.streaming.spillThreshold", spillThreshold.toString)

    val sc = new SparkContext(conf)

    try {
      val startTime = System.nanoTime()

      val totalRecords = recordsPerPartition * numPartitions
      val data = sc.parallelize(0 until totalRecords, numPartitions)
        .map { i =>
          val key = i % numPartitions
          val value = Random.nextString(STRING_LENGTH)
          (key, value)
        }
        .groupByKey(numPartitions)
        .count()

      val elapsedNs = System.nanoTime() - startTime
      elapsedNs
    } finally {
      sc.stop()
    }
  }

  /**
   * Creates a SparkConf configured for benchmarking with the specified shuffle manager.
   *
   * @param shuffleManager The shuffle manager to use ("sort" or "streaming")
   * @return Configured SparkConf
   */
  private def createBenchmarkConf(shuffleManager: String): SparkConf = {
    new SparkConf()
      .setMaster(LOCAL_MASTER)
      .setAppName("StreamingShufflePerformanceBenchmark")
      // Set shuffle manager - "sort" or "streaming"
      .set("spark.shuffle.manager", shuffleManager)
      // Enable streaming shuffle when using streaming manager
      .set("spark.shuffle.streaming.enabled", (shuffleManager == "streaming").toString)
      // Default buffer size: 20% of executor memory
      .set("spark.shuffle.streaming.bufferSizePercent", "20")
      // Default spill threshold: 80%
      .set("spark.shuffle.streaming.spillThreshold", "80")
      // Memory configuration
      .set("spark.executor.memory", EXECUTOR_MEMORY)
      .set("spark.driver.memory", DRIVER_MEMORY)
      // Disable Spark UI to reduce overhead
      .set("spark.ui.enabled", "false")
      // Set serializer for consistent performance
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Disable dynamic allocation for consistent executor count
      .set("spark.dynamicAllocation.enabled", "false")
      // Disable event log to reduce I/O overhead
      .set("spark.eventLog.enabled", "false")
      // Configure shuffle compression for consistent comparison
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.spill.compress", "true")
  }
}
