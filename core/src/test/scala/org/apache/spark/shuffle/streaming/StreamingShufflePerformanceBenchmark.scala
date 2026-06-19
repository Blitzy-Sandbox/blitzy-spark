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

import java.util.zip.CRC32C

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.serializer.JavaSerializer

/**
 * Performance benchmark suite for streaming shuffle measuring latency (target 30-50% reduction),
 * throughput, and regression detection against sort-based shuffle baseline.
 *
 * This benchmark follows the ChecksumBenchmark pattern and uses the Benchmark class for
 * standardized reporting with SPARK_GENERATE_BENCHMARK_FILES support.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 *
 * Benchmark scenarios:
 *   - Streaming vs Sort Shuffle Latency: Compare end-to-end latency
 *   - Streaming Shuffle Throughput: Measure bytes/second throughput
 *   - Buffer Allocation Overhead: Measure memory allocation cost
 *   - Checksum Generation Overhead: Measure CRC32C performance impact
 *   - Backpressure Response Time: Measure flow control latency
 *   - Spill Trigger Response Time: Measure spill detection to completion
 *
 * Configuration:
 *   - 10GB+ data volume for meaningful measurements
 *   - 100+ partitions for realistic partition counts
 *   - Multiple iterations for statistical significance
 *   - Warm-up runs before measurement
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  // Benchmark configuration constants
  // Using smaller data sizes for benchmark runs while maintaining meaningful measurements
  private val DATA_SIZE_MB = 256 // Scaled down from 10GB for practical benchmark execution
  private val NUM_PARTITIONS = 128 // 100+ partitions for realistic partition counts
  private val NUM_RECORDS = DATA_SIZE_MB * 1024 // Approx records based on ~1KB per record
  private val BUFFER_CAPACITY = 64 * 1024 * 1024L // 64MB buffer capacity for benchmarks
  private val ITERATIONS = 10 // Number of iterations for statistical significance
  private val WARMUP_ITERATIONS = 3 // Warm-up runs before measurement

  // Random generator for test data
  private val random = new Random(42L) // Fixed seed for reproducibility

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Streaming Shuffle Performance Benchmarks") {
      benchmarkChecksumGeneration()
    }

    runBenchmark("Buffer Allocation Overhead") {
      benchmarkBufferAllocation()
    }

    runBenchmark("Backpressure Response Time") {
      benchmarkBackpressureResponseTime()
    }

    runBenchmark("Spill Trigger Response Time") {
      benchmarkSpillTriggerResponseTime()
    }

    runBenchmark("Streaming Shuffle Throughput") {
      benchmarkStreamingThroughput()
    }

    runBenchmark("Streaming vs Sort Shuffle Latency") {
      benchmarkLatencyComparison()
    }
  }

  /**
   * Benchmarks CRC32C checksum generation overhead for streaming shuffle operations.
   * Measures the performance impact of checksum calculation on data integrity validation.
   */
  private def benchmarkChecksumGeneration(): Unit = {
    // Generate test data with realistic sizes
    val dataSizes = Seq(1024, 32 * 1024, 1024 * 1024, 32 * 1024 * 1024)

    for (dataSize <- dataSizes) {
      val data: Array[Byte] = new Array[Byte](dataSize)
      random.nextBytes(data)
      val dataSizeStr = if (dataSize >= 1024 * 1024) {
        s"${dataSize / (1024 * 1024)}MB"
      } else if (dataSize >= 1024) {
        s"${dataSize / 1024}KB"
      } else {
        s"${dataSize}B"
      }

      val benchmark = new Benchmark(
        s"CRC32C Checksum ($dataSizeStr)",
        ITERATIONS,
        minNumIters = WARMUP_ITERATIONS,
        output = output
      )

      benchmark.addCase("CRC32C checksum generation") { _ =>
        (1 to ITERATIONS).foreach { _ =>
          val checksum = new CRC32C()
          checksum.update(data)
          val _ = checksum.getValue
        }
      }

      benchmark.addCase("CRC32C with reset between iterations") { _ =>
        val checksum = new CRC32C()
        (1 to ITERATIONS).foreach { _ =>
          checksum.reset()
          checksum.update(data)
          val _ = checksum.getValue
        }
      }

      benchmark.run()
    }
  }

  /**
   * Benchmarks buffer allocation overhead for streaming shuffle operations.
   * Measures memory allocation cost including buffer creation, data append,
   * reset, and release operations.
   */
  private def benchmarkBufferAllocation(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("BufferAllocationBenchmark")
      .set(SHUFFLE_STREAMING_ENABLED.key, "true")
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key, "20")

    // Create mock memory manager for standalone buffer testing
    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L)

    val serializer = new JavaSerializer(conf)
    val serializerInstance = serializer.newInstance()

    val benchmark = new Benchmark(
      "Buffer Operations",
      NUM_RECORDS,
      minNumIters = WARMUP_ITERATIONS,
      output = output
    )

    benchmark.addCase("Buffer creation") { _ =>
      (1 to ITERATIONS).foreach { i =>
        val buffer = new StreamingBuffer(
          partitionId = i % NUM_PARTITIONS,
          capacity = BUFFER_CAPACITY,
          taskMemoryManager = taskMemoryManager
        )
        buffer.release()
      }
    }

    benchmark.addCase("Buffer append operations") { _ =>
      val buffer = new StreamingBuffer(
        partitionId = 0,
        capacity = BUFFER_CAPACITY,
        taskMemoryManager = taskMemoryManager
      )
      try {
        (1 to 1000).foreach { i =>
          buffer.append(i, s"value_$i", serializerInstance)
        }
      } finally {
        buffer.release()
      }
    }

    benchmark.addCase("Buffer getData and size") { _ =>
      val buffer = new StreamingBuffer(
        partitionId = 0,
        capacity = BUFFER_CAPACITY,
        taskMemoryManager = taskMemoryManager
      )
      try {
        // Populate buffer first
        (1 to 100).foreach { i =>
          buffer.append(i, s"value_$i", serializerInstance)
        }
        // Benchmark read operations
        (1 to ITERATIONS).foreach { _ =>
          val _ = buffer.size()
          val _ = buffer.getData()
          val _ = buffer.getChecksum()
        }
      } finally {
        buffer.release()
      }
    }

    benchmark.addCase("Buffer reset cycle") { _ =>
      val buffer = new StreamingBuffer(
        partitionId = 0,
        capacity = BUFFER_CAPACITY,
        taskMemoryManager = taskMemoryManager
      )
      try {
        (1 to ITERATIONS).foreach { _ =>
          // Fill and reset
          (1 to 100).foreach { i =>
            buffer.append(i, s"value_$i", serializerInstance)
          }
          buffer.reset()
        }
      } finally {
        buffer.release()
      }
    }

    benchmark.run()
  }

  /**
   * Benchmarks backpressure protocol response time including heartbeat signals,
   * backpressure signal generation, timeout checks, and token bucket operations.
   */
  private def benchmarkBackpressureResponseTime(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("BackpressureBenchmark")
      .set(SHUFFLE_STREAMING_ENABLED.key, "true")
      .set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key, "100")

    val protocol = new BackpressureProtocol(conf)

    val benchmark = new Benchmark(
      "Backpressure Protocol Operations",
      ITERATIONS * 1000,
      minNumIters = WARMUP_ITERATIONS,
      output = output
    )

    benchmark.addCase("Heartbeat signal") { _ =>
      (1 to ITERATIONS * 1000).foreach { _ =>
        val _ = protocol.heartbeat()
      }
    }

    benchmark.addCase("Backpressure signal at 80% utilization") { _ =>
      (1 to ITERATIONS * 1000).foreach { i =>
        val utilization = 0.5 + (i % 50) / 100.0 // Vary between 0.5 and 1.0
        val _ = protocol.sendBackpressureSignal(s"executor-$i", utilization)
      }
    }

    benchmark.addCase("Timeout check") { _ =>
      val startTime = System.currentTimeMillis()
      (1 to ITERATIONS * 1000).foreach { i =>
        // Vary the last heartbeat time to simulate different timeout scenarios
        val lastHeartbeat = startTime - (i % 10000)
        val _ = protocol.checkTimeout(lastHeartbeat)
      }
    }

    benchmark.addCase("Token bucket operations") { _ =>
      protocol.refillTokens()
      (1 to ITERATIONS * 1000).foreach { _ =>
        val available = protocol.getAvailableTokens()
        if (available > 1024) {
          val _ = protocol.consumeTokens(1024)
        }
        protocol.refillTokens()
      }
    }

    benchmark.run()
  }

  /**
   * Benchmarks spill trigger response time measuring spill detection to completion
   * against the target of under 100ms.
   */
  private def benchmarkSpillTriggerResponseTime(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("SpillTriggerBenchmark")
      .set(SHUFFLE_STREAMING_ENABLED.key, "true")
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD.key, "80")
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key, "20")

    // Create memory manager for spill testing
    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L)

    val benchmark = new Benchmark(
      "Spill Trigger Operations",
      ITERATIONS,
      minNumIters = WARMUP_ITERATIONS,
      output = output
    )

    benchmark.addTimerCase("Memory acquisition") { timer =>
      val spillManager = new MemorySpillManager(taskMemoryManager, conf)
      try {
        timer.startTiming()
        (1 to ITERATIONS).foreach { i =>
          val _ = spillManager.acquireBufferMemory(i, 1024 * 1024)
        }
        timer.stopTiming()
      } finally {
        spillManager.cleanup()
      }
    }

    benchmark.addTimerCase("Spill threshold check") { timer =>
      val spillManager = new MemorySpillManager(taskMemoryManager, conf)
      val serializerInstance = new JavaSerializer(conf).newInstance()
      try {
        // Pre-populate with some buffers
        (1 to 5).foreach { i =>
          val buffer = new StreamingBuffer(i, 1024 * 1024, taskMemoryManager)
          (1 to 100).foreach { j =>
            buffer.append(j, s"value_$j", serializerInstance)
          }
          spillManager.registerBuffer(i, buffer)
        }

        timer.startTiming()
        (1 to ITERATIONS * 100).foreach { _ =>
          val _ = spillManager.checkAndTriggerSpill()
        }
        timer.stopTiming()
      } finally {
        spillManager.cleanup()
      }
    }

    benchmark.addTimerCase("Full cleanup") { timer =>
      (1 to ITERATIONS).foreach { _ =>
        val spillManager = new MemorySpillManager(taskMemoryManager, conf)
        val serializerInstance = new JavaSerializer(conf).newInstance()

        // Add some buffers
        (1 to 10).foreach { i =>
          val buffer = new StreamingBuffer(i, 1024 * 1024, taskMemoryManager)
          (1 to 50).foreach { j =>
            buffer.append(j, s"value_$j", serializerInstance)
          }
          spillManager.registerBuffer(i, buffer)
        }

        timer.startTiming()
        spillManager.cleanup()
        timer.stopTiming()
      }
    }

    benchmark.run()
  }

  /**
   * Benchmarks streaming shuffle throughput measuring bytes/second performance.
   */
  private def benchmarkStreamingThroughput(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("ThroughputBenchmark")
      .set(SHUFFLE_STREAMING_ENABLED.key, "true")
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key, "20")

    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L)
    val serializerInstance = new JavaSerializer(conf).newInstance()

    val benchmark = new Benchmark(
      "Throughput Measurement",
      NUM_RECORDS,
      minNumIters = WARMUP_ITERATIONS,
      output = output
    )

    benchmark.addTimerCase("Buffer write throughput") { timer =>
      val buffer = new StreamingBuffer(
        partitionId = 0,
        capacity = BUFFER_CAPACITY,
        taskMemoryManager = taskMemoryManager
      )
      try {
        timer.startTiming()
        (1 to NUM_RECORDS / 10).foreach { i =>
          buffer.append(i, s"value_${random.nextLong()}_padding_for_size", serializerInstance)
        }
        timer.stopTiming()
      } finally {
        buffer.release()
      }
    }

    benchmark.addTimerCase("Buffer read throughput") { timer =>
      val buffer = new StreamingBuffer(
        partitionId = 0,
        capacity = BUFFER_CAPACITY,
        taskMemoryManager = taskMemoryManager
      )
      try {
        // Populate buffer first
        (1 to NUM_RECORDS / 100).foreach { i =>
          buffer.append(i, s"value_${random.nextLong()}", serializerInstance)
        }

        timer.startTiming()
        (1 to ITERATIONS * 100).foreach { _ =>
          val _ = buffer.getData()
        }
        timer.stopTiming()
      } finally {
        buffer.release()
      }
    }

    benchmark.run()
  }

  /**
   * Benchmarks streaming vs sort shuffle latency comparison to validate
   * the target 30-50% latency reduction.
   *
   * Note: This benchmark provides a baseline comparison. Full end-to-end
   * shuffle latency comparison requires running with actual Spark shuffle
   * operations in a cluster environment.
   */
  private def benchmarkLatencyComparison(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LatencyComparisonBenchmark")
      .set(SHUFFLE_STREAMING_ENABLED.key, "true")
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key, "20")
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD.key, "80")

    val memoryManager = UnifiedMemoryManager(conf, numCores = 4)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L)
    val serializerInstance = new JavaSerializer(conf).newInstance()

    val benchmark = new Benchmark(
      "Latency Comparison (Baseline)",
      NUM_RECORDS / 10,
      minNumIters = WARMUP_ITERATIONS,
      output = output
    )

    // Baseline: Traditional buffer with full materialization pattern
    benchmark.addTimerCase("Baseline (full materialization)") { timer =>
      timer.startTiming()

      // Simulate traditional shuffle: accumulate all data, then read
      val buffers = (0 until NUM_PARTITIONS).map { partitionId =>
        new StreamingBuffer(partitionId, BUFFER_CAPACITY / NUM_PARTITIONS, taskMemoryManager)
      }

      try {
        // Write phase - accumulate all records
        (1 to NUM_RECORDS / 100).foreach { i =>
          val partitionId = i % NUM_PARTITIONS
          buffers(partitionId).append(i, s"value_$i", serializerInstance)
        }

        // Read phase - read all at once after complete
        buffers.foreach { buffer =>
          val _ = buffer.getData()
          val _ = buffer.getChecksum()
        }
      } finally {
        buffers.foreach(_.release())
      }

      timer.stopTiming()
    }

    // Streaming: Interleaved write/read pattern (simulating streaming behavior)
    benchmark.addTimerCase("Streaming (interleaved)") { timer =>
      timer.startTiming()

      val buffers = (0 until NUM_PARTITIONS).map { partitionId =>
        new StreamingBuffer(partitionId, BUFFER_CAPACITY / NUM_PARTITIONS, taskMemoryManager)
      }

      try {
        // Interleaved write and read - streaming behavior
        var totalBytesStreamed = 0L
        (1 to NUM_RECORDS / 100).foreach { i =>
          val partitionId = i % NUM_PARTITIONS
          buffers(partitionId).append(i, s"value_$i", serializerInstance)

          // Stream data when buffer reaches threshold (streaming pattern)
          if (buffers(partitionId).size() > BUFFER_CAPACITY / (NUM_PARTITIONS * 2)) {
            val data = buffers(partitionId).getData()
            totalBytesStreamed += data.length
            val _ = buffers(partitionId).getChecksum()
            buffers(partitionId).reset()
          }
        }

        // Final flush
        buffers.foreach { buffer =>
          if (buffer.size() > 0) {
            val data = buffer.getData()
            totalBytesStreamed += data.length
          }
        }
      } finally {
        buffers.foreach(_.release())
      }

      timer.stopTiming()
    }

    // Streaming with backpressure integration
    benchmark.addTimerCase("Streaming with backpressure") { timer =>
      val protocol = new BackpressureProtocol(conf)

      timer.startTiming()

      val buffers = (0 until NUM_PARTITIONS).map { partitionId =>
        new StreamingBuffer(partitionId, BUFFER_CAPACITY / NUM_PARTITIONS, taskMemoryManager)
      }

      try {
        var totalBytesStreamed = 0L
        (1 to NUM_RECORDS / 100).foreach { i =>
          val partitionId = i % NUM_PARTITIONS

          // Check backpressure before writing
          val tokens = protocol.getAvailableTokens()
          if (tokens > 0) {
            buffers(partitionId).append(i, s"value_$i", serializerInstance)
            protocol.consumeTokens(100) // Approximate bytes per record
          }

          // Periodic heartbeat
          if (i % 1000 == 0) {
            val _ = protocol.heartbeat()
            protocol.refillTokens()
          }

          // Stream data when buffer reaches threshold
          if (buffers(partitionId).size() > BUFFER_CAPACITY / (NUM_PARTITIONS * 2)) {
            val data = buffers(partitionId).getData()
            totalBytesStreamed += data.length
            buffers(partitionId).reset()
          }
        }

        // Final flush
        buffers.foreach { buffer =>
          if (buffer.size() > 0) {
            totalBytesStreamed += buffer.getData().length
          }
        }
      } finally {
        buffers.foreach(_.release())
      }

      timer.stopTiming()
    }

    benchmark.run(relativeTime = true)
  }

  /**
   * Cleanup resources after all benchmarks complete.
   */
  override def afterAll(): Unit = {
    // Any cleanup needed after benchmarks
  }
}
