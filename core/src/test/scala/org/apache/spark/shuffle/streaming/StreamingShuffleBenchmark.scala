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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope

/**
 * Component micro-benchmark for the streaming-shuffle wire format. It isolates the streaming-only
 * serialization hot path -- 2 MB block framing with a 32-byte big-endian header and CRC32C
 * integrity -- from the end-to-end job comparison owned by
 * [[StreamingShufflePerformanceBenchmark]]. Each group times the streaming path against a
 * no-envelope byte-copy baseline so the overhead the
 * streaming backend adds (or removes) on the producer and consumer serialization paths is visible
 * in isolation, without a [[org.apache.spark.SparkContext]].
 *
 * The committed result file lists exactly the three groups produced here, in order, under a single
 * `Streaming Shuffle` heading. Re-running the documented command regenerates that file with the
 * same structure and the running host's measured timings, so the artifact is traceable to this
 * source. Sizing is source-derived: a 100 MB stream is framed as exactly
 * `100 MB / 2 MB = 50` blocks (see [[NUM_BLOCKS]]), and the per-block group performs
 * [[ENVELOPE_UNIT_OPS]] operations per measured iteration.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results are written to "benchmarks/StreamingShuffleBenchmark-results.txt".
 * }}}
 */
object StreamingShuffleBenchmark extends BenchmarkBase {

  /** Minimum measured iterations per case (mirrors the ChecksumBenchmark convention). */
  private val MIN_ITERS = 3

  /** Streaming block payload size: the 2 MB cap sourced from the shared envelope constant. */
  private val BLOCK_BYTES = StreamingBlockEnvelope.MAX_PAYLOAD_BYTES

  /** 1 MiB helper. */
  private val MB = 1024 * 1024

  /** Modeled stream size for the write/read framing groups. */
  private val STREAM_BYTES = 100 * MB

  /** A 100 MB stream is framed as exactly 50 blocks of 2 MB. */
  private val NUM_BLOCKS = STREAM_BYTES / BLOCK_BYTES

  /** Per-block encode/decode operations performed per measured iteration of the unit group. */
  private val ENVELOPE_UNIT_OPS = 1000

  // Sink that consumes per-iteration results so the JIT cannot elide the measured work. It is
  // read once, behind an unreachable guard, after the suite completes.
  private var blackhole: Long = 0L

  /**
   * Builds a deterministic 2 MB payload whose byte pattern depends on `seed`, so distinct blocks
   * carry distinct CRC32C values and the checksum work is real rather than constant-folded.
   */
  private def payloadBlock(seed: Int): Array[Byte] = {
    val block = new Array[Byte](BLOCK_BYTES)
    var i = 0
    while (i < block.length) {
      block(i) = ((i + seed) & 0xFF).toByte
      i += 1
    }
    block
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Streaming Shuffle") {
      writeFramingBenchmark()
      readDeframingBenchmark()
      envelopeUnitBenchmark()
    }
    // Force the accumulated sink to be observed so none of the measured work is optimized away.
    if (blackhole == Long.MinValue) {
      throw new IllegalStateException("unreachable streaming-shuffle benchmark sink guard")
    }
  }

  /** Write-side framing of a 100 MB stream into 2 MB CRC32C envelopes vs a plain byte copy. */
  private def writeFramingBenchmark(): Unit = {
    val blocks = Array.tabulate(NUM_BLOCKS)(payloadBlock)
    val benchmark = new Benchmark(
      "Streaming Shuffle Write: 100 MB, 10 partitions", NUM_BLOCKS.toLong, MIN_ITERS,
      output = output)
    benchmark.addCase("baseline byte copy (no envelope)") { _ =>
      var acc = 0L
      var i = 0
      while (i < blocks.length) {
        val out = new Array[Byte](blocks(i).length)
        System.arraycopy(blocks(i), 0, out, 0, blocks(i).length)
        acc += out.length
        i += 1
      }
      blackhole += acc
    }
    benchmark.addCase("streaming write (32B envelope + CRC32C)") { _ =>
      var acc = 0L
      var i = 0
      while (i < blocks.length) {
        val envelope = StreamingBlockEnvelope.create(0, 0L, i, i.toLong, blocks(i))
        acc += envelope.toByteArray.length
        i += 1
      }
      blackhole += acc
    }
    benchmark.run()
  }

  /** Read-side de-framing + CRC32C verification of a 100 MB stream vs a plain byte copy. */
  private def readDeframingBenchmark(): Unit = {
    val frames = Array.tabulate(NUM_BLOCKS) { i =>
      StreamingBlockEnvelope.create(0, 0L, i, i.toLong, payloadBlock(i)).toByteArray
    }
    val benchmark = new Benchmark(
      "Streaming Shuffle Read: 100 MB, 10 partitions", NUM_BLOCKS.toLong, MIN_ITERS,
      output = output)
    benchmark.addCase("baseline byte copy (no envelope)") { _ =>
      var acc = 0L
      var i = 0
      while (i < frames.length) {
        val payloadLen = frames(i).length - StreamingBlockEnvelope.HEADER_BYTES
        val out = new Array[Byte](payloadLen)
        System.arraycopy(frames(i), StreamingBlockEnvelope.HEADER_BYTES, out, 0, payloadLen)
        acc += out.length
        i += 1
      }
      blackhole += acc
    }
    benchmark.addCase("streaming read (parse + CRC32C verify)") { _ =>
      var acc = 0L
      var i = 0
      while (i < frames.length) {
        val envelope = StreamingBlockEnvelope.parse(frames(i))
        if (envelope.verifyChecksum) {
          acc += envelope.payloadLength
        }
        i += 1
      }
      blackhole += acc
    }
    benchmark.run()
  }

  /** Per-block envelope encode vs decode + CRC32C verify on a single 2 MB block. */
  private def envelopeUnitBenchmark(): Unit = {
    val payload = payloadBlock(0)
    val frame = StreamingBlockEnvelope.create(0, 0L, 0, 0L, payload).toByteArray
    val benchmark = new Benchmark(
      "Streaming Block Envelope: 2 MB block, CRC32C", ENVELOPE_UNIT_OPS.toLong, MIN_ITERS,
      output = output)
    benchmark.addCase("envelope encode (32B header + CRC32C)") { _ =>
      var acc = 0L
      var i = 0
      while (i < ENVELOPE_UNIT_OPS) {
        val envelope = StreamingBlockEnvelope.create(0, 0L, 0, i.toLong, payload)
        acc += envelope.toByteArray.length
        i += 1
      }
      blackhole += acc
    }
    benchmark.addCase("envelope decode + CRC32C verify") { _ =>
      var acc = 0L
      var i = 0
      while (i < ENVELOPE_UNIT_OPS) {
        val envelope = StreamingBlockEnvelope.parse(frame)
        if (envelope.verifyChecksum) {
          acc += envelope.payloadLength
        }
        i += 1
      }
      blackhole += acc
    }
    benchmark.run()
  }
}
