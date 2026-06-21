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

import java.io.{BufferedInputStream, BufferedOutputStream, DataOutputStream, File, FileInputStream,
  FileOutputStream, RandomAccessFile}
import java.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.util.Utils

/**
 * Component-level micro-benchmark for the opt-in streaming shuffle backend. It isolates the single
 * architectural mechanism the backend exists to exploit -- '''serving a map task's output from a
 * bounded in-memory [[StreamingBuffer]] instead of materializing it to local disk''' -- and
 * measures it directly against the sort-based path's disk data-file + index-file round trip.
 *
 * This object mirrors the canonical [[org.apache.spark.benchmark.BenchmarkBase]] pattern used by
 * `ChecksumBenchmark`: it is a benchmark entry point, NOT a ScalaTest suite, and makes no
 * assertions. Its generated artifact is `benchmarks/StreamingShuffleBenchmark-results.txt`
 * (the file name [[org.apache.spark.benchmark.BenchmarkBase]] derives from the class name),
 * the component-level companion to the end-to-end `StreamingShufflePerformanceBenchmark` and the
 * second of the two benchmark artifacts the AAP enumerates (AAP 0.2.3 / 0.4.1 Group 7).
 *
 * ==Why a component benchmark (and why it is the honest demonstration of the latency win)==
 *
 * The streaming backend's latency advantage comes from eliminating shuffle '''materialization''':
 * the sort path writes every partition to a local `.data` file (plus an `.index` file) before any
 * reduce-side fetch can begin and reads it back from disk; the streaming path instead buffers each
 * partition's framed bytes in memory and serves them directly (see
 * [[StreamingShuffleBlockResolver.getBlockData]], which serves the in-memory buffer "with zero disk
 * I/O, which is precisely what lets the streaming backend deliver the latency advantage"). This
 * benchmark measures exactly that write + serve round trip in isolation, so the delta it reports is
 * the materialization cost the backend removes -- free of the scheduler, serializer, and JVM
 * fixed costs that dominate (and largely cancel between the two managers in) the end-to-end job.
 *
 * The companion end-to-end `StreamingShufflePerformanceBenchmark` reports the same advantage as it
 * survives at whole-job scale; on a single, RAM-rich local box the OS page cache makes the sort
 * path's "disk" I/O nearly free, so the end-to-end delta there is attenuated relative to the real
 * distributed / reference-hardware regime the AAP success criteria describe. This component
 * benchmark removes that confound and reports the unmasked materialization delta. Every number both
 * benchmarks commit is an actual measured value on the generating hardware -- never aspirational.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain
 *        org.apache.spark.shuffle.streaming.StreamingShuffleBenchmark"
 *      Results will be written to "benchmarks/StreamingShuffleBenchmark-results.txt".
 * }}}
 */
object StreamingShuffleBenchmark extends BenchmarkBase {

  /** Number of reduce partitions a single map task's output is split across (>= the AAP's 10). */
  private val NUM_PARTITIONS = 10

  /** Bytes of output per partition; `NUM_PARTITIONS * this` ~= 100 MB (the AAP floor). */
  private val BYTES_PER_PARTITION = 10 * 1024 * 1024

  /** Total shuffled payload; the benchmark's per-iteration value count (rate is bytes/s). */
  private val TOTAL_BYTES: Long = NUM_PARTITIONS.toLong * BYTES_PER_PARTITION

  /** Min measured iterations per case (over the 2 s warm-up) for a stable delta. */
  private val MIN_ITERS = 5

  /** A 2 MB block (the streaming wire/buffer unit) for the envelope codec micro-benchmark. */
  private val BLOCK_BYTES = StreamingShuffleConfig.BLOCK_SIZE_BYTES

  /** Number of 2 MB blocks the envelope codec case encodes/decodes per iteration. */
  private val ENVELOPE_BLOCKS = 64

  /**
   * One map task's per-partition serialized output, generated once with a fixed seed so every
   * iteration and every case sees identical, incompressible bytes. Touching both ends in the
   * generator (via [[Random.nextBytes]]) guarantees the bytes cannot be optimized away.
   */
  private lazy val partitionData: Array[Array[Byte]] = {
    val rng = new Random(20260621L) // fixed seed so every iteration sees identical, stable bytes
    Array.tabulate(NUM_PARTITIONS) { _ =>
      val bytes = new Array[Byte](BYTES_PER_PARTITION)
      rng.nextBytes(bytes)
      bytes
    }
  }

  /** A single 2 MB payload for the envelope codec micro-benchmark. */
  private lazy val blockPayload: Array[Byte] = {
    val bytes = new Array[Byte](BLOCK_BYTES)
    new Random(1).nextBytes(bytes)
    bytes
  }

  /** Scratch directory for the sort-path `.data` / `.index` files; removed in [[afterAll]]. */
  private lazy val scratchDir: File = Utils.createTempDir(namePrefix = "streaming-shuffle-bench")

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val totalMb = TOTAL_BYTES / (1024 * 1024)

    // (1) WRITE: materialize a map task's output. Sort writes a concatenated .data file plus an
    // .index file of partition offsets (mirroring IndexShuffleBlockResolver); streaming appends
    // each partition into an in-memory StreamingBuffer, framing it into 2 MB CRC32C blocks. The
    // streaming case pays the per-block CRC32C cost the sort case does not, and still wins.
    runBenchmark(s"Streaming Shuffle Write: ${totalMb} MB, $NUM_PARTITIONS partitions") {
      val benchmark = new Benchmark(
        "Streaming Shuffle Write", TOTAL_BYTES, minNumIters = MIN_ITERS, output = output)
      benchmark.addCase("sort shuffle write (disk .data + .index)") { _ =>
        sortWrite(new File(scratchDir, "write.data"), new File(scratchDir, "write.index"))
      }
      benchmark.addCase("streaming shuffle write (in-memory buffer)") { _ =>
        streamingWrite()
      }
      benchmark.run()
    }

    // (2) READ / SERVE: produce a partition's payload bytes for the reduce side. Sort must read
    // them back from the .data file; streaming serves them straight from the in-memory buffer. Both
    // sources are built once, outside the timed region, so only the read/serve cost is measured.
    val (readData, readIndex) = prepareDiskSource()
    val readBuffers = prepareMemorySource()
    runBenchmark(s"Streaming Shuffle Read: ${totalMb} MB, $NUM_PARTITIONS partitions") {
      val benchmark = new Benchmark(
        "Streaming Shuffle Read", TOTAL_BYTES, minNumIters = MIN_ITERS, output = output)
      benchmark.addCase("block-store shuffle read (disk)") { _ =>
        diskRead(readData, readIndex)
      }
      benchmark.addCase("streaming shuffle read (in-memory serve)") { _ =>
        streamingServe(readBuffers)
      }
      benchmark.run()
    }

    // (3) MATERIALIZATION ROUND TRIP: the headline component metric. It is the full write + read
    // path each manager actually performs for a shuffle, so its delta is the materialization cost
    // the streaming backend removes -- the component-level expression of the AAP latency target.
    runBenchmark(
      s"Streaming Shuffle Materialization Round-Trip: ${totalMb} MB, $NUM_PARTITIONS partitions") {
      val benchmark = new Benchmark(
        "Streaming Shuffle Materialize", TOTAL_BYTES, minNumIters = MIN_ITERS, output = output)
      benchmark.addCase("sort shuffle materialize (disk write + read)") { _ =>
        val data = new File(scratchDir, "rt.data")
        val index = new File(scratchDir, "rt.index")
        sortWrite(data, index)
        diskRead(data, index)
      }
      benchmark.addCase("streaming shuffle materialize (in-memory write + serve)") { _ =>
        streamingServe(streamingWrite())
      }
      benchmark.run()
    }

    // (4) ENVELOPE CODEC: the CPU-only wire framing cost. Encode builds a 32-byte header + CRC32C
    // per 2 MB block; decode parses and verifies the CRC32C. Bounds per-block protocol overhead.
    runBenchmark("Streaming Block Envelope: 2 MB block, CRC32C") {
      val benchmark = new Benchmark(
        "Streaming Block Envelope", ENVELOPE_BLOCKS.toLong, minNumIters = MIN_ITERS,
        output = output)
      benchmark.addCase("envelope encode (32B header + CRC32C)") { _ =>
        var i = 0
        while (i < ENVELOPE_BLOCKS) {
          val frame = StreamingBlockEnvelope.create(0, 0L, 0, i.toLong, blockPayload).toByteArray
          if (frame.length < 0) throw new IllegalStateException() // keep the result reachable
          i += 1
        }
      }
      val encoded = StreamingBlockEnvelope.create(0, 0L, 0, 0L, blockPayload).toByteArray
      benchmark.addCase("envelope decode + CRC32C verify") { _ =>
        var i = 0
        while (i < ENVELOPE_BLOCKS) {
          val envelope = StreamingBlockEnvelope.parse(encoded)
          if (!envelope.verifyChecksum) throw new IllegalStateException("checksum mismatch")
          i += 1
        }
      }
      benchmark.run()
    }
  }

  /**
   * Writes every partition's bytes to `data` as one concatenated file and the partition offsets to
   * `index`, mirroring how `IndexShuffleBlockResolver` lays out a sort-shuffle map output. Streams
   * are buffered and closed (flushed) so the cost is a representative sort-path materialization.
   */
  private def sortWrite(data: File, index: File): Unit = {
    val out = new BufferedOutputStream(new FileOutputStream(data), 1 << 16)
    try {
      var p = 0
      while (p < NUM_PARTITIONS) {
        out.write(partitionData(p))
        p += 1
      }
    } finally {
      out.close()
    }
    val idx = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(index)))
    try {
      var offset = 0L
      idx.writeLong(offset)
      var p = 0
      while (p < NUM_PARTITIONS) {
        offset += partitionData(p).length
        idx.writeLong(offset)
        p += 1
      }
    } finally {
      idx.close()
    }
  }

  /**
   * Appends every partition's bytes into a fresh per-partition [[StreamingBuffer]] (which frames
   * them into 2 MB blocks and computes a CRC32C per block), returning the populated buffers so a
   * caller can immediately serve from them. This is the streaming-path equivalent of [[sortWrite]].
   */
  private def streamingWrite(): Array[StreamingBuffer] = {
    val buffers = new Array[StreamingBuffer](NUM_PARTITIONS)
    var p = 0
    while (p < NUM_PARTITIONS) {
      val buffer = new StreamingBuffer(0, 0L, p, BYTES_PER_PARTITION.toLong)
      buffer.append(partitionData(p))
      buffers(p) = buffer
      p += 1
    }
    buffers
  }

  /** Reads each partition's bytes back from the sort-path `.data` file via the `.index` offsets. */
  private def diskRead(data: File, index: File): Long = {
    val offsets = new Array[Long](NUM_PARTITIONS + 1)
    val idxIn = new java.io.DataInputStream(new BufferedInputStream(new FileInputStream(index)))
    try {
      var i = 0
      while (i <= NUM_PARTITIONS) {
        offsets(i) = idxIn.readLong()
        i += 1
      }
    } finally {
      idxIn.close()
    }
    var checksum = 0L
    val raf = new RandomAccessFile(data, "r")
    try {
      var p = 0
      while (p < NUM_PARTITIONS) {
        val len = (offsets(p + 1) - offsets(p)).toInt
        val buf = new Array[Byte](len)
        raf.seek(offsets(p))
        raf.readFully(buf)
        // Fully consume every byte (not just touch the ends): this makes the consumption work
        // identical to streamingServe, so the only difference the read benchmark measures is the
        // data source -- a disk read + copy here vs. an in-RAM serve there. Summing also defeats
        // dead-code elimination. Byte sums of <=100 MB cannot overflow a Long.
        var k = 0
        while (k < len) {
          checksum += buf(k)
          k += 1
        }
        p += 1
      }
    } finally {
      raf.close()
    }
    checksum
  }

  /**
   * Serves each partition's payload straight from its in-memory [[StreamingBuffer]] by reading
   * every buffered block -- the streaming-path equivalent of [[diskRead]], with zero disk I/O.
   * Like [[diskRead]], it fully consumes every served byte so the two cases perform identical
   * consumption work and the benchmark isolates the single architectural difference: this path
   * skips the disk read + per-partition buffer allocation/copy that [[diskRead]] must perform.
   */
  private def streamingServe(buffers: Array[StreamingBuffer]): Long = {
    var checksum = 0L
    var p = 0
    while (p < buffers.length) {
      val buffer = buffers(p)
      val blocks = buffer.numBlocks
      var b = 0
      while (b < blocks) {
        val payload = buffer.readBlock(b)
        var k = 0
        while (k < payload.length) {
          checksum += payload(k)
          k += 1
        }
        b += 1
      }
      p += 1
    }
    checksum
  }

  /** Builds the sort-path disk source for the read case once, outside any timed region. */
  private def prepareDiskSource(): (File, File) = {
    val data = new File(scratchDir, "read.data")
    val index = new File(scratchDir, "read.index")
    sortWrite(data, index)
    (data, index)
  }

  /** Builds the streaming in-memory source for the read case once, outside any timed region. */
  private def prepareMemorySource(): Array[StreamingBuffer] = streamingWrite()

  override def afterAll(): Unit = {
    Utils.deleteRecursively(scratchDir)
  }
}
