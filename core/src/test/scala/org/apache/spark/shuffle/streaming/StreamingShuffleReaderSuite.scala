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

import java.nio.ByteBuffer

import org.mockito.Mockito.mock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{
  MapOutputTracker,
  ShuffleDependency,
  SparkConf,
  SparkFunSuite,
  TaskContext
}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.network.{
  StreamingBlockEnvelope,
  StreamingShuffleTransport
}
import org.apache.spark.storage.{BlockManager, BlockManagerId}

/**
 * Unit tests for the reduce-side envelope de-framing / CRC32C validation of
 * [[StreamingShuffleReader]].
 *
 * These tests target the data-integrity core the CP2 review flagged: the real fetched bytes are
 * framed [[StreamingBlockEnvelope]]s, so the reader must parse every frame, verify its CRC32C, and
 * concatenate the payload-only bytes before deserialization -- never feed the 32-byte headers to
 * the serializer, and never accept a truncated/oversized/corrupt frame. The package-private
 * [[StreamingShuffleReader.extractValidatedPayloads]] is exercised directly with hand-built frames;
 * the reader is constructed with mocked collaborators (no SparkContext) because `extractValidated`
 * only touches the streaming metrics holder and the typed config.
 *
 * A corrupt, truncated, or partial-header frame is required to fail the read through
 * [[FetchFailedException]] (so Spark's lineage machinery recomputes the upstream output) and to
 * increment the `partialReadInvalidations` telemetry counter.
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with Matchers {

  private val bmId = BlockManagerId("exec-1", "host-1", 7337)

  /**
   * Builds a reader with fully mocked collaborators. No SparkContext is required: the envelope
   * extraction path only uses the real [[StreamingShuffleMetrics]] (passed in so a test can assert
   * the invalidation counter) and the real [[StreamingShuffleConfig]]. The block manager and map
   * output tracker are mocked and never invoked by the envelope extraction under test.
   */
  private def buildReader(metrics: StreamingShuffleMetrics): StreamingShuffleReader[Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      0, dep, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)
    new StreamingShuffleReader[Int, Int](
      handle,
      startMapIndex = 0,
      endMapIndex = 1,
      startPartition = 0,
      endPartition = 1,
      context = mock(classOf[TaskContext]),
      readMetrics = mock(classOf[ShuffleReadMetricsReporter]),
      config = new StreamingShuffleConfig(new SparkConf(false)),
      streamingMetrics = metrics,
      transport = mock(classOf[StreamingShuffleTransport]),
      blockManager = mock(classOf[BlockManager]),
      mapOutputTracker = mock(classOf[MapOutputTracker]))
  }

  /** Frames a payload into the canonical 32-byte-header envelope bytes (CRC computed by create). */
  private def frame(seq: Long, payload: Array[Byte]): Array[Byte] =
    StreamingBlockEnvelope.create(0, 0L, 0, seq, payload).toByteArray

  /** Concatenates frame byte arrays into a single fetched-block buffer. */
  private def concat(parts: Array[Byte]*): Array[Byte] = parts.flatten.toArray

  /** Deterministic payload of length `n`; content is irrelevant since the CRC covers it. */
  private def payload(n: Int): Array[Byte] = Array.tabulate(n)(i => (i % 127).toByte)

  test("extractValidatedPayloads de-frames and concatenates multiple payloads, headers stripped") {
    val reader = buildReader(new StreamingShuffleMetrics)
    val p0 = Array.emptyByteArray // an empty-payload frame must contribute zero bytes, not 32
    val p1 = payload(5000)
    val p2 = payload(37)
    val raw = concat(frame(0L, p0), frame(1L, p1), frame(2L, p2))

    val out = reader.extractValidatedPayloads(ByteBuffer.wrap(raw), bmId, 0L, 0, 0)

    // Only the payload bytes, in frame order, with every 32-byte header stripped.
    assert(out.sameElements(p0 ++ p1 ++ p2))
  }

  test("extractValidatedPayloads returns an empty array for an empty fetched block") {
    val reader = buildReader(new StreamingShuffleMetrics)
    val out = reader.extractValidatedPayloads(ByteBuffer.wrap(Array.emptyByteArray), bmId, 0L, 0, 0)
    assert(out.isEmpty)
  }

  test("extractValidatedPayloads fails a CRC32C-mismatched frame and counts the invalidation") {
    val metrics = new StreamingShuffleMetrics
    val reader = buildReader(metrics)
    val corrupt = frame(0L, payload(64))
    // Flip the first payload byte (just past the 32-byte header) so the recomputed CRC32C differs.
    val idx = StreamingBlockEnvelope.HEADER_BYTES
    corrupt(idx) = (corrupt(idx) ^ 0xFF).toByte
    val before = metrics.partialReadInvalidations

    intercept[FetchFailedException] {
      reader.extractValidatedPayloads(ByteBuffer.wrap(corrupt), bmId, 0L, 0, 0)
    }
    assert(metrics.partialReadInvalidations === before + 1L)
  }

  test("extractValidatedPayloads fails a truncated frame") {
    val reader = buildReader(new StreamingShuffleMetrics)
    // A full frame minus the last 10 payload bytes: parse must reject it as truncated.
    val truncated = frame(0L, payload(128)).dropRight(10)

    intercept[FetchFailedException] {
      reader.extractValidatedPayloads(ByteBuffer.wrap(truncated), bmId, 0L, 0, 0)
    }
  }

  test("extractValidatedPayloads fails on trailing partial-header bytes after a valid frame") {
    val reader = buildReader(new StreamingShuffleMetrics)
    // A valid frame followed by 10 stray bytes -- fewer than the 32-byte header -- must fail rather
    // than be silently ignored: a partial header signals a corrupt/truncated producer write.
    val raw = concat(frame(0L, payload(32)), new Array[Byte](10))

    intercept[FetchFailedException] {
      reader.extractValidatedPayloads(ByteBuffer.wrap(raw), bmId, 0L, 0, 0)
    }
  }
}
