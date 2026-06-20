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

package org.apache.spark.shuffle.streaming.network

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[StreamingBlockEnvelope]] framing, focusing on the multi-frame decode contract
 * that backs the streaming backend's zero-data-loss guarantee.
 *
 * `StreamingBuffer` serializes a buffered partition (and the disk spill mirrors it) as the in-order
 * concatenation of one canonical envelope frame per 2 MB block, so a fetched `ManagedBuffer` for a
 * multi-block partition contains multiple frames. The reader must therefore decode and CRC-validate
 * every frame -- decoding only the first would silently drop trailing blocks. These tests pin that
 * behavior end-to-end at the envelope layer:
 *
 *   - [[StreamingBlockEnvelope.parse]] decodes exactly one frame and leaves the caller's buffer
 *     position unchanged (the single-frame contract);
 *   - [[StreamingBlockEnvelope.parseAll]] decodes every concatenated frame, in wire order, with
 *     each frame's fields and payload intact;
 *   - `parseAll` rejects trailing bytes that do not form a complete frame and rejects a truncated
 *     payload, so malformed wire bytes never pass validation silently;
 *   - per-frame CRC32C is preserved through `parseAll`, so a corrupt trailing frame is still
 *     detectable via [[StreamingBlockEnvelope.verifyChecksum]].
 */
class StreamingBlockEnvelopeSuite extends SparkFunSuite {

  /** Builds a deterministic payload of `n` bytes whose contents vary with `seed`. */
  private def payloadOf(n: Int, seed: Int): Array[Byte] =
    Array.tabulate(n)(i => ((i + seed) & 0xFF).toByte)

  /** Concatenates the given byte arrays into a single contiguous array. */
  private def concat(arrays: Array[Byte]*): Array[Byte] = {
    val out = new Array[Byte](arrays.map(_.length).sum)
    var pos = 0
    arrays.foreach { a =>
      System.arraycopy(a, 0, out, pos, a.length)
      pos += a.length
    }
    out
  }

  test("create computes a verifying CRC and round-trips a single frame") {
    val payload = payloadOf(128, seed = 7)
    val env = StreamingBlockEnvelope.create(
      shuffleId = 3, mapId = 11L, reduceId = 5, sequenceNumber = 0L, payload = payload)
    assert(env.verifyChecksum, "freshly created envelope must verify")

    val parsed = StreamingBlockEnvelope.parse(env.toByteArray)
    assert(parsed.shuffleId === 3)
    assert(parsed.mapId === 11L)
    assert(parsed.reduceId === 5)
    assert(parsed.sequenceNumber === 0L)
    assert(parsed.payloadLength === payload.length)
    assert(parsed.payload.sameElements(payload))
    assert(parsed.verifyChecksum)
  }

  test("parse decodes only the first frame and leaves the caller buffer position unchanged") {
    val frame0 = StreamingBlockEnvelope.create(1, 1L, 0, 0L, payloadOf(64, 1)).toByteArray
    val frame1 = StreamingBlockEnvelope.create(1, 1L, 0, 1L, payloadOf(96, 2)).toByteArray
    val buffer = ByteBuffer.wrap(concat(frame0, frame1))
    val positionBefore = buffer.position()

    val parsed = StreamingBlockEnvelope.parse(buffer)
    // Only the first frame is returned ...
    assert(parsed.sequenceNumber === 0L)
    assert(parsed.payloadLength === 64)
    // ... and the caller's buffer is untouched (parse duplicates internally).
    assert(buffer.position() === positionBefore)
    assert(buffer.remaining() === frame0.length + frame1.length)
  }

  test("parseAll decodes every concatenated frame in wire order") {
    val payloads = Seq(payloadOf(10, 1), payloadOf(2048, 2), payloadOf(1, 3), payloadOf(512, 4))
    val frames = payloads.zipWithIndex.map { case (p, i) =>
      StreamingBlockEnvelope
        .create(shuffleId = 9, mapId = 42L, reduceId = 7, sequenceNumber = i.toLong, payload = p)
        .toByteArray
    }
    val all = concat(frames: _*)

    val envelopes = StreamingBlockEnvelope.parseAll(all)
    assert(envelopes.size === payloads.size, "every frame must be decoded")
    envelopes.zipWithIndex.foreach { case (env, i) =>
      assert(env.shuffleId === 9)
      assert(env.mapId === 42L)
      assert(env.reduceId === 7)
      assert(env.sequenceNumber === i.toLong, "frames must be returned in wire order")
      assert(env.payload.sameElements(payloads(i)), s"frame $i payload must round-trip exactly")
      assert(env.verifyChecksum, s"frame $i must verify")
    }
  }

  test("parseAll on an empty buffer returns no frames") {
    assert(StreamingBlockEnvelope.parseAll(Array.emptyByteArray).isEmpty)
    assert(StreamingBlockEnvelope.parseAll(ByteBuffer.allocate(0)).isEmpty)
  }

  test("parseAll rejects trailing bytes that do not form a complete frame") {
    val frame = StreamingBlockEnvelope.create(1, 1L, 0, 0L, payloadOf(100, 5)).toByteArray
    // Append a few trailing bytes -- fewer than a full 32-byte header -- after a valid frame.
    val withTrailing = concat(frame, Array[Byte](1, 2, 3, 4, 5))
    val ex = intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.parseAll(withTrailing)
    }
    assert(ex.getMessage.contains("smaller than"),
      "trailing partial header must be rejected as too small for a header")
  }

  test("parseAll rejects a truncated payload") {
    val frame = StreamingBlockEnvelope.create(1, 1L, 0, 0L, payloadOf(200, 6)).toByteArray
    // Drop the last 10 payload bytes so the header's payloadLength exceeds the bytes available.
    val truncated = java.util.Arrays.copyOf(frame, frame.length - 10)
    val ex = intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.parseAll(truncated)
    }
    assert(ex.getMessage.contains("truncated"), "a short payload must be rejected as truncated")
  }

  test("parseAll preserves per-frame CRC so a corrupt trailing frame is detectable") {
    val good = StreamingBlockEnvelope.create(2, 3L, 1, 0L, payloadOf(256, 7)).toByteArray
    val corruptBase = StreamingBlockEnvelope.create(2, 3L, 1, 1L, payloadOf(256, 8)).toByteArray
    // Flip a single byte inside the SECOND frame's payload region (past its 32-byte header) so its
    // header CRC no longer matches; the length is unchanged, so the frame still parses.
    val corrupt = corruptBase.clone()
    val payloadByteIndex = StreamingBlockEnvelope.HEADER_BYTES + 4
    corrupt(payloadByteIndex) = (corrupt(payloadByteIndex) ^ 0xFF).toByte

    val envelopes = StreamingBlockEnvelope.parseAll(concat(good, corrupt))
    assert(envelopes.size === 2, "both frames must decode even when the trailing one is corrupt")
    assert(envelopes.head.verifyChecksum, "the intact leading frame must verify")
    assert(!envelopes(1).verifyChecksum, "the corrupted trailing frame must fail CRC validation")
  }

  test("create rejects a payload larger than the 2 MB block cap") {
    val tooBig = new Array[Byte](StreamingBlockEnvelope.MAX_PAYLOAD_BYTES + 1)
    intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.create(1, 1L, 0, 0L, tooBig)
    }
  }
}
