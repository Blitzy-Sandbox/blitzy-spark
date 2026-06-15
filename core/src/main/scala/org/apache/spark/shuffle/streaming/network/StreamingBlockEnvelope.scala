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

import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.CRC32C

import org.apache.spark.shuffle.streaming.StreamingShuffleConfig

/**
 * Canonical on-the-wire frame for a single streaming-shuffle block.
 *
 * An envelope is a fixed 32-byte big-endian header immediately followed by the block payload.
 * It is the byte-level contract shared by both halves of the streaming backend: the map-side
 * `StreamingShuffleWriter` builds one envelope per 2 MB block and hands it to the transport,
 * while the reduce-side `StreamingShuffleReader` calls [[verifyChecksum]] on each fetched block
 * and treats a mismatch as a fetch failure.
 *
 * ==Wire layout==
 *
 * The serialized frame is the 32-byte header followed by the payload bytes. All header fields are
 * written big-endian (network byte order); the layout never depends on the platform default:
 *
 * {{{
 *   offset  field           type   bytes
 *   0       shuffleId       Int    4
 *   4       mapId           Long   8
 *   12      reduceId        Int    4
 *   16      sequenceNumber  Long   8
 *   24      crc32c          Int    4   (lower 32 bits of the CRC32C value)
 *   28      payloadLength   Int    4
 *   32      payload         bytes  payloadLength  (capped at 2 MB)
 * }}}
 *
 * The total serialized size is therefore `32 + payloadLength`.
 *
 * ==CRC32C convention==
 *
 * Integrity is protected with the JDK [[java.util.zip.CRC32C]] checksum (the same checksum family
 * used by the sort-based path's `ShuffleChecksumUtils`). The 64-bit CRC value is narrowed to its
 * lower 32 bits and stored as an `Int` (`(crc.getValue & 0xFFFFFFFFL).toInt`); [[verifyChecksum]]
 * recomputes the CRC over the payload and compares it to that stored value.
 *
 * ==Dual-channel wire/persist invariant==
 *
 * The bytes produced by [[serialize]] / [[toByteArray]] are exactly the bytes that the
 * `StreamingBuffer` writes when it spills a block to disk. Streamed and spilled bytes are thus
 * byte-for-byte interchangeable, so a block read back from a spill file parses identically to one
 * read off the wire. The 2 MB block size and 32-byte header size are sourced from
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleConfig]] rather than hardcoded, which keeps
 * this layout in lockstep with the buffer/spill path.
 *
 * This is a plain class (not a `case class`) on purpose: case-class equality over the `payload`
 * `Array[Byte]` would compare array references instead of contents, which is a well-known pitfall.
 *
 * @param shuffleId the shuffle id the block belongs to
 * @param mapId the map (producer) id that emitted the block
 * @param reduceId the reduce (consumer) partition id the block targets
 * @param sequenceNumber monotonically increasing block sequence within a (shuffle, map, reduce)
 * @param crc32c lower 32 bits of the CRC32C computed over [[payload]], stored as an `Int`
 * @param payload the block payload bytes (length is capped at 2 MB by the companion factories)
 */
private[spark] class StreamingBlockEnvelope(
    val shuffleId: Int,
    val mapId: Long,
    val reduceId: Int,
    val sequenceNumber: Long,
    val crc32c: Int,
    val payload: Array[Byte]) {

  /** Length of the payload in bytes (mirrors the header's payloadLength field). */
  def payloadLength: Int = payload.length

  /** Recompute the CRC32C over the payload and compare it to the stored header value. */
  def verifyChecksum: Boolean =
    StreamingBlockEnvelope.computeCrc32c(payload) == crc32c

  /**
   * Serialize the 32-byte big-endian header followed by the payload into a freshly allocated
   * [[java.nio.ByteBuffer]] that is flipped and ready to read.
   */
  def serialize: ByteBuffer = {
    val buf = ByteBuffer.allocate(StreamingBlockEnvelope.HEADER_BYTES + payload.length)
    buf.order(ByteOrder.BIG_ENDIAN)
    buf.putInt(shuffleId)
    buf.putLong(mapId)
    buf.putInt(reduceId)
    buf.putLong(sequenceNumber)
    buf.putInt(crc32c)
    buf.putInt(payload.length)
    buf.put(payload)
    buf.flip()
    buf
  }

  /**
   * The serialized frame as an `Array[Byte]` in the canonical layout. These are the exact
   * bytes `StreamingBuffer` writes when spilling a block to disk, upholding the dual-channel
   * invariant that streamed and spilled bytes are byte-for-byte interchangeable.
   */
  def toByteArray: Array[Byte] = {
    val buf = serialize
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    out
  }
}

private[spark] object StreamingBlockEnvelope {

  /** 32-byte big-endian header size (sourced from the shared streaming config constant). */
  val HEADER_BYTES: Int = StreamingShuffleConfig.ENVELOPE_HEADER_BYTES

  /** Maximum payload size: the 2 MB block cap (sourced from the shared config constant). */
  val MAX_PAYLOAD_BYTES: Int = StreamingShuffleConfig.BLOCK_SIZE_BYTES

  /** Compute the CRC32C of the payload and return its lower 32 bits as an Int. */
  def computeCrc32c(payload: Array[Byte]): Int = {
    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    (crc.getValue & 0xFFFFFFFFL).toInt
  }

  /** Build an envelope over a payload, computing the CRC32C and guarding the 2 MB cap. */
  def create(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      sequenceNumber: Long,
      payload: Array[Byte]): StreamingBlockEnvelope = {
    require(payload.length <= MAX_PAYLOAD_BYTES,
      s"Streaming shuffle payload ${payload.length} exceeds 2 MB cap $MAX_PAYLOAD_BYTES")
    new StreamingBlockEnvelope(
      shuffleId, mapId, reduceId, sequenceNumber, computeCrc32c(payload), payload)
  }

  /** Parse a full frame (header + payload) from a [[java.nio.ByteBuffer]]. */
  def parse(buffer: ByteBuffer): StreamingBlockEnvelope = {
    val buf = buffer.duplicate()
    buf.order(ByteOrder.BIG_ENDIAN)
    require(buf.remaining() >= HEADER_BYTES,
      s"Streaming shuffle frame ${buf.remaining()} smaller than 32-byte header")
    val shuffleId = buf.getInt()
    val mapId = buf.getLong()
    val reduceId = buf.getInt()
    val sequenceNumber = buf.getLong()
    val crc32c = buf.getInt()
    val payloadLength = buf.getInt()
    require(payloadLength >= 0 && payloadLength <= MAX_PAYLOAD_BYTES,
      s"Streaming shuffle payloadLength $payloadLength invalid (cap $MAX_PAYLOAD_BYTES)")
    require(buf.remaining() >= payloadLength,
      s"Streaming shuffle frame truncated: need $payloadLength, have ${buf.remaining()}")
    val payload = new Array[Byte](payloadLength)
    buf.get(payload)
    new StreamingBlockEnvelope(
      shuffleId, mapId, reduceId, sequenceNumber, crc32c, payload)
  }

  /** Parse a full frame (header + payload) from a byte array. */
  def parse(bytes: Array[Byte]): StreamingBlockEnvelope = parse(ByteBuffer.wrap(bytes))
}
