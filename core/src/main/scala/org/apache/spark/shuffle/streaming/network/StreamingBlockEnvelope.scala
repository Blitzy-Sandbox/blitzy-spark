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

import org.apache.spark.internal.Logging

/**
 * Decoded representation of a streaming shuffle block: a 32-byte big-endian header
 * followed by a payload of at most 2 MiB, protected by a CRC32C checksum.
 *
 * @param shuffleId the shuffle id this block belongs to
 * @param mapId the producer map task id
 * @param reduceId the consumer reduce partition id
 * @param payload the block payload bytes; length must not exceed `MAX_PAYLOAD_SIZE`
 * @param checksum the CRC32C of `payload`, stored as the low 32 bits of the CRC value
 */
private[spark] case class StreamingBlockEnvelope(
    shuffleId: Int,
    mapId: Long,
    reduceId: Int,
    payload: Array[Byte],
    checksum: Int) {

  /**
   * Recomputes the CRC32C over `payload` and compares it to the stored `checksum`.
   *
   * @return true if the recomputed checksum matches the stored checksum, false otherwise
   */
  def verifyChecksum(): Boolean = {
    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    crc.getValue.toInt == checksum
  }

  /** Number of payload bytes carried by this envelope. */
  def payloadLength: Int = payload.length
}

/**
 * Codec for the self-describing wire envelope used by the streaming shuffle data path.
 *
 * Each streamed block is framed as a fixed 32-byte big-endian header followed by a payload of
 * at most `MAX_PAYLOAD_SIZE` bytes. All multi-byte header fields are big-endian. The header
 * layout is:
 *
 * {{{
 *   Offset  Size  Field          Type
 *   0       2     magic          Short   (== MAGIC)
 *   2       2     version        Short   (== VERSION)
 *   4       4     shuffleId      Int
 *   8       8     mapId          Long
 *   16      4     reduceId       Int
 *   20      4     payloadLength  Int
 *   24      4     checksum       Int     (CRC32C of payload, low 32 bits)
 *   28      4     reserved       Int     (== 0)
 * }}}
 *
 * `encode` returns a buffer ready for reading/transmission; `decode` reconstructs the envelope
 * and validates the structural header (magic and version) but does not validate the payload
 * checksum -- the consumer validates it via `StreamingBlockEnvelope.verifyChecksum` and decides
 * the invalidation path on mismatch.
 */
private[spark] object StreamingBlockEnvelope extends Logging {

  /** Fixed big-endian header size, in bytes. */
  val HEADER_SIZE: Int = 32

  /** Maximum payload size carried by a single envelope: 2 MiB. */
  val MAX_PAYLOAD_SIZE: Int = 2 * 1024 * 1024

  /** Magic marker ("SS") identifying a streaming shuffle envelope. */
  val MAGIC: Short = 0x5353.toShort

  /** Wire-format version of the envelope. */
  val VERSION: Short = 1

  /**
   * Encodes a streaming shuffle block into a self-describing wire envelope.
   *
   * The returned buffer contains the 32-byte big-endian header followed by `payload`, and is
   * flipped so it is ready for reading/transmission. A CRC32C over `payload` is computed and
   * stored in the header.
   *
   * @param shuffleId the shuffle id this block belongs to
   * @param mapId the producer map task id
   * @param reduceId the consumer reduce partition id
   * @param payload the block payload bytes; must be non-null and at most `MAX_PAYLOAD_SIZE`
   * @return a flipped [[ByteBuffer]] of `HEADER_SIZE + payload.length` bytes
   * @throws IllegalArgumentException if `payload` is null or larger than `MAX_PAYLOAD_SIZE`
   */
  def encode(shuffleId: Int, mapId: Long, reduceId: Int, payload: Array[Byte]): ByteBuffer = {
    require(payload != null, "payload must not be null")
    require(
      payload.length <= MAX_PAYLOAD_SIZE,
      s"payload ${payload.length} exceeds max $MAX_PAYLOAD_SIZE")

    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    val checksum = crc.getValue.toInt

    val buf = ByteBuffer.allocate(HEADER_SIZE + payload.length).order(ByteOrder.BIG_ENDIAN)
    buf.putShort(MAGIC)
    buf.putShort(VERSION)
    buf.putInt(shuffleId)
    buf.putLong(mapId)
    buf.putInt(reduceId)
    buf.putInt(payload.length)
    buf.putInt(checksum)
    buf.putInt(0)
    assert(buf.position() == HEADER_SIZE, s"header position ${buf.position()} != $HEADER_SIZE")

    buf.put(payload)
    buf.flip()
    buf
  }

  /**
   * Decodes a streaming shuffle block from its wire envelope.
   *
   * Operates on a duplicate of `buf` so the caller's position and limit are not mutated. The
   * structural header is validated (magic and version); a mismatch throws
   * `IllegalArgumentException`. The payload checksum is NOT validated here -- the consumer is
   * responsible for calling [[StreamingBlockEnvelope.verifyChecksum]] and deciding the
   * invalidation path on mismatch.
   *
   * @param buf a buffer positioned at the start of an encoded envelope
   * @return the decoded [[StreamingBlockEnvelope]]
   * @throws IllegalArgumentException if `buf` is null, too short, carries an unexpected magic or
   *                                  version, or declares an out-of-range payload length
   */
  def decode(buf: ByteBuffer): StreamingBlockEnvelope = {
    require(buf != null, "buffer must not be null")
    val b = buf.duplicate()
    b.order(ByteOrder.BIG_ENDIAN)
    require(
      b.remaining() >= HEADER_SIZE,
      s"buffer remaining ${b.remaining()} < header size $HEADER_SIZE")

    val magic = b.getShort()
    if (magic != MAGIC) {
      logWarning(s"Rejecting streaming block with invalid magic $magic (expected $MAGIC)")
      throw new IllegalArgumentException(
        s"Invalid streaming block magic: $magic (expected $MAGIC)")
    }
    val version = b.getShort()
    if (version != VERSION) {
      logWarning(s"Rejecting streaming block with unsupported version $version " +
        s"(expected $VERSION)")
      throw new IllegalArgumentException(
        s"Unsupported streaming block version: $version (expected $VERSION)")
    }

    val shuffleId = b.getInt()
    val mapId = b.getLong()
    val reduceId = b.getInt()
    val payloadLength = b.getInt()
    val checksum = b.getInt()
    b.getInt() // consume the 4 reserved bytes

    require(
      payloadLength >= 0 && payloadLength <= MAX_PAYLOAD_SIZE,
      s"invalid payload length: $payloadLength")
    require(
      b.remaining() >= payloadLength,
      s"buffer remaining ${b.remaining()} < payload length $payloadLength")

    val payload = new Array[Byte](payloadLength)
    b.get(payload)
    StreamingBlockEnvelope(shuffleId, mapId, reduceId, payload, checksum)
  }
}
