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

import org.apache.spark.annotation.Since

/**
 * On-wire framing for a single streaming-shuffle block.
 *
 * The wire format is a fixed 32-byte big-endian header followed by a payload of at most 2 MB
 * (the block-size limit that keeps pipelining efficient). The header carries six 4-byte ints
 * (`shuffleId`, `mapId`, `reduceId`, `sequenceNumber`, `checksum`, `payloadLength`, i.e. 24 bytes)
 * plus 8 reserved/padding bytes so the header is always exactly 32 bytes. The reserved bytes are
 * currently written as zero and skipped on parse, leaving room for future protocol fields without
 * breaking the framing.
 *
 * The `checksum` is the CRC32C of the payload, computed with the JDK 17 `java.util.zip.CRC32C`
 * primitive. This is the same checksum family used by the sort-path shuffle checksum
 * (see `org.apache.spark.shuffle.ShuffleChecksumUtils`), so the feature adds zero third-party CRC
 * dependency (Architectural Decision Log #3).
 *
 * Isolation: this class lives entirely in the streaming `network` subpackage and is produced by
 * `StreamingShuffleWriter` (which frames buffered bytes into &le;2 MB blocks) and consumed by
 * `StreamingShuffleReader` (which calls [[verifyChecksum]] and requests retransmission on a
 * mismatch). It has no dependency on, and no effect on, the existing sort-based shuffle code path.
 *
 * Note on `mapId`: the wire header stores `mapId` as a 4-byte `Int` per the protocol definition.
 * Callers that hold a `Long` map id (for example the per-partition `StreamingBuffer`) pass its
 * `Int` form when framing a block.
 *
 * @param shuffleId      the shuffle this block belongs to
 * @param mapId          the producing map id (as a 4-byte int on the wire)
 * @param reduceId       the consuming reduce partition id
 * @param sequenceNumber monotonic per-(map, reduce) block sequence number for ordering/dedup
 * @param checksum       CRC32C of `payload` (low 32 bits) used for corruption detection
 * @param payload        the block bytes; must not exceed 2 MB (see `MAX_PAYLOAD_BYTES`)
 */
@Since("4.2.0")
private[spark] class StreamingBlockEnvelope(
    val shuffleId: Int,
    val mapId: Int,
    val reduceId: Int,
    val sequenceNumber: Int,
    val checksum: Int,
    val payload: Array[Byte]) {

  require(payload != null, "payload must not be null")
  require(payload.length <= StreamingBlockEnvelope.MAX_PAYLOAD_BYTES,
    s"payload length ${payload.length} exceeds the 2 MB streaming block cap")

  /** Number of payload bytes carried by this envelope (never exceeds 2 MB). */
  def payloadLength: Int = payload.length

  /** Total serialized size in bytes: the fixed 32-byte header plus the payload. */
  def serializedLength: Int = StreamingBlockEnvelope.HEADER_BYTES + payload.length

  /**
   * Serialize this envelope into a big-endian [[ByteBuffer]]: the fixed 32-byte header followed by
   * the payload. The returned buffer is flipped and positioned at 0, ready to read or transfer.
   */
  def serialize(): ByteBuffer = {
    val buf = ByteBuffer.allocate(StreamingBlockEnvelope.HEADER_BYTES + payload.length)
    buf.order(ByteOrder.BIG_ENDIAN)
    buf.putInt(shuffleId)
    buf.putInt(mapId)
    buf.putInt(reduceId)
    buf.putInt(sequenceNumber)
    buf.putInt(checksum)
    buf.putInt(payload.length)
    // Six 4-byte ints above occupy 24 bytes; write two reserved/padding words (8 bytes) so the
    // header is always a fixed 32 bytes. Reserved words are zero in v1 and skipped on parse.
    buf.putInt(StreamingBlockEnvelope.RESERVED_WORD)
    buf.putInt(StreamingBlockEnvelope.RESERVED_WORD)
    buf.put(payload)
    buf.flip()
    buf
  }

  /**
   * Recompute the CRC32C of the payload and compare it against the checksum carried in the header.
   * Returns true when the payload is intact; a false result signals corruption and drives
   * retransmission on the read side.
   */
  def verifyChecksum(): Boolean =
    StreamingBlockEnvelope.computeChecksum(payload) == checksum
}

/**
 * Companion object holding the wire constants, the canonical CRC32C routine, a factory that
 * computes the checksum automatically, and the inverse [[parse]] operation.
 */
@Since("4.2.0")
private[spark] object StreamingBlockEnvelope {

  /** Fixed header size in bytes: six 4-byte big-endian ints (24) plus [[RESERVED_BYTES]] (8). */
  val HEADER_BYTES: Int = 32

  /** Maximum payload per block: 2 MB, the block-size limit for pipelining efficiency. */
  val MAX_PAYLOAD_BYTES: Int = 2 * 1024 * 1024

  /** Reserved/padding bytes following the six header ints, reserved for future protocol fields. */
  val RESERVED_BYTES: Int = 8

  // Value written into each of the two reserved 4-byte words (currently zero).
  private val RESERVED_WORD: Int = 0

  /**
   * Compute the CRC32C of the given bytes using the JDK 17 `java.util.zip.CRC32C` primitive (the
   * same checksum family used by the sort-path shuffle checksum; zero third-party CRC dependency).
   * `CRC32C.getValue` returns a `Long` whose low 32 bits are the checksum; those bits are returned
   * here as a (possibly negative) `Int`, which is how the value is stored in the header.
   *
   * A `null` payload is rejected deterministically with an [[IllegalArgumentException]] rather than
   * surfacing as an incidental `NullPointerException` from the underlying `CRC32C.update`.
   *
   * @throws IllegalArgumentException if `payload` is null
   */
  def computeChecksum(payload: Array[Byte]): Int = {
    require(payload != null, "payload must not be null")
    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    (crc.getValue & 0xFFFFFFFFL).toInt
  }

  /**
   * Build an envelope for the given block, computing the payload CRC32C automatically. This is the
   * factory the writer uses so it never has to compute the checksum by hand.
   */
  def apply(
      shuffleId: Int,
      mapId: Int,
      reduceId: Int,
      sequenceNumber: Int,
      payload: Array[Byte]): StreamingBlockEnvelope = {
    new StreamingBlockEnvelope(
      shuffleId, mapId, reduceId, sequenceNumber, computeChecksum(payload), payload)
  }

  /**
   * Parse a [[StreamingBlockEnvelope]] from a big-endian buffer previously produced by
   * [[StreamingBlockEnvelope.serialize]]. A duplicate is read so the caller's buffer position is
   * left unchanged.
   *
   * Malformed or truncated input is rejected deterministically: the buffer must be non-null and
   * carry at least the fixed [[HEADER_BYTES]]-byte header before any field is read, and it must
   * carry the [[RESERVED_BYTES]] padding plus the declared payload before the payload is skipped
   * to and allocated. These guards run ahead of every read/allocation so hostile or corrupt input
   * fails with a clear [[IllegalArgumentException]] instead of an incidental
   * `NullPointerException` or `BufferUnderflowException`. The 2 MB payload cap is likewise enforced
   * before any allocation to guard against a corrupt or hostile length field.
   *
   * @throws IllegalArgumentException if `buf` is null, too short for the header, declares an
   *                                  out-of-range payload length, or lacks the reserved + payload
   *                                  bytes it declares
   */
  def parse(buf: ByteBuffer): StreamingBlockEnvelope = {
    require(buf != null, "buffer must not be null")
    val dup = buf.duplicate()
    dup.order(ByteOrder.BIG_ENDIAN)
    // Ensure the full fixed header is present before reading any of its six ints, so a truncated
    // buffer fails deterministically rather than through an incidental BufferUnderflowException.
    require(dup.remaining() >= HEADER_BYTES,
      s"buffer has ${dup.remaining()} bytes remaining, fewer than the $HEADER_BYTES-byte header")
    val shuffleId = dup.getInt()
    val mapId = dup.getInt()
    val reduceId = dup.getInt()
    val sequenceNumber = dup.getInt()
    val checksum = dup.getInt()
    val payloadLength = dup.getInt()
    require(payloadLength >= 0, s"payloadLength must be non-negative but was $payloadLength")
    require(payloadLength <= MAX_PAYLOAD_BYTES,
      s"payloadLength $payloadLength exceeds the 2 MB block cap ($MAX_PAYLOAD_BYTES bytes)")
    // The six ints above consumed 24 bytes; the reserved padding plus the declared payload must
    // still be present before we skip the reserved bytes and allocate/read the payload array.
    require(dup.remaining() >= RESERVED_BYTES + payloadLength,
      s"buffer has ${dup.remaining()} bytes remaining, fewer than the $RESERVED_BYTES reserved " +
        s"bytes plus $payloadLength payload bytes")
    // Skip the reserved/padding bytes to reach the end of the fixed 32-byte header.
    dup.position(dup.position() + RESERVED_BYTES)
    val payload = new Array[Byte](payloadLength)
    dup.get(payload)
    new StreamingBlockEnvelope(shuffleId, mapId, reduceId, sequenceNumber, checksum, payload)
  }
}
