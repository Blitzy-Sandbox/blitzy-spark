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

import java.util.zip.CRC32C

import io.netty.buffer.{ByteBuf, ByteBufAllocator}

/**
 * Serializable envelope carrying a single streaming-shuffle block from producer to consumer.
 *
 * Part of the Streaming Shuffle feature (F-001). Coexistence strategy: this envelope is used
 * exclusively by the streaming shuffle path and NEVER touches the sort-based shuffle code,
 * which continues to persist map output via [[org.apache.spark.shuffle.IndexShuffleBlockResolver]]
 * on disk with its own checksum mechanism.
 *
 * Wire format (big-endian network byte order, matching Netty's default
 * [[io.netty.buffer.ByteBuf#writeInt]] / [[io.netty.buffer.ByteBuf#writeLong]] semantics):
 * {{{
 *   offset  size  field
 *   ------  ----  -------------------------------------------------------------
 *     0      4    shuffleId        (Int, big-endian)
 *     4      8    mapId            (Long, big-endian)
 *    12      4    reduceId         (Int, big-endian)
 *    16      8    sequenceNumber   (Long, big-endian; producer-assigned per partition)
 *    24      4    checksum         (Int CRC32C of payload, big-endian)
 *    28      4    payloadLength    (Int, big-endian; 0 <= length <= 2*1024*1024)
 *    32     var   payload          (payloadLength bytes)
 * }}}
 * Header is a fixed 32 bytes: 4 + 8 + 4 + 8 + 4 + 4 = 32. Payload is capped at 2 MB per the
 * user-specified streaming block-size limit ("Block size limited to 2 MB for pipelining
 * efficiency").
 *
 * Checksum semantics:
 *   The [[checksum]] field is the CRC32C of the raw payload bytes, computed via JDK 17's
 *   built-in [[java.util.zip.CRC32C]] (zero third-party dependencies). Producers populate the
 *   field at envelope construction via [[StreamingBlockEnvelope.create]]; consumers validate
 *   it via [[StreamingBlockEnvelope.verifyChecksum]] and trigger a retransmission request on
 *   mismatch, incrementing the `shuffle.streaming.partialReadInvalidations` counter.
 *
 * @param shuffleId      The Spark shuffle identifier.
 * @param mapId          The producer map-task attempt identifier.
 * @param reduceId       The consumer reducer partition identifier.
 * @param sequenceNumber A monotonically increasing per-partition block index, used by the
 *                       consumer to detect gaps and out-of-order delivery.
 * @param checksum       CRC32C of [[payload]], stored as a 32-bit [[scala.Int]] (the low 32
 *                       bits of [[java.util.zip.CRC32C#getValue]]).
 * @param payload        The block bytes. Must satisfy `payload.length <= 2 * 1024 * 1024`.
 */
private[spark] final case class StreamingBlockEnvelope(
    shuffleId: Int,
    mapId: Long,
    reduceId: Int,
    sequenceNumber: Long,
    checksum: Int,
    payload: Array[Byte])

private[spark] object StreamingBlockEnvelope {

  /** Maximum payload size: 2 MB per user specification for pipelining efficiency. */
  val MAX_PAYLOAD_BYTES: Int = 2 * 1024 * 1024

  /** Fixed header width: 4 (shuffleId) + 8 (mapId) + 4 (reduceId) + 8 (sequenceNumber) +
   * 4 (checksum) + 4 (payloadLength) = 32 bytes. */
  val HEADER_BYTES: Int = 32

  /**
   * Computes the CRC32C checksum of the given bytes using JDK 17's built-in
   * [[java.util.zip.CRC32C]]. The 64-bit `long` returned by [[java.util.zip.CRC32C#getValue]]
   * carries the 32-bit checksum in its low bits; we narrow to [[scala.Int]] explicitly so the
   * wire format remains a fixed 4 bytes.
   *
   * @param payload The bytes to checksum.
   * @return The CRC32C checksum of [[payload]] as a 32-bit [[scala.Int]] (may be negative when
   *         interpreted as signed; the 32-bit pattern is what matters for comparison).
   */
  def computeChecksum(payload: Array[Byte]): Int = {
    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    // Narrow the 64-bit long to 32-bit int while preserving the full checksum bit pattern.
    (crc.getValue & 0xffffffffL).toInt
  }

  /**
   * Constructs an envelope and computes its CRC32C checksum automatically. Callers that
   * already hold a checksum for the payload (for example after reading one off the wire) may
   * construct the case class directly.
   *
   * @param shuffleId      See [[StreamingBlockEnvelope]].
   * @param mapId          See [[StreamingBlockEnvelope]].
   * @param reduceId       See [[StreamingBlockEnvelope]].
   * @param sequenceNumber See [[StreamingBlockEnvelope]].
   * @param payload        See [[StreamingBlockEnvelope]]. Must be <= 2 MB.
   */
  def create(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      sequenceNumber: Long,
      payload: Array[Byte]): StreamingBlockEnvelope = {
    require(payload.length <= MAX_PAYLOAD_BYTES,
      s"Streaming block payload ${payload.length} bytes exceeds 2 MB limit " +
        s"($MAX_PAYLOAD_BYTES bytes) for pipelining efficiency.")
    StreamingBlockEnvelope(shuffleId, mapId, reduceId, sequenceNumber,
      computeChecksum(payload), payload)
  }

  /**
   * Serializes an envelope into a newly-allocated [[io.netty.buffer.ByteBuf]] using the
   * fixed-format wire layout documented on the case class. Callers are responsible for
   * releasing the returned buffer via [[io.netty.buffer.ByteBuf#release]] once it has been
   * written to the channel.
   *
   * The method follows the Netty idiom of releasing the allocated buffer if encoding throws,
   * so direct memory is never leaked on the error path.
   *
   * @param env       The envelope to encode.
   * @param allocator The Netty allocator that will produce the backing buffer.
   * @return A [[ByteBuf]] whose writer index is exactly `HEADER_BYTES + env.payload.length`.
   * @throws IllegalArgumentException if the payload exceeds 2 MB.
   */
  def toByteBuf(env: StreamingBlockEnvelope, allocator: ByteBufAllocator): ByteBuf = {
    require(env.payload.length <= MAX_PAYLOAD_BYTES,
      s"Block size must be <= 2 MB for pipelining efficiency " +
        s"(got ${env.payload.length} bytes, limit is $MAX_PAYLOAD_BYTES bytes).")
    val buf = allocator.buffer(HEADER_BYTES + env.payload.length)
    try {
      buf.writeInt(env.shuffleId)
      buf.writeLong(env.mapId)
      buf.writeInt(env.reduceId)
      buf.writeLong(env.sequenceNumber)
      buf.writeInt(env.checksum)
      buf.writeInt(env.payload.length)
      buf.writeBytes(env.payload)
      buf
    } catch {
      case e: Throwable =>
        // Release the buffer before rethrowing so we don't leak direct memory.
        buf.release()
        throw e
    }
  }

  /**
   * Deserializes an envelope from a [[io.netty.buffer.ByteBuf]] produced by [[toByteBuf]].
   * Advances the buffer's reader index by `HEADER_BYTES + payloadLength` bytes on success.
   * The caller retains ownership of `buf` and is responsible for releasing it.
   *
   * @param buf A buffer whose readable bytes begin with a streaming block envelope encoded in
   *            the documented wire format.
   * @return The decoded envelope with its full payload copied into an on-heap [[Array]].
   * @throws IllegalArgumentException if the buffer is too small for the header, or if the
   *                                  encoded payload length is negative or exceeds 2 MB.
   */
  def fromByteBuf(buf: ByteBuf): StreamingBlockEnvelope = {
    require(buf.readableBytes() >= HEADER_BYTES,
      s"Buffer has ${buf.readableBytes()} readable bytes but envelope header requires " +
        s"$HEADER_BYTES bytes.")
    val shuffleId = buf.readInt()
    val mapId = buf.readLong()
    val reduceId = buf.readInt()
    val sequenceNumber = buf.readLong()
    val checksum = buf.readInt()
    val payloadLength = buf.readInt()
    require(payloadLength >= 0 && payloadLength <= MAX_PAYLOAD_BYTES,
      s"Streaming block payload length $payloadLength is invalid " +
        s"(must be 0 <= length <= $MAX_PAYLOAD_BYTES).")
    require(buf.readableBytes() >= payloadLength,
      s"Buffer has ${buf.readableBytes()} readable bytes but payload requires $payloadLength.")
    val payload = new Array[Byte](payloadLength)
    buf.readBytes(payload)
    StreamingBlockEnvelope(shuffleId, mapId, reduceId, sequenceNumber, checksum, payload)
  }

  /**
   * Validates that the envelope's stored [[StreamingBlockEnvelope.checksum]] matches a freshly
   * computed CRC32C of its [[StreamingBlockEnvelope.payload]]. A mismatched checksum signals
   * payload corruption in transit and should cause the consumer to request retransmission.
   *
   * @param env The envelope to validate.
   * @return `true` if the stored checksum matches the computed checksum, `false` otherwise.
   */
  def verifyChecksum(env: StreamingBlockEnvelope): Boolean =
    computeChecksum(env.payload) == env.checksum
}
