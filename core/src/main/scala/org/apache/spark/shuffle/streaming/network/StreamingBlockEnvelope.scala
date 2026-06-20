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
 * The canonical on-the-wire frame for a single streaming-shuffle block: a fixed 32-byte,
 * big-endian header followed by an opaque payload of at most 2 MB.
 *
 * A [[StreamingBlockEnvelope]] is the atomic unit exchanged by the streaming shuffle backend. On
 * the producer side, `StreamingShuffleWriter` frames each 2 MB block of map output into one
 * envelope (via [[StreamingBlockEnvelope.create]]) and hands it to the transport layer; on the
 * consumer side, `StreamingShuffleReader` reconstructs an envelope from the fetched bytes (via
 * [[StreamingBlockEnvelope.parse]]) and calls [[verifyChecksum]] on every block, treating a
 * mismatch as a fetch failure.
 *
 * ==Wire layout (exactly a 32-byte header, big-endian)==
 *
 * {{{
 *   off  field           type   bytes
 *   ---  --------------  -----  -----
 *     0  shuffleId       Int        4
 *     4  mapId           Long       8
 *    12  reduceId        Int        4
 *    16  sequenceNumber  Long       8
 *    24  crc32c          Int        4   // low 32 bits of the payload CRC32C
 *    28  payloadLength   Int        4
 *    32  payload         bytes      payloadLength (<= 2 MB)
 * }}}
 *
 * The total serialized size is therefore `HEADER_BYTES + payloadLength`. All multi-byte header
 * fields are written and read with [[java.nio.ByteOrder]].BIG_ENDIAN so that producer and consumer
 * agree on the byte ordering regardless of platform endianness.
 *
 * ==Dual-channel wire/persist invariant==
 *
 * The byte sequence produced by [[serialize]] / [[toByteArray]] is the single canonical block
 * encoding for the streaming backend: it is byte-for-byte identical to what `StreamingBuffer`
 * writes to disk when a buffered partition spills. Streamed bytes and spilled bytes are therefore
 * interchangeable. A consumer can [[StreamingBlockEnvelope.parse]] a block read back from a spill
 * file exactly as it parses one received over the network. Any change to this layout MUST be made
 * here and mirrored by the spill path so the invariant continues to hold.
 *
 * ==Checksum convention==
 *
 * The integrity field stores the lower 32 bits of the JDK [[java.util.zip.CRC32C]] of the payload,
 * narrowed via `(crc.getValue & 0xFFFFFFFFL).toInt`. [[verifyChecksum]] recomputes the CRC32C over
 * the payload and compares it to the stored value using the identical narrowing, so a value written
 * by [[create]] always verifies true and a single corrupted payload byte verifies false.
 *
 * ==Equality==
 *
 * This is a plain class rather than a `case class` on purpose: a case class would derive
 * `equals`/`hashCode` that compare the `payload` array by reference rather than by contents, which
 * is surprising and error-prone for a byte-buffer carrier. Callers that need content equality
 * should compare the relevant fields and payload bytes explicitly.
 *
 * @param shuffleId      the shuffle this block belongs to
 * @param mapId          the map (producer) task that emitted this block
 * @param reduceId       the reduce (consumer) partition this block targets
 * @param sequenceNumber the monotonically increasing block index within the (map, reduce) stream
 * @param crc32c         the lower 32 bits of the CRC32C of [[payload]]
 * @param payload        the block payload bytes; never null and at most 2 MB
 */
private[spark] class StreamingBlockEnvelope(
    val shuffleId: Int,
    val mapId: Long,
    val reduceId: Int,
    val sequenceNumber: Long,
    val crc32c: Int,
    val payload: Array[Byte]) {

  /** The payload length in bytes (the bytes following the fixed 32-byte header). */
  def payloadLength: Int = payload.length

  /**
   * Recomputes the CRC32C over [[payload]] and compares it to the [[crc32c]] header value.
   *
   * @return true if the recomputed checksum matches the stored value; false if the block is
   *         corrupt, in which case the reader treats it as a fetch failure
   */
  def verifyChecksum: Boolean =
    StreamingBlockEnvelope.computeCrc32c(payload) == crc32c

  /**
   * Serializes this envelope into a freshly allocated [[java.nio.ByteBuffer]] holding the 32-byte
   * big-endian header followed by the payload. The returned buffer is flipped and ready to read
   * (position 0, limit `HEADER_BYTES + payloadLength`).
   *
   * The produced bytes are the canonical block encoding shared with the disk-spill path (see the
   * dual-channel invariant in the class documentation), so streamed and spilled bytes match.
   *
   * @return a flipped, read-ready buffer of exactly `HEADER_BYTES + payloadLength` bytes
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
   * Serializes this envelope to a new byte array using the same canonical layout as [[serialize]].
   * This is the exact encoding `StreamingBuffer` persists on a disk spill, which keeps streamed and
   * spilled bytes interchangeable.
   *
   * @return a new array of exactly `HEADER_BYTES + payloadLength` bytes
   */
  def toByteArray: Array[Byte] = {
    val buf = serialize
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    out
  }
}

/**
 * Factory and constants for [[StreamingBlockEnvelope]]. The header size and the 2 MB payload cap
 * are sourced from [[StreamingShuffleConfig]] rather than re-hardcoded here, which keeps the wire
 * format and the buffer/spill sizing in lockstep (the dual-channel invariant).
 */
private[spark] object StreamingBlockEnvelope {

  /** The fixed 32-byte big-endian header size, sourced from the shared config constant. */
  val HEADER_BYTES: Int = StreamingShuffleConfig.ENVELOPE_HEADER_BYTES

  /** The maximum payload size (the 2 MB block cap), sourced from the shared config constant. */
  val MAX_PAYLOAD_BYTES: Int = StreamingShuffleConfig.BLOCK_SIZE_BYTES

  /**
   * Computes the CRC32C of the given payload and returns its lower 32 bits as an Int.
   *
   * The narrowing `(crc.getValue & 0xFFFFFFFFL).toInt` is the canonical convention used throughout
   * the streaming backend, so a checksum written here always matches one verified by
   * [[StreamingBlockEnvelope.verifyChecksum]].
   *
   * @param payload the bytes to checksum
   * @return the lower 32 bits of the CRC32C of `payload`
   */
  def computeCrc32c(payload: Array[Byte]): Int = {
    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    (crc.getValue & 0xFFFFFFFFL).toInt
  }

  /**
   * Builds an envelope from raw header fields and a payload, computing the CRC32C and enforcing the
   * 2 MB payload cap. Producers use this so they never hand-compute the checksum.
   *
   * @param shuffleId      the shuffle this block belongs to
   * @param mapId          the map (producer) task that emitted this block
   * @param reduceId       the reduce (consumer) partition this block targets
   * @param sequenceNumber the monotonically increasing block index within the (map, reduce) stream
   * @param payload        the block payload bytes; must be at most 2 MB
   * @return a fully populated envelope whose [[StreamingBlockEnvelope.verifyChecksum]] is true
   * @throws IllegalArgumentException if `payload` exceeds [[MAX_PAYLOAD_BYTES]]
   */
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

  /**
   * Reads exactly one frame starting at the current position of `buf` and advances `buf` past the
   * bytes it consumes (the 32-byte header plus `payloadLength` payload bytes).
   *
   * This is the shared frame decoder used by both [[parse]] (which reads a single frame from a
   * duplicate, leaving the caller's buffer untouched) and [[parseAll]] (which calls it repeatedly
   * to drain a buffer that concatenates one frame per 2 MB block). The caller is responsible for
   * forcing big-endian order on `buf` before the first call; this method does not duplicate so that
   * its position advance is visible to an iterating caller.
   *
   * @param buf a big-endian buffer positioned at the start of a frame
   * @return the parsed envelope; `crc32c` is read from the header and not recomputed
   * @throws IllegalArgumentException if fewer than `HEADER_BYTES` remain, if the payload length is
   *         negative or exceeds the 2 MB cap, or if the payload is truncated
   */
  private def readFrame(buf: ByteBuffer): StreamingBlockEnvelope = {
    require(buf.remaining() >= HEADER_BYTES,
      s"Streaming shuffle frame ${buf.remaining()} smaller than $HEADER_BYTES-byte header")
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

  /**
   * Parses a single complete frame (32-byte big-endian header + payload) from a
   * [[java.nio.ByteBuffer]], leaving the caller's buffer position unchanged.
   *
   * The buffer is duplicated so the caller's position is preserved; the duplicate's byte order is
   * forced to big-endian regardless of the input buffer's order. Only the first frame is read --
   * when the buffer may hold more than one concatenated frame (a multi-block partition), use
   * [[parseAll]] instead so every frame is decoded and no trailing data is silently dropped.
   *
   * @param buffer a buffer positioned at the start of a frame, with the full frame remaining
   * @return the parsed envelope; its `crc32c` is read from the header and not recomputed, so call
   *         [[StreamingBlockEnvelope.verifyChecksum]] to validate integrity
   * @throws IllegalArgumentException if the buffer is too small for the header, if the payload
   *         length is negative or exceeds the 2 MB cap, or if the payload is truncated
   */
  def parse(buffer: ByteBuffer): StreamingBlockEnvelope = {
    val buf = buffer.duplicate()
    buf.order(ByteOrder.BIG_ENDIAN)
    readFrame(buf)
  }

  /**
   * Parses a single complete frame from a byte array by wrapping it in a [[java.nio.ByteBuffer]].
   *
   * @param bytes the full frame bytes (header + payload)
   * @return the parsed envelope
   * @throws IllegalArgumentException under the same conditions as the `ByteBuffer` overload
   */
  def parse(bytes: Array[Byte]): StreamingBlockEnvelope = parse(ByteBuffer.wrap(bytes))

  /**
   * Parses every concatenated frame in `buffer`, in order, until the buffer is fully consumed.
   *
   * A buffered partition is serialized by `StreamingBuffer` (and read back from a spill file) as
   * the in-order concatenation of one [[StreamingBlockEnvelope]] frame per 2 MB block, so a fetched
   * `ManagedBuffer` for a multi-block partition contains multiple frames. This method decodes every
   * frame so the reader never silently drops trailing blocks (the zero-data-loss invariant).
   *
   * The buffer is duplicated so the caller's position is preserved, and its byte order is forced to
   * big-endian. Because [[readFrame]] requires a full header and a full payload for each frame, any
   * trailing bytes that do not form a complete frame -- a partial header or a truncated payload --
   * cause an [[IllegalArgumentException]] rather than being ignored, so malformed/trailing data is
   * rejected end-to-end. Each returned envelope's `crc32c` is read from its header and not
   * recomputed; the caller must invoke [[StreamingBlockEnvelope.verifyChecksum]] per frame.
   *
   * @param buffer a buffer positioned at the start of the first frame
   * @return the frames in wire order; empty only if `buffer` has no remaining bytes
   * @throws IllegalArgumentException if any frame has an invalid header/payload length, if a
   *         payload is truncated, or if trailing bytes do not form a complete frame
   */
  def parseAll(buffer: ByteBuffer): Seq[StreamingBlockEnvelope] = {
    val buf = buffer.duplicate()
    buf.order(ByteOrder.BIG_ENDIAN)
    val envelopes = scala.collection.mutable.ArrayBuffer.empty[StreamingBlockEnvelope]
    while (buf.hasRemaining) {
      envelopes += readFrame(buf)
    }
    envelopes.toSeq
  }

  /**
   * Parses every concatenated frame in a byte array by wrapping it in a [[java.nio.ByteBuffer]].
   *
   * @param bytes the bytes holding one or more concatenated frames
   * @return the frames in wire order
   * @throws IllegalArgumentException under the same conditions as the `ByteBuffer` overload
   */
  def parseAll(bytes: Array[Byte]): Seq[StreamingBlockEnvelope] = parseAll(ByteBuffer.wrap(bytes))
}
