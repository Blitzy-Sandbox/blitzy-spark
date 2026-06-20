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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * A bounded, per-partition in-memory buffer holding framed shuffle bytes for a single
 * `(shuffleId, mapId, partitionId)` triple produced by the streaming shuffle backend.
 *
 * Bytes appended by the streaming writer are framed into immutable blocks of at most
 * [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]] (2 MB). Each block carries a CRC32C checksum computed
 * once, at append time, by the single canonical routine
 * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope.computeCrc32c]] (the JDK
 * CRC32C algorithm, shared with the `ShuffleChecksumUtils` family). The per-block checksum equals
 * the `crc32c` header field of the block's wire envelope, so no recomputation is needed.
 *
 * ==Dual-channel wire/persist invariant==
 *
 * The byte layout returned by [[toByteArray]] and [[toChunkedByteBuffer]] is the exact, in-order
 * concatenation of one [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] frame
 * per block -- each a 32-byte big-endian header followed by the block payload. This is the single
 * canonical block encoding, byte-for-byte identical to the frames streamed on the wire, which makes
 * spilled and streamed bytes fully interchangeable: a consumer cannot tell whether a partition was
 * served from memory or rehydrated from a disk spill, so spill-and-resume is transparent to the
 * reader (AAP section 0.4.2).
 *
 * ==Thread-safety==
 *
 * The single producing map task appends while the memory spill manager may concurrently inspect and
 * drain this buffer. The hot polling path the spill manager uses every 100 ms -- [[size]],
 * [[numBlocks]], [[isFull]], [[utilizationPercent]], and [[lastAccess]] -- is fully lock-free,
 * backed by an [[java.util.concurrent.atomic.AtomicLong]], an
 * [[java.util.concurrent.atomic.AtomicInteger]], and a `@volatile` timestamp, so polling never
 * blocks the writer. Structural operations that mutate or snapshot the block list ([[append]],
 * [[readBlock]], [[checksumOf]], [[toByteArray]], [[toChunkedByteBuffer]], and [[clear]]) hold a
 * single lightweight monitor; because appends come from one producer they contend only with the
 * rare snapshot/clear performed at spill time, never on the throughput-critical poll path.
 *
 * ==LRU and spill==
 *
 * [[lastAccess]] is refreshed on every read or write and, combined with [[size]], gives the spill
 * manager exactly the signals it needs to spill the largest / least-recently-used partitions first.
 * [[clear]] releases the in-memory blocks and resets the counters once a spill or acknowledgment
 * has made the bytes redundant; any [[ChunkedByteBuffer]] previously handed out for spilling
 * retains its own references to the payloads, so clearing the buffer is safe mid-spill.
 *
 * @param shuffleId     the shuffle this buffer belongs to
 * @param mapId         the map task that produced the buffered output
 * @param partitionId   the reduce partition the buffered bytes are destined for
 * @param capacityBytes the soft capacity, in bytes, used by [[isFull]] / [[utilizationPercent]];
 *                      appends are never rejected, capacity governs spill decisions only
 */
private[spark] class StreamingBuffer(
    val shuffleId: Int,
    val mapId: Long,
    val partitionId: Int,
    val capacityBytes: Long) {

  import StreamingBuffer.Block

  /** Monitor guarding structural mutation/snapshot of [[blocks]] (append/read/clear/snapshot). */
  private val lock = new Object()

  /** Ordered, append-only list of sealed immutable blocks; guarded by [[lock]]. */
  private val blocks = new ArrayBuffer[Block]()

  /** Total buffered payload bytes; lock-free so spill polling never blocks the writer. */
  private val currentSizeBytes = new AtomicLong(0L)

  /** Number of sealed blocks; lock-free counterpart to `blocks.length`. */
  private val blockCount = new AtomicInteger(0)

  /** Nanosecond timestamp of the most recent read or write, for LRU spill arbitration. */
  @volatile private var lastAccessNanos: Long = System.nanoTime()

  /** Records buffer activity for LRU tracking. */
  private def touch(): Unit = {
    lastAccessNanos = System.nanoTime()
  }

  /**
   * Appends `bytes` to the buffer, framing them into sealed blocks of at most
   * [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]]. Each block is copied out of the caller's array so
   * the buffer owns immutable payloads (the caller may safely reuse or mutate `bytes` afterwards),
   * and each block's CRC32C checksum is computed once here. Size, block count, and the LRU
   * timestamp are updated per block. A `null` or empty input is a no-op.
   *
   * Appends are never rejected on capacity; [[isFull]] and [[utilizationPercent]] drive spill
   * decisions externally in the memory spill manager.
   *
   * @param bytes the serialized shuffle bytes to buffer; may span multiple 2 MB blocks
   */
  def append(bytes: Array[Byte]): Unit = {
    if (bytes == null || bytes.length == 0) {
      return
    }
    lock.synchronized {
      var offset = 0
      while (offset < bytes.length) {
        val len = math.min(StreamingShuffleConfig.BLOCK_SIZE_BYTES, bytes.length - offset)
        val payload = bytes.slice(offset, offset + len)
        blocks += new Block(payload, StreamingBlockEnvelope.computeCrc32c(payload))
        blockCount.incrementAndGet()
        currentSizeBytes.addAndGet(len.toLong)
        offset += len
      }
      touch()
    }
  }

  /** The total number of buffered payload bytes across all blocks. */
  def size: Long = currentSizeBytes.get()

  /** The number of sealed 2 MB-framed blocks currently buffered. */
  def numBlocks: Int = blockCount.get()

  /** True once the buffered size has reached or exceeded [[capacityBytes]]. */
  def isFull: Boolean = size >= capacityBytes

  /**
   * Buffer fill level as a percentage of [[capacityBytes]], clamped to `[0.0, 100.0]`. Returns
   * `0.0` for a non-positive capacity to avoid division by zero.
   */
  def utilizationPercent: Double = {
    if (capacityBytes <= 0) {
      0.0
    } else {
      math.min(100.0, size * 100.0 / capacityBytes)
    }
  }

  /** Nanosecond timestamp of the most recent access, for LRU comparisons by the spill manager. */
  def lastAccess: Long = lastAccessNanos

  /**
   * Returns the payload of the block at `index`. The returned array is the buffer's own immutable
   * payload and must be treated as read-only by callers. Refreshes the LRU timestamp.
   *
   * @param index zero-based block index in `[0, numBlocks)`
   * @return the block's payload bytes
   * @throws IndexOutOfBoundsException if `index` is not a valid block index
   */
  def readBlock(index: Int): Array[Byte] = lock.synchronized {
    requireValidIndex(index)
    touch()
    blocks(index).payload
  }

  /**
   * Returns the CRC32C checksum of the block at `index`, computed once when the block was appended
   * via [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope.computeCrc32c]]. The
   * value is the lower 32 bits of the CRC32C held in an `Int`, identical to the `crc32c` header
   * field of the block's wire envelope. Refreshes the LRU timestamp.
   *
   * @param index zero-based block index in `[0, numBlocks)`
   * @return the block's CRC32C checksum (lower 32 bits, as an `Int`)
   * @throws IndexOutOfBoundsException if `index` is not a valid block index
   */
  def checksumOf(index: Int): Int = lock.synchronized {
    requireValidIndex(index)
    touch()
    blocks(index).checksum
  }

  /**
   * Materializes all buffered blocks into a single contiguous array, in append order, as the
   * in-order concatenation of one canonical
   * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] frame per block (32-byte
   * header + payload). The result is byte-for-byte identical to the frames streamed on the wire
   * (the dual-channel invariant), making it a drop-in source for disk spill or verification.
   *
   * [[toChunkedByteBuffer]] is preferred for the spill path: it avoids a large contiguous
   * allocation and supports buffers whose framed size exceeds `Int.MaxValue` bytes, which this
   * method cannot.
   *
   * @return the in-order concatenation of every block's wire envelope (header + payload)
   * @throws IllegalArgumentException if the framed size (payloads + per-block headers) exceeds
   *         `Int.MaxValue`
   */
  def toByteArray: Array[Byte] = lock.synchronized {
    val headerBytes = StreamingBlockEnvelope.HEADER_BYTES.toLong
    val framedTotal = currentSizeBytes.get() + blocks.length.toLong * headerBytes
    require(framedTotal <= Int.MaxValue,
      s"Framed buffered size $framedTotal bytes exceeds the maximum single-array length; " +
        "use toChunkedByteBuffer instead")
    val out = new Array[Byte](framedTotal.toInt)
    var pos = 0
    var i = 0
    while (i < blocks.length) {
      // Frame each block as the canonical wire envelope so spilled bytes are byte-for-byte
      // identical to streamed bytes (dual-channel invariant, AAP section 0.4.2).
      val frame = StreamingBlockEnvelope
        .create(shuffleId, mapId, partitionId, i.toLong, blocks(i).payload).toByteArray
      System.arraycopy(frame, 0, out, pos, frame.length)
      pos += frame.length
      i += 1
    }
    touch()
    out
  }

  /**
   * Exposes the buffered blocks as a [[ChunkedByteBuffer]] -- one chunk per block, in append order
   * -- for spilling via `BlockManager.putBytes`. Each chunk is a freshly serialized canonical
   * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] frame (32-byte header +
   * payload); because each frame is a standalone allocation independent of the buffer's blocks,
   * this buffer may be [[clear]]ed while the spill write is still in progress.
   *
   * The chunk sequence preserves the dual-channel invariant: its concatenated bytes equal both
   * [[toByteArray]] and the envelope frames streamed on the wire.
   *
   * @return a read-only chunked view of the buffered bytes, framed as wire envelopes
   */
  def toChunkedByteBuffer: ChunkedByteBuffer = lock.synchronized {
    val chunks = new Array[ByteBuffer](blocks.length)
    var i = 0
    while (i < blocks.length) {
      // Frame each block as the canonical wire envelope (serialize() returns a flipped, position-0
      // buffer) so the spilled chunk sequence is byte-for-byte identical to the streamed frames.
      chunks(i) = StreamingBlockEnvelope
        .create(shuffleId, mapId, partitionId, i.toLong, blocks(i).payload).serialize
      i += 1
    }
    touch()
    new ChunkedByteBuffer(chunks)
  }

  /**
   * Releases all buffered blocks and resets the size and block-count counters. Invoked after a
   * spill or acknowledgment makes the in-memory copy redundant. Any [[ChunkedByteBuffer]] handed
   * out earlier for spilling keeps its own references to the payloads, so they remain valid until
   * that consumer is done with them.
   */
  def clear(): Unit = lock.synchronized {
    blocks.clear()
    blockCount.set(0)
    currentSizeBytes.set(0L)
    touch()
  }

  override def toString: String =
    s"StreamingBuffer(shuffleId=$shuffleId, mapId=$mapId, partitionId=$partitionId, " +
      s"numBlocks=$numBlocks, sizeBytes=$size, capacityBytes=$capacityBytes)"

  /** Validates `index` against the current block count; the caller must hold [[lock]]. */
  private def requireValidIndex(index: Int): Unit = {
    if (index < 0 || index >= blocks.length) {
      throw new IndexOutOfBoundsException(
        s"Block index $index out of bounds for buffer with ${blocks.length} block(s)")
    }
  }
}

/**
 * Companion holding the immutable block representation. The CRC32C of each block is computed by the
 * single canonical routine
 * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope.computeCrc32c]], so the
 * buffer and the wire envelope always agree on the checksum bits (no second algorithm copy here).
 */
private[spark] object StreamingBuffer {

  /**
   * An immutable, sealed buffer block: a 2 MB-bounded payload paired with the CRC32C checksum
   * computed over it. Payloads are never mutated after construction, which is what makes lock-free
   * reads safe and lets each block be framed into its wire envelope on demand.
   *
   * @param payload  the framed block bytes (length is at most
   *                 [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]])
   * @param checksum the CRC32C checksum of `payload` -- the lower 32 bits held in an `Int`, the
   *                 same value carried in the block's wire envelope `crc32c` header field
   */
  private final class Block(val payload: Array[Byte], val checksum: Int)
}
