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
import java.util.zip.CRC32C

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * A bounded, per-partition in-memory buffer holding framed shuffle bytes for a single
 * `(shuffleId, mapId, partitionId)` triple produced by the streaming shuffle backend.
 *
 * Bytes appended by the streaming writer are framed into immutable blocks of at most
 * [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]] (2 MB). Each block carries a CRC32C checksum computed
 * once, at append time, using the JDK [[java.util.zip.CRC32C]] primitive -- the canonical algorithm
 * shared with the `ShuffleChecksumUtils` family. The checksum is exposed per block so the wire
 * envelope and the reduce-side reader can verify integrity without recomputing it.
 *
 * ==Dual-channel wire/persist invariant==
 *
 * The byte layout returned by [[toByteArray]] and [[toChunkedByteBuffer]] is the exact, in-order
 * concatenation of the block payloads, which is byte-for-byte identical to the payload stream sent
 * on the wire (each block becomes the payload of one streaming block envelope). This makes spilled
 * bytes and streamed bytes fully interchangeable: a consumer cannot tell whether a partition was
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
        blocks += new Block(payload, StreamingBuffer.computeChecksum(payload))
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
   * Returns the CRC32C checksum (an unsigned 32-bit value held in a `Long`) of the block at
   * `index`, computed once when the block was appended. Refreshes the LRU timestamp.
   *
   * @param index zero-based block index in `[0, numBlocks)`
   * @return the block's CRC32C checksum
   * @throws IndexOutOfBoundsException if `index` is not a valid block index
   */
  def checksumOf(index: Int): Long = lock.synchronized {
    requireValidIndex(index)
    touch()
    blocks(index).checksum
  }

  /**
   * Materializes all buffered blocks into a single contiguous array, in append order. The result is
   * byte-for-byte identical to the payload stream sent on the wire (the dual-channel invariant),
   * making it a drop-in source for disk spill or verification.
   *
   * [[toChunkedByteBuffer]] is preferred for the spill path: it avoids a large contiguous
   * allocation and supports buffers larger than `Int.MaxValue` bytes, which this method cannot.
   *
   * @return the in-order concatenation of every block payload
   * @throws IllegalArgumentException if the buffered size exceeds `Int.MaxValue`
   */
  def toByteArray: Array[Byte] = lock.synchronized {
    val total = currentSizeBytes.get()
    require(total <= Int.MaxValue,
      s"Buffered size $total bytes exceeds the maximum single-array length; " +
        "use toChunkedByteBuffer instead")
    val out = new Array[Byte](total.toInt)
    var pos = 0
    var i = 0
    while (i < blocks.length) {
      val payload = blocks(i).payload
      System.arraycopy(payload, 0, out, pos, payload.length)
      pos += payload.length
      i += 1
    }
    touch()
    out
  }

  /**
   * Exposes the buffered blocks as a [[ChunkedByteBuffer]] -- one chunk per block, in append order
   * -- for spilling via `BlockManager.putBytes`. Each chunk wraps the block's payload with no copy;
   * because payloads are immutable and the returned buffer holds its own references, this buffer
   * may be [[clear]]ed while the spill write is still in progress.
   *
   * The chunk sequence preserves the dual-channel invariant: its concatenated bytes equal both
   * [[toByteArray]] and the wire payload stream.
   *
   * @return a read-only chunked view of the buffered bytes
   */
  def toChunkedByteBuffer: ChunkedByteBuffer = lock.synchronized {
    val chunks = new Array[ByteBuffer](blocks.length)
    var i = 0
    while (i < blocks.length) {
      // ByteBuffer.wrap yields a buffer with position() == 0, satisfying ChunkedByteBuffer's
      // contract; the underlying payload array is immutable so the zero-copy share is safe.
      chunks(i) = ByteBuffer.wrap(blocks(i).payload)
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
 * Companion holding the immutable block representation and the shared CRC32C helper. Keeping the
 * checksum routine here (rather than inline) keeps the per-append hot path allocation-light and the
 * algorithm choice -- JDK [[java.util.zip.CRC32C]] -- in exactly one place.
 */
private[spark] object StreamingBuffer {

  /**
   * An immutable, sealed buffer block: a 2 MB-bounded payload paired with the CRC32C checksum
   * computed over it. Payloads are never mutated after construction, which is what makes lock-free
   * reads and zero-copy [[ChunkedByteBuffer]] wrapping safe.
   *
   * @param payload  the framed block bytes (length is at most
   *                 [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]])
   * @param checksum the CRC32C checksum of `payload`, an unsigned 32-bit value held in a `Long`
   */
  private final class Block(val payload: Array[Byte], val checksum: Long)

  /**
   * Computes the CRC32C checksum of `payload` using the JDK primitive, returned as a `Long`
   * carrying the unsigned 32-bit checksum (matching `java.util.zip.Checksum#getValue`).
   *
   * @param payload the bytes to checksum
   * @return the CRC32C checksum of `payload`
   */
  private def computeChecksum(payload: Array[Byte]): Long = {
    val crc = new CRC32C()
    crc.update(payload, 0, payload.length)
    crc.getValue()
  }
}
