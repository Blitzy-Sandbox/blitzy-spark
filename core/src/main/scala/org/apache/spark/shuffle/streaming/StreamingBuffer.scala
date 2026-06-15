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

import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * A bounded, per-partition in-memory buffer of framed shuffle bytes for the opt-in streaming
 * shuffle backend.
 *
 * Each instance accumulates the map-side output destined for a single reduce partition,
 * framing the incoming bytes into fixed-size blocks of at most
 * [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]] (2 MB). Every sealed block carries a CRC32C
 * checksum (JDK `java.util.zip.CRC32C`) computed once over exactly its payload bytes, so the
 * wire-envelope and read-verification paths can attach and re-check integrity without ever
 * recomputing it. A trailing, not-yet-full block (the "pending" remainder) is kept
 * exact-sized so an empty or lightly-used buffer never reserves a whole block up front,
 * keeping the per-partition memory footprint bounded.
 *
 * ==Dual-channel wire/persist invariant==
 * This buffer holds each block as its raw payload bytes plus a CRC32C, and exposes two
 * families of accessor over one canonical byte layout. The ''payload accessors''
 * ([[readBlock]] / [[checksumOf]] / [[blockWithChecksum]]) hand out a single block's unframed
 * payload and its checksum: the raw material the writer wraps into a frame. The ''frame
 * accessors'' ([[envelopeOf]] / [[toChunkedByteBuffer]] / [[toByteArray]]) emit the canonical
 * [[StreamingBlockEnvelope]] framing for those same blocks: a 32-byte big-endian header
 * followed by the payload. The bytes produced by [[toChunkedByteBuffer]] / [[toByteArray]] are
 * therefore byte-for-byte identical to the bytes the same blocks travel as on the wire and to
 * the bytes written to disk on spill; this is the dual-channel invariant (AAP 0.4.2). Because
 * the spilled view and the streamed view are the same enveloped bytes, a partition can be
 * spilled to disk and later resumed or re-streamed transparently to the reader, which parses
 * either source with [[StreamingBlockEnvelope.parse]]. Every sealed block carries exactly 2 MB
 * of payload and only the final (pending) block may be shorter.
 *
 * ==Concurrency==
 * The map-side writer appends while the spill manager may concurrently inspect, read, or
 * clear the buffer. Frequently-polled metadata ([[size]], [[numBlocks]], [[isFull]],
 * [[utilizationPercent]], [[lastAccess]]) is backed by lock-free atomics and a volatile
 * timestamp, so the spill manager's 100 ms scan never contends with the writer. The mutable
 * block layout is guarded by a single private monitor held only for the brief structural
 * critical sections (framing on append, snapshotting on read or spill, and reset on clear),
 * avoiding any coarse lock that would throttle the append hot path.
 *
 * Only genuine data operations ([[append]], [[readBlock]], [[checksumOf]],
 * [[blockWithChecksum]], [[envelopeOf]], [[toChunkedByteBuffer]], [[toByteArray]]) update
 * [[lastAccess]]; pure metadata polls deliberately do not, so the LRU ordering the spill
 * manager relies on to evict the largest, least-recently-used partitions first stays
 * meaningful.
 *
 * This type coexists with the sort-based shuffle path and is constructed only when the
 * streaming backend is active; it neither reads configuration nor touches
 * [[org.apache.spark.SparkEnv]] directly.
 *
 * @param shuffleId the shuffle id this buffer belongs to
 * @param mapId the map task id that produced the buffered output
 * @param partitionId the reduce partition id the buffered bytes are destined for
 * @param capacityBytes the soft capacity used by [[isFull]] and [[utilizationPercent]]; the
 *                      buffer never hard-rejects an [[append]], so a transient overshoot is
 *                      possible until the spill manager reclaims memory
 */
private[spark] class StreamingBuffer(
    val shuffleId: Int,
    val mapId: Long,
    val partitionId: Int,
    val capacityBytes: Long) {

  import StreamingBuffer.Block

  // Monitor guarding the mutable block layout: `sealedBlocks`, `pending`, and `pendingLen`.
  // A dedicated lock object (rather than `this`) prevents external code from interfering with
  // the buffer's internal synchronization.
  private val lock = new Object

  // Sealed, immutable, fully-framed 2 MB blocks, in append order. Guarded by `lock`.
  private val sealedBlocks = new ArrayBuffer[Block]()

  // The not-yet-sealed trailing bytes, always strictly smaller than one block. The array is
  // kept exact-sized (`pending.length == pendingLen`) so a lightly-used buffer never reserves
  // a full block. Guarded by `lock`.
  private var pending: Array[Byte] = Array.emptyByteArray
  private var pendingLen: Int = 0

  // Lock-free metadata for the spill manager's frequent polling path. `currentSizeBytes` is
  // the total buffered byte count; `blockCount` is the number of readable blocks (sealed plus
  // the pending one, if any); `lastAccessNanos` is the LRU timestamp.
  private val currentSizeBytes = new AtomicLong(0L)
  private val blockCount = new AtomicInteger(0)
  @volatile private var lastAccessNanos: Long = System.nanoTime()

  /**
   * Appends raw bytes to this buffer, framing them into blocks of at most
   * [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]] (2 MB).
   *
   * Incoming bytes first top up any partial trailing block; once a block reaches the full
   * 2 MB it is sealed (its CRC32C is computed once and retained), and the remaining input is
   * carved into additional full blocks with any final leftover retained as the new pending
   * remainder. A `null` or empty array is a no-op and does not update [[lastAccess]].
   *
   * @param bytes the bytes to append; may be empty
   */
  def append(bytes: Array[Byte]): Unit = {
    if (bytes != null && bytes.length > 0) {
      val blockSize = StreamingShuffleConfig.BLOCK_SIZE_BYTES
      lock.synchronized {
        val total = bytes.length
        var offset = 0
        // 1) Top up an existing partial block before starting new ones.
        if (pendingLen > 0) {
          val take = math.min(blockSize - pendingLen, total)
          val merged = new Array[Byte](pendingLen + take)
          System.arraycopy(pending, 0, merged, 0, pendingLen)
          System.arraycopy(bytes, 0, merged, pendingLen, take)
          pending = merged
          pendingLen = merged.length
          offset += take
          if (pendingLen == blockSize) {
            sealPending()
          }
        }
        // 2) Emit as many full, exactly-sized blocks as the remaining input allows.
        while (total - offset >= blockSize) {
          val block = bytes.slice(offset, offset + blockSize)
          sealedBlocks += new Block(block, computeChecksum(block, 0, block.length))
          offset += blockSize
        }
        // 3) Retain the final leftover (smaller than one block) as the new pending remainder.
        val remaining = total - offset
        if (remaining > 0) {
          pending = bytes.slice(offset, total)
          pendingLen = remaining
        }
        currentSizeBytes.addAndGet(total.toLong)
        blockCount.set(sealedBlocks.size + (if (pendingLen > 0) 1 else 0))
        touch()
      }
    }
  }

  /** @return the total number of bytes currently buffered (sealed blocks plus pending). */
  def size: Long = currentSizeBytes.get()

  /** @return the number of readable blocks: full sealed blocks plus the pending one, if any. */
  def numBlocks: Int = blockCount.get()

  /** @return `true` once the buffered size reaches or exceeds the soft [[capacityBytes]]. */
  def isFull: Boolean = size >= capacityBytes

  /**
   * @return the buffered size as a percentage of [[capacityBytes]] in `[0, 100]`. A
   *         non-positive capacity yields `0.0`, and the result is capped at `100.0` even if a
   *         transient overshoot pushed the size above capacity.
   */
  def utilizationPercent: Double =
    if (capacityBytes <= 0) 0.0 else math.min(100.0, size * 100.0 / capacityBytes)

  /**
   * @return the monotonic [[System.nanoTime]] timestamp of the most recent data operation,
   *         used by the spill manager for least-recently-used eviction ordering. Reading this
   *         value does not itself count as an access.
   */
  def lastAccess: Long = lastAccessNanos

  /**
   * Returns the bytes of the block at the given index.
   *
   * For a sealed block this returns the buffer's internal, immutable payload array without
   * copying; callers must treat it as read-only. For the pending tail block a fresh defensive
   * copy is returned. When both the bytes and the checksum of the tail block are needed
   * together, prefer [[blockWithChecksum]] so they cannot be observed inconsistently across a
   * concurrent [[append]].
   *
   * @param index the block index in `[0, numBlocks)`
   * @return the block's bytes
   * @throws IndexOutOfBoundsException if `index` is outside `[0, numBlocks)`
   */
  def readBlock(index: Int): Array[Byte] = lock.synchronized {
    val sealedCount = sealedBlocks.size
    val result =
      if (index >= 0 && index < sealedCount) {
        sealedBlocks(index).data
      } else if (index == sealedCount && pendingLen > 0) {
        pending.clone()
      } else {
        throw indexError(index, sealedCount)
      }
    touch()
    result
  }

  /**
   * Returns the CRC32C checksum of the block at the given index.
   *
   * For a sealed block the checksum was computed once at seal time and is returned directly.
   * For the pending tail block it is computed over the current pending bytes.
   *
   * @param index the block index in `[0, numBlocks)`
   * @return the unsigned CRC32C value held in the low 32 bits of the returned `Long`
   * @throws IndexOutOfBoundsException if `index` is outside `[0, numBlocks)`
   */
  def checksumOf(index: Int): Long = lock.synchronized {
    val sealedCount = sealedBlocks.size
    val result =
      if (index >= 0 && index < sealedCount) {
        sealedBlocks(index).checksum
      } else if (index == sealedCount && pendingLen > 0) {
        computeChecksum(pending, 0, pendingLen)
      } else {
        throw indexError(index, sealedCount)
      }
    touch()
    result
  }

  /**
   * Atomically returns both the bytes and the CRC32C checksum of the block at the given index.
   *
   * This is the preferred accessor for the wire-envelope path: because the bytes and checksum
   * are captured under a single lock acquisition, they are always mutually consistent even for
   * the pending tail block, which a concurrent [[append]] may otherwise grow between separate
   * [[readBlock]] and [[checksumOf]] calls. Sealed-block bytes are returned without copying
   * (read-only); the pending tail is snapshotted defensively and its checksum is computed over
   * that exact snapshot.
   *
   * @param index the block index in `[0, numBlocks)`
   * @return a `(bytes, checksum)` pair for the block
   * @throws IndexOutOfBoundsException if `index` is outside `[0, numBlocks)`
   */
  def blockWithChecksum(index: Int): (Array[Byte], Long) = lock.synchronized {
    val sealedCount = sealedBlocks.size
    val result =
      if (index >= 0 && index < sealedCount) {
        val block = sealedBlocks(index)
        (block.data, block.checksum)
      } else if (index == sealedCount && pendingLen > 0) {
        val snapshot = pending.clone()
        (snapshot, computeChecksum(snapshot, 0, snapshot.length))
      } else {
        throw indexError(index, sealedCount)
      }
    touch()
    result
  }

  /**
   * Returns the canonical [[StreamingBlockEnvelope]] frame for the block at the given index:
   * the exact 32-byte-header-plus-payload bytes the block travels as on the wire and that are
   * written to disk on spill. This is the per-block unit of the dual-channel invariant. The
   * envelope carries this buffer's `shuffleId` and `mapId`, the [[partitionId]] as its
   * `reduceId`, the block index as its sequence number, and the block's CRC32C (reused from
   * seal time, never recomputed). The pending tail block is snapshotted defensively first.
   *
   * @param index the block index in `[0, numBlocks)`
   * @return the wire/persist envelope for the block
   * @throws IndexOutOfBoundsException if `index` is outside `[0, numBlocks)`
   */
  def envelopeOf(index: Int): StreamingBlockEnvelope = lock.synchronized {
    val result = envelopeAt(index)
    touch()
    result
  }

  /**
   * Exposes the buffered bytes as a [[ChunkedByteBuffer]] for spilling via
   * `BlockManager.putBytes`, without flattening them into a single contiguous array.
   *
   * Each block becomes one chunk holding its complete [[StreamingBlockEnvelope]] frame (a
   * 32-byte big-endian header followed by the payload). [[StreamingBlockEnvelope.serialize]]
   * returns a buffer positioned at `0`, as [[ChunkedByteBuffer]] requires. Per the
   * dual-channel invariant, concatenating the returned chunks yields exactly the bytes the
   * same blocks travel as on the wire and the bytes written to disk on spill, so spilled and
   * streamed bytes are interchangeable; recover a block's payload with [[readBlock]] or by
   * parsing a frame via [[StreamingBlockEnvelope.parse]].
   *
   * @return a chunked, read-only view of the enveloped bytes (empty when nothing is buffered)
   */
  def toChunkedByteBuffer: ChunkedByteBuffer = lock.synchronized {
    val sealedCount = sealedBlocks.size
    val frameCount = sealedCount + (if (pendingLen > 0) 1 else 0)
    val chunks = new Array[ByteBuffer](frameCount)
    var i = 0
    while (i < frameCount) {
      chunks(i) = envelopeAt(i).serialize
      i += 1
    }
    touch()
    new ChunkedByteBuffer(chunks)
  }

  /**
   * Materializes all buffered bytes into a single contiguous array.
   *
   * This is a convenience that honors the same dual-channel invariant as
   * [[toChunkedByteBuffer]]: the returned array equals the in-order concatenation of every
   * block's [[StreamingBlockEnvelope]] frame, so it is byte-for-byte identical to the chunked
   * spill view and to the on-wire bytes. It is intended for tests and small buffers; the
   * spill path should prefer [[toChunkedByteBuffer]], which supports payloads larger than a
   * single array.
   *
   * @return a fresh array holding every buffered byte, as enveloped frames, in order
   * @throws IllegalArgumentException if the enveloped size exceeds the maximum array length
   */
  def toByteArray: Array[Byte] = lock.synchronized {
    val sealedCount = sealedBlocks.size
    val frameCount = sealedCount + (if (pendingLen > 0) 1 else 0)
    // Enveloped total = raw payload bytes + one 32-byte header per framed block.
    val headerBytes = StreamingBlockEnvelope.HEADER_BYTES.toLong
    val totalSize = currentSizeBytes.get() + headerBytes * frameCount
    require(totalSize <= Int.MaxValue,
      s"cannot materialize $totalSize bytes into a single array (exceeds ${Int.MaxValue})")
    val out = new Array[Byte](totalSize.toInt)
    var pos = 0
    var i = 0
    while (i < frameCount) {
      val frame = envelopeAt(i).toByteArray
      System.arraycopy(frame, 0, out, pos, frame.length)
      pos += frame.length
      i += 1
    }
    touch()
    out
  }

  /**
   * Releases all buffered blocks and resets every counter to zero.
   *
   * Called after the buffered partition has been spilled to disk or acknowledged by the
   * consumer. Dropping the references to the sealed block arrays makes them eligible for
   * garbage collection, reclaiming the bulk of the buffer's footprint, and resets the
   * lock-free counters so subsequent metadata polls observe an empty buffer.
   */
  def clear(): Unit = lock.synchronized {
    sealedBlocks.clear()
    pending = Array.emptyByteArray
    pendingLen = 0
    currentSizeBytes.set(0L)
    blockCount.set(0)
    touch()
  }

  override def toString: String =
    s"StreamingBuffer(shuffleId=$shuffleId, mapId=$mapId, partitionId=$partitionId, " +
      s"sizeBytes=$size, numBlocks=$numBlocks, capacityBytes=$capacityBytes)"

  // ---------------------------------------------------------------------------------------
  // Internal helpers. All callers below already hold `lock` unless noted otherwise.
  // ---------------------------------------------------------------------------------------

  // Records the current time as the most recent data-access timestamp for LRU ordering.
  // Writing the volatile field is cheap and lock-independent; it is invoked from every data
  // operation but never from a pure metadata poll, so the LRU ordering stays meaningful.
  private def touch(): Unit = {
    lastAccessNanos = System.nanoTime()
  }

  // Seals the current pending block, which the caller guarantees is exactly one block in
  // size, computing and retaining its CRC32C and resetting the pending remainder to empty.
  private def sealPending(): Unit = {
    sealedBlocks += new Block(pending, computeChecksum(pending, 0, pendingLen))
    pending = Array.emptyByteArray
    pendingLen = 0
  }

  // Computes the CRC32C of `length` bytes of `data` starting at `offset`. Thread-safe: a
  // fresh CRC32C is used per call so concurrent readers never share checksum state.
  private def computeChecksum(data: Array[Byte], offset: Int, length: Int): Long = {
    val crc = new CRC32C()
    crc.update(data, offset, length)
    crc.getValue
  }

  // Builds the canonical StreamingBlockEnvelope frame for the block at `index`. Assumes the
  // caller already holds `lock`, and intentionally does NOT update the LRU timestamp so a
  // serialization helper can frame every block as a single logical access. The sealed block's
  // CRC32C is reused (narrowed to the low 32 bits to match the envelope's Int field); the
  // pending tail is snapshotted before its checksum is computed so a concurrent append cannot
  // tear the frame. `reduceId` is this buffer's partition and the sequence number is the index.
  private def envelopeAt(index: Int): StreamingBlockEnvelope = {
    val sealedCount = sealedBlocks.size
    val (payload, checksum) =
      if (index >= 0 && index < sealedCount) {
        val block = sealedBlocks(index)
        (block.data, block.checksum)
      } else if (index == sealedCount && pendingLen > 0) {
        val snapshot = pending.clone()
        (snapshot, computeChecksum(snapshot, 0, snapshot.length))
      } else {
        throw indexError(index, sealedCount)
      }
    new StreamingBlockEnvelope(
      shuffleId, mapId, partitionId, index.toLong, (checksum & 0xFFFFFFFFL).toInt, payload)
  }

  // Builds a descriptive out-of-bounds error for the given index and current sealed count.
  private def indexError(index: Int, sealedCount: Int): IndexOutOfBoundsException = {
    val count = sealedCount + (if (pendingLen > 0) 1 else 0)
    new IndexOutOfBoundsException(s"block index $index out of range [0, $count)")
  }
}

/**
 * Companion holding the immutable block representation used internally by [[StreamingBuffer]].
 */
private[spark] object StreamingBuffer {

  /**
   * An immutable, fully-framed block: the payload bytes together with the CRC32C checksum
   * computed once over exactly those bytes. Instances are created only while holding the
   * owning buffer's lock and are never mutated afterwards, which is what lets
   * [[StreamingBuffer.readBlock]] hand out the payload array without copying.
   *
   * @param data the block payload (exactly [[StreamingShuffleConfig.BLOCK_SIZE_BYTES]] for a
   *             sealed block)
   * @param checksum the unsigned CRC32C of `data`, held in the low 32 bits of the `Long`
   */
  private final class Block(val data: Array[Byte], val checksum: Long)
}
