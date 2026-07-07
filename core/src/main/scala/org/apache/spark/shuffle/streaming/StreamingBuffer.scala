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

import java.io.ByteArrayOutputStream
import java.util.zip.CRC32C

import org.apache.spark.annotation.Since

/**
 * A per-partition, in-memory buffer holding the serialized bytes destined for a single reduce
 * partition of a single map output during a streaming shuffle.
 *
 * This is the fundamental data primitive of the streaming-shuffle write path and is deliberately
 * kept free of any dependency on the rest of the streaming subsystem so it can be produced,
 * spilled and framed independently:
 *
 *  - `StreamingShuffleWriter` appends serialized records for one reduce partition into an instance
 *    of this class.
 *  - `MemorySpillManager` inspects [[currentSize]] and [[lastAccessMillis]] to select the largest /
 *    least-recently-used buffers to spill to disk when buffer utilization crosses the spill
 *    threshold.
 *  - The network layer (`network.StreamingBlockEnvelope`) frames [[snapshot]] output into blocks of
 *    at most 2 MB, stamping each block with [[checksum]] for corruption detection.
 *
 * '''Incremental CRC32C.''' A running CRC32C is advanced on every [[append]] rather than recomputed
 * on demand. This keeps integrity maintenance to a single pass over the data and holds the overhead
 * well under the streaming-shuffle budget of 1% CPU. The JDK 17 built-in `java.util.zip.CRC32C` is
 * used, so no third-party checksum dependency is introduced (the same primitive backs the existing
 * sort-path checksum).
 *
 * '''Block framing.''' A buffer instance may grow beyond 2 MB and is intentionally not capped here.
 * Splitting the buffered bytes into wire blocks of at most 2 MB is the responsibility of the
 * network envelope / transport layer, not of this buffer.
 *
 * '''Thread-safety.''' The map-side write path appends to a given partition buffer from a single
 * task thread, but the spill manager may read a buffer concurrently while scanning for spill
 * candidates. All operations that mutate or copy the compound state (backing store plus running
 * checksum) -- [[append]], [[snapshot]], [[reset]] and [[checksum]] -- are guarded by a private
 * monitor so a reader always observes a consistent view. [[size]] / [[currentSize]] rely on the
 * backing `ByteArrayOutputStream`'s own internal synchronization, and [[lastAccessMillis]] is a
 * `@volatile` read; both are kept lock-light so the spill manager's frequent polling does not
 * contend with the append hot path.
 *
 * @param shuffleId       the shuffle this buffer belongs to
 * @param mapId           the map (producer) task whose output this buffer holds
 * @param reduceId        the reduce (consumer) partition this buffer feeds
 * @param initialCapacity initial capacity in bytes of the backing store; a sizing hint only, as the
 *                        store grows automatically as bytes are appended
 */
@Since("4.2.0")
private[spark] class StreamingBuffer(
    val shuffleId: Int,
    val mapId: Long,
    val reduceId: Int,
    initialCapacity: Int = StreamingBuffer.DEFAULT_INITIAL_CAPACITY) {

  require(initialCapacity > 0,
    s"initialCapacity must be positive but was $initialCapacity")

  /**
   * Private monitor guarding compound mutations and snapshots of [[store]] plus [[crc]] so that a
   * concurrent reader (e.g. the spill manager) always observes a consistent buffer view. A
   * dedicated lock object is used rather than `this` to avoid leaking the monitor to callers.
   */
  private val lock = new Object()

  /**
   * Growable backing store for the buffered bytes. A `ByteArrayOutputStream` is chosen over a
   * manually grown `Array[Byte]` because it already provides amortized growth and internally
   * synchronized `size` / `write` / `toByteArray` / `reset`, which is reused for cheap size reads.
   *
   * Declared `@volatile var` (not `val`) so [[reset]] can swap in a fresh, small stream and release
   * the retained backing array; this is what actually frees heap on reclaim, since
   * `ByteArrayOutputStream.reset` would keep the (possibly multi-MB) grown array allocated. The
   * `@volatile` reference lets the lock-free [[size]] reader observe swaps safely.
   */
  @volatile private var store = new ByteArrayOutputStream(initialCapacity)

  /** Running CRC32C over every appended byte. Not thread-safe, hence always guarded by [[lock]]. */
  private val crc = new CRC32C()

  /**
   * Wall-clock timestamp, in milliseconds, of the most recent [[append]] or [[snapshot]], used by
   * the spill manager for least-recently-used eviction ordering. Declared `@volatile` so it can be
   * read via [[lastAccessMillis]] without acquiring [[lock]].
   */
  @volatile private var lastAccessTime: Long = System.currentTimeMillis()

  /**
   * Append a slice of `bytes` to this buffer, advancing the running CRC32C over the same slice and
   * refreshing the LRU access timestamp. Guarded by [[lock]] so the store and checksum stay in
   * lockstep even if a reader scans this buffer concurrently.
   *
   * @param bytes  source array; must not be null
   * @param offset start index of the slice within `bytes`; must be non-negative
   * @param length number of bytes to append; must be non-negative and fit within `bytes` from
   *               `offset`
   */
  def append(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    require(bytes != null, "bytes must not be null")
    // Compare against (length - offset) rather than (offset + length) to avoid integer overflow.
    require(offset >= 0 && length >= 0 && length <= bytes.length - offset,
      s"Invalid append range: offset=$offset, length=$length, arrayLength=${bytes.length}")
    lock.synchronized {
      store.write(bytes, offset, length)
      crc.update(bytes, offset, length)
      lastAccessTime = System.currentTimeMillis()
    }
  }

  /**
   * Append the entirety of `bytes` to this buffer. Convenience overload equivalent to
   * `append(bytes, 0, bytes.length)`.
   *
   * @param bytes source array; must not be null
   */
  def append(bytes: Array[Byte]): Unit = {
    require(bytes != null, "bytes must not be null")
    append(bytes, 0, bytes.length)
  }

  /**
   * The number of bytes currently buffered. Backed by the `ByteArrayOutputStream`'s own
   * synchronized `size`, so it is safe to call concurrently with [[append]] without holding
   * [[lock]] and is cheap enough for the spill manager's frequent utilization polling.
   */
  def size: Long = store.size().toLong

  /** Alias of [[size]]: the buffered byte count feeding memory accounting and utilization. */
  def currentSize: Long = size

  /**
   * The running CRC32C as a 32-bit checksum. `CRC32C.getValue` returns a `Long` whose low 32 bits
   * carry the checksum; those bits are returned here as an `Int`, matching the width stored by
   * `network.StreamingBlockEnvelope`. Guarded by [[lock]] because `CRC32C` is not thread-safe.
   */
  def checksum: Int = lock.synchronized {
    (crc.getValue() & 0xFFFFFFFFL).toInt
  }

  /**
   * Return a fresh copy of the currently buffered bytes for the wire and/or persist channels and
   * refresh the LRU access timestamp. The returned array is a defensive copy, so callers may retain
   * or mutate it independently of this buffer.
   */
  def snapshot(): Array[Byte] = lock.synchronized {
    lastAccessTime = System.currentTimeMillis()
    store.toByteArray()
  }

  /**
   * The wall-clock time, in milliseconds, of the last append or snapshot. Consumed by the spill
   * manager to order buffers for least-recently-used eviction. Read from a `@volatile` field, so it
   * never contends on [[lock]].
   */
  def lastAccessMillis: Long = lastAccessTime

  /**
   * Clear the buffered bytes and reset the running CRC32C to its initial state, typically after the
   * buffered data has been acknowledged and reclaimed. A fresh backing store sized at
   * `initialCapacity` is installed so the previously grown array becomes eligible for garbage
   * collection, actually releasing heap rather than merely zeroing the byte count. The access
   * timestamp is refreshed.
   */
  def reset(): Unit = lock.synchronized {
    // Replace (not ByteArrayOutputStream.reset) so the grown backing array is released for GC.
    store = new ByteArrayOutputStream(initialCapacity)
    crc.reset()
    lastAccessTime = System.currentTimeMillis()
  }

  override def toString: String =
    s"StreamingBuffer(shuffleId=$shuffleId, mapId=$mapId, reduceId=$reduceId, size=$size)"
}

/**
 * Companion object holding shared constants for [[StreamingBuffer]].
 */
private[spark] object StreamingBuffer {

  /**
   * Default initial capacity (64 KiB) for a freshly allocated buffer's backing store. Chosen to
   * amortize early growth for a typical shuffle partition while keeping a small fixed footprint;
   * the store still grows automatically for larger partitions.
   */
  val DEFAULT_INITIAL_CAPACITY: Int = 64 * 1024
}
