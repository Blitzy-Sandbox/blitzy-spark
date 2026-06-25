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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.zip.CRC32C

import org.apache.spark.internal.Logging

/**
 * A per-partition, in-memory buffer that accumulates serialized shuffle bytes destined for a
 * single reduce partition in the streaming shuffle data path.
 *
 * Each instance owns the bytes for exactly one reduce partition (identified by [[partitionId]]).
 * As the producer (`StreamingShuffleWriter`) appends serialized records, the buffer:
 *   - accumulates the raw bytes in an internal [[java.io.ByteArrayOutputStream]];
 *   - maintains a running CRC32C checksum (`java.util.zip.CRC32C`, available on JDK 9+) so the
 *     consumer can validate block integrity without making a second pass over the data;
 *   - records a monotonically updated "last access" timestamp (in nanoseconds) that the
 *     `MemorySpillManager` uses for least-recently-used (LRU) eviction ordering when memory
 *     pressure forces a disk spill;
 *   - exposes lock-free atomic counters ([[size]], [[lastAccess]]) so the spill manager's polling
 *     loop can observe buffer state cheaply without contending on the producer's write path.
 *
 * '''Concurrency model.''' The underlying `ByteArrayOutputStream` and `CRC32C` are not
 * thread-safe, and the spill manager may snapshot a buffer (via [[toBytes]] / [[checksum]])
 * concurrently with the producer appending to it. All mutation and snapshotting of those two
 * structures is therefore guarded by a single private monitor, guaranteeing that a spill snapshot
 * is internally consistent: the bytes returned by [[toBytes]] and the value reported by
 * [[checksum]] always reflect the same prefix of appended data. The [[size]], [[lastAccess]] and
 * spilled flag are backed by atomics and can be read without acquiring the monitor.
 *
 * '''Block sizing.''' The 2 MB canonical streaming block size is enforced by the WRITER, not by
 * this buffer. This class simply accumulates bytes and reports its [[size]] so that the writer
 * and the spill manager can decide when to flush a block or spill the buffer to disk.
 *
 * @param partitionId     the reduce partition this buffer accumulates bytes for
 * @param initialCapacity the initial capacity, in bytes, of the backing byte array; a small value
 *                        keeps per-partition memory overhead low until data actually arrives
 */
private[spark] class StreamingBuffer(
    val partitionId: Int,
    initialCapacity: Int = 64 * 1024)
  extends Logging {

  /**
   * Dedicated monitor guarding all access to the non-thread-safe [[baos]] and [[crc]]. A private
   * lock object is used (rather than `this`) so that external code cannot accidentally block the
   * buffer's write path by synchronizing on the instance.
   */
  private val lock = new Object()

  /** Backing byte accumulator. Guarded by [[lock]]. */
  private val baos = new ByteArrayOutputStream(initialCapacity)

  /** Running CRC32C over all bytes appended since the last reset. Guarded by [[lock]]. */
  private val crc = new CRC32C()

  /** Total number of bytes appended since construction or the last [[reset]]. */
  private val bytesWritten = new AtomicLong(0L)

  /** Timestamp (`System.nanoTime`) of the most recent read or write, used for LRU ordering. */
  private val lastAccessNanos = new AtomicLong(System.nanoTime())

  /** Whether this buffer has already been spilled to disk by the spill manager. */
  private val spilled = new AtomicBoolean(false)

  /**
   * Append a slice of `bytes` to this buffer, updating the running checksum, the cumulative byte
   * count and the LRU access timestamp. The checksum and byte accumulator are updated atomically
   * with respect to a concurrent [[toBytes]] / [[checksum]] snapshot.
   *
   * @param bytes the source array
   * @param off   the start offset within `bytes`
   * @param len   the number of bytes to append
   */
  def append(bytes: Array[Byte], off: Int, len: Int): Unit = lock.synchronized {
    baos.write(bytes, off, len)
    crc.update(bytes, off, len)
    bytesWritten.addAndGet(len.toLong)
    lastAccessNanos.set(System.nanoTime())
  }

  /** Convenience overload that appends the entire `bytes` array. */
  def append(bytes: Array[Byte]): Unit = append(bytes, 0, bytes.length)

  /** The total number of bytes appended since construction or the last [[reset]]. */
  def size: Long = bytesWritten.get()

  /**
   * The CRC32C checksum of all bytes appended so far. Computed under [[lock]] so the returned
   * value is consistent with a concurrent [[toBytes]] snapshot.
   */
  def checksum: Long = lock.synchronized {
    crc.getValue()
  }

  /**
   * Return a defensive copy of the bytes accumulated so far and refresh the LRU access timestamp.
   * The snapshot is taken under [[lock]] so it is internally consistent with [[checksum]].
   */
  def toBytes: Array[Byte] = lock.synchronized {
    val snapshot = baos.toByteArray()
    lastAccessNanos.set(System.nanoTime())
    snapshot
  }

  /** Timestamp (`System.nanoTime`) of the most recent read or write, for LRU ordering. */
  def lastAccess: Long = lastAccessNanos.get()

  /** Whether this buffer has been spilled to disk. */
  def isSpilled: Boolean = spilled.get()

  /** Mark this buffer as having been spilled to disk. Idempotent. */
  def markSpilled(): Unit = {
    spilled.set(true)
    logTrace(s"StreamingBuffer(partition=$partitionId) marked as spilled to disk")
  }

  /**
   * Clear all accumulated state so the buffer can be reused after its contents have been
   * reclaimed (for example, acknowledged by the consumer or spilled to disk). Resets the byte
   * accumulator, the running checksum, the cumulative byte count and the spilled flag.
   */
  def reset(): Unit = {
    lock.synchronized {
      baos.reset()
      crc.reset()
      bytesWritten.set(0L)
      spilled.set(false)
    }
    logTrace(s"StreamingBuffer(partition=$partitionId) reset for reuse")
  }
}
