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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{atLeastOnce, mock, never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{BlockId, BlockManager, ByteBufferBlockData, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Unit tests for [[MemorySpillManager]], the threshold-driven disk-spill safety valve of the
 * opt-in streaming shuffle backend.
 *
 * The suite is pure and deterministic: it needs no SparkContext or MetricsSystem and never
 * touches real disk. The BlockManager and MemoryManager are mocked so spills report success
 * with no I/O and the spill denominator is a known, controllable value. The tests assert
 * behavior and the AAP's memory-exhaustion-prevention invariants rather than poll timing:
 *
 *  - spilling is gated on aggregate buffer utilization reaching the configured threshold
 *    (default 80%) measured against MemoryManager.maxOnHeapStorageMemory;
 *  - a spill persists the buffer with StorageLevel.DISK_ONLY and increments spillCount;
 *  - the largest buffers are evicted first.
 *
 * It also checks that the 100 ms poll thread starts and stops cleanly without leaking.
 */
class MemorySpillManagerSuite extends SparkFunSuite with Matchers {

  // 100 MB default denominator; an individual test may override it to size the 80% threshold
  // so that a small, cheaply-allocated buffer deterministically crosses or stays below it.
  private def newManager(maxMem: Long = 100L * 1024 * 1024)
      : (MemorySpillManager, BlockManager, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val bm = mock(classOf[BlockManager])
    // Stub the only BlockManager surface the spill path uses. The trailing (any()) matches
    // the implicit ClassTag of putBytes[T]; the fourth any() matches the defaulted tellMaster.
    when(bm.putBytes(any(), any(), any(), any())(any())).thenReturn(true)
    val mm = mock(classOf[MemoryManager])
    when(mm.maxOnHeapStorageMemory).thenReturn(maxMem)
    val metrics = new StreamingShuffleMetrics
    (new MemorySpillManager(cfg, bm, mm, metrics), bm, metrics)
  }

  // Builds a per-partition buffer whose live size is exactly sizeBytes (one sub-2MB block).
  // The soft capacity equals the size; capacity is irrelevant to maybeSpill, which sizes the
  // spill decision against maxOnHeapStorageMemory, not the buffer's own capacity.
  private def filledBuffer(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sizeBytes: Int): StreamingBuffer = {
    val buffer = new StreamingBuffer(shuffleId, mapId, partitionId, sizeBytes.toLong)
    buffer.append(new Array[Byte](sizeBytes))
    buffer
  }

  // Derives the spill-manager registry key from a buffer's identity fields.
  private def keyOf(buffer: StreamingBuffer): MemorySpillManager.BufferKey =
    MemorySpillManager.BufferKey(buffer.shuffleId, buffer.mapId, buffer.partitionId)

  // A BlockManager whose putBytes captures the spilled bytes into `store` and whose getLocalBytes
  // serves them back as a ByteBufferBlockData, so a spill can be exercised end-to-end (store then
  // read back) without touching real disk. Returns the manager, the backing store, and metrics.
  private def storingManager(maxMem: Long = 100L * 1024 * 1024)
      : (MemorySpillManager, ConcurrentHashMap[BlockId, Array[Byte]], StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val store = new ConcurrentHashMap[BlockId, Array[Byte]]()
    val bm = mock(classOf[BlockManager])
    when(bm.putBytes(any(), any(), any(), any())(any())).thenAnswer { (inv: InvocationOnMock) =>
      val blockId = inv.getArgument[BlockId](0)
      val bytes = inv.getArgument[ChunkedByteBuffer](1)
      store.put(blockId, bytes.toArray)
      true
    }
    when(bm.getLocalBytes(any())).thenAnswer { (inv: InvocationOnMock) =>
      val blockId = inv.getArgument[BlockId](0)
      Option(store.get(blockId))
        .map(arr => new ByteBufferBlockData(new ChunkedByteBuffer(ByteBuffer.wrap(arr)), false))
    }
    val mm = mock(classOf[MemoryManager])
    when(mm.maxOnHeapStorageMemory).thenReturn(maxMem)
    val metrics = new StreamingShuffleMetrics
    (new MemorySpillManager(cfg, bm, mm, metrics), store, metrics)
  }

  // Sums the payload bytes across every StreamingBlockEnvelope frame in an enveloped byte array,
  // skipping the 32-byte headers. Used to assert payload conservation across a concurrent spill.
  private def sumPayloadBytes(enveloped: Array[Byte]): Long = {
    val bb = ByteBuffer.wrap(enveloped)
    var total = 0L
    while (bb.remaining() >= StreamingBlockEnvelope.HEADER_BYTES) {
      val env = StreamingBlockEnvelope.parse(bb)
      total += env.payloadLength
      bb.position(bb.position() + StreamingBlockEnvelope.HEADER_BYTES + env.payloadLength)
    }
    total
  }

  test("no spill below the threshold") {
    // 80% of 10000 == an 8000-byte threshold; a 4000-byte buffer is well under it.
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    mgr.register(filledBuffer(0, 0L, 0, 4000))

    mgr.maybeSpill() mustBe 0L
    verify(bm, never()).putBytes(any(), any(), any(), any())(any())
    metrics.spillCount mustBe 0L
  }

  test("spill triggered at or above the 80% threshold via DISK_ONLY") {
    // 9000 bytes exceeds the 8000-byte (80% of 10000) threshold, so a spill must occur.
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    mgr.register(filledBuffer(1, 0L, 0, 9000))

    val reclaimed = mgr.maybeSpill()

    assert(reclaimed > 0L)
    // DISK_ONLY is mandatory: spilled bytes must be persisted to the disk store only.
    verify(bm, atLeastOnce())
      .putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
    assert(metrics.spillCount >= 1L)
  }

  test("spillBuffer writes via the BlockManager and reports success") {
    val (mgr, bm, metrics) = newManager()
    val buffer = filledBuffer(2, 3L, 4, 2048)
    mgr.register(buffer)

    mgr.spillBuffer(keyOf(buffer)) mustBe true
    verify(bm).putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
    metrics.spillCount mustBe 1L
    mgr.isSpilled(keyOf(buffer)) mustBe true
  }

  test("largest buffer is spilled first") {
    // threshold == 8000; total == 11000. Spilling the 6000-byte buffer drops the running
    // total to 5000 (< 8000), so the 5000-byte buffer is retained: largest-first eviction.
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    val large = filledBuffer(3, 0L, 0, 6000)
    val small = filledBuffer(3, 0L, 1, 5000)
    mgr.register(large)
    mgr.register(small)

    val reclaimed = mgr.maybeSpill()

    assert(reclaimed > 0L)
    mgr.isSpilled(keyOf(large)) mustBe true
    mgr.isSpilled(keyOf(small)) mustBe false
    metrics.spillCount mustBe 1L
    verify(bm).putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
  }

  test("reclaims memory within the 100ms reclaim SLA") {
    // The AAP mandates a 100 ms reclaim SLA. With a mocked BlockManager the synchronous spill is
    // sub-millisecond; warm the path once on a separate manager so first-call JIT does not inflate
    // the timed run, then assert the timed reclaim finishes within the design SLA (not seconds).
    val warm = newManager(maxMem = 10000L)._1
    warm.register(filledBuffer(40, 0L, 0, 9000))
    warm.maybeSpill()

    val (mgr, _, _) = newManager(maxMem = 10000L)
    mgr.register(filledBuffer(4, 0L, 0, 9000))

    val startNanos = System.nanoTime()
    val reclaimed = mgr.maybeSpill()
    val elapsedMs = (System.nanoTime() - startNanos) / 1000000L

    assert(reclaimed > 0L)
    assert(elapsedMs < StreamingShuffleConfig.SPILL_RECLAIM_SLA_MS,
      s"spill reclaim took ${elapsedMs}ms, exceeding the " +
        s"${StreamingShuffleConfig.SPILL_RECLAIM_SLA_MS}ms SLA")
  }

  test("start and stop manage the poll thread without leaking") {
    val (mgr, _, _) = newManager()
    try {
      mgr.start()
      mgr.isRunning mustBe true
    } finally {
      // Always stop a started manager so the 100 ms poll thread is never leaked.
      mgr.stop()
    }
    mgr.isRunning mustBe false
    // stop() is idempotent: a second stop must be a safe no-op and must not throw.
    mgr.stop()
    mgr.isRunning mustBe false
  }

  test("spill denominator is MemoryManager.maxOnHeapStorageMemory") {
    // Small denominator: 80% of 10000 == 8000, so a 9000-byte buffer exceeds it and spills.
    val (mgrSmall, bmSmall, _) = newManager(maxMem = 10000L)
    mgrSmall.register(filledBuffer(5, 0L, 0, 9000))
    assert(mgrSmall.maybeSpill() > 0L)
    verify(bmSmall, atLeastOnce())
      .putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())

    // Large denominator: the same 9000-byte buffer is far below 80% of 100 MB and must not
    // spill, proving the spill decision is sized against maxOnHeapStorageMemory.
    val (mgrLarge, bmLarge, metricsLarge) = newManager(maxMem = 100L * 1024 * 1024)
    mgrLarge.register(filledBuffer(6, 0L, 0, 9000))
    mgrLarge.maybeSpill() mustBe 0L
    verify(bmLarge, never()).putBytes(any(), any(), any(), any())(any())
    metricsLarge.spillCount mustBe 0L
  }

  test("maybeSpill is a no-op when no buffers are registered") {
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    mgr.registeredBufferCount mustBe 0
    mgr.maybeSpill() mustBe 0L
    verify(bm, never()).putBytes(any(), any(), any(), any())(any())
    metrics.spillCount mustBe 0L
  }

  test("maybeSpill stops once every buffer has been spilled") {
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    mgr.register(filledBuffer(7, 0L, 0, 9000))
    assert(mgr.maybeSpill() > 0L)
    val afterFirst = metrics.spillCount
    assert(afterFirst >= 1L)
    // The only buffer is now empty (size 0), so a second pass finds nothing to spill.
    mgr.maybeSpill() mustBe 0L
    metrics.spillCount mustBe afterFirst
    verify(bm, times(1)).putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
  }

  test("equal-size buffers spill least-recently-accessed first (LRU tie-break)") {
    val (mgr, _, metrics) = newManager(maxMem = 10000L)
    // Two equal-size (5000-byte) buffers total 10000 >= the 8000-byte (80%) threshold, but
    // spilling either one alone drops the running total to 5000 (< 8000). The selection orders
    // by (-size, lastAccess), so among equal sizes the least-recently-accessed buffer spills.
    val older = filledBuffer(8, 0L, 0, 5000)
    // Busy-wait one clock tick so `newer` gets a strictly greater access stamp, making the
    // tie-break deterministic regardless of clock granularity (nanoTime is monotonic).
    while (System.nanoTime() <= older.lastAccess) { Thread.onSpinWait() }
    val newer = filledBuffer(8, 0L, 1, 5000)
    assert(newer.lastAccess > older.lastAccess)
    mgr.register(older)
    mgr.register(newer)

    assert(mgr.maybeSpill() > 0L)
    mgr.isSpilled(keyOf(older)) mustBe true
    mgr.isSpilled(keyOf(newer)) mustBe false
    metrics.spillCount mustBe 1L
  }

  test("a failed BlockManager.putBytes retains the buffer and loses no data") {
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    // Force the durable store to fail: spill-and-clear must NOT clear the buffer (zero data loss).
    when(bm.putBytes(any(), any(), any(), any())(any())).thenReturn(false)
    val buffer = filledBuffer(9, 0L, 0, 9000)
    mgr.register(buffer)

    mgr.maybeSpill() mustBe 0L
    buffer.size mustBe 9000L
    mgr.isSpilled(keyOf(buffer)) mustBe false
    metrics.spillCount mustBe 0L
  }

  test("a throwing BlockManager.putBytes is caught and the buffer is retained") {
    val (mgr, bm, metrics) = newManager(maxMem = 10000L)
    when(bm.putBytes(any(), any(), any(), any())(any()))
      .thenThrow(new RuntimeException("blitzy-disk-full"))
    val buffer = filledBuffer(10, 0L, 0, 9000)
    mgr.register(buffer)

    // The NonFatal catch inside the spill path swallows the store error and retains the bytes.
    mgr.maybeSpill() mustBe 0L
    buffer.size mustBe 9000L
    mgr.isSpilled(keyOf(buffer)) mustBe false
    metrics.spillCount mustBe 0L
  }

  test("a spilled segment is read back byte-for-byte via readSpilledSegment") {
    val (mgr, store, _) = storingManager(maxMem = 10000L)
    val buffer = filledBuffer(11, 2L, 3, 9000)
    val expectedEnveloped = buffer.toByteArray // canonical enveloped frames BEFORE the spill
    mgr.register(buffer)

    assert(mgr.spillBuffer(keyOf(buffer)))
    buffer.size mustBe 0L // cleared only after the durable store succeeded
    store.size() mustBe 1
    val segments = mgr.spilledBlockIds(keyOf(buffer))
    segments.size mustBe 1
    val readBack = segments.flatMap(id => mgr.readSpilledSegment(id))
      .foldLeft(Array.emptyByteArray)(_ ++ _)
    // Dual-channel invariant: the spilled bytes are byte-identical to the buffer's frames.
    assert(readBack.sameElements(expectedEnveloped))
  }

  test("concurrent appends are never lost across a spill (CWE-367 regression)") {
    // A producer appends a known total of payload bytes while a spiller races repeated spills.
    // The fixed atomic spill-and-clear guarantees every appended byte ends up either in a
    // persisted spill segment or in the final buffer; the previous snapshot-then-clear (which
    // cleared under a different monitor than append) could wipe an append that landed between
    // snapshot and clear, losing bytes. Asserting payload conservation catches that race.
    val (mgr, store, _) = storingManager(maxMem = 10000L)
    val buffer = new StreamingBuffer(12, 0L, 0, 64L * 1024 * 1024)
    mgr.register(buffer)

    val recordBytes = 256
    val numRecords = 4000
    val totalPayload = recordBytes.toLong * numRecords

    val producer = new Thread(() => {
      var i = 0
      while (i < numRecords) {
        buffer.append(new Array[Byte](recordBytes))
        i += 1
      }
    })
    val keepSpilling = new AtomicBoolean(true)
    val spiller = new Thread(() => {
      while (keepSpilling.get()) {
        mgr.spillBuffer(keyOf(buffer))
      }
    })
    producer.start()
    spiller.start()
    producer.join()
    keepSpilling.set(false)
    spiller.join()
    // Flush whatever remains after the spiller stopped, then assert no bytes were lost.
    mgr.spillBuffer(keyOf(buffer))
    buffer.size mustBe 0L

    var recovered = 0L
    store.values().asScala.foreach(seg => recovered += sumPayloadBytes(seg))
    recovered mustBe totalPayload
  }
}
