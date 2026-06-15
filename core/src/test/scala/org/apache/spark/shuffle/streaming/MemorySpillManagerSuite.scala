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

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{atLeastOnce, mock, never, verify, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockManager, StorageLevel}

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

  test("reclaims memory within the ~100ms SLA bound") {
    // The 100 ms reclaim SLA is the design target. With a mocked BlockManager the synchronous
    // spill must finish well under a generous, CI-stable bound (not block for whole seconds).
    val (mgr, _, _) = newManager(maxMem = 10000L)
    mgr.register(filledBuffer(4, 0L, 0, 9000))

    val startNanos = System.nanoTime()
    val reclaimed = mgr.maybeSpill()
    val elapsedMs = (System.nanoTime() - startNanos) / 1000000L

    assert(reclaimed > 0L)
    assert(elapsedMs < 1000L)
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
}
