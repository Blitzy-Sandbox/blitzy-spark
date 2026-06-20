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
 * Unit tests for [[MemorySpillManager]], the bounded-footprint guarantor of the streaming shuffle
 * backend.
 *
 * The suite validates the three memory-exhaustion-prevention invariants mandated by the AAP:
 *
 *   - '''80% spill threshold''': no buffer is spilled while aggregate utilization stays below the
 *     configured spill threshold, and a spill is triggered once utilization reaches it.
 *   - '''DISK_ONLY spill''': a spill persists buffered bytes through the existing
 *     [[BlockManager.putBytes]] API at [[StorageLevel.DISK_ONLY]]; the `BlockManager` is mocked so
 *     the test never writes a real disk block.
 *   - '''maxOnHeapStorageMemory denominator''': aggregate utilization is measured against
 *     [[MemoryManager.maxOnHeapStorageMemory]], so an identically sized buffer spills under a small
 *     budget and does not under a large one.
 *
 * Spill-victim selection (largest first, least-recently-used tie-break) and the `spillCount`
 * telemetry are also asserted, along with the lifecycle safety of the 100 ms poll thread. Every
 * assertion targets observable behavior -- bytes reclaimed, `putBytes` arguments, the spill
 * counter, and per-buffer state -- rather than poll-tick timing, which would be flaky. The 100 ms
 * reclamation SLA is the design target; the timing test asserts only a generous non-blocking
 * upper bound.
 */
class MemorySpillManagerSuite extends SparkFunSuite with Matchers {

  /** A generous per-buffer soft capacity; capacity does not influence the spill decision. */
  private val oneMiB: Long = 1024L * 1024L

  /**
   * Builds a [[MemorySpillManager]] wired to mocked collaborators so spills are deterministic and
   * never touch real disk. The mocked [[BlockManager.putBytes]] reports success, and the mocked
   * [[MemoryManager.maxOnHeapStorageMemory]] returns `maxMem` so a test controls the spill
   * denominator exactly.
   *
   * @param maxMem the value the mocked `maxOnHeapStorageMemory` returns (the spill denominator)
   * @return the manager under test together with its `BlockManager` and metrics collaborators
   */
  private def newManager(maxMem: Long = 100L * 1024 * 1024)
      : (MemorySpillManager, BlockManager, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val bm = mock(classOf[BlockManager])
    when(bm.putBytes(any(), any(), any(), any())(any())).thenReturn(true)
    val mm = mock(classOf[MemoryManager])
    when(mm.maxOnHeapStorageMemory).thenReturn(maxMem)
    val metrics = new StreamingShuffleMetrics
    (new MemorySpillManager(cfg, bm, mm, metrics), bm, metrics)
  }

  /**
   * Runs `body` against a freshly built manager and always stops it afterwards, so the daemon poll
   * executor is released even if an assertion fails (no leaked thread under
   * `spark.unsafe.exceptionOnMemoryLeak=true`).
   *
   * @param maxMem the spill denominator the mocked `maxOnHeapStorageMemory` returns
   * @param body   the test body, receiving the manager and its mocked collaborators
   * @return whatever `body` returns
   */
  private def withManager[T](maxMem: Long = 100L * 1024 * 1024)(
      body: (MemorySpillManager, BlockManager, StreamingShuffleMetrics) => T): T = {
    val (mgr, bm, metrics) = newManager(maxMem)
    try {
      body(mgr, bm, metrics)
    } finally {
      mgr.stop()
    }
  }

  /**
   * Builds a per-partition buffer holding `bytes` bytes of map output and registers it with `mgr`
   * so it participates in spill accounting. Returns the buffer for tests that inspect it.
   */
  private def registerBuffer(
      mgr: MemorySpillManager,
      shuffleId: Int,
      partitionId: Int,
      bytes: Int): StreamingBuffer = {
    val buffer = new StreamingBuffer(shuffleId, 0L, partitionId, oneMiB)
    buffer.append(new Array[Byte](bytes))
    mgr.register(buffer)
    buffer
  }

  test("no spill below the utilization threshold") {
    withManager(maxMem = 10000L) { (mgr, bm, metrics) =>
      // 1000 / 10000 = 10% utilization, well under the 80% spill threshold.
      registerBuffer(mgr, shuffleId = 0, partitionId = 0, bytes = 1000)
      mgr.maybeSpill() mustBe 0L
      verify(bm, never()).putBytes(any(), any(), any(), any())(any())
      metrics.spillCount mustBe 0L
    }
  }

  test("spill triggered at or above the 80% threshold via DISK_ONLY") {
    withManager(maxMem = 10000L) { (mgr, bm, metrics) =>
      // 8500 / 10000 = 85% utilization, at/above the 80% spill threshold.
      registerBuffer(mgr, shuffleId = 1, partitionId = 0, bytes = 8500)
      val reclaimed = mgr.maybeSpill()
      reclaimed must be > 0L
      // DISK_ONLY is the mandatory storage level for streaming-shuffle spills.
      verify(bm, atLeastOnce())
        .putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
      metrics.spillCount must be >= 1L
    }
  }

  test("spillBuffer writes one buffer via BlockManager and reports success") {
    withManager() { (mgr, bm, metrics) =>
      val buffer = registerBuffer(mgr, shuffleId = 2, partitionId = 0, bytes = 4096)
      // spillBuffer is an on-demand spill independent of the aggregate threshold; it resolves the
      // buffer by key from the registry, so the buffer must already be registered (it is, above).
      assert(mgr.spillBuffer(MemorySpillManager.keyFor(buffer)))
      verify(bm).putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
      metrics.spillCount mustBe 1L
    }
  }

  test("largest / least-recently-used buffer is spilled first") {
    withManager(maxMem = 10000L) { (mgr, _, _) =>
      // total 9000 / 10000 = 90%; reclaiming back under 80% needs only ~1000 bytes, so the
      // largest buffer alone is spilled and the smaller one stays resident in memory.
      val large = registerBuffer(mgr, shuffleId = 3, partitionId = 0, bytes = 7000)
      val small = registerBuffer(mgr, shuffleId = 3, partitionId = 1, bytes = 2000)
      mgr.maybeSpill() must be > 0L
      mgr.isSpilled(MemorySpillManager.keyFor(large)) mustBe true
      mgr.isSpilled(MemorySpillManager.keyFor(small)) mustBe false
      large.size mustBe 0L
      small.size mustBe 2000L
    }
  }

  test("spill reclaims memory well within a non-blocking bound") {
    withManager(maxMem = 10000L) { (mgr, _, _) =>
      registerBuffer(mgr, shuffleId = 4, partitionId = 0, bytes = 9000)
      val startNanos = System.nanoTime()
      mgr.maybeSpill()
      val elapsedMs = (System.nanoTime() - startNanos) / 1000000L
      // The design SLA is 100 ms; assert a generous upper bound to prove the synchronous spill
      // never blocks for seconds while staying robust against CI scheduling jitter.
      elapsedMs must be < 1000L
    }
  }

  test("start and stop manage the poll thread without leaking") {
    val (mgr, _, _) = newManager()
    try {
      mgr.start()
      mgr.isStarted mustBe true
      mgr.stop()
      // A second stop must be a safe no-op (idempotent lifecycle).
      mgr.stop()
      mgr.isStarted mustBe false
    } finally {
      mgr.stop()
    }
  }

  test("spill denominator is maxOnHeapStorageMemory") {
    // A small on-heap storage budget makes a 9000-byte buffer exceed the 80% threshold.
    withManager(maxMem = 10000L) { (mgr, _, metrics) =>
      registerBuffer(mgr, shuffleId = 5, partitionId = 0, bytes = 9000)
      mgr.maybeSpill() must be > 0L
      metrics.spillCount must be >= 1L
    }
    // With a large budget, an identically sized buffer stays far below the threshold: the spill
    // decision flips solely on maxOnHeapStorageMemory, proving the denominator wiring.
    withManager(maxMem = 100L * 1024 * 1024) { (mgr, bm, metrics) =>
      registerBuffer(mgr, shuffleId = 5, partitionId = 0, bytes = 9000)
      mgr.maybeSpill() mustBe 0L
      verify(bm, never()).putBytes(any(), any(), any(), any())(any())
      metrics.spillCount mustBe 0L
    }
  }
}
