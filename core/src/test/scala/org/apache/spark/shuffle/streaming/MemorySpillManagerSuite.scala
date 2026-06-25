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

import java.util.concurrent.atomic.AtomicBoolean

import org.mockito.ArgumentMatchers.{any, anyBoolean, eq => meq}
import org.mockito.Mockito.{atLeastOnce, mock, never, times, verify, when}
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockManager, StorageLevel}

/**
 * Unit tests for [[MemorySpillManager]] (streaming shuffle feature F-109), the monitor that polls
 * aggregate buffer utilization and spills the largest / least-recently-used in-memory
 * [[StreamingBuffer]]s to disk once utilization reaches the configured spill threshold.
 *
 * '''Why these tests avoid the scheduler.''' The production class drives spilling from a
 * `ScheduledExecutorService` that fires every `POLL_INTERVAL_MS` milliseconds. Asserting against
 * that wall-clock cadence would be inherently flaky, so the spill-decision tests instead invoke
 * the synchronous, private `pollOnce()` method directly through ScalaTest's
 * [[org.scalatest.PrivateMethodTester]]. This makes every threshold assertion deterministic.
 * Exactly one test exercises the real `start()` / `stop()` lifecycle, and every manager built by
 * [[withManager]] is unconditionally stopped in a `finally` block so the suite never leaks the
 * daemon spill-monitor thread that the manager creates in its constructor.
 *
 * '''Mocking strategy.''' `BlockManager` and `MemoryManager` are Mockito mocks: the former
 * records the `DISK_ONLY` spill writes that the tests verify, and the latter supplies a small,
 * deterministic `maxOnHeapStorageMemory` denominator (1000 bytes) so that threshold crossings
 * are exact and easy to reason about. The [[StreamingBuffer]] instances are REAL (bytes are
 * appended so their reported `size` is known precisely) and [[StreamingShuffleMetrics]] is the
 * real counter holder, so spill accounting is asserted end to end rather than through a stub.
 */
class MemorySpillManagerSuite extends SparkFunSuite with PrivateMethodTester {

  import MemorySpillManager.BufferKey

  // Synchronous handles into the otherwise-private monitor internals. PrivateMethodTester invokes
  // the compiler-generated accessor for a `private val` and the method itself for a `private`
  // `def`, letting the tests drive a single poll cycle and read the contractual constants.
  private val pollOnce = PrivateMethod[Unit](Symbol("pollOnce"))
  private val currentBufferedBytes = PrivateMethod[Long](Symbol("currentBufferedBytes"))
  private val pollIntervalMs = PrivateMethod[Long](Symbol("POLL_INTERVAL_MS"))
  private val reclaimDeadlineMs = PrivateMethod[Long](Symbol("RECLAIM_DEADLINE_MS"))
  private val startedFlag = PrivateMethod[AtomicBoolean](Symbol("started"))
  private val stoppedFlag = PrivateMethod[AtomicBoolean](Symbol("stopped"))

  /** On-heap storage memory denominator used by every test; small so 80% maps to 800 bytes. */
  private val MaxOnHeapBytes = 1000L

  /**
   * Construct a [[MemorySpillManager]] backed by fresh mocks and run `body` against it, always
   * stopping the manager afterwards so the scheduled spill-monitor thread is never leaked.
   *
   * @param thresholdPercent the spill threshold to configure (defaults to the contractual 80%)
   * @param body             receives the manager, the mocked block manager and the real metrics
   */
  private def withManager[T](thresholdPercent: Int = 80)(
      body: (MemorySpillManager, BlockManager, StreamingShuffleMetrics) => T): T = {
    val blockManager = mock(classOf[BlockManager])
    val memoryManager = mock(classOf[MemoryManager])
    when(memoryManager.maxOnHeapStorageMemory).thenReturn(MaxOnHeapBytes)
    val metrics = new StreamingShuffleMetrics
    val manager = new MemorySpillManager(blockManager, memoryManager, metrics, thresholdPercent)
    try {
      body(manager, blockManager, metrics)
    } finally {
      manager.stop()
    }
  }

  /** Build a real buffer for `partitionId` pre-filled with exactly `numBytes` bytes. */
  private def bufferOf(partitionId: Int, numBytes: Int): StreamingBuffer = {
    val buffer = new StreamingBuffer(partitionId)
    if (numBytes > 0) {
      buffer.append(Array.fill[Byte](numBytes)(1.toByte))
    }
    buffer
  }

  /** A registry key for the given partition; the shuffle / map ids are irrelevant here. */
  private def keyFor(partitionId: Int): BufferKey = BufferKey(0, 0L, partitionId)

  /**
   * Stub `blockManager.putBytes(...)` so a `DISK_ONLY` spill "succeeds" and returns `true`.
   * Without this the Mockito default of `false` would make the production code treat every spill
   * as failed and retain the buffer in memory, so the success path (`markSpilled` plus the
   * spill-count increment) must be stubbed explicitly. The trailing `(any())` matches the
   * implicit `ClassTag` argument carried by the generic `putBytes[T: ClassTag]` signature.
   */
  private def stubSpillSucceeds(blockManager: BlockManager): Unit = {
    when(
      blockManager
        .putBytes[Any](any(), any(), meq(StorageLevel.DISK_ONLY), anyBoolean())(any()))
      .thenReturn(true)
  }

  /** Verify the `DISK_ONLY` spill write happened exactly `count` times. */
  private def verifySpillWrites(blockManager: BlockManager, count: Int): Unit = {
    verify(blockManager, times(count))
      .putBytes[Any](any(), any(), meq(StorageLevel.DISK_ONLY), anyBoolean())(any())
  }

  test("POLL_INTERVAL_MS and RECLAIM_DEADLINE_MS are 100 ms") {
    withManager() { (manager, _, _) =>
      assert(manager.invokePrivate(pollIntervalMs()) === 100L)
      assert(manager.invokePrivate(reclaimDeadlineMs()) === 100L)
    }
  }

  test("crossing the spill threshold spills to disk via putBytes(DISK_ONLY)") {
    withManager() { (manager, blockManager, _) =>
      stubSpillSucceeds(blockManager)
      // One buffer at 850 bytes is 85% of the 1000-byte denominator, above the 80% threshold.
      manager.registerBuffer(keyFor(0), bufferOf(0, 850))

      manager.invokePrivate(pollOnce())

      verify(blockManager, atLeastOnce())
        .putBytes[Any](any(), any(), meq(StorageLevel.DISK_ONLY), anyBoolean())(any())
    }
  }

  test("each spill increments the spillCount metric") {
    withManager() { (manager, blockManager, metrics) =>
      stubSpillSucceeds(blockManager)
      manager.registerBuffer(keyFor(0), bufferOf(0, 850))
      assert(metrics.getSpillCount === 0L)

      manager.invokePrivate(pollOnce())

      assert(metrics.getSpillCount === 1L)
    }
  }

  test("spill selection prefers the largest buffer") {
    withManager() { (manager, blockManager, metrics) =>
      stubSpillSucceeds(blockManager)
      val largest = bufferOf(0, 700)
      val smaller = bufferOf(1, 200)
      manager.registerBuffer(keyFor(0), largest)
      manager.registerBuffer(keyFor(1), smaller)
      // Total is 900 bytes (90%); freeing the 700-byte buffer alone drops the projection to 200
      // bytes (20%), so the manager must spill exactly the largest buffer and stop.

      manager.invokePrivate(pollOnce())

      assert(largest.isSpilled, "the largest buffer should have been spilled first")
      assert(!smaller.isSpilled, "the smaller buffer should remain in memory")
      verifySpillWrites(blockManager, 1)
      assert(metrics.getSpillCount === 1L)
    }
  }

  test("spill selection breaks size ties by least-recently-used order") {
    withManager() { (manager, blockManager, metrics) =>
      stubSpillSucceeds(blockManager)
      val older = bufferOf(0, 500)
      // Guarantee a strictly later access timestamp for the second buffer without a fixed sleep:
      // spin until the monotonic clock advances past the first buffer's lastAccess. nanoTime is
      // monotonic and always advances, so this terminates almost immediately and is not flaky.
      while (System.nanoTime() <= older.lastAccess) {
        // busy-wait for the nanosecond clock to advance
      }
      val newer = bufferOf(1, 500)
      assert(newer.lastAccess > older.lastAccess)
      manager.registerBuffer(keyFor(0), older)
      manager.registerBuffer(keyFor(1), newer)
      // Both buffers are 500 bytes (total 100%); freeing one drops the projection to 50%, so the
      // manager spills exactly one and, with equal sizes, it must be the least-recently-used.

      manager.invokePrivate(pollOnce())

      assert(older.isSpilled, "the least-recently-used buffer should have been spilled")
      assert(!newer.isSpilled, "the most-recently-used buffer should remain in memory")
      verifySpillWrites(blockManager, 1)
      assert(metrics.getSpillCount === 1L)
    }
  }

  test("buffers below the spill threshold are never spilled") {
    withManager() { (manager, blockManager, metrics) =>
      // 300 + 200 = 500 bytes is 50% utilization, comfortably below the 80% threshold.
      manager.registerBuffer(keyFor(0), bufferOf(0, 300))
      manager.registerBuffer(keyFor(1), bufferOf(1, 200))

      manager.invokePrivate(pollOnce())

      verify(blockManager, never()).putBytes[Any](any(), any(), any(), anyBoolean())(any())
      assert(metrics.getSpillCount === 0L)
      assert(metrics.getBufferUtilizationPercent === 50)
    }
  }

  test("start() and stop() are idempotent") {
    withManager() { (manager, _, _) =>
      manager.start()
      manager.start()
      assert(manager.invokePrivate(startedFlag()).get(), "started flag set after start()")

      manager.stop()
      manager.stop()
      assert(manager.invokePrivate(stoppedFlag()).get(), "stopped flag set after stop()")
    }
  }

  test("reclaim frees the acknowledged buffer and drops it from accounting") {
    withManager() { (manager, _, _) =>
      val buffer = bufferOf(0, 500)
      val key = keyFor(0)
      manager.registerBuffer(key, buffer)
      assert(manager.invokePrivate(currentBufferedBytes()) === 500L)

      manager.reclaim(key)

      assert(buffer.size === 0L, "the reclaimed buffer's bytes should have been released")
      assert(manager.invokePrivate(currentBufferedBytes()) === 0L,
        "the reclaimed buffer should no longer count toward buffered bytes")
      // Reclaiming an unknown key must be a harmless no-op.
      manager.reclaim(keyFor(99))
    }
  }
}
