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

import org.mockito.ArgumentMatchers.{any, anyBoolean, eq => meq}
import org.mockito.Mockito.{atLeastOnce, mock, verify, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Unit tests for [[MemorySpillManager]].
 *
 * These tests validate the memory-pressure monitor and disk-spill coordinator of the streaming
 * shuffle backend (`spark.shuffle.manager=streaming`) in isolation:
 *
 *  - the 100 ms utilization polling cadence (`POLL_INTERVAL_MS`);
 *  - spill of buffered partitions to `BlockManager` at [[StorageLevel.DISK_ONLY]] once utilization
 *    exceeds the configured `spark.shuffle.streaming.spillThreshold`;
 *  - largest-first eviction ordering, so the fewest spills reclaim the most memory;
 *  - synchronous, sub-100 ms memory reclamation on consumer acknowledgment; and
 *  - the metrics side effects (`spillCount` increment and `bufferUtilizationPercent` refresh).
 *
 * The [[org.apache.spark.storage.BlockManager]] is mocked because the manager only ''consumes'' its
 * public `putBytes` API; the streaming collaborators [[StreamingShuffleConfig]],
 * [[StreamingShuffleMetrics]] and [[StreamingBuffer]] are exercised for real since they are cheap,
 * dependency-free primitives. No `SparkContext`/`SparkEnv` is required: every collaborator the
 * manager needs is injected through its constructor.
 *
 * '''Anti-flakiness.''' Spill and polling are driven by a background daemon thread, so those
 * assertions are wrapped in `eventually` (never in wall-clock timing assertions) and the poller is
 * always shut down in a `finally` block to avoid leaking the 100 ms scheduler thread across tests.
 * The buffer budget denominator is set explicitly via `setBufferBudgetBytes` so utilization -- and
 * therefore the spill decision -- is fully deterministic rather than dependent on executor memory.
 */
class MemorySpillManagerSuite extends SparkFunSuite with Eventually {

  /**
   * Builds a real [[StreamingShuffleConfig]] with the given spill threshold. The threshold is set
   * through the public `spark.shuffle.streaming.spillThreshold` key so the value flows through the
   * same typed `ConfigEntry` (and its `[50, 95]` range guard) that production uses at runtime.
   *
   * @param spillThreshold buffer-utilization percentage at which spilling begins; must be in
   *                       `[50, 95]` to satisfy the config entry's range check
   */
  private def newConf(spillThreshold: Int = 80): StreamingShuffleConfig = {
    val c = new SparkConf(false)
      .set("spark.shuffle.streaming.spillThreshold", spillThreshold.toString)
    new StreamingShuffleConfig(c)
  }

  /**
   * Creates a mocked [[org.apache.spark.storage.BlockManager]]. The spill path treats `putBytes`'
   * `Boolean` return as a durable-store confirmation: the in-memory buffer is reset, the block is
   * marked spilled, and `spillCount` is incremented ONLY when `putBytes` returns `true` (the
   * streaming-shuffle zero-data-loss guarantee -- an unconfirmed store retains the buffer for a
   * later retry). These tests exercise the successful-spill path, so the mock confirms persistence
   * by returning `true`; the `DISK_ONLY` invocation itself is still asserted via `verify`.
   */
  private def newBlockManager(): BlockManager = {
    val bm = mock(classOf[BlockManager])
    when(bm.putBytes(
      any(classOf[ShuffleBlockId]),
      any(classOf[ChunkedByteBuffer]),
      meq(StorageLevel.DISK_ONLY),
      anyBoolean())(any())).thenReturn(true)
    bm
  }

  /**
   * Creates a real [[StreamingBuffer]] pre-filled with `numBytes` bytes so it has a non-zero
   * in-memory size (empty buffers are dropped rather than spilled by the manager).
   */
  private def bufferWith(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      numBytes: Int): StreamingBuffer = {
    val buffer = new StreamingBuffer(shuffleId, mapId, reduceId)
    buffer.append(Array.fill(numBytes)(1.toByte))
    buffer
  }

  /**
   * Reflectively overrides a buffer's last-access timestamp. [[StreamingBuffer]] stamps
   * `lastAccessTime` to "now" on every `append`, so two buffers created in the same test are
   * effectively tied; the manager's spill ordering uses that timestamp as the LRU tie-breaker
   * among equal-sized partitions. There is no public setter (the field is intentionally internal),
   * so this test overrides it directly to make the LRU ordering deterministic.
   */
  private def setLastAccess(buffer: StreamingBuffer, millis: Long): Unit = {
    val field = classOf[StreamingBuffer].getDeclaredField("lastAccessTime")
    field.setAccessible(true)
    field.setLong(buffer, millis)
  }

  test("POLL_INTERVAL_MS is 100") {
    val manager =
      new MemorySpillManager(newConf(), newBlockManager(), new StreamingShuffleMetrics())
    // POLL_INTERVAL_MS is a private val on the production class -- there is no public accessor or
    // companion constant -- so it is read reflectively to assert the 100 ms polling cadence that
    // the feature's sub-100 ms reclamation SLA is built on.
    val field = classOf[MemorySpillManager].getDeclaredField("POLL_INTERVAL_MS")
    field.setAccessible(true)
    assert(field.get(manager).asInstanceOf[Long] == 100L)
  }

  test("utilizationPercent starts at zero when no buffers are registered") {
    val manager =
      new MemorySpillManager(newConf(), newBlockManager(), new StreamingShuffleMetrics())
    assert(manager.utilizationPercent() == 0)
  }

  test("register then unregister tracks and releases a buffer") {
    val manager =
      new MemorySpillManager(newConf(), newBlockManager(), new StreamingShuffleMetrics())
    manager.setBufferBudgetBytes(10000L)
    manager.register(0, 0L, 0, bufferWith(0, 0L, 0, 1024))
    // A freshly registered buffer is tracked in memory (non-zero utilization) and not yet spilled.
    assert(!manager.isSpilled(0, 0L, 0))
    assert(manager.utilizationPercent() > 0)
    // Unregistering stops tracking without error and returns utilization to zero.
    manager.unregister(0, 0L, 0)
    assert(manager.utilizationPercent() == 0)
  }

  test("spill writes the buffer to disk via putBytes(DISK_ONLY) above the threshold") {
    val metrics = new StreamingShuffleMetrics()
    val blockManager = newBlockManager()
    val manager = new MemorySpillManager(newConf(spillThreshold = 80), blockManager, metrics)
    manager.setBufferBudgetBytes(1000L)
    // The buffer reference is retained so the test can assert that spilling actually frees the
    // in-memory bytes, not merely that a copy was persisted to disk.
    val buffer = bufferWith(0, 0L, 0, 900)
    manager.register(0, 0L, 0, buffer)
    manager.start()
    try {
      // 900 B of a 1000 B budget is 90% utilization, above the 80% spill threshold, so the poller
      // must spill the partition. spillCount is incremented last in the spill path, so once it is
      // observed the putBytes call and the spilled-block marker are already in place.
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        assert(metrics.spillCounter.getCount >= 1)
      }
      verify(blockManager, atLeastOnce()).putBytes(
        any(classOf[ShuffleBlockId]),
        any(classOf[ChunkedByteBuffer]),
        meq(StorageLevel.DISK_ONLY),
        anyBoolean())(any())
      assert(manager.isSpilled(0, 0L, 0))
      // Zero retained memory after spill: reset() released the buffered bytes (size drops to 0)
      // and the manager stopped tracking the partition, so reported utilization returns to 0%.
      assert(buffer.size == 0L)
      assert(manager.utilizationPercent() == 0)
    } finally {
      manager.stop()
    }
  }

  test("spill evicts the largest partition first above the threshold") {
    val metrics = new StreamingShuffleMetrics()
    val blockManager = newBlockManager()
    val manager = new MemorySpillManager(newConf(spillThreshold = 80), blockManager, metrics)
    manager.setBufferBudgetBytes(1000L)
    // Two partitions total 90% of the budget. Evicting only the larger (800 B) partition drops
    // utilization to 10%, back under the 80% threshold, so the smaller (100 B) partition is left in
    // memory. This asserts the production's largest-first eviction ordering.
    manager.register(0, 0L, 0, bufferWith(0, 0L, 0, 800))
    manager.register(0, 0L, 1, bufferWith(0, 0L, 1, 100))
    manager.start()
    try {
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        assert(metrics.spillCounter.getCount >= 1)
      }
      // Exactly the largest partition is spilled; the smaller one stays in memory and no further
      // spill occurs because utilization is now below the threshold.
      assert(manager.isSpilled(0, 0L, 0))
      assert(!manager.isSpilled(0, 0L, 1))
      assert(metrics.spillCounter.getCount == 1)
    } finally {
      manager.stop()
    }
  }

  test("spill breaks ties between equal-sized partitions by evicting the least recently used") {
    val metrics = new StreamingShuffleMetrics()
    val blockManager = newBlockManager()
    val manager = new MemorySpillManager(newConf(spillThreshold = 80), blockManager, metrics)
    manager.setBufferBudgetBytes(1000L)
    // Two equal-sized (500 B) partitions fill the budget to 100%. Because their sizes tie, the
    // manager's largest-first ordering falls back to the least-recently-used timestamp. Evicting
    // one 500 B partition drops utilization to 50% -- back under the 80% threshold -- so exactly
    // one partition is spilled and the ordering under test is unambiguous.
    val lru = bufferWith(0, 0L, 0, 500)
    val mru = bufferWith(0, 0L, 1, 500)
    // append() stamps both timestamps to "now"; override them so the LRU/MRU roles are explicit
    // and the tie-break is deterministic rather than dependent on scheduling jitter.
    setLastAccess(lru, 1000L)
    setLastAccess(mru, 2000L)
    manager.register(0, 0L, 0, lru)
    manager.register(0, 0L, 1, mru)
    manager.start()
    try {
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        assert(metrics.spillCounter.getCount >= 1)
      }
      // The least-recently-used partition is evicted; the more-recently-used one stays in memory
      // and no second spill occurs because utilization is now below the threshold.
      assert(manager.isSpilled(0, 0L, 0))
      assert(!manager.isSpilled(0, 0L, 1))
      assert(metrics.spillCounter.getCount == 1)
    } finally {
      manager.stop()
    }
  }

  test("onConsumerAck reclaims buffer memory") {
    val manager =
      new MemorySpillManager(newConf(), newBlockManager(), new StreamingShuffleMetrics())
    manager.setBufferBudgetBytes(1000L)
    manager.register(0, 0L, 0, bufferWith(0, 0L, 0, 500))
    val before = manager.utilizationPercent()
    assert(before > 0)
    manager.onConsumerAck(0, 0L, 0)
    val after = manager.utilizationPercent()
    // Reclamation is synchronous (well within the 100 ms SLA): the acknowledged buffer is removed
    // from tracking immediately, so utilization drops right away rather than on the next poll.
    assert(after < before)
    assert(after == 0)
  }

  test("updateBufferUtilization is refreshed by the polling thread") {
    val metrics = new StreamingShuffleMetrics()
    val manager = new MemorySpillManager(newConf(), newBlockManager(), metrics)
    manager.setBufferBudgetBytes(1000L)
    manager.register(0, 0L, 0, bufferWith(0, 0L, 0, 500))
    manager.start()
    try {
      // 500 B of a 1000 B budget is 50% utilization, below the 80% threshold, so nothing spills.
      // Each poll calls utilizationPercent(), which publishes the value to the metrics gauge, so
      // the reported utilization converges to 50 without any direct call from the test.
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        assert(metrics.currentBufferUtilization == 50)
      }
    } finally {
      manager.stop()
    }
  }

}
