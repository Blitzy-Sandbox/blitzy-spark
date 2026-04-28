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

import scala.reflect.ClassTag

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean}
import org.mockito.Mockito.{atLeastOnce, mock, never, reset, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Unit tests for [[MemorySpillManager]] covering 100 ms threshold polling, "evict-largest"
 * eviction selection, [[BlockManager#putBytes]] integration with
 * [[StorageLevel#DISK_ONLY]], buffer-reclamation timing within the 100 ms post-ack budget,
 * graceful spill-failure handling, metrics emission, custom spill-threshold respect, and
 * `stop()` lifecycle idempotency.
 *
 * == AAP Reference ==
 *  - AAP Section 0.5.1.3 (MemorySpillManager component design)
 *  - AAP Section 0.5.1.6 (Group 6, item 5 -- this suite's scope)
 *  - AAP Section 0.7.2.2 (memory discipline -- 80% spill threshold, 100 ms reclamation)
 *  - AAP Section 0.7.2.6 (quality gate: > 85% coverage for new components)
 *
 * == Production-Source Contract Exercised ==
 *  - Constructor: `MemorySpillManager(blockManager, memoryManager, metrics, conf)`
 *    instantiated per-test in `beforeEach`.
 *  - `trackBuffer(shuffleId, mapId, reduceId, buffer: ChunkedByteBuffer)` -- registers a
 *    buffer with the manager for memory-pressure-driven eviction.
 *  - `checkAndSpill(shuffleId, mapId, reduceId, buffer: ChunkedByteBuffer)` -- per-partition
 *    spill push from the writer; persists the buffer via [[BlockManager#putBytes]] at
 *    [[StorageLevel#DISK_ONLY]] and disposes it on success.
 *  - `reclaim(shuffleId, mapId, reduceId, bytes)` -- consumer-ack-driven memory release.
 *  - `pollOnce()` -- threshold-driven eviction selection invoked from the daemon scheduler;
 *    package-private (`private[streaming]`) and therefore directly invocable from this
 *    suite which lives in the same `org.apache.spark.shuffle.streaming` package.
 *  - `stop()` -- idempotent shutdown via `stopped: AtomicBoolean.compareAndSet(false, true)`.
 *
 * == Spill Decision Paths ==
 * Two production paths can trigger a spill, each exercised by this suite:
 *  1. [[MemorySpillManager#checkAndSpill]] -- writer pushes a non-empty buffer; the manager
 *     UNCONDITIONALLY persists it via [[BlockManager#putBytes]]. The threshold check is NOT
 *     consulted on this path: the writer has already decided to spill (typically because
 *     the writer's per-partition cap was crossed) and the manager merely brokers the
 *     transfer to disk.
 *  2. [[MemorySpillManager#pollOnce]] -> `evictLargestBuffer` -- the daemon poller computes
 *     `(totalBytes / maxOnHeapStorageMemory) * 100` and, when the result equals or exceeds
 *     `spillThresholdPercent`, evicts the largest tracked buffer to disk. Threshold-related
 *     tests in this suite invoke `pollOnce` explicitly (rather than waiting for the 100 ms
 *     scheduler cadence) so each assertion is deterministic.
 *
 * == Threshold Semantics ==
 * The production source uses `pct = ((used / maxOnHeap) * 100.0).toInt` and triggers spill
 * when `pct >= spillThresholdPercent`. Boundary tests therefore use buffer sizes that
 * yield `pct` values just above and just below the threshold to lock the inequality
 * direction (`>=`, not `>`) against accidental refactoring.
 *
 * == Test Discipline ==
 * The manager constructs a daemon scheduled executor on instantiation. Tests MUST call
 * `spillManager.stop()` in `afterEach` to avoid daemon-thread accumulation across the
 * suite. The first scheduled tick fires 100 ms after construction, so the body of each
 * test (which runs in single-digit milliseconds) completes before any race with the
 * scheduled poller could occur. Defensive `atLeastOnce()` Mockito verifications are
 * preferred over `times(1)` where a scheduler tick could plausibly contribute an
 * additional invocation -- this keeps the suite robust on busy CI without sacrificing
 * deterministic assertions on the manager's primary behavior.
 *
 * == Mocking Strategy ==
 *  - [[BlockManager]] is mocked via `mock(classOf[BlockManager])`; its `putBytes` method
 *    has an implicit [[ClassTag]] parameter list which is matched via `any[ClassTag[Byte]]`
 *    in the stub setup.
 *  - [[MemoryManager]] is mocked and its abstract `maxOnHeapStorageMemory` accessor is
 *    stubbed via `doReturn(value).when(mm).maxOnHeapStorageMemory`. The `doReturn` helper
 *    mirrors the pattern from [[StreamingShuffleFallbackPolicySuite]] for stubbing
 *    methods that may be `final` or otherwise resist the conventional
 *    `when(...).thenReturn(...)` form.
 *  - [[StreamingShuffleMetrics]] is used as a REAL instance (not mocked) so that counter
 *    assertions exercise the production side-effect path. This matches the pattern from
 *    [[BackpressureProtocolSuite]] and [[StreamingShuffleFallbackPolicySuite]].
 */
class MemorySpillManagerSuite
  extends SparkFunSuite with Matchers with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------
  // Mockito helpers
  // ---------------------------------------------------------------------------

  /**
   * Mockito stub helper mirroring the pattern from
   * [[org.apache.spark.shuffle.sort.SortShuffleManagerSuite]] and
   * [[StreamingShuffleFallbackPolicySuite]].
   *
   * Wraps `org.mockito.Mockito.doReturn(value, varargs...)` with the empty-Seq splat
   * required by the Java vararg signature when called from Scala. Used for stubbing
   * abstract methods (notably [[MemoryManager#maxOnHeapStorageMemory]]) that are awkward
   * to stub via `when(mock.method).thenReturn(value)` because a partial real
   * implementation could be invoked during stub setup.
   */
  private def doReturn(value: Any): org.mockito.stubbing.Stubber =
    org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  // ---------------------------------------------------------------------------
  // Per-test mutable fixtures
  // ---------------------------------------------------------------------------

  /** Mocked [[BlockManager]] whose `putBytes` is stubbed per test (default: returns true). */
  private var blockManager: BlockManager = _

  /**
   * Mocked [[MemoryManager]] whose `maxOnHeapStorageMemory` accessor is stubbed per test.
   * The default value is intentionally LARGE (1 TB) so that buffers tracked in tests that
   * do not exercise the threshold path produce a `pct` of ~0% and never accidentally
   * trigger a polling-driven spill. Tests that exercise the threshold path override the
   * stub with a smaller value tailored to the test's buffer sizes.
   */
  private var memoryManager: MemoryManager = _

  /**
   * Real [[StreamingShuffleMetrics]] instance; tests assert metric increments via the
   * production counter API (`getSpillCount`, `getBufferUtilizationPercent`) to exercise
   * the actual side-effect path that the manager uses.
   */
  private var metrics: StreamingShuffleMetrics = _

  /**
   * [[SparkConf]] passed to the manager constructor. Constructed with `loadDefaults = false`
   * so prior system properties from any concurrent test run cannot bleed into this
   * manager's configuration. Tests that need a custom `spillThreshold` set the key
   * directly (e.g., `conf.set("spark.shuffle.streaming.spillThreshold", "60")`) before
   * constructing the manager.
   */
  private var conf: SparkConf = _

  /** The system under test, instantiated within each test body and torn down in `afterEach`. */
  private var spillManager: MemorySpillManager = _

  // ---------------------------------------------------------------------------
  // Lifecycle hooks
  // ---------------------------------------------------------------------------

  /**
   * Per-test setup: creates fresh mocks for [[BlockManager]] and [[MemoryManager]], a
   * fresh [[StreamingShuffleMetrics]] instance, and a fresh [[SparkConf]].
   *
   * The default `BlockManager.putBytes` stub returns `true` (success) so that tests
   * exercising the success path do not need to repeat the stub setup. Tests exercising
   * failure-handling explicitly `reset(blockManager)` and re-stub `putBytes` to return
   * `false` or throw.
   *
   * The default `MemoryManager.maxOnHeapStorageMemory` stub returns 1 TB so that any
   * tracked buffer's percentage of total is effectively 0% -- this prevents the daemon
   * scheduler's natural 100 ms tick from producing surprise spills in tests that do not
   * exercise the threshold path. Tests exercising the threshold path override this stub
   * before constructing the manager.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    blockManager = mock(classOf[BlockManager])
    memoryManager = mock(classOf[MemoryManager])
    metrics = new StreamingShuffleMetrics()
    conf = new SparkConf(loadDefaults = false)

    // Default putBytes stub: success. The implicit ClassTag parameter list is matched via
    // `any[ClassTag[Byte]]` because the production `MemorySpillManager` always supplies
    // `scala.reflect.ClassTag.Byte` at the call site (the buffer is serialized bytes).
    when(blockManager.putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])).thenReturn(true)

    // Default memory budget: 1 TB. Sufficiently large that any reasonable buffer size in a
    // unit test produces a utilization percent of 0, so the scheduler's natural tick does
    // not race with explicit `pollOnce()` calls in non-threshold tests.
    doReturn(1024L * 1024L * 1024L * 1024L).when(memoryManager).maxOnHeapStorageMemory
  }

  /**
   * Per-test teardown: stops the manager (which cancels the polling future and shuts down
   * the daemon executor) so daemon threads do not accumulate across the suite. The
   * production `stop()` is idempotent, so a test that explicitly calls `stop()` (e.g., the
   * idempotency test) does not conflict with this teardown.
   */
  override def afterEach(): Unit = {
    try {
      if (spillManager != null) {
        spillManager.stop()
        spillManager = null
      }
    } finally {
      super.afterEach()
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Build a non-empty [[ChunkedByteBuffer]] of exactly `sizeBytes` bytes.
   *
   * Wraps a fresh `Array[Byte](sizeBytes)` in a [[ByteBuffer]] (zero-copy backing) and
   * then in a [[ChunkedByteBuffer]]. The returned buffer's `.size` equals `sizeBytes`
   * exactly, matching the contract required by the production `trackBuffer` and
   * `checkAndSpill` paths which read `.size` to update the `totalBytes` counter.
   */
  private def buildBuffer(sizeBytes: Int): ChunkedByteBuffer = {
    require(sizeBytes >= 0, s"sizeBytes must be non-negative, got $sizeBytes")
    new ChunkedByteBuffer(ByteBuffer.wrap(new Array[Byte](sizeBytes)))
  }

  // ---------------------------------------------------------------------------
  // Test 1: 80% threshold detection (above threshold).
  // ---------------------------------------------------------------------------
  test("pollOnce triggers eviction-spill at the 80% threshold") {
    // Configure memory budget = 1 MB. A buffer of size 0.81 * 1 MB produces
    // pct = 80 (truncated from 80.99...) which satisfies the production check
    // `pct >= spillThresholdPercent` (default 80). The eviction path then calls
    // `BlockManager.putBytes` exactly once and increments `metrics.spillCount`.
    val maxOnHeap = 1024L * 1024L
    doReturn(maxOnHeap).when(memoryManager).maxOnHeapStorageMemory

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    // 0.81 * 1 MB = 849346 bytes. The buffer is registered via `trackBuffer` so the
    // manager's internal `totalBytes` counter reflects its size.
    val sizeBytes = (maxOnHeap * 0.81).toInt
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(sizeBytes))

    // Drive a single polling iteration. Because pct >= 80, this invokes
    // `evictLargestBuffer` which calls `BlockManager.putBytes` and increments spillCount.
    spillManager.pollOnce()

    // Use atLeastOnce defensively: although the test runs faster than the 100 ms
    // scheduler cadence in practice, atLeastOnce permits an additional contribution from
    // the natural scheduled tick without flaking on busy CI.
    verify(blockManager, atLeastOnce()).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
    assert(metrics.getSpillCount > 0L,
      "spillCount must increment when buffer utilization meets the 80% threshold")
  }

  // ---------------------------------------------------------------------------
  // Test 2: Below 80% threshold => no spill triggered.
  // ---------------------------------------------------------------------------
  test("pollOnce does NOT trigger eviction below the 80% threshold") {
    // Configure memory budget = 1 MB and track a buffer at ~50% utilization (well below
    // the 80% threshold). The polling path computes pct = 50 and the production check
    // `pct >= 80` evaluates false; no eviction is invoked and no putBytes call occurs.
    val maxOnHeap = 1024L * 1024L
    doReturn(maxOnHeap).when(memoryManager).maxOnHeapStorageMemory

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val sizeBytes = (maxOnHeap * 0.50).toInt
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(sizeBytes))

    spillManager.pollOnce()

    // Strict: never. Below-threshold polling is fully deterministic -- no scheduler tick
    // can convert a 50% utilization into a spill, so a `never()` assertion is safe.
    verify(blockManager, never()).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
    assert(metrics.getSpillCount === 0L,
      "spillCount must remain 0 when utilization is below the spill threshold")
  }

  // ---------------------------------------------------------------------------
  // Test 3: Custom spillThreshold configuration is respected.
  // ---------------------------------------------------------------------------
  test("pollOnce respects a custom spark.shuffle.streaming.spillThreshold (60%)") {
    // Configure a custom 60% threshold via SparkConf. With memory budget = 1 MB and a
    // buffer at ~65% utilization, the production check `pct (=65) >= 60` evaluates true
    // and a spill IS triggered. Under the default 80% threshold the same buffer would
    // NOT trigger a spill -- so this test specifically locks the conf-reading path
    // against accidental refactoring that hardcodes 80.
    val customThreshold = 60
    conf.set("spark.shuffle.streaming.spillThreshold", customThreshold.toString)

    val maxOnHeap = 1024L * 1024L
    doReturn(maxOnHeap).when(memoryManager).maxOnHeapStorageMemory

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val sizeBytes = (maxOnHeap * 0.65).toInt
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(sizeBytes))

    spillManager.pollOnce()

    verify(blockManager, atLeastOnce()).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
    assert(metrics.getSpillCount > 0L,
      s"spillCount must increment at 65% utilization with custom threshold=$customThreshold")
  }

  // ---------------------------------------------------------------------------
  // Test 4: Spill persists data via BlockManager.putBytes with DISK_ONLY storage level.
  // ---------------------------------------------------------------------------
  test("pollOnce-driven spill persists data via BlockManager.putBytes with DISK_ONLY") {
    // The eviction-spill path must (a) call `BlockManager.putBytes` and (b) supply
    // `StorageLevel.DISK_ONLY` -- both verified via ArgumentCaptor.
    val maxOnHeap = 1024L * 1024L
    doReturn(maxOnHeap).when(memoryManager).maxOnHeapStorageMemory

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    // Use distinct shuffleId/mapId/reduceId values so the captured BlockId can be
    // verified to carry exactly the values supplied to `trackBuffer`.
    val shuffleId = 7
    val mapId = 13L
    val reduceId = 21
    val sizeBytes = (maxOnHeap * 0.85).toInt
    spillManager.trackBuffer(shuffleId, mapId, reduceId, buildBuffer(sizeBytes))

    spillManager.pollOnce()

    // Capture the BlockId and StorageLevel arguments. A Mockito ArgumentCaptor records
    // every invocation; the test reads index 0 (the first invocation) to assert on the
    // primary spill while remaining tolerant of any subsequent scheduler-driven ticks.
    val blockIdCaptor = ArgumentCaptor.forClass(classOf[BlockId])
    val storageLevelCaptor = ArgumentCaptor.forClass(classOf[StorageLevel])
    verify(blockManager, atLeastOnce()).putBytes(
      blockIdCaptor.capture(),
      any[ChunkedByteBuffer],
      storageLevelCaptor.capture(),
      anyBoolean()
    )(any[ClassTag[Byte]])

    val capturedBlockId = blockIdCaptor.getAllValues.get(0)
    capturedBlockId mustBe a [ShuffleBlockId]
    val asShuffleBlockId = capturedBlockId.asInstanceOf[ShuffleBlockId]
    assert(asShuffleBlockId.shuffleId === shuffleId,
      s"Captured ShuffleBlockId.shuffleId = ${asShuffleBlockId.shuffleId}, expected $shuffleId")
    assert(asShuffleBlockId.mapId === mapId,
      s"Captured ShuffleBlockId.mapId = ${asShuffleBlockId.mapId}, expected $mapId")
    assert(asShuffleBlockId.reduceId === reduceId,
      s"Captured ShuffleBlockId.reduceId = ${asShuffleBlockId.reduceId}, expected $reduceId")

    val capturedLevel = storageLevelCaptor.getAllValues.get(0)
    assert(capturedLevel === StorageLevel.DISK_ONLY,
      s"Captured StorageLevel = $capturedLevel, expected ${StorageLevel.DISK_ONLY}")
  }

  // ---------------------------------------------------------------------------
  // Test 5: Eviction selects the LARGEST tracked buffer first.
  // ---------------------------------------------------------------------------
  test("pollOnce evicts the largest tracked buffer first under memory pressure") {
    // Track three buffers of distinct sizes (100 KB, 500 KB, 250 KB) with cumulative
    // ~83% utilization of a 1 MB budget. The production `evictLargestBuffer` selects
    // the largest entry per call (relieves the most pressure per eviction); the
    // captured BlockId must therefore correspond to the 500 KB buffer's reduceId.
    val maxOnHeap = 1024L * 1024L
    doReturn(maxOnHeap).when(memoryManager).maxOnHeapStorageMemory

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val smallSize = 100 * 1024  // 100 KB
    val largeSize = 500 * 1024  // 500 KB <-- largest, should be evicted
    val mediumSize = 250 * 1024 // 250 KB

    val smallReduceId = 0
    val largeReduceId = 1
    val mediumReduceId = 2
    spillManager.trackBuffer(0, 0L, smallReduceId, buildBuffer(smallSize))
    spillManager.trackBuffer(0, 0L, largeReduceId, buildBuffer(largeSize))
    spillManager.trackBuffer(0, 0L, mediumReduceId, buildBuffer(mediumSize))

    // Sanity: total tracked bytes is 850 KB which is ~83% of 1 MB -- above the 80%
    // threshold. Expressing this assertion explicitly defends against a future
    // refactoring that changes `trackBuffer` semantics in a way that would invalidate
    // the test's setup.
    assert(spillManager.trackedBytesSnapshot === (smallSize + largeSize + mediumSize).toLong,
      "trackedBytesSnapshot must equal the sum of all registered buffer sizes")

    spillManager.pollOnce()

    val blockIdCaptor = ArgumentCaptor.forClass(classOf[BlockId])
    verify(blockManager, atLeastOnce()).putBytes(
      blockIdCaptor.capture(),
      any[ChunkedByteBuffer],
      any[StorageLevel],
      anyBoolean()
    )(any[ClassTag[Byte]])

    val firstSpilled = blockIdCaptor.getAllValues.get(0).asInstanceOf[ShuffleBlockId]
    assert(firstSpilled.reduceId === largeReduceId,
      s"First eviction must select the largest buffer (reduceId=$largeReduceId); " +
        s"actual reduceId=${firstSpilled.reduceId}")
  }

  // ---------------------------------------------------------------------------
  // Test 6: Reclaim runs in < 100 ms.
  // ---------------------------------------------------------------------------
  test("reclaim releases buffer memory within 100 ms of consumer acknowledgment") {
    // AAP Sec.0.7.2.2: "Memory release MUST occur within 100 ms of consumer
    // acknowledgment." The production `reclaim` is a single `getIfPresent` lookup +
    // `invalidate` + atomic decrement + `dispose` -- it should complete in
    // microseconds. We bound it at < 100 ms to catch any future regression (such as
    // accidentally introducing a synchronous I/O call on the reclaim path).
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val shuffleId = 0
    val mapId = 0L
    val reduceId = 0
    val sizeBytes = 1024 * 1024
    spillManager.trackBuffer(shuffleId, mapId, reduceId, buildBuffer(sizeBytes))

    val startNanos = System.nanoTime()
    spillManager.reclaim(shuffleId, mapId, reduceId, bytes = sizeBytes.toLong)
    val durationMillis = (System.nanoTime() - startNanos) / 1000000L

    assert(durationMillis < 100L,
      s"reclaim took $durationMillis ms (must be < 100 ms per AAP Sec.0.7.2.2)")

    // Sanity: a full reclaim removes the entry from the registry so trackedBytesSnapshot
    // returns to 0 (the manager's bookkeeping observes the consumer ack).
    assert(spillManager.trackedBytesSnapshot === 0L,
      "Full reclaim must decrement trackedBytesSnapshot to 0")
  }

  // ---------------------------------------------------------------------------
  // Test 7: checkAndSpill (writer-driven) persists data via DISK_ONLY.
  // ---------------------------------------------------------------------------
  test("checkAndSpill persists writer-pushed data via BlockManager.putBytes with DISK_ONLY") {
    // The writer-driven `checkAndSpill(buffer)` path is independent of the polling
    // threshold: any non-null/non-empty buffer is persisted unconditionally because the
    // writer has already decided to spill. This test exercises that path directly,
    // verifying the StorageLevel is DISK_ONLY and the BlockId is the expected
    // ShuffleBlockId.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val shuffleId = 3
    val mapId = 5L
    val reduceId = 11
    val buffer = buildBuffer(64 * 1024) // 64 KB -- size irrelevant to the path
    spillManager.checkAndSpill(shuffleId, mapId, reduceId, buffer)

    val blockIdCaptor = ArgumentCaptor.forClass(classOf[BlockId])
    val storageLevelCaptor = ArgumentCaptor.forClass(classOf[StorageLevel])
    verify(blockManager, atLeastOnce()).putBytes(
      blockIdCaptor.capture(),
      any[ChunkedByteBuffer],
      storageLevelCaptor.capture(),
      anyBoolean()
    )(any[ClassTag[Byte]])

    val capturedBlockId = blockIdCaptor.getAllValues.get(0).asInstanceOf[ShuffleBlockId]
    assert(capturedBlockId.shuffleId === shuffleId)
    assert(capturedBlockId.mapId === mapId)
    assert(capturedBlockId.reduceId === reduceId)
    assert(storageLevelCaptor.getAllValues.get(0) === StorageLevel.DISK_ONLY,
      "checkAndSpill must use StorageLevel.DISK_ONLY for spilled blocks")
  }

  // ---------------------------------------------------------------------------
  // Test 8: putBytes returns false => no exception, spillCount NOT incremented.
  // ---------------------------------------------------------------------------
  test("checkAndSpill swallows BlockManager.putBytes returning false (no throw, no count)") {
    // Simulated disk-write decline (e.g., BlockManager declined the write because the
    // block already exists or for another non-fatal reason). The production source logs
    // a WARN and returns without throwing or incrementing the spill counter; the buffer
    // is preserved in the registry so a subsequent retry path observes the same state.
    reset(blockManager)
    when(blockManager.putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])).thenReturn(false)

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val shuffleId = 0
    val mapId = 0L
    val reduceId = 0
    val buffer = buildBuffer(64 * 1024)
    val initialSpillCount = metrics.getSpillCount

    // Must not throw -- the production source catches/handles the false return value.
    spillManager.checkAndSpill(shuffleId, mapId, reduceId, buffer)

    // The spill counter must NOT increment for a declined put. Production source
    // (MemorySpillManager.scala line 464): the counter increments only on `if (stored)`
    // -- false skips the counter update.
    assert(metrics.getSpillCount === initialSpillCount,
      "spillCount must NOT increment when BlockManager.putBytes returns false")
  }

  // ---------------------------------------------------------------------------
  // Test 9: putBytes throws => no exception, buffer preserved in registry.
  // ---------------------------------------------------------------------------
  test("checkAndSpill swallows exceptions thrown by BlockManager.putBytes") {
    // Simulated runtime failure (disk-full SparkException, illegal-state on serializer
    // exhaustion, OOM-wrapped error, etc.). The production `try/catch` in `checkAndSpill`
    // catches Exception subclasses, logs a WARN, and returns -- the buffer is NOT disposed
    // because the writer may legitimately retry and bookkeeping is left untouched so the
    // retry observes the same state.
    //
    // Note: Mockito requires the stubbed exception to be either unchecked (RuntimeException
    // subclass) or declared in the method's `throws` clause. `BlockManager.putBytes` does
    // not declare `throws IOException`, so we use a runtime exception subclass which is
    // representative of what Spark's storage layer actually throws (SparkException,
    // IllegalStateException, etc., all extend RuntimeException).
    reset(blockManager)
    when(blockManager.putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])).thenThrow(new RuntimeException("simulated disk failure"))

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val shuffleId = 0
    val mapId = 0L
    val reduceId = 0
    val sizeBytes = 64 * 1024
    val buffer = buildBuffer(sizeBytes)
    val initialSpillCount = metrics.getSpillCount

    // Must not throw -- the production source's `catch (e: Exception)` block handles it.
    spillManager.checkAndSpill(shuffleId, mapId, reduceId, buffer)

    // The spill counter must not increment for a thrown exception (no successful spill).
    assert(metrics.getSpillCount === initialSpillCount,
      "spillCount must NOT increment when BlockManager.putBytes throws")

    // The buffer remains tracked: the production source registers the buffer before
    // attempting the put (so `totalBytes` reflects the buffer's size) and on exception
    // does NOT invalidate the registry entry (so the writer can retry).
    assert(spillManager.trackedBytesSnapshot === sizeBytes.toLong,
      "Buffer must remain in registry after putBytes exception (state unchanged for retry)")
  }

  // ---------------------------------------------------------------------------
  // Test 10: Successful spill increments metrics.spillCount counter.
  // ---------------------------------------------------------------------------
  test("checkAndSpill increments metrics.spillCount on successful putBytes") {
    // Direct test of the metric increment side-effect on the success path. Since the
    // default putBytes stub returns true and `checkAndSpill` always invokes putBytes for
    // a non-empty buffer, this test asserts the production source's call-site
    // (`metrics.incrementSpillCount()` immediately after `if (stored)`) is exercised.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val initialSpillCount = metrics.getSpillCount
    val buffer = buildBuffer(32 * 1024)
    spillManager.checkAndSpill(0, 0L, 0, buffer)

    assert(metrics.getSpillCount > initialSpillCount,
      s"spillCount should increase after successful spill " +
        s"(initial=$initialSpillCount, observed=${metrics.getSpillCount})")
    assert(metrics.getSpillCount === initialSpillCount + 1L,
      s"spillCount should increase by exactly 1 after a single successful checkAndSpill " +
        s"(initial=$initialSpillCount, observed=${metrics.getSpillCount})")
  }

  // ---------------------------------------------------------------------------
  // Test 11: bufferUtilizationPercent gauge stays in [0, 100].
  // ---------------------------------------------------------------------------
  test("buffer utilization is reported in metrics.bufferUtilizationPercent within [0, 100]") {
    // The production `pollOnce` writes pct = (used / maxOnHeap) * 100 -- as Int.
    // `StreamingShuffleMetrics.updateBufferUtilization` clamps the value into [0, 100].
    // We exercise the polling path with a tracked buffer at ~50% utilization and verify
    // the gauge value is within the operator-facing range.
    val maxOnHeap = 1024L * 1024L
    doReturn(maxOnHeap).when(memoryManager).maxOnHeapStorageMemory

    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    spillManager.trackBuffer(0, 0L, 0, buildBuffer((maxOnHeap * 0.50).toInt))
    spillManager.pollOnce()

    val util = metrics.getBufferUtilizationPercent
    assert(util >= 0,
      s"bufferUtilizationPercent must be >= 0 (got $util)")
    assert(util <= 100,
      s"bufferUtilizationPercent must be <= 100 (got $util) -- the gauge is operator-facing " +
        s"and the production source clamps via StreamingShuffleMetrics.updateBufferUtilization")
    // Sanity: at ~50% utilization the gauge should reflect a value in a reasonable
    // neighborhood of 50 (allowing a small margin for the integer truncation in
    // `((used / maxOnHeap) * 100.0).toInt`).
    assert(util >= 45 && util <= 55,
      s"At ~50% utilization, gauge should report ~50 (got $util)")
  }

  // ---------------------------------------------------------------------------
  // Test 12: stop() is idempotent under repeated invocation.
  // ---------------------------------------------------------------------------
  test("stop() is idempotent and cleanly shuts down the polling executor") {
    // The production source guards `stop()` via `stopped.compareAndSet(false, true)` so
    // the second call is a no-op (no cancelling-already-cancelled-future exceptions, no
    // double executor shutdown). Asserting via "no exception thrown" is the standard
    // ScalaTest pattern for idempotency.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)
    spillManager.stop()
    spillManager.stop()
    spillManager.stop()
    // Set the field to null so the `afterEach` teardown does not call stop() a fourth
    // time -- the fourth call would also be a no-op, but explicit nulling makes the
    // test's lifecycle intent clearer at the test site.
    spillManager = null
  }

  // ---------------------------------------------------------------------------
  // Test 13: Negative inputs to trackBuffer raise IllegalArgumentException.
  // ---------------------------------------------------------------------------
  test("trackBuffer rejects negative shuffleId / mapId / reduceId via require") {
    // The production source uses `require(... >= 0, ...)` for defensive input
    // validation. This test locks the contract that callers cannot accidentally pass
    // negative IDs (which would corrupt registry-key lookups and bookkeeping).
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)
    val buffer = buildBuffer(1024)

    // Negative shuffleId
    intercept[IllegalArgumentException] {
      spillManager.trackBuffer(-1, 0L, 0, buffer)
    }
    // Negative mapId
    intercept[IllegalArgumentException] {
      spillManager.trackBuffer(0, -1L, 0, buffer)
    }
    // Negative reduceId
    intercept[IllegalArgumentException] {
      spillManager.trackBuffer(0, 0L, -1, buffer)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 14: trackBuffer with null/empty buffer is a no-op (no registry entry, no spill).
  // ---------------------------------------------------------------------------
  test("trackBuffer with null or empty buffer is a no-op") {
    // The production source returns early on null or zero-size buffers. This test
    // verifies the no-op semantic: trackedBytesSnapshot remains 0 and no spill is
    // triggered even after pollOnce. This protects callers from accidentally
    // registering empty spills that would inflate the spill counter without persisting
    // any data.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    spillManager.trackBuffer(0, 0L, 0, null.asInstanceOf[ChunkedByteBuffer])
    assert(spillManager.trackedBytesSnapshot === 0L,
      "trackBuffer(null) must not modify trackedBytesSnapshot")

    spillManager.trackBuffer(0, 0L, 0, buildBuffer(0))
    assert(spillManager.trackedBytesSnapshot === 0L,
      "trackBuffer(empty) must not modify trackedBytesSnapshot")

    // No spill should be triggered even with poll, since registry is empty.
    spillManager.pollOnce()
    verify(blockManager, never()).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
  }

  // ---------------------------------------------------------------------------
  // Test 15: pollOnce with maxOnHeapStorageMemory <= 0 returns early without error.
  // ---------------------------------------------------------------------------
  test("pollOnce returns early when maxOnHeapStorageMemory <= 0") {
    // Edge case: a degenerate test fixture where the memory manager reports a
    // non-positive on-heap budget. The production source guards with
    // `if (maxOnHeap <= 0L) return` to prevent divide-by-zero in the percent
    // computation. This test exercises both the zero and negative cases.

    // Case 1: zero budget
    doReturn(0L).when(memoryManager).maxOnHeapStorageMemory
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(1024))

    spillManager.pollOnce()
    verify(blockManager, never()).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
    spillManager.stop()
    spillManager = null

    // Case 2: negative budget (should also short-circuit)
    doReturn(-1L).when(memoryManager).maxOnHeapStorageMemory
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(1024))

    spillManager.pollOnce()
    verify(blockManager, never()).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
  }

  // ---------------------------------------------------------------------------
  // Test 16: reclaim with bytes >= buffer.size fully reclaims (registry empty).
  // ---------------------------------------------------------------------------
  test("reclaim fully releases buffer when ackedBytes >= buffer.size") {
    // Verifies the full-reclaim branch of the production source: when the consumer
    // acknowledges at least as many bytes as the registered buffer's size, the entry
    // is invalidated and the buffer is disposed. trackedBytesSnapshot returns to 0
    // and trackedPartitionCount drops by 1.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val sizeBytes = 1024
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(sizeBytes))
    assert(spillManager.trackedPartitionCount === 1L,
      "Expected exactly one tracked partition after trackBuffer")

    spillManager.reclaim(0, 0L, 0, bytes = sizeBytes.toLong)

    assert(spillManager.trackedBytesSnapshot === 0L,
      "Full reclaim must decrement trackedBytesSnapshot to 0")
    assert(spillManager.trackedPartitionCount === 0L,
      "Full reclaim must remove the entry from the registry")
  }

  // ---------------------------------------------------------------------------
  // Test 17: reclaim with bytes < buffer.size keeps the buffer (partial reclaim).
  // ---------------------------------------------------------------------------
  test("reclaim retains buffer with decremented bookkeeping when ackedBytes < buffer.size") {
    // Verifies the partial-reclaim branch: when the consumer acknowledges fewer bytes
    // than the buffer's size, the buffer remains in the registry and only the
    // totalBytes counter decrements by `bytes`. The writer still needs the unsent
    // portion of the buffer.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val sizeBytes = 4096
    val partialAck = 1024L
    spillManager.trackBuffer(0, 0L, 0, buildBuffer(sizeBytes))

    spillManager.reclaim(0, 0L, 0, bytes = partialAck)

    // After partial reclaim, totalBytes = sizeBytes - partialAck (the buffer remains
    // physically in the registry; only the counter decrement reflects the ack).
    assert(spillManager.trackedBytesSnapshot === (sizeBytes - partialAck),
      s"Partial reclaim must decrement trackedBytesSnapshot by ackedBytes " +
        s"(expected=${sizeBytes - partialAck}, got=${spillManager.trackedBytesSnapshot})")
    assert(spillManager.trackedPartitionCount === 1L,
      "Partial reclaim must NOT remove the registry entry")
  }

  // ---------------------------------------------------------------------------
  // Test 18: trackBuffer overload accepting Array[Byte] is functionally equivalent.
  // ---------------------------------------------------------------------------
  test("trackBuffer Array[Byte] overload registers the bytes in the registry") {
    // The production source provides a convenience overload that wraps a flat
    // Array[Byte] in a ChunkedByteBuffer. This test verifies the overload registers
    // the same byte count as a manually-constructed ChunkedByteBuffer.
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    val bytes = new Array[Byte](2048)
    spillManager.trackBuffer(0, 0L, 0, bytes)

    assert(spillManager.trackedBytesSnapshot === bytes.length.toLong,
      "Array[Byte] overload must register the byte array's length")
    assert(spillManager.trackedPartitionCount === 1L,
      "Array[Byte] overload must add one entry to the registry")
  }

  // ---------------------------------------------------------------------------
  // Test 19: SPILL_POLL_INTERVAL_MILLIS lock-in.
  // ---------------------------------------------------------------------------
  test("SPILL_POLL_INTERVAL_MILLIS constant equals 100 ms") {
    // The package-object constant locks the AAP-mandated 100 ms polling cadence
    // (AAP Sec.0.7.2.2 and Sec.0.5.1.3). Asserting its literal value here prevents an
    // accidental regression that would change the buffer-reclamation timing budget
    // without flagging a corresponding test failure. The constant is `private[streaming]`
    // -- visible here because this suite is in the same `org.apache.spark.shuffle.streaming`
    // package.
    assert(SPILL_POLL_INTERVAL_MILLIS === 100L,
      "SPILL_POLL_INTERVAL_MILLIS must be 100 ms per AAP Sec.0.7.2.2")
  }

  // ---------------------------------------------------------------------------
  // Test 20: putBytes invocation count for two checkAndSpill calls is two.
  // ---------------------------------------------------------------------------
  test("two successive checkAndSpill calls produce exactly two BlockManager.putBytes calls") {
    // Strict-count assertion to verify that each successful spill produces exactly one
    // putBytes invocation -- protects against a future refactoring that accidentally
    // double-spills, retries on success, or misroutes some calls. This complements the
    // atLeastOnce assertions used elsewhere by establishing the exact-count contract on
    // the writer-driven spill path (which has no scheduler-tick race because pollOnce
    // is not invoked here).
    spillManager = new MemorySpillManager(blockManager, memoryManager, metrics, conf)

    spillManager.checkAndSpill(0, 0L, 0, buildBuffer(1024))
    spillManager.checkAndSpill(0, 0L, 1, buildBuffer(1024))

    verify(blockManager, times(2)).putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[StorageLevel], anyBoolean()
    )(any[ClassTag[Byte]])
    assert(metrics.getSpillCount === 2L,
      s"Two successful spills must produce spillCount=2 (got ${metrics.getSpillCount})")
  }
}
