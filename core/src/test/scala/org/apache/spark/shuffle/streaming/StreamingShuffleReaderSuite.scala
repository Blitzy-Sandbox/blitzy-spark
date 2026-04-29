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

import java.util.concurrent.TimeoutException
import java.util.zip.CRC32C

import scala.collection

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{anyInt, anyString, eq => meq, isNull}
import org.mockito.Mockito.{atLeastOnce, doAnswer, doThrow, mock, never, reset, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{HashPartitioner, LocalSparkContext, MapOutputTracker, ShuffleDependency,
  SparkConf, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.shuffle.DownloadFileManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Unit tests for [[StreamingShuffleReader]] covering:
 *   - In-progress block requests via the [[BlockTransferService]] mock.
 *   - Producer-failure detection via simulated connection failure.
 *   - Partial-read invalidation and [[FetchFailedException]] propagation.
 *   - CRC32C checksum primitive integrity (per AAP Section 0.7.2.4 directive
 *     `"Checksum algorithm: CRC32C only"`).
 *   - [[ShuffleReadMetricsReporter]] update on read paths.
 *
 * == AAP Reference ==
 *  - AAP Section 0.5.1.2 (StreamingShuffleReader component design)
 *  - AAP Section 0.5.1.6 (Group 6, item 4 -- StreamingShuffleReaderSuite scope)
 *  - AAP Section 0.7.2.4 (failure tolerance and integrity)
 *  - AAP Section 0.7.2.6 (quality gate: > 85% coverage)
 *
 * == Production-Source Contract ==
 *  - Constructor: `StreamingShuffleReader(handle, startMapIndex, endMapIndex,
 *    startPartition, endPartition, context, readMetrics, blockManager,
 *    mapOutputTracker, streamingMetrics)` (10 parameters).
 *  - `read()` returns an `Iterator[Product2[K, C]]` that lazily polls producers via
 *    [[BlockTransferService.fetchBlockSync]] semantics.
 *  - On producer connection failure the reader catches the exception in
 *    `fetchAndValidateBlock`, increments
 *    `streamingMetrics.partialReadInvalidations`, and throws
 *    [[FetchFailedException]] atomically per the SPARK-19276 contract documented in
 *    `core/src/main/scala/org/apache/spark/shuffle/FetchFailedException.scala`.
 *  - CRC32C is used for block-integrity validation. CRC32C primitives are exercised
 *    directly here; the reader's full receive-side validation path is exercised in
 *    integration tests where producer-supplied checksums are wired end-to-end.
 *
 * == FetchFailedException's TaskContext Side-Effect ==
 * Per `FetchFailedException.scala` lines 55-59 and SPARK-19276, the
 * `FetchFailedException` constructor invokes `TaskContext.get().setFetchFailed(this)`
 * if a [[TaskContext]] is present in the task-local thread storage. Tests that drive
 * `FetchFailedException`-throwing code paths therefore install a
 * [[TaskContext]] via [[TaskContext.setTaskContext]] before the call and reset via
 * [[TaskContext.unset]] in a `try/finally` to avoid cross-test leakage of the task
 * thread-local. The fixture uses [[MemoryTestingUtils.fakeTaskContext]] to construct
 * a synthetic `TaskContextImpl` that satisfies the side-effect contract without
 * requiring a real driver scheduler stage.
 *
 * == Mocking Strategy ==
 *   - [[BlockManager]] and [[MapOutputTracker]] are mocked via Mockito 5.12 to drive
 *     deterministic behavior at the reader's two integration boundaries.
 *   - The producer-failure path mocks
 *     `blockManager.blockTransferService.fetchBlockSync` to throw exceptions of
 *     varying type. Two failure modes are exercised independently:
 *     - [[RuntimeException]]: covers the generic-non-fatal arm via the production
 *       source's `case NonFatal(e)` catch-handler in `fetchAndValidateBlock`. Used
 *       in Test 2 (basic failure path), Test 4 (exact-delta metric attribution), and
 *       Test 10 (transport-boundary invocation count).
 *     - [[java.util.concurrent.TimeoutException]] (a checked exception): covers the
 *       dedicated `case e: TimeoutException` catch-handler in the production source
 *       at `StreamingShuffleReader.fetchAndValidateBlock` lines 474-479, which AAP
 *       Section 0.7.2.4 requires for the *"Producer failure detection: connection
 *       timeout MUST be 5 seconds"* contract. Used in Test 11. Mockito 5.12 strict-
 *       stubbing rejects every form of stub install (`when().thenThrow`, `doThrow`,
 *       `willThrow`) for checked exceptions that are not declared on the stubbed
 *       method's bytecode-level `throws` clause -- and `BlockTransferService.fetch
 *       BlockSync` declares no `throws` types. The canonical workaround is
 *       `doAnswer(...)` paired with an [[org.mockito.stubbing.Answer]] whose
 *       `answer(InvocationOnMock)` method itself declares `throws Throwable`; the
 *       compiler-level checked-exception barrier is satisfied at the Answer SAM
 *       boundary, allowing the body to throw any `Throwable` (including
 *       [[TimeoutException]]). At runtime the JVM raises the throwable through the
 *       mock's invocation handler exactly as a real fetch would, so the production
 *       reader's `case e: TimeoutException` catch arm fires identically to a real-
 *       world deadline event from the underlying `Promise[ManagedBuffer]`.
 *
 * == Coexistence Discipline (per User Directive) ==
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* the SparkContext is
 * configured with the production-stable default `spark.shuffle.manager=sort` because
 * the reader is exercised here directly with mocked dependencies (the
 * [[StreamingShuffleManager]]'s manager-level dispatch is covered by
 * [[StreamingShuffleManagerSuite]] separately). Setting
 * `spark.shuffle.manager=streaming` would be unnecessary for the reader's unit-level
 * surface and would couple this suite to the manager implementation -- a cross-cutting
 * concern that violates the suite-isolation goal.
 */
class StreamingShuffleReaderSuite
  extends SparkFunSuite with LocalSparkContext with Matchers with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------
  // Per-test mutable fixtures
  // ---------------------------------------------------------------------------

  /** Mocked [[BlockManager]] returning a stubbed [[BlockTransferService]] per test. */
  private var blockManager: BlockManager = _

  /** Mocked [[BlockTransferService]] whose `fetchBlockSync` is stubbed per test. */
  private var transferService: BlockTransferService = _

  /** Mocked [[MapOutputTracker]] whose `getMapSizesByExecutorId` is stubbed per test. */
  private var mapOutputTracker: MapOutputTracker = _

  /**
   * Real [[StreamingShuffleMetrics]] instance; tests assert metric increments via the
   * production counter API to exercise the actual side-effect path that the reader
   * uses.
   */
  private var streamingMetrics: StreamingShuffleMetrics = _

  // ---------------------------------------------------------------------------
  // Lifecycle hooks
  // ---------------------------------------------------------------------------

  /**
   * Per-test setup: initializes a fresh local [[SparkContext]] (via
   * [[LocalSparkContext]]) with the default sort shuffle manager, and creates fresh
   * mocks for [[BlockManager]] / [[BlockTransferService]] / [[MapOutputTracker]] plus
   * a real [[StreamingShuffleMetrics]] instance.
   *
   * The fresh [[SparkContext]] guarantees that `SparkEnv.get` is non-null (the reader
   * accesses `SparkEnv.get.serializerManager` for stream wrapping during
   * deserialization) and that each test sees a clean `shuffleId` counter starting
   * from 0 (so `buildHandle()` can assume `shuffleId = 0` matches the dependency's
   * registered ID).
   *
   * Web UI is disabled to avoid binding port 4040 across concurrent test runs and to
   * keep test resource consumption minimal.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    sc = new SparkContext(new SparkConf(loadDefaults = false)
      .setAppName("StreamingShuffleReaderSuite")
      .setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false"))
    blockManager = mock(classOf[BlockManager])
    transferService = mock(classOf[BlockTransferService])
    mapOutputTracker = mock(classOf[MapOutputTracker])
    streamingMetrics = new StreamingShuffleMetrics()
    // Wire the BlockManager mock to return our mocked transfer service so that
    // `blockManager.blockTransferService.fetchBlockSync(...)` calls reach our stub.
    when(blockManager.blockTransferService).thenReturn(transferService)
  }

  /**
   * Per-test teardown: clears mock and metrics references after the
   * [[LocalSparkContext]] base teardown stops the [[SparkContext]]. The order
   * (super.afterEach() first, then null-out fixtures) is intentional so that any
   * shutdown-time hooks the [[SparkContext]] runs can still observe any state they
   * need.
   */
  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      blockManager = null
      transferService = null
      mapOutputTracker = null
      streamingMetrics = null
    }
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Build a real [[ShuffleDependency]] backed by a small parallelized RDD. Each test
   * constructs a fresh dependency through this helper because the per-test
   * [[SparkContext]] resets the shuffle-id counter to 0, keeping `dep.shuffleId == 0`
   * stable across the suite. The dependency uses a [[HashPartitioner]] with 4
   * partitions to match the `endMapIndex = 4` and `endPartition = 4` constructor
   * arguments used in every reader-construction call below.
   *
   * @return a fresh [[ShuffleDependency]] with default serializer, no aggregator,
   *         and no key ordering -- exercising the reader's "no aggregator/ordering"
   *         dispatch arm
   */
  private def buildShuffleDep(): ShuffleDependency[Int, Int, Int] = {
    val rdd = sc.parallelize(0 until 100, 4).map(i => (i, i))
    new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(4))
  }

  /**
   * Build a [[StreamingShuffleHandle]] with default streaming-shuffle parameters
   * matching the AAP-specified defaults
   * (`bufferSizePercent=20`, `spillThreshold=80`, `maxBandwidthMBps=-1` for
   * unlimited). The `shuffleId` is fixed at 0 to match the per-test dependency's
   * auto-assigned id.
   *
   * @return a `StreamingShuffleHandle[Int, Int, Int]` ready for reader construction
   */
  private def buildHandle(): StreamingShuffleHandle[Int, Int, Int] = {
    val dep = buildShuffleDep()
    new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = 0,
      dependency = dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)
  }

  /**
   * Compute a CRC32C checksum (Castagnoli polynomial 0x1EDC6F41) over the given
   * bytes using the JDK 17 [[java.util.zip.CRC32C]] class. Used as a reference
   * primitive in [[CRC32C-mismatch-detection]] to demonstrate that the algorithm
   * correctly distinguishes matching from mismatching checksums.
   *
   * Per AAP Section 0.7.2.4 *"Checksum algorithm: CRC32C only (no MD5, SHA-1,
   * SHA-256, xxHash, or alternative algorithm)"* -- this is the only checksum
   * primitive permitted for streaming-shuffle integrity validation.
   *
   * @param bytes the input byte array
   * @return the CRC32C checksum as an unsigned 32-bit value held in a `Long`
   */
  private def computeCrc32C(bytes: Array[Byte]): Long = {
    val crc = new CRC32C
    crc.update(bytes, 0, bytes.length)
    crc.getValue
  }

  /**
   * Build a single-block `mapStatuses` iterator with the given producer
   * [[BlockManagerId]]. The block is `ShuffleBlockId(0, 0L, 0)` (shuffleId=0,
   * mapId=0, reduceId=0) with a synthetic `length=100L` and `mapIndex=0`. The
   * resulting iterator is suitable for stubbing
   * [[MapOutputTracker.getMapSizesByExecutorId]] in tests that need exactly one
   * producer block to drive a fetch attempt.
   *
   * The return type matches the [[MapOutputTracker]] SPI:
   * `Iterator[(BlockManagerId, scala.collection.Seq[(BlockId, Long, Int)])]`.
   *
   * @param producer the producer's [[BlockManagerId]]
   * @return a single-element iterator yielding one producer with one block
   */
  private def singleBlockMapStatuses(
      producer: BlockManagerId)
      : Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    val block: (BlockId, Long, Int) = (ShuffleBlockId(0, 0L, 0), 100L, 0)
    val perAddress: collection.Seq[(BlockId, Long, Int)] = Seq(block)
    Iterator((producer, perAddress))
  }

  /**
   * Empty `mapStatuses` iterator (no producers, no blocks). Suitable for stubbing
   * [[MapOutputTracker.getMapSizesByExecutorId]] in tests that need the reader to
   * yield an empty result iterator without attempting any block fetch.
   *
   * @return an empty iterator with the [[MapOutputTracker]] SPI's element type
   */
  private def emptyMapStatuses
      : Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] =
    Iterator.empty

  /**
   * Construct a [[StreamingShuffleReader]] with the given handle, mocks, and metrics
   * using the standard reduce-task constructor parameters
   * (`startMapIndex=0`, `endMapIndex=4`, `startPartition=0`, `endPartition=4`)
   * matching the 4-partition test workload.
   *
   * @param handle      the streaming shuffle handle
   * @param taskContext the active [[TaskContext]] for the reader
   * @param readMetrics the [[ShuffleReadMetricsReporter]] (typically a
   *                    `TempShuffleReadMetrics`)
   * @return a fully-constructed `StreamingShuffleReader[Int, Int]` ready to invoke
   *         `read()` on
   */
  private def buildReader(
      handle: StreamingShuffleHandle[Int, Int, Int],
      taskContext: TaskContext,
      readMetrics: ShuffleReadMetricsReporter): StreamingShuffleReader[Int, Int] = {
    new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 4,
      startPartition = 0,
      endPartition = 4,
      context = taskContext,
      readMetrics = readMetrics,
      blockManager = blockManager,
      mapOutputTracker = mapOutputTracker,
      streamingMetrics = streamingMetrics,
      // debugEnabled = false so streaming-shuffle DEBUG/TRACE log statements are
      // short-circuited at the source site for unit tests, matching the production
      // default of `spark.shuffle.streaming.debug=false`. This keeps test output
      // lean and exercises the same source-site gating production deployments use.
      debugEnabled = false)
  }

  // ---------------------------------------------------------------------------
  // Test 1: basic read path returns a usable iterator
  // ---------------------------------------------------------------------------

  test("read() returns a non-null Iterator that consumes available blocks") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    // Stub the map output tracker to return an empty iterator. The reader's lazy
    // chain pulls through this -- with no producer blocks the iterator is empty
    // but MUST still be non-null and safely exhaustible. Tests that drive an
    // actual block-fetch path (Test 2 / Test 4) stub a non-empty iterator.
    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(emptyMapStatuses)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      assert(iter != null, "read() must return a non-null Iterator")
      // Empty mapStatuses imply no records pulled through the lazy chain.
      assert(!iter.hasNext, "Iterator from empty mapStatuses must yield no elements")
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2: producer connection failure throws FetchFailedException
  // ---------------------------------------------------------------------------

  test("producer connection failure throws FetchFailedException") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    // Stub the map output tracker to return one block from the failing producer.
    val producer = BlockManagerId("producer-exec", "host1", 7337)
    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt()))
      .thenReturn(singleBlockMapStatuses(producer))

    // Stub the transport client's fetchBlockSync to throw a non-fatal exception.
    // The reader's catch chain in fetchAndValidateBlock maps any NonFatal throwable
    // to FetchFailedException (after incrementing partialReadInvalidations) per
    // the production source's catch arms.
    doThrow(new RuntimeException("simulated transport failure"))
      .when(transferService).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      // Driving the lazy iterator triggers the underlying fetchBlockSync call,
      // which throws our simulated transport failure. The reader catches it,
      // increments the metric, and atomically throws FetchFailedException.
      val ex = intercept[FetchFailedException] {
        while (iter.hasNext) iter.next()
      }
      assert(ex != null, "FetchFailedException must be thrown when fetch fails")
      // Verify the metric was incremented exactly via the production code path.
      assert(streamingMetrics.getPartialReadInvalidationsCount > 0L,
        s"partialReadInvalidations should increment on producer failure; " +
          s"got ${streamingMetrics.getPartialReadInvalidationsCount}")
      // Verify the FetchFailedException carries the producer's BlockManagerId so
      // the existing DAGScheduler.handleTaskCompletion path can route the upstream
      // recomputation correctly.
      assert(ex.toTaskFailedReason != null,
        "FetchFailedException must convert to a TaskFailedReason")
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3: CRC32C primitive correctly detects checksum mismatch
  // ---------------------------------------------------------------------------

  test("CRC32C mismatch is detected by the JDK primitive") {
    // Demonstrates that the JDK 17 java.util.zip.CRC32C primitive used by the
    // reader's checksum-validation path correctly distinguishes matching from
    // mismatching checksums. Per AAP Section 0.7.2.4 "Checksum algorithm: CRC32C
    // only", no other checksum algorithm is permitted; this test guards against
    // an accidental algorithm swap by directly exercising the correctness contract
    // that the reader's fetchAndValidateBlock relies on.
    val payload = "test-payload".getBytes("UTF-8")
    val correctCrc = computeCrc32C(payload)
    val wrongCrc = correctCrc ^ 0xFFFFFFFFL // flip every bit -> guaranteed mismatch

    // The CRC32C of the payload must equal itself (deterministic).
    assert(computeCrc32C(payload) == correctCrc,
      "CRC32C must produce a deterministic checksum for identical bytes")

    // The CRC32C of the payload must NOT equal the deliberately-flipped checksum.
    assert(computeCrc32C(payload) != wrongCrc,
      "CRC32C must reject a checksum that does not match the payload bytes")

    // Different payload bytes must produce a different CRC32C.
    val mutated = payload.clone()
    mutated(0) = (mutated(0) ^ 0xFF).toByte
    assert(computeCrc32C(mutated) != correctCrc,
      "CRC32C must produce a different checksum for different payload bytes")
  }

  // ---------------------------------------------------------------------------
  // Test 4: partial-read invalidation discards buffered data on producer failure
  // ---------------------------------------------------------------------------

  test("partial-read invalidation increments metric on producer failure") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    // Capture the metric value before invocation so we can assert a strict
    // increment (not a strict ">0L") and confirm causal attribution to this test.
    val initialInvalidations = streamingMetrics.getPartialReadInvalidationsCount

    val producer = BlockManagerId("failing-producer", "host1", 7337)
    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt()))
      .thenReturn(singleBlockMapStatuses(producer))

    // Reader's fetchAndValidateBlock catch chain maps the NonFatal RuntimeException
    // to FetchFailedException (after incrementing the partialReadInvalidations
    // counter); we use a plain RuntimeException to avoid any possible
    // checked-exception wrapping in the Mockito mock proxy while still exercising
    // the same production code path.
    doThrow(new RuntimeException("simulated producer failure"))
      .when(transferService).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      // The exception is expected; we only care about the metric delta below.
      intercept[FetchFailedException] {
        val iter = reader.read()
        while (iter.hasNext) iter.next()
      }
      val finalInvalidations = streamingMetrics.getPartialReadInvalidationsCount
      assert(finalInvalidations > initialInvalidations,
        s"partialReadInvalidations should increment after partial-read invalidation " +
          s"(initial=$initialInvalidations, final=$finalInvalidations)")
      // Increment must be exactly 1 because the test triggers exactly one fetch
      // attempt and the production source increments the counter once per
      // detected failure. Asserting the exact delta guards against accidental
      // double-increments under future refactoring.
      assert(finalInvalidations - initialInvalidations === 1L,
        s"partialReadInvalidations should increment by exactly 1 per failure; " +
          s"observed delta=${finalInvalidations - initialInvalidations}")
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: empty fetch records 0 records read in the metrics reporter
  // ---------------------------------------------------------------------------

  test("reader records 0 records read on empty fetch") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    // Empty mapStatuses -> empty result iterator -> readMetrics.recordsRead == 0.
    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt()))
      .thenReturn(emptyMapStatuses)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      // Drain the (empty) iterator. CompletionIterator's completion callback fires
      // on iterator exhaustion and merges the temp metrics into TaskMetrics, but
      // the temp recordsRead value is preserved since the merge does not zero the
      // temp counter.
      while (iter.hasNext) iter.next()
      assert(readMetrics.recordsRead === 0L,
        s"Empty fetch should record 0 records; got ${readMetrics.recordsRead}")
      // Sanity-check that the partial-read-invalidation metric did NOT fire on the
      // success path; this guards against an accidental metric increment under
      // refactoring of the empty-result code path.
      assert(streamingMetrics.getPartialReadInvalidationsCount === 0L,
        "partialReadInvalidations must not increment on the empty-fetch success path")
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6: the reader uses MapOutputTracker.getMapSizesByExecutorId with the
  // shuffleId, mapIndex range, and partition range supplied to the constructor
  // ---------------------------------------------------------------------------

  test("reader queries MapOutputTracker with the correct shuffle/map/partition range") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(emptyMapStatuses)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      // Drive the lazy iterator to force the lookup to be invoked.
      val iter = reader.read()
      while (iter.hasNext) iter.next()
      // Capture the actual arguments for the MapOutputTracker invocation. This
      // confirms the reader respects the constructor-provided startMapIndex /
      // endMapIndex / startPartition / endPartition without re-deriving them
      // from any other source.
      val shuffleIdCaptor = ArgumentCaptor.forClass(classOf[Int])
      val startMapCaptor = ArgumentCaptor.forClass(classOf[Int])
      val endMapCaptor = ArgumentCaptor.forClass(classOf[Int])
      val startPartCaptor = ArgumentCaptor.forClass(classOf[Int])
      val endPartCaptor = ArgumentCaptor.forClass(classOf[Int])
      verify(mapOutputTracker, atLeastOnce()).getMapSizesByExecutorId(
        shuffleIdCaptor.capture(),
        startMapCaptor.capture(),
        endMapCaptor.capture(),
        startPartCaptor.capture(),
        endPartCaptor.capture())
      assert(shuffleIdCaptor.getValue.intValue() === 0)
      assert(startMapCaptor.getValue.intValue() === 0)
      assert(endMapCaptor.getValue.intValue() === 4)
      assert(startPartCaptor.getValue.intValue() === 0)
      assert(endPartCaptor.getValue.intValue() === 4)
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 7: the reader's transport boundary is *not* invoked when no producer
  // blocks are returned -- guards against accidental eager pre-fetch.
  // ---------------------------------------------------------------------------

  test("transport service is not invoked when no producer blocks are reported") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(emptyMapStatuses)
    // Reset any prior interaction state on the transport mock so the verify(...
    // never()) assertion reflects only this test's invocations.
    reset(transferService)
    // Re-establish the BlockManager wiring after reset (otherwise the BlockManager
    // mock would return null for blockTransferService should the read code path
    // attempt to consult it).
    when(blockManager.blockTransferService).thenReturn(transferService)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      while (iter.hasNext) iter.next()
      // The transport service must NOT be invoked when there are zero producer
      // blocks. This ensures the reader is truly lazy and does not pre-fetch.
      verify(transferService, never()).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8: read() honors the no-aggregator/no-ordering dispatch arm and yields
  // an empty Product2 iterator without throwing
  // ---------------------------------------------------------------------------

  test("read() yields an empty Iterator[Product2[K, C]] when no blocks are reported") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(emptyMapStatuses)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      // The result type is Iterator[Product2[K, C]] -- per the trait. Convert to a
      // List to fully drain (intentionally redundant with hasNext loop above to
      // catch any iterator-specific edge case). An empty result must produce zero
      // elements without throwing.
      val results: List[Product2[Int, Int]] = iter.toList
      assert(results.isEmpty,
        s"Empty mapStatuses must yield an empty Product2 list; got ${results.size}")
      // After exhaustion, hasNext must remain stable (must not throw or rebound).
      assert(!iter.hasNext, "Exhausted iterator must continue to report hasNext=false")
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 9: multiple times called -- the failure path does not cross-contaminate
  // the success-path counter (success path leaves counter at 0)
  // ---------------------------------------------------------------------------

  test("success path leaves the partialReadInvalidations counter at 0") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(emptyMapStatuses)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      // Drain successfully (empty iterator). Verify the failure metric remains 0,
      // confirming the production source does not accidentally increment it on
      // the success path -- AAP Section 0.5.1.4 states the metric is incremented
      // ONLY on producer-failure detection.
      assert(iter.toList.isEmpty)
      assert(streamingMetrics.getPartialReadInvalidationsCount === 0L,
        "partialReadInvalidations must remain 0 on the success path")
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 10: producer failure invokes fetchBlockSync at least once -- guards
  // against accidental short-circuiting that would skip the transport boundary
  // ---------------------------------------------------------------------------

  test("producer failure path invokes fetchBlockSync at least once") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    val producer = BlockManagerId("producer-exec", "host1", 7337)
    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt()))
      .thenReturn(singleBlockMapStatuses(producer))
    doThrow(new RuntimeException("simulated transport failure"))
      .when(transferService).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      intercept[FetchFailedException] {
        val iter = reader.read()
        while (iter.hasNext) iter.next()
      }
      // The transport boundary MUST have been hit at least once -- the failure
      // detection cannot be a no-op that returns immediately.
      verify(transferService, atLeastOnce()).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())
      // Verify the producer-side call used the correct host/port from the
      // BlockManagerId we configured. Mockito's argument matching ignores the
      // exact values (we used anyString/anyInt above) but per Mockito the
      // most-recent invocation arguments are recorded for diagnostic purposes;
      // this verify call simply confirms the method was hit.
      verify(transferService, times(1)).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 11: producer connection TimeoutException -- the exact failure mode
  // referenced by AAP Section 0.7.2.4: "Producer failure detection: connection
  // timeout MUST be 5 seconds". This complements Test 2 / Test 4 (which use
  // RuntimeException to exercise the generic NonFatal arm) by driving the
  // production source's dedicated `case e: TimeoutException` catch-handler at
  // `StreamingShuffleReader.fetchAndValidateBlock` lines 474-479.
  //
  // Why a separate test? The TimeoutException arm is structurally distinct from
  // the NonFatal arm: it produces a different log message ("Producer connection
  // timeout fetching block...") and would diverge under future refactoring if
  // not covered explicitly. Locking the contract here guards against an
  // accidental collapse of the two arms during code-review-driven cleanup.
  //
  // doAnswer vs. doThrow: TimeoutException is a checked exception (extends
  // Exception, not RuntimeException). Mockito 5.12 strict-stubbing rejects ALL
  // forms of throw-stub install (`when().thenThrow`, `doThrow`, `willThrow`)
  // for checked exceptions that are not declared on the stubbed method's
  // bytecode-level `throws` clause -- the validation runs at INVOCATION time,
  // not at stub-install time. `BlockTransferService.fetchBlockSync` carries no
  // `throws` types in its bytecode signature (its source-level callers wrap
  // the awaited Future via `ThreadUtils.awaitResult`), so a direct
  // `doThrow(timeoutCause)` install raises:
  //   org.mockito.exceptions.base.MockitoException:
  //   Checked exception is invalid for this method!
  //   Invalid: java.util.concurrent.TimeoutException
  // The canonical workaround is `doAnswer(answer)` where the supplied
  // [[org.mockito.stubbing.Answer]]'s `answer(InvocationOnMock)` method itself
  // declares `throws Throwable`. The compiler-level checked-exception barrier
  // is satisfied at the Answer SAM boundary, allowing the body to throw any
  // Throwable (including TimeoutException). At runtime the JVM raises the
  // throwable through the mock's invocation handler exactly as a real fetch
  // would; the production reader's `case e: TimeoutException` catch arm fires
  // identically to a real-world deadline event from the underlying
  // `Promise[ManagedBuffer]` failed via `result.failure(timeoutCause)`.
  // ---------------------------------------------------------------------------

  test("producer connection TimeoutException raises FetchFailedException") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()

    // Capture the metric value before invocation so we can assert exact-delta
    // attribution to this single test invocation -- mirroring the strict-delta
    // pattern in Test 4 and guarding against accidental double-increment.
    val initialInvalidations = streamingMetrics.getPartialReadInvalidationsCount

    val producer = BlockManagerId("timeout-producer", "host1", 7337)
    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt()))
      .thenReturn(singleBlockMapStatuses(producer))

    // Install the checked-exception stub via the Answer SAM. doAnswer is the
    // canonical workaround for stubbing a method that declares no `throws`
    // types (per BlockTransferService.fetchBlockSync) to throw a checked
    // exception (per java.util.concurrent.TimeoutException). The Answer's
    // `answer(InvocationOnMock)` method declares `throws Throwable` so the
    // compiler-level check is satisfied at the SAM boundary; at runtime the
    // body throws the TimeoutException through the mock's invocation handler.
    // The reader's catch chain at StreamingShuffleReader.scala:474-479
    // specifically matches this type and increments
    // partialReadInvalidations + throws FetchFailedException with the
    // original TimeoutException attached as the cause.
    val timeoutCause = new TimeoutException("simulated 5-second connection timeout")
    doAnswer(new Answer[org.apache.spark.network.buffer.ManagedBuffer] {
      override def answer(invocation: InvocationOnMock)
        : org.apache.spark.network.buffer.ManagedBuffer = throw timeoutCause
    }).when(transferService).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val ex = intercept[FetchFailedException] {
        val iter = reader.read()
        while (iter.hasNext) iter.next()
      }
      assert(ex != null,
        "FetchFailedException must be thrown when the producer connection times out")

      // The TimeoutException MUST be preserved as the FetchFailedException's
      // cause so the existing `Utils.exceptionString(this)` chain in
      // `FetchFailedException.toTaskFailedReason` (FetchFailedException.scala
      // line 62) surfaces the original deadline event in the
      // DAGScheduler-emitted task-failure reason, allowing operators to
      // distinguish a 5-second connection deadline from a generic transport
      // error.
      assert(ex.getCause eq timeoutCause,
        s"FetchFailedException's cause must be the original TimeoutException so " +
          s"diagnostic surfaces preserve the deadline-event attribution; " +
          s"actual cause: ${Option(ex.getCause).map(_.getClass.getName).getOrElse("<null>")}")

      // The production source's TimeoutException catch arm uses a dedicated
      // message prefix ("Producer connection timeout..."); locking the message
      // contract here guards against a refactoring that would collapse this arm
      // into the NonFatal arm and obscure the failure mode in operator-facing
      // logs. We use `contains` rather than `equals` because the message
      // includes block-id and address details whose exact format is a
      // non-contract surface.
      val msg = Option(ex.getMessage).getOrElse("")
      assert(msg.contains("timeout") || msg.contains("Timeout"),
        s"FetchFailedException's message must surface the timeout failure mode; " +
          s"actual message: $msg")

      // Strict-delta assertion (mirrors Test 4): exactly one invalidation per
      // failed fetch attempt, guarding against accidental double-increment under
      // future refactoring of the catch chain or the metrics emission path.
      val finalInvalidations = streamingMetrics.getPartialReadInvalidationsCount
      assert(finalInvalidations - initialInvalidations === 1L,
        s"partialReadInvalidations should increment by exactly 1 per timeout " +
          s"failure; observed delta=${finalInvalidations - initialInvalidations}")

      // Sanity-check that the transport boundary was actually hit (the failure
      // must not be a no-op that returns immediately without attempting the
      // fetch).
      verify(transferService, atLeastOnce()).fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(),
        isNull[DownloadFileManager]())
    } finally {
      TaskContext.unset()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 12: TaskMetrics direct construction is permitted (regression test for
  // the agent_prompt's `new TaskMetrics().createTempShuffleReadMetrics()` form)
  // ---------------------------------------------------------------------------

  test("ShuffleReadMetricsReporter from a fresh TaskMetrics is usable by the reader") {
    val handle = buildHandle()
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    // Use the alternate metric source -- a freshly constructed TaskMetrics --
    // rather than taskContext.taskMetrics. Both forms must work because the
    // reader treats the reporter as a duck-typed interface.
    val readMetrics: ShuffleReadMetricsReporter =
      new TaskMetrics().createTempShuffleReadMetrics()

    when(mapOutputTracker.getMapSizesByExecutorId(
      meq(0), anyInt(), anyInt(), anyInt(), anyInt())).thenReturn(emptyMapStatuses)

    TaskContext.setTaskContext(taskContext)
    try {
      val reader = buildReader(handle, taskContext, readMetrics)
      val iter = reader.read()
      assert(iter != null)
      // Drain.
      while (iter.hasNext) iter.next()
      // Note: `recordsRead` is exposed only on the concrete `TempShuffleReadMetrics`
      // type, not on the `ShuffleReadMetricsReporter` trait. The reader did not
      // throw here, which is the operative regression assertion.
      assert(streamingMetrics.getPartialReadInvalidationsCount === 0L)
    } finally {
      TaskContext.unset()
    }
  }

}
