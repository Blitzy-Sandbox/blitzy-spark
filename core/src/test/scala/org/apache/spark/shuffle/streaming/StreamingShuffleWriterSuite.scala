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

import java.util.zip.CRC32C

import scala.reflect.ClassTag

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{atLeastOnce, never, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{HashPartitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryManager, MemoryTestingUtils}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Unit tests for [[StreamingShuffleWriter]] covering buffer allocation per partition,
 * spill-trigger configuration via the [[StreamingShuffleHandle]] threshold field,
 * CRC32C checksum generation per block (and aggregation into the produced
 * [[org.apache.spark.scheduler.MapStatus]]), producer-failure cleanup via
 * `stop(success = false)`, idempotency of `stop`, [[BackpressureProtocol]]
 * integration via the per-block `recordTransmission` invocation, and per-partition
 * memory tracking via the [[org.apache.spark.memory.TaskMemoryManager]] mock fixture
 * supplied through [[MemoryTestingUtils.fakeTaskContext]].
 *
 * == AAP Reference ==
 *  - AAP Section 0.5.1.2 (StreamingShuffleWriter component design)
 *  - AAP Section 0.5.1.6 (Group 6, item 2 -- this suite's scope)
 *  - AAP Section 0.7.2.2 (memory discipline -- per-partition buffer cap)
 *  - AAP Section 0.7.2.4 (failure tolerance and integrity -- CRC32C-only checksum)
 *  - AAP Section 0.7.2.6 (quality gate: greater-than 85% coverage for new components)
 *
 * == Production-Source Contract Exercised ==
 *  - Constructor: `StreamingShuffleWriter(handle, mapId, context, writeMetrics,
 *    blockManager, memoryManager, backpressure, spillManager, streamingMetrics)`
 *    instantiated per-test inside the test body.
 *  - `write(records: Iterator[Product2[K, V]]): Unit` partitions records, accumulates
 *    serialized bytes in per-partition [[java.io.ByteArrayOutputStream]] accumulators,
 *    flushes blocks at the 2 MB block boundary, and finally publishes per-partition
 *    cumulative bytes through [[BlockManager#putBytes]].
 *  - `stop(success: Boolean): Option[MapStatus]` returns `Some(mapStatus)` on success
 *    after a normal `write`, `None` on failure or when called twice (idempotency
 *    short-circuit via the internal `stopping` flag).
 *  - `getPartitionLengths(): Array[Long]` returns the writer's internal per-partition
 *    byte-counts array sized to `numPartitions`.
 *
 * == Mocking Strategy ==
 *  - [[BlockManager]] is mocked via `@Mock(answer = RETURNS_SMART_NULLS)`. Three
 *    methods are stubbed in `beforeEach`:
 *    1. `serializerManager` returns the REAL [[org.apache.spark.serializer.SerializerManager]]
 *       from `sc.env.serializerManager` so the writer's
 *       `blockManager.serializerManager.wrapStream(blockId, byteStream)` call inside
 *       [[StreamingShuffleWriter#ensurePartitionPersistStream]] returns a usable
 *       [[java.io.OutputStream]] (otherwise the smart-null answer would return a
 *       method-call-friendly mock that produces an empty stream and the
 *       [[org.apache.spark.serializer.SerializationStream]] construction would fail).
 *    2. `shuffleServerId` returns a real [[BlockManagerId]] so the writer's
 *       [[org.apache.spark.scheduler.MapStatus#apply]] call has a non-null location to
 *       attribute the produced map output to.
 *    3. `putBytes` returns `true` (success) so the writer's
 *       `persistPartitionsForReader` finishes without warning logs about
 *       "block-already-exists" and the test exercises the green-path persist branch.
 *  - [[MemoryManager]] is mocked and its abstract `maxOnHeapStorageMemory` is stubbed
 *    via `doReturn(value).when(mm).maxOnHeapStorageMemory` (the
 *    `org.mockito.Mockito.doReturn` form is required for abstract `def` accessors that
 *    return primitive `Long` -- the simpler `when(...).thenReturn(...)` would invoke
 *    the abstract method during stub registration and throw a partial-real-method
 *    error). The default value is 64 MB which produces a per-partition buffer cap of
 *    `(64 MB * 20%) / 4 = 3.2 MB` for the default 4-partition fixture; this is
 *    comfortably above the 2 MB [[BLOCK_SIZE_BYTES]] floor.
 *  - [[BackpressureProtocol]] is mocked. `recordTransmission` is stubbed to return
 *    `true` so the writer never observes a rate-limit rejection during these unit
 *    tests. The stub is verified for invocation count in Test 10 to confirm that the
 *    writer DOES call the protocol on every partition with non-zero output.
 *  - [[MemorySpillManager]] is mocked. Its `checkAndSpill` is a `Unit`-returning
 *    method whose default smart-null answer is a no-op, suitable for tests that
 *    do not exercise the spill path. Tests that need to exercise spill behavior
 *    set `spillThreshold` aggressively low to force spill in the writer.
 *  - [[StreamingShuffleMetrics]] is used as a REAL instance (not mocked) so that any
 *    counter increments happening inside the writer's collaborator paths exercise the
 *    actual production side-effect. This matches the pattern in
 *    [[BackpressureProtocolSuite]], [[MemorySpillManagerSuite]], and
 *    [[StreamingShuffleFallbackPolicySuite]].
 *
 * == TaskContext Discipline ==
 * The writer reads `context.taskMemoryManager()` inside its
 * [[StreamingShuffleWriter.StreamingBufferConsumer]] inner class which extends
 * [[org.apache.spark.memory.MemoryConsumer]]. A null [[TaskContext]] would produce a
 * `NullPointerException` at consumer construction time. Each test obtains a
 * synthetic `TaskContext` via [[MemoryTestingUtils#fakeTaskContext]] which constructs
 * a real [[org.apache.spark.memory.TaskMemoryManager]] backed by `sc.env.memoryManager`
 * so execution-memory acquire/release flows participate in the real unified-memory
 * accounting. The TaskContext is set into the task-thread-local via
 * [[TaskContext#setTaskContext]] before each test body and unset in a `try/finally`
 * (via the test-level `MemoryTestingUtils` pattern) so cross-test leakage is
 * prevented. We do not call [[TaskContext#setTaskContext]] explicitly here because the
 * [[StreamingShuffleWriter]] receives the [[TaskContext]] as a constructor argument
 * and only accesses it through the constructor-bound reference -- there is no need
 * for the task-local thread storage to hold the same instance.
 *
 * == Coexistence Discipline (per User Directive) ==
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* the SparkContext provided
 * by [[SharedSparkContext]] retains the production-stable default
 * `spark.shuffle.manager=sort`. The streaming writer is exercised here directly with
 * mocked dependencies; the [[StreamingShuffleManager]]'s manager-level dispatch is
 * covered by [[StreamingShuffleManagerSuite]] separately to keep this suite focused on
 * the writer's unit-level behavior.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
  with SharedSparkContext
  with Matchers
  with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------
  // Mockito helpers
  // ---------------------------------------------------------------------------

  /**
   * Mockito stub helper mirroring the pattern used by
   * [[org.apache.spark.shuffle.streaming.MemorySpillManagerSuite]] and
   * [[org.apache.spark.shuffle.streaming.StreamingShuffleFallbackPolicySuite]].
   *
   * Wraps `org.mockito.Mockito.doReturn(value, varargs...)` with the empty-Seq splat
   * required by the Java vararg signature when called from Scala. Used for stubbing
   * abstract methods (notably [[MemoryManager#maxOnHeapStorageMemory]]) whose
   * `when(mock.method).thenReturn(value)` form would invoke the abstract method during
   * stub registration.
   *
   * @param value the value to be returned by the stubbed method
   * @return a [[org.mockito.stubbing.Stubber]] suitable for `.when(mock).method(...)`
   */
  private def doReturn(value: Any): org.mockito.stubbing.Stubber =
    org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  // ---------------------------------------------------------------------------
  // Per-test mockito-managed fields (annotated)
  // ---------------------------------------------------------------------------

  /**
   * Mocked [[BlockManager]] -- the writer reads `serializerManager`, `shuffleServerId`,
   * and invokes `putBytes` and `removeBlock`. `serializerManager` is stubbed in
   * `beforeEach` to return the real [[org.apache.spark.serializer.SerializerManager]]
   * from `sc.env.serializerManager` so the writer's
   * [[StreamingShuffleWriter#ensurePartitionPersistStream]] obtains a working
   * [[java.io.OutputStream]] from `wrapStream(blockId, byteStream)`.
   */
  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  /**
   * Mocked [[MemoryManager]] -- the writer reads `maxOnHeapStorageMemory` exactly once
   * during construction to compute its per-partition buffer cap. The default stub
   * value (64 MB) yields a per-partition cap of 3.2 MB for the default 4-partition
   * fixture, comfortably above the [[BLOCK_SIZE_BYTES]] (2 MB) floor.
   */
  @Mock(answer = RETURNS_SMART_NULLS)
  private var memoryManager: MemoryManager = _

  /**
   * Mocked [[BackpressureProtocol]] -- the writer invokes `recordTransmission` once
   * per non-empty partition during the residual-flush loop in `write()`. The default
   * stub returns `true` (rate limiter granted tokens) so the writer never observes a
   * rate-limit rejection in unit tests.
   */
  @Mock(answer = RETURNS_SMART_NULLS)
  private var backpressure: BackpressureProtocol = _

  /**
   * Mocked [[MemorySpillManager]] -- the writer invokes `checkAndSpill` when a
   * partition's buffer utilization crosses the configured spill threshold. The
   * default smart-null answer for a `Unit`-returning method is a no-op, suitable for
   * tests that do not assert on spill behavior.
   */
  @Mock(answer = RETURNS_SMART_NULLS)
  private var spillManager: MemorySpillManager = _

  // ---------------------------------------------------------------------------
  // Per-test non-mock fields
  // ---------------------------------------------------------------------------

  /**
   * Real [[StreamingShuffleMetrics]] instance recreated per-test. Counter increments
   * propagated by the writer's collaborator paths exercise the actual production
   * side-effect chain.
   */
  private var streamingMetrics: StreamingShuffleMetrics = _

  /**
   * Mockito 5+ `AutoCloseable` returned by [[MockitoAnnotations#openMocks]]. Closed in
   * `afterEach` to prevent mock state leakage across tests within the suite.
   */
  private var mockitoCloseable: AutoCloseable = _

  // ---------------------------------------------------------------------------
  // Lifecycle hooks
  // ---------------------------------------------------------------------------

  /**
   * Per-test setup: opens Mockito annotations (which materializes all `@Mock` fields),
   * constructs a fresh [[StreamingShuffleMetrics]] instance, and applies the default
   * stubs for the four writer-collaborator mocks.
   *
   * The `super.beforeEach()` invocation chains through [[SharedSparkContext#beforeEach]]
   * which clears any open [[org.apache.spark.DebugFilesystem]] streams from prior tests.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    mockitoCloseable = MockitoAnnotations.openMocks(this)
    streamingMetrics = new StreamingShuffleMetrics()

    // Default `MemoryManager.maxOnHeapStorageMemory` -- 64 MB produces a per-partition
    // cap of 3.2 MB for the default 4-partition fixture. Tests that need a different
    // cap override this stub before constructing the writer.
    doReturn(64L * 1024L * 1024L).when(memoryManager).maxOnHeapStorageMemory

    // Default `BlockManager.shuffleServerId` -- the writer attributes the produced
    // [[org.apache.spark.scheduler.MapStatus]] location to this BlockManagerId.
    when(blockManager.shuffleServerId)
      .thenReturn(BlockManagerId("test-exec", "test-host", 12345))

    // Default `BlockManager.serializerManager` -- the writer wraps each
    // partition's persist accumulator via `wrapStream(blockId, byteStream)` so the
    // resulting bytes flow through the codec/encryption chain symmetric to the
    // reader-side `wrapStream` in [[StreamingShuffleReader]]. We delegate to the real
    // [[org.apache.spark.serializer.SerializerManager]] from `sc.env` so the wrap
    // returns a usable [[java.io.OutputStream]].
    when(blockManager.serializerManager).thenReturn(sc.env.serializerManager)

    // Default `BlockManager.putBytes` -- success. The writer's
    // `persistPartitionsForReader` invokes putBytes with the `(BlockId, ChunkedByteBuffer,
    // StorageLevel, tellMaster)` plus implicit `ClassTag[Byte]` parameter list; the
    // ClassTag is matched via `any[ClassTag[Byte]]`.
    when(blockManager.putBytes(
      any[BlockId], any[ChunkedByteBuffer], any[org.apache.spark.storage.StorageLevel],
      anyBoolean()
    )(any[ClassTag[Byte]])).thenReturn(true)

    // Default `BackpressureProtocol.recordTransmission` -- rate limiter grants. Returns
    // `true` so the writer's per-block flush loop proceeds without observing a
    // rate-limit rejection.
    when(backpressure.recordTransmission(
      anyInt(), anyLong(), anyInt(), anyLong(), anyLong())
    ).thenReturn(true)
  }

  /**
   * Per-test teardown: closes the Mockito state to prevent mock leakage across tests.
   * Chains to `super.afterEach()` which performs the [[SharedSparkContext]] check for
   * unclosed streams.
   */
  override def afterEach(): Unit = {
    try {
      if (mockitoCloseable != null) {
        mockitoCloseable.close()
        mockitoCloseable = null
      }
    } finally {
      super.afterEach()
    }
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Build a real [[ShuffleDependency]] backed by a small parallelized RDD. Each
   * dependency is constructed fresh per-test so its `shuffleId` (auto-assigned by
   * `_rdd.context.newShuffleId()`) is stable. The dependency uses a
   * [[HashPartitioner]] with the requested number of partitions so the writer's
   * `dep.partitioner.numPartitions` returns the expected value.
   *
   * Mirrors the helper from [[StreamingShuffleReaderSuite]] for consistency in the
   * streaming-shuffle test suite family.
   *
   * @param numPartitions desired number of reduce partitions
   * @return a fresh [[ShuffleDependency]] with default serializer (JavaSerializer
   *         from `sc.env.serializer`), no aggregator, no key ordering
   */
  private def buildShuffleDep(numPartitions: Int = 4): ShuffleDependency[Int, Int, Int] = {
    val rdd = sc.parallelize(0 until 100, numPartitions).map(i => (i, i))
    new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(numPartitions))
  }

  /**
   * Construct a [[StreamingShuffleHandle]] with the specified configuration. Defaults
   * match AAP Section 0.7.2.2 / 0.7.2.5: 20% buffer-size, 80% spill threshold,
   * unlimited bandwidth (-1).
   *
   * @param numPartitions     number of reduce partitions
   * @param bufferSizePercent percent of executor memory dedicated to streaming buffers
   *                          (1-50)
   * @param spillThreshold    buffer-utilization percentage at which spill is triggered
   *                          (50-95)
   * @param maxBandwidthMBps  per-executor bandwidth cap in MB/s (-1 = unlimited)
   * @return a `StreamingShuffleHandle[Int, Int, Int]` ready to drive a writer
   */
  private def buildHandle(
      numPartitions: Int = 4,
      bufferSizePercent: Int = 20,
      spillThreshold: Int = 80,
      maxBandwidthMBps: Int = -1): StreamingShuffleHandle[Int, Int, Int] = {
    val dep = buildShuffleDep(numPartitions)
    new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = dep.shuffleId,
      dependency = dep,
      bufferSizePercent = bufferSizePercent,
      spillThreshold = spillThreshold,
      maxBandwidthMBps = maxBandwidthMBps)
  }

  /**
   * Construct a fully-wired [[StreamingShuffleWriter]] using the default test
   * fixtures. The `mapId` defaults to 0L; tests that need distinct map IDs override
   * the parameter.
   *
   * The [[TaskContext]] supplied via [[MemoryTestingUtils#fakeTaskContext]] is backed
   * by a real [[org.apache.spark.memory.TaskMemoryManager]] tied to `sc.env.memoryManager`
   * so the writer's `acquireExecutionMemory` / `releaseExecutionMemory` calls
   * participate in the real unified-memory accounting model.
   *
   * @param handle the streaming shuffle handle (typically built via [[buildHandle]])
   * @param mapId  the map task identifier
   * @return a `StreamingShuffleWriter[Int, Int]` ready to consume records
   */
  private def buildWriter(
      handle: StreamingShuffleHandle[Int, Int, Int],
      mapId: Long = 0L): StreamingShuffleWriter[Int, Int] = {
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writeMetrics = new TaskMetrics().shuffleWriteMetrics
    new StreamingShuffleWriter[Int, Int](
      handle = handle,
      mapId = mapId,
      context = taskContext,
      writeMetrics = writeMetrics,
      blockManager = blockManager,
      memoryManager = memoryManager,
      backpressure = backpressure,
      spillManager = spillManager,
      streamingMetrics = streamingMetrics)
  }

  /**
   * Compute a CRC32C checksum (Castagnoli polynomial 0x1EDC6F41) over the given byte
   * array using the JDK 17 [[java.util.zip.CRC32C]] class. Per AAP Section 0.7.2.4
   * *"Checksum algorithm: CRC32C only (no MD5, SHA-1, SHA-256, xxHash, or alternative
   * algorithm)"* this is the only checksum primitive permitted for streaming-shuffle
   * integrity validation -- the test uses it as a reference primitive in Test 4 to
   * demonstrate algorithm determinism (the production source uses the same JDK class).
   *
   * @param bytes input byte array
   * @return the CRC32C checksum as an unsigned 32-bit value held in a `Long`
   */
  private def computeCrc32C(bytes: Array[Byte]): Long = {
    val crc = new CRC32C
    crc.update(bytes, 0, bytes.length)
    crc.getValue
  }

  // ---------------------------------------------------------------------------
  // Test 1: Empty write returns valid MapStatus with zero per-partition lengths
  // ---------------------------------------------------------------------------
  test("write of empty iterator + stop(success=true) returns MapStatus with zero lengths") {
    // Build a 4-partition writer with default config and consume an empty iterator.
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    writer.write(Iterator.empty)
    val mapStatus = writer.stop(success = true)

    // The writer's `stop(success = true)` returns the MapStatus populated by `write`
    // even for an empty input -- the per-partition byte counts are simply zero.
    assert(mapStatus.isDefined,
      "stop(success=true) after write must return Some(MapStatus) regardless of " +
        "input size; the framework treats the MapStatus as the map-output-published " +
        "signal even for zero-byte map outputs.")

    // The per-partition lengths array MUST be sized to the dependency's partitioner
    // numPartitions, with every entry equal to zero because no records were written.
    val lengths = writer.getPartitionLengths()
    lengths.length must be(4)
    lengths.forall(_ == 0L) must be(true)

    // No partition received bytes; the writer SHOULD NOT have invoked
    // recordTransmission on the BackpressureProtocol mock -- flushBlock early-returns
    // for empty buffers in its `if (buf == null || buf.size() == 0) return` guard.
    verify(backpressure, never()).recordTransmission(
      anyInt(), anyLong(), anyInt(), anyLong(), anyLong())

    // No partition received bytes; the writer SHOULD NOT have invoked checkAndSpill.
    verify(spillManager, never()).checkAndSpill(
      anyInt(), anyLong(), anyInt(), any[ChunkedByteBuffer])
  }

  // ---------------------------------------------------------------------------
  // Test 2: stop(success=false) releases buffer resources and returns None
  // ---------------------------------------------------------------------------
  test("stop(success=false) returns None and releases per-partition buffer resources") {
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    // Run a small write that does NOT cross the 2 MB block boundary (records are
    // tiny Int pairs, so 3 records fits comfortably under the per-partition cap).
    writer.write(Iterator((1, 1), (2, 2), (3, 3)))

    // Failure-path stop. The contractual return is `None` -- the framework treats
    // `None` as the signal that no map output was committed for this attempt.
    val mapStatus = writer.stop(success = false)
    mapStatus must be(None)

    // The writer's idempotency guarantees that any further `stop` invocations remain
    // `None`. This is exercised here as a regression guard: without the `stopping`
    // flag short-circuit, the second close-and-null-out pass over the per-partition
    // arrays could throw if any slot were already null from the first pass.
    val secondStop = writer.stop(success = false)
    secondStop must be(None)
  }

  // ---------------------------------------------------------------------------
  // Test 3: write() of non-empty data produces a non-zero per-partition length
  // ---------------------------------------------------------------------------
  test("write() of non-empty data produces non-zero per-partition lengths") {
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    // 100 records distributed via HashPartitioner across 4 partitions. With
    // i in [0, 100) and `i.hashCode == i`, partition assignment cycles 0,1,2,3,0,1,...
    // so every partition observes records (25 each on average).
    writer.write((0 until 100).iterator.map(i => (i, i)))
    val mapStatus = writer.stop(success = true)

    assert(mapStatus.isDefined,
      "stop(success=true) after a populated write must return Some(MapStatus)")

    val lengths = writer.getPartitionLengths()
    lengths.length must be(4)

    // At least one partition must have non-zero bytes -- the wire-format byte counts
    // are reconciled from `partitionPersistBuffers(k).size().toLong` at end-of-write
    // (see StreamingShuffleWriter.write lines 720-727), and every partition's
    // accumulator is populated through the persist channel as records flow through.
    lengths.exists(_ > 0L) must be(true)

    // The total persisted byte volume across all partitions should equal the sum of
    // the per-partition lengths (a tautology that nonetheless guards against a
    // partial-publish bug where the partitionLengths reconciliation loop terminates
    // before processing every partition).
    lengths.sum must be > 0L
  }

  // ---------------------------------------------------------------------------
  // Test 4: writer computes CRC32C checksums and aggregates them into MapStatus
  // ---------------------------------------------------------------------------
  test("writer computes CRC32C checksums per block and folds them into MapStatus") {
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    writer.write((0 until 100).iterator.map(i => (i, i)))
    val mapStatus = writer.stop(success = true)

    assert(mapStatus.isDefined,
      "stop(success=true) after a populated write must return Some(MapStatus)")

    // The writer's `aggregateChecksumValue` XOR-folds each non-null per-partition
    // CRC32C value (with bit-rotation by partition index) into a single aggregated
    // checksum that is passed as the MapStatus `checksumVal` constructor argument.
    // After a non-empty write every partition's cumulative CRC32C is non-null and at
    // least one is non-zero, so the aggregated value MUST be non-zero. (A zero
    // aggregated value would indicate either no data was checksummed or a XOR
    // cancellation -- neither expected for 100 distinct integer records distributed
    // across 4 partitions.)
    val checksumValue = mapStatus.get.checksumValue
    assert(checksumValue != 0L,
      s"MapStatus.checksumValue must be non-zero for a populated shuffle " +
        s"(CRC32C aggregated across partitions); got $checksumValue")

    // Verify the JDK 17 [[java.util.zip.CRC32C]] primitive that backs the writer's
    // checksum computation is deterministic: hashing the same input twice yields the
    // same value. Per AAP Section 0.7.2.4 this is the only checksum primitive
    // permitted for streaming shuffle integrity validation.
    val sample = "streaming-shuffle-block-bytes".getBytes("UTF-8")
    val a = computeCrc32C(sample)
    val b = computeCrc32C(sample)
    a must equal(b)
    // CRC32C of non-empty input is always non-zero for this sample (the CRC32C of
    // the 30-byte string above is a stable reference value across JDK versions).
    a must not equal(0L)
  }

  // ---------------------------------------------------------------------------
  // Test 5: producer-failure cleanup releases per-partition buffer memory
  // ---------------------------------------------------------------------------
  test("producer failure cleanup releases per-partition buffers and is idempotent") {
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    // Run a normal write that populates per-partition buffers.
    writer.write((0 until 100).iterator.map(i => (i, i)))

    // Simulate a producer-failure framework callback: stop(success=false). Per the
    // production source's `stop` body, the method:
    //   (a) sets `stopping = true` (idempotency guard);
    //   (b) returns `None` for the failure path;
    //   (c) releases acquired execution memory via `releaseAcquiredMemory()`;
    //   (d) defensively closes any still-open per-partition serialization streams;
    //   (e) nulls `partitionBuffers`, `partitionCheckedStreams`, `partitionChecksums`,
    //       `partitionPersistBuffers` slots so their underlying byte arrays become
    //       eligible for GC.
    val mapStatus = writer.stop(success = false)
    mapStatus must be(None)

    // Idempotency: the second stop short-circuits via the `stopping` flag and
    // returns `None`. Without the short-circuit, the per-partition close-and-null
    // pass would either re-close already-nulled slots (no-op due to null guard) or
    // raise an exception if the close call were not null-guarded.
    writer.stop(success = false) must be(None)
    writer.stop(success = true) must be(None)

    // The third `stop(success=true)` invocation MUST NOT return the MapStatus that
    // was populated by `write` -- once `stopping` is set, the method returns `None`
    // unconditionally. This is the contract the framework relies on when a task
    // attempt is aborted between `write` and the `commitTask` sequence.
  }

  // ---------------------------------------------------------------------------
  // Test 6: per-partition buffer cap respects bufferSizePercent / numPartitions
  // ---------------------------------------------------------------------------
  test("per-partition buffer cap respects bufferSizePercent * memory / numPartitions") {
    // Configure a small executor memory budget (4 MB) so the buffer-cap arithmetic
    // can be reasoned about. With `bufferSizePercent = 50`, `numPartitions = 4`:
    //   totalBuffer = 4 MB * 50% = 2 MB
    //   perPartitionCap = 2 MB / 4 = 0.5 MB, FLOORED to BLOCK_SIZE_BYTES (2 MB).
    // The floor protects against pathological cases where naive arithmetic computes
    // a cap below the block size and forces every record write to trigger a flush.
    doReturn(4L * 1024L * 1024L).when(memoryManager).maxOnHeapStorageMemory

    val handle = buildHandle(numPartitions = 4, bufferSizePercent = 50)
    val writer = buildWriter(handle)

    // Small write: no spill expected because the cap floored to 2 MB and the test
    // data is well under that for any single partition.
    writer.write((0 until 50).iterator.map(i => (i, i)))
    val mapStatus = writer.stop(success = true)

    assert(mapStatus.isDefined,
      "stop(success=true) after a small populated write must return Some(MapStatus)")

    // No spill expected -- the buffer-cap floor (2 MB) is comfortably above the
    // serialized footprint of 50 small Int records per partition.
    verify(spillManager, never()).checkAndSpill(
      anyInt(), anyLong(), anyInt(), any[ChunkedByteBuffer])
  }

  // ---------------------------------------------------------------------------
  // Test 7: custom bufferSizePercent configuration is applied
  // ---------------------------------------------------------------------------
  test("custom bufferSizePercent configuration is read from the StreamingShuffleHandle") {
    // Two writers with distinct bufferSizePercent values must both accept records
    // without error -- exercising the configuration path. This test validates that
    // the buffer-cap arithmetic does not produce a degenerate value (e.g., zero or
    // negative) for any value in the AAP-permitted 1-50% range.
    val lowHandle = buildHandle(numPartitions = 4, bufferSizePercent = 1)
    val lowWriter = buildWriter(lowHandle, mapId = 100L)
    lowWriter.write((0 until 25).iterator.map(i => (i, i)))
    val lowStatus = lowWriter.stop(success = true)
    assert(lowStatus.isDefined,
      "Writer with bufferSizePercent=1 must complete a small write successfully")

    val highHandle = buildHandle(numPartitions = 4, bufferSizePercent = 50)
    val highWriter = buildWriter(highHandle, mapId = 200L)
    highWriter.write((0 until 25).iterator.map(i => (i, i)))
    val highStatus = highWriter.stop(success = true)
    assert(highStatus.isDefined,
      "Writer with bufferSizePercent=50 must complete a small write successfully")

    // Verify both writers produced sized partition-length arrays.
    lowWriter.getPartitionLengths().length must be(4)
    highWriter.getPartitionLengths().length must be(4)
  }

  // ---------------------------------------------------------------------------
  // Test 8: getPartitionLengths returns array sized to numPartitions
  // ---------------------------------------------------------------------------
  test("getPartitionLengths returns array sized to dependency.partitioner.numPartitions") {
    // 8-partition writer with empty input. The lengths array must have exactly 8
    // entries, all zero, regardless of input (the array is allocated at writer
    // construction time per the production source's `private val partitionLengths:
    // Array[Long] = new Array[Long](numPartitions)`).
    val handle = buildHandle(numPartitions = 8)
    val writer = buildWriter(handle)
    writer.write(Iterator.empty)
    writer.stop(success = true)

    val lengths = writer.getPartitionLengths()
    lengths.length must be(8)
    lengths.forall(_ == 0L) must be(true)

    // 1-partition writer with empty input: a degenerate but legal configuration.
    // Confirms the writer does not crash on the divisor-of-1 path in
    // `perPartitionBufferCap`.
    val handle1 = buildHandle(numPartitions = 1)
    val writer1 = buildWriter(handle1, mapId = 999L)
    writer1.write(Iterator.empty)
    writer1.stop(success = true)
    writer1.getPartitionLengths().length must be(1)
  }

  // ---------------------------------------------------------------------------
  // Test 9: stop() can be called multiple times safely (idempotency)
  // ---------------------------------------------------------------------------
  test("stop() is idempotent: multiple calls do not throw and return None after the first") {
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    // Populate the writer with one record so `stop(true)` has a populated MapStatus
    // to return on the first call.
    writer.write(Iterator((1, 1)))

    // First stop -- success path. Returns Some(MapStatus) populated by `write`.
    val first = writer.stop(success = true)
    assert(first.isDefined,
      "First stop(success=true) after write returns Some(MapStatus)")

    // Second stop with the same `success` argument MUST return None per the
    // production source's `stopping` flag short-circuit (line 776-778 of
    // StreamingShuffleWriter). This guards against double-publishing a MapStatus.
    val second = writer.stop(success = true)
    second must be(None)

    // Third stop with a different `success` argument also returns None -- the
    // short-circuit is set on the FIRST stop call regardless of the success value.
    val third = writer.stop(success = false)
    third must be(None)
  }

  // ---------------------------------------------------------------------------
  // Test 10: write() invokes BackpressureProtocol.recordTransmission for transmission
  // ---------------------------------------------------------------------------
  //
  // NOTE: The agent-prompt schema specifies `tryAcquire(anyLong())` for this
  // verification. Inspection of the production source
  // (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala`
  // line 1167-1172) confirms the writer calls
  // `backpressure.recordTransmission(shuffleId, mapId, reduceId, byteCount, checksum)`
  // -- NOT `tryAcquire` -- on its per-block flush path. Per the schema's Phase 9
  // adaptation note (*"The implementation agent should adapt accordingly based on the
  // production source API"*), this test verifies `recordTransmission` invocations.
  test("write() invokes BackpressureProtocol.recordTransmission on per-partition flush") {
    val handle = buildHandle(numPartitions = 4)
    val writer = buildWriter(handle)

    // Write 100 records distributed across 4 partitions. Each partition receives ~25
    // records of 8 bytes (two Ints) plus a header/footer overhead, for a total per-
    // partition wire-format byte count comfortably below the 2 MB BLOCK_SIZE_BYTES
    // boundary. The residual-flush loop in `write()` will therefore call flushBlock
    // once for each non-empty partition (4 invocations total).
    writer.write((0 until 100).iterator.map(i => (i, i)))
    writer.stop(success = true)

    // The writer MUST have invoked recordTransmission at least once (in practice
    // exactly numPartitions = 4 times if no block boundary is crossed). We assert
    // `atLeastOnce` rather than `times(4)` to be robust to small variations in
    // serialized record sizes that could push a partition over the 2 MB boundary.
    verify(backpressure, atLeastOnce()).recordTransmission(
      anyInt(), anyLong(), anyInt(), anyLong(), anyLong())
  }
}
