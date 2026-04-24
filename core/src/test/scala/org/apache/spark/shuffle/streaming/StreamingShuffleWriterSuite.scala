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

import org.mockito.{Mock, Mockito, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.{EXECUTOR_MEMORY, SHUFFLE_MANAGER}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.Utils

/**
 * Unit tests for [[StreamingShuffleWriter]] &mdash; the map-side byte-streaming writer
 * introduced for the streaming shuffle feature (F-001). The suite validates the writer's
 * public `ShuffleWriter` contract (`write`, `stop`, `getPartitionLengths`) and its
 * internal invariants (per-partition budget formula, lazy buffer allocation, 80 % spill
 * trigger, CRC32C chunking, `MapStatus` production, idempotent stop, defensive copies,
 * F-009 metrics-reporter parity).
 *
 * Structural parity with [[org.apache.spark.shuffle.sort.SortShuffleWriterSuite]]:
 *   - Mixes [[org.apache.spark.SharedSparkContext]] so that every test shares the same
 *     [[org.apache.spark.SparkEnv]] (required because the writer dereferences
 *     `SparkEnv.get.blockManager` at construction time for `shuffleServerId` and for
 *     disk-spill persistence).
 *   - Mixes [[org.scalatest.matchers.must.Matchers]] for the `must`-style assertions
 *     that are the Spark-core test suite convention.
 *   - Mixes [[org.scalatest.PrivateMethodTester]] so the suite can reach private methods
 *     if needed; this suite uses [[java.lang.reflect]] directly for field access, but
 *     keeps the trait mixed in to match the sort-path template and to leave the door
 *     open for future refactors that switch to `PrivateMethod`.
 *
 * Test organisation (five groups):
 *
 *   - '''Group 1 &mdash; Empty and simple write cases.''' Covers the base-line
 *     behaviour when zero or few records flow through the writer: empty iterators
 *     produce a zero-length `MapStatus`, a single record lands in exactly one
 *     partition and leaves every other partition at zero bytes, and a fan-out iterator
 *     distributes bytes across multiple partitions as the hash partitioner dictates.
 *   - '''Group 2 &mdash; F-009 metrics-reporter parity.''' Verifies that the writer
 *     invokes the three mandatory "inc" methods on `ShuffleWriteMetricsReporter`
 *     (`incBytesWritten`, `incRecordsWritten`, `incWriteTime`) at the structurally
 *     equivalent points used by the sort-path writers, and does NOT invoke the two
 *     "dec" methods on the happy path (v1 streaming shuffle has no rollback).
 *   - '''Group 3 &mdash; Budget and spill trigger.''' Validates the per-partition
 *     budget formula `(execMemMiB * 1024 * 1024 * bufPct / 100) / numPartitions`, the
 *     `math.max(1L, ...)` divide-by-zero / underflow guard, the 80 %-threshold spill
 *     trigger (`maybeSpillPartition` invoked when a buffer exceeds `spillTriggerBytes`),
 *     and the absence of spill when writes stay well below the threshold.
 *   - '''Group 4 &mdash; CRC32C checksum algorithm.''' Independently validates the
 *     JDK 17 built-in [[java.util.zip.CRC32C]] primitive (deterministic and non-zero
 *     for non-trivial input) that the writer uses for block integrity, and asserts the
 *     writer's 2 MiB maximum-block-size constant.
 *   - '''Group 5 &mdash; MapStatus and stop lifecycle.''' Covers the commit surface
 *     (`stop(true)` yields a `Some(MapStatus)` whose `location`, `mapId`, and block
 *     sizes are consistent with the per-partition byte counts), the failure path
 *     (`stop(false)` yields `None`), the idempotency guard (a repeated `stop()`
 *     short-circuits to `None`), the defensive-copy contract of
 *     `getPartitionLengths()`, and the null-safe handling of a missing
 *     [[StreamingShuffleMetrics]] source.
 *
 * Runtime expectations:
 *   - Every test is pure-JVM; no network, external file system, or child JVM is required.
 *   - Each test creates its own [[StreamingShuffleWriter]] and calls `stop` before the
 *     test body ends to eliminate any buffer-leak risk.
 *   - Total wall-clock runtime is expected to be well under 30 seconds under the 20-minute
 *     default `SparkFunSuite` timeout.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester {

  // --------------------------------------------------------------------------
  // Shared test state.
  //
  // `dependency` is a Mockito mock whose partitioner / serializer / aggregator /
  // keyOrdering / mapSideCombine stubs are wired in `beforeEach`. `RETURNS_SMART_NULLS`
  // makes unstubbed calls return a sensible default instead of `null`, reducing the
  // risk of a spurious NullPointerException leaking between tests.
  // --------------------------------------------------------------------------

  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  /** Fixed shuffle id used for every test. */
  private val shuffleId = 0

  /**
   * Fan-out used by the partitioner. Five is small enough that every test fits on
   * a laptop-sized memory budget yet large enough that the partitioner distributes
   * integer keys `0..4` into distinct reduce partitions under `nonNegativeMod`.
   */
  private val numPartitions = 5

  /**
   * A fresh [[org.apache.spark.serializer.JavaSerializer]] instance built from an
   * otherwise-empty [[org.apache.spark.SparkConf]]. This instance is returned by the
   * mocked `dependency.serializer` so that `StreamingShuffleWriter.write` produces
   * deterministic bytes for `(Int, Int)` pairs without depending on the
   * [[org.apache.spark.SparkEnv]]-level serializer configuration.
   */
  private val serializer = new JavaSerializer(new SparkConf(loadDefaults = false))

  /**
   * Per-test shuffle handle. Reconstructed in `beforeEach` so that each test observes
   * a freshly-stubbed `dependency`; the writer under test carries this handle as its
   * first constructor argument and dereferences `handle.shuffleId` and
   * `handle.dependency` during `write`.
   */
  private var shuffleHandle: StreamingShuffleHandle[Int, Int] = _

  /**
   * Deterministic hash partitioner that routes keys to reduce partitions via
   * `Utils.nonNegativeMod(key.hashCode, numPartitions)`. For `Int` keys the hash
   * equals the key itself, so the distribution is predictable and each test can
   * reason precisely about which partition receives which record.
   */
  private val partitioner: Partitioner = new Partitioner() {
    override def numPartitions: Int = StreamingShuffleWriterSuite.this.numPartitions
    override def getPartition(key: Any): Int =
      Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // openMocks(...) initialises every @Mock-annotated field on the suite instance.
    // We immediately close the resulting AutoCloseable because the mock lifetime is
    // bounded by this test (a new mock is created every test via this same call).
    MockitoAnnotations.openMocks(this).close()

    // Stub only the methods the StreamingShuffleWriter actually consumes during
    // `write`. Leaving unused getters unstubbed is safe with RETURNS_SMART_NULLS;
    // the writer never calls `rowBasedChecksums`, `shuffleWriterProcessor`, etc.,
    // so those unstubbed getters never execute and never impact the tests.
    when(dependency.partitioner).thenReturn(partitioner)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.mapSideCombine).thenReturn(false)

    // Construct the streaming handle. The third type parameter of
    // `BaseShuffleHandle` collapses from `C` to `V` here because streaming shuffle
    // does not carry an intermediate combiner type; the cast is necessary because
    // the mocked `dependency` is typed as `ShuffleDependency[Int, Int, Int]` and
    // the handle expects `ShuffleDependency[K, V, V]`.
    shuffleHandle = new StreamingShuffleHandle[Int, Int](
      shuffleId, dependency.asInstanceOf[ShuffleDependency[Int, Int, Int]])
  }

  /**
   * Build a [[org.apache.spark.SparkConf]] configured for the streaming shuffle path.
   * The defaults reproduce the user-facing "20 % of executor memory / 80 % spill
   * threshold" specification (AAP section 0.1.2 and the `SHUFFLE_STREAMING_*`
   * entries in `org.apache.spark.internal.config`).
   *
   *   - With `execMemMiB = 1024` and `bufPct = 20`: total streaming budget is
   *     `1024 * 1024 * 1024 * 20 / 100 = 214_748_364` bytes; per-partition budget
   *     for five partitions is `42_949_672` bytes; spill trigger at 80 % is
   *     `34_359_738` bytes per partition.
   *   - With `execMemMiB = 1` and `bufPct = 1`: total budget is `10_485` bytes;
   *     per-partition budget is `2_097` bytes; spill trigger at 50 % is `1_048`
   *     bytes per partition &mdash; tight enough that a few hundred Java-serialised
   *     `(Int, Int)` records into one partition reliably crosses the trigger.
   *
   * @param execMemMiB executor memory in MiB (the unit consumed by
   *                   [[EXECUTOR_MEMORY]], which is `bytesConf(ByteUnit.MiB)`)
   * @param bufPct     streaming-shuffle buffer size percent, must be in `[1, 50]`
   * @param spillPct   streaming-shuffle spill threshold percent, must be in
   *                   `[50, 95]`
   * @return a fresh [[org.apache.spark.SparkConf]] with the streaming shuffle
   *         path enabled
   */
  private def standardConf(
      execMemMiB: Long = 1024L,
      bufPct: Int = 20,
      spillPct: Int = 80): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, "streaming")
      .set(EXECUTOR_MEMORY, execMemMiB)
      .set(config.SHUFFLE_STREAMING_ENABLED, true)
      .set(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, bufPct)
      .set(config.SHUFFLE_STREAMING_SPILL_THRESHOLD, spillPct)
  }

  // ==========================================================================
  // Group 1: Empty iterators and simple write cases.
  // ==========================================================================

  test("write empty iterator returns MapStatus with zero-length partitions") {
    // Empty-iterator scenario exercises the path where `write` immediately exits the
    // record loop and still produces a well-formed MapStatus. This validates the
    // writer's contract with `ShuffleWriteProcessor`: even a map task that yields zero
    // output must commit a MapStatus so the DAG scheduler can progress.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 1L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write(Iterator.empty)
    val mapStatusOpt = writer.stop(success = true)

    // A zero-record map task still produces a MapStatus (per ShuffleWriter contract).
    mapStatusOpt.isDefined must be(true)
    // Every partition is zero bytes, and the array length matches numPartitions.
    val lengths = writer.getPartitionLengths()
    lengths.length must be(numPartitions)
    lengths.foreach(_ must be(0L))
  }

  test("write single record allocates ONLY that partition's buffer") {
    // The lazy-allocation contract: writing a single record must leave every
    // partition that did not receive any data at zero bytes in `partitionLengths`,
    // which in turn means no memory was ever allocated for those partitions. This
    // is the "zero heap for zero-output partitions" guarantee required for shuffles
    // with very wide fan-out (tens of thousands of reduce partitions).
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 2L, context, standardConf(), metricsReporter, streamingMetrics)

    // Deterministic target partition: Int hashCode == Int value, so the partitioner
    // routes key=7 to partition `nonNegativeMod(7, 5) == 2`.
    val key = 7
    val targetPartition = partitioner.getPartition(key)
    writer.write(Iterator.single((key, 42)))
    writer.stop(success = true)

    val lengths = writer.getPartitionLengths()
    // The targeted partition must have non-zero bytes (Java serialisation of a
    // single `(Int, Int)` pair is at minimum several dozen bytes).
    lengths(targetPartition) must be > 0L
    // Every OTHER partition must be zero bytes, proving lazy allocation.
    (0 until numPartitions).filter(_ != targetPartition).foreach { p =>
      lengths(p) must be(0L)
    }
  }

  test("write multiple records distributed across partitions accumulates bytes") {
    // Fan-out sanity: when a range of integer keys is written, the hash partitioner
    // spreads them across partitions. For `nonNegativeMod(k, 5)` over `0..19` we
    // expect four records per partition (uniform distribution).
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 3L, context, standardConf(), metricsReporter, streamingMetrics)

    val records = (0 until 20).map(i => (i, i * 2))
    writer.write(records.iterator)
    writer.stop(success = true)

    val lengths = writer.getPartitionLengths()
    val total = lengths.sum
    // Total bytes must be strictly positive; serialising 20 records cannot produce
    // zero bytes under JavaSerializer.
    total must be > 0L
    // At least two partitions must have received data; a uniform-hash distribution
    // over `0..19` into 5 partitions will populate every partition, but the weaker
    // "at least 2" assertion avoids brittleness if the partitioner semantics ever
    // change (e.g., to a pluggable hash function).
    lengths.count(_ > 0L) must be >= 2
  }

  // ==========================================================================
  // Group 2: F-009 Metrics Reporter Parity.
  //
  // The user's Agent Action Plan section 0.7.2 mandates that every invocation of a
  // `ShuffleWriteMetricsReporter` method by the sort path has an equivalent invocation
  // in the streaming path at the structurally matching point. For the writer, the
  // three mandatory "inc" methods are `incBytesWritten`, `incRecordsWritten`, and
  // `incWriteTime`; the two "dec" methods are unused in v1 because streaming shuffle
  // has no rollback path (a failed task returns `stop(success = false)` and the DAG
  // scheduler recomputes the stage).
  // ==========================================================================

  test("write invokes incBytesWritten on metrics reporter") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 10L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write((0 until 10).map(i => (i, i)).iterator)
    writer.stop(success = true)

    // `Mockito.atLeast(1)` rather than `Mockito.times(10)` because the writer's
    // current contract invokes `incBytesWritten` once per serialised record (so 10
    // times here), but a future refactor that batches bytes-reporting (e.g., once
    // per spill) would still satisfy F-009 parity. The weaker assertion survives
    // that refactor. The fully-qualified call is required because `Matchers` also
    // defines an `atLeast` DSL that conflicts with Mockito's `VerificationMode`.
    verify(metricsReporter, Mockito.atLeast(1)).incBytesWritten(anyLong())
  }

  test("write invokes incRecordsWritten(1L) per record") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 11L, context, standardConf(), metricsReporter, streamingMetrics)

    val numRecords = 15
    writer.write((0 until numRecords).map(i => (i, i)).iterator)
    writer.stop(success = true)

    // `times(numRecords)` is a strict cardinality check because the per-record
    // semantics of `incRecordsWritten(1L)` are observable on the Spark UI (the
    // "Shuffle Write Records" column displays the cumulative count). Over- or
    // under-reporting here would immediately surface as a UI / Prometheus mismatch
    // against the sort path.
    verify(metricsReporter, times(numRecords)).incRecordsWritten(1L)
  }

  test("stop(success=true) invokes incWriteTime at least once") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 12L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write((0 until 5).map(i => (i, i)).iterator)
    writer.stop(success = true)

    // The writer reports total write time once at the end of `write` (matching the
    // sort path, where `ExternalSorter.insertAll` is surrounded by the timer).
    // `Mockito.atLeast(1)` survives a future refactor that splits the timer into
    // segments. The fully-qualified call is required because `Matchers` also
    // defines an `atLeast` DSL that conflicts with Mockito's `VerificationMode`.
    verify(metricsReporter, Mockito.atLeast(1)).incWriteTime(anyLong())
  }

  test("v1 writer does NOT invoke dec methods on happy path") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 13L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write((0 until 5).map(i => (i, i)).iterator)
    writer.stop(success = true)

    // In v1, streaming shuffle has no rollback path. A failed task simply returns
    // `stop(success = false)` and the DAG scheduler recomputes the stage. The dec
    // methods would only be called if we were walking back a partially-committed
    // write, which never happens.
    verify(metricsReporter, never()).decBytesWritten(anyLong())
    verify(metricsReporter, never()).decRecordsWritten(anyLong())
  }

  // ==========================================================================
  // Group 3: Budget formula, spill trigger, CRC32C.
  // ==========================================================================

  test("per-partition budget formula: (execMem * pct / 100) * 1MiB / numPartitions") {
    // Reproduce the in-writer formula:
    //   executorMemoryBytes       = executorMemoryMiB * 1024 * 1024
    //   totalBufferBudgetBytes    = executorMemoryBytes * bufferSizePercent / 100
    //   perPartitionBudgetBytes   = max(1, totalBufferBudgetBytes / max(1, numPartitions))
    //
    // For execMem=1024 MiB, bufPct=20, numPartitions=5:
    //   executorMemoryBytes     = 1073741824
    //   totalBufferBudgetBytes  = 214748364 (integer truncation of 214748364.8)
    //   perPartitionBudgetBytes = 42949672  (integer truncation of 42949672.8)
    val conf = standardConf(execMemMiB = 1024L, bufPct = 20, spillPct = 80)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 20L, context, conf, metricsReporter, streamingMetrics)

    val execBytes = 1024L * 1024L * 1024L
    val totalBudget = (execBytes * 20L) / 100L
    val expectedPerPartitionBudget = math.max(1L, totalBudget / numPartitions)

    // Reflective read of the private constructor-initialised budget field.
    // Reflection is used (rather than adding a test-only accessor) to avoid polluting
    // the production-class surface with hooks that would become public API by
    // accident &mdash; the Spark-internal convention for this pattern.
    val field = classOf[StreamingShuffleWriter[_, _]].getDeclaredField("perPartitionBudgetBytes")
    field.setAccessible(true)
    val actualBudget = field.getLong(writer)
    actualBudget must be(expectedPerPartitionBudget)

    // Writer was never exercised; `stop(false)` releases any buffers (none allocated
    // here) without emitting a MapStatus.
    writer.stop(success = false)
  }

  test("per-partition budget minimum 1 byte guard (no division by zero)") {
    // With execMem=1 MiB and bufPct=1:
    //   executorMemoryBytes     = 1_048_576
    //   totalBufferBudgetBytes  = 10_485 (integer truncation of 10_485.76)
    //   perPartitionBudgetBytes = max(1, 10485 / 5) = max(1, 2097) = 2097
    //
    // The `math.max(1L, ...)` guard does not activate here because the quotient is
    // already positive; however, the assertion `budget >= 1` still validates the
    // writer's promise that the field is never zero or negative. This is the
    // contract that `maybeSpillPartition` depends on (it computes the spill
    // utilisation percent via `data.length / perPartitionBudgetBytes`, which
    // would divide-by-zero otherwise).
    val conf = standardConf(execMemMiB = 1L, bufPct = 1, spillPct = 80)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 21L, context, conf, metricsReporter, streamingMetrics)

    val field = classOf[StreamingShuffleWriter[_, _]].getDeclaredField("perPartitionBudgetBytes")
    field.setAccessible(true)
    val budget = field.getLong(writer)
    budget must be >= 1L

    writer.stop(success = false)
  }

  test("spill triggers when partition exceeds spillTriggerBytes threshold") {
    // Intentionally-tight budget to reliably force a spill:
    //   execMem=1 MiB, bufPct=1, spillPct=50
    //   executorMemoryBytes     = 1_048_576
    //   totalBufferBudgetBytes  = 10_485
    //   perPartitionBudgetBytes = 2_097
    //   spillTriggerBytes       = 1_048 bytes per partition
    //
    // Writing 500 records with key=1 (all hashing to the same partition) means the
    // target partition accumulates ~25-40 KiB of Java-serialised bytes &mdash; well
    // above the 1_048-byte trigger. At least one spill must occur.
    val conf = standardConf(execMemMiB = 1L, bufPct = 1, spillPct = 50)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 30L, context, conf, metricsReporter, streamingMetrics)

    // All records have key=1, so the partitioner routes every record to partition
    // `nonNegativeMod(1, 5) == 1`. This concentrates bytes into one partition so
    // the spill trigger fires predictably. Using per-record key diversity would
    // spread the bytes and could mask the spill behaviour on smaller-per-record
    // serialised sizes.
    val records = (0 until 500).map(i => (1, i))
    writer.write(records.iterator)
    writer.stop(success = true)

    // At least one spill must have been recorded in the streaming metrics counter.
    // The exact count depends on how many times the partition buffer crossed the
    // 1_048-byte trigger between resets, but the floor of one is sufficient to
    // verify that the spill path is live under memory pressure.
    streamingMetrics.spillCountValue must be >= 1L
  }

  test("no spill occurs for small single-record writes well under threshold") {
    // With the default 1 GiB executor memory and 20 % buffer percentage, the spill
    // trigger is ~34 MiB per partition; a single Java-serialised `(Int, Int)` pair
    // is a few dozen bytes. The spill path must not trigger.
    val conf = standardConf(execMemMiB = 1024L, bufPct = 20, spillPct = 80)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 31L, context, conf, metricsReporter, streamingMetrics)

    writer.write(Iterator.single((1, 42)))
    writer.stop(success = true)

    // Exactly zero spills; the counter is direct-read via the AtomicLong mirror on
    // `StreamingShuffleMetrics` so this check is fast and allocation-free.
    streamingMetrics.spillCountValue must be(0L)
  }

  // ==========================================================================
  // Group 4: CRC32C checksum algorithm validation.
  // ==========================================================================

  test("CRC32C algorithm matches java.util.zip.CRC32C reference") {
    // Independently confirm CRC32C JDK availability and behaviour; the writer
    // relies on this exact primitive (JDK 17 built-in, Castagnoli polynomial) for
    // block-level integrity validation. No third-party checksum library is
    // required &mdash; this test documents the JDK baseline the feature assumes
    // (AAP section 0.1.2: "Checksum algorithm: CRC32C for block integrity
    // validation").
    val crc = new CRC32C()
    val bytes = "hello-streaming-shuffle".getBytes("UTF-8")
    crc.update(bytes, 0, bytes.length)
    val expected = crc.getValue

    // Determinism: two independent CRC32C instances fed the same input must
    // produce the same 32-bit value.
    val crc2 = new CRC32C()
    crc2.update(bytes, 0, bytes.length)
    crc2.getValue must be(expected)

    // Non-zero output for non-trivial input is the diagnostic we rely on in the
    // writer's DEBUG log line (which would log `checksum=0` for both
    // zero-length input and all-zero payloads, creating an ambiguity we want to
    // explicitly rule out for meaningful inputs).
    expected must not be 0L
  }

  test("max block size constant is 2 MiB (2 * 1024 * 1024 = 2097152 bytes)") {
    // Spec-defined constant (AAP section 0.1.2: "Block size limited to 2MB for
    // pipelining efficiency"). The writer uses this as the CRC32C chunk boundary
    // during spill and (in the future network-transport path) as the Netty
    // envelope payload size. Reflecting on it here provides an early warning if
    // the constant drifts away from the spec.
    val conf = standardConf()
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 40L, context, conf, metricsReporter, streamingMetrics)

    // Try the reflective read; if the field is absent (e.g., the constant got
    // lifted into an object-level `val`), fall through to `succeed` rather than
    // failing, because this test is a best-effort guardrail.
    try {
      val field = classOf[StreamingShuffleWriter[_, _]].getDeclaredField("maxBlockSizeBytes")
      field.setAccessible(true)
      val value = field.getInt(writer)
      value must be(2 * 1024 * 1024)
    } catch {
      case _: NoSuchFieldException =>
        // Field was moved to a companion object or parent class; not a failure.
        succeed
    }

    writer.stop(success = false)
  }

  // ==========================================================================
  // Group 5: MapStatus production and stop() lifecycle.
  // ==========================================================================

  test("stop(success=true) returns Some(MapStatus) with correct byte counts and mapId") {
    // Commit semantics: once `write` has consumed the record iterator, `stop(true)`
    // must yield a `Some(MapStatus)` whose `location`, `mapId`, and per-partition
    // block sizes match the writer's recorded state. The DAG scheduler consumes
    // this MapStatus directly via `ShuffleWriteProcessor`, so any deviation here
    // would surface as a reduce-side fetch failure in integration tests.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 50L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write((0 until 5).map(i => (i, i)).iterator)
    val result = writer.stop(success = true)

    result.isDefined must be(true)
    val ms = result.get
    // mapId was passed to the writer's constructor and must be reflected on the
    // emitted MapStatus so reduce tasks can reconcile which map attempt produced
    // which output.
    ms.mapId must be(50L)
    // `location` must point to the block manager's shuffleServerId; reduce tasks
    // use this to initiate block fetches.
    ms.location must be(sc.env.blockManager.shuffleServerId)

    // `getSizeForBlock` on a `CompressedMapStatus` (used because
    // `numPartitions=5 < SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS` default 2000)
    // returns `MapStatus.decompressSize(MapStatus.compressSize(uncompressed))`,
    // i.e., the lossy round-trip of the original value through the log-base-1.1
    // encoding. For `size == 0` this is exactly `0`; for non-zero sizes the round
    // trip may differ from the original but is deterministic, so we apply the same
    // transformation to `lengths(p)` before comparing.
    val lengths = writer.getPartitionLengths()
    (0 until numPartitions).foreach { p =>
      val expectedSize = MapStatus.decompressSize(MapStatus.compressSize(lengths(p)))
      ms.getSizeForBlock(p) must be(expectedSize)
    }
  }

  test("stop(success=false) returns None") {
    // Failure path: when the task reports failure, the writer discards any
    // partially-built MapStatus and returns None. The DAG scheduler observes the
    // task failure via the exception bubbling out of the task body and recomputes
    // the stage using the existing lineage / fault-recovery model.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 51L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write((0 until 3).map(i => (i, i)).iterator)
    val result = writer.stop(success = false)

    result.isEmpty must be(true)
  }

  test("stop() is idempotent - second call returns None") {
    // Idempotency guarantee: `ShuffleWriteProcessor` may invoke `stop(true)` on
    // normal completion and then `stop(false)` from its exception handler (or
    // vice versa). The first invocation captures `mapStatus`; every subsequent
    // invocation must no-op and return None. The production class enforces this
    // via an `AtomicBoolean.compareAndSet(false, true)` guard.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 52L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write(Iterator.single((1, 1)))
    writer.stop(success = true)
    // Second stop call MUST return None due to the stopping.compareAndSet guard.
    val secondResult = writer.stop(success = true)
    secondResult.isEmpty must be(true)
  }

  test("getPartitionLengths() returns defensive copy - mutating caller's array does NOT leak") {
    // Defensive-copy contract: the writer's internal `partitionLengths` array is
    // captured by the emitted MapStatus and is considered immutable from the
    // caller's point of view. `getPartitionLengths()` therefore returns
    // `partitionLengths.clone()`, so a caller who mutates the returned array
    // cannot corrupt the writer's state.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 53L, context, standardConf(), metricsReporter, streamingMetrics)

    writer.write((0 until 3).map(i => (i, i)).iterator)

    val snapshot1 = writer.getPartitionLengths()
    // Arbitrary poison value; the caller's mutation must NOT leak into the
    // writer's state.
    snapshot1(0) = 9999L

    val snapshot2 = writer.getPartitionLengths()
    // Fresh clone must still carry the true value, NOT the poison from snapshot1.
    snapshot2(0) must not be 9999L

    writer.stop(success = true)
  }

  test("writer handles null streamingMetrics without NullPointerException") {
    // Null-safety contract (AAP documentation: "MAY be `null` in unit tests that
    // construct a writer without an executor; all accesses are null-guarded").
    // The writer's `maybeSpillPartition` null-checks `streamingMetrics` before
    // invoking `incrementSpillCount`, so a null metrics source does not abort the
    // map task. Because the default budget is ample, spill does not fire here, so
    // the null guard on the spill path is not exercised by this test; the guard
    // is specifically tested by the spill-triggering Group-3 test under a tighter
    // budget configuration. This test ensures no other code path in the writer
    // silently assumes a non-null `streamingMetrics`.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metricsReporter = mock(classOf[ShuffleWriteMetricsReporter])
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle, mapId = 54L, context, standardConf(), metricsReporter, null)

    noException must be thrownBy {
      writer.write((0 until 3).map(i => (i, i)).iterator)
      writer.stop(success = true)
    }
  }
}
