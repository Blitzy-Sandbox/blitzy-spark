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

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyLong
// Import `Mockito` as a namespace so `Mockito.atLeast(1)` can be called with
// the fully-qualified prefix &mdash; ScalaTest's `Matchers` trait inherits an
// `atLeast(num, xs)` collection-matcher as a class member, and inherited
// members have higher precedence than explicit imports in Scala. Importing
// `atLeast` directly would therefore be shadowed by the `Matchers.atLeast`
// method even with an explicit named import. The other streaming-shuffle
// suites (MemorySpillManagerSuite, BackpressureProtocolSuite) use the same
// qualified-reference pattern for the same reason.
import org.mockito.Mockito
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleReadMetricsReporter

/**
 * Unit tests for [[StreamingShuffleReader]] &mdash; the reduce-side iterator that backs
 * the streaming shuffle feature (F-001). This suite validates the class-level contract
 * the AAP (section 0.5.1.3) binds to the reader:
 *
 *   1. `read()` returns an [[Iterator]] typed as `Iterator[Product2[K, C]]`. In the v1
 *      landable increment (prior to end-to-end transport wiring) the iterator is empty,
 *      which is the correct degenerate-case answer when no streaming blocks have arrived
 *      for the requested partition range and when streaming shuffle is opt-in via
 *      `spark.shuffle.manager=streaming`.
 *   2. F-009 (Shuffle Metrics Preservation) parity: every `read()` invocation flushes at
 *      least the three reporter methods that `BlockStoreShuffleReader` flushes at its
 *      task-completion listener &mdash; `incRemoteBytesRead`, `incRecordsRead`, and
 *      `incFetchWaitTime`. The nine push-based merge-metric methods (ADR-005 opt-in
 *      scope) are '''not''' invoked in v1 because streaming shuffle and push-based
 *      shuffle are mutually exclusive for an active shuffle.
 *   3. `read()` registers a [[org.apache.spark.util.TaskCompletionListener]] against the
 *      supplied [[TaskContext]] so the F-009 counters are flushed even if the task
 *      completes via failure or cancellation.
 *   4. The reader's 9-parameter constructor tolerates a `null` `streamingMetrics`
 *      argument, because unit tests that exercise the reader outside an executor
 *      context may construct a reader without a backing `MetricsSystem`. The
 *      constructor must defend nullability at every call site and `read()` must not
 *      throw a [[NullPointerException]] when `streamingMetrics` is `null`.
 *
 * This suite follows the `SparkFunSuite with SharedSparkContext with Matchers` pattern
 * used by [[org.apache.spark.shuffle.sort.SortShuffleWriterSuite]] and by the sister
 * streaming-shuffle suites (`StreamingShuffleHandleSuite`, `BackpressureProtocolSuite`,
 * `MemorySpillManagerSuite`). A local `SparkContext` is shared across tests so the
 * reader can acquire a real executor-scoped `BlockManager` reference via
 * `SparkEnv.get.blockManager` at construction time.
 *
 * Group 5 contains [[ignore]]d test cases that are documented placeholders for behavior
 * deferred to the v2 transport wiring (producer-failure detection via 5 s connection
 * timeout, CRC32C checksum mismatch retransmission, atomic partial-read invalidation).
 * These tests are registered but not executed; they serve as contract documentation and
 * MUST NOT be deleted when v2 work begins &mdash; they will be promoted to `test(...)`
 * form then.
 */
class StreamingShuffleReaderSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers {

  // --------------------------------------------------------------------------
  // Per-suite immutable fixture state. Shared by every test via beforeEach().
  // --------------------------------------------------------------------------

  /** Synthetic shuffle id used by every reader constructed in this suite. */
  private val shuffleId = 100

  /** Fixed reduce-partition count used by the inline Partitioner subclass. */
  private val numPartitions = 5

  /**
   * Mockito-stubbed [[ShuffleDependency]]. Re-created in `beforeEach` so that
   * every test starts with fresh stub state &mdash; tests that check call counts
   * or wire new behavior do not contaminate one another.
   */
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  /**
   * [[StreamingShuffleHandle]] carrying the shuffleId and the stubbed dependency.
   * Re-built in `beforeEach` so each test starts with a fresh instance.
   */
  private var shuffleHandle: StreamingShuffleHandle[Int, Int] = _

  // --------------------------------------------------------------------------
  // Fixture setup. Invoked before every test by ScalaTest's BeforeAndAfterEach
  // (inherited transitively through SharedSparkContext).
  // --------------------------------------------------------------------------

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Fresh Mockito stub per test &mdash; guards against interaction leakage.
    dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    // Inline Partitioner subclass delegating to the enclosing suite's
    // `numPartitions` field. The qualified `StreamingShuffleReaderSuite.this`
    // disambiguates the outer-class reference from the `numPartitions` member
    // of the anonymous Partitioner subclass that we are in the process of
    // defining (they share the same simple name).
    val partitioner = new Partitioner() {
      override def numPartitions: Int = StreamingShuffleReaderSuite.this.numPartitions
      override def getPartition(key: Any): Int = 0
    }
    when(dependency.partitioner).thenReturn(partitioner)
    // JavaSerializer is used instead of KryoSerializer to avoid registering
    // Kryo classes in this narrow test scope; the reader never actually
    // deserializes bytes in v1 (empty iterator), so the choice of serializer
    // here is inconsequential &mdash; only `dep.serializer.newInstance()` is
    // invoked and the returned instance is never used.
    when(dependency.serializer)
      .thenReturn(new JavaSerializer(new SparkConf(loadDefaults = false)))
    // Aggregator and keyOrdering MUST return non-null Option values so the
    // reader's `if (dep.keyOrdering.isDefined)` / `if (dep.aggregator.isDefined)`
    // branches do not NPE. The v1 empty-iterator path descends only through
    // the else branch, but correct Option stubbing keeps the suite resilient
    // to future reader refactors that may query these fields eagerly.
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.mapSideCombine).thenReturn(false)
    shuffleHandle = new StreamingShuffleHandle[Int, Int](shuffleId, dependency)
  }

  // --------------------------------------------------------------------------
  // Helpers used by every test to construct a fresh reader.
  // --------------------------------------------------------------------------

  /**
   * Build a `SparkConf` pinned to the streaming shuffle path. The three keys
   * referenced here are the ones the reader's constructor and budget logic
   * consult:
   *
   *   - `spark.shuffle.manager=streaming` &mdash; selects the streaming path
   *     at the short-name selector in `ShuffleManager.shortShuffleMgrNames`.
   *   - `spark.shuffle.streaming.enabled=true` &mdash; activates the opt-in
   *     behavior that the reader's debug log line consults.
   *   - `spark.executor.memory=1024` (MiB) &mdash; supplies the executor
   *     memory budget for any future per-partition buffer sizing. In v1 the
   *     reader does not allocate buffers, but the field is retained for
   *     budget parity with the sort path.
   */
  private def standardConf(): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, "streaming")
      .set(config.SHUFFLE_STREAMING_ENABLED, true)
      .set(config.EXECUTOR_MEMORY, 1024L)
  }

  /**
   * Convenience tuple returned by [[buildReaderWithContext]] &mdash; pairs the
   * freshly-constructed reader with its TaskContext so callers can call
   * `context.markTaskCompleted(None)` to trigger the F-009 counter flush
   * performed by the task-completion listener that `read()` registers.
   */
  private case class ReaderWithContext(
      reader: StreamingShuffleReader[Int, Int],
      context: TaskContext)

  /**
   * Construct a fresh [[StreamingShuffleReader]] alongside its backing
   * `TaskContext`. Exposing the context is intentional: the reader registers
   * a task-completion listener inside `read()` that flushes the F-009
   * counters (`incRemoteBytesRead`, `incRecordsRead`, `incFetchWaitTime`)
   * at task end. `TaskContextImpl` only invokes completion listeners when
   * `markTaskCompleted(...)` is called &mdash; it does NOT fire listeners
   * eagerly. Tests verifying F-009 parity therefore call
   * `markTaskCompleted(None)` on the returned context after `read()` to
   * simulate task completion and flush the counter invocations that
   * Mockito's `verify(...)` then asserts.
   *
   * The `TaskContext` is a `MemoryTestingUtils.fakeTaskContext` bound to
   * the shared `sc.env` so that the reader's `SparkEnv.get.blockManager`
   * access at construction resolves to a real executor-scoped
   * `BlockManager`. This follows the same pattern used by
   * `SortShuffleWriterSuite` (at
   * `core/src/test/scala/org/apache/spark/shuffle/sort/
   * SortShuffleWriterSuite.scala` line 97) and reuses the shared
   * `SparkContext` provided by `SharedSparkContext`.
   */
  private def buildReaderWithContext(
      readMetrics: ShuffleReadMetricsReporter,
      streamingMetrics: StreamingShuffleMetrics = new StreamingShuffleMetrics(),
      startMap: Int = 0,
      endMap: Int = Int.MaxValue,
      startPart: Int = 0,
      endPart: Int = numPartitions): ReaderWithContext = {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val reader = new StreamingShuffleReader[Int, Int](
      shuffleHandle, startMap, endMap, startPart, endPart,
      context, standardConf(), readMetrics, streamingMetrics)
    ReaderWithContext(reader, context)
  }

  /**
   * Narrower helper that returns just the reader. Used by tests that do
   * not need to trigger task-completion-listener flushing (e.g. tests
   * asserting constructor behavior or the empty-iterator return value).
   */
  private def buildReader(
      readMetrics: ShuffleReadMetricsReporter,
      streamingMetrics: StreamingShuffleMetrics = new StreamingShuffleMetrics(),
      startMap: Int = 0,
      endMap: Int = Int.MaxValue,
      startPart: Int = 0,
      endPart: Int = numPartitions): StreamingShuffleReader[Int, Int] = {
    buildReaderWithContext(readMetrics, streamingMetrics,
      startMap, endMap, startPart, endPart).reader
  }

  // ==========================================================================
  // Group 1: v1 empty-iterator behavior (transport not yet wired)
  //
  // The AAP (section 0.5.1.2 and StreamingShuffleReader.scala class-level
  // scaladoc under "v1 implementation note") binds the v1 reader to return
  // an empty iterator whenever no streaming blocks have been buffered for
  // the requested partition range. Because the transport is not yet wired,
  // every v1 read() call satisfies this condition trivially. These tests
  // pin that behavior so a future transport-wiring iteration that lands a
  // non-empty iterator must explicitly update or retire them.
  // ==========================================================================

  test("read() returns empty iterator in v1") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val reader = buildReader(readMetrics)

    val iter = reader.read()
    iter.hasNext must be(false)
    iter.isEmpty must be(true)
  }

  test("read() returns Iterator[Product2[K, C]] of correct type") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val reader = buildReader(readMetrics)

    // Compile-time type ascription: if `read()` narrowed or broadened its
    // return type, this assignment would fail to type-check. The assertion
    // is intentionally minimal because the compile-time check is the
    // contract under test.
    val iter: Iterator[Product2[Int, Int]] = reader.read()
    iter must not be null
  }

  // ==========================================================================
  // Group 2: F-009 Metrics Reporter Parity (3 required inc methods)
  //
  // The F-009 mandate (AAP section 0.7.2) requires that every invocation of
  // a ShuffleReadMetricsReporter method by BlockStoreShuffleReader has an
  // equivalent invocation at the structurally matching point in
  // StreamingShuffleReader. The minimum set is:
  //   - incRemoteBytesRead  (cumulative bytes pulled from remote producers)
  //   - incRecordsRead      (cumulative deserialized records)
  //   - incFetchWaitTime    (total millis the reader blocked)
  // The nine push-based merge-metric methods (ADR-005 opt-in scope) are
  // verified to never fire in v1 because streaming shuffle and push-based
  // shuffle are mutually exclusive for an active shuffle.
  // ==========================================================================

  test("read() invokes incRemoteBytesRead on metrics reporter") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val rwc = buildReaderWithContext(readMetrics)

    rwc.reader.read()
    // Trigger the task-completion listener that the reader registered
    // inside read(). The listener is what flushes the F-009 counters
    // &mdash; without this call TaskContextImpl holds the listener
    // registered-but-never-invoked, matching the semantics at task-end
    // when the executor's task-runner calls markTaskCompleted(None).
    rwc.context.markTaskCompleted(None)

    // The value passed to the counter is 0L in v1 (empty iterator) but
    // the counter method is still invoked, which is the F-009 parity
    // contract. `atLeast(1)` rather than `times(1)` keeps the assertion
    // resilient to a future refactor that emits the same counter at
    // multiple structural points (e.g. per-block progress reporting).
    verify(readMetrics, Mockito.atLeast(1)).incRemoteBytesRead(anyLong())
  }

  test("read() invokes incRecordsRead on metrics reporter") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val rwc = buildReaderWithContext(readMetrics)

    rwc.reader.read()
    rwc.context.markTaskCompleted(None)

    verify(readMetrics, Mockito.atLeast(1)).incRecordsRead(anyLong())
  }

  test("read() invokes incFetchWaitTime on metrics reporter") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val rwc = buildReaderWithContext(readMetrics)

    rwc.reader.read()
    rwc.context.markTaskCompleted(None)

    verify(readMetrics, Mockito.atLeast(1)).incFetchWaitTime(anyLong())
  }

  test("read() does NOT invoke push-based merge metric methods in v1 (ADR-005 isolation)") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val rwc = buildReaderWithContext(readMetrics)

    rwc.reader.read()
    rwc.context.markTaskCompleted(None)

    // Push-based shuffle (ADR-005) is mutually exclusive with streaming
    // shuffle for an active shuffle. The fallback policy (documented in
    // AAP section 0.7.2) routes push-based-enabled shuffles to the held
    // SortShuffleManager delegate instead of through this reader, so
    // these nine reporter methods MUST remain at zero invocations for
    // every v1 streaming read.
    verify(readMetrics, never()).incRemoteMergedBlocksFetched(anyLong())
    verify(readMetrics, never()).incLocalMergedBlocksFetched(anyLong())
    verify(readMetrics, never()).incRemoteMergedBytesRead(anyLong())
    verify(readMetrics, never()).incLocalMergedBytesRead(anyLong())
    verify(readMetrics, never()).incRemoteMergedChunksFetched(anyLong())
    verify(readMetrics, never()).incLocalMergedChunksFetched(anyLong())
    verify(readMetrics, never()).incCorruptMergedBlockChunks(anyLong())
    verify(readMetrics, never()).incMergedFetchFallbackCount(anyLong())
    verify(readMetrics, never()).incRemoteMergedReqsDuration(anyLong())
  }

  // ==========================================================================
  // Group 3: TaskContext lifecycle integration
  //
  // The reader MUST register a task-completion listener via
  // `context.addTaskCompletionListener` at the start of read(). This listener
  // is what flushes the three F-009 counters at task end &mdash; if the
  // registration is missing, the Stages page reports zero bytes/records for
  // every streaming reduce task, violating F-009 parity.
  // ==========================================================================

  test("read() invocation with a fakeTaskContext does not throw") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val reader = buildReader(readMetrics)

    noException must be thrownBy reader.read()
  }

  test("reader registers a task completion listener") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val context = mock(classOf[TaskContext])
    val reader = new StreamingShuffleReader[Int, Int](
      shuffleHandle, 0, Int.MaxValue, 0, numPartitions,
      context, standardConf(), readMetrics, new StreamingShuffleMetrics())

    reader.read()

    // The reader calls `context.addTaskCompletionListener[Unit] { _ => ... }`
    // &mdash; the Scala-closure variant declared at `TaskContext.scala:141`
    // that internally delegates to the abstract `addTaskCompletionListener
    // (listener: TaskCompletionListener)` method. Because `context` is a
    // mock, the concrete parameterized overload is intercepted and does
    // NOT call through to the abstract one. We therefore verify the
    // `[U]`-parameterized overload directly.
    //
    // Scala 2.13's SAM (Single Abstract Method) conversion makes a plain
    // `TaskContext => Unit` argument eligible for both overloads &mdash;
    // it can be adapted to `TaskCompletionListener` (the abstract
    // overload) or consumed directly by the parametric overload. We
    // therefore apply an explicit `[Unit]` type argument to force the
    // parametric overload at compile time; the typed matcher
    // `any[TaskContext => Unit]()` then provides the runtime argument.
    verify(context, Mockito.atLeast(1))
      .addTaskCompletionListener[Unit](ArgumentMatchers.any[TaskContext => Unit]())
  }

  // ==========================================================================
  // Group 4: Constructor robustness
  //
  // The reader's constructor exposes a 9-parameter surface. These tests pin
  // the surface shape and validate the two edge cases the AAP section 0.5.1.1
  // calls out explicitly:
  //   - A null `streamingMetrics` parameter must not cause NPE in read().
  //   - Non-trivial map-index and reduce-partition ranges must construct
  //     successfully and read() must not throw under those ranges.
  // ==========================================================================

  test("constructor accepts all 9 parameters including streamingMetrics") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val streamingMetrics = new StreamingShuffleMetrics()
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val reader = new StreamingShuffleReader[Int, Int](
      shuffleHandle, 0, Int.MaxValue, 0, numPartitions,
      context, standardConf(), readMetrics, streamingMetrics)

    reader must not be null
  }

  test("constructor tolerates null streamingMetrics (read does not NPE)") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val reader = new StreamingShuffleReader[Int, Int](
      shuffleHandle, 0, Int.MaxValue, 0, numPartitions,
      context, standardConf(), readMetrics, null)

    // The reader's `invalidatePartialReads()` path guards every access to
    // `streamingMetrics` with a `streamingMetrics != null` check so the
    // v1 read() path does not NPE even when no metrics source is wired.
    noException must be thrownBy reader.read()
  }

  test("constructor accepts non-trivial startMapIndex / endMapIndex range") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val reader = buildReader(readMetrics, startMap = 3, endMap = 10)

    noException must be thrownBy reader.read()
  }

  test("constructor accepts non-trivial startPartition / endPartition range") {
    val readMetrics = mock(classOf[ShuffleReadMetricsReporter])
    val reader = buildReader(readMetrics, startPart = 2, endPart = 4)

    noException must be thrownBy reader.read()
  }

  // ==========================================================================
  // Group 5: Deferred tests pending transport wiring (v2)
  //
  // These tests are registered as `ignore(...)` so ScalaTest emits them as
  // "pending" rather than skipping silently. Each carries a TODO comment
  // documenting the expected v2 behavior that will promote the test to
  // `test(...)` form once the sibling network/ sub-package completes the
  // transport wiring. MUST NOT be deleted &mdash; they are contract
  // documentation per AAP section 0.5.1.3 "Deferred tests documented as
  // ignore(...)".
  // ==========================================================================

  ignore("producer failure detection via 5s connection timeout increments " +
    "partialReadInvalidations") {
    // TODO (v2): wire into fake StreamingShuffleTransport once v2 lands.
    // See decision-log entry for deferral rationale.
    // Expected behavior: reader detects producer timeout via TCP keepalive;
    // invokes streamingMetrics.incrementPartialReadInvalidations();
    // discards buffered data from failed shuffle attempt.
  }

  ignore("checksum mismatch triggers retransmission request") {
    // TODO (v2): wire into fake StreamingShuffleTransport.
    // Expected: reader validates CRC32C on each block; on mismatch, requests
    // retransmit and increments an internal retry counter (exponential backoff
    // 1 s initial, max 5 attempts per AAP section 0.1.2).
  }

  ignore("partial read invalidation is atomic across all pending block reads") {
    // TODO (v2): ensure AtomicBoolean partialReadInvalidated ensures atomic
    // discard of all pending reads from a failed producer. The reader
    // already exposes `invalidatePartialReads()` and an internal
    // `isPartialReadInvalidated` accessor; v2 must exercise both under
    // concurrent producer-failure injection.
  }
}
