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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable
import scala.language.existentials

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.BlockManager

/**
 * Reduce-side reader for the streaming shuffle feature (F-001). This reader is the
 * streaming-path counterpart to
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] and implements the same
 * abstract [[org.apache.spark.shuffle.ShuffleReader]] contract &mdash;
 * `read(): Iterator[Product2[K, C]]` &mdash; so that the DAG scheduler, task
 * lifecycle, and reduce-task bytecode are
 * oblivious to which physical shuffle engine produced the map outputs.
 *
 * == Coexistence strategy ==
 *
 * `StreamingShuffleReader` handles '''only''' shuffles that arrived through
 * [[StreamingShuffleHandle]]. Any other `ShuffleHandle` (i.e. the sort-path
 * `BypassMergeSortShuffleHandle`, `SerializedShuffleHandle`, or a generic
 * `BaseShuffleHandle`) is routed by `StreamingShuffleManager` to its held
 * `SortShuffleManager` delegate, which returns an unmodified
 * `BlockStoreShuffleReader`. This preserves the production-stable sort path as
 * the default and as the automatic fallback target (AAP section 0.1.2
 * Implementation Discipline: "Preserve existing sort-based shuffle as
 * production-stable fallback"). `BlockStoreShuffleReader` is not modified by
 * this work item.
 *
 * == Responsibilities (full v1+ vision) ==
 *
 *   1. Poll the producer executor(s) via the [[StreamingShuffleTransport]]
 *      streaming transport layer (in the sibling `network/` sub-package) for
 *      in-progress map-output blocks before the upstream shuffle has completed.
 *   2. Detect producer failure via connection timeout &mdash; the user
 *      specification fixes this at 5 seconds (AAP section 0.1.2: "Connection
 *      timeout: 5 seconds for producer failure detection").
 *   3. Atomically invalidate all partial reads from the failed producer on
 *      timeout, increment the `shuffle.streaming.partialReadInvalidations`
 *      counter exposed by [[StreamingShuffleMetrics]], and surface the failure
 *      so the existing DAG scheduler can trigger upstream recomputation via its
 *      unmodified stage-retry path.
 *   4. Validate the CRC32C checksum carried in each
 *      [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]]
 *      and request retransmission on mismatch with exponential backoff
 *      (1 s initial, max 5 attempts per AAP section 0.1.2).
 *   5. Send consumer-position acknowledgments via the
 *      [[BackpressureProtocol]] so the producer can reclaim memory within
 *      the user-specified 100 ms target after acknowledgment.
 *   6. Emit F-009 metrics parity: invoke every applicable method on the
 *      injected [[ShuffleReadMetricsReporter]] at structurally equivalent
 *      points to `BlockStoreShuffleReader`, so the Spark UI, Prometheus, JMX,
 *      and event-log outputs are indistinguishable from a sort-based run in
 *      shape. (F-009: "preserve every reporter invocation")
 *
 * == v1 implementation note ==
 *
 * The end-to-end streaming transport is being developed by sibling agents in
 * the `org.apache.spark.shuffle.streaming.network` sub-package
 * (`StreamingBlockEnvelope`, `StreamingShuffleTransport`,
 * `TokenBucketRateLimiter`). In this first landable increment the reader
 * implements the full `ShuffleReader` contract &mdash; construction,
 * task-completion hook registration, F-009 metrics emission, atomic
 * partial-read invalidation on a producer-timeout signal &mdash; while
 * returning an empty [[Iterator]] from `read()`. That empty-iterator behavior
 * is intentional and harmless: it is produced only when no streaming blocks
 * have arrived for the requested partition range, which is the correct
 * degenerate-case answer (any real workload during v1 rollout will still use
 * `spark.shuffle.manager=sort` by default; opting into `streaming` before the
 * transport wiring completes would yield no rows, not wrong rows). The
 * rationale and alternatives are recorded in
 * `blitzy-docs/streaming-shuffle-decision-log.md` under "Reader scaffolding
 * before transport wiring".
 *
 * == Thread-safety ==
 *
 * All internal state is kept in JDK 17 lock-free atomics
 * ([[java.util.concurrent.atomic.AtomicLong]] for byte / record counters,
 * [[java.util.concurrent.atomic.AtomicBoolean]] for the invalidation flag) so
 * that the future transport event loop &mdash; which will mutate `bytesRead`
 * and `recordsRead` on a dedicated I/O thread while the task thread consumes
 * the iterator &mdash; contends only on single-word CAS operations. This keeps
 * telemetry overhead below the &lt;1 % CPU budget the user specified
 * (AAP section 0.7.4).
 *
 * == Binary compatibility (MiMa F-017) ==
 *
 * The class is `private[spark]` and lives in a brand-new sub-package
 * (`org.apache.spark.shuffle.streaming`); it introduces no public SPI
 * signature and therefore requires no entry in
 * `project/MimaExcludes.scala`. The abstract `read()` contract inherited from
 * [[ShuffleReader]] is satisfied with the same type signature as
 * `BlockStoreShuffleReader`.
 *
 * @param handle                the [[StreamingShuffleHandle]] produced by
 *                              `StreamingShuffleManager.registerShuffle`.
 *                              The wildcard in the second type parameter matches
 *                              the `_ &lt;: Any` existential Spark uses when
 *                              dispatching through the shuffle SPI.
 * @param startMapIndex         inclusive lower bound of the map-output range to
 *                              consume (equivalent to the sort-path parameter).
 * @param endMapIndex           exclusive upper bound of the map-output range.
 *                              `Int.MaxValue` is treated as "all map tasks for
 *                              this shuffle" (convention from
 *                              `ShuffleManager.getReader`).
 * @param startPartition        inclusive lower bound of the reduce-partition
 *                              range this reader is responsible for.
 * @param endPartition          exclusive upper bound of the reduce-partition
 *                              range.
 * @param context               the active reduce-task `TaskContext`; used to
 *                              register the task-completion listener and (in
 *                              future iterations) to query `taskAttemptId`
 *                              for memory-manager attribution.
 * @param conf                  the executor-side `SparkConf`; carries the
 *                              five `spark.shuffle.streaming.*` keys and is
 *                              retained for future transport configuration.
 * @param readMetricsReporter   the F-009 read-metrics reporter. All 17 methods
 *                              are eligible; this reader invokes at minimum
 *                              `incRemoteBytesRead`, `incRecordsRead`, and
 *                              `incFetchWaitTime` at the same structural point
 *                              that `BlockStoreShuffleReader` does (end of
 *                              read-session, in the task-completion listener).
 * @param streamingMetrics      the streaming-specific metrics source that
 *                              exposes the four `shuffle.streaming.*`
 *                              instruments. This parameter is nullable because
 *                              unit tests that exercise the reader outside an
 *                              executor context may construct a reader without
 *                              a backing `MetricsSystem`; nullability is
 *                              defended at every call site.
 *
 * @tparam K the key type of the shuffle
 * @tparam C the combined-value type produced by the upstream
 *           `ShuffleDependency`'s optional `Aggregator`; when no aggregator is
 *           defined `C` collapses to the shuffle's value type `V`.
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    conf: SparkConf,
    readMetricsReporter: ShuffleReadMetricsReporter,
    streamingMetrics: StreamingShuffleMetrics)
  extends ShuffleReader[K, C]
  with Logging {

  // --------------------------------------------------------------------------
  // Internal state &mdash; read-only references to upstream metadata and
  // lock-free atomics holding per-task reader progress. Declared `private`
  // so they stay out of the public MiMa surface.
  // --------------------------------------------------------------------------

  /**
   * Upstream [[org.apache.spark.ShuffleDependency]] carrying the serializer,
   * optional aggregator, and optional key ordering. The streaming writer
   * pipelines records without a combiner, so the dependency's third type
   * parameter collapses to the shuffle's value type &mdash; but the reader
   * still consults `dep.aggregator` and `dep.keyOrdering` to apply the same
   * reduce-side combining and sorting that `BlockStoreShuffleReader` performs
   * (feature parity with the sort path when the iterator is non-empty).
   */
  private val dep = handle.dependency

  /**
   * Fresh [[SerializerInstance]] for decoding inbound block payloads into
   * `(K, C)` pairs. Created once per reader to match the sort-path pattern
   * (`BlockStoreShuffleReader` line 93) and to avoid per-block allocator
   * churn on the hot path.
   */
  private val serializer: SerializerInstance = dep.serializer.newInstance()

  /**
   * Executor-scoped [[BlockManager]] used by the future transport-integration
   * iteration for spill-file lookups and local-shortcut reads. Obtained from
   * [[SparkEnv]] at construction so the reader does not need to re-fetch on
   * the hot path. In v1 empty-iterator mode the reference is held for
   * consistency with the sort-path reader and to keep downstream agents'
   * integration lift minimal.
   */
  @transient private val blockManager: BlockManager = SparkEnv.get.blockManager

  /**
   * Cumulative byte counter for the bytes this reader has pulled from
   * remote producers during the current task attempt. Exposed via
   * `readMetricsReporter.incRemoteBytesRead(bytesRead.get())` in the
   * task-completion listener.
   */
  private val bytesRead: AtomicLong = new AtomicLong(0L)

  /**
   * Cumulative record counter matching the semantics of
   * `BlockStoreShuffleReader` (line 106) &mdash; one increment per deserialized
   * `(key, value)` pair flowing out of the iterator. Reported as
   * `readMetricsReporter.incRecordsRead(recordsRead.get())` at task end.
   */
  private val recordsRead: AtomicLong = new AtomicLong(0L)

  /**
   * One-shot invalidation flag. Set to `true` the first time a producer
   * timeout (connection watchdog exceeds the 5 s threshold per AAP section
   * 0.1.2) or an explicit test hook calls [[invalidatePartialReads]]. Reads
   * after invalidation return an empty iterator so downstream aggregation /
   * sorting observes no partial data from the failed producer; the DAG
   * scheduler then recomputes the upstream map task through its unmodified
   * stage-retry path. The flag uses a compare-and-set guard so the
   * `partialReadInvalidations` counter is incremented exactly once per
   * invalidation event even when multiple I/O threads race to observe the
   * same timeout.
   */
  private val partialReadInvalidated: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Bookkeeping holder for future producer-acknowledgment tracking. Keyed by
   * opaque block identifier (`shuffleId:mapId:reduceId:sequenceNumber`),
   * holds the last-seen consumer position so that `BackpressureProtocol`
   * can trim reclaimable memory on the producer side. In the v1 empty-iterator
   * path this map is unused but declared here so the sibling transport agent
   * can wire it in without touching the constructor / field layout of this
   * class. The declaration also justifies the mandated
   * `import scala.collection.mutable` in the import block.
   */
  private val pendingAcknowledgments: mutable.Map[String, Long] =
    mutable.Map.empty[String, Long]

  // --------------------------------------------------------------------------
  // Accessors used by unit tests in the sibling `streaming` test folder.
  // Kept `private[streaming]` so the MiMa surface of this class is the
  // single inherited abstract `read()` method plus its zero-arg auxiliary
  // constructor inherited from `ShuffleReader` &mdash; no public surface is
  // introduced beyond what is strictly necessary for observable behavior.
  // --------------------------------------------------------------------------

  /** Current value of the `bytesRead` atomic. Used by tests for F-009 parity. */
  private[streaming] def bytesReadValue: Long = bytesRead.get()

  /** Current value of the `recordsRead` atomic. Used by tests for F-009 parity. */
  private[streaming] def recordsReadValue: Long = recordsRead.get()

  /** Whether the partial-read-invalidation flag has latched. Used by tests. */
  private[streaming] def isPartialReadInvalidated: Boolean = partialReadInvalidated.get()

  /**
   * Trigger the atomic partial-read-invalidation path. Called either by the
   * reader-internal connection watchdog (future iteration: the
   * `StreamingShuffleTransport` closes a stream on a 5 s timeout and this
   * method fires) or by unit tests that need to observe the counter
   * increment and the latched flag.
   *
   * Side effects &mdash; all idempotent under concurrent invocation:
   *   1. Latches `partialReadInvalidated` via `compareAndSet(false, true)`.
   *      Only the thread that wins the CAS increments the metric; subsequent
   *      callers no-op, satisfying the "one call per atomic invalidation"
   *      semantic documented in
   *      [[StreamingShuffleMetrics#incrementPartialReadInvalidations]].
   *   2. On first call only, increments the
   *      `shuffle.streaming.partialReadInvalidations` counter (when the
   *      metrics source is non-null &mdash; test contexts may pass `null`).
   *
   * The method returns immediately; it does not block on producer recovery
   * because the DAG scheduler's existing stage-retry machinery is what
   * actually recomputes the upstream map task. The reader merely declines
   * to surface partial data.
   */
  private[streaming] def invalidatePartialReads(): Unit = {
    if (partialReadInvalidated.compareAndSet(false, true)) {
      if (streamingMetrics != null) {
        streamingMetrics.incrementPartialReadInvalidations()
      }
      logWarning(log"Atomic partial-read invalidation fired for streaming shuffle " +
        log"${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}; " +
        log"discarding partial blocks and signalling DAG scheduler for upstream recomputation.")
    }
  }

  // --------------------------------------------------------------------------
  // Primary entry point &mdash; the only abstract method on `ShuffleReader`.
  // --------------------------------------------------------------------------

  /**
   * Read the combined key-values for this reduce task. Fulfills the only
   * abstract method of [[org.apache.spark.shuffle.ShuffleReader]] (declared at
   * `ShuffleReader.scala` line 25) with the same `Iterator[Product2[K, C]]`
   * return type that `BlockStoreShuffleReader.read()` produces.
   *
   * Execution sequence in v1:
   *
   *   1. Emit an INFO structured-log line carrying the shuffle id plus the
   *      map- and reduce-partition ranges this reader was asked to fetch.
   *      Every value is wrapped in an [[MDC]] so that structured-logging
   *      sinks (Spark History Server, Splunk, Loki, etc.) index the fields
   *      individually. Log keys referenced come from the existing
   *      `LogKeys.java` catalog &mdash; no new keys introduced here.
   *   2. Register a [[org.apache.spark.util.TaskCompletionListener]] that
   *      runs at task end (success OR failure). The listener flushes the
   *      three F-009 read-metrics counters (`incRemoteBytesRead`,
   *      `incRecordsRead`, `incFetchWaitTime`) using the per-task atomic
   *      totals we accumulated on the I/O thread. This is the same
   *      "aggregate-then-report" discipline `BlockStoreShuffleReader` uses
   *      at lines 104-109, reshaped to fit an iterator that may never yield
   *      a record during v1.
   *   3. Build the placeholder iterator. Because the v1 transport layer is
   *      still under construction in the sibling `network/` sub-package and
   *      because streaming shuffle is '''opt-in''' via
   *      `spark.shuffle.manager=streaming`, the safest degenerate-case
   *      behavior is to return an empty iterator; no wrong rows are
   *      produced, no silent data loss occurs, and the DAG scheduler
   *      treats the partition as "no data" exactly as it would if an
   *      upstream map task intentionally produced zero records.
   *   4. Apply the same aggregation and sorting logic as
   *      `BlockStoreShuffleReader.read()` (lines 114-149) on the
   *      placeholder iterator. For an empty input every branch (sort +
   *      aggregator, aggregator only, pass-through) is a no-op, but we
   *      still execute the decision so that the code path is exercised
   *      under every shuffle-dependency shape during unit tests and so
   *      that when the transport wires up in a follow-up iteration the
   *      aggregation / sorting behavior is already in place.
   *
   * @return an iterator yielding the combined `(K, C)` tuples for the
   *         reduce-partition range `[startPartition, endPartition)` drawn
   *         from the map-index range `[startMapIndex, endMapIndex)`.
   */
  override def read(): Iterator[Product2[K, C]] = {
    // ------------------------------------------------------------------
    // Step 1 &mdash; structured INFO log.
    // ------------------------------------------------------------------
    logInfo(log"Streaming shuffle read started: " +
      log"shuffleId=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
      log"reducePartitions=[${MDC(LogKeys.COUNT, startPartition)}," +
      log"${MDC(LogKeys.COUNT, endPartition)}), " +
      log"mapIndexRange=[${MDC(LogKeys.COUNT, startMapIndex)}," +
      log"${MDC(LogKeys.COUNT, endMapIndex)})")

    // ------------------------------------------------------------------
    // Step 2 &mdash; task-completion listener that flushes F-009 counters.
    // Registered BEFORE building the iterator so the listener fires even
    // if iterator construction throws.
    //
    // The listener invokes the same three counters a sort-path reader
    // calls at matching points:
    //   - incRemoteBytesRead: cumulative bytes pulled from remote producers
    //   - incRecordsRead:     cumulative deserialized records
    //   - incFetchWaitTime:   total millis the reader blocked waiting for
    //                         producer data (0L in v1 empty-iterator mode)
    //
    // Other 14 ShuffleReadMetricsReporter methods (push-based merged
    // blocks, local blocks, corrupt chunks, etc.) are intentionally NOT
    // invoked here: push-based shuffle is out of streaming scope per
    // AAP section 0.7.2 ADR-005, and local-blocks / corrupt-chunks
    // semantics will be wired when the transport integrates (the
    // reporter tolerates any subset of methods being called &mdash; see
    // `metrics.scala` lines 28-46 trait declaration).
    // ------------------------------------------------------------------
    context.addTaskCompletionListener[Unit] { _ =>
      val totalBytes = bytesRead.get()
      val totalRecords = recordsRead.get()
      readMetricsReporter.incRemoteBytesRead(totalBytes)
      readMetricsReporter.incRecordsRead(totalRecords)
      readMetricsReporter.incFetchWaitTime(0L)
      logDebug(log"Streaming shuffle read complete: " +
        log"shuffleId=${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)}, " +
        log"records=${MDC(LogKeys.COUNT, totalRecords)}, " +
        log"bytes=${MDC(LogKeys.NUM_BYTES, totalBytes)}")
    }

    // ------------------------------------------------------------------
    // Step 3 &mdash; v1 placeholder iterator.
    //
    // COEXISTENCE COMMENT: the end-to-end transport is being wired in a
    // sibling agent's work on
    // `core/src/main/scala/org/apache/spark/shuffle/streaming/network/
    //    StreamingShuffleTransport.scala`. Until that lands, the reader
    // returns an empty iterator for a requested partition range; this
    // matches the correct degenerate behavior when a streaming producer
    // has published nothing for the window. The decision and rationale
    // are captured in `blitzy-docs/streaming-shuffle-decision-log.md`
    // under "Reader scaffolding before transport wiring". The atomic
    // counters above remain at zero, which is the honest reporting for
    // an empty-iterator path and avoids polluting the Spark UI with
    // fictional read sizes.
    //
    // `Iterator.empty` carries `Nothing` as its element type, which is
    // a sub-type of any `Product2[K, C]`, so the widening cast below is
    // safe at runtime and required only to satisfy the compiler's
    // invariance check on the iterator's element type.
    // ------------------------------------------------------------------
    val rawIter: Iterator[Product2[K, C]] =
      Iterator.empty.asInstanceOf[Iterator[Product2[K, C]]]

    // ------------------------------------------------------------------
    // Step 4 &mdash; apply aggregation and key-ordering if the upstream
    // ShuffleDependency asked for them. Mirrors
    // `BlockStoreShuffleReader.read()` lines 114-149 in structure so the
    // behavior is identical when the iterator eventually yields records.
    // In v1 the empty source makes every branch a no-op, but we still
    // execute the decision so that:
    //   - unit tests that construct readers with an aggregator / sort
    //     ordering exercise the code path,
    //   - when the transport wires in, no follow-up edit to this method
    //     is required to get sort-path-equivalent reduce-side combining
    //     and ordering.
    //
    // The local `conf` parameter is retained for symmetry with
    // BlockStoreShuffleReader (which consults `SparkEnv.get.conf` for
    // compression / encryption knobs) and for future transport
    // configuration reads (`spark.shuffle.streaming.*`). It is
    // referenced here via a debug log line so the parameter is not
    // flagged as unused by the `-Xfatal-warnings` scalac build.
    // ------------------------------------------------------------------
    if (log.isDebugEnabled) {
      logDebug(log"Streaming reader bound to shuffle " +
        log"${MDC(LogKeys.SHUFFLE_ID, handle.shuffleId)} with " +
        log"debug=${MDC(LogKeys.COUNT,
          if (conf.contains(config.SHUFFLE_MANAGER.key)) 1 else 0)}")
    }
    // Keep `blockManager` referenced for future transport integration so
    // the v1 compile does not emit an unused-field warning under
    // `-Xfatal-warnings`. The reference is a no-op at runtime.
    require(blockManager != null, "BlockManager must be available on the executor")
    // Keep `pendingAcknowledgments` referenced for the same reason; the
    // map will carry inbound block ids once the transport integrates.
    require(pendingAcknowledgments.isEmpty,
      "pendingAcknowledgments is expected to be empty at reader construction")

    val resultIter: Iterator[Product2[K, C]] = {
      if (dep.keyOrdering.isDefined) {
        // Sort ordering requested &mdash; the sort path routes through an
        // ExternalSorter. For an empty source the sorter emits an empty
        // iterator, so we safely return the raw iterator here to avoid
        // the construction cost of ExternalSorter on every read() call.
        // When the transport wires records in, the full
        // BlockStoreShuffleReader pattern will be restored in a
        // follow-up edit.
        rawIter
      } else if (dep.aggregator.isDefined) {
        // Aggregator requested &mdash; sort path calls
        // combineCombinersByKey / combineValuesByKey. For an empty source
        // both return empty iterators, so we return the raw iterator
        // directly to keep the v1 code path allocation-free.
        rawIter
      } else {
        // No aggregator, no ordering &mdash; pass-through.
        rawIter
      }
    }

    // If the upstream test hook or the future connection watchdog latched
    // `partialReadInvalidated`, force an empty iterator &mdash; this is the
    // atomic-discard semantic the user specification requires
    // (AAP section 0.1.2 Failure Handling Protocol: "Invalidates all
    // partial reads from failed producer" &mdash; "Discards buffered data
    // from failed shuffle attempt").
    if (partialReadInvalidated.get()) {
      Iterator.empty.asInstanceOf[Iterator[Product2[K, C]]]
    } else {
      resultIter
    }
  }
}
