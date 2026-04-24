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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Opt-in streaming [[org.apache.spark.shuffle.ShuffleManager]] introduced as feature F-001 of the
 * Apache Spark 4.2 streaming shuffle proposal. This is the primary SPI entry point that Spark's
 * reflection-based factory ([[org.apache.spark.util.Utils#instantiateSerializerOrShuffleManager]])
 * instantiates at [[org.apache.spark.SparkEnv]] construction time when the operator sets
 * `spark.shuffle.manager=streaming`. When any other value is bound &mdash; including the default
 * `sort` or `tungsten-sort` &mdash; this class is never loaded and the sort-path
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] runs exactly as it did before this feature
 * was introduced.
 *
 * == Coexistence strategy ==
 *
 * `StreamingShuffleManager` COEXISTS with `SortShuffleManager` rather than replacing it:
 *
 *   1. A DELEGATE [[SortShuffleManager]] instance is held as `fallbackManager` for the entire
 *      lifetime of this class. The delegate is constructed once, at our own construction time,
 *      with the same [[SparkConf]] that Spark bound at [[SparkEnv]] creation. The sort path
 *      continues to own its existing [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] and
 *      therefore preserves ADR-002 (atomic metadata commit via
 *      `IndexShuffleBlockResolver.writeMetadataFileAndCommit`) for every shuffle whose blocks
 *      end up on disk &mdash; whether that disk landing is caused by sort-mode writes or by
 *      streaming-mode spills persisted through [[org.apache.spark.storage.BlockManager]]
 *      (AAP section 0.7.2).
 *   2. Per-shuffle fallback routing is evaluated exactly once per
 *      [[ShuffleDependency]] at `registerShuffle` time via
 *      [[StreamingShuffleFallbackPolicy#evaluate]]. When the policy returns `Some(reason)`, the
 *      `shuffleId -> reason` mapping is recorded in the concurrent `fallbackShuffles` map and
 *      the call is forwarded to `fallbackManager.registerShuffle`, which produces a
 *      sort-path handle. Subsequent `getWriter`, `getReader`, and `unregisterShuffle` calls on
 *      that shuffle type-match on the non-[[StreamingShuffleHandle]] and delegate to the
 *      sort-path manager. The DAG scheduler, task lifecycle, and user-facing APIs (RDD /
 *      DataFrame / Dataset) are never touched &mdash; the zero-touch invariants (AAP sections
 *      0.6.2 and 0.7.1) are preserved exactly.
 *   3. When the policy returns `None`, a new [[StreamingShuffleHandle]] is created and returned;
 *      a subsequent `getWriter` / `getReader` for that shuffle constructs a streaming-mode
 *      writer or reader. Any non-[[StreamingShuffleHandle]] that arrives at `getWriter` or
 *      `getReader` is automatically dispatched to the sort-path delegate without inspecting the
 *      fallback map, because the handle type alone is authoritative (the handle was minted by
 *      whichever manager actually registered the shuffle).
 *
 * == Lifecycle ==
 *
 *   - Construction: invoked on both the driver and every executor because
 *     [[org.apache.spark.SparkEnv#initializeShuffleManager]] calls
 *     [[org.apache.spark.shuffle.ShuffleManager#create]] with the driver/executor context
 *     determined by [[SparkContext#DRIVER_IDENTIFIER]]. The [[StreamingShuffleMetrics]]
 *     source is registered with [[org.apache.spark.metrics.MetricsSystem]] on every non-null
 *     [[SparkEnv]] (QA checkpoint 6 Issue #4 fix). The prior driver-only exclusion caused
 *     local / local-cluster mode (where driver and executor share a JVM) to miss
 *     registration and violated the AAP Observability Rule's "Working in local development
 *     environment" criterion; [[org.apache.spark.metrics.MetricsSystem#registerSource]]
 *     already handles the duplicate-name edge case internally, so universal registration
 *     is safe.
 *   - Stop: invoked by [[SparkEnv#stop]] on shutdown. [[java.util.concurrent.atomic.AtomicBoolean]]
 *     guards idempotency; repeat invocations are no-ops. The delegate `SortShuffleManager.stop()`
 *     is always called so that its [[org.apache.spark.shuffle.IndexShuffleBlockResolver]]
 *     tear-down remains exactly as it was before this feature was introduced.
 *
 * == Binary compatibility (MiMa F-017) ==
 *
 * The class is `private[spark]` and lives in a brand-new sub-package
 * (`org.apache.spark.shuffle.streaming`); it introduces no public SPI signature and therefore
 * requires no entry in `project/MimaExcludes.scala`. The constructor signature `(SparkConf)`
 * matches the second-preference constructor that
 * [[org.apache.spark.util.Utils#instantiateSerializerOrShuffleManager]] looks up by reflection
 * (see the Utils implementation: `(SparkConf, Boolean)` first, then `(SparkConf)`, then `()`),
 * keeping the instantiation shape symmetric with [[SortShuffleManager]] itself.
 *
 * == Thread-safety ==
 *
 * All internal state is either immutable after construction (`fallbackManager`, `blockResolver`,
 * `conf`) or backed by lock-free concurrent primitives
 * ([[java.util.concurrent.ConcurrentHashMap]] for `fallbackShuffles`,
 * [[java.util.concurrent.atomic.AtomicBoolean]] for `stopped`, `@volatile var` for
 * `metricsSource`). This matches the driver-side invocation pattern (multiple
 * DAG-scheduler threads may register shuffles concurrently) and the executor-side invocation
 * pattern (multiple task threads may call `getWriter` / `getReader` simultaneously).
 *
 * @param conf the Spark configuration bound at [[SparkEnv]] construction; carries the five
 *             `spark.shuffle.streaming.*` typed entries (see [[config]]) plus the inherited
 *             `spark.shuffle.manager` selector.
 */
private[spark] class StreamingShuffleManager(conf: SparkConf)
  extends ShuffleManager with Logging {

  // ---------------------------------------------------------------------------
  // Internal state.
  //
  // COEXISTENCE COMMENT: the fields below define the entirety of the bridge
  // between the streaming path and the production-stable sort path. The
  // fallback manager is the authoritative sort-path delegate and is NEVER
  // modified &mdash; we simply consult it for non-streaming shuffles and for
  // our own `stop()` tear-down. This cleanly isolates the streaming logic in
  // a new sub-package while keeping the existing sort-based shuffle exactly
  // as the production-stable fallback target (AAP section 0.1.2 "Preserve
  // existing sort-based shuffle as production-stable fallback").
  // ---------------------------------------------------------------------------

  /**
   * The delegate [[org.apache.spark.shuffle.sort.SortShuffleManager]] held for fallback routing.
   *
   * COEXISTENCE COMMENT: this is the authoritative sort-path delegate &mdash; NEVER modified,
   * consulted on fallback. It is constructed eagerly with the same [[SparkConf]] we received,
   * so that its internal [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] (and every
   * other construction-time resource such as `taskIdMapsForShuffle`) is initialized to the
   * exact state the stock sort-based shuffle would have on this executor. Operators who
   * enabled sort-path-specific tuning (e.g. `spark.shuffle.sort.bypassMergeThreshold`,
   * `spark.shuffle.sort.io.plugin.class`) continue to see their configuration applied when
   * a shuffle falls back.
   */
  private val fallbackManager: SortShuffleManager = new SortShuffleManager(conf)

  /**
   * Records the `shuffleId -> fallback-reason` mapping for every shuffle whose
   * [[StreamingShuffleFallbackPolicy#evaluate]] returned `Some(reason)`. Used strictly for
   * diagnostic logging from [[unregisterShuffle]] and for any future introspection hooks; the
   * authoritative dispatch decision in `getWriter` / `getReader` is handled by pattern-matching
   * on the handle type rather than by a lookup in this map. Backing store is
   * [[java.util.concurrent.ConcurrentHashMap]] because `registerShuffle` / `unregisterShuffle`
   * may be invoked from multiple DAG-scheduler threads on the driver.
   */
  private val fallbackShuffles: ConcurrentHashMap[Int, String] =
    new ConcurrentHashMap[Int, String]()

  /**
   * Streaming shuffle metrics source. Initialized by the bootstrap block below on every
   * non-null [[SparkEnv]] construction (QA checkpoint 6 Issue #4 fix &mdash; previously this
   * initialization was guarded by an executor-only check that broke local / local-cluster
   * mode visibility). Remains `null` only when the manager is constructed under a unit test
   * that explicitly sets `SparkEnv.set(null)`; every downstream consumer (writer, reader,
   * fallback policy) null-guards this reference before use so those tests continue to work.
   *
   * Marked `@volatile` so that publication of the reference from the constructor thread is
   * visible to any subsequent `getWriter` / `getReader` call on a different thread &mdash; the
   * JVM memory model does not guarantee that ordinary writes performed during object
   * construction are visible to threads that did not participate in the construction without
   * this happens-before edge.
   */
  @volatile private var metricsSource: StreamingShuffleMetrics = _

  /**
   * Lock-free idempotency guard on [[stop]]. [[SparkEnv#stop]] may invoke our `stop` through
   * the shutdown hook, while test harnesses (e.g. `SparkContext.stop()` inside a test's
   * `afterAll`) may invoke it a second time; the [[AtomicBoolean#compareAndSet]] idiom
   * guarantees the second call is an observable no-op that neither double-tears-down the
   * delegate nor double-clears the `fallbackShuffles` map.
   */
  private val stopped: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Shared [[org.apache.spark.shuffle.ShuffleBlockResolver]] resolved through the delegate
   * [[SortShuffleManager]]. Because streaming-mode spills flow through
   * [[org.apache.spark.storage.BlockManager#putBytes]] (see [[StreamingShuffleWriter]]) and are
   * re-read by reduce tasks via the same [[BlockManager]] API, the streaming path does not need
   * a parallel resolver &mdash; reusing the sort path's
   * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] preserves ADR-002 (atomic metadata
   * commit via `writeMetadataFileAndCommit`, AAP section 0.7.2) for every block that ever lands
   * on disk.
   *
   * COEXISTENCE COMMENT: the resolver instance returned here is the SAME instance the fallback
   * `SortShuffleManager` exposes. There is exactly one resolver per executor for both paths,
   * which is what ESS, decommission, and migration code paths expect.
   */
  private val blockResolver: ShuffleBlockResolver = fallbackManager.shuffleBlockResolver

  // ---------------------------------------------------------------------------
  // Bootstrap block. Runs at construction time on BOTH driver and executors.
  // Two side effects below:
  //   1. Metrics-source registration (executed on every non-null SparkEnv so
  //      local / local-cluster deployments and the driver's own JMX sinks
  //      both observe the `shuffle.streaming.*` instruments - QA checkpoint 6
  //      Issue #4 fix). The MetricsSystem registry is idempotent against
  //      duplicate names (see MetricsSystem.registerSource, which catches
  //      IllegalArgumentException), so multi-registration within one JVM
  //      degrades to at-most-once registration automatically.
  //   2. Optional log-level elevation for the `org.apache.spark.shuffle.
  //      streaming` logger when `spark.shuffle.streaming.debug=true` (QA
  //      checkpoint 6 Issue #2 fix). Without this wiring the flag would be
  //      purely decorative; AAP IC-17 intent ("enable via
  //      `spark.shuffle.streaming.debug=true`") requires the flag to actually
  //      elevate runtime verbosity.
  // ---------------------------------------------------------------------------

  // Emit a structured INFO record with the five streaming-shuffle configuration values so
  // operators can verify at startup that the manager is bound and that their configuration
  // took effect. The sort-path delegate's class name is also logged to make the coexistence
  // strategy explicit in the executor bootstrap trail. Each of the five streaming config
  // values is carried through its own MDC entry so that structured-logging consumers can
  // filter / aggregate by individual config value. The reads are lifted into local vals so
  // that the literal expression inside each log"..." remains within the 100-char line limit
  // enforced by scalastyle.
  private val bootstrapStreamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)
  private val bootstrapBufferSizePercent: Int =
    conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
  private val bootstrapSpillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val bootstrapMaxBandwidthMBps: Int =
    conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
  private val bootstrapDebugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  logInfo(log"StreamingShuffleManager initialized: " +
    log"class=${MDC(LogKeys.CLASS_NAME, classOf[StreamingShuffleManager].getName)}, " +
    log"fallbackClass=" +
    log"${MDC(LogKeys.OPTIMIZER_CLASS_NAME, classOf[SortShuffleManager].getName)}, " +
    log"spark.shuffle.streaming.enabled=" +
    log"${MDC(LogKeys.CONFIG, bootstrapStreamingEnabled.toString)}, " +
    log"spark.shuffle.streaming.bufferSizePercent=" +
    log"${MDC(LogKeys.CONFIG2, bootstrapBufferSizePercent.toString)}, " +
    log"spark.shuffle.streaming.spillThreshold=" +
    log"${MDC(LogKeys.CONFIG3, bootstrapSpillThreshold.toString)}, " +
    log"spark.shuffle.streaming.maxBandwidthMBps=" +
    log"${MDC(LogKeys.CONFIG4, bootstrapMaxBandwidthMBps.toString)}, " +
    log"spark.shuffle.streaming.debug=" +
    log"${MDC(LogKeys.CONFIG5, bootstrapDebugEnabled.toString)}")

  // ---------------------------------------------------------------------------
  // QA Checkpoint 6 Issue #2 fix: wire `spark.shuffle.streaming.debug=true`
  // to an actual Log4j2 logger-level elevation for the
  // `org.apache.spark.shuffle.streaming` logger. Before this change, the flag
  // was read into `bootstrapDebugEnabled` and included in the INFO line above
  // but did NOT influence runtime verbosity, so operators who set the flag
  // saw no DEBUG output (AAP IC-17 intent contravened). The code path below
  // uses the Log4j2 Configurator API (identical pattern to `Utils.setLogLevel`
  // at core/src/main/scala/org/apache/spark/util/Utils.scala:2334) scoped to
  // the streaming sub-package, so the elevation affects exactly this feature's
  // classes and never the root logger or other Spark subsystems. Failures are
  // swallowed at WARN level because logging misconfiguration must not abort
  // shuffle manager construction - data plane stability always beats
  // observability perfection.
  // ---------------------------------------------------------------------------
  if (bootstrapDebugEnabled) {
    try {
      val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
      val loggerName = "org.apache.spark.shuffle.streaming"
      val loggerConfig = ctx.getConfiguration.getLoggerConfig(loggerName)
      // If there is no dedicated LoggerConfig for our package, getLoggerConfig
      // returns the nearest ancestor (typically the root logger). Mutating the
      // root in place would be a cross-cutting concern; instead, we attach a
      // new additive LoggerConfig at DEBUG level exactly when no
      // package-scoped config pre-exists. This mirrors how operators would
      // express `logger.shuffle_streaming.level=DEBUG` in log4j2.properties.
      if (loggerConfig.getName != loggerName) {
        val newLoggerConfig = new org.apache.logging.log4j.core.config.LoggerConfig(
          loggerName, Level.DEBUG, true)
        ctx.getConfiguration.addLogger(loggerName, newLoggerConfig)
      } else {
        loggerConfig.setLevel(Level.DEBUG)
      }
      ctx.updateLoggers()
      logInfo(log"Streaming shuffle debug logging ENABLED for " +
        log"${MDC(LogKeys.CLASS_NAME, loggerName)} via " +
        log"spark.shuffle.streaming.debug=true")
    } catch {
      // Logging configuration failure is a degraded-observability condition,
      // not a data-plane correctness issue. Swallow and continue with the
      // default INFO level so the shuffle path remains fully functional.
      case t: Throwable =>
        logWarning(log"Failed to elevate log level for " +
          log"org.apache.spark.shuffle.streaming to DEBUG despite " +
          log"spark.shuffle.streaming.debug=true; continuing with default " +
          log"log level. Operators can work around this by configuring " +
          log"log4j2 directly (logger.shuffle_streaming.level=DEBUG).", t)
    }
  }

  // ---------------------------------------------------------------------------
  // QA Checkpoint 6 Issue #4 fix: auto-register the Dropwizard metrics source
  // with `SparkEnv.get.metricsSystem` on EVERY non-null-SparkEnv construction
  // (both driver and executor). The four shuffle.streaming.* instruments then
  // become visible in every configured sink (JMX, Prometheus, Graphite, CSV,
  // Slf4jSink) without requiring operators to hand-wire registration.
  //
  // Rationale for removing the prior driver-only exclusion (see git history):
  //   - Local / local-cluster mode collapses driver and executor into one
  //     JVM; the prior guard's `executorId != DRIVER_IDENTIFIER` check
  //     skipped registration in that topology and broke the AAP Observability
  //     Rule's "Working in local development environment" success criterion
  //     (AAP section 0.7.7). QA checkpoint 6 verified this regression
  //     empirically (`getSourcesByName("shuffle.streaming")` returned count=0
  //     immediately after SparkEnv bootstrap).
  //   - Duplicate-registration risk is already handled by
  //     `MetricsSystem.registerSource` itself: that method catches
  //     IllegalArgumentException raised by the CodaHale MetricRegistry when
  //     two sources share a name (core/src/main/scala/org/apache/spark/
  //     metrics/MetricsSystem.scala:168-170), so we cannot corrupt the
  //     registry by registering twice.
  //   - In cluster mode, the driver JVM has its own MetricsSystem instance
  //     (separate from every executor JVM), so there is no cross-JVM name
  //     collision to worry about either.
  //
  // The null guard on `SparkEnv.get` is retained so that unit tests that
  // construct the manager without a `SparkEnv` (e.g. the Pure Mockito tests
  // in `StreamingShuffleManagerSuite`) continue to work.
  // ---------------------------------------------------------------------------
  if (SparkEnv.get != null) {
    try {
      metricsSource = new StreamingShuffleMetrics()
      SparkEnv.get.metricsSystem.registerSource(metricsSource)
      logInfo(log"StreamingShuffleMetrics registered with the MetricsSystem " +
        log"for ${MDC(LogKeys.EXECUTOR_ID, SparkEnv.get.executorId)}")
    } catch {
      // Telemetry registration failure must not abort the shuffle manager construction.
      // AAP section 0.7.4: "Telemetry overhead limited to <1% CPU utilization" &mdash; and the
      // corollary is that telemetry failures must not take the shuffle path down with them.
      // Losing metrics is degraded observability, not data loss; the streaming path remains
      // fully functional and the metrics reference is left null so that every downstream
      // caller that uses it continues to null-guard correctly.
      case t: Throwable =>
        logWarning(log"Failed to register StreamingShuffleMetrics; streaming shuffle will " +
          log"continue without the shuffle.streaming.* instruments. " +
          log"executor=${MDC(LogKeys.EXECUTOR_ID, SparkEnv.get.executorId)}", t)
        metricsSource = null
    }
  }

  // ---------------------------------------------------------------------------
  // ShuffleManager trait method overrides. All six abstract members from
  // core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala
  // (lines 30-99) are implemented below: registerShuffle, getWriter,
  // getReader, unregisterShuffle, shuffleBlockResolver, stop. The final
  // two-arg getReader wrapper on the trait (lines 61-68) delegates to our
  // seven-arg override automatically and does not need to be re-implemented.
  // ---------------------------------------------------------------------------

  /**
   * Registers a shuffle with this manager and returns an opaque
   * [[org.apache.spark.shuffle.ShuffleHandle]] for the DAG scheduler to attach to every
   * downstream task.
   *
   * Flow:
   *   1. Validate the partition count. The streaming writer does not use
   *      [[org.apache.spark.shuffle.sort.PackedRecordPointer]]'s 24-bit partition-id encoding
   *      (there is no sort-mode serialized buffer), so the 16,777,216 cap in
   *      [[org.apache.spark.shuffle.sort.SortShuffleManager]] line 204 does not mechanically
   *      apply. AAP section 0.7.4 nonetheless requires an explicit upper bound
   *      (`Int.MaxValue / 2`) so that per-partition array allocations in the streaming writer
   *      (`partitionLengths`, `buffers`) do not risk integer overflow in downstream arithmetic.
   *      Violations raise a [[SparkException]] rather than silently producing undefined
   *      behavior.
   *   2. Consult [[StreamingShuffleFallbackPolicy]]. A `Some(reason)` result routes this
   *      shuffle to the sort path for the rest of its lifetime; a `None` result commits to
   *      streaming.
   *   3. On fallback: record the `shuffleId -> reason` mapping, emit a structured INFO log,
   *      and delegate to [[SortShuffleManager#registerShuffle]]. The returned handle is a
   *      sort-path handle type (one of `BypassMergeSortShuffleHandle`,
   *      `SerializedShuffleHandle`, or a bare [[org.apache.spark.shuffle.BaseShuffleHandle]])
   *      and will be dispatched accordingly by `getWriter` / `getReader`.
   *   4. On streaming: emit a structured INFO log and return a new [[StreamingShuffleHandle]].
   *
   * @param shuffleId  the unique shuffle identifier assigned by the DAG scheduler
   * @param dependency the [[ShuffleDependency]] produced by the upstream RDD transformation
   * @tparam K the key type of the shuffle
   * @tparam V the value type of the shuffle
   * @tparam C the combiner type of the shuffle (collapses to `V` for the streaming path;
   *           see [[StreamingShuffleHandle]] for the rationale)
   * @return an opaque handle to pass to downstream tasks
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Explicit partition-count sanity guard. The sort path's 16,777,216 cap comes from
    // PackedRecordPointer's 24-bit partition-id bit-field; streaming shuffle does not
    // encode partition ids into pointers, but unbounded partition counts would still break
    // downstream integer arithmetic in the writer. AAP section 0.7.4.
    val numPartitions = dependency.partitioner.numPartitions
    if (numPartitions > Int.MaxValue / 2) {
      throw new SparkException(
        s"StreamingShuffleManager does not support shuffles with more than " +
          s"${Int.MaxValue / 2} partitions (requested: $numPartitions)")
    }

    // COEXISTENCE COMMENT: the fallback policy is evaluated here, at registration time, for
    // every shuffle. When it returns `Some(reason)` the call is delegated to the held
    // SortShuffleManager for the entire lifetime of this shuffle, which preserves the
    // DAG-scheduler and user-facing API invariants from AAP section 0.7.1 Implementation
    // Discipline: "Preserve existing sort-based shuffle as production-stable fallback. Never
    // modify DAG scheduler, task lifecycle, or user-facing APIs."
    val fallbackReason =
      StreamingShuffleFallbackPolicy.evaluate(shuffleId, dependency, conf, metricsSource)

    fallbackReason match {
      case Some(reason) =>
        // Record the fallback decision so unregisterShuffle can later surface a DEBUG line
        // tagged with the reason and so introspection tooling can classify shuffles.
        fallbackShuffles.put(shuffleId, reason)
        // QA Checkpoint 6 Issue #1 (IC-15 log volume) fix: this per-shuffle log is
        // emitted at DEBUG level rather than INFO. StreamingShuffleFallbackPolicy.fallback
        // already emits a first-seen INFO per unique reason and a per-shuffle DEBUG for
        // subsequent occurrences; emitting a second INFO here would double the per-shuffle
        // log volume and push saturated workloads over the AAP IC-15 `<10 MB/hr` budget.
        // Operators who want per-shuffle visibility enable
        // `spark.shuffle.streaming.debug=true`, which elevates the
        // `org.apache.spark.shuffle.streaming` logger level to DEBUG and restores both
        // the policy's per-shuffle DEBUG line and this DEBUG line to the log stream.
        logDebug(log"Routing shuffle ${MDC(SHUFFLE_ID, shuffleId)} to sort-based fallback: " +
          log"${MDC(REASON, reason)}")
        // Delegate to the held SortShuffleManager. The returned handle is a sort-path type
        // and will be dispatched to fallbackManager by getWriter / getReader via type-match.
        fallbackManager.registerShuffle(shuffleId, dependency)
      case None =>
        // Streaming path selected: construct a StreamingShuffleHandle.
        // The `asInstanceOf[ShuffleDependency[K, V, V]]` cast mirrors SortShuffleManager's
        // handle construction at lines 99-100 and 103-104; the third type parameter collapses
        // from C to V because StreamingShuffleHandle does not carry a combiner type (streaming
        // pipelines records without map-side aggregation).
        //
        // QA Checkpoint 6 Issue #1 (IC-15 log volume) fix: this per-shuffle log is emitted
        // at DEBUG level rather than INFO. Under saturated shuffle rates (>=5 shuffles/sec)
        // the original INFO emission contributed ~1/3 of the 43.88 MB/hr overflow measured
        // by QA. Aggregate observability is preserved through the four
        // `shuffle.streaming.*` Dropwizard counters (gauge + 3 counters exposed via JMX and
        // Prometheus), which carry the same per-shuffle signal in a bounded-volume form;
        // operators who need the line-per-shuffle signal enable
        // `spark.shuffle.streaming.debug=true`.
        logDebug(log"Streaming path active for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} with " +
          log"${MDC(NUM_PARTITIONS, numPartitions)} partitions")
        new StreamingShuffleHandle[K, V](
          shuffleId,
          dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    }
  }

  /**
   * Constructs a [[org.apache.spark.shuffle.ShuffleWriter]] for a specific map task.
   *
   * Dispatch strategy:
   *   - A [[StreamingShuffleHandle]] produced by our own `registerShuffle` when the fallback
   *     policy returned `None` selects [[StreamingShuffleWriter]].
   *   - Any other handle (sort-path types like
   *     [[org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle]],
   *     [[org.apache.spark.shuffle.sort.SerializedShuffleHandle]], or a bare
   *     [[org.apache.spark.shuffle.BaseShuffleHandle]]) is handed to
   *     [[SortShuffleManager#getWriter]] verbatim. The sort-path writer choice within that
   *     delegate is authoritative &mdash; we do not second-guess it.
   *
   * @param handle   the shuffle handle returned by [[registerShuffle]] (possibly via the
   *                 fallback delegate)
   * @param mapId    the unique map-task identifier assigned by the DAG scheduler
   * @param context  the active [[TaskContext]]
   * @param metrics  the F-009 write-metrics reporter
   * @tparam K the key type of the shuffle
   * @tparam V the value type of the shuffle
   * @return a writer appropriate for the handle's routing decision
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    // COEXISTENCE COMMENT: the type-match below is the single dispatch point between the
    // streaming writer and the sort-path delegate. Any non-StreamingShuffleHandle is, by
    // construction, a handle the fallbackManager minted during its own registerShuffle call,
    // so we simply forward the writer request to it. This preserves the sort path's existing
    // three-way dispatch (unsafe / bypass-merge / base) byte-for-byte.
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked] =>
        new StreamingShuffleWriter[K, V](
          streamingHandle, mapId, context, conf, metrics, metricsSource)
      case _ =>
        fallbackManager.getWriter[K, V](handle, mapId, context, metrics)
    }
  }

  /**
   * Constructs a [[org.apache.spark.shuffle.ShuffleReader]] that reads a specific reduce
   * partition range `[startPartition, endPartition)` drawn from a specific map-index range
   * `[startMapIndex, endMapIndex)`.
   *
   * Dispatch strategy mirrors [[getWriter]]: a [[StreamingShuffleHandle]] selects the
   * streaming reader, any other handle is forwarded to [[SortShuffleManager#getReader]].
   *
   * @param handle          the shuffle handle returned by [[registerShuffle]]
   * @param startMapIndex   inclusive lower bound of the map-output range
   * @param endMapIndex     exclusive upper bound of the map-output range
   *                        (`Int.MaxValue` means "all map tasks for this shuffle")
   * @param startPartition  inclusive lower bound of the reduce-partition range
   * @param endPartition    exclusive upper bound of the reduce-partition range
   * @param context         the active reduce [[TaskContext]]
   * @param metrics         the F-009 read-metrics reporter
   * @tparam K the key type of the shuffle
   * @tparam C the combined-value type (collapses to `V` on the streaming path)
   * @return a reader appropriate for the handle's routing decision
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // COEXISTENCE COMMENT: identical dispatch discipline to getWriter. A StreamingShuffleHandle
    // always came from our own registerShuffle while the fallback policy permitted streaming;
    // any other handle came from fallbackManager.registerShuffle and must be forwarded back to
    // it so that the sort path's getReader can construct a BlockStoreShuffleReader against the
    // sort-path metadata.
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, _] =>
        new StreamingShuffleReader[K, C](
          streamingHandle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          conf,
          metrics,
          metricsSource)
      case _ =>
        fallbackManager.getReader[K, C](
          handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Removes a shuffle's metadata from this manager.
   *
   * Because the delegate [[SortShuffleManager]] owns the shared
   * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] (via `blockResolver`), its
   * `unregisterShuffle` is ALWAYS invoked first so that any on-disk files produced by a
   * fallback shuffle &mdash; or by streaming-mode spills persisted to the resolver &mdash; are
   * cleaned up through the existing sort-path tear-down code. The Boolean it returns is
   * authoritative and forms our own return value.
   *
   * Streaming-specific bookkeeping (the per-shuffle `fallbackShuffles` entry) is removed
   * next. Per-task buffers are not tracked here because they live inside the writer / reader
   * instances and are torn down deterministically in those classes' own lifecycle
   * (`StreamingShuffleWriter.stop`, the reader's task-completion listener).
   *
   * @param shuffleId the shuffle identifier to unregister
   * @return the delegate's `unregisterShuffle` result, which is authoritative
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // Delegate first so that the shared IndexShuffleBlockResolver (owned by fallbackManager)
    // is able to remove on-disk files for every shuffleId, regardless of whether this shuffle
    // was routed to streaming or to the sort fallback. For streaming-mode shuffles that
    // spilled to disk, the spill files live under the same ShuffleBlockId namespace as sort
    // blocks (see StreamingShuffleWriter.maybeSpillPartition), so delegating is the correct
    // and minimal integration point.
    val result = fallbackManager.unregisterShuffle(shuffleId)

    // Drop the fallback-reason bookkeeping. remove() tolerates a missing key (returns null)
    // so we do not need to pre-check containsKey; this keeps the path branch-free for the
    // common case where the shuffle was streaming-routed and therefore never added to the map.
    val previousReason = fallbackShuffles.remove(shuffleId)
    if (previousReason != null) {
      logDebug(log"Unregistered fallback-routed streaming shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} (fallback reason was " +
        log"${MDC(REASON, previousReason)})")
    } else {
      logDebug(log"Unregistered streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
    }

    result
  }

  /**
   * Returns the [[ShuffleBlockResolver]] shared with the delegate [[SortShuffleManager]].
   *
   * COEXISTENCE COMMENT: this is the SAME resolver instance [[SortShuffleManager]] exposes,
   * so the [[org.apache.spark.shuffle.IndexShuffleBlockResolver#writeMetadataFileAndCommit]]
   * atomic-commit guarantee (ADR-002, AAP section 0.7.2) applies unchanged for every block
   * that lands on disk &mdash; whether it came through a sort-path writer or a streaming-mode
   * spill persisted via [[org.apache.spark.storage.BlockManager#putBytes]]. No parallel
   * resolver is introduced, no new [[ShuffleBlockResolver]] trait implementation is needed.
   *
   * @return the resolver used by both streaming-mode spills and sort-path writes on this
   *         executor
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = blockResolver

  /**
   * Shuts down this manager.
   *
   * Semantics:
   *   - Idempotent: the [[AtomicBoolean]] `stopped` guard ensures the second (and subsequent)
   *     invocation is an observable no-op. Two-phase tear-down (SparkEnv shutdown hook plus
   *     test-harness `SparkContext.stop()`) is the norm, not the exception.
   *   - The delegate [[SortShuffleManager#stop]] is always called first so that its
   *     [[IndexShuffleBlockResolver]] release runs through the same code path that the stock
   *     sort-path shutdown would trigger. Any exception raised by the delegate is logged at
   *     WARN and swallowed so that our own tear-down still clears the streaming-specific
   *     bookkeeping.
   *   - The per-shuffle bookkeeping map is cleared so that the backing array can be garbage
   *     collected promptly (AAP section 0.1.2 "Zero memory leaks under failure scenarios").
   */
  override def stop(): Unit = {
    // Idempotency guard: a second stop (e.g. from SparkEnv shutdown + test-harness SparkContext
    // stop) returns immediately without double-releasing the delegate or double-clearing
    // bookkeeping. compareAndSet returns true only on the first call.
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    logInfo(log"Stopping StreamingShuffleManager")

    try {
      // Always stop the delegate so its IndexShuffleBlockResolver tears down just as it would
      // in a stock sort-based deployment. A throw here is logged but does not prevent us from
      // clearing our own bookkeeping; shutdown must always reach a terminal state.
      try {
        fallbackManager.stop()
      } catch {
        case t: Throwable =>
          logWarning(log"Failed to stop the sort-based fallback manager cleanly; " +
            log"continuing with streaming-shuffle shutdown", t)
      }
    } finally {
      // Clear the per-shuffle bookkeeping whether or not the delegate stop succeeded. This
      // releases any retained String references (fallback reason codes) so the backing array
      // becomes eligible for GC promptly.
      fallbackShuffles.clear()
      logInfo(log"StreamingShuffleManager stopped")
    }
  }
}
