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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS, STREAMING_SHUFFLE_SPILL_THRESHOLD}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{BaseShuffleHandle, MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId, ShuffleMergedBlockId}

// scalastyle:off classforname
/**
 * Streaming shuffle manager -- opt-in alternative to
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] that pipelines map-side data
 * directly to reduce-side consumers with in-memory buffering, backpressure control,
 * and graceful disk-spill fallback.
 *
 * == Coexistence Strategy ==
 * This manager is selected ONLY when the user explicitly opts in via either:
 *   - `spark.shuffle.manager=streaming` (short name registered in
 *     [[org.apache.spark.shuffle.ShuffleManager.getShuffleManagerClassName]]), or
 *   - `spark.shuffle.manager=org.apache.spark.shuffle.streaming.StreamingShuffleManager`
 *     (FQCN fallback).
 *
 * The default `spark.shuffle.manager=sort` continues to use
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] unchanged -- byte-for-byte identical
 * wire format, file format, and metric emission. The existing `SortShuffleManager`
 * implementation is NOT modified.
 *
 * == Automatic Fallback ==
 * For automatic fallback when streaming-shuffle preconditions are not met (slow
 * consumer, memory pressure, network saturation, or producer/consumer version
 * mismatch), this manager holds a private [[SortShuffleManager]] collaborator.
 * Fallback is governed by [[StreamingShuffleFallbackPolicy]] and is transparent
 * to user code.
 *
 * In addition to the policy-driven streaming-vs-sort decision for streaming-aware
 * handles, this manager unconditionally delegates to the inner [[SortShuffleManager]]
 * for any non-[[StreamingShuffleHandle]] handle types it observes. This safety net
 * preserves correctness for two operationally important edge cases:
 *   - Stage recomputation after a job-level configuration change in which a previous
 *     attempt registered a `BaseShuffleHandle` (or one of its sort-shuffle subclasses)
 *     before the user enabled streaming. Because `ShuffleHandle` is `Serializable` and
 *     travels with the task definition, those legacy handles can still arrive at the
 *     streaming manager's `getWriter`/`getReader` callbacks during a retry.
 *   - Defensive interoperability with a hypothetical future scheduler that mixes
 *     handles from multiple registered managers within the same `SparkEnv` lifetime.
 *
 * == Reflective Instantiation Contract ==
 * This class exposes a public two-argument `(SparkConf, Boolean)` constructor matching
 * the contract of [[org.apache.spark.util.Utils.instantiateSerializerOrShuffleManager]].
 * The class declaration is intentionally NOT annotated `private[spark]` so the
 * reflective `Class.forName(...).getConstructor(...)` lookup succeeds without depending
 * on the Scala-compiler-generated `private[spark]` access flags. The class is loaded by
 * `SparkEnv` during `SparkEnv.create` when the configured `spark.shuffle.manager` value
 * resolves to this class via the short-name alias map or the FQCN fallback.
 *
 * == Driver-vs-Executor Construction ==
 * `SparkEnv` constructs one [[ShuffleManager]] on the driver and one on each
 * executor. In `local` and `local-cluster` deploy modes the driver and executor
 * share a JVM, so the SAME [[ShuffleManager]] instance both registers shuffles
 * (driver responsibility) and serves [[getWriter]] / [[getReader]] calls
 * (executor responsibility). To support all deploy modes uniformly, the
 * data-plane collaborators ([[BackpressureProtocol]], [[MemorySpillManager]],
 * [[StreamingShuffleFallbackPolicy]]) are wrapped in `lazy val Option`s gated
 * on `SparkEnv.get != null` rather than on `isDriver`. This mirrors the
 * [[SortShuffleManager]] pattern (which always instantiates its
 * `IndexShuffleBlockResolver` regardless of `isDriver`), avoids the local-mode
 * `IllegalStateException` that would arise from `if (isDriver) None`, and remains
 * test-safe via the `SparkEnv.get` null check used throughout `core`.
 *
 * Lazy materialization is critical: [[org.apache.spark.SparkContext]] calls
 * `_env.initializeShuffleManager()` (which constructs this manager) BEFORE
 * `_env.initializeMemoryManager(...)` is invoked (see [[org.apache.spark.SparkContext]]
 * lines 596-597). If the collaborators were eager `val`s, `env.memoryManager`
 * would be `null` at construction time and downstream calls into
 * [[MemorySpillManager]] / [[StreamingShuffleFallbackPolicy]] would throw NPE
 * during the first task. Deferring capture until first SPI access guarantees
 * the [[org.apache.spark.SparkEnv]] is fully wired. The lazy
 * [[SortShuffleManager]] collaborator additionally avoids eager
 * `IndexShuffleBlockResolver` setup until first delegation.
 *
 * == Metric Registration ==
 * Whenever `SparkEnv.get` and `SparkEnv.get.metricsSystem` are non-null, the
 * constructor registers a [[StreamingShuffleSource]] with the existing
 * [[org.apache.spark.metrics.MetricsSystem]]. JMX, CSV, Slf4j, Graphite, Prometheus,
 * and Web-UI sinks already configured in the host application automatically pick up
 * the four streaming-shuffle metrics with no schema changes elsewhere -- satisfying
 * the streaming-shuffle observability rule. Registration is gated only on the
 * MetricsSystem's availability rather than on `isDriver` so that local-mode
 * applications also expose streaming-shuffle telemetry.
 *
 * == Thread Safety ==
 * All public methods are safe to invoke concurrently from multiple task threads on an
 * executor. Per-shuffle bookkeeping uses a [[ConcurrentHashMap]] for lock-free
 * register/unregister; pattern-matching dispatch is stateless; the inner
 * [[SortShuffleManager]] is itself thread-safe per its existing contract.
 *
 * @param conf SparkConf used for configuration reads and lazy `SortShuffleManager`
 *             instantiation
 * @param isDriver true if this manager is created on the driver, false on executors;
 *                 retained for the reflective two-argument constructor contract of
 *                 [[org.apache.spark.util.Utils.instantiateSerializerOrShuffleManager]]
 *                 and surfaced via the diagnostic startup log line. Component
 *                 instantiation is gated on `SparkEnv.get` availability rather than
 *                 on `isDriver` because `local` mode passes `isDriver = true` while
 *                 still operating the data plane in the same JVM.
 */
// scalastyle:on classforname
class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  // -------------------------------------------------------------------------------
  // Collaborators
  // -------------------------------------------------------------------------------

  /**
   * Held privately for fallback delegation when streaming-shuffle preconditions are not
   * met, AND for handling non-streaming `ShuffleHandle` types (e.g.,
   * `BypassMergeSortShuffleHandle`, `SerializedShuffleHandle`, `BaseShuffleHandle`)
   * that should always go through the sort path.
   *
   * Lazy so that manager instances which never delegate to the sort path (e.g.,
   * cluster-mode driver-only managers, or executor managers whose first job
   * exclusively uses streaming-shuffle handles without fallback) avoid the cost of
   * eagerly initializing the [[SortShuffleManager]]'s `IndexShuffleBlockResolver`.
   * The field force-initializes on the first `getWriter`/`getReader` call against
   * a non-streaming handle, on the first fallback delegation, on the first call to
   * [[shuffleBlockResolver]], on the first call to [[unregisterShuffle]], or
   * during [[stop]] (whichever comes first).
   */
  private lazy val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  /**
   * Streaming-shuffle-specific metrics published through the existing `MetricsSystem`.
   * The four counters/gauges are: `bufferUtilizationPercent`, `spillCount`,
   * `backpressureEvents`, `partialReadInvalidations`. Always instantiated (on both
   * driver and executors) because the metric set itself is lightweight -- four
   * Dropwizard primitives plus an `AtomicInteger` -- and the executor's metric source
   * registration below holds a reference to it.
   */
  private val streamingMetrics: StreamingShuffleMetrics = new StreamingShuffleMetrics()

  /**
   * Cached value of `spark.shuffle.streaming.debug` resolved once at manager
   * construction time. Honors the AAP Section 0.1.2 user directive *"Configuration
   * changes require executor restart (no dynamic reconfiguration in v1)"* by
   * snapshotting the flag at startup and threading the resolved Boolean down to
   * each [[StreamingShuffleWriter]] and [[StreamingShuffleReader]] via constructor
   * parameters.
   *
   * Per AAP Section 0.1.2 user directive *"Debug logging disabled by default
   * (enable via `spark.shuffle.streaming.debug=true`)"* and the AAP Section 0.7.2.5
   * quality budget *"Log volume capped at <10MB/hour per executor for streaming
   * events"*, this flag controls source-site emission of streaming-shuffle DEBUG
   * and TRACE log statements: when `false`, those log calls are short-circuited
   * before any [[org.apache.spark.internal.MDC]] field expansion or string
   * interpolation occurs, eliminating their CPU and log-volume overhead. WARN and
   * ERROR statements are intentionally NOT subject to this gate -- they pass
   * through freely so operators retain visibility into actionable failure
   * conditions regardless of debug-flag state.
   *
   * The value is re-read by individual collaborators
   * ([[BackpressureProtocol]], [[MemorySpillManager]]) which take a [[SparkConf]]
   * directly and call the [[org.apache.spark.shuffle.streaming.streamingDebugEnabled]]
   * helper themselves; capturing it once here keeps the
   * [[StreamingShuffleWriter]] and [[StreamingShuffleReader]] constructor
   * signatures `SparkConf`-free per their existing per-task constructor contract.
   */
  private val debugEnabled: Boolean = streamingDebugEnabled(conf)

  /**
   * Heartbeat-based flow control with token-bucket rate limiting and priority
   * arbitration.
   *
   * Materialized lazily on first access (during [[registerShuffle]],
   * [[getWriter]], [[getReader]], [[unregisterShuffle]], or [[stop]]). The
   * lazy-val gating works uniformly across all deploy modes:
   *   - In cluster modes (yarn/k8s executors), `SparkEnv` is fully constructed
   *     before the executor begins serving tasks.
   *   - In `local` and `local-cluster` modes, the driver JVM also hosts the
   *     executor that calls [[getWriter]] / [[getReader]]; the same lazy access
   *     occurs after [[SparkContext]] finishes initializing both
   *     `_shuffleManager` and `_memoryManager` (see [[SparkContext]] lines 596-597).
   *   - In synthetic tests where `SparkEnv.get` is null at this manager's
   *     construction time, the lazy access still runs at first use.
   *
   * Lazy materialization is REQUIRED here (not just stylistic) because
   * [[org.apache.spark.SparkEnv.initializeShuffleManager]] (which constructs
   * this manager) runs BEFORE [[org.apache.spark.SparkEnv.initializeMemoryManager]]
   * in [[org.apache.spark.SparkContext]] driver setup -- if these `Option`s were
   * eager `val`s, `env.memoryManager` would be `null` at construction time and
   * downstream calls into the materialized collaborators would throw NPE.
   *
   * The [[BackpressureProtocol]] constructor starts daemon scheduler threads for
   * periodic refill and heartbeat scanning; they idle when no shuffle is active
   * and stop cleanly via [[stop]]. Although `BackpressureProtocol` itself does
   * not depend on `memoryManager`, its initialization is laid out lazily for
   * consistency with the other two collaborator fields below.
   */
  private lazy val backpressureOpt: Option[BackpressureProtocol] =
    if (SparkEnv.get != null) Some(new BackpressureProtocol(streamingMetrics, conf)) else None

  /**
   * Polls memory utilization and spills oldest LRU partition buffers to disk via
   * [[org.apache.spark.storage.BlockManager]] when utilization exceeds the configured
   * `spillThreshold`.
   *
   * Materialized lazily so that `env.memoryManager` and `env.blockManager` are
   * fully initialized by the time this collaborator captures them. If we
   * materialized eagerly in the manager constructor body, `env.memoryManager`
   * would be `null` because [[org.apache.spark.SparkEnv.initializeMemoryManager]]
   * runs AFTER [[org.apache.spark.SparkEnv.initializeShuffleManager]] (see
   * [[org.apache.spark.SparkContext]] lines 596-597). Lazy initialization defers
   * the field-capture until first use -- typically inside [[getWriter]] -- by
   * which point both `_memoryManager` and `_blockManager` are guaranteed
   * non-null on driver and executor SparkEnvs alike.
   *
   * In synthetic test harnesses without a `SparkEnv` (where `SparkEnv.get` is
   * `null` at first access), the option is `None`; downstream [[getWriter]] calls
   * will throw [[IllegalStateException]] because the streaming write path
   * requires a live spill manager.
   */
  private lazy val spillManagerOpt: Option[MemorySpillManager] = {
    val env = SparkEnv.get
    if (env != null) {
      Some(new MemorySpillManager(env.blockManager, env.memoryManager, streamingMetrics, conf))
    } else {
      // Test path where SparkEnv.get returns null. Skip spill-manager instantiation.
      None
    }
  }

  /**
   * Decision class evaluating fallback conditions per the streaming-shuffle
   * specification: slow consumer (>60 s sustained 2x slower), memory pressure
   * (executionMemoryUsed > 95% of capacity), network saturation (cumulative
   * backpressure events above threshold), and producer/consumer Spark-version
   * mismatch.
   *
   * Materialized lazily for the same reason as [[spillManagerOpt]]: the policy
   * captures `env.memoryManager` for memory-pressure introspection, and that
   * field is `null` when this manager's constructor body executes (see
   * [[org.apache.spark.SparkContext]] lines 596-597).
   *
   * In test harnesses without `SparkEnv`, the field is `None` and the streaming
   * path is preferred (no fallback decision is taken), since neither memory
   * pressure nor network saturation can be meaningfully evaluated without the
   * executor runtime.
   */
  private lazy val fallbackPolicyOpt: Option[StreamingShuffleFallbackPolicy] = {
    val env = SparkEnv.get
    if (env != null) {
      Some(new StreamingShuffleFallbackPolicy(conf, env.memoryManager))
    } else {
      None
    }
  }

  /**
   * Set of streaming-shuffle IDs that have been registered through THIS manager (vs.
   * any non-streaming handle that may have been routed to the inner
   * [[SortShuffleManager]] via the legacy-handle dispatch in [[getWriter]]).
   *
   * The tracking enables [[unregisterShuffle]] to correctly distinguish
   * streaming-managed shuffles for proper cleanup. [[ConcurrentHashMap]] ensures
   * concurrent-access safety from multiple task threads invoking the SPI methods on
   * executors.
   *
   * Map values are `java.lang.Boolean.TRUE` sentinels -- the map is used as a
   * concurrent set. A dedicated `Set` API is not used because the JDK's
   * `java.util.concurrent` package does not provide a `ConcurrentSet` and the
   * `Collections.newSetFromMap(new ConcurrentHashMap[K, java.lang.Boolean]())` idiom
   * is no clearer than this direct map usage at the call sites here.
   */
  private val streamingShuffleIds = new ConcurrentHashMap[Integer, java.lang.Boolean]()

  // -------------------------------------------------------------------------------
  // Construction-time side effects
  // -------------------------------------------------------------------------------

  // Register the streaming metric source with the existing MetricsSystem so the
  // configured JMX/CSV/Slf4j/Graphite/Prometheus/Web-UI sinks pick up the four
  // streaming metrics automatically. Gated on `SparkEnv.get != null` (and a
  // non-null metricsSystem) -- not on `isDriver` -- so registration succeeds in
  // `local` / `local-cluster` mode where the driver JVM hosts the executor that
  // operates the data plane. Per the user directive *"Document all integration
  // points with clear comments explaining coexistence strategy."* this is the
  // streaming-shuffle subsystem's sole touch point with the existing
  // MetricsSystem -- the source itself lives in the streaming subpackage and has
  // no influence on any other registered source.
  {
    val env = SparkEnv.get
    if (env != null && env.metricsSystem != null) {
      try {
        env.metricsSystem.registerSource(new StreamingShuffleSource(streamingMetrics))
        logInfo("Registered StreamingShuffleSource with MetricsSystem")
      } catch {
        case e: Exception =>
          // Non-fatal: metrics registration failure should not prevent shuffle from
          // working. Operators will lose visibility into streaming-shuffle telemetry
          // for this executor but the data plane remains functional.
          logWarning("Failed to register StreamingShuffleSource", e)
      }
    }
  }

  logInfo(s"StreamingShuffleManager initialized (isDriver=$isDriver)")

  // -------------------------------------------------------------------------------
  // ShuffleManager SPI implementation
  // -------------------------------------------------------------------------------

  /**
   * Register a streaming-aware [[ShuffleHandle]]. The handle carries configuration
   * metadata (buffer-size percent, spill threshold, max bandwidth) read from
   * [[SparkConf]] at registration time so that subsequent writer/reader instantiation
   * does not re-read configuration on every task -- in keeping with the user
   * directive *"Configuration changes require executor restart (no dynamic
   * reconfiguration in v1)"*.
   *
   * Streaming-shuffle handles are eagerly registered with the executor-side
   * [[BackpressureProtocol]] (when present) so that the protocol's token-bucket
   * refill divisor reflects the count of distinct currently-active shuffles. The
   * registration is idempotent: the protocol's `registerShuffle` is a no-op if the
   * shuffle is already registered.
   *
   * @param shuffleId  shuffle identifier from
   *                   [[org.apache.spark.scheduler.DAGScheduler]]
   * @param dependency shuffle dependency carrying serializer, partitioner,
   *                   aggregator, and other shuffle metadata
   * @return a streaming-aware [[ShuffleHandle]] subtype carrying the captured
   *         configuration metadata
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val bufferSizePercent = conf.get(STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)
    val spillThreshold = conf.get(STREAMING_SHUFFLE_SPILL_THRESHOLD)
    val maxBandwidthMBps = conf.get(STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS)
    streamingShuffleIds.put(Integer.valueOf(shuffleId), java.lang.Boolean.TRUE)
    backpressureOpt.foreach(_.registerShuffle(shuffleId))
    logInfo(
      s"Registering streaming shuffle $shuffleId " +
        s"(bufferSizePercent=$bufferSizePercent, " +
        s"spillThreshold=$spillThreshold, " +
        s"maxBandwidthMBps=$maxBandwidthMBps)")
    new StreamingShuffleHandle[K, V, C](
      shuffleId, dependency, bufferSizePercent, spillThreshold, maxBandwidthMBps)
  }

  /**
   * Get a writer for the given shuffle handle. Pattern-matches on the handle type:
   *   - [[StreamingShuffleHandle]] -> consult the
   *     [[StreamingShuffleFallbackPolicy]]; if streaming OK, return a
   *     [[StreamingShuffleWriter]]; otherwise delegate to the inner
   *     [[SortShuffleManager]] using a fresh [[BaseShuffleHandle]] adapter so the
   *     sort manager observes a handle of the type it expects.
   *   - Any other handle type (legacy `BypassMergeSortShuffleHandle`,
   *     `SerializedShuffleHandle`, `BaseShuffleHandle`) -> always delegate to
   *     [[SortShuffleManager]] without policy consultation.
   *
   * The `@unchecked` annotations on type parameters mirror the existing pattern in
   * [[org.apache.spark.shuffle.sort.SortShuffleManager.getWriter]] (which uses
   * `case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked]`).
   * They silence Scala 2.13 erasure warnings without affecting runtime semantics --
   * type tags are erased at runtime regardless of the annotation.
   *
   * @param handle  the [[ShuffleHandle]] obtained from [[registerShuffle]] (possibly
   *                on a previous job attempt with a different manager configuration);
   *                MUST be non-null
   * @param mapId   map task identifier (typically `taskAttemptId`)
   * @param context the task's [[TaskContext]] for cancellation, metric merging, and
   *                memory accounting
   * @param metrics single-threaded shuffle-write metrics reporter
   * @return a [[ShuffleWriter]] -- streaming or sort-based depending on the handle
   *         type and fallback-policy decision
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        val shouldFallback = fallbackPolicyOpt.exists { policy =>
          policy.shouldFallback(streamingHandle, streamingMetrics)
        }
        if (shouldFallback) {
          logInfo(
            s"Falling back to sort-based shuffle writer for shuffle " +
              s"${streamingHandle.shuffleId}")
          // Wrap the streaming handle's dependency in a fresh BaseShuffleHandle so the
          // inner SortShuffleManager observes a handle of a type it natively accepts.
          // The streaming-specific fields on `streamingHandle` (bufferSizePercent etc.)
          // are unused on the sort path and are correctly elided here.
          sortShuffleManager.getWriter[K, V](
            new BaseShuffleHandle(streamingHandle.shuffleId, streamingHandle.dependency),
            mapId,
            context,
            metrics)
        } else {
          val env = SparkEnv.get
          new StreamingShuffleWriter[K, V](
            streamingHandle,
            mapId,
            context,
            metrics,
            env.blockManager,
            env.memoryManager,
            backpressureOpt.getOrElse(
              throw new IllegalStateException(
                "BackpressureProtocol not initialized; SparkEnv was unavailable when " +
                  "this StreamingShuffleManager was constructed")),
            spillManagerOpt.getOrElse(
              throw new IllegalStateException(
                "MemorySpillManager not initialized; SparkEnv was unavailable when " +
                  "this StreamingShuffleManager was constructed")),
            streamingMetrics,
            debugEnabled)
        }
      case other =>
        // Legacy handle types: always delegate to SortShuffleManager without policy
        // consultation. This preserves correctness for stage recomputation under a
        // mixed-handle scenario per the class-level coexistence documentation.
        sortShuffleManager.getWriter(other, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for the given shuffle handle. Pattern-matches on the handle type:
   *   - [[StreamingShuffleHandle]] -> consult the
   *     [[StreamingShuffleFallbackPolicy]]; if streaming OK, return a
   *     [[StreamingShuffleReader]]; otherwise delegate to the inner
   *     [[SortShuffleManager]] using a fresh [[BaseShuffleHandle]] adapter.
   *   - Any other handle type -> always delegate to [[SortShuffleManager]] without
   *     policy consultation.
   *
   * @param handle         the [[ShuffleHandle]] obtained from [[registerShuffle]];
   *                       MUST be non-null
   * @param startMapIndex  inclusive lower bound of the map-output range to read
   * @param endMapIndex    exclusive upper bound of the map-output range to read
   *                       (`Int.MaxValue` = "all map outputs", per the SPI contract)
   * @param startPartition inclusive lower bound of the reduce-partition range
   * @param endPartition   exclusive upper bound of the reduce-partition range
   * @param context        the task's [[TaskContext]] for cancellation, metric
   *                       merging, and serializer-manager access
   * @param metrics        single-threaded shuffle-read metrics reporter
   * @return a [[ShuffleReader]] -- streaming or sort-based depending on the handle
   *         type and fallback-policy decision
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, _, C @unchecked] =>
        val shouldFallback = fallbackPolicyOpt.exists { policy =>
          policy.shouldFallback(streamingHandle, streamingMetrics)
        }
        if (shouldFallback) {
          logInfo(
            s"Falling back to sort-based shuffle reader for shuffle " +
              s"${streamingHandle.shuffleId}")
          sortShuffleManager.getReader[K, C](
            new BaseShuffleHandle(streamingHandle.shuffleId, streamingHandle.dependency),
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition,
            context,
            metrics)
        } else {
          val env = SparkEnv.get
          new StreamingShuffleReader[K, C](
            streamingHandle,
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition,
            context,
            metrics,
            env.blockManager,
            env.mapOutputTracker,
            streamingMetrics,
            debugEnabled)
        }
      case other =>
        sortShuffleManager.getReader(
          other, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Remove a shuffle's metadata from this manager. Cleans up streaming-shuffle
   * internal state (the `streamingShuffleIds` set and the
   * [[BackpressureProtocol]]'s active-shuffle set) and ALSO delegates to the inner
   * [[SortShuffleManager]] so any shuffles routed through fallback are cleaned up.
   *
   * The dual-cleanup is necessary because a single `shuffleId` may have produced
   * outputs through either or both paths during its lifetime: the writer for the
   * first map task may have been a [[StreamingShuffleWriter]], while a subsequent
   * fallback-triggered retry may have used the inner sort manager. Cleaning both
   * paths guarantees no leaked resources regardless of which path produced output.
   *
   * @param shuffleId the shuffle identifier
   * @return `true` if streaming-shuffle metadata was removed for this id, OR if the
   *         inner sort manager removed metadata for it; `false` only if neither
   *         path knew of the id (which is the documented SPI return for "no
   *         metadata to remove")
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    val wasStreaming = streamingShuffleIds.remove(Integer.valueOf(shuffleId)) != null
    backpressureOpt.foreach(_.unregisterShuffle(shuffleId))
    // Always delegate to the sort manager for safety: shuffles that fell back will
    // have been registered through the sort manager's `getWriter` call path even if
    // their corresponding `registerShuffle` was processed by us, so cleanup is
    // required there too. The `lazy val sortShuffleManager` is only force-evaluated
    // when this delegation actually runs, so manager instances that never touched
    // the data plane do not pay the IndexShuffleBlockResolver setup cost here.
    val sortResult = sortShuffleManager.unregisterShuffle(shuffleId)
    if (wasStreaming) {
      logInfo(s"Unregistered streaming shuffle $shuffleId")
    }
    wasStreaming || sortResult
  }

  /**
   * Return the resolver capable of retrieving shuffle block data based on block
   * coordinates. Returns a [[StreamingShuffleBlockResolver]] -- a lightweight wrapper
   * over the inner [[SortShuffleManager]]'s [[ShuffleBlockResolver]] -- that
   * intercepts shuffle-block lookups for blocks produced by the streaming-shuffle
   * data plane.
   *
   * == Why a Custom Resolver ==
   * The [[StreamingShuffleWriter]] persists each partition's cumulative bytes via
   * `BlockManager.putBytes(ShuffleBlockId(shuffleId, mapId, partitionId), ..., DISK_ONLY)`
   * in [[StreamingShuffleWriter#persistPartitionsForReader]]. The resulting blocks
   * live in the executor's [[org.apache.spark.storage.DiskStore]] under the standard
   * shuffle blockId namespace, but they do NOT have the index/data file layout that
   * the [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] expects (no `.index`
   * sidecar exists for streaming blocks). When the [[StreamingShuffleReader]] issues
   * `BlockManager.blockTransferService.fetchBlockSync(host, port, executorId, blockId.name, ...)`
   * the network-side path eventually resolves through `BlockManager.getLocalBlockData`
   * which dispatches shuffle blockIds to `shuffleManager.shuffleBlockResolver.getBlockData`.
   * Without this custom resolver, the call would land on `IndexShuffleBlockResolver
   * .getBlockData` which would attempt to open the missing `.index` file and throw
   * `NoSuchFileException`.
   *
   * The custom resolver's `getBlockData` therefore checks first if the requested
   * `ShuffleBlockId` lives in the [[org.apache.spark.storage.DiskStore]] (where the
   * streaming writer placed it) and returns a [[NioManagedBuffer]] wrapping the bytes
   * read from `DiskStore.getBytes(blockId).toByteBuffer()`; only if the block is NOT
   * present in the disk store does it delegate to the inner resolver. This ordering
   * correctly handles the case where a streaming-shuffle map task produced output via
   * the streaming path AND the inner resolver might still know about a fallback-
   * produced block under the same blockId (the disk-store entry from the streaming
   * writer takes precedence; if absent, the inner resolver serves whatever sort-path
   * block exists).
   *
   * == MigratableResolver Compliance ==
   * [[BlockManager#migratableResolver]] is a `lazy val` that performs
   * `shuffleManager.shuffleBlockResolver.asInstanceOf[MigratableResolver]`. This cast
   * fires whenever decommissioning workflows or `putBlockDataAsStream` operations run.
   * To avoid `ClassCastException` in those paths, the custom resolver implements
   * [[MigratableResolver]] by delegating every method to the inner resolver
   * (which is `IndexShuffleBlockResolver` for the default sort path, itself a
   * `MigratableResolver`). Streaming-shuffle blocks themselves are not migrated --
   * the streaming path's recovery model relies on producer-failure detection and DAG-
   * scheduler upstream recomputation rather than executor block migration.
   *
   * == Lazy Materialization ==
   * Implemented as a `lazy val` (rather than a `def`) so the resolver instance is
   * constructed exactly once per manager lifetime. Callers (e.g.
   * `BlockManager.migratableResolver`'s lazy val cast) therefore observe a stable
   * resolver reference and any downstream caching of the cast is correct.
   *
   * Per the user directive *"select approach requiring least modification to
   * executor memory model and network transport layer."* this resolver introduces
   * no new disk-storage interface, no new block-id namespace, and no new transport
   * primitive -- it composes existing [[org.apache.spark.storage.BlockManager]] and
   * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] APIs.
   */
  override lazy val shuffleBlockResolver: ShuffleBlockResolver = {
    val env = SparkEnv.get
    if (env != null) {
      new StreamingShuffleBlockResolver(env.blockManager, sortShuffleManager.shuffleBlockResolver)
    } else {
      // Test-harness fallback when SparkEnv is not initialized: hand back the inner
      // resolver directly so callers that exercise this method without a live
      // BlockManager (e.g. unit tests that only validate the SPI surface) still get
      // a non-null resolver.
      sortShuffleManager.shuffleBlockResolver
    }
  }

  /**
   * Shut down this [[ShuffleManager]]: stop streaming components, release resources,
   * and delegate to the inner [[SortShuffleManager]]'s `stop()` to close its
   * `IndexShuffleBlockResolver`.
   *
   * Each step is wrapped in a try/catch so that a failure in one component never
   * prevents subsequent components from being stopped -- this satisfies the
   * resource-leak prevention rule (no retained heap after shutdown) even under
   * pathological executor-shutdown timing.
   *
   * Order of operations:
   *   1. Stop the [[BackpressureProtocol]] (cancels its scheduled tasks and clears
   *      its heartbeat-tracking maps).
   *   2. Stop the [[MemorySpillManager]] (cancels its polling tick, disposes any
   *      remaining buffer references, and clears its registry).
   *   3. Stop the inner [[SortShuffleManager]] (closes its
   *      `IndexShuffleBlockResolver`).
   *   4. Clear the local `streamingShuffleIds` tracking map so any retained
   *      references are released for GC.
   *
   * Steps 1 and 2 are skipped if the corresponding `Option`s are `None` (i.e., this
   * manager was constructed without a live `SparkEnv`, as can happen in synthetic
   * test harnesses). Step 3 is always invoked; if the lazy `sortShuffleManager`
   * field was never force-evaluated, the call will create and immediately stop an
   * idle resolver -- inexpensive and harmless.
   */
  override def stop(): Unit = {
    logInfo("Stopping StreamingShuffleManager")
    // Stop streaming components first. The Options are None when this manager was
    // constructed without a live SparkEnv (synthetic test harnesses); in that case
    // there is nothing to stop and `foreach` is a no-op.
    backpressureOpt.foreach { bp =>
      try bp.stop() catch {
        case e: Exception => logWarning("Error stopping BackpressureProtocol", e)
      }
    }
    spillManagerOpt.foreach { sm =>
      try sm.stop() catch {
        case e: Exception => logWarning("Error stopping MemorySpillManager", e)
      }
    }
    // Then delegate to sort manager (which closes its IndexShuffleBlockResolver).
    try sortShuffleManager.stop() catch {
      case e: Exception => logWarning("Error stopping inner SortShuffleManager", e)
    }
    streamingShuffleIds.clear()
  }

  // -------------------------------------------------------------------------------
  // Custom block resolver for streaming-shuffle blocks
  // -------------------------------------------------------------------------------

  /**
   * Custom [[ShuffleBlockResolver]] that intercepts `getBlockData` lookups for shuffle
   * blocks produced by the streaming-shuffle data plane.
   *
   * == How Streaming Blocks Land Here ==
   * [[StreamingShuffleWriter#persistPartitionsForReader]] persists each partition's
   * cumulative bytes via `BlockManager.putBytes(ShuffleBlockId(shuffleId, mapId,
   * partitionId), bytes, StorageLevel.DISK_ONLY)`. The bytes land in the executor's
   * [[org.apache.spark.storage.DiskStore]] under the standard shuffle blockId
   * namespace. They do NOT have the index/data sidecar layout that
   * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] uses for sort-shuffle
   * blocks.
   *
   * == Lookup Path ==
   * When a [[StreamingShuffleReader]] (running in the same or a remote executor)
   * issues a fetch through `BlockManager.blockTransferService.fetchBlockSync`, the
   * lookup eventually reaches `BlockManager.getLocalBlockData` which dispatches
   * shuffle blockIds to `shuffleManager.shuffleBlockResolver.getBlockData`. This
   * resolver's `getBlockData` therefore:
   *   1. Checks if the [[ShuffleBlockId]] is present in the executor's
   *      [[org.apache.spark.storage.DiskStore]] (the streaming-writer's
   *      `BlockManager.putBytes` route stored bytes there). If yes, returns a
   *      [[NioManagedBuffer]] wrapping `diskStore.getBytes(blockId).toByteBuffer()`.
   *   2. Otherwise, delegates to the inner sort-shuffle resolver. This handles
   *      blocks produced by fallback-driven sort-shuffle writes plus all non-
   *      [[ShuffleBlockId]] subtypes (e.g. push-merged blocks).
   *
   * The disk-store-first check is correct because:
   *   - The streaming writer is the only producer that puts shuffle bytes into the
   *     disk store via `BlockManager.putBytes`. Sort-shuffle writers use the index
   *     shuffle block resolver's index/data file layout, NOT `BlockManager.putBytes`.
   *   - Spilled streaming buffers (from `MemorySpillManager.checkAndSpill`) are also
   *     placed in the disk store under the same `ShuffleBlockId(shuffleId, mapId,
   *     reduceId)` namespace -- but the streaming writer's `persistPartitionsForReader`
   *     calls `BlockManager.removeBlock` first to ensure the cumulative writer-side
   *     bytes (which subsume the spilled bytes) replace the spill before the reader
   *     observes anything. The reader therefore always sees one canonical block per
   *     `(shuffleId, mapId, reduceId)`.
   *
   * == MigratableResolver ==
   * Implements [[MigratableResolver]] by delegating every method to the inner
   * resolver. [[BlockManager#migratableResolver]]'s cast at
   * `shuffleManager.shuffleBlockResolver.asInstanceOf[MigratableResolver]` therefore
   * succeeds without producing a `ClassCastException`. Streaming-shuffle blocks
   * themselves are NOT migrated: the streaming-shuffle recovery model relies on
   * producer-failure detection (consumer-side 5-second connection timeout in
   * [[StreamingShuffleReader]]) and DAG-scheduler upstream recomputation, not on
   * executor block migration. Operators initiating decommissioning of an executor
   * with active streaming-shuffle blocks will observe those blocks lost and
   * recomputed via the standard `FetchFailedException` path -- the same recovery
   * semantic as the sort-shuffle path's lost map outputs. The `MigratableResolver`
   * delegate-to-inner approach is therefore safe even though the inner resolver
   * does not itself know about streaming blocks: streaming blocks are NEVER reported
   * via `getStoredShuffles` and therefore are never migrated.
   *
   * == Thread Safety ==
   * Stateless beyond the immutable `blockManager` and `inner` references; safe to
   * invoke concurrently from multiple block-fetch threads.
   *
   * @param blockManager the executor's [[BlockManager]] -- queried for disk-store
   *                     lookups. Captured at resolver-construction time and held by
   *                     reference; the field is immutable for the resolver's lifetime
   *                     (which matches the [[ShuffleManager]]'s lifetime).
   * @param inner        the wrapped sort-shuffle resolver (an
   *                     [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] for the
   *                     default sort-shuffle implementation). Receives delegations
   *                     for non-streaming block lookups, all `MigratableResolver`
   *                     calls, and `getMergedBlockData` / `getMergedBlockMeta` which
   *                     are not produced by streaming-shuffle.
   */
  private final class StreamingShuffleBlockResolver(
      blockManager: BlockManager,
      inner: ShuffleBlockResolver)
    extends ShuffleBlockResolver with MigratableResolver with Logging {

    /**
     * Lazily resolved [[MigratableResolver]] view of the inner resolver. Cast at
     * construction-time rather than per-call so the [[ClassCastException]] (if any)
     * surfaces eagerly with a clear stack pointing at this class -- in practice the
     * default `IndexShuffleBlockResolver` IS a [[MigratableResolver]] so the cast
     * always succeeds. Held as a lazy val so a `null` inner (defensive against
     * unforeseen test paths) does not NPE during resolver construction.
     */
    private lazy val migratableInner: MigratableResolver = inner match {
      case m: MigratableResolver => m
      case _ =>
        throw new IllegalStateException(
          s"Inner ShuffleBlockResolver ${inner.getClass.getName} does not implement " +
            "MigratableResolver; streaming-shuffle requires the inner sort-shuffle " +
            "resolver to be a MigratableResolver for BlockManager.migratableResolver " +
            "compatibility")
    }

    /**
     * Retrieve the bytes for the specified block. For [[ShuffleBlockId]]s that the
     * streaming writer persisted via `BlockManager.putBytes`, returns a
     * [[NioManagedBuffer]] wrapping the on-disk bytes; for any other blockId or for
     * shuffle blocks that the streaming path did not produce, delegates to the inner
     * resolver.
     *
     * The disk-store check is intentionally done BEFORE the inner-resolver delegation
     * so that streaming-produced blocks short-circuit the sort-shuffle index lookup.
     * For shuffle blocks NOT in the disk store (i.e., produced by sort-shuffle
     * fallback during the same job lifetime), the inner resolver's index/data file
     * layout is used -- this preserves correctness for mixed-mode jobs.
     *
     * @param blockId the requested block identifier
     * @param dirs    optional directories override (passed through to inner resolver
     *                when delegating; ignored for streaming-disk-store lookups since
     *                `DiskStore` reads from the [[org.apache.spark.storage.DiskBlockManager]]'s
     *                configured local dirs)
     * @return a [[ManagedBuffer]] backed by the streaming disk-store entry or by the
     *         inner resolver's response
     */
    override def getBlockData(
        blockId: BlockId,
        dirs: Option[Array[String]] = None): ManagedBuffer = {
      blockId match {
        case shuffleBlockId: ShuffleBlockId =>
          // Disk-store-first lookup: the streaming writer's `persistPartitionsForReader`
          // route stores bytes here under `ShuffleBlockId(shuffleId, mapId, reduceId)`
          // via `BlockManager.putBytes(_, _, DISK_ONLY)`. We check the diskStore
          // directly (rather than going through `BlockManager.getLocalBytes`) to avoid
          // re-entering the BlockManager dispatch path.
          if (blockManager.diskStore.contains(shuffleBlockId)) {
            val data = blockManager.diskStore.getBytes(shuffleBlockId)
            if (log.isDebugEnabled) {
              logDebug(
                s"Serving streaming shuffle block ${shuffleBlockId} from diskStore " +
                  s"size=${data.size}")
            }
            // BlockData.toByteBuffer() returns a ByteBuffer with position=0 and
            // limit=size suitable for NioManagedBuffer wrapping. The bytes are
            // memory-mapped (for files larger than `spark.storage.memoryMapThreshold`)
            // or copied into a heap ByteBuffer (for smaller files).
            new NioManagedBuffer(data.toByteBuffer())
          } else {
            // Block not produced by the streaming path. Delegate to the inner
            // resolver, which serves sort-shuffle index/data files for fallback-
            // produced blocks plus the standard sort-shuffle path.
            inner.getBlockData(blockId, dirs)
          }
        case _ =>
          // Non-ShuffleBlockId (e.g. ShuffleMergedBlockId, ShuffleDataBlockId,
          // ShuffleIndexBlockId, ShuffleChecksumBlockId, etc.). Delegate to the
          // inner resolver which knows their disk layout.
          inner.getBlockData(blockId, dirs)
      }
    }

    /**
     * Retrieve a list of [[BlockId]]s for the given shuffle map. Delegates to the
     * inner resolver -- streaming-shuffle does NOT expose its blocks via this API
     * because the External Shuffle Service uses it to clean up shuffle files after
     * an executor exit, and streaming blocks live in the executor-local disk store
     * (cleaned up by [[org.apache.spark.storage.DiskBlockManager]] shutdown hook
     * directly, no ESS coordination needed).
     */
    override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
      inner.getBlocksForShuffle(shuffleId, mapId)
    }

    /**
     * Retrieve merged-shuffle data. Streaming-shuffle does NOT participate in push-
     * based shuffle merging (per AAP scope: push-based shuffle is out of scope for
     * v1), so this method always delegates to the inner resolver which serves the
     * sort-shuffle merged path.
     */
    override def getMergedBlockData(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
      inner.getMergedBlockData(blockId, dirs)
    }

    /**
     * Retrieve merged-shuffle metadata. Mirrors [[getMergedBlockData]] -- streaming-
     * shuffle does not produce merged blocks, so all calls delegate to the inner
     * resolver.
     */
    override def getMergedBlockMeta(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): MergedBlockMeta = {
      inner.getMergedBlockMeta(blockId, dirs)
    }

    /**
     * Stop the resolver. The inner resolver is stopped by
     * [[StreamingShuffleManager#stop]] via the `sortShuffleManager.stop()`
     * delegation; this resolver itself holds no closeable resources beyond the inner
     * reference, so its `stop()` is a no-op. Documented as such so future
     * maintainers do not add cleanup that would double-stop the inner resolver.
     */
    override def stop(): Unit = {
      // No-op: the inner resolver is stopped by sortShuffleManager.stop() which is
      // invoked from StreamingShuffleManager.stop(). Double-stopping would be
      // harmless but unnecessary.
    }

    // ------------------------------------------------------------
    // MigratableResolver delegation
    // ------------------------------------------------------------

    /**
     * Get the shuffle ids that are stored locally. Delegates to the inner resolver
     * because streaming-shuffle blocks are NOT eligible for migration (see the
     * "MigratableResolver" section in the resolver class-level Scaladoc above).
     */
    override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
      migratableInner.getStoredShuffles()
    }

    /**
     * Mark a shuffle that should not be migrated. Delegates to the inner resolver
     * for sort-shuffle compatibility.
     */
    override def addShuffleToSkip(shuffleId: Int): Unit = {
      migratableInner.addShuffleToSkip(shuffleId)
    }

    /**
     * Write a provided shuffle block as a stream. Delegates to the inner resolver.
     * Streaming-shuffle blocks themselves are not received via the migration stream
     * path, so this delegation only ever receives sort-shuffle migration blocks.
     */
    override def putShuffleBlockAsStream(
        blockId: BlockId,
        serializerManager: SerializerManager): StreamCallbackWithID = {
      migratableInner.putShuffleBlockAsStream(blockId, serializerManager)
    }

    /**
     * Get the blocks for migration for a particular shuffle and map. Delegates to
     * the inner resolver. Streaming-shuffle blocks are not enumerated for migration
     * because the streaming-shuffle recovery path uses `FetchFailedException` and
     * DAG-scheduler upstream recomputation rather than block migration.
     */
    override def getMigrationBlocks(
        shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
      migratableInner.getMigrationBlocks(shuffleBlockInfo)
    }
  }
}
