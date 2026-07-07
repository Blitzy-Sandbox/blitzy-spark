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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleManager, ShuffleReader,
  ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.streaming.network.{StreamingShuffleTransport,
  TokenBucketRateLimiter}
import org.apache.spark.util.collection.OpenHashSet

/**
 * A [[ShuffleManager]] implementation that adds an opt-in, memory-buffered ''streaming'' shuffle
 * backend which pipelines map-side output directly to reduce-side consumers, while preserving the
 * production-stable sort-based shuffle as both the default and the fallback.
 *
 * ==Selection==
 * This manager is instantiated by the unchanged `SparkEnv.initializeShuffleManager` ->
 * `ShuffleManager.create` factory when `spark.shuffle.manager=streaming` resolves to this class
 * (through the short-name alias registered in the companion `ShuffleManager` factory). Its
 * constructor signature `(conf: SparkConf, isDriver: Boolean)` is therefore a hard contract with
 * that reflective factory and must not change.
 *
 * ==Coexistence by composition (zero regression)==
 * The manager holds an inner [[org.apache.spark.shuffle.sort.SortShuffleManager]] by composition
 * and delegates to it for EVERY non-streaming handle and EVERY fallback condition. The sort path
 * is never subclassed or modified, so selecting the streaming backend cannot regress the behavior
 * of any shuffle that is not actively streamed. Streaming is engaged for a shuffle only when the
 * dual activation gate holds (`spark.shuffle.manager=streaming` AND
 * `spark.shuffle.streaming.enabled=true`) and the fallback policy does not veto it; in that case
 * `registerShuffle` returns a [[StreamingShuffleHandle]] and `getWriter` / `getReader`
 * pattern-match that concrete type to dispatch to the streaming components. Every other handle
 * falls through to the inner sort manager.
 *
 * ==Local-mode / test safety==
 * Every streaming collaborator (metrics, metrics source, fallback policy, rate limiter,
 * backpressure protocol, spill manager, and transport) is created, and every daemon started, only
 * when a full [[org.apache.spark.SparkEnv]] is present AND streaming is active. A unit test that
 * constructs this manager without a `SparkEnv`, or with streaming disabled, therefore starts no
 * threads, registers no metrics, and binds no endpoint -- it degrades to a thin pass-through over
 * the inner sort manager.
 *
 * ==Executor-only backpressure endpoint==
 * The [[BackpressureRpcEndpoint]] is bound only on executors (never the driver or local mode) so
 * remote consumers can signal flow control; the manager retains the endpoint ref so [[stop]] can
 * unbind it during teardown.
 *
 * ==Isolation==
 * All streaming logic lives in this class and its collaborators inside the
 * `org.apache.spark.shuffle.streaming` package. The only change to existing shuffle code is the
 * one-entry short-name alias in the `ShuffleManager` factory; no streaming code is injected into
 * the sort path, honoring the feature's "zero cross-contamination" discipline.
 *
 * @param conf     the active [[SparkConf]] carrying the resolved `spark.shuffle.*` keys
 * @param isDriver whether this manager is being created in the driver process; gates the
 *                 executor-only backpressure RPC endpoint binding
 */
@Since("4.2.0")
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  // Typed, read-only accessor for the spark.shuffle.streaming.* keys. Always constructed (no env
  // required) because it drives both the activation decision and the informational startup log.
  private val streamingConf = new StreamingShuffleConfig(conf)

  // A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
  // Mirrors SortShuffleManager exactly so unregisterShuffle can clean up streaming shuffles the
  // same way the sort path cleans up its own, and so the streaming block resolver shares one
  // consistent producer-id view for decommission/migration bookkeeping.
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  // COEXISTENCE: all non-streaming handles and every fallback condition delegate to this
  // production-stable SortShuffleManager; the sort path is never modified. Holding it by
  // composition (rather than subclassing) is what guarantees zero regression -- the sort
  // implementation is used verbatim for every shuffle that streaming does not serve.
  private[this] val sortShuffleManager = new SortShuffleManager(conf)

  // Whether the streaming machinery should be wired up. A full SparkEnv must be present (so
  // no-env unit tests and local construction stay clean) AND the dual activation gate must hold
  // (spark.shuffle.manager=streaming AND spark.shuffle.streaming.enabled=true). When false, the
  // manager is a thin pass-through: no collaborator is built, no daemon starts, no metrics source
  // is registered, and every shuffle is delegated to the inner SortShuffleManager.
  private[this] val streamingEnabled: Boolean =
    SparkEnv.get != null && streamingConf.isStreamingActive

  // Streaming telemetry holder (four metrics). Non-null only when streamingEnabled; the metrics
  // source below exports these exact instances through the MetricsSystem.
  private[this] val streamingMetrics: StreamingShuffleMetrics =
    if (streamingEnabled) new StreamingShuffleMetrics() else null

  // metrics.source.Source named "streamingShuffle"; registered with the MetricsSystem in the init
  // block so all configured sinks (JMX, Prometheus, CSV, Slf4j) pick up the metrics automatically.
  private[this] val streamingSource: StreamingShuffleSource =
    if (streamingEnabled) new StreamingShuffleSource(streamingMetrics) else null

  // Four-condition fallback decision engine. Consulted at registration (immediate veto) and by the
  // streaming components at runtime; on any trigger the shuffle is delegated to the sort path.
  private[this] val fallbackPolicy: StreamingShuffleFallbackPolicy =
    if (streamingEnabled) new StreamingShuffleFallbackPolicy(streamingConf, streamingMetrics)
    else null

  // Per-executor, byte-granular rate limiter shared by the backpressure protocol. v1 passes a
  // single concurrent-shuffle share; because the v1 transport is a logging stub, the limiter does
  // not yet gate real wire traffic, but it is wired so the v2 transport path is real and testable.
  private[this] val rateLimiter: TokenBucketRateLimiter =
    if (streamingEnabled) new TokenBucketRateLimiter(streamingConf.maxBandwidthMBps, 1) else null

  // Token-bucket + heartbeat flow-control engine (daemon started in the init block).
  private[this] val backpressure: BackpressureProtocol =
    if (streamingEnabled) new BackpressureProtocol(streamingConf, streamingMetrics, rateLimiter)
    else null

  // Memory-pressure monitor and LRU disk-spill coordinator (daemon started in the init block). The
  // BlockManager is resolved from the live SparkEnv, which is guaranteed present when enabled.
  private[this] val spillManager: MemorySpillManager =
    if (streamingEnabled) {
      new MemorySpillManager(streamingConf, SparkEnv.get.blockManager, streamingMetrics)
    } else {
      null
    }

  // Transport integration (v1 logging-only stub that reuses the executor BlockTransferService). It
  // is stateless and holds no daemon, so it needs no start/stop lifecycle.
  private[this] val transport: StreamingShuffleTransport =
    if (streamingEnabled) new StreamingShuffleTransport(streamingConf) else null

  // An all-clear FallbackStats used to consult the fallback policy at registration time, before any
  // runtime pressure signal exists. Rates and utilizations are zero and the protocol versions
  // match, so this vetoes streaming only on a hard, registration-time condition (e.g. a protocol
  // version mismatch), never on transient runtime pressure.
  private val baselineFallbackStats = FallbackStats(
    consumerRateBytesPerSec = 0.0,
    producerRateBytesPerSec = 0.0,
    sustainedSlowMillis = 0L,
    memoryUtilizationPercent = 0,
    networkUtilizationPercent = 0,
    localProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION,
    remoteProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION)

  // COEXISTENCE: the streaming block resolver holds an inner sort-path IndexShuffleBlockResolver
  // (built from the SAME (conf, blockManager, taskIdMapsForShuffle) triple SortShuffleManager uses)
  // and delegates all decommission/migration and untracked-block reads to it, so shuffle-block
  // migration keeps working unchanged. It is always constructed because the SPI requires a resolver
  // even when streaming is inactive (sort-fallback reads flow through its delegation). The
  // BlockManager may be null in a no-env test; the resolver resolves it lazily in that case.
  override val shuffleBlockResolver =
    new StreamingShuffleBlockResolver(
      conf,
      if (SparkEnv.get != null) SparkEnv.get.blockManager else null,
      taskIdMapsForShuffle)

  // Executor-only backpressure endpoint ref, retained so stop() can unbind it. Null until bound
  // (only on executors, in the init block) and in local/driver/no-env construction.
  private[this] var backpressureEndpointRef: RpcEndpointRef = _

  // Guards stop() so the ordered shutdown runs exactly once (idempotent across repeated calls).
  private[this] val stopped = new AtomicBoolean(false)

  // Honor spark.shuffle.streaming.debug by raising the "org.apache.spark.shuffle.streaming" logger
  // to DEBUG so the diagnostics the streaming components already gate behind the debug flag are
  // actually emitted (see maybeElevateStreamingLogLevel). This runs regardless of whether streaming
  // is active on this node, so an operator who set the flag can capture streaming diagnostics even
  // from a pass-through (sort-fallback) manager.
  maybeElevateStreamingLogLevel()

  // ==============================================================================================
  // Construction side effects: register the metrics source and start the daemons ONLY when the
  // streaming machinery is enabled (SparkEnv present AND streaming active). The RPC endpoint is
  // additionally executor-only. This keeps local-mode and no-env construction free of threads,
  // metrics, and endpoints, satisfying the feature's local-mode-safety requirement.
  // ==============================================================================================
  if (streamingEnabled) {
    // Fail fast on an out-of-range streaming configuration before any daemon starts.
    streamingConf.validate()
    // Register the streaming metrics source so every configured MetricsSystem sink exports the
    // four streaming metrics with no sink-specific wiring (mirrors how DAGSchedulerSource and
    // ExecutorSource register their own telemetry). LIFECYCLE GUARD: MetricsSystem.registerSource
    // appends to its `sources` list BEFORE the underlying Dropwizard MetricRegistry rejects a
    // duplicate metric-set name, so re-registering the same source name (e.g. when more than one
    // StreamingShuffleManager is constructed in the same JVM -- common across tests) would leak a
    // duplicate Source object even though the registry throws. Guard on the source name so
    // registration is idempotent and never leaks; stop() removes exactly this source.
    val metricsSystem = SparkEnv.get.metricsSystem
    if (metricsSystem.getSourcesByName(streamingSource.sourceName).isEmpty) {
      metricsSystem.registerSource(streamingSource)
    }
    // Start the flow-control and spill daemons.
    backpressure.start()
    spillManager.start()
    // COEXISTENCE / executor-only: bind the backpressure endpoint only on executors so remote
    // consumers can signal flow control. In local mode isDriver is true, so nothing is bound (there
    // are no remote peers); the ref is retained for an orderly unbind in stop().
    if (!isDriver) {
      backpressureEndpointRef = SparkEnv.get.rpcEnv.setupEndpoint(
        BackpressureRpcEndpoint.ENDPOINT_NAME,
        new BackpressureRpcEndpoint(SparkEnv.get.rpcEnv, backpressure))
    }
  }

  logInfo(s"StreamingShuffleManager initialized (isDriver=$isDriver): streaming shuffle is " +
    s"${if (streamingConf.isStreamingActive) "ACTIVE" else "INACTIVE"} " +
    s"(spark.shuffle.manager=${streamingConf.shuffleManager}, " +
    s"spark.shuffle.streaming.enabled=${streamingConf.enabled}). Non-streaming handles and all " +
    s"fallback conditions delegate to the inner SortShuffleManager.")

  /**
   * Whether the given (about-to-register) shuffle may use the streaming path. Requires the
   * streaming machinery to be enabled (dual activation gate + live SparkEnv), the transport to be
   * capable of actually moving bytes producer-to-consumer, and the fallback policy to raise no
   * immediate veto against a baseline, all-clear snapshot.
   *
   * ==v1 forced sort fallback (zero-regression guarantee)==
   * The `transport.isWireTransferAvailable` term is the authoritative capability gate. In v1 the
   * transport is a logging-only stub, so this term is `false` and `canUseStreaming` is ALWAYS
   * `false`: every production shuffle is therefore delegated to the inner `SortShuffleManager` by
   * [[registerShuffle]], and the streaming writer/reader are never placed on a real task's data
   * path. This is what makes "streaming coexists as an opt-in while sort stays the
   * production-stable default" honest -- the backend is selectable via configuration, but until a
   * durable, reducer-fetchable wire path exists, no map task can report a `MapStatus` for bytes
   * that were never transferred (the Checkpoint-4 critical data-integrity finding). When the v2
   * transport lands, `isWireTransferAvailable` becomes `true` and the per-shuffle
   * [[isStreamingEligible]] check below begins to govern which shuffles stream and which continue
   * to fall back to sort.
   *
   * Because `streamingEnabled` short-circuits the `&&`, both `transport` and `fallbackPolicy` are
   * only dereferenced when they are guaranteed non-null (both are constructed whenever
   * `streamingEnabled` is true).
   */
  private def canUseStreaming: Boolean =
    streamingEnabled &&
      transport.isWireTransferAvailable &&
      !fallbackPolicy.shouldFallback(baselineFallbackStats)

  /**
   * Whether a specific shuffle's dependency is compatible with the streaming writer/reader data
   * model. This is the per-shuffle eligibility gate that complements the executor-wide
   * [[canUseStreaming]] capability gate, mirroring how `SortShuffleManager.registerShuffle` decides
   * between its bypass-merge-sort, serialized, and base code paths per shuffle.
   *
   * The streaming writer serializes raw `(K, V)` records and performs no map-side aggregation, so a
   * shuffle that requests '''map-side combine''' (where the reduce side expects pre-combined `C`
   * values) cannot be served correctly by the streaming path and MUST fall back to sort. Any
   * dependency deemed ineligible here is delegated to `sortShuffleManager.registerShuffle`, so its
   * full set of sort-side decisions (bypass-merge, serialized shuffle, serializer relocation,
   * partition-count thresholds, push-based merge) continues to apply unchanged. In v1 this method
   * is effectively unreached because [[canUseStreaming]] is already `false` (stub transport), but
   * it is wired now so the v2 wire path routes unsupported dependencies to sort from day one.
   */
  private def isStreamingEligible(dependency: ShuffleDependency[_, _, _]): Boolean =
    !dependency.mapSideCombine

  /**
   * Honor `spark.shuffle.streaming.debug=true` by elevating the streaming package logger
   * (`org.apache.spark.shuffle.streaming`) to DEBUG at runtime.
   *
   * The debug flag alone only decides whether the streaming components CALL `logDebug`; the
   * effective logger level still governs whether the message is actually emitted, and the default
   * level is INFO. Without this elevation, enabling the flag would appear to do nothing. This
   * raises the level for the streaming package so those gated diagnostics reach the configured
   * appenders, mirroring the AAP contract that the flag "elevates the streaming logger to DEBUG".
   *
   * It drives the same log4j2 backend Spark's own `Utils.setLogLevel` uses, via the log4j2 core
   * `Configurator`, which creates a dedicated `LoggerConfig` for the named logger (unlike
   * `getLoggerConfig`, which would return -- and wrongly mutate -- the nearest ancestor such as
   * root). The whole call is wrapped so that a non-log4j2 logging backend degrades gracefully: the
   * debug calls remain correctly gated by the flag, and operators can still raise the level for
   * this logger through their own logging configuration. Streaming configuration is immutable for
   * the application lifetime (v1 requires an executor restart to change it), so this runs exactly
   * once, at construction.
   */
  private def maybeElevateStreamingLogLevel(): Unit = {
    if (streamingConf.debug) {
      try {
        org.apache.logging.log4j.core.config.Configurator.setLevel(
          "org.apache.spark.shuffle.streaming", org.apache.logging.log4j.Level.DEBUG)
        logInfo("spark.shuffle.streaming.debug=true: elevated logger " +
          "'org.apache.spark.shuffle.streaming' to DEBUG.")
      } catch {
        case t: Throwable =>
          logWarning("Unable to elevate the 'org.apache.spark.shuffle.streaming' logger to " +
            "DEBUG for spark.shuffle.streaming.debug=true; debug calls remain gated by the " +
            "flag and can be enabled via your log4j2 configuration for that logger.", t)
      }
    }
  }

  /**
   * Register a shuffle and obtain a handle for tasks. When the shuffle is eligible for streaming a
   * [[StreamingShuffleHandle]] carrying the per-shuffle resource envelope is returned; otherwise
   * the registration is delegated to the inner [[SortShuffleManager]], which returns a base/sort
   * handle. The handle type is the single dispatch signal used later by [[getWriter]] and
   * [[getReader]].
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (canUseStreaming && isStreamingEligible(dependency)) {
      // ELIGIBLE: return a streaming handle stamped with this shuffle's resolved resource envelope
      // (buffer percent, spill threshold, bandwidth). getWriter/getReader pattern-match this exact
      // type to route to the streaming components. NOTE: in v1 this branch is unreachable because
      // canUseStreaming is gated on the stub transport's isWireTransferAvailable (false), so every
      // shuffle takes the sort-fallback branch below; the branch is retained as the v2 data path.
      logInfo(s"Registering shuffle $shuffleId with the streaming shuffle backend " +
        s"(bufferSizePercent=${streamingConf.bufferSizePercent}, " +
        s"spillThreshold=${streamingConf.spillThreshold}, " +
        s"maxBandwidthMBps=${streamingConf.maxBandwidthMBps}).")
      new StreamingShuffleHandle[K, V, C](
        shuffleId,
        dependency,
        streamingConf.bufferSizePercent,
        streamingConf.spillThreshold,
        streamingConf.maxBandwidthMBps)
    } else {
      // COEXISTENCE / FORCED FALLBACK: the shuffle is delegated to the production-stable inner
      // SortShuffleManager when streaming is inactive, when SparkEnv is absent, when the fallback
      // policy vetoes, when the transport cannot yet stream bytes over the wire (always the case in
      // v1), or when the dependency is not streaming-eligible (e.g. map-side combine). Delegating
      // to sortShuffleManager.registerShuffle preserves ALL of the sort path's per-shuffle choices
      // (bypass-merge-sort, serialized shuffle, serializer relocation, partition-count thresholds,
      // push-based merge). Every subsequent getWriter/getReader call for the returned (base/sort)
      // handle likewise falls through to the sort path, so shuffle output is always durably
      // materialized and reducer-fetchable -- the zero-regression guarantee.
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case h: StreamingShuffleHandle[K @unchecked, V @unchecked, _] if streamingEnabled =>
        // STREAMING PATH: mirror SortShuffleManager's producer-id bookkeeping so unregisterShuffle
        // cleans up streaming shuffles uniformly, then build a streaming writer wired to the shared
        // collaborators. The existential third type parameter of the handle is captured by type
        // inference into the writer's C (as SortShuffleManager does when constructing its writer).
        val mapTaskIds =
          taskIdMapsForShuffle.computeIfAbsent(handle.shuffleId, _ => new OpenHashSet[Long](16))
        mapTaskIds.synchronized {
          mapTaskIds.add(mapId)
        }
        new StreamingShuffleWriter(
          h,
          mapId,
          context,
          metrics,
          SparkEnv.get.blockManager,
          transport,
          spillManager,
          backpressure,
          streamingMetrics,
          streamingConf)
      case _ =>
        // COEXISTENCE: any non-streaming handle -- and, defensively, a streaming handle that
        // reaches an executor where streaming is not active (a config mismatch) -- is served by the
        // inner SortShuffleManager unchanged. This is type-safe because StreamingShuffleHandle
        // extends BaseShuffleHandle, so the sort manager handles it as a base handle.
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from a range of map outputs (startMapIndex to endMapIndex-1, inclusive). This is the
   * non-final 7-arg overload; the final 5-arg [[ShuffleManager.getReader]] auto-delegates here and
   * is intentionally not overridden.
   *
   * Called on executors by reduce tasks.
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
      case h: StreamingShuffleHandle[K @unchecked, _, C @unchecked] if streamingEnabled =>
        // STREAMING PATH: resolve producer block locations through the UNMODIFIED MapOutputTracker
        // (exactly as the sort path does) and hand them to the streaming reader, which performs
        // in-progress reads with partial-read invalidation over the reused BlockTransferService.
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        new StreamingShuffleReader[K, C](
          h, blocksByAddress, context, metrics, streamingMetrics, streamingConf)
      case _ =>
        // COEXISTENCE: non-streaming handles (and streaming handles when streaming is inactive on
        // this executor) are read by the inner SortShuffleManager unchanged.
        sortShuffleManager.getReader(
          handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Remove a shuffle's metadata from the manager. Cleans up BOTH paths so a shuffle is fully
   * unregistered regardless of which backend served it: the streaming producer-id bookkeeping and
   * the streaming resolver's in-memory / spilled block map, then the inner sort manager's state.
   *
   * @return true once the removal has been attempted on both paths.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // Streaming path: drop this shuffle's producer-id set and clear the resolver's tracked blocks.
    // The streaming resolver tracks blocks by shuffle id, so a single removeShuffle drops every
    // (mapId, reduceId) entry at once -- no per-map iteration is required.
    taskIdMapsForShuffle.remove(shuffleId)
    shuffleBlockResolver.removeShuffle(shuffleId)
    // DISK CLEANUP: the streaming spill path can persist per-partition blocks to disk via
    // BlockManager.putBytes(ShuffleBlockId(...), bytes, DISK_ONLY). Those spilled blocks are
    // tracked by the MemorySpillManager, NOT by the sort manager's task-id bookkeeping, so the
    // sort manager's cleanup below would never remove them. Ask the spill manager to remove this
    // shuffle's spilled blocks here so they do not leak on disk until the application exits. The
    // spill manager is null for a pass-through (streaming-inactive / no-env) manager, so guard it.
    if (spillManager != null) {
      spillManager.removeShuffle(shuffleId)
    }
    // COEXISTENCE: delegate to the inner SortShuffleManager so any shuffle it served (through the
    // sort fallback or when streaming was inactive) is also cleaned. Its unregisterShuffle is a
    // no-op for a shuffle id it never registered, so calling it unconditionally is safe and keeps
    // both managers' bookkeeping consistent.
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /**
   * Shut down this manager. The shutdown is ORDERED and idempotent:
   *   1. Stop the streaming collaborators first (the backpressure and spill daemons, then unbind
   *      the executor-only endpoint) so no in-flight streaming work touches the resolver or the
   *      sort path during teardown. The transport is a stateless v1 stub and has no daemon to stop.
   *   2. Deregister the streaming metrics source so a subsequent manager in the same JVM (common in
   *      tests) does not observe a stale duplicate source.
   *   3. Stop the streaming block resolver (it clears its map and stops the inner index resolver).
   *   4. Stop the inner SortShuffleManager LAST, since it is the fallback and must outlive the
   *      streaming components.
   * All references are null-guarded so a pass-through (inactive / no-env) manager stops cleanly.
   */
  override def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      if (backpressure != null) {
        backpressure.stop()
      }
      if (spillManager != null) {
        spillManager.stop()
      }
      if (backpressureEndpointRef != null && SparkEnv.get != null) {
        SparkEnv.get.rpcEnv.stop(backpressureEndpointRef)
      }
      if (streamingSource != null && SparkEnv.get != null) {
        SparkEnv.get.metricsSystem.removeSource(streamingSource)
      }
      shuffleBlockResolver.stop()
      sortShuffleManager.stop()
    }
  }
}
