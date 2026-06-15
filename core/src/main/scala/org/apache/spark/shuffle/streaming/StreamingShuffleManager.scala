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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.streaming.network.{StreamingShuffleTransport, TokenBucketRateLimiter}

/**
 * The [[org.apache.spark.shuffle.ShuffleManager]] entry point and orchestrator for the opt-in
 * streaming shuffle backend.
 *
 * ==Role and reflective instantiation==
 *
 * This is the single class that the [[org.apache.spark.shuffle.ShuffleManager]] factory names:
 * the factory's `shortShuffleMgrNames` map aliases `"streaming"` to this class's fully-qualified
 * name, and `SparkEnv.create()` reflectively instantiates it with the verified SPI constructor
 * `(conf: SparkConf, isDriver: Boolean)`. Every other streaming class hangs off this manager - it
 * produces the streaming writer, reader, handle, and block resolver, registers the streaming
 * metrics source, owns the executor-only backpressure RPC endpoint, and holds the lazily-built
 * inner [[org.apache.spark.shuffle.sort.SortShuffleManager]] used for fallback.
 *
 * ==Coexistence with, and fallback to, the sort-based path (zero-regression guarantee)==
 *
 * The streaming backend NEVER replaces sort-based shuffle; it coexists with it. Two independent
 * signals must both be on for the streaming path to engage: the manager alias selection
 * `spark.shuffle.manager=streaming` (resolved in the factory, before this class is constructed)
 * AND the feature flag `spark.shuffle.streaming.enabled=true` (read here via
 * [[StreamingShuffleConfig]]). Because BOTH default to off, the default behavior of every
 * existing Spark deployment is byte-for-byte unchanged.
 *
 * Even when streaming is selected and enabled, the manager continuously consults
 * [[StreamingShuffleFallbackPolicy]] and reverts to the inner `SortShuffleManager` whenever a
 * fallback condition trips (slow consumer, memory pressure, network saturation, or version
 * mismatch). The sort path is composed unchanged and is never bypassed when fallback is indicated -
 * see [[useStreaming]], [[registerShuffle]], and [[getWriter]].
 *
 * ==Backend consistency via handle-type dispatch==
 *
 * A shuffle registered with a [[StreamingShuffleHandle]] is always served by the streaming
 * writer/reader, and a shuffle registered with a sort handle is always served by the sort
 * writer/reader. [[getWriter]] and [[getReader]] therefore dispatch on the concrete handle type
 * rather than re-deciding from scratch, which guarantees that a single shuffle is served
 * end-to-end by exactly one backend. The one nuance is that if the fallback policy trips between
 * registration and the map write, [[getWriter]] reverts that map task to the sort writer; the
 * streaming reader nonetheless reads the result correctly because it mirrors
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] and reuses the existing fetch path.
 *
 * ==Local-mode safety==
 *
 * Construction is pure and touches no [[org.apache.spark.SparkEnv]] so the manager can be built
 * before the environment exists (as happens during `SparkEnv.create()`) and in unit tests. All
 * executor-side wiring that needs a live environment (the spill manager, the transport, metrics
 * registration, and the backpressure RPC endpoint) is deferred to [[ensureExecutorComponents]],
 * which runs once on first streaming use and is only invoked after a `SparkEnv.get != null` check
 * by the caller.
 *
 * @param conf
 *   the application [[org.apache.spark.SparkConf]]
 * @param isDriver
 *   `true` when this manager runs on the driver; gates the executor-only backpressure RPC
 *   endpoint, which is rejected on the driver
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends ShuffleManager
    with Logging {

  // Typed, immutable configuration accessor. Reading the streaming-shuffle keys here also applies
  // their ConfigEntry range guards. Pure: no SparkEnv access, so it is safe at construction time.
  private val streamingConfig = new StreamingShuffleConfig(conf)

  // Fail fast on out-of-range tuning, but only when streaming is actually enabled so a misconfig
  // in a default (disabled) deployment can never break startup. This is belt-and-suspenders on
  // top of the ConfigEntry guards.
  if (streamingConfig.enabled) {
    streamingConfig.validate()
  }

  // Lock-free telemetry holder shared by the writer, reader, backpressure protocol, spill
  // manager, and fallback policy. Adapted to the MetricsSystem by StreamingShuffleSource.
  private val streamingMetrics = new StreamingShuffleMetrics

  // The decision object for automatic fallback. It only decides; this manager performs the
  // actual delegation to the inner SortShuffleManager when shouldFallback is true.
  private val fallbackPolicy =
    new StreamingShuffleFallbackPolicy(streamingConfig, streamingMetrics)

  // The streaming block resolver, typed as its concrete class so this manager can call the
  // tracking/cleanup hooks (untrackShuffle, stop) and wire it into the writer. Construction is
  // env-safe: it builds an inner IndexShuffleBlockResolver from conf only (its block manager is
  // resolved lazily). Built eagerly, mirroring SortShuffleManager.
  private val streamingResolver = new StreamingShuffleBlockResolver(conf)

  // The SPI resolver returned to callers. It is the SAME instance as streamingResolver; under a
  // pure-fallback deployment it still resolves correctly because it delegates .data/.index and
  // migration to its inner IndexShuffleBlockResolver.
  override val shuffleBlockResolver: ShuffleBlockResolver = streamingResolver

  // Lazily-instantiated inner sort manager used for fallback and for the disabled path. Held in
  // an Option (rather than a plain `lazy val`) so stop()/unregisterShuffle can tell whether it
  // was ever built and avoid forcing it when the streaming path was never abandoned.
  @volatile private var sortManagerOpt: Option[SortShuffleManager] = None
  private val sortInitLock = new Object()

  // Executor-side streaming collaborators, published once by ensureExecutorComponents under
  // initLock. They remain null until the first streaming write/read on an executor builds them.
  @volatile private var backpressureProtocol: BackpressureProtocol = null
  @volatile private var spillManager: MemorySpillManager = null
  @volatile private var transport: StreamingShuffleTransport = null
  @volatile private var backpressureEndpointRef: Option[RpcEndpointRef] = None
  @volatile private var executorReady: Boolean = false
  private val initLock = new Object()

  // Guards stop() so teardown runs exactly once and is idempotent under repeated calls.
  private val stopped = new AtomicBoolean(false)

  logInfo(log"StreamingShuffleManager initialized (streaming.enabled=" +
    log"${MDC(LogKeys.CONFIG, streamingConfig.enabled)}); the streaming path engages only when " +
    log"spark.shuffle.manager=streaming AND spark.shuffle.streaming.enabled=true, otherwise it " +
    log"delegates to the inner SortShuffleManager (sort-based shuffle is never bypassed)")

  /**
   * Returns the inner [[org.apache.spark.shuffle.sort.SortShuffleManager]], building it on first
   * use under a double-checked lock. Subsequent calls return the cached instance. Keeping the
   * instance in [[sortManagerOpt]] lets teardown and unregistration skip the fallback manager
   * entirely when it was never needed (a pure-streaming deployment).
   */
  private def sortShuffleManager: SortShuffleManager = {
    sortManagerOpt.getOrElse {
      sortInitLock.synchronized {
        sortManagerOpt.getOrElse {
          // The sort manager is composed UNCHANGED; this is the coexistence/fallback anchor.
          val created = new SortShuffleManager(conf)
          sortManagerOpt = Some(created)
          created
        }
      }
    }
  }

  /**
   * Builds the executor-side streaming collaborators exactly once and starts their daemons.
   *
   * This constructs the token-bucket rate limiter and backpressure protocol (and starts the 1 s
   * timeout-scan thread), the 100 ms spill manager, and the v1 logging-only transport; it then
   * registers the streaming metrics source with the executor `MetricsSystem` and registers the
   * backpressure RPC endpoint on executors only (the driver registers nothing). Double-checked
   * locking on [[executorReady]] keeps construction single and publishes the field writes to
   * other threads.
   *
   * Callers MUST confirm `SparkEnv.get != null` before invoking this; the executor-only
   * collaborators require a live environment (block manager, memory manager, metrics, RPC env).
   */
  private def ensureExecutorComponents(): Unit = {
    if (!executorReady) {
      initLock.synchronized {
        if (!executorReady) {
          val env = SparkEnv.get
          // Rate limiter is unlimited by default (maxBandwidthMBps <= 0) and allocates nothing;
          // the protocol retains it, so this manager keeps no separate field for it.
          val limiter = TokenBucketRateLimiter(streamingConfig)
          val protocol = new BackpressureProtocol(streamingConfig, limiter, streamingMetrics)
          protocol.start()
          val spill = new MemorySpillManager(
            streamingConfig,
            env.blockManager,
            env.memoryManager,
            streamingMetrics)
          spill.start()
          val xport = new StreamingShuffleTransport(
            streamingConfig,
            Option(env.blockManager).map(_.blockTransferService))
          // Executor-side telemetry: surface the four shuffle.streaming.* metrics through the
          // existing MetricsSystem (AAP 0.3.3); no change to the metrics framework itself.
          env.metricsSystem.registerSource(new StreamingShuffleSource(streamingMetrics))
          // Backpressure heartbeat mailbox: executors only. registerIfExecutor returns None on
          // the driver, which neither produces nor consumes streamed shuffle blocks.
          val endpointRef =
            BackpressureRpcEndpoint.registerIfExecutor(env.rpcEnv, isDriver, protocol)

          backpressureProtocol = protocol
          spillManager = spill
          transport = xport
          backpressureEndpointRef = endpointRef
          executorReady = true
          logInfo(log"Streaming shuffle executor components initialized")
        }
      }
    }
  }

  /**
   * The runtime activation gate for the streaming path. The `"streaming"` alias was already
   * selected by the factory; what remains is the feature flag AND the absence of any tripped
   * fallback condition. When this is false the manager delegates to the inner sort manager,
   * upholding the zero-regression guarantee.
   */
  private def useStreaming: Boolean = streamingConfig.enabled && !fallbackPolicy.shouldFallback

  /**
   * Registers a shuffle and returns a handle to pass to tasks. When the streaming path is active
   * the handle is a [[StreamingShuffleHandle]] carrying the resolved tuning values so the
   * writer/reader receive their configuration without re-reading the `SparkConf`. Otherwise the
   * registration is delegated to the inner sort manager so the handle (and therefore the whole
   * shuffle) is sort-based.
   *
   * The handle type recorded here is what [[getWriter]] and [[getReader]] dispatch on, so a
   * shuffle is consistently served by the SAME backend it was registered with.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (useStreaming) {
      val handle = new StreamingShuffleHandle[K, V, C](
        shuffleId,
        dependency,
        streamingConfig.bufferSizePercent,
        streamingConfig.spillThreshold,
        streamingConfig.maxBandwidthMBps)
      if (streamingConfig.debug) {
        logDebug(log"Registered streaming shuffle ${MDC(LogKeys.SHUFFLE_ID, shuffleId)}")
      }
      handle
    } else {
      // Fallback / disabled path: delegate to the inner SortShuffleManager so the produced handle
      // is a sort handle. This is the zero-regression guarantee - the sort path is unchanged.
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /**
   * Returns a map-side writer. Called on executors by map tasks. The handle type selects the
   * backend: a [[StreamingShuffleHandle]] yields a [[StreamingShuffleWriter]] when streaming is
   * still active, while every other case (a sort handle, or a streaming handle after a fallback
   * trip) delegates to the inner sort manager so the sort writer is used unchanged.
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      // Streaming handle AND streaming still active. We additionally require useStreaming so a
      // fallback that trips between registration and the map write reverts this task to sort.
      case h: StreamingShuffleHandle[K @unchecked, V @unchecked, _] if useStreaming =>
        if (SparkEnv.get == null) {
          // Local-mode safety: building the spill manager/transport needs a live executor env.
          // Without one, fall back to the sort writer rather than risk a partial streaming setup.
          sortShuffleManager.getWriter(handle, mapId, context, metrics)
        } else {
          ensureExecutorComponents()
          new StreamingShuffleWriter[K, V](
            h,
            mapId,
            context,
            metrics,
            streamingConfig,
            streamingMetrics,
            backpressureProtocol,
            spillManager,
            transport,
            streamingResolver)
        }
      // Sort handle, or a streaming handle whose backend has fallen back: delegate to sort.
      case _ =>
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Returns a reduce-side reader for the given map/partition ranges. Called on executors by
   * reduce tasks. This overrides the abstract 7-arg `getReader`; the 5-arg overload is `final` in
   * the trait and forwards here, so it is intentionally NOT overridden.
   *
   * Dispatch is purely by handle type: a shuffle written with a [[StreamingShuffleHandle]] is
   * read by the [[StreamingShuffleReader]] so both sides agree. The streaming reader mirrors
   * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] and reuses the existing fetch path, so
   * it correctly reads output that a fallback-time sort writer produced for a streaming handle.
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
      case h: StreamingShuffleHandle[K @unchecked, _, C @unchecked] =>
        if (SparkEnv.get == null) {
          // Local-mode safety: the streaming reader resolves its env-backed collaborators; with
          // no live env, delegate to the sort reader (which the sort path also requires).
          sortShuffleManager.getReader(
            handle,
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition,
            context,
            metrics)
        } else {
          ensureExecutorComponents()
          new StreamingShuffleReader[K, C](
            h,
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition,
            context,
            metrics,
            streamingConfig,
            streamingMetrics,
            transport)
        }
      // Sort handle: delegate to sort unchanged.
      case _ =>
        sortShuffleManager.getReader(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics)
    }
  }

  /**
   * Removes a shuffle's metadata. Cleans up the streaming tracking (in-memory buffers and the
   * recorded spilled-block locations) held by the streaming resolver, and ALSO delegates to the
   * inner sort manager so fallback shuffles are cleaned too - but only if that manager was ever
   * built, so a pure-streaming deployment never forces it here.
   *
   * @return
   *   always `true`; cleanup is best-effort and never reports failure.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    streamingResolver.untrackShuffle(shuffleId)
    sortManagerOpt.foreach(_.unregisterShuffle(shuffleId))
    if (streamingConfig.debug) {
      logDebug(log"Unregistered shuffle ${MDC(LogKeys.SHUFFLE_ID, shuffleId)}")
    }
    true
  }

  /**
   * Shuts down this manager in the fixed teardown order mandated by the feature plan:
   * backpressure -> spill -> inner sort -> clear shuffle ids. Every underlying `stop()` is
   * idempotent and each field is null-guarded, so a manager that never engaged the streaming path
   * (or never built the sort fallback) still tears down cleanly. Guarded so it runs once.
   */
  override def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      // 1. Backpressure: stop the timeout-scan thread, then unregister the executor-only RPC
      // endpoint (registered only on executors, so the ref is empty on the driver).
      val protocol = backpressureProtocol
      if (protocol != null) {
        protocol.stop()
      }
      backpressureEndpointRef.foreach { ref =>
        val env = SparkEnv.get
        if (env != null) {
          env.rpcEnv.stop(ref)
        }
      }
      // 2. Spill: shut down the 100 ms poller and release the live buffer registry.
      val spill = spillManager
      if (spill != null) {
        spill.stop()
      }
      // 3. Inner sort fallback: stop it only if it was ever instantiated.
      sortManagerOpt.foreach(_.stop())
      // 4. Clear streaming tracking maps / shuffle ids via the streaming resolver, which also
      // stops its inner IndexShuffleBlockResolver.
      streamingResolver.stop()
      logInfo(log"StreamingShuffleManager stopped")
    }
  }
}
