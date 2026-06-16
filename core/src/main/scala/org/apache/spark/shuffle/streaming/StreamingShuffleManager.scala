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
 * Even when streaming is selected and enabled, the manager consults
 * [[StreamingShuffleFallbackPolicy]] at each [[registerShuffle]] and reverts that shuffle to the
 * inner `SortShuffleManager` whenever a fallback condition is tripped (slow consumer, memory
 * pressure, network saturation, or version mismatch). The policy is continuously fed live runtime
 * signals by the executor collaborators (the backpressure protocol's 1 s scan and the spill
 * manager's 100 ms poll), so a condition that arises mid-application takes effect on the next
 * shuffle registration. The sort path is composed unchanged and is never bypassed when fallback is
 * indicated - see [[useStreaming]] and [[registerShuffle]] (the single decision point).
 *
 * ==Backend consistency via handle-type dispatch (backend is immutable per shuffle)==
 *
 * The backend for a shuffle is decided exactly once, at [[registerShuffle]]: when the streaming
 * path is active a [[StreamingShuffleHandle]] is produced, otherwise registration is delegated to
 * the inner sort manager and a sort handle is produced. From that point the choice is immutable
 * for the lifetime of that shuffle. [[getWriter]] and [[getReader]] BOTH dispatch purely on the
 * concrete handle type and never re-consult the fallback policy, so a shuffle registered as
 * streaming is served by the streaming writer AND the streaming reader end-to-end, and a sort
 * handle is served by the sort writer AND the sort reader end-to-end. This is a correctness
 * requirement, not merely an optimization: the streaming reader expects streaming-framed bytes
 * (32-byte envelopes carrying CRC32C-validated frames), so it must never be paired with a sort
 * writer's `.data`/`.index` output. A fallback condition that trips after a shuffle is already
 * registered therefore affects only SUBSEQUENT registrations; a shuffle already in flight keeps
 * the backend it was registered with, which guarantees writer and reader always agree.
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
  // actual delegation to the inner SortShuffleManager when shouldFallback is true. Exposed as
  // private[streaming] so the executor collaborators built in ensureExecutorComponents (the
  // backpressure protocol and the spill manager) receive a reference and feed it live runtime
  // signals - throughput, network and memory utilization, and peer protocol version - so the four
  // revert conditions trip from real measurements, and so same-package manager/integration tests
  // can drive the manager's own policy and assert dispatch delegates to sort.
  private[streaming] val fallbackPolicy =
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
   * other threads. The backpressure protocol and spill manager are handed the shared
   * [[fallbackPolicy]] so their scan/poll loops feed it the live throughput, network, and memory
   * signals that drive automatic fallback.
   *
   * Lifecycle race safety: the build is guarded by a `stopped` check taken INSIDE [[initLock]],
   * the same lock [[stop]] holds for its teardown. This closes the init-vs-stop TOCTOU - if a
   * stop has already won the lock, this builds nothing on a dead manager; if a stop races in
   * while this is mid-build, it blocks on [[initLock]] until the build is published and then tears
   * the newly-created components down in order. Either way no daemon thread or RPC endpoint is
   * left running on a stopped manager.
   *
   * Callers MUST confirm `SparkEnv.get != null` before invoking this; the executor-only
   * collaborators require a live environment (block manager, memory manager, metrics, RPC env).
   */
  private def ensureExecutorComponents(): Unit = {
    if (!executorReady) {
      initLock.synchronized {
        // Build only when not already built AND no stop has raced in. Taking the stopped check
        // under initLock (which stop() also holds) is what makes initialization and teardown
        // mutually exclusive, preventing background daemons/RPC registration on a stopped manager.
        if (!executorReady && !stopped.get()) {
          val env = SparkEnv.get
          // Rate limiter is unlimited by default (maxBandwidthMBps <= 0) and allocates nothing;
          // the protocol retains it, so this manager keeps no separate field for it.
          val limiter = TokenBucketRateLimiter(streamingConfig)
          // Pass the shared fallbackPolicy so the protocol's 1 s scan feeds it producer/consumer
          // throughput (slow-consumer condition), derived network utilization (saturation), and
          // peer protocol versions (version mismatch) from live runtime state.
          val protocol =
            new BackpressureProtocol(streamingConfig, limiter, streamingMetrics, fallbackPolicy)
          protocol.start()
          // Pass the shared fallbackPolicy so the spill manager's 100 ms poll feeds it the live
          // aggregate buffer-utilization percent (memory-pressure condition).
          val spill = new MemorySpillManager(
            streamingConfig,
            env.blockManager,
            env.memoryManager,
            streamingMetrics,
            fallbackPolicy)
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
   * Registration-time memory-bound check used by [[registerShuffle]] to keep an unsuitable
   * workload off the streaming path entirely (writer AND reader), rather than discovering the
   * problem mid-write when it can no longer be safely reverted.
   *
   * The executor buffer budget mirrors the writer's own sizing input: the streaming backend may
   * use `maxOnHeapStorageMemory * bufferSizePercent / 100` bytes for its per-partition buffers.
   * The actual memory-bound predicate (comparing that budget against the 2 MB-floored
   * per-partition requirement) lives in
   * [[StreamingShuffleFallbackPolicy.isMemoryBoundForPartitions]] so the decision logic stays in
   * one place and is independently testable.
   *
   * When `SparkEnv` is unavailable (driver-side registration in some local topologies) the budget
   * resolves to `0`, which the policy treats as "unknown" and never memory-bound; the writer and
   * reader then make their own `SparkEnv`-null sort fallback, so both ends still agree.
   *
   * @param numPartitions the number of reduce partitions the shuffle will produce
   * @return `true` when the workload cannot fit the streaming buffer budget and must use sort
   */
  private def isWorkloadMemoryBound(numPartitions: Int): Boolean = {
    val executorMemoryBytes =
      Option(SparkEnv.get).map(_.memoryManager.maxOnHeapStorageMemory).getOrElse(0L)
    val budgetBytes = executorMemoryBytes * streamingConfig.bufferSizePercent / 100
    fallbackPolicy.isMemoryBoundForPartitions(numPartitions, budgetBytes)
  }

  /**
   * Registers a shuffle and returns a handle to pass to tasks. When the streaming path is active
   * AND the workload is not memory-bound, the handle is a [[StreamingShuffleHandle]] carrying the
   * resolved tuning values so the writer/reader receive their configuration without re-reading the
   * `SparkConf`. Otherwise the registration is delegated to the inner sort manager so the handle
   * (and therefore the whole shuffle) is sort-based.
   *
   * The handle type recorded here is what [[getWriter]] and [[getReader]] dispatch on, so a
   * shuffle is consistently served by the SAME backend it was registered with. This is precisely
   * why the memory-bound decision must be made HERE, before the handle exists: a streaming shuffle
   * cannot be safely reverted to sort after registration (the writer would emit sort
   * `.data`/`.index` bytes while the reader still expects 32-byte streaming envelopes), so an
   * unsuitable workload is steered onto the sort path up front rather than mid-write. This is the
   * registration-time arm of the AAP's "memory pressure prevents buffer allocation (OOM risk)"
   * revert condition and the linchpin of the zero-regression guarantee for memory-bound workloads.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Evaluate the general activation gate first; only when streaming is otherwise active do we
    // inspect the workload shape (short-circuiting keeps the disabled/fallback paths free of any
    // dependency access). `numPartitions` is the reduce-side fan-out that drives buffer sizing.
    val streamingActive = useStreaming
    val memoryBound =
      streamingActive && isWorkloadMemoryBound(dependency.partitioner.numPartitions)
    if (streamingActive && !memoryBound) {
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
      if (memoryBound) {
        // A memory-bound workload taking the zero-regression sort path is an infrequent,
        // per-registration event (not a hot-path log), so a single info line stays well within
        // the per-executor log budget while documenting why streaming was declined.
        logInfo(log"Streaming shuffle ${MDC(LogKeys.SHUFFLE_ID, shuffleId)} is memory-bound " +
          log"(reduce partitions exceed the streaming buffer budget); registering on the sort " +
          log"path for zero-regression fallback")
      }
      // Fallback / disabled / memory-bound path: delegate to the inner SortShuffleManager so the
      // produced handle is a sort handle. This is the zero-regression guarantee - the sort path is
      // unchanged, and a memory-bound workload is served end-to-end by sort (writer AND reader).
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /**
   * Returns a map-side writer. Called on executors by map tasks. Dispatch is purely by handle
   * type: a [[StreamingShuffleHandle]] (which [[registerShuffle]] only ever produces when the
   * streaming path was active at registration) always yields a [[StreamingShuffleWriter]], and a
   * sort handle always delegates to the inner sort manager's writer unchanged.
   *
   * The fallback policy is deliberately NOT re-consulted here. Doing so would let a fallback that
   * trips between registration and the map write revert this map task to the sort writer while the
   * matching [[getReader]] still used the streaming reader, feeding the sort writer's
   * `.data`/`.index` bytes into a reader that expects 32-byte streaming envelopes - a data
   * integrity bug. Because the backend is immutable per shuffle (see the class doc), a tripped
   * fallback only changes the handle type of SUBSEQUENT registrations.
   *
   * Memory-bound unsuitability is therefore handled the only safe way: it is detected BEFORE the
   * handle exists, in [[registerShuffle]] via [[isWorkloadMemoryBound]], so a memory-bound
   * workload never reaches this method with a [[StreamingShuffleHandle]] in the first place. That
   * pre-registration check is what makes the AAP's "memory pressure prevents buffer allocation"
   * revert condition enforceable without ever mixing sort and streaming bytes within one shuffle.
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      // Streaming handle: this shuffle was registered streaming, so it is written streaming. The
      // matching getReader dispatches on the same handle type, so writer and reader always agree.
      case h: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        if (SparkEnv.get == null) {
          // Local-mode safety: building the spill manager/transport needs a live executor env.
          // Without one, fall back to the sort writer rather than risk a partial streaming setup.
          // This env check is stable for the executor, so getReader makes the same choice and the
          // write/read backends still agree.
          sortShuffleManager.getWriter(handle, mapId, context, metrics)
        } else {
          ensureExecutorComponents()
          if (!executorReady) {
            // A stop() raced ahead of this writer initialization; refuse to build a half-wired
            // streaming writer (with null collaborators) on a stopped manager.
            throw new IllegalStateException(
              "StreamingShuffleManager is stopped; cannot create a streaming shuffle writer")
          }
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
      // Sort handle: delegate to the inner sort manager's writer unchanged.
      case _ =>
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Returns a reduce-side reader for the given map/partition ranges. Called on executors by
   * reduce tasks. This overrides the abstract 7-arg `getReader`; the 5-arg overload is `final` in
   * the trait and forwards here, so it is intentionally NOT overridden.
   *
   * Dispatch is purely by handle type, exactly mirroring [[getWriter]]: a shuffle registered with
   * a [[StreamingShuffleHandle]] is read by the [[StreamingShuffleReader]], and a sort handle by
   * the sort reader. Because the backend is immutable per shuffle (decided once at
   * [[registerShuffle]]), the reader is guaranteed to consume bytes produced by the SAME backend's
   * writer - the streaming reader only ever reads streaming-framed (32-byte envelope, CRC32C)
   * output, never a sort writer's `.data`/`.index` files. The streaming reader mirrors
   * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] and reuses the existing fetch path.
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
          // no live env, delegate to the sort reader (which the sort path also requires). This
          // env check matches getWriter's, so the write/read backends still agree.
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
          if (!executorReady) {
            // A stop() raced ahead of this reader initialization; refuse to build a half-wired
            // streaming reader on a stopped manager.
            throw new IllegalStateException(
              "StreamingShuffleManager is stopped; cannot create a streaming shuffle reader")
          }
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
            transport,
            // Wire the consumer-to-producer backpressure control plane: the reader emits
            // heartbeat/ack/peer-version messages to this executor's backpressure RPC endpoint
            // (None on the driver/local-mode path, where the endpoint is intentionally not
            // registered). Closes the loop the writer already reacts to via the rate limiter and
            // consumer-timeout handling. Block locations/data still flow over the unchanged
            // MapOutputTracker + BlockTransferService pull path.
            backpressureEndpointRef)
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
   * recorded spilled-block locations) held by the streaming resolver, releases the spill
   * manager's live buffers and spilled disk segments for the shuffle, and ALSO delegates to the
   * inner sort manager so fallback shuffles are cleaned too - but only if that manager was ever
   * built, so a pure-streaming deployment never forces it here.
   *
   * Clearing the spill manager here is what keeps a completed shuffle from leaving buffered heap
   * and on-disk spill segments behind until executor shutdown (the resource-cleanup /
   * zero-retained-heap guarantee). The spill manager is null until the executor components are
   * built, so the call is null-guarded for the driver and for a manager that never streamed.
   *
   * @return
   *   always `true`; cleanup is best-effort and never reports failure.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    streamingResolver.untrackShuffle(shuffleId)
    // Release the spill manager's per-shuffle buffers and spilled disk blocks. Null-guarded
    // because the spill manager is only built on first streaming use on an executor.
    val spill = spillManager
    if (spill != null) {
      spill.unregisterShuffle(shuffleId)
    }
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
   *
   * Lifecycle race safety: the `stopped` flag is raised first (so a concurrent
   * [[ensureExecutorComponents]] that has not yet started building observes it and builds
   * nothing), then the teardown runs under [[initLock]] - the same lock the build holds. This
   * makes initialization and teardown mutually exclusive: a build that is mid-flight when stop is
   * called completes and publishes its fields first, and this teardown then reads those non-null
   * fields and stops the just-created components, so no daemon thread or RPC endpoint is ever left
   * running on a stopped manager.
   */
  override def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      initLock.synchronized {
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
}
