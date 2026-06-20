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

import scala.util.control.NonFatal

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * The opt-in streaming shuffle backend's [[ShuffleManager]] implementation and the single class the
 * shuffle factory names by fully-qualified class name. `SparkEnv.create()` instantiates it
 * reflectively because the `ShuffleManager` companion aliases `"streaming"` to this class's FQCN;
 * everything else in the streaming subsystem hangs off this orchestrator.
 *
 * ==Coexistence with the sort-based shuffle (zero-regression guarantee)==
 *
 * This manager NEVER replaces the sort-based shuffle; it composes it. Two configuration signals
 * must both hold for the streaming path to engage: `spark.shuffle.manager=streaming` (which selects
 * this class through the factory alias) AND `spark.shuffle.streaming.enabled=true`. Both default to
 * off, so the default behavior of every existing Spark deployment is byte-for-byte unchanged. When
 * streaming is not engaged -- because the feature flag is off, or because
 * [[StreamingShuffleFallbackPolicy]] trips one of its four revert conditions -- this manager
 * delegates every operation to a lazily-instantiated inner [[SortShuffleManager]], which is held
 * unchanged and is never bypassed.
 *
 * ==One backend per shuffle: dispatch by handle type==
 *
 * The fallback decision is made exactly once, on the driver, in [[registerShuffle]] (via
 * [[useStreaming]]): it returns a [[StreamingShuffleHandle]] when streaming is engaged and a sort
 * handle otherwise. The handle TYPE is then the single, cluster-wide source of truth for that
 * shuffle's backend. [[getWriter]] and the 7-arg [[getReader]] dispatch on the handle type ALONE --
 * a streaming handle is always served by the streaming writer/reader, and any other handle by the
 * inner sort manager. They deliberately do NOT re-evaluate the (per-executor) activation gate,
 * because doing so could split one shuffle across both backends on different executors and let
 * streaming-written output be read by the sort reader (or vice versa) -- a fatal format mismatch.
 * Dispatching by handle type is what guarantees a shuffle is served end-to-end by exactly one
 * backend and that the sort path is never bypassed once a shuffle has been registered.
 *
 * ==Isolation and local-mode safety==
 *
 * All streaming logic lives in the `org.apache.spark.shuffle.streaming` package; this class only
 * wires the collaborators together and touches no existing Spark code beyond the two surgical
 * integration edits (the factory alias and the five `spark.shuffle.streaming.*` config entries).
 * The executor-side collaborators (the metrics source, the spill manager's 100 ms poll loop, the
 * backpressure scan, and the executor-only backpressure RPC endpoint) are built and started lazily
 * by [[ensureExecutorComponents]] on first executor use, and every access to the global `SparkEnv`
 * is gated on `SparkEnv.get != null` so this manager is safe to construct in local mode and in unit
 * tests where no `SparkEnv` has been installed.
 *
 * @param conf                   the [[SparkConf]] this manager and its collaborators read settings
 *                               from
 * @param isDriver               whether this manager runs on the driver; the backpressure RPC
 *                               endpoint is registered on executors only, so the driver passes
 *                               through with no endpoint
 * @param fallbackPolicyOverride optional pre-built policy. Production NEVER supplies one --
 *                               `SparkEnv` instantiates this manager reflectively through the
 *                               two-argument auxiliary constructor below, which passes `None`, so a
 *                               fresh policy is built and owned here. It exists purely as a test
 *                               seam: a suite injects a policy whose signals (and monotonic clock)
 *                               it controls, then asserts that the manager's own registration-time
 *                               decision delegates to the inner [[SortShuffleManager]] under each
 *                               of the four revert conditions while streaming is enabled.
 */
private[spark] class StreamingShuffleManager(
    conf: SparkConf,
    isDriver: Boolean,
    fallbackPolicyOverride: Option[StreamingShuffleFallbackPolicy])
  extends ShuffleManager with Logging {

  /**
   * The constructor `SparkEnv` invokes reflectively (via
   * `Utils.instantiateSerializerOrShuffleManager`), whose required JVM signature is exactly
   * `(SparkConf, java.lang.Boolean)`. It delegates to the 3-arg primary constructor with no
   * fallback-policy override, so production always builds and owns its own policy; the primary
   * constructor's third argument is reserved for tests that inject a custom policy to exercise
   * the manager's real fallback decision.
   */
  def this(conf: SparkConf, isDriver: Boolean) = this(conf, isDriver, None)

  // The typed, validated view of the five spark.shuffle.streaming.* settings. Read once here and
  // shared with every collaborator; the configuration is immutable for the application lifetime
  // (no dynamic reconfiguration in v1), so this single reading is authoritative.
  private val streamingConfig = new StreamingShuffleConfig(conf)
  // Belt-and-suspenders range check. The ConfigEntry.checkValue predicates are the authoritative
  // gate (and already ran when the values were read above); this re-validates for SparkConfs that
  // were assembled programmatically. Cheap and safe: the defaults are in range.
  streamingConfig.validate()

  // The shared, dependency-free telemetry holder. One instance is handed to the writer, reader,
  // backpressure protocol, and spill manager, and is adapted by StreamingShuffleSource, so every
  // component updates the same four shuffle.streaming.* values.
  private val streamingMetrics = new StreamingShuffleMetrics

  // The zero-regression decision object. It only DECIDES whether to fall back; the delegation to
  // the sort-based manager is performed here, in registerShuffle. It is pure (no SparkEnv access),
  // so it is safe to construct eagerly -- including on the driver, where registerShuffle runs. In
  // production fallbackPolicyOverride is None (the reflective two-arg ctor passes None), so a
  // fresh policy is built; a test may inject one whose signals and clock it controls. This SAME
  // instance is handed to the backpressure protocol and spill manager below, so the live
  // throughput, network, and memory samples they push feed the very policy useStreaming consults.
  private val fallbackPolicy =
    fallbackPolicyOverride.getOrElse(new StreamingShuffleFallbackPolicy(streamingConfig))

  // -- Lazy, pure collaborators (no SparkEnv dependency) ------------------------------------------

  // The per-executor byte-budget token bucket shared by all producers. Pure, so it is built lazily
  // on first use (the divisor defaults to one concurrent shuffle in v1).
  private lazy val rateLimiter = TokenBucketRateLimiter(streamingConfig)

  // The heartbeat + token-bucket flow-control brain. Constructing it is side-effect-free; its
  // background timeout scan is started by ensureExecutorComponents() on the executor side only.
  private lazy val backpressureProtocol =
    new BackpressureProtocol(streamingConfig, rateLimiter, streamingMetrics, Some(fallbackPolicy))

  // The v1 logging-only transport seam. Its companion apply() gates on SparkEnv.get internally and
  // yields a transport with no transfer service in local mode / on the driver.
  private lazy val transport = StreamingShuffleTransport(streamingConfig)

  // -- Existence-tracked collaborators ------------------------------------------------------------
  // These two use a @volatile ref + double-checked getter rather than a `lazy val` so that stop()
  // and unregisterShuffle can test whether the collaborator was ever created and avoid forcing its
  // creation in a deployment that never needs it.

  // The lazily-instantiated inner sort-based manager -- the automatic fallback that preserves the
  // existing behavior unchanged.
  @volatile private var sortShuffleManagerRef: SortShuffleManager = _

  // The streaming block resolver returned by shuffleBlockResolver (it delegates .data/.index and
  // migration to an inner IndexShuffleBlockResolver, so it also works in a pure-fallback cluster).
  @volatile private var streamingResolverRef: StreamingShuffleBlockResolver = _

  // -- One-shot executor-side state (assigned only by ensureExecutorComponents) -------------------

  // Guards the one-time executor-side initialization. Set true only after every executor component
  // has been built and started, so a partially-failed init never appears complete.
  @volatile private var executorComponentsReady = false

  // The env-dependent spill manager; built and started in ensureExecutorComponents(). Null until
  // then (and on a driver / in a test that constructs this manager without a SparkEnv).
  @volatile private var spillManager: MemorySpillManager = _

  // The metrics Source registered with the MetricsSystem; retained so stop() can remove it.
  @volatile private var metricsSource: StreamingShuffleSource = _

  // The executor-only backpressure endpoint ref; None on the driver and until init runs.
  @volatile private var backpressureEndpoint: Option[RpcEndpointRef] = None

  /**
   * Returns the inner [[SortShuffleManager]], creating it on first use. Uses double-checked locking
   * over a `@volatile` ref so the common (already-created) path is lock-free while creation happens
   * at most once.
   */
  private def sortShuffleManager: SortShuffleManager = {
    val existing = sortShuffleManagerRef
    if (existing != null) {
      existing
    } else {
      synchronized {
        if (sortShuffleManagerRef == null) {
          // Comment (coexistence): the inner sort manager is the unchanged fallback path. It is
          // constructed only when first needed -- the first fallback registration, or teardown.
          sortShuffleManagerRef = new SortShuffleManager(conf)
        }
        sortShuffleManagerRef
      }
    }
  }

  /**
   * Returns the [[StreamingShuffleBlockResolver]], creating it on first use with the same
   * double-checked locking pattern as [[sortShuffleManager]]. The convenience constructor owns a
   * fresh inner [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] that defers its
   * `BlockManager` lookup, so this is safe to build in local mode.
   */
  private def streamingResolver: StreamingShuffleBlockResolver = {
    val existing = streamingResolverRef
    if (existing != null) {
      existing
    } else {
      synchronized {
        if (streamingResolverRef == null) {
          streamingResolverRef = new StreamingShuffleBlockResolver(conf)
        }
        streamingResolverRef
      }
    }
  }

  /**
   * Builds and starts the executor-side collaborators exactly once, and only when a live `SparkEnv`
   * is present. This is invoked from [[getWriter]] and [[getReader]] -- executor entry points --
   * rather than from the constructor, so that in cluster mode the driver (which registers shuffles
   * but never reads or writes blocks) starts no background threads and registers no metrics source
   * it will never use.
   *
   * The work performed once, under a `synchronized` double-check, is:
   *  1. register a [[StreamingShuffleSource]] with the executor `MetricsSystem` so the four
   *     `shuffle.streaming.*` metrics flow through JMX and the Prometheus endpoint;
   *  2. build the [[MemorySpillManager]] against the executor `BlockManager`/`MemoryManager` and
   *     start its 100 ms spill-poll loop;
   *  3. start the [[BackpressureProtocol]] timeout scan and register the executor-only
   *     [[BackpressureRpcEndpoint]] (which returns `None` on the driver).
   *
   * Every `SparkEnv` access is guarded, so on a null env (local-mode bootstrap or a unit test) this
   * is a no-op and the manager continues to function for SPI dispatch and fallback.
   */
  private def ensureExecutorComponents(): Unit = {
    if (!executorComponentsReady) {
      synchronized {
        val env = SparkEnv.get
        if (!executorComponentsReady && env != null) {
          // 1. Telemetry: register the streaming Source. Wrapped so a metrics hiccup cannot block
          //    shuffle execution; the metric values still update on the holder regardless.
          try {
            val source = new StreamingShuffleSource(streamingMetrics)
            env.metricsSystem.registerSource(source)
            metricsSource = source
          } catch {
            case NonFatal(e) =>
              logWarning("Failed to register StreamingShuffleSource with the MetricsSystem", e)
          }

          // 2. Bounded-footprint guarantor: build the spill manager and start its 100 ms poll loop.
          //    It shares the same StreamingShuffleBlockResolver the writer tracks buffers in, so
          //    when a partition is spilled to disk the resolver is flipped from its in-memory entry
          //    to the on-disk block (trackSpill). Without this bridge a reduce-side read after a
          //    spill would resolve a reclaimed in-memory buffer instead of the spilled bytes.
          val spill = new MemorySpillManager(
            streamingConfig, env.blockManager, env.memoryManager, streamingMetrics,
            Some(streamingResolver), Some(fallbackPolicy))
          spill.start()
          spillManager = spill

          // 3. Flow control: start the timeout scan and register the executor-only backpressure RPC
          //    endpoint. registerIfExecutor returns None on the driver, honoring the
          //    executor-only contract at the single point of registration.
          backpressureProtocol.start()
          backpressureEndpoint =
            BackpressureRpcEndpoint.registerIfExecutor(env.rpcEnv, isDriver, backpressureProtocol)

          executorComponentsReady = true
          logInfo("Streaming shuffle executor components initialized (metrics source registered, " +
            s"spill poller and backpressure scan started, backpressureEndpoint defined=" +
            s"${backpressureEndpoint.isDefined}).")
        }
      }
    }
  }

  /**
   * The activation gate, evaluated once per shuffle at registration time. Streaming is engaged only
   * when the feature flag is on AND no [[StreamingShuffleFallbackPolicy]] revert condition holds.
   * The `spark.shuffle.manager=streaming` half of the activation contract was already satisfied by
   * the factory selecting this class, so only the feature flag and the fallback policy remain.
   *
   * @return true when this registration should mint a streaming handle; false to delegate to sort
   */
  private def useStreaming: Boolean = {
    // Production fallback wiring (registration-time half): pull a fresh executor-memory sample into
    // the policy BEFORE deciding, so a memory-bound executor reverts to the sort path on the very
    // first registration -- before any buffer is allocated and before the executor-side spill poll
    // loop has run. The continuous slow-consumer, network, and ongoing-memory samples are pushed by
    // the backpressure scan and the spill poll loop into this same policy instance; this refresh
    // covers the pre-allocation / first-registration memory-pressure case.
    refreshFallbackSignals()
    streamingConfig.enabled && !fallbackPolicy.shouldFallback
  }

  /**
   * Samples the executor's current on-heap memory utilization from the live [[SparkEnv]] memory
   * manager and feeds it to the fallback policy. This is the registration-time half of the
   * memory-pressure production signal wiring: it lets a registration decision observe pressure that
   * already exists on the executor (e.g. from other consumers) before this shuffle buffers a single
   * byte. Guarded on a live `SparkEnv`/`MemoryManager`, so it is a safe no-op on the driver, in
   * local-mode bootstrap, and in unit tests with no environment. Utilization is measured against
   * `maxOnHeapStorageMemory` -- the same denominator [[MemorySpillManager]] uses -- so the
   * registration-time and poll-loop memory samples are directly comparable.
   */
  private def refreshFallbackSignals(): Unit = {
    val env = SparkEnv.get
    if (env != null) {
      try {
        val mm = env.memoryManager
        val maxOnHeap = mm.maxOnHeapStorageMemory
        if (maxOnHeap > 0L) {
          val used = mm.storageMemoryUsed + mm.onHeapExecutionMemoryUsed
          val pct = math.min(100L, math.max(0L, math.round(used * 100.0 / maxOnHeap))).toInt
          fallbackPolicy.updateMemoryUtilization(pct)
        }
      } catch {
        case NonFatal(e) =>
          logDebug("Could not sample executor memory utilization for the fallback policy", e)
      }
    }
  }

  /**
   * Registers a shuffle and returns the handle that fixes its backend for the whole cluster. When
   * [[useStreaming]] holds, a [[StreamingShuffleHandle]] is minted carrying the resolved tuning
   * values; otherwise registration is delegated to the inner [[SortShuffleManager]] so the handle
   * (and therefore the entire shuffle) is a sort handle and the sort path is preserved unchanged.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (useStreaming) {
      // Mint a streaming handle. Its TYPE is the single, cluster-wide source of truth that this
      // shuffle is served by the streaming backend; the tuning values are resolved once here so the
      // writer and reader never re-read SparkConf (configuration is immutable for app lifetime).
      new StreamingShuffleHandle[K, V, C](
        shuffleId,
        dependency,
        streamingConfig.bufferSizePercent,
        streamingConfig.spillThreshold,
        streamingConfig.maxBandwidthMBps)
    } else {
      // Coexistence: streaming is disabled or a fallback condition tripped, so delegate to the
      // sort-based manager. The resulting sort handle ensures getWriter/getReader serve it
      // entirely from the sort path -- the sort-based shuffle is never bypassed.
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /**
   * Returns a writer for a map task. Dispatch is by handle TYPE alone (see the class-level note):
   * a [[StreamingShuffleHandle]] is served by a [[StreamingShuffleWriter]] wired with the shared
   * collaborators; any other handle is delegated to the inner [[SortShuffleManager]]. The
   * per-executor activation gate is NOT consulted here so the shuffle is served by the
   * same backend that minted its handle on the driver.
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        // Streaming handle => streaming writer. Ensure the executor-side collaborators (spill poll,
        // backpressure scan, metrics, RPC) are running before producing any output.
        ensureExecutorComponents()
        new StreamingShuffleWriter[K, V](
          streamingHandle,
          mapId,
          context,
          metrics,
          streamingConfig,
          streamingMetrics,
          backpressureProtocol,
          spillManager,
          transport,
          streamingResolver)
      case _ =>
        // Coexistence: a non-streaming handle was registered under the sort backend; serve it from
        // sort so the existing sort-based write path is preserved unchanged.
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Returns a reader for a range of map outputs and reduce partitions. This overrides the abstract
   * 7-arg form; the 5-arg [[ShuffleManager.getReader]] is `final` and forwards here, so it must NOT
   * be overridden. Dispatch is by handle TYPE alone, symmetric to [[getWriter]]: a
   * [[StreamingShuffleHandle]] is served by a [[StreamingShuffleReader]] and any other handle is
   * delegated to the inner [[SortShuffleManager]].
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
        // Streaming handle => streaming reader. The reader sources its serializerManager,
        // blockManager, and mapOutputTracker from the running SparkEnv via its default arguments.
        ensureExecutorComponents()
        new StreamingShuffleReader[K, C](
          streamingHandle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics,
          streamingConfig,
          streamingMetrics,
          transport,
          backpressureProtocol)
      case _ =>
        // Coexistence: delegate non-streaming handles to the sort reader, preserving the existing
        // read path unchanged.
        sortShuffleManager.getReader(
          handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Removes a shuffle's metadata. Streaming tracking (buffers and spilled-file entries) is
   * purged through the streaming resolver, and the call is also delegated to the inner
   * [[SortShuffleManager]] so a sort-handled (fallback) shuffle is cleaned. Each side is skipped
   * if its collaborator was never instantiated, so pure-streaming or pure-fallback deployments do
   * not force creation of the unused one.
   *
   * @return always true (cleanup is best-effort and idempotent, matching the sort manager)
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    val resolver = streamingResolverRef
    if (resolver != null) {
      resolver.untrackShuffle(shuffleId)
    }
    val sort = sortShuffleManagerRef
    if (sort != null) {
      sort.unregisterShuffle(shuffleId)
    }
    true
  }

  /**
   * Returns the resolver capable of retrieving shuffle block data. The streaming resolver serves
   * streaming blocks from memory or disk spill and delegates `.data`/`.index`/migration to an inner
   * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]], so it remains correct even when this
   * manager is running purely in fallback.
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = streamingResolver

  /**
   * Shuts the manager down in the mandated teardown order: backpressure -> spill -> inner sort ->
   * clear shuffle ids. Each step is isolated in its own `try`/`catch` so a failure in one never
   * prevents the others from running, and each is guarded so a collaborator that was never created
   * is simply skipped (no resurrection at shutdown).
   */
  override def stop(): Unit = {
    // 1. Backpressure: stop the timeout scan and unregister the executor-only RPC endpoint. Guarded
    //    on executorComponentsReady so a manager that never initialized executor components (the
    //    driver in cluster mode, or a unit test) does not force-create the protocol to stop it.
    if (executorComponentsReady) {
      try {
        backpressureProtocol.stop()
      } catch {
        case NonFatal(e) => logWarning("Error stopping the streaming backpressure protocol", e)
      }
      backpressureEndpoint.foreach { ref =>
        try {
          val env = SparkEnv.get
          if (env != null) {
            env.rpcEnv.stop(ref)
          }
        } catch {
          case NonFatal(e) => logWarning("Error stopping the backpressure RPC endpoint", e)
        }
      }
      backpressureEndpoint = None
    }

    // 2. Spill: shut down the 100 ms poll loop (idempotent; null when never initialized).
    val spill = spillManager
    if (spill != null) {
      try {
        spill.stop()
      } catch {
        case NonFatal(e) => logWarning("Error stopping the streaming memory spill manager", e)
      }
      spillManager = null
    }

    // 3. Inner sort manager: stop it only if it was ever instantiated, releasing the sort path's
    //    resources without forcing creation in a pure-streaming deployment.
    val sort = sortShuffleManagerRef
    if (sort != null) {
      try {
        sort.stop()
      } catch {
        case NonFatal(e) => logWarning("Error stopping the inner SortShuffleManager", e)
      }
    }

    // 4. Clear shuffle ids: stop the streaming resolver (which clears buffer/spill tracking maps
    //    and stops the inner IndexShuffleBlockResolver), then remove the metrics Source.
    val resolver = streamingResolverRef
    if (resolver != null) {
      try {
        resolver.stop()
      } catch {
        case NonFatal(e) => logWarning("Error stopping the streaming shuffle block resolver", e)
      }
    }
    val source = metricsSource
    if (source != null) {
      try {
        val env = SparkEnv.get
        if (env != null) {
          env.metricsSystem.removeSource(source)
        }
      } catch {
        case NonFatal(e) => logWarning("Error removing the StreamingShuffleSource", e)
      }
      metricsSource = null
    }
  }

  // -- Test-only accessors (package-private to org.apache.spark.shuffle.streaming) ----------------
  // These expose the manager's OWN collaborators so suites can drive and observe the real
  // registration-time fallback decision instead of asserting on a standalone, disconnected policy.
  // They are `private[streaming]`, so they never widen the public ShuffleManager SPI surface.

  /**
   * Test seam: the manager's own [[StreamingShuffleFallbackPolicy]] -- the exact instance consulted
   * by [[useStreaming]] and fed by the backpressure protocol and spill manager. A suite drives a
   * revert condition on this instance (or injects a clock-controlled policy at construction) and
   * then asserts that [[registerShuffle]] delegates to the inner [[SortShuffleManager]].
   */
  private[streaming] def fallbackPolicyForTesting: StreamingShuffleFallbackPolicy = fallbackPolicy

  /**
   * Test seam: the manager's own [[BackpressureProtocol]], so a suite can assert that the protocol
   * was constructed wired to the manager's fallback policy (the production slow-consumer/network
   * signal path) rather than to a disconnected instance.
   */
  private[streaming] def backpressureProtocolForTesting: BackpressureProtocol = backpressureProtocol
}
