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

import scala.util.control.NonFatal

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager,
  ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * The Service Provider Interface (SPI) entry point for the opt-in streaming shuffle data path
 * (feature F-101). This is the class that the reflective shuffle-manager factory resolves from
 * the `"streaming"` short-name alias (`spark.shuffle.manager=streaming`).
 *
 * '''Composition over replacement.''' This manager never replaces the built-in sort-based
 * shuffle. It '''composes''' an inner [[org.apache.spark.shuffle.sort.SortShuffleManager]] that
 * serves two roles: it is the delegation target whenever the streaming data path is not engaged
 * (the dual-flag `spark.shuffle.streaming.enabled` is `false`), and it is the safety-net fallback
 * for the sort-based path. When streaming is disabled every `registerShuffle` / `getWriter` /
 * `getReader` call is forwarded verbatim to the inner manager, so behavior is provably identical
 * to plain sort-based shuffle.
 *
 * '''Dispatch.''' Routing is driven by the shuffle handle type. [[registerShuffle]] returns a
 * [[StreamingShuffleHandle]] only when the streaming path is active; otherwise it returns
 * whatever handle the inner sort manager produces. [[getWriter]] and the seven-argument
 * [[getReader]] then pattern-match on the handle: a [[StreamingShuffleHandle]] is routed to the
 * streaming writer/reader, and every other handle is delegated to the inner sort manager. Because
 * a single handle type drives both the writer and the reader, the on-the-wire format never
 * desynchronizes between the two sides of a shuffle.
 *
 * '''Shared block resolver.''' The resolver returned from [[shuffleBlockResolver]] is the inner
 * sort manager's [[org.apache.spark.shuffle.IndexShuffleBlockResolver]], '''not''' the streaming
 * resolver, preserving the cast contract relied on by Spark internals and keeping block migration
 * / decommission state unified (see decision log ADR-16). The streaming in-memory block index is
 * maintained by a separate [[StreamingShuffleBlockResolver]] held internally and constructed over
 * that same shared index resolver.
 *
 * '''Observability.''' When a [[org.apache.spark.SparkEnv]] is available, the manager registers a
 * [[StreamingShuffleSource]] with the existing `MetricsSystem` so the four streaming metrics
 * surface through the already-configured JMX / Prometheus / CSV / SLF4J sinks with no new
 * endpoint.
 *
 * '''Executor-only collaborators.''' The backpressure RPC endpoint and the memory spill monitor
 * are created '''only on executors''' (`!isDriver`), guarded so the driver's startup and the
 * zero-modification boundary are preserved. The lock-free [[BackpressureProtocol]] and the
 * decision-only [[StreamingShuffleFallbackPolicy]] are constructed lazily wherever a `SparkEnv`
 * exists (including the driver in local mode, where reduce tasks run in-process).
 *
 * '''Reflective construction contract.''' `SparkEnv` instantiates this class via
 * `ShuffleManager.create(conf, isDriver)`, which tries the `(SparkConf, Boolean)` constructor
 * first. The `isDriver` flag is therefore required and is used to gate the executor-only
 * collaborators.
 *
 * This class is `private[spark]`; it introduces a new internal class and no new public,
 * binary-compatible API.
 *
 * @param conf     the application [[SparkConf]] used to construct the inner sort manager, resolve
 *                 the streaming configuration, and reach the active `SparkEnv` collaborators
 * @param isDriver `true` when this manager runs on the driver, `false` on an executor; gates the
 *                 executor-only backpressure endpoint and spill monitor
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  // ------------------------------------------------------------------------------------------
  // Eagerly constructed, driver-safe collaborators
  // ------------------------------------------------------------------------------------------

  /**
   * The inner sort-based manager. It is the delegation target when streaming is disabled and the
   * fallback target for the sort-based path; it is never bypassed. Constructed eagerly because
   * its own SparkEnv-dependent state is internally lazy, exactly as on the plain sort path.
   */
  private[this] val sortShuffleManager = new SortShuffleManager(conf)

  /**
   * Typed, immutable view of the `spark.shuffle.streaming.*` configuration. Validated once at
   * construction so a misconfigured tuning range fails fast rather than at first task execution.
   */
  private[this] val streamingConfig = new StreamingShuffleConfig(conf)
  streamingConfig.validate()

  /**
   * The block resolver exposed to the rest of Spark. This is intentionally the inner sort
   * manager's [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] (its static type), because
   * `BlockManager` casts `ShuffleManager.shuffleBlockResolver` to `IndexShuffleBlockResolver`.
   * The inner sort manager owns this resolver's lifecycle, so [[stop]] does not stop it directly.
   */
  override val shuffleBlockResolver: ShuffleBlockResolver =
    sortShuffleManager.shuffleBlockResolver

  /**
   * Internal streaming block-index collaborator. It shares the inner sort manager's
   * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] (injected, not separately constructed)
   * so migration/decommission state stays unified, and it maintains the in-memory index of
   * in-flight streaming blocks. It is held privately and is never returned from
   * [[shuffleBlockResolver]].
   */
  private[this] val streamingBlockResolver =
    new StreamingShuffleBlockResolver(sortShuffleManager.shuffleBlockResolver)

  /**
   * Shuffle ids registered onto the streaming path, tracked for [[unregisterShuffle]] / [[stop]]
   * bookkeeping. A concurrent key set is used because registration and unregistration can be
   * driven from different scheduler threads.
   */
  private[this] val registeredStreamingShuffleIds = ConcurrentHashMap.newKeySet[Integer]()

  // ------------------------------------------------------------------------------------------
  // Lazy, runtime-gated collaborators (constructed only when a SparkEnv is available)
  // ------------------------------------------------------------------------------------------

  /**
   * Mutable holder for the four streaming-shuffle metrics; shared by every collaborator below.
   */
  private[this] lazy val streamingMetrics: StreamingShuffleMetrics = new StreamingShuffleMetrics

  /**
   * The consumer-to-producer flow-control protocol used by the streaming reader. Present wherever
   * a `SparkEnv` exists (executors, and the driver in local mode where reduce tasks run
   * in-process); `None` otherwise. The protocol holds only in-memory atomics, so it is safe to
   * construct on the driver.
   */
  private[this] lazy val backpressureOpt: Option[BackpressureProtocol] = {
    if (SparkEnv.get != null) {
      val linkCapacityBytes =
        if (streamingConfig.maxBandwidthMBps > 0) {
          streamingConfig.maxBandwidthMBps.toLong * 1024L * 1024L
        } else {
          0L
        }
      Some(new BackpressureProtocol(
        streamingMetrics,
        linkCapacityBytes,
        streamingConfig.maxBandwidthMBps,
        streamingConfig.spillThreshold))
    } else {
      None
    }
  }

  /**
   * The memory spill monitor. Created '''only on executors''' (`!isDriver`) because it starts a
   * daemon polling thread and reads the executor memory manager; never created on the driver,
   * which runs no shuffle buffers (preserving the zero-modification boundary at driver startup).
   */
  private[this] lazy val spillManagerOpt: Option[MemorySpillManager] = {
    if (!isDriver && SparkEnv.get != null) {
      Some(new MemorySpillManager(streamingMetrics, streamingConfig.spillThreshold))
    } else {
      None
    }
  }

  /**
   * The decision-only degradation policy. Present wherever a `SparkEnv` exists; it performs no
   * I/O and mutates no state, so it is safe to construct on the driver. It is consulted by
   * [[getWriter]] for the one degradation condition observable at dispatch time (memory
   * pressure).
   */
  private[this] lazy val fallbackPolicyOpt: Option[StreamingShuffleFallbackPolicy] = {
    if (SparkEnv.get != null) {
      Some(new StreamingShuffleFallbackPolicy(streamingConfig, streamingMetrics))
    } else {
      None
    }
  }

  /**
   * The [[org.apache.spark.rpc.RpcEndpointRef]] of the executor-only backpressure endpoint, or
   * `None` on the driver. Registration is delegated to [[BackpressureRpcEndpoint.register]],
   * which hard-enforces the executor-only contract by returning `None` when `isDriver` is `true`.
   * The ref is retained so the endpoint can be unregistered deterministically in [[stop]].
   */
  private[this] val backpressureEndpointRefOpt: Option[RpcEndpointRef] = {
    if (!isDriver && SparkEnv.get != null) {
      backpressureOpt.flatMap { protocol =>
        BackpressureRpcEndpoint.register(
          SparkEnv.get.rpcEnv, protocol, isDriver, streamingConfig.debug)
      }
    } else {
      None
    }
  }

  // ------------------------------------------------------------------------------------------
  // Construction-time wiring
  // ------------------------------------------------------------------------------------------

  // Register the metrics source with the existing MetricsSystem when a SparkEnv is available
  // (both driver and executors). MetricsSystem.registerSource already tolerates a duplicate
  // registry name; the extra guard keeps a transient failure from aborting manager construction.
  if (SparkEnv.get != null) {
    try {
      SparkEnv.get.metricsSystem.registerSource(new StreamingShuffleSource(streamingMetrics))
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to register the streaming shuffle metrics source: " +
          log"${MDC(ERROR, e.getMessage)}")
    }
  }

  // Start the executor-only spill monitor (idempotent; a no-op on the driver, where the option is
  // None). The poller is started here so memory protection is active before the first map task.
  spillManagerOpt.foreach(_.start())

  logInfo(s"StreamingShuffleManager initialized (isDriver=$isDriver, " +
    s"streamingEnabled=${streamingConfig.enabled}, " +
    s"bufferSizePercent=${streamingConfig.bufferSizePercent}, " +
    s"spillThreshold=${streamingConfig.spillThreshold}, " +
    s"maxBandwidthMBps=${streamingConfig.maxBandwidthMBps})")

  // ------------------------------------------------------------------------------------------
  // ShuffleManager contract
  // ------------------------------------------------------------------------------------------

  /**
   * Register a shuffle, returning a [[StreamingShuffleHandle]] when the streaming path is active
   * '''and''' no automatic-fallback condition holds, and delegating to the inner sort manager
   * otherwise. The handle carries the per-shuffle tuning parameters so the writer and reader
   * honor them without re-reading the [[SparkConf]].
   *
   * Automatic fallback (feature F-111) is decided here, '''per shuffle''': when
   * [[registrationFallbackReason]] reports a triggered condition the entire shuffle is registered
   * on the inner [[org.apache.spark.shuffle.sort.SortShuffleManager]], so [[getWriter]] and
   * [[getReader]] consistently route it to the sort-based path. See decision log ADR-14.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (streamingActive) {
      registrationFallbackReason() match {
        case Some(reason) =>
          logInfo(log"Streaming shuffle falling back to sort-based shuffle for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} (reason=${MDC(REASON, reason.message)})")
          sortShuffleManager.registerShuffle(shuffleId, dependency)
        case None =>
          registeredStreamingShuffleIds.add(Integer.valueOf(shuffleId))
          logDebug(log"Registering shuffle ${MDC(SHUFFLE_ID, shuffleId)} on the " +
            log"streaming data path")
          new StreamingShuffleHandle(
            shuffleId,
            dependency,
            streamingConfig.bufferSizePercent,
            streamingConfig.spillThreshold,
            streamingConfig.maxBandwidthMBps)
      }
    } else {
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /**
   * Get a writer for a map task. A [[StreamingShuffleHandle]] is routed to a
   * [[StreamingShuffleWriter]]; any other handle is delegated to the inner sort manager. A handle
   * is a [[StreamingShuffleHandle]] only when [[registerShuffle]] already determined that no
   * automatic-fallback condition held for the shuffle, so no per-task fallback check is performed
   * here (see [[registrationFallbackReason]]).
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        // Inject the shared IndexShuffleBlockResolver so the writer's framed output is committed
        // to the same fetchable channel reducers read from, and the executor's MemorySpillManager
        // so the writer registers its buffers for 100 ms utilization monitoring / threshold
        // spilling and shares one ordered spill ledger with the monitor (features F-103 / F-109
        // wiring).
        new StreamingShuffleWriter(
          streamingHandle,
          mapId,
          context,
          metrics,
          streamingConfig,
          streamingBlockResolver.indexResolver,
          spillManagerOpt)
      case other =>
        sortShuffleManager.getWriter(other, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for a range of map outputs and reduce partitions (the seven-argument overload;
   * the five-argument overload is `final` on the trait and is not overridden). A
   * [[StreamingShuffleHandle]] is routed to a [[StreamingShuffleReader]] using the shared
   * backpressure protocol and metrics; any other handle is delegated to the inner sort manager.
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
        val protocol = backpressureOpt.getOrElse {
          throw new IllegalStateException(
            "StreamingShuffleReader requires the executor-side BackpressureProtocol, but no " +
              "active SparkEnv is available to construct it")
        }
        new StreamingShuffleReader(
          streamingHandle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics,
          protocol,
          streamingMetrics,
          spillManagerOpt = spillManagerOpt)
      case other =>
        sortShuffleManager.getReader(
          other, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Remove a shuffle's metadata. Local streaming bookkeeping (the tracked id, the in-memory
   * streaming block index, and the backpressure protocol's per-stream flow-control state for the
   * shuffle) is cleared, and the call is delegated to the inner sort manager, which owns the
   * materialized index/data files via the shared resolver. The delegated boolean is returned.
   *
   * The backpressure eviction is '''unconditional''' -- it is not gated on
   * [[registeredStreamingShuffleIds]], because that set is populated only on the driver (by
   * [[registerShuffle]]) whereas the leaking per-stream maps are populated '''consumer-side''',
   * once per reduce-task attempt, by [[StreamingShuffleReader]] on each executor. Spark invokes
   * `unregisterShuffle` on every executor when a shuffle is reclaimed (the block manager's
   * `RemoveShuffle` broadcast), so evicting here keeps those maps bounded by the set of live
   * shuffles on every node -- mirroring how the inner sort manager reclaims its own per-shuffle
   * executor-side state. See QA Issue #1.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    registeredStreamingShuffleIds.remove(Integer.valueOf(shuffleId))
    backpressureOpt.foreach(_.removeShuffle(shuffleId))
    streamingBlockResolver.removeStreamingShuffle(shuffleId)
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /**
   * Shut down the manager in a deterministic order, each step guarded so a single failure cannot
   * skip the remaining teardown:
   *
   *  1. Backpressure: unregister the executor-only RPC endpoint (the protocol itself holds only
   *     in-memory atomics, so there is nothing else to close).
   *  2. Spill: stop the memory spill monitor (idempotent).
   *  3. Inner sort manager: this also stops the shared [[shuffleBlockResolver]], so it is the
   *     sole owner of that resolver's teardown.
   *  4. Streaming state: clear the in-memory streaming block index (which does not re-stop the
   *     shared resolver) and forget the tracked streaming shuffle ids.
   */
  override def stop(): Unit = {
    try {
      stopBackpressureEndpoint()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to unregister the " +
          log"${MDC(ENDPOINT_NAME, BackpressureRpcEndpoint.ENDPOINT_NAME)} endpoint during " +
          log"stop: ${MDC(ERROR, e.getMessage)}")
    }

    try {
      stopSpillManager()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to stop the streaming shuffle spill manager during stop: " +
          log"${MDC(ERROR, e.getMessage)}")
    }

    try {
      stopInnerSortManager()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to stop the inner SortShuffleManager during stop: " +
          log"${MDC(ERROR, e.getMessage)}")
    }

    try {
      clearStreamingState()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to clear streaming shuffle state during stop: " +
          log"${MDC(ERROR, e.getMessage)}")
    }
  }

  /**
   * Teardown step 1 of [[stop]]: unregister the executor-only backpressure RPC endpoint (a no-op
   * on the driver, where no endpoint was registered; the protocol itself holds only in-memory
   * atomics). Exposed as an overridable seam so tests can observe the deterministic
   * Backpressure -> Spill -> Sort teardown order.
   */
  protected def stopBackpressureEndpoint(): Unit = {
    backpressureEndpointRefOpt.foreach { ref =>
      val env = SparkEnv.get
      if (env != null) {
        env.rpcEnv.stop(ref)
      }
    }
  }

  /**
   * Teardown step 2 of [[stop]]: stop the executor-only memory spill monitor (idempotent; a no-op
   * on the driver, where the option is `None`). Overridable teardown seam (see step 1).
   */
  protected def stopSpillManager(): Unit = spillManagerOpt.foreach(_.stop())

  /**
   * Teardown step 3 of [[stop]]: stop the inner sort manager, which also stops the shared
   * [[shuffleBlockResolver]] (so it is the sole owner of that resolver's teardown). Overridable
   * seam; see [[stopBackpressureEndpoint]].
   */
  protected def stopInnerSortManager(): Unit = sortShuffleManager.stop()

  /**
   * Teardown step 4 of [[stop]]: empty the backpressure protocol's per-stream flow-control maps
   * (so they return to their baseline even for any shuffles still live at shutdown), clear the
   * in-memory streaming block index (which does not re-stop the shared resolver), and forget the
   * tracked streaming shuffle ids. Overridable seam; see [[stopBackpressureEndpoint]].
   */
  protected def clearStreamingState(): Unit = {
    backpressureOpt.foreach(_.clear())
    streamingBlockResolver.stop()
    registeredStreamingShuffleIds.clear()
  }

  // ------------------------------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------------------------------

  /**
   * Whether the streaming data path is active under the dual-flag activation contract: the
   * `"streaming"` alias must be selected '''and''' `spark.shuffle.streaming.enabled` must be
   * `true`. Delegates to [[StreamingShuffleConfig.active]]; selection by fully-qualified class
   * name leaves the path disengaged. See decision log ADR-02.
   */
  private def streamingActive: Boolean = streamingConfig.active

  /**
   * Evaluate the automatic-fallback policy (feature F-111) at shuffle registration time and
   * return the highest-priority triggered [[StreamingShuffleFallbackPolicy.FallbackReason]], or
   * `None` when the streaming data path may proceed.
   *
   * The four policy conditions are wired to the signals observable on the driver at registration:
   * memory pressure (the active memory manager's on-heap storage budget), consumer lag (the
   * backpressure protocol's consumer-liveness detector), network saturation, and a
   * [[producerStreamingVersion]] vs [[consumerStreamingVersion]] mismatch. Fallback is decided
   * per shuffle at registration; see decision log ADR-14 (and deviations D-2 and D-3 for the v1
   * network-saturation and version-mismatch signals).
   *
   * Returns `None` when no fallback policy is available (no active `SparkEnv`), which occurs only
   * in isolated unit tests; on a real driver a `SparkEnv` is always present.
   *
   * @return the triggered fallback reason, or `None` to proceed with the streaming data path
   */
  private[streaming] def registrationFallbackReason()
      : Option[StreamingShuffleFallbackPolicy.FallbackReason] = {
    fallbackPolicyOpt.flatMap { policy =>
      val env = SparkEnv.get
      val canAllocate =
        env != null && env.memoryManager != null && env.memoryManager.maxOnHeapStorageMemory > 0L
      val consumerLagging = backpressureOpt.exists(_.timedOutStreams.nonEmpty)
      val (producerRate, consumerRate, sustainedMs) =
        if (consumerLagging) {
          (1.0, 0.0, StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_DURATION_MS + 1L)
        } else {
          (0.0, 0.0, 0L)
        }
      val networkUtilizationFraction = 0.0
      policy.evaluate(
        producerRate,
        consumerRate,
        sustainedMs,
        canAllocate,
        networkUtilizationFraction,
        producerStreamingVersion,
        consumerStreamingVersion)
    }
  }

  /**
   * The streaming-subsystem version reported by the producing side, used by
   * [[registrationFallbackReason]]'s version-mismatch check. Defaults to the running Spark
   * version; exposed as an overridable seam so tests can simulate a rolling-version mismatch.
   */
  private[streaming] def producerStreamingVersion: String = org.apache.spark.SPARK_VERSION

  /**
   * The streaming-subsystem version reported by the consuming side; see
   * [[producerStreamingVersion]].
   */
  private[streaming] def consumerStreamingVersion: String = org.apache.spark.SPARK_VERSION

  // ------------------------------------------------------------------------------------------
  // Package-private accessors for tests and observability
  // ------------------------------------------------------------------------------------------

  /** Whether the streaming data path is active for this manager. */
  private[streaming] def isStreamingActive: Boolean = streamingActive

  /** The resolved streaming configuration backing this manager. */
  private[streaming] def streamingShuffleConfig: StreamingShuffleConfig = streamingConfig

  /** The inner sort-based manager used for delegation and fallback. */
  private[streaming] def innerSortShuffleManager: SortShuffleManager = sortShuffleManager

  /** The shared metrics holder exposed through [[StreamingShuffleSource]]. */
  private[streaming] def streamingMetricsHolder: StreamingShuffleMetrics = streamingMetrics

  /** The backpressure protocol, present wherever a `SparkEnv` is available. */
  private[streaming] def backpressureProtocol: Option[BackpressureProtocol] = backpressureOpt

  /** The memory spill monitor, present only on executors. */
  private[streaming] def memorySpillManager: Option[MemorySpillManager] = spillManagerOpt

  /** The decision-only fallback policy, present wherever a `SparkEnv` is available. */
  private[streaming] def fallbackPolicy: Option[StreamingShuffleFallbackPolicy] =
    fallbackPolicyOpt

  /** The backpressure RPC endpoint ref, present only on executors. */
  private[streaming] def backpressureEndpointRef: Option[RpcEndpointRef] =
    backpressureEndpointRefOpt

  /** The internal streaming block-index resolver. */
  private[streaming] def streamingBlockIndexResolver: StreamingShuffleBlockResolver =
    streamingBlockResolver

  /** The number of shuffle ids currently registered on the streaming path. */
  private[streaming] def registeredStreamingShuffleCount: Int =
    registeredStreamingShuffleIds.size()
}
