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
 * resolver. Spark internals (notably `BlockManager.diagnoseShuffleBlockCorruption`) cast
 * `ShuffleManager.shuffleBlockResolver` directly to `IndexShuffleBlockResolver`, so exposing the
 * shared index resolver preserves that contract and keeps block migration / decommission state
 * unified. The streaming in-memory block index is maintained by a separate
 * [[StreamingShuffleBlockResolver]] held internally and constructed over that same shared index
 * resolver.
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
        logWarning(s"Failed to register the streaming shuffle metrics source: ${e.getMessage}")
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
   * and delegating to the inner sort manager otherwise. The handle carries the per-shuffle tuning
   * parameters so the writer and reader honor them without re-reading the [[SparkConf]].
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (streamingActive) {
      registeredStreamingShuffleIds.add(Integer.valueOf(shuffleId))
      logDebug(s"Registering shuffle $shuffleId on the streaming data path")
      new StreamingShuffleHandle(
        shuffleId,
        dependency,
        streamingConfig.bufferSizePercent,
        streamingConfig.spillThreshold,
        streamingConfig.maxBandwidthMBps)
    } else {
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /**
   * Get a writer for a map task. A [[StreamingShuffleHandle]] is routed to a
   * [[StreamingShuffleWriter]]; any other handle is delegated to the inner sort manager. The
   * fallback policy is consulted for memory pressure before constructing the streaming writer
   * (see [[warnIfMemoryPressure]]).
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        warnIfMemoryPressure(streamingHandle.shuffleId, mapId)
        new StreamingShuffleWriter(streamingHandle, mapId, context, metrics, streamingConfig)
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
          streamingMetrics)
      case other =>
        sortShuffleManager.getReader(
          other, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Remove a shuffle's metadata. Local streaming bookkeeping (the tracked id and the in-memory
   * streaming block index for the shuffle) is cleared, and the call is delegated to the inner
   * sort manager, which owns the materialized index/data files via the shared resolver. The
   * delegated boolean is returned.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    registeredStreamingShuffleIds.remove(Integer.valueOf(shuffleId))
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
      backpressureEndpointRefOpt.foreach { ref =>
        val env = SparkEnv.get
        if (env != null) {
          env.rpcEnv.stop(ref)
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to unregister the ${BackpressureRpcEndpoint.ENDPOINT_NAME} " +
          s"endpoint during stop: ${e.getMessage}")
    }

    try {
      spillManagerOpt.foreach(_.stop())
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to stop the streaming shuffle spill manager during stop: " +
          s"${e.getMessage}")
    }

    try {
      sortShuffleManager.stop()
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to stop the inner SortShuffleManager during stop: ${e.getMessage}")
    }

    try {
      streamingBlockResolver.stop()
      registeredStreamingShuffleIds.clear()
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to clear streaming shuffle state during stop: ${e.getMessage}")
    }
  }

  // ------------------------------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------------------------------

  /**
   * Whether the streaming data path is active. The decisive gate is
   * `spark.shuffle.streaming.enabled`.
   *
   * The second half of the dual-flag activation contract (`spark.shuffle.manager=streaming`) is
   * already satisfied by the very fact that this manager was instantiated: the reflective factory
   * only resolves this class when streaming was selected, whether by the `"streaming"` alias or
   * by the fully-qualified class name. Re-checking the literal `spark.shuffle.manager` value here
   * would incorrectly disable streaming when it is selected by class name (the value would be the
   * FQCN, not `"streaming"`), so the check is intentionally not duplicated.
   */
  private def streamingActive: Boolean = streamingConfig.enabled

  /**
   * Consult the fallback policy for the one degradation condition observable when a writer is
   * created -- memory pressure that would prevent streaming-buffer allocation -- and surface it
   * for operators.
   *
   * The writer type is intentionally '''not''' switched per map task. A
   * [[StreamingShuffleHandle]] is created once per shuffle on the driver and shipped to every
   * task, and the reader dispatches on that same handle type; swapping an individual writer to
   * the sort path would desynchronize the on-the-wire format from what [[StreamingShuffleReader]]
   * expects and break the zero-data-loss invariant. Graceful degradation under memory pressure is
   * instead provided by the streaming writer's own cooperative spilling. The remaining fallback
   * conditions (sustained consumer lag, network saturation, version mismatch) depend on runtime
   * signals that are not available at dispatch time and are evaluated by the
   * writer/reader/backpressure layers as those signals flow.
   */
  private def warnIfMemoryPressure(shuffleId: Int, mapId: Long): Unit = {
    fallbackPolicyOpt.foreach { policy =>
      val env = SparkEnv.get
      val canAllocate =
        env != null && env.memoryManager != null && env.memoryManager.maxOnHeapStorageMemory > 0L
      if (policy.shouldFallbackForMemoryPressure(canAllocate)) {
        logWarning(s"Streaming shuffle detected memory pressure creating a writer for shuffle " +
          s"$shuffleId map $mapId; the streaming writer will spill to disk to degrade gracefully")
      }
    }
  }

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
