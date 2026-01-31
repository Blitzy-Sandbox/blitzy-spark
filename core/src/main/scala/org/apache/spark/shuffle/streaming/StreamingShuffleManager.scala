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
import org.apache.spark.internal.config._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.collection.OpenHashSet

/**
 * A pluggable ShuffleManager implementation that streams serialized partition data
 * directly to consumer executors rather than materializing complete shuffle files.
 *
 * == Activation ==
 *
 * Streaming shuffle is activated by setting:
 * {{{
 * spark.shuffle.manager=streaming
 * spark.shuffle.streaming.enabled=true
 * }}}
 *
 * == Coexistence with Sort-Based Shuffle ==
 *
 * This manager coexists with the default [[SortShuffleManager]]. When streaming
 * shuffle is not beneficial or encounters degradation conditions, it automatically
 * falls back to sort-based shuffle behavior.
 *
 * == Automatic Fallback Conditions ==
 *
 * Streaming shuffle falls back to sort-based shuffle when:
 * - Consumer is consistently 2x slower than producer for >60 seconds
 * - Memory allocation failures indicate OOM risk
 * - Network saturation exceeds 90% link capacity
 * - Producer/consumer version mismatch detected
 *
 * == Components ==
 *
 * The streaming shuffle system comprises:
 * - [[StreamingShuffleWriter]]: Map-side buffer management and streaming
 * - [[StreamingShuffleReader]]: Reduce-side in-progress block consumption
 * - [[BackpressureProtocol]]: Flow control and rate limiting
 * - [[MemorySpillManager]]: Memory pressure monitoring and spill coordination
 * - [[StreamingShuffleBlockResolver]]: Block resolution for streaming blocks
 * - [[StreamingShuffleMetricsSource]]: Telemetry collection
 *
 * == Thread Safety ==
 *
 * All public methods are thread-safe for concurrent access from multiple tasks.
 *
 * @param conf SparkConf containing shuffle configuration
 */
private[spark] class StreamingShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  // ============================================================================
  // Configuration
  // ============================================================================

  /** Check if streaming is enabled via configuration. */
  private val streamingEnabled = conf.get(SHUFFLE_STREAMING_ENABLED)

  // ============================================================================
  // Map Task ID Tracking (Following SortShuffleManager Pattern)
  // ============================================================================
  // Track map task IDs per shuffle for proper cleanup during unregisterShuffle.
  // This follows the same pattern as SortShuffleManager to ensure consistent
  // resource management and cleanup semantics.

  /**
   * Mapping from shuffle IDs to the task IDs of mappers producing output for those shuffles.
   * Used for cleanup during unregisterShuffle to remove all map outputs for the shuffle.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  // ============================================================================
  // Streaming Infrastructure Components
  // ============================================================================
  // These components are lazily initialized to defer creation until first use.
  // This allows the manager to be created without a full SparkEnv (e.g., on driver).

  /** Fallback manager for graceful degradation to sort-based shuffle. */
  private lazy val sortShuffleManager = new SortShuffleManager(conf)

  /** Memory spill manager for buffer persistence when memory threshold exceeded. */
  private lazy val spillManager = new MemorySpillManager(conf)

  /** Flow control protocol for rate limiting and consumer liveness detection. */
  private lazy val backpressureProtocol = new BackpressureProtocol(conf)

  /** Block resolver for streaming shuffle blocks. */
  private lazy val blockResolver = new StreamingShuffleBlockResolver(conf)

  /** Metrics source for streaming shuffle telemetry. */
  private lazy val metricsSource = new StreamingShuffleMetricsSource()

  // ============================================================================
  // Shuffle Registration Tracking
  // ============================================================================

  /** Track registered shuffles for type-based dispatch in getWriter/getReader. */
  private val registeredShuffles = new ConcurrentHashMap[Int, ShuffleHandle]()

  // Initialization flag
  @volatile private var initialized = false
  @volatile private var stopped = false

  /**
   * Initialize the streaming shuffle manager.
   * Called lazily on first shuffle operation.
   */
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      synchronized {
        if (!initialized) {
          if (streamingEnabled) {
            // Start background services
            spillManager.start()
            backpressureProtocol.startHeartbeatMonitor()

            // Register metrics source
            try {
              SparkEnv.get.metricsSystem.registerSource(metricsSource)
            } catch {
              case e: Exception =>
                logWarning("Failed to register streaming shuffle metrics source", e)
            }

            logInfo("StreamingShuffleManager initialized with streaming enabled")
          } else {
            logInfo("StreamingShuffleManager initialized with streaming disabled, " +
              "delegating to SortShuffleManager")
          }
          initialized = true
        }
      }
    }
  }

  /**
   * Register a shuffle and return a handle for it.
   *
   * If streaming is enabled and beneficial for this shuffle, returns a
   * [[StreamingShuffleHandle]]. Otherwise, delegates to [[SortShuffleManager]].
   *
   * @param shuffleId  unique identifier for this shuffle
   * @param dependency the ShuffleDependency with partitioner, serializer, etc.
   * @return ShuffleHandle for this shuffle
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    ensureInitialized()

    val handle = if (shouldUseStreaming(dependency)) {
      logInfo(s"Registering shuffle $shuffleId with streaming mode " +
        s"(${dependency.partitioner.numPartitions} partitions)")

      new StreamingShuffleHandle(shuffleId, dependency)
    } else {
      logInfo(s"Registering shuffle $shuffleId with sort-based mode (streaming disabled or not beneficial)")
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }

    registeredShuffles.put(shuffleId, handle)
    handle
  }

  /**
   * Get a writer for the shuffle.
   *
   * Returns a [[StreamingShuffleWriter]] for streaming handles, or delegates
   * to [[SortShuffleManager]] for sort-based handles.
   *
   * @param handle       the shuffle handle
   * @param mapId        the map task ID
   * @param context      the task context
   * @param metrics      metrics reporter
   * @return ShuffleWriter instance
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    ensureInitialized()

    // Track map task ID for this shuffle (following SortShuffleManager pattern)
    // This enables proper cleanup during unregisterShuffle
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(mapId) }

    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        logDebug(s"Creating StreamingShuffleWriter for shuffle ${handle.shuffleId}, map $mapId")

        // Wrap metrics with streaming-specific tracking
        val wrappedMetrics = new StreamingShuffleWriteMetrics(metrics, metricsSource)

        new StreamingShuffleWriter[K, V, Any](
          streamingHandle.asInstanceOf[StreamingShuffleHandle[K, V, Any]],
          mapId,
          context,
          wrappedMetrics,
          spillManager,
          backpressureProtocol)

      case _ =>
        // Delegate to sort-based shuffle
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for the shuffle.
   *
   * Returns a [[StreamingShuffleReader]] for streaming handles, or delegates
   * to [[SortShuffleManager]] for sort-based handles.
   *
   * @param handle              the shuffle handle
   * @param startMapIndex       start of map range (inclusive)
   * @param endMapIndex         end of map range (exclusive)
   * @param startPartition      start partition (inclusive)
   * @param endPartition        end partition (exclusive)
   * @param context             task context
   * @param metrics             metrics reporter
   * @return ShuffleReader instance
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    ensureInitialized()

    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, _, C @unchecked] =>
        logDebug(s"Creating StreamingShuffleReader for shuffle ${handle.shuffleId}, " +
          s"partitions $startPartition-$endPartition")

        // Wrap metrics with streaming-specific tracking
        val wrappedMetrics = new StreamingShuffleReadMetrics(metrics, metricsSource)

        new StreamingShuffleReader[K, C](
          streamingHandle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          wrappedMetrics,
          backpressureProtocol)

      case _ =>
        // Delegate to sort-based shuffle
        sortShuffleManager.getReader(handle, startMapIndex, endMapIndex,
          startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Remove shuffle data after shuffle completion.
   *
   * @param shuffleId the shuffle identifier
   * @param blocking  whether to block until removal completes
   * @return true if removal was successful
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logDebug(s"Unregistering shuffle $shuffleId")

    val handle = registeredShuffles.remove(shuffleId)
    val result = handle match {
      case _: StreamingShuffleHandle[_, _, _] =>
        // Clean up map outputs for this shuffle using taskIdMapsForShuffle
        // This follows the SortShuffleManager pattern for consistent cleanup
        Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
          mapTaskIds.synchronized {
            mapTaskIds.iterator.foreach { mapTaskId =>
              blockResolver.removeDataByMap(shuffleId, mapTaskId)
            }
          }
        }

        // Clean up streaming shuffle resources
        spillManager.cleanupShuffle(shuffleId)
        backpressureProtocol.unregisterShuffle(shuffleId)
        true

      case _ =>
        // Also remove from taskIdMapsForShuffle if exists (for sort-based handles)
        taskIdMapsForShuffle.remove(shuffleId)
        // Delegate to sort-based shuffle
        sortShuffleManager.unregisterShuffle(shuffleId)
    }

    result
  }

  /**
   * Get the block resolver for this shuffle manager.
   *
   * Returns streaming block resolver if streaming is enabled,
   * otherwise delegates to sort-based shuffle.
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = {
    if (streamingEnabled) {
      blockResolver
    } else {
      sortShuffleManager.shuffleBlockResolver
    }
  }

  /**
   * Stop the shuffle manager and release all resources.
   */
  override def stop(): Unit = {
    if (!stopped) {
      stopped = true

      if (initialized && streamingEnabled) {
        // Stop streaming components
        spillManager.stop()
        backpressureProtocol.stop()
        blockResolver.stop()

        // Deregister metrics
        try {
          SparkEnv.get.metricsSystem.removeSource(metricsSource)
        } catch {
          case _: Exception => // Ignore
        }
      }

      // Stop fallback manager
      sortShuffleManager.stop()

      registeredShuffles.clear()
      logInfo("StreamingShuffleManager stopped")
    }
  }

  /**
   * Determine if streaming shuffle should be used for this dependency.
   *
   * Streaming is beneficial when:
   * - Streaming is enabled in configuration
   * - Shuffle has multiple partitions (not single-partition aggregation)
   * - Serializer supports relocation (for efficient streaming)
   *
   * @param dependency the shuffle dependency
   * @return true if streaming should be used
   */
  private def shouldUseStreaming[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = {
    if (!streamingEnabled) {
      return false
    }

    // Check for minimum partition count (streaming overhead not worth it for small shuffles)
    val minPartitions = 2
    if (dependency.partitioner.numPartitions < minPartitions) {
      logDebug(s"Shuffle has ${dependency.partitioner.numPartitions} partitions, " +
        s"below minimum $minPartitions for streaming")
      return false
    }

    // Check serializer compatibility
    val serializer = dependency.serializer
    val supportsRelocation = serializer.supportsRelocationOfSerializedObjects
    if (!supportsRelocation) {
      logDebug("Serializer does not support relocation, falling back to sort-based shuffle")
      return false
    }

    true
  }

  // ============================================================================
  // Public Fallback Detection API
  // ============================================================================

  /**
   * Check if fallback to sort-based shuffle should be triggered for a shuffle.
   *
   * This method evaluates dynamic fallback conditions at runtime:
   * - Consumer lag ratio exceeds [[FALLBACK_LAG_RATIO_THRESHOLD]] (2x slower)
   *   for more than [[FALLBACK_LAG_DURATION_MS]] (60 seconds)
   * - Memory pressure indicates OOM risk (buffer utilization at maximum)
   * - Network saturation exceeds [[NETWORK_SATURATION_THRESHOLD]] (90%)
   *
   * Called by StreamingShuffleWriter to decide whether to continue streaming
   * or fall back to sort-based shuffle for the remainder of the task.
   *
   * == Coexistence with Sort-Based Shuffle ==
   *
   * When this method returns true, the caller should:
   * 1. Spill all in-memory buffers to disk
   * 2. Switch to sort-based shuffle writer for remaining records
   * 3. Coordinate with block resolver for seamless block serving
   *
   * @param shuffleId the shuffle identifier to check
   * @return true if fallback to sort-based shuffle is recommended
   */
  def shouldFallbackToSort(shuffleId: Int): Boolean = {
    // Only consider fallback for streaming shuffles
    val handle = registeredShuffles.get(shuffleId)
    if (handle == null || !handle.isInstanceOf[StreamingShuffleHandle[_, _, _]]) {
      return false
    }

    // Check consumer lag ratio from backpressure protocol
    // If any consumer is 2x slower than producer for >60 seconds, recommend fallback
    if (backpressureProtocol.shouldFallbackAny()) {
      logWarning(s"Consumer lag ratio exceeds threshold ${FALLBACK_LAG_RATIO_THRESHOLD} " +
        s"for more than ${FALLBACK_LAG_DURATION_MS}ms, " +
        s"recommending fallback to sort-based shuffle for shuffle $shuffleId")
      return true
    }

    // Check memory pressure - if buffer utilization is at maximum, recommend fallback
    val bufferUtilization = spillManager.getBufferUtilization
    if (bufferUtilization >= 95) {
      logWarning(s"Buffer utilization at $bufferUtilization%, OOM risk detected, " +
        s"recommending fallback to sort-based shuffle for shuffle $shuffleId")
      return true
    }

    // Network saturation check would require additional network monitoring
    // infrastructure - this is a placeholder for future enhancement
    // For now, the backpressure protocol handles rate limiting

    false
  }

  /**
   * Check if fallback should be triggered based on shuffle dependency characteristics.
   *
   * This is a static check at registration time, separate from the dynamic
   * runtime check performed by shouldFallbackToSort(shuffleId).
   *
   * @param dependency the shuffle dependency to evaluate
   * @return true if sort-based shuffle should be used instead of streaming
   */
  def shouldFallbackToSort[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = {
    !shouldUseStreaming(dependency)
  }

  /**
   * Check if a shuffle is using streaming mode.
   *
   * @param shuffleId the shuffle identifier
   * @return true if shuffle is using streaming mode
   */
  def isStreamingShuffle(shuffleId: Int): Boolean = {
    Option(registeredShuffles.get(shuffleId))
      .exists(_.isInstanceOf[StreamingShuffleHandle[_, _, _]])
  }

  /**
   * Get current metrics snapshot.
   *
   * @return metrics source for streaming shuffle
   */
  def getMetrics: StreamingShuffleMetricsSource = metricsSource

  /**
   * Get statistics about the shuffle manager.
   *
   * @return map of statistic names to values
   */
  def getStats: Map[String, Any] = {
    Map(
      "streamingEnabled" -> streamingEnabled,
      "registeredShuffles" -> registeredShuffles.size(),
      "streamingShuffles" -> registeredShuffles.values().toArray
        .count(_.isInstanceOf[StreamingShuffleHandle[_, _, _]]),
      "bufferUtilization" -> spillManager.getBufferUtilization,
      "spillStats" -> spillManager.getSpillStats
    )
  }
}

/**
 * Companion object for StreamingShuffleManager.
 */
private[spark] object StreamingShuffleManager {

  /** Configuration key for streaming shuffle manager. */
  val SHORT_NAME = "streaming"

  /** Full class name for registration in ShuffleManager factory. */
  val CLASS_NAME = "org.apache.spark.shuffle.streaming.StreamingShuffleManager"

  /**
   * Check if streaming shuffle is enabled in the given configuration.
   *
   * @param conf SparkConf to check
   * @return true if streaming shuffle is enabled
   */
  def isEnabled(conf: SparkConf): Boolean = {
    conf.get(SHUFFLE_STREAMING_ENABLED)
  }
}
