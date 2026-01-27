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

import java.util.concurrent.atomic.AtomicReference

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source

/**
 * Telemetry emission component for streaming shuffle operational visibility.
 *
 * This class implements the [[Source]] trait for integration with Spark's executor metrics
 * system, enabling JMX exposure for external monitoring. It provides real-time visibility
 * into streaming shuffle operations through gauges and counters.
 *
 * Metrics exposed:
 *   - bufferUtilizationPercent (Gauge): Real-time buffer occupancy percentage (0.0-100.0)
 *   - spillCount (Counter): Number of disk spill events triggered
 *   - backpressureEvents (Counter): Consumer rate limiting incidents
 *   - partialReadInvalidations (Counter): Producer failure detection count
 *   - bytesStreamed (Counter): Total bytes transferred via streaming
 *   - fallbackCount (Counter): Automatic fallback to sort-based shuffle events
 *
 * Performance Considerations:
 *   - All metric updates use lock-free atomic operations to meet the <1% CPU utilization
 *     overhead constraint
 *   - Counter increments use native atomic operations (CAS-free for single increments)
 *   - Gauge reads use AtomicReference for thread-safe access without locking
 *   - JMX exposure is automatic via standard Dropwizard MetricRegistry registration
 *
 * Coexistence Note:
 *   This metrics class is specific to streaming shuffle and coexists with existing
 *   shuffle metrics from SortShuffleManager. When streaming shuffle falls back to
 *   sort-based shuffle, those operations report through the standard shuffle metrics.
 *
 * @since 4.2.0
 */
private[spark] class StreamingShuffleMetrics extends Source with Logging {

  /**
   * Source name used by Spark's metrics system for namespace identification.
   * Metrics will be exposed under the "StreamingShuffleMetrics" namespace in JMX.
   */
  override val sourceName: String = "StreamingShuffleMetrics"

  /**
   * Dropwizard MetricRegistry holding all streaming shuffle metrics.
   * This registry is automatically integrated with Spark's executor metrics system
   * for JMX exposure and sink reporting.
   */
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // ============================================================================
  // Thread-safe storage for gauge values
  // ============================================================================

  /**
   * Thread-safe storage for buffer utilization percentage.
   * Uses AtomicReference[java.lang.Double] to enable safe concurrent updates
   * from multiple writer threads while the gauge reads the current value.
   * Initial value is 0.0 (no buffer utilization).
   */
  private val bufferUtilizationValue: AtomicReference[java.lang.Double] =
    new AtomicReference[java.lang.Double](0.0d)

  // ============================================================================
  // Metric Registration - Counters
  // ============================================================================

  /**
   * Counter tracking the number of disk spill events.
   * Incremented by MemorySpillManager when buffer utilization exceeds the
   * 80% threshold and data is persisted to disk.
   */
  private val spillCountCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("spillCount"))

  /**
   * Counter tracking consumer rate limiting incidents.
   * Incremented by BackpressureProtocol when consumers are detected as
   * slower than producers and flow control is applied.
   */
  private val backpressureEventsCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("backpressureEvents"))

  /**
   * Counter tracking producer failure detection events.
   * Incremented by StreamingShuffleReader when partial reads are invalidated
   * due to producer failures detected via connection timeout.
   */
  private val partialReadInvalidationsCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("partialReadInvalidations"))

  /**
   * Counter tracking total bytes transferred via streaming.
   * Incremented by StreamingShuffleWriter when data is successfully streamed
   * to consumer executors.
   */
  private val bytesStreamedCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("bytesStreamed"))

  /**
   * Counter tracking automatic fallback events to sort-based shuffle.
   * Incremented by StreamingShuffleManager when streaming conditions are
   * not met and the shuffle falls back to SortShuffleManager.
   */
  private val fallbackCountCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("fallbackCount"))

  // ============================================================================
  // Metric Registration - Gauges
  // ============================================================================

  /**
   * Gauge reporting real-time buffer occupancy percentage.
   * Range is 0.0 to 100.0, representing the percentage of allocated
   * streaming buffer memory currently in use.
   *
   * This gauge is read by the metrics system at configured intervals.
   * Thread-safe reading is ensured via AtomicReference.get().
   */
  metricRegistry.register(
    MetricRegistry.name("bufferUtilizationPercent"),
    new Gauge[java.lang.Double] {
      override def getValue: java.lang.Double = bufferUtilizationValue.get()
    }
  )

  // Log registration completion
  logInfo("StreamingShuffleMetrics registered with metrics system")
  logDebug(s"Registered metrics: bufferUtilizationPercent (gauge), spillCount, " +
    "backpressureEvents, partialReadInvalidations, bytesStreamed, fallbackCount")

  // ============================================================================
  // Metric Update Methods
  // ============================================================================

  /**
   * Updates the buffer utilization gauge with the current percentage.
   *
   * This method is called by StreamingShuffleWriter and MemorySpillManager
   * to report real-time buffer occupancy. Thread-safe via AtomicReference.set().
   *
   * @param percent Current buffer utilization as a percentage (0.0 to 100.0).
   *                Values outside this range are clamped for safety.
   */
  def updateBufferUtilization(percent: Double): Unit = {
    // Clamp value to valid range [0.0, 100.0] for safety
    val clampedValue = math.max(0.0, math.min(100.0, percent))
    bufferUtilizationValue.set(clampedValue)
    logDebug(s"Buffer utilization updated to $clampedValue%")
  }

  /**
   * Increments the spill count counter.
   *
   * Called by MemorySpillManager when memory threshold is exceeded and
   * data is spilled to disk.
   */
  def incSpillCount(): Unit = {
    spillCountCounter.inc()
    logDebug(s"Spill count incremented to ${spillCountCounter.getCount}")
  }

  /**
   * Increments the backpressure events counter.
   *
   * Called by BackpressureProtocol when consumer rate limiting is applied.
   */
  def incBackpressureEvents(): Unit = {
    backpressureEventsCounter.inc()
    logDebug(s"Backpressure events incremented to ${backpressureEventsCounter.getCount}")
  }

  /**
   * Increments the partial read invalidations counter.
   *
   * Called by StreamingShuffleReader when partial reads are discarded
   * due to producer failure detection.
   */
  def incPartialReadInvalidations(): Unit = {
    partialReadInvalidationsCounter.inc()
    val count = partialReadInvalidationsCounter.getCount
    logDebug(s"Partial read invalidations incremented to $count")
  }

  /**
   * Increments the bytes streamed counter by the specified amount.
   *
   * Called by StreamingShuffleWriter when data is successfully transferred
   * to consumer executors via streaming.
   *
   * @param bytes Number of bytes successfully streamed. Must be non-negative.
   */
  def incBytesStreamed(bytes: Long): Unit = {
    if (bytes > 0) {
      bytesStreamedCounter.inc(bytes)
      logDebug(s"Bytes streamed incremented by $bytes to ${bytesStreamedCounter.getCount}")
    }
  }

  /**
   * Increments the fallback count counter.
   *
   * Called by StreamingShuffleManager when streaming shuffle falls back
   * to sort-based shuffle due to:
   * - Memory pressure preventing buffer allocation
   * - Network saturation exceeding 90% link capacity
   * - Consumer being 2x slower than producer for >60 seconds
   * - Protocol version mismatch
   */
  def incFallbackCount(): Unit = {
    fallbackCountCounter.inc()
    val count = fallbackCountCounter.getCount
    logInfo(s"Streaming shuffle fallback triggered, total fallbacks: $count")
  }

  // ============================================================================
  // Metric Getter Methods
  // ============================================================================

  /**
   * Returns the current buffer utilization percentage.
   *
   * @return Buffer utilization as a percentage (0.0 to 100.0)
   */
  def getBufferUtilization: Double = bufferUtilizationValue.get()

  /**
   * Returns the current spill count.
   *
   * @return Number of disk spill events since metrics initialization
   */
  def getSpillCount: Long = spillCountCounter.getCount

  /**
   * Returns the current backpressure events count.
   *
   * @return Number of consumer rate limiting incidents since metrics initialization
   */
  def getBackpressureEvents: Long = backpressureEventsCounter.getCount

  /**
   * Returns the current partial read invalidations count.
   *
   * @return Number of producer failure detection events since metrics initialization
   */
  def getPartialReadInvalidations: Long = partialReadInvalidationsCounter.getCount

  /**
   * Returns the total bytes streamed.
   *
   * @return Total bytes transferred via streaming since metrics initialization
   */
  def getBytesStreamed: Long = bytesStreamedCounter.getCount

  /**
   * Returns the current fallback count.
   *
   * @return Number of automatic fallback events since metrics initialization
   */
  def getFallbackCount: Long = fallbackCountCounter.getCount

  // ============================================================================
  // Utility Methods
  // ============================================================================

  /**
   * Resets all metrics to their initial values.
   * Primarily useful for testing purposes.
   *
   * Note: Counters are decremented by their current value rather than
   * directly set to zero, following the Dropwizard Counter API pattern
   * used elsewhere in Spark (see HiveCatalogMetrics.reset()).
   */
  def reset(): Unit = {
    bufferUtilizationValue.set(0.0d)
    spillCountCounter.dec(spillCountCounter.getCount)
    backpressureEventsCounter.dec(backpressureEventsCounter.getCount)
    partialReadInvalidationsCounter.dec(partialReadInvalidationsCounter.getCount)
    bytesStreamedCounter.dec(bytesStreamedCounter.getCount)
    fallbackCountCounter.dec(fallbackCountCounter.getCount)
    logInfo("StreamingShuffleMetrics reset to initial values")
  }

  /**
   * Returns a snapshot of all metric values for debugging/logging.
   *
   * @return Map of metric names to their current values
   */
  def snapshot(): Map[String, Any] = {
    Map(
      "bufferUtilizationPercent" -> getBufferUtilization,
      "spillCount" -> getSpillCount,
      "backpressureEvents" -> getBackpressureEvents,
      "partialReadInvalidations" -> getPartialReadInvalidations,
      "bytesStreamed" -> getBytesStreamed,
      "fallbackCount" -> getFallbackCount
    )
  }
}

/**
 * Companion object providing a factory method for creating StreamingShuffleMetrics
 * instances and utility methods for metrics source registration.
 */
private[spark] object StreamingShuffleMetrics {

  /**
   * Creates a new StreamingShuffleMetrics instance.
   *
   * The returned instance should be registered with Spark's metrics system via:
   * {{{
   *   val metrics = StreamingShuffleMetrics()
   *   SparkEnv.get.metricsSystem.registerSource(metrics)
   * }}}
   *
   * @return A new StreamingShuffleMetrics instance ready for registration
   */
  def apply(): StreamingShuffleMetrics = new StreamingShuffleMetrics()
}
