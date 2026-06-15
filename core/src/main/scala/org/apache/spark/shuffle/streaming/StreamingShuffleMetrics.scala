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

import java.util.concurrent.atomic.AtomicLong

/**
 * Thread-safe, dependency-free holder for the four streaming-shuffle telemetry metrics.
 *
 * This class is intentionally a pure holder backed only by `java.util.concurrent.atomic`
 * primitives so it can be updated from the hot map-side and reduce-side paths with
 * lock-free, O(1), allocation-free operations. The streaming-shuffle telemetry-overhead
 * budget is below 1% of executor CPU, so every accessor here avoids locks, boxing, and
 * intermediate allocations.
 *
 * Keeping the holder free of any Dropwizard/Codahale dependency lets the sibling
 * `StreamingShuffleSource` adapt these accessors to Codahale `Gauge`/`Counter` instances
 * at registration time, and lets unit tests assert metric values without a live
 * `MetricsSystem`. The metrics are registered under the `shuffle.streaming.` prefix
 * (see `METRIC_PREFIX`) so the emitted metric paths are, for example,
 * `shuffle.streaming.spillCount`.
 *
 * The four metrics are:
 *  - `bufferUtilizationPercent` (gauge, 0-100): current aggregate buffer utilization.
 *  - `spillCount` (counter): number of disk-spill events.
 *  - `backpressureEvents` (counter): number of backpressure throttling events.
 *  - `partialReadInvalidations` (counter): partial reads invalidated on producer failure.
 *
 * All mutating and reading operations are safe to call concurrently from any number of
 * producer, consumer, backpressure, and spill threads.
 */
private[spark] class StreamingShuffleMetrics {

  // Monotonic counters. AtomicLong increments are lock-free (CAS-based) and never block,
  // keeping per-event telemetry overhead negligible on the shuffle hot path.
  private val spillCounter = new AtomicLong(0L)
  private val backpressureCounter = new AtomicLong(0L)
  private val partialReadInvalidationCounter = new AtomicLong(0L)

  // Sampled gauge for the current aggregate buffer-utilization percentage in [0, 100].
  // A volatile Double is read and written atomically by the JVM (JLS 17.7) without locks.
  @volatile private var bufferUtilization: Double = 0.0

  /** Records a single disk-spill event. Lock-free and O(1). */
  def incSpillCount(): Unit = {
    spillCounter.incrementAndGet()
  }

  /** @return the total number of disk-spill events observed so far. */
  def spillCount: Long = spillCounter.get()

  /** Records a single backpressure throttling event. Lock-free and O(1). */
  def incBackpressureEvents(): Unit = {
    backpressureCounter.incrementAndGet()
  }

  /** @return the total number of backpressure throttling events observed so far. */
  def backpressureEvents: Long = backpressureCounter.get()

  /** Records a single partial-read invalidation triggered by a producer failure. */
  def incPartialReadInvalidations(): Unit = {
    partialReadInvalidationCounter.incrementAndGet()
  }

  /** @return the total number of partial-read invalidations observed so far. */
  def partialReadInvalidations: Long = partialReadInvalidationCounter.get()

  /**
   * Updates the current aggregate buffer-utilization gauge.
   *
   * The supplied value is defensively clamped into the inclusive range [0, 100]. A `NaN`
   * input (which can arise from a 0/0 utilization computation when no buffers are yet
   * allocated) is normalized to 0, and positive/negative infinity collapse to 100/0
   * respectively. The update is a single volatile write, so it is lock-free and O(1).
   *
   * @param v the freshly sampled utilization percentage to publish.
   */
  def setBufferUtilizationPercent(v: Double): Unit = {
    val safe = if (java.lang.Double.isNaN(v)) 0.0 else v
    bufferUtilization = math.min(100.0, math.max(0.0, safe))
  }

  /** @return the current aggregate buffer-utilization percentage, always in [0, 100]. */
  def bufferUtilizationPercent: Double = bufferUtilization

  /**
   * Resets every counter and the gauge back to their initial values.
   *
   * This exists primarily so unit tests can assert metric deltas in isolation; it is not
   * used on any production code path. Each field is reset independently with a lock-free
   * write, so the method is safe to call concurrently with metric updates, although
   * callers typically invoke it only from a quiescent state.
   */
  def reset(): Unit = {
    spillCounter.set(0L)
    backpressureCounter.set(0L)
    partialReadInvalidationCounter.set(0L)
    bufferUtilization = 0.0
  }
}

/**
 * Constants describing how the streaming-shuffle metrics are named when registered.
 *
 * The sibling `StreamingShuffleSource` combines `METRIC_PREFIX` with each metric's short
 * name to produce the fully qualified Dropwizard metric path (for example
 * `shuffle.streaming.spillCount`). Centralizing the names here keeps the holder and the
 * source in agreement and guarantees the emitted metric paths match the documented
 * `shuffle.streaming.*` contract.
 */
private[spark] object StreamingShuffleMetrics {

  /** Common metric-name prefix shared by all streaming-shuffle telemetry. */
  val METRIC_PREFIX: String = "shuffle.streaming"

  /** Short name of the aggregate buffer-utilization gauge (0-100). */
  val BUFFER_UTILIZATION_PERCENT: String = "bufferUtilizationPercent"

  /** Short name of the disk-spill event counter. */
  val SPILL_COUNT: String = "spillCount"

  /** Short name of the backpressure-event counter. */
  val BACKPRESSURE_EVENTS: String = "backpressureEvents"

  /** Short name of the partial-read invalidation counter. */
  val PARTIAL_READ_INVALIDATIONS: String = "partialReadInvalidations"
}
