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
 * Thread-safe, dependency-free holder for the four `shuffle.streaming.*` telemetry values that
 * the streaming shuffle backend publishes.
 *
 * This class is intentionally a plain holder of lock-free JDK atomics and carries no dependency
 * on Dropwizard/Codahale metrics. Keeping it dependency-free serves two goals:
 *
 *  - Reuse and isolation: the producer (`StreamingShuffleWriter`), the consumer
 *    (`StreamingShuffleReader`), `BackpressureProtocol`, and `MemorySpillManager` all update
 *    these values on hot paths, while `StreamingShuffleSource` adapts the same accessors to
 *    Codahale `Gauge`/`Counter` instances for JMX and Prometheus emission. The holder itself
 *    never needs a live `MetricRegistry`.
 *  - Testability: suites can assert counts and gauge clamping without standing up a
 *    `MetricsSystem`.
 *
 * Thread-safety and performance: every mutator and accessor is lock-free and O(1), so telemetry
 * overhead stays well under the 1% executor-CPU budget mandated for the streaming backend. The
 * three counters are backed by `AtomicLong`; the single gauge is a `@volatile Double` whose reads
 * and writes are atomic per the Java Memory Model (JLS 17.7), following last-writer-wins,
 * "sampled on read" semantics.
 *
 * The four metrics exposed here are:
 *  1. `bufferUtilizationPercent` - gauge in the inclusive range `[0, 100]`: the current aggregate
 *     buffer utilization percentage across the executor's streaming buffers.
 *  2. `spillCount` - monotonic counter: number of disk-spill events.
 *  3. `backpressureEvents` - monotonic counter: number of backpressure throttling events.
 *  4. `partialReadInvalidations` - monotonic counter: number of partial-read invalidations caused
 *     by producer failure.
 *
 * @note Instances are cheap; one is shared per `StreamingShuffleManager` and handed to the
 *       collaborating components and to `StreamingShuffleSource`.
 */
private[spark] class StreamingShuffleMetrics {

  // Counter backing fields. Each is a monotonically increasing tally of discrete events, mutated
  // from multiple threads via the lock-free compare-and-swap loop inside AtomicLong.
  private val spillCounter = new AtomicLong(0L)
  private val backpressureEventCounter = new AtomicLong(0L)
  private val partialReadInvalidationCounter = new AtomicLong(0L)

  // Gauge backing field, holding the most recently sampled aggregate buffer-utilization percent.
  // A @volatile Double gives atomic, immediately-visible reads/writes (JLS 17.7) without locking,
  // matching the "sampled on read, last writer wins" semantics of a gauge.
  @volatile private var bufferUtilization: Double = 0.0

  /**
   * Records a single disk-spill event. Lock-free and allocation-free; safe to call concurrently
   * from any producer thread or from the `MemorySpillManager` poll loop.
   */
  def incSpillCount(): Unit = {
    spillCounter.incrementAndGet()
  }

  /** Returns the total number of disk-spill events recorded so far. */
  def spillCount: Long = spillCounter.get()

  /**
   * Records a single backpressure throttling event. Lock-free and allocation-free; safe to call
   * concurrently from the `BackpressureProtocol` flow-control path.
   */
  def incBackpressureEvents(): Unit = {
    backpressureEventCounter.incrementAndGet()
  }

  /** Returns the total number of backpressure throttling events recorded so far. */
  def backpressureEvents: Long = backpressureEventCounter.get()

  /**
   * Records a single partial-read invalidation, raised when a consumer invalidates partial reads
   * after a producer failure. Lock-free and allocation-free.
   */
  def incPartialReadInvalidations(): Unit = {
    partialReadInvalidationCounter.incrementAndGet()
  }

  /** Returns the total number of partial-read invalidations recorded so far. */
  def partialReadInvalidations: Long = partialReadInvalidationCounter.get()

  /**
   * Updates the aggregate buffer-utilization gauge. The supplied value is clamped into the
   * inclusive `[0, 100]` range (and a `NaN` sample is mapped defensively to `0.0`) so a malformed
   * reading can never corrupt the published gauge. Lock-free and allocation-free.
   *
   * @param v the latest buffer-utilization sample, expressed as a percentage
   */
  def setBufferUtilizationPercent(v: Double): Unit = {
    bufferUtilization = StreamingShuffleMetrics.clampPercent(v)
  }

  /** Returns the current aggregate buffer-utilization percentage, always within `[0, 100]`. */
  def bufferUtilizationPercent: Double = bufferUtilization

  /**
   * Resets every metric to its initial value (all counters to `0` and the gauge to `0.0`).
   * Intended for test isolation; it is not used on production code paths.
   */
  def reset(): Unit = {
    spillCounter.set(0L)
    backpressureEventCounter.set(0L)
    partialReadInvalidationCounter.set(0L)
    bufferUtilization = 0.0
  }
}

/**
 * Companion object exposing the canonical metric-naming constants shared with
 * `StreamingShuffleSource`, together with the internal gauge-clamping helper.
 */
private[spark] object StreamingShuffleMetrics {

  /**
   * Common dotted prefix under which every streaming-shuffle metric is registered. Combined with
   * the per-metric short names below, the emitted metric paths become `shuffle.streaming.<name>`
   * (for example, `shuffle.streaming.bufferUtilizationPercent`).
   */
  val METRIC_PREFIX: String = "shuffle.streaming"

  /** Short metric name for the buffer-utilization gauge (range `[0, 100]`). */
  val BUFFER_UTILIZATION_PERCENT: String = "bufferUtilizationPercent"

  /** Short metric name for the disk-spill-event counter. */
  val SPILL_COUNT: String = "spillCount"

  /** Short metric name for the backpressure-throttling-event counter. */
  val BACKPRESSURE_EVENTS: String = "backpressureEvents"

  /** Short metric name for the partial-read-invalidation counter. */
  val PARTIAL_READ_INVALIDATIONS: String = "partialReadInvalidations"

  // Inclusive bounds for the buffer-utilization gauge, expressed as a percentage.
  private val MIN_PERCENT = 0.0
  private val MAX_PERCENT = 100.0

  /**
   * Clamps an arbitrary percentage sample into the inclusive `[0, 100]` range. A `NaN` sample is
   * mapped to `MIN_PERCENT` so that a malformed reading can never propagate to the published
   * gauge.
   *
   * @param v the raw percentage sample
   * @return `v` constrained to `[0, 100]`, with `NaN` mapped to `0.0`
   */
  private def clampPercent(v: Double): Double = {
    if (java.lang.Double.isNaN(v)) {
      MIN_PERCENT
    } else if (v < MIN_PERCENT) {
      MIN_PERCENT
    } else if (v > MAX_PERCENT) {
      MAX_PERCENT
    } else {
      v
    }
  }
}
