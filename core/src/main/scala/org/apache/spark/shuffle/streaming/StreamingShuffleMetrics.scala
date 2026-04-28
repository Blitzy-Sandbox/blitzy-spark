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

import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.{Counter, Gauge, Metric, MetricSet}

/**
 * Streaming-shuffle metric set per AAP Section 0.1.1 and Section 0.5.1.4.
 *
 * Exposes the four required streaming-shuffle observability metrics consumed by
 * `StreamingShuffleSource` and registered with the existing Spark `MetricsSystem`:
 *
 *   - `bufferUtilizationPercent`: `Gauge[Int]` sampled by the streaming subsystem's memory
 *     poller (typically `MemorySpillManager` running on its 100 ms scheduler tick); reports
 *     the current percent of streaming-buffer memory in use across all active shuffles on
 *     this executor (range `[0, 100]`).
 *   - `spillCount`: `Counter` incremented on each disk-spill event emitted by the streaming
 *     subsystem (typically `MemorySpillManager` when buffer utilization crosses the
 *     configured spill threshold).
 *   - `backpressureEvents`: `Counter` incremented on each backpressure event emitted by the
 *     streaming subsystem (typically `BackpressureProtocol` for rate-limit hits, missed
 *     heartbeats, and priority-arbitration outcomes).
 *   - `partialReadInvalidations`: `Counter` incremented on each producer-failure detection
 *     emitted by the streaming subsystem (typically `StreamingShuffleReader` when a 5-second
 *     producer-connection timeout fires and partial reads are discarded before the existing
 *     [[org.apache.spark.shuffle.FetchFailedException]] path drives upstream recomputation).
 *
 * == Namespace ==
 * The metric keys returned by [[getMetrics]] include the AAP-required
 * `"shuffle.streaming."` prefix verbatim:
 *   - `shuffle.streaming.bufferUtilizationPercent`
 *   - `shuffle.streaming.spillCount`
 *   - `shuffle.streaming.backpressureEvents`
 *   - `shuffle.streaming.partialReadInvalidations`
 *
 * Embedding the prefix in the metric key (rather than relying on the future
 * `StreamingShuffleSource.sourceName` to supply it) keeps the AAP-mandated operator-facing
 * namespace visible at the metric-set definition site and decouples the names from the
 * source-name choice. When this set is registered with the executor
 * [[org.apache.spark.metrics.MetricsSystem]] via `StreamingShuffleSource`, the Spark
 * `MetricsSystem` composes the application/executor/source prefixes per its standard
 * convention (see `MetricsSystem.buildRegistryName`), yielding final JMX object names of
 * the form `<application>.<executor-id>.<source-name>.shuffle.streaming.<metric-name>`.
 * The `shuffle.streaming.<metric-name>` substring -- the operator-facing namespace
 * required by AAP Section 0.1.1 -- is preserved end-to-end regardless of which `sourceName`
 * the future `StreamingShuffleSource` chooses.
 *
 * == Concurrency ==
 * Metric updates are received from multiple threads -- one per streaming writer instance,
 * one per streaming reader instance, the spill-poll daemon thread, and the backpressure
 * scheduler thread can all concurrently mutate this set. All update operations are lock-free
 * to satisfy the streaming-shuffle "<1% executor CPU utilization" telemetry budget:
 *
 *   - [[Counter]] is backed internally by `LongAdder` (per-thread cells with eventual
 *     summation in `getCount`), avoiding contention on a single CAS site under high
 *     concurrent writer/reader load.
 *   - [[Gauge]] backing storage is a single [[AtomicInteger]] read at metric-emission time
 *     only (not on the hot path); writes from the spill poller are simple `set` operations.
 *
 * Note that this metric set is intentionally NOT an implementation of
 * [[org.apache.spark.shuffle.ShuffleReadMetricsReporter]] or
 * [[org.apache.spark.shuffle.ShuffleWriteMetricsReporter]]. Those traits carry a
 * single-threaded contract for per-task metric reporting and are implemented separately by
 * the streaming writer and reader. This class is a cross-task metric set updated from
 * multiple threads concurrently and uses the lock-free primitives described above.
 *
 * == Lifecycle ==
 * Constructed once per `StreamingShuffleManager` instance (i.e., once per executor JVM) and
 * registered with the executor `MetricsSystem` via `StreamingShuffleSource`. The lifetime of
 * this instance equals the lifetime of the executor; counters accumulate monotonically over
 * the application lifetime, matching operator expectations for cumulative event counts.
 *
 * == Coexistence ==
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* This metric set lives entirely
 * within the `org.apache.spark.shuffle.streaming` package, registers via the existing
 * `MetricsSystem` extension surface, and does not modify the `ExecutorMetrics` typed array
 * or any other existing telemetry carrier.
 */
private[spark] class StreamingShuffleMetrics extends MetricSet {

  /**
   * Backing storage for the [[bufferUtilizationPercent]] gauge. Written by the streaming
   * memory poller (typically `MemorySpillManager.pollOnce` at 100 ms cadence) via
   * [[updateBufferUtilization]]; read by the gauge at metric-emission time.
   *
   * Initialized to `0` so that immediately after construction (before any poll has run) the
   * gauge reports a sensible "no buffers in use" baseline rather than an undefined value.
   */
  private val bufferUtilizationPercentRef = new AtomicInteger(0)

  /**
   * Gauge: percent of streaming-buffer memory currently in use, in the closed interval
   * `[0, 100]`. The gauge value is a level (current state) rather than a count (cumulative
   * total), which is why a [[Gauge]] is used here instead of a [[Counter]].
   *
   * The Dropwizard generic parameter is the boxed [[java.lang.Integer]] type because the
   * Dropwizard Java API requires boxed types for generic parameters; using `Gauge[Int]`
   * would attempt to use the primitive `int` and not satisfy the interface contract.
   * The `getValue` implementation reads the [[bufferUtilizationPercentRef]] and explicitly
   * boxes via [[java.lang.Integer.valueOf]] to avoid any auto-boxing surprises.
   */
  val bufferUtilizationPercent: Gauge[java.lang.Integer] = new Gauge[java.lang.Integer] {
    override def getValue: java.lang.Integer =
      java.lang.Integer.valueOf(bufferUtilizationPercentRef.get())
  }

  /**
   * Counter: cumulative number of disk-spill events emitted by the streaming subsystem
   * over the lifetime of this executor. Incremented by [[incrementSpillCount]] from
   * `MemorySpillManager`. Counters in Dropwizard are monotonic by convention, matching
   * operator expectations that this metric reports total events rather than current
   * pending spills.
   */
  val spillCount: Counter = new Counter()

  /**
   * Counter: cumulative number of backpressure events emitted by the streaming subsystem
   * over the lifetime of this executor. Incremented by [[incrementBackpressureEvents]] from
   * `BackpressureProtocol` for events including rate-limit triggers, missed heartbeats,
   * and priority-arbitration outcomes.
   */
  val backpressureEvents: Counter = new Counter()

  /**
   * Counter: cumulative number of partial-read invalidations emitted by the streaming
   * subsystem over the lifetime of this executor. Incremented by
   * [[incrementPartialReadInvalidations]] from `StreamingShuffleReader` when a producer
   * connection times out and the reader discards partial buffered data before the existing
   * [[org.apache.spark.shuffle.FetchFailedException]] path triggers DAG-scheduler upstream
   * recomputation.
   */
  val partialReadInvalidations: Counter = new Counter()

  /**
   * Update the [[bufferUtilizationPercent]] gauge. Intended to be called by the streaming
   * memory poller (typically `MemorySpillManager.pollOnce`) at the configured polling
   * cadence (100 ms by the streaming-shuffle specification).
   *
   * Out-of-range values are clamped into `[0, 100]` rather than rejected so that callers
   * computing the percent from a ratio (`usedBytes * 100 / totalBytes`) cannot accidentally
   * publish a value that violates the operator-facing invariant on this gauge -- for example
   * when transient over-allocation pushes the ratio above 100, or when integer arithmetic
   * underflow produces a negative value during a buffer-resize race.
   *
   * @param pct buffer utilization as an integer percent; values outside `[0, 100]` are
   *            clamped to the nearest endpoint
   */
  def updateBufferUtilization(pct: Int): Unit = {
    val clamped = math.max(0, math.min(100, pct))
    bufferUtilizationPercentRef.set(clamped)
  }

  /**
   * Increment the [[spillCount]] counter by one. Intended to be called by
   * `MemorySpillManager` on each spill event.
   */
  def incrementSpillCount(): Unit = spillCount.inc()

  /**
   * Increment the [[backpressureEvents]] counter by one. Intended to be called by
   * `BackpressureProtocol` on each backpressure event.
   */
  def incrementBackpressureEvents(): Unit = backpressureEvents.inc()

  /**
   * Increment the [[partialReadInvalidations]] counter by one. Intended to be called by
   * `StreamingShuffleReader` on each producer-failure detection that results in partial
   * buffered data being discarded.
   */
  def incrementPartialReadInvalidations(): Unit = partialReadInvalidations.inc()

  /**
   * @return the current buffer-utilization gauge value as an unboxed `Int` in `[0, 100]`.
   *         Provided for callers (notably `StreamingShuffleFallbackPolicy.shouldFallback`)
   *         that need to read the current gauge level without going through the
   *         Dropwizard [[Gauge.getValue]] boxing path.
   */
  def getBufferUtilizationPercent: Int = bufferUtilizationPercentRef.get()

  /**
   * @return the cumulative spill-count counter value. Provided for callers that need a
   *         direct read without traversing the Dropwizard registry; semantically
   *         equivalent to `spillCount.getCount`.
   */
  def getSpillCount: Long = spillCount.getCount

  /**
   * @return the cumulative backpressure-event counter value. Provided for callers that
   *         need a direct read without traversing the Dropwizard registry; semantically
   *         equivalent to `backpressureEvents.getCount`.
   */
  def getBackpressureEventsCount: Long = backpressureEvents.getCount

  /**
   * @return the cumulative partial-read-invalidation counter value. Provided for callers
   *         that need a direct read without traversing the Dropwizard registry;
   *         semantically equivalent to `partialReadInvalidations.getCount`.
   */
  def getPartialReadInvalidationsCount: Long = partialReadInvalidations.getCount

  /**
   * Return all four streaming-shuffle metrics keyed by their AAP-mandated operator-facing
   * names (each prefixed with `"shuffle.streaming."` per AAP Section 0.1.1). The returned
   * map is unmodifiable per the [[MetricSet]] contract -- attempts to mutate it raise
   * [[UnsupportedOperationException]].
   *
   * Including the `"shuffle.streaming."` prefix at the metric-key level guarantees the
   * AAP-required substring appears in the final JMX object name regardless of which
   * `sourceName` the future `StreamingShuffleSource` registers under. The Spark
   * `MetricsSystem` will further compose the application, executor, and source prefixes
   * per its standard convention (see `MetricsSystem.buildRegistryName`), producing final
   * names of the form
   * `<application>.<executor-id>.<source-name>.shuffle.streaming.<metric-name>`.
   *
   * The map is constructed fresh on every call rather than cached because Dropwizard
   * permits -- but does not require -- `getMetrics` to return a stable reference, and the
   * metric values themselves are mutable references whose updates remain visible through
   * either copy. Constructing fresh avoids any subtle aliasing concern in registries that
   * store the returned map directly.
   */
  override def getMetrics(): java.util.Map[String, Metric] = {
    val map = new java.util.HashMap[String, Metric]()
    map.put("shuffle.streaming.bufferUtilizationPercent", bufferUtilizationPercent)
    map.put("shuffle.streaming.spillCount", spillCount)
    map.put("shuffle.streaming.backpressureEvents", backpressureEvents)
    map.put("shuffle.streaming.partialReadInvalidations", partialReadInvalidations)
    java.util.Collections.unmodifiableMap(map)
  }
}
