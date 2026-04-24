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

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
 * Dropwizard [[org.apache.spark.metrics.source.Source]] for the streaming shuffle
 * path. Exposes four instruments under the `shuffle.streaming.*` namespace that
 * downstream Dropwizard sinks (JMX, Prometheus, Graphite, CSV, Slf4jSink, etc.) pick
 * up automatically once this source is registered against the executor-scoped
 * [[org.apache.spark.metrics.MetricsSystem]]:
 *
 *   1. `shuffle.streaming.bufferUtilizationPercent` &mdash; `Gauge[java.lang.Double]`
 *      reflecting the most recent aggregate streaming-buffer utilization reading
 *      (0.0 .. 100.0) published by `MemorySpillManager`'s 100 ms poll.
 *   2. `shuffle.streaming.spillCount` &mdash; `Counter` incremented by
 *      `StreamingShuffleWriter` on each spill event triggered by the 80 % threshold.
 *   3. `shuffle.streaming.backpressureEvents` &mdash; `Counter` incremented by
 *      `BackpressureProtocol` on each throttle / token-bucket-block event.
 *   4. `shuffle.streaming.partialReadInvalidations` &mdash; `Counter` incremented by
 *      `StreamingShuffleReader` each time a producer timeout forces the consumer to
 *      discard partial blocks and request upstream recomputation.
 *
 * Coexistence strategy &mdash; new-only, never-touches-sort:
 *   - This source is instantiated and registered by `StreamingShuffleManager` at
 *     executor-side construction ONLY when `spark.shuffle.manager=streaming`; an
 *     executor running the default `SortShuffleManager` never constructs this
 *     class and the `shuffle.streaming.*` namespace is absent from its
 *     `MetricsSystem` output. This keeps sort-path JMX / Prometheus surface
 *     byte-for-byte identical to pre-feature behavior (AAP section 0.7.1 Implementation
 *     Discipline: "Isolate streaming logic ... zero cross-contamination").
 *   - The 17-method `ShuffleReadMetricsReporter` and 5-method
 *     `ShuffleWriteMetricsReporter` contracts (F-009 parity) are NOT replaced by
 *     this source; those reporters continue to feed the Stages page and the
 *     standard `shuffle.read.*` / `shuffle.write.*` instrument names.
 *     The four instruments here are supplementary streaming-shuffle-specific
 *     telemetry, not a replacement for any existing instrument.
 *
 * Thread-safety contract &mdash; every mutation path is lock-free:
 *   - Dropwizard `Counter.inc()` is internally backed by `java.util.concurrent.
 *     atomic.LongAdder`; writers contend only on thread-local cells.
 *   - A mirroring [[java.util.concurrent.atomic.AtomicLong]] per counter is
 *     exposed as a direct-read fast path for in-JVM decision logic (for
 *     example, `StreamingShuffleFallbackPolicy`, `BackpressureProtocol`, or
 *     unit tests that assert on counter state).
 *   - The buffer-utilization gauge is backed by an
 *     [[java.util.concurrent.atomic.AtomicReference]] of `java.lang.Double`.
 *
 * Binary compatibility (MiMa F-017): this class is `private[spark]` and lives in
 * a brand-new sub-package, so it introduces no public SPI signature and requires
 * no entry in `project/MimaExcludes.scala`. See
 * `blitzy-docs/streaming-shuffle-decision-log.md` for the full design rationale
 * (dual-update pattern, lock-free discipline, direct-read mirror choice).
 */
private[spark] class StreamingShuffleMetrics extends Source {

  /**
   * Top-level grouping that every Dropwizard sink prepends to the instrument
   * names below (e.g. Prometheus exposes `shuffle_streaming_spillCount`, JMX
   * exposes MBean `metrics:name=shuffle.streaming.spillCount`). MUST remain in
   * lock-step with `metrics.properties.template` (AAP section N13) so operators who
   * copy that template see the instruments at the same path the Spark runtime
   * uses.
   */
  override val sourceName: String = "shuffle.streaming"

  /**
   * Dropwizard registry that holds the gauge plus three counters. A fresh
   * registry instance is created per `StreamingShuffleMetrics`, matching the
   * `JVMCPUSource` and `ExecutorAllocationManagerSource` pattern &mdash; Spark's
   * `MetricsSystem.registerSource` subsequently scans this registry and pushes
   * every registered metric into the configured sinks.
   */
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // --------------------------------------------------------------------------
  // Atomic state (mutated from writer / reader / backpressure / spill threads).
  // --------------------------------------------------------------------------

  /**
   * Most-recent buffer-utilization percent observed by `MemorySpillManager`
   * (domain: 0.0 .. 100.0). Exposed to sinks through the `bufferUtilizationPercent`
   * Gauge below. Initial value 0.0 reflects an idle executor before any streaming
   * shuffle has begun. Typed as `java.lang.Double` to match the Dropwizard
   * `Gauge[T]` type parameter.
   */
  private val bufferUtilizationPercent: AtomicReference[java.lang.Double] =
    new AtomicReference[java.lang.Double](java.lang.Double.valueOf(0.0))

  /**
   * Direct-read mirror of `spillCountCounter`. Read by internal decision logic
   * (fallback policy, backpressure protocol) and tests via `spillCountValue`.
   */
  private val spillCount: AtomicLong = new AtomicLong(0L)

  /** Direct-read mirror of `backpressureEventsCounter`. */
  private val backpressureEvents: AtomicLong = new AtomicLong(0L)

  /** Direct-read mirror of `partialReadInvalidationsCounter`. */
  private val partialReadInvalidations: AtomicLong = new AtomicLong(0L)

  // --------------------------------------------------------------------------
  // Dropwizard instruments registered eagerly against `metricRegistry` so that
  // `MetricsSystem.registerSource` enumerates all four metrics immediately.
  // --------------------------------------------------------------------------

  metricRegistry.register(
    MetricRegistry.name("bufferUtilizationPercent"),
    new Gauge[java.lang.Double] {
      override def getValue: java.lang.Double = bufferUtilizationPercent.get()
    })

  /**
   * `MetricRegistry.counter` lazily registers a fresh `Counter` backed by
   * `LongAdder`. Pattern matches `ExecutorSource` (lines 86+) and
   * `LiveListenerBus` (line 275).
   */
  private val spillCountCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("spillCount"))

  private val backpressureEventsCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("backpressureEvents"))

  private val partialReadInvalidationsCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("partialReadInvalidations"))

  // --------------------------------------------------------------------------
  // Public mutation API. Every call updates both the AtomicLong mirror and the
  // Dropwizard Counter; both paths are lock-free and allocation-free.
  // --------------------------------------------------------------------------

  /**
   * Called by `StreamingShuffleWriter` (and `MemorySpillManager` on behalf of
   * the writer) each time a buffered partition is evicted to disk because the
   * memory threshold was crossed. One call per spill event.
   */
  def incrementSpillCount(): Unit = {
    spillCount.getAndIncrement()
    spillCountCounter.inc()
  }

  /**
   * Called by `BackpressureProtocol` each time a throttle decision fires &mdash;
   * e.g. the token bucket refuses a block send, the consumer heartbeat asks the
   * producer to slow down, or the consumer-position acknowledgement is delayed
   * beyond the 10-second threshold. One call per throttle event.
   */
  def incrementBackpressureEvents(): Unit = {
    backpressureEvents.getAndIncrement()
    backpressureEventsCounter.inc()
  }

  /**
   * Called by `StreamingShuffleReader` each time a producer connection times
   * out (5 s threshold per AAP section 0.1.2) and the consumer atomically discards all
   * partial blocks from that producer, notifying the DAG scheduler so the
   * upstream map task is recomputed. One call per atomic invalidation.
   */
  def incrementPartialReadInvalidations(): Unit = {
    partialReadInvalidations.getAndIncrement()
    partialReadInvalidationsCounter.inc()
  }

  /**
   * Called by `MemorySpillManager` on each 100 ms poll (AAP section 0.1.1) to publish
   * the current aggregate streaming-buffer utilization percent (0.0 .. 100.0)
   * across all shuffles on this executor. Values outside this range are not
   * rejected &mdash; callers may briefly overshoot during heavy bursts &mdash;
   * but the fallback policy interprets values &gt;= 100.0 as a memory-pressure
   * signal.
   *
   * @param value the buffer-utilization percent observed by this poll
   */
  def setBufferUtilizationPercent(value: Double): Unit = {
    bufferUtilizationPercent.set(java.lang.Double.valueOf(value))
  }

  // --------------------------------------------------------------------------
  // Public read-access helpers. Used by internal decision logic (fallback
  // policy, backpressure protocol) and unit tests. Each reads the `AtomicLong`
  // mirror or the `AtomicReference` directly.
  // --------------------------------------------------------------------------

  /** Current value of the `shuffle.streaming.spillCount` counter. */
  def spillCountValue: Long = spillCount.get()

  /** Current value of the `shuffle.streaming.backpressureEvents` counter. */
  def backpressureEventsValue: Long = backpressureEvents.get()

  /** Current value of the `shuffle.streaming.partialReadInvalidations` counter. */
  def partialReadInvalidationsValue: Long = partialReadInvalidations.get()

  /**
   * Current value of the `shuffle.streaming.bufferUtilizationPercent` gauge,
   * unboxed to a primitive `Double` for callers that do not need the
   * `java.lang.Double` return type the gauge exposes to Dropwizard.
   */
  def bufferUtilizationPercentValue: Double = bufferUtilizationPercent.get().doubleValue()
}
