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

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source

/**
 * Dropwizard [[org.apache.spark.metrics.source.Source]] that exposes the four streaming-shuffle
 * metrics held by [[StreamingShuffleMetrics]] through Spark's existing metrics infrastructure.
 *
 * This class is a thin, read-only adapter: it owns a Dropwizard `MetricRegistry` populated with
 * four gauges, each of which reads the current value of the corresponding metric directly from
 * the supplied [[StreamingShuffleMetrics]] instance. Because the gauges read through to the live
 * metric holder rather than caching values, [[StreamingShuffleMetrics]] remains the single source
 * of truth and no value can ever drift or be double-counted. Crucially, no separate Dropwizard
 * `Counter`s are created for the counter-style metrics; instead each is surfaced as a
 * `Gauge[Long]` over the underlying `LongAdder` sum, which keeps reporting consistent with the
 * owning holder.
 *
 * The streaming shuffle subsystem introduces NO new metrics endpoint. Once an instance of this
 * source is registered with the existing `MetricsSystem`, the four metrics are exposed through
 * the already-configured JMX, Prometheus, CSV, and SLF4J sinks with zero additional wiring.
 *
 * Naming: the four metrics are registered under the `shuffle.streaming.` namespace. The
 * `MetricsSystem` composes the fully-qualified metric name by combining the application id, the
 * executor id, this source's [[sourceName]] (`"streamingShuffle"`), and the per-metric registry
 * name. For example, a metric surfaces through JMX with an object name of the form
 * `metrics:name=<app-id>.<executor-id>.streamingShuffle.shuffle.streaming.<metric>`. The registry
 * names below therefore intentionally contain only the `shuffle.streaming.<metric>` suffix; the
 * application/executor prefix and the `streamingShuffle` source segment are applied automatically
 * by the `MetricsSystem` and must not be prepended here.
 *
 * The four registered metrics are:
 *  - `shuffle.streaming.bufferUtilizationPercent` (gauge, 0-100): current buffer fill level.
 *  - `shuffle.streaming.spillCount` (counter, surfaced as a gauge): number of disk spill events.
 *  - `shuffle.streaming.backpressureEvents` (counter, surfaced as a gauge): backpressure
 *    activations raised by the flow-control protocol.
 *  - `shuffle.streaming.partialReadInvalidations` (counter, surfaced as a gauge): partial reads
 *    invalidated on producer failure (each defers to the existing DAG-scheduler recomputation).
 *
 * Registration is intentionally NOT performed by this class. `StreamingShuffleManager` constructs
 * a single instance of this source and registers it via
 * `SparkEnv.get.metricsSystem.registerSource(...)` when a `SparkEnv` is available (i.e. on
 * executors and the driver). This file only DEFINES the source.
 *
 * Thread-safety: this source is safe to read concurrently. The gauges merely delegate to the
 * thread-safe accessors on [[StreamingShuffleMetrics]] (backed by `AtomicInteger`/`LongAdder`),
 * so `MetricsSystem` reporter threads can sample the metrics without extra synchronization.
 *
 * @param metrics the live, mutable holder whose values this source exposes. The reference is
 *                captured by the registered gauges so that every sample reflects the current
 *                state.
 */
private[spark] class StreamingShuffleSource(metrics: StreamingShuffleMetrics)
  extends Source with Logging {

  /**
   * The Dropwizard registry backing this source. Populated eagerly during construction with the
   * four streaming-shuffle gauges below.
   */
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * The source segment of the fully-qualified metric name. Must remain exactly
   * `"streamingShuffle"` so the JMX/Prometheus naming convention documented above is preserved.
   */
  override val sourceName: String = "streamingShuffle"

  // Gauge for the current buffer fill level, a percentage in the inclusive range [0, 100].
  metricRegistry.register(
    MetricRegistry.name("shuffle.streaming.bufferUtilizationPercent"),
    new Gauge[Int] {
      override def getValue: Int = metrics.getBufferUtilizationPercent
    })

  // Gauge over the disk-spill event tally. Read through to the underlying LongAdder sum so the
  // value never diverges from the owning StreamingShuffleMetrics holder.
  metricRegistry.register(
    MetricRegistry.name("shuffle.streaming.spillCount"),
    new Gauge[Long] {
      override def getValue: Long = metrics.getSpillCount
    })

  // Gauge over the backpressure-activation tally.
  metricRegistry.register(
    MetricRegistry.name("shuffle.streaming.backpressureEvents"),
    new Gauge[Long] {
      override def getValue: Long = metrics.getBackpressureEvents
    })

  // Gauge over the partial-read-invalidation tally (producer-failure recovery events).
  metricRegistry.register(
    MetricRegistry.name("shuffle.streaming.partialReadInvalidations"),
    new Gauge[Long] {
      override def getValue: Long = metrics.getPartialReadInvalidations
    })
}
