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

import com.codahale.metrics.MetricRegistry

import org.apache.spark.metrics.source.Source

/**
 * Spark [[org.apache.spark.metrics.source.Source]] implementation for streaming-shuffle
 * metrics.
 *
 * Wires [[StreamingShuffleMetrics]] into the existing Dropwizard
 * [[org.apache.spark.metrics.MetricsSystem]] so that the JMX, CSV, Slf4j, Graphite,
 * Prometheus, and Web-UI sinks already configured in the host Spark application
 * automatically pick up the four streaming-shuffle observability metrics with no schema
 * changes anywhere else in the codebase.
 *
 * == Source Trait Contract ==
 * Per the existing [[org.apache.spark.metrics.source.Source]] trait, an implementation
 * must provide both:
 *   - a stable `sourceName: String` used by `MetricsSystem` to compose the source-prefix
 *     portion of every final JMX object name, and
 *   - a stable `metricRegistry: MetricRegistry` whose contents are scraped by every
 *     attached sink at the configured polling interval.
 *
 * Both members are exposed as `val`s here (constructed once per instance, never
 * reassigned) so that the registry reference remains stable across every read by every
 * attached sink. This is required because `MetricsSystem` caches the source's registry
 * reference at registration time and reuses that exact reference on every subsequent
 * scrape.
 *
 * == Metric Inventory ==
 * The metric keys carried in the registry come from
 * [[StreamingShuffleMetrics.getMetrics]] verbatim and are pre-prefixed by that metric
 * set with the AAP-mandated `shuffle.streaming.` operator-facing namespace:
 *   - `shuffle.streaming.bufferUtilizationPercent` -- Gauge of buffer-memory utilization,
 *     reported as an integer percent in the closed interval `[0, 100]`.
 *   - `shuffle.streaming.spillCount` -- monotonically increasing Counter of
 *     disk-spill events emitted by the streaming subsystem.
 *   - `shuffle.streaming.backpressureEvents` -- monotonically increasing Counter of
 *     backpressure events (rate-limit hits, missed heartbeats, priority-arbitration
 *     outcomes).
 *   - `shuffle.streaming.partialReadInvalidations` -- monotonically increasing Counter
 *     of producer-failure detections that resulted in partial buffered data being
 *     discarded before the existing
 *     [[org.apache.spark.shuffle.FetchFailedException]] path triggered DAG-scheduler
 *     upstream recomputation.
 *
 * Spark's `MetricsSystem` composes the application, executor, and source prefixes with
 * the metric key per its standard convention (see `MetricsSystem.buildRegistryName`),
 * yielding final names of the form
 * `<application>.<executor-id>.<source-name>.shuffle.streaming.<metric-name>`. The
 * `shuffle.streaming.<metric-name>` substring -- the operator-facing namespace required
 * by the streaming-shuffle specification -- is preserved end-to-end regardless of any
 * future change to the source name chosen below.
 *
 * == Coexistence ==
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* this source is registered
 * exclusively by `StreamingShuffleManager` when the user opts in to streaming shuffle
 * via `spark.shuffle.manager=streaming`. The default sort-shuffle path
 * (`spark.shuffle.manager=sort`, the production default) does not register this source
 * and therefore incurs zero additional metric overhead. Likewise, this class does not
 * touch the [[org.apache.spark.executor.ExecutorMetrics]] typed array nor any other
 * existing telemetry carrier; all four metrics flow exclusively through the
 * `MetricsSystem` Dropwizard registry path.
 *
 * == Idempotence ==
 * Two `StreamingShuffleSource` instances constructed against the same
 * `StreamingShuffleMetrics` produce registries containing the same four metric names
 * referencing the same underlying `Counter` / `Gauge` objects, so external consumers
 * observe identical readings from either instance. In practice only one instance is
 * created per executor JVM (one per `StreamingShuffleManager`).
 *
 * == Concurrency ==
 * The constructor performs a one-time `registerAll` against a freshly allocated
 * registry; no further mutation occurs from this class. Concurrent metric updates from
 * the streaming writer, reader, spill poller, and backpressure scheduler operate on the
 * `Counter` / `Gauge` instances inside `StreamingShuffleMetrics` directly -- this source
 * holds no additional locks and adds no additional synchronization overhead, satisfying
 * the streaming-shuffle telemetry CPU-budget requirement.
 *
 * @param metrics the underlying streaming-shuffle metric set whose four metrics are
 *                pre-populated into [[metricRegistry]] at construction time via the
 *                Dropwizard `MetricRegistry#registerAll(MetricSet)` idiom; must not be
 *                `null`
 */
private[spark] class StreamingShuffleSource(
    metrics: StreamingShuffleMetrics) extends Source {

  /**
   * Stable name of this metric source. Spark's `MetricsSystem` uses this as the
   * source-prefix segment in every final JMX object name and metric path produced by
   * its attached sinks (see `MetricsSystem.buildRegistryName`). The chosen name follows
   * the camelCase convention used by other Spark-registered sources such as
   * [[org.apache.spark.metrics.source.JvmSource]] (`"jvm"`) and
   * [[org.apache.spark.metrics.source.JVMCPUSource]] (`"JVMCPU"`).
   */
  override val sourceName: String = "streamingShuffle"

  /**
   * Registry containing the four streaming-shuffle metrics from
   * [[StreamingShuffleMetrics]]. Constructed once at instance creation and never
   * mutated thereafter.
   *
   * The implementation uses Dropwizard's idiomatic `MetricRegistry#registerAll` to
   * expand the [[StreamingShuffleMetrics]] `MetricSet` into individual metric entries
   * keyed by the names declared in
   * [[StreamingShuffleMetrics.getMetrics]]. This is the same pattern used by
   * [[org.apache.spark.metrics.source.JvmSource]] for its three JVM metric sets and
   * keeps the source body minimal and consistent with the project's existing source
   * implementations.
   *
   * If `registerAll` raises [[IllegalArgumentException]] because a metric with the same
   * name is already registered (a defensive guard inside Dropwizard), the exception is
   * surfaced to the caller; in practice this only occurs if the same
   * `StreamingShuffleMetrics` instance is registered twice into the same registry,
   * which never happens in correct usage because each `StreamingShuffleSource`
   * allocates its own fresh registry above.
   */
  override val metricRegistry: MetricRegistry = {
    val registry = new MetricRegistry()
    // Register all four metrics from the StreamingShuffleMetrics MetricSet in one call.
    // Dropwizard's MetricRegistry#registerAll expands a MetricSet into individual metric
    // registrations under the names declared by MetricSet#getMetrics -- here producing
    // four entries keyed shuffle.streaming.{bufferUtilizationPercent, spillCount,
    // backpressureEvents, partialReadInvalidations}. This is the same pattern used by
    // org.apache.spark.metrics.source.JvmSource for its garbage-collector, memory-usage,
    // and buffer-pool metric sets.
    registry.registerAll(metrics)
    registry
  }
}
