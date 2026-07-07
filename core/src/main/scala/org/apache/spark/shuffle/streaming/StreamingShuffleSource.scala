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

import org.apache.spark.annotation.Since
import org.apache.spark.metrics.source.Source

/**
 * A [[org.apache.spark.metrics.source.Source]] that exposes the streaming-shuffle telemetry held
 * by [[StreamingShuffleMetrics]] to Spark's `MetricsSystem`.
 *
 * This is the observability bridge for the opt-in streaming shuffle backend
 * (`spark.shuffle.manager=streaming`). `StreamingShuffleManager` constructs a single instance of
 * this source - passing the shared [[StreamingShuffleMetrics]] holder - and registers it on
 * executors via `SparkEnv.get.metricsSystem.registerSource(...)`, gated on `SparkEnv.get != null`
 * for local-mode safety. Once registered, every configured metrics sink (JMX, Prometheus, CSV,
 * Slf4j) automatically picks up the four metrics below with no sink-specific wiring, exactly as
 * the existing `DAGSchedulerSource` and `ExecutorSource` do for their own telemetry.
 *
 * '''Naming contract.''' Because [[sourceName]] is `"streamingShuffle"`, each metric surfaces
 * across all sinks under the qualified name `<app>.<executor>.streamingShuffle.<name>`. The four
 * leaf names form a public, stable observability contract shared with `docs/monitoring.md`,
 * `blitzy-docs/streaming-shuffle/observability.md`, and the Grafana dashboard template; they must
 * not be renamed without updating every consumer:
 *
 *  - `bufferUtilizationPercent` - a gauge (0-100) reporting current per-executor buffer usage.
 *  - `spillCount` - a counter of buffered partitions spilled to disk.
 *  - `backpressureEvents` - a counter of backpressure throttling events raised by flow control.
 *  - `partialReadInvalidations` - a counter of in-progress reads invalidated on producer failure.
 *
 * '''Shared-instance wiring.''' The three counters are registered as the exact `Counter` objects
 * owned by the supplied [[StreamingShuffleMetrics]] (obtained through its `*Counter` accessors)
 * rather than freshly-created counters. This is the crux of the observability wiring: increments
 * performed at the producing call sites (`incSpillCount()`, `incBackpressureEvents()`,
 * `incPartialReadInvalidations()`) mutate the very same objects the `MetricsSystem` reports, so
 * remote increments are always reflected in the exported values. Registering fresh counters here
 * (for example via `metricRegistry.counter(...)`) would silently decouple them from those sites.
 * The gauge instead delegates to [[StreamingShuffleMetrics.currentBufferUtilization]] so that
 * every scrape observes the most recently published value.
 *
 * All reads are single lock-free atomic loads, keeping telemetry overhead well under the 1% CPU
 * budget mandated for the streaming shuffle feature.
 *
 * @param metrics the shared metrics holder whose live gauge state and counter instances are
 *                exported through this source
 */
@Since("4.2.0")
private[spark] class StreamingShuffleSource(metrics: StreamingShuffleMetrics) extends Source {

  override val sourceName: String = "streamingShuffle"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // `bufferUtilizationPercent` is a live gauge: each scrape reads the most recent utilization
  // percentage that `MemorySpillManager` publishes into the shared metrics holder.
  metricRegistry.register(MetricRegistry.name("bufferUtilizationPercent"), new Gauge[Int] {
    override def getValue: Int = metrics.currentBufferUtilization
  })

  // Register the SAME Counter instances that StreamingShuffleMetrics increments so that every
  // increment at the producing call sites is reflected in the exported metric. Creating fresh
  // counters here would decouple the exported values from those increment call sites.
  metricRegistry.register(MetricRegistry.name("spillCount"), metrics.spillCounter)
  metricRegistry.register(MetricRegistry.name("backpressureEvents"), metrics.backpressureCounter)
  metricRegistry.register(
    MetricRegistry.name("partialReadInvalidations"), metrics.partialReadInvalidationsCounter)
}
