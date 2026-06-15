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

import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.streaming.StreamingShuffleMetrics._

/**
 * A [[Source]] that adapts the lock-free [[StreamingShuffleMetrics]] holder into a
 * Dropwizard/Codahale [[MetricRegistry]] so the four streaming-shuffle telemetry values are
 * exposed through Spark's existing `MetricsSystem` (and therefore through every configured
 * metrics sink, including JMX and the Prometheus servlet) with no change to the metrics framework
 * itself.
 *
 * ==Single source of truth==
 *
 * Every metric is registered as a Codahale [[Gauge]] that simply reads the corresponding accessor
 * on the supplied [[StreamingShuffleMetrics]] holder. The holder owns the underlying
 * `java.util.concurrent.atomic` state and remains the single source of truth, so wiring gauges
 * (rather than maintaining a second set of Codahale `Counter`s that the hot path would also have
 * to increment) structurally rules out the classic double-counting bug. Each gauge read is a
 * single lock-free volatile/atomic load, keeping telemetry overhead within the
 * sub-1%-executor-CPU budget.
 *
 * ==Naming==
 *
 * The metrics are registered under the `shuffle.streaming.*` namespace by combining
 * [[StreamingShuffleMetrics.METRIC_PREFIX]] with each metric's short name (for example
 * `shuffle.streaming.spillCount`). Centralizing those names on the holder keeps the holder and
 * this source in agreement. The [[sourceName]] is the stable, simple `StreamingShuffle`; the
 * `MetricsSystem` performs any further instance/namespace prefixing.
 *
 * ==Registration==
 *
 * This class never touches `SparkEnv`. It is instantiated and registered with the executor
 * `MetricsSystem` by `StreamingShuffleManager`, which gates registration on
 * `SparkEnv.get != null` for local-mode safety. Keeping that gate in the manager lets this
 * adapter stay a pure, dependency-light translation layer that is trivial to unit test without a
 * live `MetricsSystem`.
 *
 * @param metrics
 *   the live, lock-free metrics holder whose accessors back every registered gauge
 */
private[spark] class StreamingShuffleSource(metrics: StreamingShuffleMetrics) extends Source {

  // A stable, simple source name; the MetricsSystem namespaces it per instance/sink.
  override val sourceName: String = "StreamingShuffle"

  // Declared before the register(...) statements below so the registry exists when they run.
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // Gauge: current aggregate buffer-utilization percentage, always in [0, 100].
  metricRegistry.register(
    MetricRegistry.name(METRIC_PREFIX, BUFFER_UTILIZATION_PERCENT),
    new Gauge[Double] {
      override def getValue: Double = metrics.bufferUtilizationPercent
    })

  // Counter-style gauges over the holder's monotonic atomic counters. Reading the atomic through
  // a gauge keeps the holder authoritative and avoids incrementing two counters on the hot path.
  metricRegistry.register(
    MetricRegistry.name(METRIC_PREFIX, SPILL_COUNT),
    new Gauge[Long] {
      override def getValue: Long = metrics.spillCount
    })

  metricRegistry.register(
    MetricRegistry.name(METRIC_PREFIX, BACKPRESSURE_EVENTS),
    new Gauge[Long] {
      override def getValue: Long = metrics.backpressureEvents
    })

  metricRegistry.register(
    MetricRegistry.name(METRIC_PREFIX, PARTIAL_READ_INVALIDATIONS),
    new Gauge[Long] {
      override def getValue: Long = metrics.partialReadInvalidations
    })
}
