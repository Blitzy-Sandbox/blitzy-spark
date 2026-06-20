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

/**
 * A [[org.apache.spark.metrics.source.Source]] that adapts the dependency-free
 * [[StreamingShuffleMetrics]] holder onto a Dropwizard/Codahale [[MetricRegistry]] so the four
 * `shuffle.streaming.*` telemetry values are emitted through Spark's existing `MetricsSystem`
 * (and therefore through every configured sink, including JMX and the Prometheus endpoint).
 *
 * The source is intentionally a thin adapter and owns no metric state of its own. Each registered
 * metric is a `Gauge` whose `getValue` delegates directly to the corresponding lock-free accessor
 * on the supplied holder. The three event tallies (`spillCount`, `backpressureEvents`,
 * `partialReadInvalidations`) are exposed as `Gauge[Long]` views over the holder's atomics rather
 * than as Codahale `Counter`s. Doing so keeps [[StreamingShuffleMetrics]] the single source of
 * truth and removes any risk of double counting that a parallel Codahale counter would introduce,
 * while the gauge of `bufferUtilizationPercent` simply samples the holder's clamped `[0, 100]`
 * value on read. Because every read merely forwards to an atomic or volatile field, the adapter
 * adds no locking and keeps telemetry overhead within the streaming backend's sub-1% CPU budget.
 *
 * All metrics are registered under the canonical `shuffle.streaming.<name>` namespace. The
 * `MetricsSystem` further namespaces the source by application/instance prefix when it is
 * registered, so [[sourceName]] is kept short and stable.
 *
 * This class never touches `SparkEnv`. The owning `StreamingShuffleManager` is responsible for
 * registering the instance with the `MetricsSystem`, gated on `SparkEnv.get != null` for
 * local-mode safety, so the adapter is safe to construct in any environment (including tests).
 *
 * @param metrics the live, shared telemetry holder published by the streaming shuffle backend;
 *                its accessors are read on every sink scrape.
 */
private[spark] class StreamingShuffleSource(metrics: StreamingShuffleMetrics) extends Source {

  // Kept short and stable; the MetricsSystem prepends the application/instance namespace.
  override val sourceName: String = "StreamingShuffle"

  // A fresh registry owned by this source. It is populated below, in the primary constructor,
  // before this source is ever handed to the MetricsSystem.
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // Builds the canonical `shuffle.streaming.<name>` metric path in a single place so that every
  // registration below is guaranteed to resolve into the same namespace.
  private def metricName(name: String): String =
    MetricRegistry.name("shuffle", "streaming", name)

  // Gauge: current aggregate buffer-utilization percentage, always within [0, 100].
  metricRegistry.register(
    metricName(StreamingShuffleMetrics.BUFFER_UTILIZATION_PERCENT),
    new Gauge[Double] {
      override def getValue: Double = metrics.bufferUtilizationPercent
    })

  // Counter view: monotonic number of disk-spill events, sampled from the holder's atomic.
  metricRegistry.register(
    metricName(StreamingShuffleMetrics.SPILL_COUNT),
    new Gauge[Long] {
      override def getValue: Long = metrics.spillCount
    })

  // Counter view: monotonic number of backpressure throttling events.
  metricRegistry.register(
    metricName(StreamingShuffleMetrics.BACKPRESSURE_EVENTS),
    new Gauge[Long] {
      override def getValue: Long = metrics.backpressureEvents
    })

  // Counter view: monotonic number of partial-read invalidations caused by producer failure.
  metricRegistry.register(
    metricName(StreamingShuffleMetrics.PARTIAL_READ_INVALIDATIONS),
    new Gauge[Long] {
      override def getValue: Long = metrics.partialReadInvalidations
    })
}
