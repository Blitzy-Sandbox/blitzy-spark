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

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[StreamingShuffleMetrics]] and its companion [[StreamingShuffleSource]].
 *
 * These are pure unit tests: they exercise the four streaming-shuffle telemetry metrics
 * (`bufferUtilizationPercent`, `spillCount`, `backpressureEvents`, and
 * `partialReadInvalidations`) directly against the metrics holder and the Dropwizard
 * [[org.apache.spark.metrics.source.Source]] bridge, with no `SparkContext` or `MetricsSystem`
 * bootstrap required. They validate the streaming-shuffle observability quality gate: that every
 * counter increment and gauge update is recorded and surfaced correctly, and that the source
 * shares the very same underlying metric instances (so remote increments are always reflected in
 * the exported values).
 *
 * Dropwizard counter values are asserted through `counter.getCount` (a `Long`); the gauge is
 * asserted through [[StreamingShuffleMetrics.currentBufferUtilization]].
 */
class StreamingShuffleMetricsSuite extends SparkFunSuite {

  test("counters start at zero") {
    val metrics = new StreamingShuffleMetrics()
    assert(metrics.spillCounter.getCount == 0L)
    assert(metrics.backpressureCounter.getCount == 0L)
    assert(metrics.partialReadInvalidationsCounter.getCount == 0L)
    assert(metrics.currentBufferUtilization == 0)
  }

  test("incSpillCount increments spillCount") {
    val metrics = new StreamingShuffleMetrics()
    metrics.incSpillCount()
    metrics.incSpillCount()
    metrics.incSpillCount()
    assert(metrics.spillCounter.getCount == 3L)
  }

  test("incBackpressureEvents increments backpressureEvents") {
    val metrics = new StreamingShuffleMetrics()
    metrics.incBackpressureEvents()
    metrics.incBackpressureEvents()
    assert(metrics.backpressureCounter.getCount == 2L)
  }

  test("incPartialReadInvalidations increments partialReadInvalidations") {
    val metrics = new StreamingShuffleMetrics()
    metrics.incPartialReadInvalidations()
    assert(metrics.partialReadInvalidationsCounter.getCount == 1L)
  }

  test("updateBufferUtilization sets the gauge") {
    val metrics = new StreamingShuffleMetrics()
    metrics.updateBufferUtilization(73)
    assert(metrics.currentBufferUtilization == 73)
    metrics.updateBufferUtilization(0)
    assert(metrics.currentBufferUtilization == 0)
    metrics.updateBufferUtilization(100)
    assert(metrics.currentBufferUtilization == 100)
  }

  test("StreamingShuffleSource exposes sourceName and registry") {
    val metrics = new StreamingShuffleMetrics()
    val source = new StreamingShuffleSource(metrics)
    assert(source.sourceName == "streamingShuffle")
    assert(source.metricRegistry != null)

    // The four leaf metric names form the public observability contract. The production source
    // registers them under exact (non-dotted) names, but we also accept a dotted/qualified name
    // that ends with the short name to stay robust against registry-prefix changes.
    val names = source.metricRegistry.getNames
    assert(hasMetric(source, "spillCount"), s"missing spillCount in $names")
    assert(hasMetric(source, "backpressureEvents"), s"missing backpressureEvents in $names")
    assert(
      hasMetric(source, "partialReadInvalidations"), s"missing partialReadInvalidations in $names")
    assert(
      hasMetric(source, "bufferUtilizationPercent"), s"missing bufferUtilizationPercent in $names")
  }

  test("source reflects metric mutations") {
    val metrics = new StreamingShuffleMetrics()
    val source = new StreamingShuffleSource(metrics)

    // The source registers the SAME Counter instance owned by the metrics holder, so an increment
    // performed at the producing call site must be observable through the source's registry.
    metrics.incSpillCount()
    metrics.incSpillCount()
    val registered = source.metricRegistry.getCounters.get("spillCount")
    assert(registered != null)
    assert(registered.getCount == 2L)
  }

  /**
   * Returns `true` if the source's metric registry exposes a metric whose registered name either
   * equals `shortName` exactly or ends with it (tolerating a dotted/qualified prefix).
   */
  private def hasMetric(source: StreamingShuffleSource, shortName: String): Boolean = {
    val names = source.metricRegistry.getNames
    if (names.contains(shortName)) {
      true
    } else {
      val iter = names.iterator()
      var found = false
      while (!found && iter.hasNext) {
        val name = iter.next()
        if (name == shortName || name.endsWith("." + shortName)) {
          found = true
        }
      }
      found
    }
  }
}
