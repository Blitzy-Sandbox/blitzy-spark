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

import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.shuffle.streaming.StreamingShuffleMetrics.{
  BACKPRESSURE_EVENTS,
  BUFFER_UTILIZATION_PERCENT,
  METRIC_PREFIX,
  PARTIAL_READ_INVALIDATIONS,
  SPILL_COUNT
}

/**
 * Unit tests for [[StreamingShuffleMetrics]] and its [[StreamingShuffleSource]] adapter.
 *
 * These tests are pure and deterministic: they need no `SparkContext` and no running
 * `MetricsSystem`. They verify the four `shuffle.streaming.*` telemetry values: the
 * `bufferUtilizationPercent` gauge (clamped to the inclusive range [0, 100]) and the three
 * monotonic counters (`spillCount`, `backpressureEvents`, `partialReadInvalidations`). They also
 * assert the lock-free, exact increment semantics of the atomic counters, and that
 * [[StreamingShuffleSource]] registers exactly those four metrics under the `shuffle.streaming.*`
 * namespace so JMX/Prometheus exposure works without a live backend.
 */
class StreamingShuffleMetricsSuite extends SparkFunSuite with Matchers {

  /**
   * Builds the fully qualified Dropwizard metric name for a streaming-shuffle short name, e.g.
   * `metricName("spillCount")` resolves to `shuffle.streaming.spillCount`.
   */
  private def metricName(shortName: String): String =
    MetricRegistry.name(METRIC_PREFIX, shortName)

  test("counters start at zero and increment by one") {
    val m = new StreamingShuffleMetrics

    // Every reader must report its initial value before any mutation occurs.
    m.spillCount mustBe 0L
    m.backpressureEvents mustBe 0L
    m.partialReadInvalidations mustBe 0L
    m.bufferUtilizationPercent mustBe (0.0 +- 1e-9)

    // Distinct increment counts guard against any cross-wiring between the three counters.
    (0 until 3).foreach(_ => m.incSpillCount())
    (0 until 2).foreach(_ => m.incBackpressureEvents())
    (0 until 5).foreach(_ => m.incPartialReadInvalidations())

    m.spillCount mustBe 3L
    m.backpressureEvents mustBe 2L
    m.partialReadInvalidations mustBe 5L
  }

  test("bufferUtilizationPercent is clamped to [0, 100]") {
    val m = new StreamingShuffleMetrics

    // An in-range value is published verbatim.
    m.setBufferUtilizationPercent(42.5)
    m.bufferUtilizationPercent mustBe (42.5 +- 1e-9)

    // Values above the range collapse to the 100 ceiling.
    m.setBufferUtilizationPercent(150.0)
    m.bufferUtilizationPercent mustBe (100.0 +- 1e-9)

    // Values below the range collapse to the 0 floor.
    m.setBufferUtilizationPercent(-10.0)
    m.bufferUtilizationPercent mustBe (0.0 +- 1e-9)
  }

  test("concurrent increments are lock-free and exact") {
    val m = new StreamingShuffleMetrics
    val numThreads = 8
    val incrementsPerThread = 10000

    // A single stateless worker is safe to share: it only touches the lock-free atomic holder.
    // An AtomicLong guarantees no lost updates, so the final total must be exactly
    // numThreads * incrementsPerThread without any locking on the hot path.
    val worker = new Runnable {
      override def run(): Unit = (0 until incrementsPerThread).foreach(_ => m.incSpillCount())
    }
    val threads = (0 until numThreads).map(_ => new Thread(worker))
    threads.foreach(_.start())
    threads.foreach(_.join())

    m.spillCount mustBe numThreads.toLong * incrementsPerThread
  }

  test("StreamingShuffleSource exposes the four metrics under shuffle.streaming.*") {
    val m = new StreamingShuffleMetrics
    val src = new StreamingShuffleSource(m)

    src.sourceName mustBe "StreamingShuffle"

    // getNames returns the registered Dropwizard names without needing a live MetricsSystem.
    val names = src.metricRegistry.getNames.asScala
    names.size mustBe 4
    names.exists(_.contains(SPILL_COUNT)) mustBe true
    names.exists(_.contains(BACKPRESSURE_EVENTS)) mustBe true
    names.exists(_.contains(PARTIAL_READ_INVALIDATIONS)) mustBe true
    names.exists(_.contains(BUFFER_UTILIZATION_PERCENT)) mustBe true
    names.forall(_.startsWith(METRIC_PREFIX)) mustBe true
  }

  test("source gauge values reflect the holder") {
    val m = new StreamingShuffleMetrics
    val src = new StreamingShuffleSource(m)

    // Mutate the holder with distinct counts and a known gauge sample.
    (0 until 2).foreach(_ => m.incSpillCount())
    m.incBackpressureEvents()
    (0 until 3).foreach(_ => m.incPartialReadInvalidations())
    m.setBufferUtilizationPercent(55.5)

    // The Source registers gauges that read straight through to the holder, so each Codahale
    // value must equal the holder accessor (proving the adapter does not double count).
    val gauges = src.metricRegistry.getGauges

    val spill = gauges.get(metricName(SPILL_COUNT)).asInstanceOf[Gauge[Long]]
    spill.getValue mustBe m.spillCount
    spill.getValue mustBe 2L

    val backpressure = gauges.get(metricName(BACKPRESSURE_EVENTS)).asInstanceOf[Gauge[Long]]
    backpressure.getValue mustBe m.backpressureEvents
    backpressure.getValue mustBe 1L

    val partial = gauges.get(metricName(PARTIAL_READ_INVALIDATIONS)).asInstanceOf[Gauge[Long]]
    partial.getValue mustBe m.partialReadInvalidations
    partial.getValue mustBe 3L

    val buffer = gauges.get(metricName(BUFFER_UTILIZATION_PERCENT)).asInstanceOf[Gauge[Double]]
    buffer.getValue mustBe (m.bufferUtilizationPercent +- 1e-9)
    buffer.getValue mustBe (55.5 +- 1e-9)
  }

  test("reset returns every counter and the gauge to its initial value") {
    val m = new StreamingShuffleMetrics

    // Drive all four telemetry values away from their defaults.
    (0 until 4).foreach(_ => m.incSpillCount())
    (0 until 2).foreach(_ => m.incBackpressureEvents())
    m.incPartialReadInvalidations()
    m.setBufferUtilizationPercent(73.25)
    m.spillCount mustBe 4L
    m.backpressureEvents mustBe 2L
    m.partialReadInvalidations mustBe 1L
    m.bufferUtilizationPercent mustBe (73.25 +- 1e-9)

    // reset() is the test-isolation/stress-reuse hook: it must zero every field independently.
    m.reset()
    m.spillCount mustBe 0L
    m.backpressureEvents mustBe 0L
    m.partialReadInvalidations mustBe 0L
    m.bufferUtilizationPercent mustBe (0.0 +- 1e-9)
  }
}
