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

import com.codahale.metrics.MetricRegistry
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[StreamingShuffleMetrics]] and its [[StreamingShuffleSource]] adapter.
 *
 * The suite verifies the explicit AAP observability requirements for the streaming shuffle
 * backend:
 *
 *   - the three monotonic counters (`spillCount`, `backpressureEvents`,
 *     `partialReadInvalidations`) start at zero and tally each increment exactly;
 *   - the `bufferUtilizationPercent` gauge is clamped into the inclusive `[0, 100]` range so a
 *     malformed sample can never corrupt the published value;
 *   - the lock-free (AtomicLong) increment semantics are exact under concurrency, which is what
 *     keeps telemetry overhead within the streaming backend's sub-1% executor-CPU budget;
 *   - [[StreamingShuffleSource]] registers exactly the four metrics under the canonical
 *     `shuffle.streaming.*` namespace and adapts the holder without double counting, guaranteeing
 *     JMX/Prometheus exposure works without standing up a live `MetricsSystem`.
 *
 * The tests are pure and deterministic: they need no `SparkContext`, and the concurrency test
 * uses `Thread.join()` (never sleeps) so assertions observe a fully-published state.
 */
class StreamingShuffleMetricsSuite extends SparkFunSuite with Matchers {

  // Builds the canonical `shuffle.streaming.<name>` path exactly as StreamingShuffleSource does,
  // so registry lookups in the value-reflection test resolve to the registered gauges.
  private def metricName(name: String): String =
    MetricRegistry.name("shuffle", "streaming", name)

  test("counters start at zero and increment by one") {
    val m = new StreamingShuffleMetrics
    // All four readers must report their initial (empty) state.
    m.spillCount mustBe 0L
    m.backpressureEvents mustBe 0L
    m.partialReadInvalidations mustBe 0L
    m.bufferUtilizationPercent mustBe 0.0

    // Distinct counts per counter guard against the increments being routed to the wrong atomic.
    (0 until 3).foreach(_ => m.incSpillCount())
    (0 until 2).foreach(_ => m.incBackpressureEvents())
    (0 until 5).foreach(_ => m.incPartialReadInvalidations())

    m.spillCount mustBe 3L
    m.backpressureEvents mustBe 2L
    m.partialReadInvalidations mustBe 5L
  }

  test("bufferUtilizationPercent is clamped to [0,100]") {
    val m = new StreamingShuffleMetrics
    // An in-range sample is stored verbatim.
    m.setBufferUtilizationPercent(42.5)
    m.bufferUtilizationPercent mustBe 42.5 +- 1e-9
    // An over-range sample saturates at the upper bound.
    m.setBufferUtilizationPercent(150.0)
    m.bufferUtilizationPercent mustBe 100.0 +- 1e-9
    // A negative sample saturates at the lower bound.
    m.setBufferUtilizationPercent(-10.0)
    m.bufferUtilizationPercent mustBe 0.0 +- 1e-9
  }

  test("concurrent increments are lock-free and exact") {
    val m = new StreamingShuffleMetrics
    val numThreads = 8
    val incrementsPerThread = 10000
    // Each thread hammers the same counter; the AtomicLong CAS loop must lose no increments.
    val threads = (0 until numThreads).map { _ =>
      new Thread(() => {
        var i = 0
        while (i < incrementsPerThread) {
          m.incSpillCount()
          i += 1
        }
      })
    }
    threads.foreach(_.start())
    // join() establishes a happens-before edge, so the final read observes every increment.
    threads.foreach(_.join())
    m.spillCount mustBe numThreads.toLong * incrementsPerThread
  }

  test("StreamingShuffleSource exposes the four metrics under shuffle.streaming.*") {
    val m = new StreamingShuffleMetrics
    val src = new StreamingShuffleSource(m)
    src.sourceName mustBe "StreamingShuffle"

    val names = src.metricRegistry.getNames().asScala.toSet
    // Exactly the four streaming metrics are registered, no more and no fewer.
    names.size mustBe 4
    names.exists(_.contains(StreamingShuffleMetrics.SPILL_COUNT)) mustBe true
    names.exists(_.contains(StreamingShuffleMetrics.BACKPRESSURE_EVENTS)) mustBe true
    names.exists(_.contains(StreamingShuffleMetrics.PARTIAL_READ_INVALIDATIONS)) mustBe true
    names.exists(_.contains(StreamingShuffleMetrics.BUFFER_UTILIZATION_PERCENT)) mustBe true
    // Every registered metric lives under the canonical shuffle.streaming namespace.
    names.forall(_.startsWith(StreamingShuffleMetrics.METRIC_PREFIX)) mustBe true
  }

  test("source gauge values reflect the holder") {
    val m = new StreamingShuffleMetrics
    val src = new StreamingShuffleSource(m)

    // Mutate the holder after the source is built to prove the gauges sample live state.
    (0 until 4).foreach(_ => m.incSpillCount())
    (0 until 6).foreach(_ => m.incBackpressureEvents())
    (0 until 9).foreach(_ => m.incPartialReadInvalidations())
    m.setBufferUtilizationPercent(73.5)

    val gauges = src.metricRegistry.getGauges()
    def gaugeLong(name: String): Long =
      gauges.get(metricName(name)).getValue().asInstanceOf[Long]
    def gaugeDouble(name: String): Double =
      gauges.get(metricName(name)).getValue().asInstanceOf[Double]

    val expSpill = m.spillCount
    val expBackpressure = m.backpressureEvents
    val expPartial = m.partialReadInvalidations
    val expUtil = m.bufferUtilizationPercent

    // The Source is a thin adapter over the atomic holder, so each gauge must echo it exactly.
    gaugeLong(StreamingShuffleMetrics.SPILL_COUNT) mustBe expSpill
    gaugeLong(StreamingShuffleMetrics.BACKPRESSURE_EVENTS) mustBe expBackpressure
    gaugeLong(StreamingShuffleMetrics.PARTIAL_READ_INVALIDATIONS) mustBe expPartial
    gaugeDouble(StreamingShuffleMetrics.BUFFER_UTILIZATION_PERCENT) mustBe expUtil +- 1e-9
  }
}
