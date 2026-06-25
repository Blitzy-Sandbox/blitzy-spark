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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[StreamingShuffleMetrics]] (the four-metric state holder) and
 * [[StreamingShuffleSource]] (the Dropwizard `Source` that exposes those four metrics through
 * Spark's existing metrics infrastructure).
 *
 * These are pure-unit tests: they exercise the in-memory metric state and the Dropwizard
 * `MetricRegistry` directly, so they need neither a `SparkContext` nor a live `MetricsSystem`.
 * Registering the source with the `MetricsSystem` is the streaming manager's job and is covered
 * by separate manager-level tests. Here we assert only the holder's semantics and the source's
 * registration contract: `sourceName`, the exact metric-name set, and that every gauge reads
 * through to the live holder.
 */
class StreamingShuffleMetricsSuite extends SparkFunSuite {

  /** The four fully-qualified metric names the source must register, order-independent. */
  private val expectedMetricNames = Set(
    "shuffle.streaming.bufferUtilizationPercent",
    "shuffle.streaming.spillCount",
    "shuffle.streaming.backpressureEvents",
    "shuffle.streaming.partialReadInvalidations")

  // --- StreamingShuffleMetrics: the mutable, thread-safe metric holder ---

  test("initial metric values are zero") {
    val metrics = new StreamingShuffleMetrics
    assert(metrics.getBufferUtilizationPercent === 0)
    assert(metrics.getSpillCount === 0L)
    assert(metrics.getBackpressureEvents === 0L)
    assert(metrics.getPartialReadInvalidations === 0L)
  }

  test("bufferUtilizationPercent gauge clamps to [0, 100]") {
    val metrics = new StreamingShuffleMetrics

    // A value inside the range is stored verbatim.
    metrics.setBufferUtilizationPercent(50)
    assert(metrics.getBufferUtilizationPercent === 50)

    // Values below the lower bound clamp to 0.
    metrics.setBufferUtilizationPercent(-5)
    assert(metrics.getBufferUtilizationPercent === 0)

    // Values above the upper bound clamp to 100.
    metrics.setBufferUtilizationPercent(150)
    assert(metrics.getBufferUtilizationPercent === 100)

    // The inclusive boundaries are preserved exactly.
    metrics.setBufferUtilizationPercent(100)
    assert(metrics.getBufferUtilizationPercent === 100)
    metrics.setBufferUtilizationPercent(0)
    assert(metrics.getBufferUtilizationPercent === 0)
  }

  test("spillCount increments are reflected by getter") {
    val metrics = new StreamingShuffleMetrics
    metrics.incrementSpillCount()
    metrics.incrementSpillCount()
    metrics.incrementSpillCount()
    assert(metrics.getSpillCount === 3L)
  }

  test("backpressureEvents increments are reflected by getter") {
    val metrics = new StreamingShuffleMetrics
    val n = 5
    (1 to n).foreach(_ => metrics.incrementBackpressureEvents())
    assert(metrics.getBackpressureEvents === n.toLong)
  }

  test("partialReadInvalidations increments are reflected by getter") {
    val metrics = new StreamingShuffleMetrics
    val n = 7
    (1 to n).foreach(_ => metrics.incrementPartialReadInvalidations())
    assert(metrics.getPartialReadInvalidations === n.toLong)
  }

  test("counter increments are thread-safe under concurrent mutation") {
    val metrics = new StreamingShuffleMetrics
    val numThreads = 4
    val incrementsPerThread = 1000
    val pool = Executors.newFixedThreadPool(numThreads)
    // A shared start barrier maximizes real contention; the done latch lets the main thread
    // wait deterministically for completion without busy-polling.
    val startBarrier = new CountDownLatch(1)
    val doneLatch = new CountDownLatch(numThreads)
    try {
      (1 to numThreads).foreach { _ =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            startBarrier.await()
            var i = 0
            while (i < incrementsPerThread) {
              metrics.incrementSpillCount()
              i += 1
            }
            doneLatch.countDown()
          }
        })
      }
      startBarrier.countDown()
      assert(doneLatch.await(30, TimeUnit.SECONDS), "worker threads did not finish in time")
      assert(metrics.getSpillCount === numThreads.toLong * incrementsPerThread)
    } finally {
      pool.shutdownNow()
    }
  }

  // --- StreamingShuffleSource: the Dropwizard Source adapting the holder above ---

  test("sourceName is exactly 'streamingShuffle'") {
    val source = new StreamingShuffleSource(new StreamingShuffleMetrics)
    assert(source.sourceName === "streamingShuffle")
  }

  test("metricRegistry exposes exactly the 4 expected metric keys") {
    val source = new StreamingShuffleSource(new StreamingShuffleMetrics)
    val registeredNames = source.metricRegistry.getMetrics.keySet.asScala.toSet
    assert(registeredNames === expectedMetricNames)
    assert(registeredNames.size === 4)
  }

  test("gauges reflect underlying StreamingShuffleMetrics state") {
    val metrics = new StreamingShuffleMetrics
    val source = new StreamingShuffleSource(metrics)

    // Mutate the live holder after the source is built: because each gauge reads through to the
    // holder, the registry must observe the updated values with no re-registration.
    metrics.setBufferUtilizationPercent(73)
    metrics.incrementSpillCount()
    metrics.incrementSpillCount()
    metrics.incrementBackpressureEvents()
    (1 to 4).foreach(_ => metrics.incrementPartialReadInvalidations())

    val gauges = source.metricRegistry.getGauges
    assert(gauges.get("shuffle.streaming.bufferUtilizationPercent").getValue === 73)
    assert(gauges.get("shuffle.streaming.spillCount").getValue === 2L)
    assert(gauges.get("shuffle.streaming.backpressureEvents").getValue === 1L)
    assert(gauges.get("shuffle.streaming.partialReadInvalidations").getValue === 4L)
  }
}
