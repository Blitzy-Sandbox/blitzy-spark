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

import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.Counter

import org.apache.spark.annotation.Since

/**
 * Holder for the four streaming-shuffle telemetry metrics that are surfaced to Spark's
 * `MetricsSystem` through the companion `StreamingShuffleSource`.
 *
 * The streaming shuffle backend (`spark.shuffle.manager=streaming`) uses these metrics to expose
 * the runtime health of its in-memory buffering, spill, and backpressure subsystems:
 *
 *  - `bufferUtilizationPercent` - a gauge (0-100) reporting the percentage of the per-executor
 *    streaming buffer budget currently occupied. It is backed by a single [[AtomicInteger]] that
 *    `MemorySpillManager` refreshes on every 100 ms utilization poll and that is read back by the
 *    gauge registered in `StreamingShuffleSource` via [[currentBufferUtilization]].
 *  - `spillCount` - a counter of the number of buffered partitions spilled to disk.
 *  - `backpressureEvents` - a counter of the number of backpressure throttling events raised by
 *    the flow-control protocol.
 *  - `partialReadInvalidations` - a counter of the number of in-progress reads that were
 *    atomically invalidated because a producer failed before the shuffle completed.
 *
 * '''Metric-name contract.''' The four metric names above form a public, stable contract:
 * `StreamingShuffleSource` registers each object under exactly these names, and
 * `docs/monitoring.md` plus the external Grafana dashboard reference them as
 * `shuffle.streaming.<name>`. They must not be renamed without updating every consumer.
 *
 * '''Thread-safety and overhead.''' Increment and update calls originate concurrently from many
 * executor threads (map-side writers, the spill monitor, the backpressure daemon, and reduce-side
 * readers). Dropwizard [[Counter]] is internally backed by a `LongAdder`, and the utilization
 * value is an [[AtomicInteger]], so every mutation is a cheap lock-free atomic operation and every
 * read is a single atomic load. This keeps the telemetry overhead well under the 1% CPU budget
 * mandated for the streaming shuffle feature; no locks are taken on any hot path.
 *
 * Producers of these metrics:
 *  - `MemorySpillManager` calls [[incSpillCount]] and [[updateBufferUtilization]].
 *  - `BackpressureProtocol` calls [[incBackpressureEvents]].
 *  - `StreamingShuffleReader` calls [[incPartialReadInvalidations]].
 */
@Since("4.2.0")
private[spark] class StreamingShuffleMetrics {

  /**
   * Backing state for the `bufferUtilizationPercent` gauge. Holds the most recently observed
   * buffer utilization as an integer percentage in the inclusive range [0, 100]. Updated by
   * `MemorySpillManager` and read by the gauge registered in `StreamingShuffleSource`.
   */
  private val bufferUtilization = new AtomicInteger(0)

  /** Backs the `spillCount` metric: the total number of buffered partitions spilled to disk. */
  private val spillCount = new Counter()

  /** Backs the `backpressureEvents` metric: the total number of throttling events raised. */
  private val backpressureEvents = new Counter()

  /** Backs the `partialReadInvalidations` metric: the total number of reads invalidated. */
  private val partialReadInvalidations = new Counter()

  /**
   * Returns the live [[Counter]] backing the `spillCount` metric so that `StreamingShuffleSource`
   * can register this exact instance with its `MetricRegistry`.
   */
  def spillCounter: Counter = spillCount

  /**
   * Returns the live [[Counter]] backing the `backpressureEvents` metric so that
   * `StreamingShuffleSource` can register this exact instance with its `MetricRegistry`.
   */
  def backpressureCounter: Counter = backpressureEvents

  /**
   * Returns the live [[Counter]] backing the `partialReadInvalidations` metric so that
   * `StreamingShuffleSource` can register this exact instance with its `MetricRegistry`.
   */
  def partialReadInvalidationsCounter: Counter = partialReadInvalidations

  /**
   * Returns the current buffer utilization as an integer percentage in [0, 100]. The gauge
   * registered by `StreamingShuffleSource` for `bufferUtilizationPercent` delegates to this.
   */
  def currentBufferUtilization: Int = bufferUtilization.get()

  /**
   * Records the latest observed buffer utilization for the `bufferUtilizationPercent` gauge.
   * Values are clamped to the inclusive range [0, 100] so the exported gauge can never report an
   * out-of-range percentage, even if a caller passes a transient over- or under-shoot. Safe to
   * call concurrently; it performs a single atomic store.
   *
   * @param percent the observed utilization percentage; clamped into [0, 100]
   */
  def updateBufferUtilization(percent: Int): Unit = {
    val clamped = if (percent < 0) 0 else if (percent > 100) 100 else percent
    bufferUtilization.set(clamped)
  }

  /** Increments the `spillCount` counter by one. Safe to call from multiple threads. */
  def incSpillCount(): Unit = spillCount.inc()

  /**
   * Increments the `spillCount` counter by the given amount. Safe to call from multiple threads.
   *
   * @param n the number of spills to add to the counter
   */
  def incSpillCount(n: Long): Unit = spillCount.inc(n)

  /** Increments the `backpressureEvents` counter by one. Safe to call from multiple threads. */
  def incBackpressureEvents(): Unit = backpressureEvents.inc()

  /**
   * Increments the `partialReadInvalidations` counter by one. Safe to call from multiple threads.
   */
  def incPartialReadInvalidations(): Unit = partialReadInvalidations.inc()
}
