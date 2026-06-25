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

import java.util.concurrent.atomic.{AtomicInteger, LongAdder}

import org.apache.spark.internal.Logging

/**
 * Mutable, thread-safe holder for the four streaming shuffle metrics.
 *
 * This class is a pure state container for the streaming shuffle subsystem's telemetry. It owns
 * the mutable metric values (one gauge and three counters) and exposes ergonomic mutators and
 * readers so that collaborating components never have to manipulate the underlying atomics
 * directly.
 *
 * Registration with Spark's metrics infrastructure is intentionally NOT performed here: the
 * companion `StreamingShuffleSource` adapts the values held by this class into a Dropwizard
 * `MetricRegistry`, and the existing `MetricsSystem` then exposes them through the already
 * configured JMX, Prometheus, CSV, and SLF4J sinks. No new metrics endpoint is introduced by
 * the streaming shuffle subsystem.
 *
 * Once registered, the four metrics surface under the `shuffle.streaming.` namespace (the prefix
 * is applied by the source via `MetricRegistry.name(...)`):
 *  - `shuffle.streaming.bufferUtilizationPercent` (gauge, 0-100): current buffer fill level.
 *  - `shuffle.streaming.spillCount` (counter): number of disk spill events.
 *  - `shuffle.streaming.backpressureEvents` (counter): number of backpressure activations.
 *  - `shuffle.streaming.partialReadInvalidations` (counter): partial reads invalidated when a
 *    producer fails.
 *
 * Thread-safety: every metric is backed by a JDK atomic (`AtomicInteger` or `LongAdder`), so all
 * mutators and readers are safe to invoke concurrently from multiple executor threads without any
 * external synchronization. The gauge uses an `AtomicInteger` because it represents an absolute,
 * frequently overwritten value, whereas the counters use `LongAdder`, which offers higher
 * throughput than `AtomicLong` under the high contention typical of monotonically increasing
 * event tallies.
 */
private[spark] class StreamingShuffleMetrics extends Logging {

  /**
   * Gauge tracking the current buffer fill level as a percentage in the inclusive range [0, 100].
   * Surfaced as `shuffle.streaming.bufferUtilizationPercent`.
   */
  val bufferUtilizationPercent: AtomicInteger = new AtomicInteger(0)

  /**
   * Counter of disk spill events triggered when buffer utilization crosses the spill threshold.
   * Surfaced as `shuffle.streaming.spillCount`.
   */
  val spillCount: LongAdder = new LongAdder()

  /**
   * Counter of backpressure activations raised by the flow-control protocol.
   * Surfaced as `shuffle.streaming.backpressureEvents`.
   */
  val backpressureEvents: LongAdder = new LongAdder()

  /**
   * Counter of partial reads invalidated on producer failure (each invalidation defers to the
   * existing DAG-scheduler recomputation path). Surfaced as
   * `shuffle.streaming.partialReadInvalidations`.
   */
  val partialReadInvalidations: LongAdder = new LongAdder()

  /**
   * Sets the current buffer-utilization gauge, clamping the supplied value into [0, 100] so the
   * gauge never reports a nonsensical percentage regardless of the caller's input.
   *
   * @param pct the raw utilization percentage; values below 0 or above 100 are clamped.
   */
  def setBufferUtilizationPercent(pct: Int): Unit = {
    bufferUtilizationPercent.set(math.max(0, math.min(100, pct)))
  }

  /** Returns the current buffer-utilization percentage in the inclusive range [0, 100]. */
  def getBufferUtilizationPercent: Int = bufferUtilizationPercent.get()

  /** Records a single disk spill event. */
  def incrementSpillCount(): Unit = spillCount.increment()

  /** Returns the total number of disk spill events recorded so far. */
  def getSpillCount: Long = spillCount.sum()

  /** Records a single backpressure activation. */
  def incrementBackpressureEvents(): Unit = backpressureEvents.increment()

  /** Returns the total number of backpressure activations recorded so far. */
  def getBackpressureEvents: Long = backpressureEvents.sum()

  /** Records a single partial-read invalidation triggered by a producer failure. */
  def incrementPartialReadInvalidations(): Unit = partialReadInvalidations.increment()

  /** Returns the total number of partial-read invalidations recorded so far. */
  def getPartialReadInvalidations: Long = partialReadInvalidations.sum()
}
