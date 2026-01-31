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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}

/**
 * Metrics source for streaming shuffle telemetry.
 *
 * Provides Dropwizard-based metrics integrated with Spark's MetricsSystem including:
 * - `bufferUtilizationPercent`: Current buffer memory usage as percentage
 * - `spillCount`: Total number of disk spill events when memory threshold exceeded
 * - `backpressureEvents`: Total flow control signals sent to producers
 * - `partialReadInvalidations`: Total partial reads invalidated due to producer failure
 *
 * == Integration ==
 *
 * This source integrates with Spark's MetricsSystem for JMX and REST API exposure.
 * Register via `SparkEnv.get.metricsSystem.registerSource(metricsSource)`.
 *
 * == Performance ==
 *
 * Designed to add <1% CPU overhead by using atomic primitives for lock-free updates
 * and minimizing method call overhead.
 *
 * == Thread Safety ==
 *
 * All operations are thread-safe via atomic primitives.
 */
private[spark] class StreamingShuffleMetricsSource extends Source {

  override val sourceName: String = "StreamingShuffle"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // Atomic counters for thread-safe updates
  private val bufferUtilization = new AtomicInteger(0)
  private val spillCounter = new AtomicLong(0)
  private val backpressureCounter = new AtomicLong(0)
  private val partialReadInvalidationCounter = new AtomicLong(0)

  // Additional metrics for detailed monitoring
  private val bytesStreamedCounter = new AtomicLong(0)
  private val bytesSpilledCounter = new AtomicLong(0)
  private val activeConsumersGauge = new AtomicInteger(0)
  private val fallbackCounter = new AtomicLong(0)

  // Register gauges with Dropwizard MetricRegistry
  metricRegistry.register(MetricRegistry.name("bufferUtilizationPercent"),
    new Gauge[Int] {
      override def getValue: Int = bufferUtilization.get()
    })

  metricRegistry.register(MetricRegistry.name("spillCount"),
    new Gauge[Long] {
      override def getValue: Long = spillCounter.get()
    })

  metricRegistry.register(MetricRegistry.name("backpressureEvents"),
    new Gauge[Long] {
      override def getValue: Long = backpressureCounter.get()
    })

  metricRegistry.register(MetricRegistry.name("partialReadInvalidations"),
    new Gauge[Long] {
      override def getValue: Long = partialReadInvalidationCounter.get()
    })

  metricRegistry.register(MetricRegistry.name("bytesStreamed"),
    new Gauge[Long] {
      override def getValue: Long = bytesStreamedCounter.get()
    })

  metricRegistry.register(MetricRegistry.name("bytesSpilled"),
    new Gauge[Long] {
      override def getValue: Long = bytesSpilledCounter.get()
    })

  metricRegistry.register(MetricRegistry.name("activeConsumers"),
    new Gauge[Int] {
      override def getValue: Int = activeConsumersGauge.get()
    })

  metricRegistry.register(MetricRegistry.name("fallbackCount"),
    new Gauge[Long] {
      override def getValue: Long = fallbackCounter.get()
    })

  // ========================================================================================
  // Update Methods (Thread-Safe)
  // ========================================================================================

  /**
   * Update the current buffer utilization percentage.
   *
   * @param percent utilization percentage (0-100)
   */
  def updateBufferUtilization(percent: Int): Unit = {
    bufferUtilization.set(percent)
  }

  /**
   * Increment the spill count.
   */
  def incSpillCount(): Unit = {
    spillCounter.incrementAndGet()
  }

  /**
   * Increment the spill count by a specific amount.
   *
   * @param count number of spills to add
   */
  def incSpillCount(count: Long): Unit = {
    spillCounter.addAndGet(count)
  }

  /**
   * Increment the backpressure events counter.
   */
  def incBackpressureEvents(): Unit = {
    backpressureCounter.incrementAndGet()
  }

  /**
   * Increment the backpressure events counter by a specific amount.
   *
   * @param count number of events to add
   */
  def incBackpressureEvents(count: Long): Unit = {
    backpressureCounter.addAndGet(count)
  }

  /**
   * Increment the partial read invalidations counter.
   */
  def incPartialReadInvalidations(): Unit = {
    partialReadInvalidationCounter.incrementAndGet()
  }

  /**
   * Increment the partial read invalidations counter by a specific amount.
   *
   * @param count number of invalidations to add
   */
  def incPartialReadInvalidations(count: Long): Unit = {
    partialReadInvalidationCounter.addAndGet(count)
  }

  /**
   * Add bytes streamed to the counter.
   *
   * @param bytes number of bytes streamed
   */
  def addBytesStreamed(bytes: Long): Unit = {
    bytesStreamedCounter.addAndGet(bytes)
  }

  /**
   * Add bytes spilled to the counter.
   *
   * @param bytes number of bytes spilled
   */
  def addBytesSpilled(bytes: Long): Unit = {
    bytesSpilledCounter.addAndGet(bytes)
  }

  /**
   * Update the active consumers count.
   *
   * @param count number of active consumers
   */
  def setActiveConsumers(count: Int): Unit = {
    activeConsumersGauge.set(count)
  }

  /**
   * Increment the fallback counter.
   */
  def incFallbackCount(): Unit = {
    fallbackCounter.incrementAndGet()
  }

  // ========================================================================================
  // Snapshot Methods (For Testing/Debugging)
  // ========================================================================================

  /** Get current buffer utilization. */
  def getBufferUtilization: Int = bufferUtilization.get()

  /** Get total spill count. */
  def getSpillCount: Long = spillCounter.get()

  /** Get total backpressure events. */
  def getBackpressureEvents: Long = backpressureCounter.get()

  /** Get total partial read invalidations. */
  def getPartialReadInvalidations: Long = partialReadInvalidationCounter.get()

  /** Get total bytes streamed. */
  def getBytesStreamed: Long = bytesStreamedCounter.get()

  /** Get total bytes spilled. */
  def getBytesSpilled: Long = bytesSpilledCounter.get()

  /** Get active consumer count. */
  def getActiveConsumers: Int = activeConsumersGauge.get()

  /** Get total fallback count. */
  def getFallbackCount: Long = fallbackCounter.get()

  /**
   * Reset all metrics (for testing purposes).
   */
  private[streaming] def reset(): Unit = {
    bufferUtilization.set(0)
    spillCounter.set(0)
    backpressureCounter.set(0)
    partialReadInvalidationCounter.set(0)
    bytesStreamedCounter.set(0)
    bytesSpilledCounter.set(0)
    activeConsumersGauge.set(0)
    fallbackCounter.set(0)
  }
}

/**
 * Wrapper for ShuffleWriteMetricsReporter that adds streaming-specific metrics.
 *
 * Delegates standard metrics to the underlying reporter while tracking streaming-specific
 * metrics via the provided metrics source.
 *
 * @param underlying    the underlying ShuffleWriteMetricsReporter to delegate to
 * @param metricsSource the streaming shuffle metrics source for streaming-specific metrics
 */
private[spark] class StreamingShuffleWriteMetrics(
    underlying: ShuffleWriteMetricsReporter,
    metricsSource: StreamingShuffleMetricsSource)
  extends ShuffleWriteMetricsReporter {

  // Delegate standard metrics to underlying
  override def incBytesWritten(v: Long): Unit = {
    underlying.incBytesWritten(v)
    metricsSource.addBytesStreamed(v)
  }

  override def incRecordsWritten(v: Long): Unit = underlying.incRecordsWritten(v)
  override def incWriteTime(v: Long): Unit = underlying.incWriteTime(v)
  override def decBytesWritten(v: Long): Unit = underlying.decBytesWritten(v)
  override def decRecordsWritten(v: Long): Unit = underlying.decRecordsWritten(v)

  // Streaming-specific metrics
  override def incStreamingSpillCount(v: Long): Unit = {
    underlying.incStreamingSpillCount(v)
    metricsSource.incSpillCount(v)
  }

  override def incStreamingBackpressureEvents(v: Long): Unit = {
    underlying.incStreamingBackpressureEvents(v)
    metricsSource.incBackpressureEvents(v)
  }
}

/**
 * Wrapper for ShuffleReadMetricsReporter that adds streaming-specific metrics.
 *
 * Delegates standard metrics to the underlying reporter while tracking streaming-specific
 * metrics via the provided metrics source.
 *
 * @param underlying    the underlying ShuffleReadMetricsReporter to delegate to
 * @param metricsSource the streaming shuffle metrics source for streaming-specific metrics
 */
private[spark] class StreamingShuffleReadMetrics(
    underlying: ShuffleReadMetricsReporter,
    metricsSource: StreamingShuffleMetricsSource)
  extends ShuffleReadMetricsReporter {

  // Delegate standard metrics to underlying
  override def incRemoteBlocksFetched(v: Long): Unit = underlying.incRemoteBlocksFetched(v)
  override def incLocalBlocksFetched(v: Long): Unit = underlying.incLocalBlocksFetched(v)
  override def incRemoteBytesRead(v: Long): Unit = underlying.incRemoteBytesRead(v)
  override def incRemoteBytesReadToDisk(v: Long): Unit = underlying.incRemoteBytesReadToDisk(v)
  override def incLocalBytesRead(v: Long): Unit = underlying.incLocalBytesRead(v)
  override def incFetchWaitTime(v: Long): Unit = underlying.incFetchWaitTime(v)
  override def incRecordsRead(v: Long): Unit = underlying.incRecordsRead(v)
  override def incCorruptMergedBlockChunks(v: Long): Unit =
    underlying.incCorruptMergedBlockChunks(v)
  override def incMergedFetchFallbackCount(v: Long): Unit =
    underlying.incMergedFetchFallbackCount(v)
  override def incRemoteMergedBlocksFetched(v: Long): Unit =
    underlying.incRemoteMergedBlocksFetched(v)
  override def incLocalMergedBlocksFetched(v: Long): Unit =
    underlying.incLocalMergedBlocksFetched(v)
  override def incRemoteMergedChunksFetched(v: Long): Unit =
    underlying.incRemoteMergedChunksFetched(v)
  override def incLocalMergedChunksFetched(v: Long): Unit =
    underlying.incLocalMergedChunksFetched(v)
  override def incRemoteMergedBytesRead(v: Long): Unit = underlying.incRemoteMergedBytesRead(v)
  override def incLocalMergedBytesRead(v: Long): Unit = underlying.incLocalMergedBytesRead(v)
  override def incRemoteReqsDuration(v: Long): Unit = underlying.incRemoteReqsDuration(v)
  override def incRemoteMergedReqsDuration(v: Long): Unit =
    underlying.incRemoteMergedReqsDuration(v)

  // Streaming-specific metrics
  override def incStreamingBufferUtilization(v: Long): Unit = {
    underlying.incStreamingBufferUtilization(v)
    metricsSource.updateBufferUtilization(v.toInt)
  }

  override def incStreamingPartialReadInvalidations(v: Long): Unit = {
    underlying.incStreamingPartialReadInvalidations(v)
    metricsSource.incPartialReadInvalidations(v)
  }
}
