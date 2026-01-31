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

package org.apache.spark.shuffle

/**
 * An interface for reporting shuffle read metrics, for each shuffle. This interface assumes
 * all the methods are called on a single-threaded, i.e. concrete implementations would not need
 * to synchronize.
 *
 * All methods have additional Spark visibility modifier to allow public, concrete implementations
 * that still have these methods marked as private[spark].
 */
private[spark] trait ShuffleReadMetricsReporter {
  private[spark] def incRemoteBlocksFetched(v: Long): Unit
  private[spark] def incLocalBlocksFetched(v: Long): Unit
  private[spark] def incRemoteBytesRead(v: Long): Unit
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit
  private[spark] def incLocalBytesRead(v: Long): Unit
  private[spark] def incFetchWaitTime(v: Long): Unit
  private[spark] def incRecordsRead(v: Long): Unit
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit
  private[spark] def incLocalMergedBytesRead(v: Long): Unit
  private[spark] def incRemoteReqsDuration(v: Long): Unit
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit

  // ========================================================================
  // Streaming shuffle metrics - only active when spark.shuffle.manager=streaming
  // These methods integrate with StreamingShuffleReader in the streaming subpackage.
  // Coexists with existing sort-based shuffle which does not use these methods.
  // ========================================================================

  /**
   * Reports buffer memory usage percentage for streaming shuffle reads.
   * Called by StreamingShuffleReader to track memory utilization during
   * in-progress block fetching. Higher values may indicate backpressure
   * is needed or spill threshold approaching.
   *
   * This metric is useful for monitoring memory pressure during streaming
   * shuffle operations and diagnosing performance bottlenecks.
   *
   * @param v buffer utilization value (percentage scaled to Long, e.g., 75 = 75%)
   */
  private[spark] def incStreamingBufferUtilization(v: Long): Unit

  /**
   * Counts invalidated partial reads from failed producers in streaming shuffle.
   * Incremented when a producer crashes during shuffle write and the reader
   * must invalidate partial data received from that producer. Used for
   * monitoring fault tolerance and recovery overhead.
   *
   * High values may indicate cluster instability or frequent executor failures
   * requiring investigation. Each invalidation triggers upstream recomputation
   * via the DAG scheduler's lineage-based recovery mechanism.
   *
   * @param v number of partial read invalidations to add (typically 1 per event)
   */
  private[spark] def incStreamingPartialReadInvalidations(v: Long): Unit
}


/**
 * An interface for reporting shuffle write metrics. This interface assumes all the methods are
 * called on a single-threaded, i.e. concrete implementations would not need to synchronize.
 *
 * All methods have additional Spark visibility modifier to allow public, concrete implementations
 * that still have these methods marked as private[spark].
 */
private[spark] trait ShuffleWriteMetricsReporter {
  private[spark] def incBytesWritten(v: Long): Unit
  private[spark] def incRecordsWritten(v: Long): Unit
  private[spark] def incWriteTime(v: Long): Unit
  private[spark] def decBytesWritten(v: Long): Unit
  private[spark] def decRecordsWritten(v: Long): Unit

  // ========================================================================
  // Streaming shuffle metrics - only active when spark.shuffle.manager=streaming
  // These methods integrate with StreamingShuffleWriter in the streaming subpackage.
  // Coexists with existing sort-based shuffle which does not use these methods.
  // ========================================================================

  /**
   * Counts disk spill events when memory threshold exceeded during streaming shuffle write.
   * Incremented when MemorySpillManager triggers automatic disk spill due to buffer
   * utilization exceeding the configured threshold (default 80%).
   *
   * Frequent spills may indicate insufficient executor memory for the workload
   * or that the buffer size percentage (spark.shuffle.streaming.bufferSizePercent)
   * should be adjusted. Each spill involves coordination with DiskBlockManager
   * for persistence and subsequent retrieval.
   *
   * @param v number of spill events to add (typically 1 per spill operation)
   */
  private[spark] def incStreamingSpillCount(v: Long): Unit

  /**
   * Counts backpressure signals sent to producers in streaming shuffle.
   * Incremented when BackpressureProtocol determines that consumers cannot
   * keep up with producer rate and sends throttling signals.
   *
   * High values indicate that consumers are processing slower than producers
   * are generating data. Sustained backpressure (consumer 2x slower for >60s)
   * may trigger automatic fallback to sort-based shuffle. Useful for
   * diagnosing flow control issues and tuning buffer/bandwidth settings.
   *
   * @param v number of backpressure events to add (typically 1 per signal)
   */
  private[spark] def incStreamingBackpressureEvents(v: Long): Unit
}
