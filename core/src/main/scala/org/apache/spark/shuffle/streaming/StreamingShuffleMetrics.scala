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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.util.LongAccumulator

/**
 * Companion object containing metric name constants for streaming shuffle metrics.
 * Metrics follow the spark.shuffle.streaming.* naming convention for JMX and Spark UI integration.
 */
private[spark] object StreamingShuffleMetrics {
  
  /** Prefix for all streaming shuffle metrics */
  val METRIC_PREFIX: String = "spark.shuffle.streaming"
  
  /** Metric name for buffer bytes currently held in memory */
  val STREAMING_BUFFER_BYTES: String = s"$METRIC_PREFIX.bufferBytes"
  
  /** Metric name for backpressure events count */
  val BACKPRESSURE_EVENTS: String = s"$METRIC_PREFIX.backpressureEvents"
  
  /** Metric name for partial reads from incomplete shuffle data */
  val PARTIAL_READS: String = s"$METRIC_PREFIX.partialReads"
  
  /** Metric name for block retransmissions due to corruption or failure */
  val RETRANSMISSIONS: String = s"$METRIC_PREFIX.retransmissions"
  
  /** Metric name for bytes written to streaming shuffle */
  val BYTES_WRITTEN: String = s"$METRIC_PREFIX.bytesWritten"
  
  /** Metric name for records written to streaming shuffle */
  val RECORDS_WRITTEN: String = s"$METRIC_PREFIX.recordsWritten"
  
  /** Metric name for write time spent on streaming shuffle */
  val WRITE_TIME: String = s"$METRIC_PREFIX.writeTime"
  
  /** Metric name for remote blocks fetched */
  val REMOTE_BLOCKS_FETCHED: String = s"$METRIC_PREFIX.remoteBlocksFetched"
  
  /** Metric name for local blocks fetched */
  val LOCAL_BLOCKS_FETCHED: String = s"$METRIC_PREFIX.localBlocksFetched"
  
  /** Metric name for remote bytes read */
  val REMOTE_BYTES_READ: String = s"$METRIC_PREFIX.remoteBytesRead"
  
  /** Metric name for records read */
  val RECORDS_READ: String = s"$METRIC_PREFIX.recordsRead"
}

/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing streaming shuffle data.
 * Extends ShuffleWriteMetricsReporter with streaming-specific counters for buffer bytes
 * and backpressure events.
 *
 * Operations are not thread-safe - implementations assume single-threaded access
 * following the same pattern as standard Spark shuffle metrics.
 */
@DeveloperApi
class StreamingShuffleWriteMetrics private[spark] () 
    extends ShuffleWriteMetricsReporter with Serializable {
  
  // Base shuffle write metrics using LongAccumulator for efficient tracking
  private[streaming] val _bytesWritten = new LongAccumulator
  private[streaming] val _recordsWritten = new LongAccumulator
  private[streaming] val _writeTime = new LongAccumulator
  
  // Streaming-specific metrics
  private[streaming] val _streamingBufferBytes = new LongAccumulator
  private[streaming] val _backpressureEvents = new LongAccumulator
  
  /**
   * Number of bytes written to the streaming shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum
  
  /**
   * Total number of records written to the streaming shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum
  
  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum
  
  /**
   * Current number of bytes held in streaming shuffle buffers.
   * This is a point-in-time metric that can increase or decrease as data is
   * buffered and then acknowledged by consumers.
   */
  def streamingBufferBytes: Long = _streamingBufferBytes.sum
  
  /**
   * Number of backpressure events sent to consumers.
   * Indicates how often the writer had to signal consumers to slow down.
   */
  def backpressureEvents: Long = _backpressureEvents.sum
  
  // Base ShuffleWriteMetricsReporter interface implementations
  
  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
  
  // Streaming-specific ShuffleWriteMetricsReporter implementations
  
  /**
   * Increment the count of bytes currently held in streaming shuffle buffers.
   * Called when data is buffered before streaming to consumers.
   */
  private[spark] override def incStreamingBufferBytes(v: Long): Unit = {
    _streamingBufferBytes.add(v)
  }
  
  /**
   * Decrement the count of bytes currently held in streaming shuffle buffers.
   * Called when consumers acknowledge receipt and buffer memory is reclaimed.
   */
  private[spark] override def decStreamingBufferBytes(v: Long): Unit = {
    _streamingBufferBytes.setValue(streamingBufferBytes - v)
  }
  
  /**
   * Increment the count of backpressure events sent to consumers.
   * Called when the writer signals consumers to slow down due to buffer pressure.
   */
  private[spark] override def incBackpressureEvents(v: Long): Unit = {
    _backpressureEvents.add(v)
  }
  
  // Helper methods for metrics management
  
  /**
   * Sets the bytes written value directly. Useful for correcting metrics after errors.
   */
  private[spark] def setBytesWritten(v: Long): Unit = _bytesWritten.setValue(v)
  
  /**
   * Sets the records written value directly. Useful for correcting metrics after errors.
   */
  private[spark] def setRecordsWritten(v: Long): Unit = _recordsWritten.setValue(v)
  
  /**
   * Sets the write time value directly.
   */
  private[spark] def setWriteTime(v: Long): Unit = _writeTime.setValue(v)
  
  /**
   * Sets the streaming buffer bytes value directly.
   */
  private[spark] def setStreamingBufferBytes(v: Long): Unit = _streamingBufferBytes.setValue(v)
  
  /**
   * Sets the backpressure events value directly.
   */
  private[spark] def setBackpressureEvents(v: Long): Unit = _backpressureEvents.setValue(v)
}

/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading streaming shuffle data.
 * Extends ShuffleReadMetricsReporter with streaming-specific counters for buffer bytes,
 * backpressure events, partial reads, and retransmissions.
 *
 * Operations are not thread-safe - implementations assume single-threaded access
 * following the same pattern as standard Spark shuffle metrics.
 */
@DeveloperApi
class StreamingShuffleReadMetrics private[spark] () extends Serializable {
  
  // Base shuffle read metrics using LongAccumulator for efficient tracking
  private[streaming] val _remoteBlocksFetched = new LongAccumulator
  private[streaming] val _localBlocksFetched = new LongAccumulator
  private[streaming] val _remoteBytesRead = new LongAccumulator
  private[streaming] val _remoteBytesReadToDisk = new LongAccumulator
  private[streaming] val _localBytesRead = new LongAccumulator
  private[streaming] val _fetchWaitTime = new LongAccumulator
  private[streaming] val _recordsRead = new LongAccumulator
  private[streaming] val _corruptMergedBlockChunks = new LongAccumulator
  private[streaming] val _mergedFetchFallbackCount = new LongAccumulator
  private[streaming] val _remoteMergedBlocksFetched = new LongAccumulator
  private[streaming] val _localMergedBlocksFetched = new LongAccumulator
  private[streaming] val _remoteMergedChunksFetched = new LongAccumulator
  private[streaming] val _localMergedChunksFetched = new LongAccumulator
  private[streaming] val _remoteMergedBytesRead = new LongAccumulator
  private[streaming] val _localMergedBytesRead = new LongAccumulator
  private[streaming] val _remoteReqsDuration = new LongAccumulator
  private[streaming] val _remoteMergedReqsDuration = new LongAccumulator
  
  // Streaming-specific metrics
  private[streaming] val _streamingBufferBytes = new LongAccumulator
  private[streaming] val _backpressureEvents = new LongAccumulator
  private[streaming] val _partialReads = new LongAccumulator
  private[streaming] val _retransmissions = new LongAccumulator
  
  // Base metric accessors
  
  /** Number of remote blocks fetched in this streaming shuffle by this task. */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.sum
  
  /** Number of local blocks fetched in this streaming shuffle by this task. */
  def localBlocksFetched: Long = _localBlocksFetched.sum
  
  /** Total number of remote bytes read from the streaming shuffle by this task. */
  def remoteBytesRead: Long = _remoteBytesRead.sum
  
  /** Total number of remote bytes read to disk from the streaming shuffle by this task. */
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk.sum
  
  /** Streaming shuffle data that was read from the local disk. */
  def localBytesRead: Long = _localBytesRead.sum
  
  /** Time the task spent waiting for remote streaming shuffle blocks. */
  def fetchWaitTime: Long = _fetchWaitTime.sum
  
  /** Total number of records read from the streaming shuffle by this task. */
  def recordsRead: Long = _recordsRead.sum
  
  /** Total bytes fetched in the streaming shuffle by this task. */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead
  
  /** Number of blocks fetched in this streaming shuffle by this task. */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched
  
  /** Number of corrupt merged streaming shuffle block chunks encountered. */
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks.sum
  
  /** Number of times the task had to fallback to fetch original blocks for a merged chunk. */
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount.sum
  
  /** Number of remote merged blocks fetched. */
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched.sum
  
  /** Number of local merged blocks fetched. */
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched.sum
  
  /** Number of remote merged chunks fetched. */
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched.sum
  
  /** Number of local merged chunks fetched. */
  def localMergedChunksFetched: Long = _localMergedChunksFetched.sum
  
  /** Total number of remote merged bytes read. */
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead.sum
  
  /** Total number of local merged bytes read. */
  def localMergedBytesRead: Long = _localMergedBytesRead.sum
  
  /** Total time taken for remote requests to complete. */
  def remoteReqsDuration: Long = _remoteReqsDuration.sum
  
  /** Total time taken for remote merged requests. */
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration.sum
  
  // Streaming-specific metric accessors
  
  /**
   * Current number of bytes held in streaming shuffle read buffers.
   * This is a point-in-time metric tracking buffer memory usage on the read side.
   */
  def streamingBufferBytes: Long = _streamingBufferBytes.sum
  
  /**
   * Number of backpressure events received from producers.
   * Indicates how often the reader was signaled to slow down consumption.
   */
  def backpressureEvents: Long = _backpressureEvents.sum
  
  /**
   * Number of partial reads from incomplete streaming shuffle data.
   * Indicates reads that occurred before the shuffle was fully complete.
   */
  def partialReads: Long = _partialReads.sum
  
  /**
   * Number of block retransmissions due to corruption or failure.
   * Indicates data integrity issues or network problems during streaming.
   */
  def retransmissions: Long = _retransmissions.sum
  
  // Base ShuffleReadMetricsReporter-style increment methods
  
  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks.add(v)
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount.add(v)
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched.add(v)
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched.add(v)
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched.add(v)
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched.add(v)
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit = _remoteMergedBytesRead.add(v)
  private[spark] def incLocalMergedBytesRead(v: Long): Unit = _localMergedBytesRead.add(v)
  private[spark] def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.add(v)
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration.add(v)
  
  // Streaming-specific increment methods
  
  /**
   * Increment the count of bytes currently held in streaming shuffle read buffers.
   */
  private[spark] def incStreamingBufferBytes(v: Long): Unit = {
    _streamingBufferBytes.add(v)
  }
  
  /**
   * Decrement the count of bytes currently held in streaming shuffle read buffers.
   */
  private[spark] def decStreamingBufferBytes(v: Long): Unit = {
    _streamingBufferBytes.setValue(streamingBufferBytes - v)
  }
  
  /**
   * Increment the count of backpressure events received from producers.
   */
  private[spark] def incBackpressureEvents(v: Long): Unit = {
    _backpressureEvents.add(v)
  }
  
  /**
   * Increment the count of partial reads from incomplete streaming shuffle data.
   */
  private[spark] def incPartialReads(v: Long): Unit = {
    _partialReads.add(v)
  }
  
  /**
   * Increment the count of block retransmissions due to corruption or failure.
   */
  private[spark] def incRetransmissions(v: Long): Unit = {
    _retransmissions.add(v)
  }
  
  /**
   * Resets the value of the current metrics and merges all the independent
   * [[TempStreamingShuffleReadMetrics]] into this.
   */
  private[spark] def setMergeValues(metrics: Seq[TempStreamingShuffleReadMetrics]): Unit = {
    // Reset all base metrics
    _remoteBlocksFetched.setValue(0)
    _localBlocksFetched.setValue(0)
    _remoteBytesRead.setValue(0)
    _remoteBytesReadToDisk.setValue(0)
    _localBytesRead.setValue(0)
    _fetchWaitTime.setValue(0)
    _recordsRead.setValue(0)
    _corruptMergedBlockChunks.setValue(0)
    _mergedFetchFallbackCount.setValue(0)
    _remoteMergedBlocksFetched.setValue(0)
    _localMergedBlocksFetched.setValue(0)
    _remoteMergedChunksFetched.setValue(0)
    _localMergedChunksFetched.setValue(0)
    _remoteMergedBytesRead.setValue(0)
    _localMergedBytesRead.setValue(0)
    _remoteReqsDuration.setValue(0)
    _remoteMergedReqsDuration.setValue(0)
    
    // Reset streaming-specific metrics
    _streamingBufferBytes.setValue(0)
    _backpressureEvents.setValue(0)
    _partialReads.setValue(0)
    _retransmissions.setValue(0)
    
    // Merge all temporary metrics
    metrics.foreach { metric =>
      _remoteBlocksFetched.add(metric.remoteBlocksFetched)
      _localBlocksFetched.add(metric.localBlocksFetched)
      _remoteBytesRead.add(metric.remoteBytesRead)
      _remoteBytesReadToDisk.add(metric.remoteBytesReadToDisk)
      _localBytesRead.add(metric.localBytesRead)
      _fetchWaitTime.add(metric.fetchWaitTime)
      _recordsRead.add(metric.recordsRead)
      _corruptMergedBlockChunks.add(metric.corruptMergedBlockChunks)
      _mergedFetchFallbackCount.add(metric.mergedFetchFallbackCount)
      _remoteMergedBlocksFetched.add(metric.remoteMergedBlocksFetched)
      _localMergedBlocksFetched.add(metric.localMergedBlocksFetched)
      _remoteMergedChunksFetched.add(metric.remoteMergedChunksFetched)
      _localMergedChunksFetched.add(metric.localMergedChunksFetched)
      _remoteMergedBytesRead.add(metric.remoteMergedBytesRead)
      _localMergedBytesRead.add(metric.localMergedBytesRead)
      _remoteReqsDuration.add(metric.remoteReqsDuration)
      _remoteMergedReqsDuration.add(metric.remoteMergedReqsDuration)
      
      // Merge streaming-specific metrics
      _streamingBufferBytes.add(metric.streamingBufferBytes)
      _backpressureEvents.add(metric.backpressureEvents)
      _partialReads.add(metric.partialReads)
      _retransmissions.add(metric.retransmissions)
    }
  }
}

/**
 * A temporary streaming shuffle read metrics holder that is used to collect
 * streaming shuffle read metrics for each shuffle dependency. All temporary
 * metrics will be merged into [[StreamingShuffleReadMetrics]] at last.
 *
 * This class implements ShuffleReadMetricsReporter so it can be passed to
 * shuffle read operations that expect the standard reporter interface while
 * also tracking streaming-specific metrics.
 */
private[spark] class TempStreamingShuffleReadMetrics extends ShuffleReadMetricsReporter {
  
  // Base shuffle read metrics using simple vars for single-threaded access
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _localBlocksFetched = 0L
  private[this] var _remoteBytesRead = 0L
  private[this] var _remoteBytesReadToDisk = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L
  private[this] var _corruptMergedBlockChunks = 0L
  private[this] var _mergedFetchFallbackCount = 0L
  private[this] var _remoteMergedBlocksFetched = 0L
  private[this] var _localMergedBlocksFetched = 0L
  private[this] var _remoteMergedChunksFetched = 0L
  private[this] var _localMergedChunksFetched = 0L
  private[this] var _remoteMergedBytesRead = 0L
  private[this] var _localMergedBytesRead = 0L
  private[this] var _remoteReqsDuration = 0L
  private[this] var _remoteMergedReqsDuration = 0L
  
  // Streaming-specific metrics using simple vars
  private[this] var _streamingBufferBytes = 0L
  private[this] var _backpressureEvents = 0L
  private[this] var _partialReads = 0L
  private[this] var _retransmissions = 0L
  
  // Base ShuffleReadMetricsReporter implementations
  
  override def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
  override def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched += v
  override def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  override def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk += v
  override def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  override def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  override def incRecordsRead(v: Long): Unit = _recordsRead += v
  override def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks += v
  override def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount += v
  override def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched += v
  override def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched += v
  override def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched += v
  override def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched += v
  override def incRemoteMergedBytesRead(v: Long): Unit = _remoteMergedBytesRead += v
  override def incLocalMergedBytesRead(v: Long): Unit = _localMergedBytesRead += v
  override def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration += v
  override def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration += v
  
  // Streaming-specific ShuffleReadMetricsReporter implementations
  
  /**
   * Increment the count of bytes currently held in streaming shuffle buffers.
   */
  override def incStreamingBufferBytes(v: Long): Unit = _streamingBufferBytes += v
  
  /**
   * Decrement the count of bytes currently held in streaming shuffle buffers.
   */
  override def decStreamingBufferBytes(v: Long): Unit = _streamingBufferBytes -= v
  
  /**
   * Increment the count of backpressure events received from producers.
   */
  override def incBackpressureEvents(v: Long): Unit = _backpressureEvents += v
  
  /**
   * Increment the count of partial reads from incomplete streaming shuffle data.
   */
  override def incPartialReads(v: Long): Unit = _partialReads += v
  
  /**
   * Increment the count of block retransmissions due to corruption or failure.
   */
  override def incRetransmissions(v: Long): Unit = _retransmissions += v
  
  // Accessors for merging into StreamingShuffleReadMetrics
  
  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def localBlocksFetched: Long = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched
  def localMergedChunksFetched: Long = _localMergedChunksFetched
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead
  def localMergedBytesRead: Long = _localMergedBytesRead
  def remoteReqsDuration: Long = _remoteReqsDuration
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration
  
  // Streaming-specific accessors
  def streamingBufferBytes: Long = _streamingBufferBytes
  def backpressureEvents: Long = _backpressureEvents
  def partialReads: Long = _partialReads
  def retransmissions: Long = _retransmissions
}
