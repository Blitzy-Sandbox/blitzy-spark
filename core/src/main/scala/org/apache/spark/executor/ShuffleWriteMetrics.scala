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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  // Streaming shuffle metrics - only populated when spark.shuffle.manager=streaming
  private[executor] val _streamingSpillCount = new LongAccumulator
  private[executor] val _streamingBackpressureEvents = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum

  /**
   * Number of times streaming shuffle buffers were spilled to disk due to memory pressure.
   * Only populated when using streaming shuffle mode (spark.shuffle.manager=streaming).
   */
  def streamingSpillCount: Long = _streamingSpillCount.sum

  /**
   * Number of backpressure events received from consumers during streaming shuffle.
   * Only populated when using streaming shuffle mode (spark.shuffle.manager=streaming).
   */
  def streamingBackpressureEvents: Long = _streamingBackpressureEvents.sum

  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
  // Streaming shuffle metric incrementers
  private[spark] override def incStreamingSpillCount(v: Long): Unit = _streamingSpillCount.add(v)
  private[spark] override def incStreamingBackpressureEvents(v: Long): Unit =
    _streamingBackpressureEvents.add(v)
}
