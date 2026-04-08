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

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.ShuffleHandle

/**
 * A ShuffleHandle implementation for streaming shuffle mode.
 *
 * This handle is created by [[StreamingShuffleManager.registerShuffle()]] and is used to
 * identify shuffles that should use streaming semantics (direct streaming to consumers
 * rather than full materialization).
 *
 * The handle is serialized and sent to executors as part of the ShuffleMapTask, where
 * it is used to instantiate the appropriate [[StreamingShuffleWriter]] and
 * [[StreamingShuffleReader]].
 *
 * == Coexistence with Sort-Based Shuffle ==
 *
 * When fallback to sort-based shuffle is required (due to consumer lag, OOM risk,
 * or network saturation), [[StreamingShuffleManager]] returns a
 * [[org.apache.spark.shuffle.BaseShuffleHandle]] instead of this class, ensuring
 * graceful degradation to the sort-based shuffle path.
 *
 * == Type Parameters ==
 *
 * @tparam K the key type of shuffle records
 * @tparam V the value type of shuffle records
 * @tparam C the combined value type (for aggregation, may be same as V if no aggregation)
 *
 * @param shuffleId  unique identifier for this shuffle
 * @param dependency the ShuffleDependency containing partitioner, serializer, and aggregation info
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    val dependency: ShuffleDependency[K, V, C])
  extends ShuffleHandle(shuffleId) {

  /**
   * Returns true to indicate this shuffle should use streaming semantics.
   * Used by [[StreamingShuffleManager.getWriter()]] and [[StreamingShuffleManager.getReader()]]
   * for proper instantiation of streaming writer/reader.
   *
   * @return always true for StreamingShuffleHandle
   */
  def isStreaming: Boolean = true

  /**
   * Get the number of reduce partitions for this shuffle.
   * Convenience method to access partitioner information.
   *
   * @return the number of partitions from the dependency's partitioner
   */
  def numPartitions: Int = dependency.partitioner.numPartitions

  override def toString: String = {
    s"StreamingShuffleHandle(shuffleId=$shuffleId, numPartitions=$numPartitions)"
  }
}

/**
 * Companion object for StreamingShuffleHandle providing utility methods.
 */
private[spark] object StreamingShuffleHandle {

  /**
   * Utility method to check if a ShuffleHandle is a streaming handle.
   * Useful in pattern matching scenarios where type erasure may obscure the handle type.
   *
   * @param handle the ShuffleHandle to check
   * @return true if the handle is a StreamingShuffleHandle, false otherwise
   */
  def isStreamingHandle(handle: ShuffleHandle): Boolean = {
    handle.isInstanceOf[StreamingShuffleHandle[_, _, _]]
  }
}
