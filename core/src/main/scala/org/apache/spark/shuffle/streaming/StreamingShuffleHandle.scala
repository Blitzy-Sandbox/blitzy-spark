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
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * streaming shuffle path for reduced latency.
 *
 * This handle is created by [[StreamingShuffleManager.registerShuffle]] when streaming
 * shuffle mode is enabled and conditions for streaming are met. The streaming path
 * eliminates shuffle materialization latency by streaming data directly from producers
 * to consumers with memory buffering and backpressure protocols.
 *
 * The streaming shuffle targets:
 *  - 30-50% end-to-end latency reduction for shuffle-heavy workloads (10GB+ data, 100+ partitions)
 *  - 5-10% improvement for CPU-bound workloads through reduced scheduler overhead
 *  - Zero performance regression for memory-bound workloads through automatic fallback
 *
 * Coexistence Strategy:
 * This handle coexists with existing shuffle handles (SerializedShuffleHandle,
 * BypassMergeSortShuffleHandle, BaseShuffleHandle). StreamingShuffleManager will fallback
 * to SortShuffleManager (returning BaseShuffleHandle) when streaming conditions are not met,
 * including:
 *  - Memory pressure prevents buffer allocation
 *  - Consumer is 2x slower than producer for >60 seconds
 *  - Network saturation exceeds 90% link capacity
 *  - Producer/consumer protocol version mismatch
 *
 * This class is intentionally minimal - it serves as a marker to distinguish the streaming
 * shuffle path from the traditional sort-based shuffle path. The Spark shuffle framework
 * uses type matching on the handle to select appropriate writer/reader implementations.
 *
 * @param shuffleId The unique identifier for this shuffle operation
 * @param dependency The shuffle dependency containing partitioner and serializer information
 * @tparam K The key type for shuffle records
 * @tparam V The value type for shuffle records
 * @tparam C The combiner output type (may be same as V if no combiner)
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, dependency)
