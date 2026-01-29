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
 * A ShuffleHandle for streaming shuffle operations.
 *
 * This handle extends [[BaseShuffleHandle]] and serves as a marker type for pattern matching
 * in [[StreamingShuffleManager.getWriter]] and [[StreamingShuffleManager.getReader]] methods.
 * It enables the streaming shuffle manager to distinguish streaming shuffle registrations from
 * sort-based shuffle handles and return the appropriate writer/reader implementations.
 *
 * The streaming shuffle mechanism allows shuffle data to be streamed directly from map (producer)
 * tasks to reduce (consumer) tasks with memory buffering and backpressure protocols, eliminating
 * the need to fully materialize shuffle output to disk before consumers can read.
 *
 * This class is Serializable (inherited from [[org.apache.spark.shuffle.ShuffleHandle]]) to
 * support task shipping across executors.
 *
 * @param shuffleId the unique identifier for this shuffle operation
 * @param dependency the shuffle dependency containing serializer, aggregator, mapSideCombine,
 *                   and keyOrdering information needed for shuffle coordination
 * @tparam K the key type
 * @tparam V the value type
 * @tparam C the combiner type
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {

  // StreamingShuffleHandle is a marker type - no additional fields or methods are needed
  // beyond what BaseShuffleHandle provides. The following properties are accessible:
  //
  // - shuffleId: Int (from ShuffleHandle)
  // - dependency: ShuffleDependency[K, V, C] (from BaseShuffleHandle)
  //
  // Through dependency, the following are also accessible:
  // - dependency.serializer: Serializer
  // - dependency.aggregator: Option[Aggregator[K, V, C]]
  // - dependency.mapSideCombine: Boolean
  // - dependency.keyOrdering: Option[Ordering[K]]
}
