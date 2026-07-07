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
import org.apache.spark.annotation.Since
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * A [[org.apache.spark.shuffle.BaseShuffleHandle]] subtype that marks a shuffle chosen to use the
 * streaming shuffle path rather than the default sort-based path.
 *
 * An instance of this handle is returned by `StreamingShuffleManager.registerShuffle` when a
 * shuffle is eligible for streaming -- that is, the streaming manager was selected via
 * `spark.shuffle.manager=streaming`, the feature was opted in via
 * `spark.shuffle.streaming.enabled=true`, and no fallback condition applies. The manager then
 * pattern-matches on this concrete type in `getWriter` and `getReader` to dispatch to the
 * streaming components; every other handle type is delegated to the inner `SortShuffleManager`,
 * which remains the production-stable fallback. This single dispatch point keeps the streaming
 * logic isolated from the existing sort code paths.
 *
 * The handle also carries the per-shuffle resource envelope, resolved by `StreamingShuffleConfig`
 * from the `spark.shuffle.streaming.*` keys at registration time, so that every writer, reader,
 * and buffer created for this shuffle observes one consistent set of limits. Because a shuffle
 * handle is serialized and shipped to tasks, all fields added here are serializable primitives
 * (three `Int` values).
 *
 * @tparam K the type of the keys being shuffled
 * @tparam V the type of the values being shuffled
 * @tparam C the type of the combined values if map-side aggregation is used (else same as V)
 * @param shuffleId the unique id of this shuffle, passed through to the base handle
 * @param dependency the shuffle dependency; exposed by the base handle, not redeclared here
 * @param bufferSizePercent percent of executor memory (1-50) for this shuffle's stream buffers
 * @param spillThreshold buffer-utilization percent (50-95) at which partitions spill to disk
 * @param maxBandwidthMBps per-executor streaming rate limit in MB/s (0 means unlimited)
 */
@Since("4.2.0")
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val bufferSizePercent: Int,
    val spillThreshold: Int,
    val maxBandwidthMBps: Int)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {
}
