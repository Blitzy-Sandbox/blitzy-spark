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
 * Shuffle handle for streaming-shuffle, extending [[BaseShuffleHandle]] with
 * streaming-specific configuration metadata captured at `registerShuffle` time on the
 * driver and conveyed to executors over the standard driver-executor RPC boundary.
 *
 * The handle is `Serializable` (inherited from
 * [[org.apache.spark.shuffle.ShuffleHandle]]) and carries:
 *   - `bufferSizePercent` -- percent of executor execution memory dedicated to streaming
 *     buffers (configurable 1-50% via `spark.shuffle.streaming.bufferSizePercent`,
 *     default 20).
 *   - `spillThreshold` -- buffer-utilization percentage at which spill is triggered
 *     (configurable 50-95% via `spark.shuffle.streaming.spillThreshold`, default 80).
 *   - `maxBandwidthMBps` -- per-executor bandwidth cap for streaming-shuffle traffic
 *     (configurable via `spark.shuffle.streaming.maxBandwidthMBps`, default -1 =
 *     unlimited).
 *
 * == Coexistence Strategy ==
 * This handle type is constructed exclusively by
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleManager.registerShuffle]] when
 * streaming-shuffle is opted in (via `spark.shuffle.manager=streaming` or
 * `spark.shuffle.streaming.enabled=true`). Other handle types
 * (`BaseShuffleHandle`, `SerializedShuffleHandle`, `BypassMergeSortShuffleHandle`)
 * continue to drive the existing sort-based shuffle path through
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] without modification, preserving
 * production stability for the default code path.
 *
 * Because [[BaseShuffleHandle]] itself is `private[spark]`, this subclass is also kept
 * `private[spark]` to honor the same encapsulation boundary and to avoid expanding the
 * public binary surface tracked by MiMa.
 *
 * The new fields are declared as `val` (read-only) so that the streaming writer and
 * reader factories can read them on the hot path without method-call overhead, while
 * preserving immutability per the user directive that "configuration changes require
 * executor restart (no dynamic reconfiguration in v1)".
 *
 * @param shuffleId           shuffle identifier from
 *                            [[org.apache.spark.scheduler.DAGScheduler]]
 * @param dependency          shuffle dependency carrying serializer, partitioner,
 *                            aggregator, and other shuffle metadata
 * @param bufferSizePercent   buffer-size percent of executor execution memory (1-50);
 *                            controls aggregate streaming-buffer footprint
 * @param spillThreshold      spill-trigger threshold percentage (50-95); when buffer
 *                            utilization reaches this value the
 *                            [[StreamingShuffleManager]]'s `MemorySpillManager`
 *                            persists the largest partitions to disk
 * @param maxBandwidthMBps    bandwidth cap in MB/s (-1 = unlimited); enforced by the
 *                            `BackpressureProtocol` token-bucket rate limiter
 * @tparam K key type produced by the upstream stage
 * @tparam V value type produced by the upstream stage
 * @tparam C combined value type after map-side aggregation (equals V if no aggregator)
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val bufferSizePercent: Int,
    val spillThreshold: Int,
    val maxBandwidthMBps: Int)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {

  /**
   * Returns an operator-friendly string representation including all streaming-specific
   * configuration fields. The default `BaseShuffleHandle` does not override `toString`,
   * so without this override the streaming handle would print as
   * `org.apache.spark.shuffle.streaming.StreamingShuffleHandle@<hashcode>`, which is
   * not useful for log-based debugging in production deployments.
   *
   * The returned string is intended to be safe for INFO-level logging -- it contains
   * only configuration scalars and the shuffle ID (no record data, no PII).
   */
  override def toString: String =
    s"StreamingShuffleHandle(shuffleId=$shuffleId, " +
      s"bufferSizePercent=$bufferSizePercent, " +
      s"spillThreshold=$spillThreshold, " +
      s"maxBandwidthMBps=$maxBandwidthMBps)"
}
