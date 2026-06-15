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
 * A [[BaseShuffleHandle]] subtype produced by the streaming shuffle backend.
 *
 * In addition to the base shuffle id and dependency, this handle carries the three
 * streaming tuning values that are resolved once at registration time. Shipping them on
 * the handle lets the streaming writer and reader receive their configuration directly
 * rather than re-reading the [[org.apache.spark.SparkConf]] on every executor, which keeps
 * the streaming configuration immutable for the lifetime of the application (there is no
 * dynamic reconfiguration in v1; an executor restart is required to change it).
 *
 * The handle is serialized and shipped to tasks, so it must stay serializable: it adds only
 * three primitive `Int` tuning values on top of the inherited fields and captures no
 * non-serializable references.
 *
 * This type coexists with the sort-based path: when the streaming backend falls back to the
 * inner `SortShuffleManager`, that manager produces its own `BaseShuffleHandle` instead and
 * this subtype is simply not used.
 *
 * @param shuffleId the shuffle id, forwarded to [[BaseShuffleHandle]]
 * @param dependency the shuffle dependency, forwarded to [[BaseShuffleHandle]]
 * @param bufferSizePercent percent of executor memory used to size per-partition buffers
 * @param spillThreshold buffer-utilization percent that triggers spilling the largest buffers
 * @param maxBandwidthMBps per-executor streaming bandwidth cap in MB/s (<= 0 means unlimited)
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val bufferSizePercent: Int,
    val spillThreshold: Int,
    val maxBandwidthMBps: Int)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {

  override def toString: String =
    s"StreamingShuffleHandle(shuffleId=$shuffleId, bufferSizePercent=$bufferSizePercent, " +
      s"spillThreshold=$spillThreshold, maxBandwidthMBps=$maxBandwidthMBps)"
}
