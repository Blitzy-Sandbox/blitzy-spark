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
 * A [[BaseShuffleHandle]] specialization returned by `StreamingShuffleManager.registerShuffle`
 * when the streaming shuffle backend is engaged for a shuffle.
 *
 * In addition to the shuffle id and [[org.apache.spark.ShuffleDependency]] captured by
 * [[BaseShuffleHandle]], this handle carries the three streaming tuning values that are resolved
 * once on the driver at registration time. Shipping the tuning on the handle rather than
 * re-reading the configuration on every executor keeps the streaming configuration immutable for
 * the lifetime of the application (no dynamic reconfiguration in v1) and lets the streaming writer
 * and reader obtain their tuning without consulting `SparkConf` again.
 *
 * The handle is serialized and shipped with every task, so it intentionally adds only the
 * trivially-serializable `Int` tuning values on top of the dependency already captured by the
 * superclass; no additional non-serializable references are introduced here.
 *
 * @tparam K the type of the keys being shuffled
 * @tparam V the type of the values being shuffled
 * @tparam C the type of the combined values produced when map-side combine is used
 * @param shuffleId the unique id of the shuffle, forwarded to [[BaseShuffleHandle]]
 * @param dependency the shuffle dependency for this shuffle, forwarded to [[BaseShuffleHandle]]
 * @param bufferSizePercent percent of executor memory budgeted for per-partition streaming buffers
 * @param spillThreshold buffer-utilization percentage that triggers a spill to disk
 * @param maxBandwidthMBps per-executor bandwidth cap in MB/s; a non-positive value means unlimited
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
