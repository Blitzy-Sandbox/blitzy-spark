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
 * A [[BaseShuffleHandle]] subtype that marks a shuffle for the streaming data path.
 *
 * This handle serves two purposes. First, it is the dispatch discriminator: the streaming
 * shuffle manager pattern-matches on its type to route a shuffle through the streaming
 * read/write path while delegating every other shuffle to the inner sort-based manager within a
 * single `registerShuffle` / `getWriter` / `getReader` flow. Second, it carries the per-shuffle
 * streaming tuning parameters captured at registration time, so the writer and reader can honor
 * them without re-reading the `SparkConf`.
 *
 * Like every shuffle handle, instances are serialized and shipped to tasks; all constructor
 * parameters (the inherited `shuffleId` and `dependency` plus the three `Int` tuning fields)
 * are therefore serializable.
 *
 * @param shuffleId the unique id of the shuffle, forwarded to [[BaseShuffleHandle]]
 * @param dependency the shuffle dependency for this shuffle, forwarded to [[BaseShuffleHandle]]
 * @param bufferSizePercent percentage of executor memory budgeted for streaming buffers
 * @param spillThreshold buffer-utilization percentage at which spilling to disk is triggered
 * @param maxBandwidthMBps per-executor streaming bandwidth cap in MB/s (0 means unlimited)
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val bufferSizePercent: Int,
    val spillThreshold: Int,
    val maxBandwidthMBps: Int)
  extends BaseShuffleHandle(shuffleId, dependency)
