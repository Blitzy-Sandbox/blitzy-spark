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
 * A marker [[org.apache.spark.shuffle.ShuffleHandle]] indicating that a shuffle has been
 * routed to the streaming path by `StreamingShuffleManager`. Carries no additional
 * state beyond the base fields so that it participates in binary compatibility
 * (MiMa) trivially &mdash; the class is in a new sub-package and is `private[spark]`.
 *
 * The inherited public fields are:
 *   - `shuffleId: Int` &mdash; inherited from
 *     [[org.apache.spark.shuffle.ShuffleHandle]] (declared as a `val` there).
 *   - `dependency: ShuffleDependency[K, V, V]` &mdash; inherited from
 *     [[org.apache.spark.shuffle.BaseShuffleHandle]] (declared as a `val` there).
 *
 * Coexistence strategy: the streaming path identifies its shuffles purely by
 * type-match on this class; any `BaseShuffleHandle` that is NOT a
 * `StreamingShuffleHandle` is dispatched by `StreamingShuffleManager` to its held
 * `SortShuffleManager` delegate (either because the four automatic fallback
 * conditions triggered at `registerShuffle` time, or because streaming shuffle was
 * never selected for that particular shuffle). This preserves zero-touch behavior
 * on the sort path and allows fine-grained per-shuffle fallback without disturbing
 * the DAG scheduler or the executor memory model.
 *
 * The third type parameter of the underlying `BaseShuffleHandle` collapses from
 * `C` to `V` because the streaming writer pipelines records directly to the
 * consumer without an intermediate combiner type &mdash; mirroring the
 * `BypassMergeSortShuffleHandle` and `SerializedShuffleHandle` marker subclasses
 * that the sort-based manager uses for its own dispatch decisions.
 *
 * @param shuffleId  ID of the shuffle
 * @param dependency the `ShuffleDependency` describing the upstream shuffle
 * @tparam K the key type of the shuffle
 * @tparam V the value type of the shuffle
 */
private[spark] class StreamingShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
