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

import org.apache.spark.internal.LogKey

/**
 * Streaming-shuffle-local structured-logging keys.
 *
 * Spark's structured logging derives each MDC field name from `LogKey.name().toLowerCase(ROOT)`
 * (see `org.apache.spark.internal.Logging`), so a key named `ATTEMPT_ID` surfaces as the MDC
 * dimension `attempt_id`. The streaming-shuffle observability contract requires the exact
 * correlation dimensions `shuffle_id`, `map_id`, `reduce_partition_range`, and `attempt_id`.
 * `shuffle_id` and `map_id` already exist as canonical keys in `org.apache.spark.internal.LogKeys`
 * (reused directly), but `attempt_id` and `reduce_partition_range` do not -- and `LogKeys.java`
 * is outside this feature's modification scope. Spark explicitly supports defining custom
 * `LogKey`s outside that registry (documented on the `Logging` trait), so the two missing
 * dimensions are declared here, isolated within the streaming package, leaving the shared key
 * registry untouched.
 *
 * These are deliberately distinct from the canonical `LogKeys.TASK_ATTEMPT_ID`
 * (`task_attempt_id`), `LogKeys.REDUCE_ID` (`reduce_id`), and `LogKeys.PARTITION_ID`
 * (`partition_id`): the observability rule mandates the precise downstream field names
 * `attempt_id` and `reduce_partition_range`, which only these keys emit.
 */
private[streaming] object StreamingShuffleLogKeys {

  /**
   * Emits the MDC dimension `attempt_id` carrying the reduce/map task-attempt id, the
   * correlation handle that ties a streaming-shuffle log line to a specific task attempt.
   */
  case object ATTEMPT_ID extends LogKey {
    override def name: String = "ATTEMPT_ID"
  }

  /**
   * Emits the MDC dimension `reduce_partition_range` carrying the half-open reduce-partition
   * range a reader covers, formatted `[start,end)`; it identifies which slice of the shuffle a
   * streaming-shuffle read log line concerns.
   */
  case object REDUCE_PARTITION_RANGE extends LogKey {
    override def name: String = "REDUCE_PARTITION_RANGE"
  }
}
