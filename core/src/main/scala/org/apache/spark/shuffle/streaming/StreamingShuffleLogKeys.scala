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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.LogKey

/**
 * Streaming-shuffle-specific structured-logging [[LogKey]]s and correlation-id value formatters.
 *
 * The streaming shuffle backend tags its structured logs with exactly four MDC correlation-id keys
 * so operators can join a single shuffle's log lines across the producer (map), spill, backpressure
 * and consumer (reduce) executor boundaries: `shuffle_id`, `map_id`, `reduce_partition_range`, and
 * `attempt_id`. The first two are served by the shared `org.apache.spark.internal.LogKeys` enum
 * (`SHUFFLE_ID` and `MAP_ID` already lowercase to the required names). The last two have no
 * equivalent in that enum -- the closest shared keys, `REDUCE_ID` and `TASK_ATTEMPT_ID`, lowercase
 * to `reduce_id` / `task_attempt_id`, which do not match the mandated schema.
 *
 * '''Why these keys live here (coexistence / isolation strategy).''' Rather than mutate the shared
 * `common/utils-java` `LogKeys` enum -- a cross-cutting change used by every Spark module -- the
 * two streaming-only keys are defined locally, keeping all streaming logic inside the
 * `org.apache.spark.shuffle.streaming` package with zero cross-contamination into shared code.
 * This uses Spark's documented custom-`LogKey` extension point (see the
 * `org.apache.spark.internal.Logging` Scaladoc, which shows implementing [[LogKey]] directly): the
 * structured-logging machinery renders an MDC field name as `key.name().toLowerCase(Locale.ROOT)`,
 * so the `UPPER_SNAKE_CASE` constant names below surface as the required `reduce_partition_range`
 * and `attempt_id`.
 *
 * '''Value convention.''' `reduce_partition_range` always carries a half-open reduce-partition
 * range `[startInclusive, endExclusive)`. On the reduce-side reader this is the actual contiguous
 * range of reduce partitions the task consumes; at single-partition producer, spill, and
 * backpressure sites it is the degenerate one-partition range built by [[singlePartition]].
 * Keeping the value a range at every site makes the key name honest and uniform across the
 * subsystem.
 */
@Since("4.2.0")
private[spark] object StreamingShuffleLogKeys {

  /**
   * MDC key that renders as `reduce_partition_range`. Carries a half-open reduce-partition range
   * `[start, end)` (see [[range]] / [[singlePartition]]).
   */
  case object REDUCE_PARTITION_RANGE extends LogKey {
    override def name(): String = "REDUCE_PARTITION_RANGE"
  }

  /** MDC key that renders as `attempt_id`. Carries the task attempt id. */
  case object ATTEMPT_ID extends LogKey {
    override def name(): String = "ATTEMPT_ID"
  }

  /**
   * Formats a contiguous half-open reduce-partition range for the `reduce_partition_range` MDC key.
   *
   * @param startInclusive the first reduce partition consumed (inclusive)
   * @param endExclusive    one past the last reduce partition consumed (exclusive)
   * @return the range rendered as `"[startInclusive, endExclusive)"`
   */
  def range(startInclusive: Int, endExclusive: Int): String = s"[$startInclusive, $endExclusive)"

  /**
   * Formats the degenerate one-partition range for a site that pertains to a single reduce
   * partition. The upper bound is computed in `Long` space so it is well-defined even for the
   * maximum partition id.
   *
   * @param partitionId the single reduce partition the log site pertains to
   * @return the range rendered as `"[partitionId, partitionId + 1)"`
   */
  def singlePartition(partitionId: Int): String = s"[$partitionId, ${partitionId.toLong + 1L})"
}
