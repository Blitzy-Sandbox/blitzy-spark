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

package org.apache.spark.shuffle.streaming;

import org.apache.spark.internal.LogKey;

/**
 * Streaming-shuffle-specific MDC (Mapped Diagnostic Context) correlation keys.
 *
 * <p>These keys are defined here, inside the isolated
 * {@code org.apache.spark.shuffle.streaming} package, rather than in the shared, frozen
 * {@code org.apache.spark.internal.LogKeys} registry. The cross-cutting observability rule
 * names four streaming correlation keys: {@code shuffle_id}, {@code map_id},
 * {@code reduce_partition_range}, and {@code attempt_id}. The first two already exist as
 * canonical {@code LogKeys} ({@code SHUFFLE_ID}, {@code MAP_ID}) and are reused unchanged; the
 * remaining two have no canonical equivalent, so they are provided here through Spark's
 * documented custom-{@code LogKey} extension mechanism (see
 * {@code org.apache.spark.internal.Logging}). Defining them locally keeps all streaming logic
 * inside the streaming package (zero cross-contamination) and leaves the shared
 * {@code LogKeys.java} untouched, while still emitting the exact key names the observability
 * contract requires.
 *
 * <p>As with every {@code LogKey}, the emitted MDC key string is {@code name()} lowercased by
 * the logging framework: {@code ATTEMPT_ID} renders as {@code attempt_id} and
 * {@code REDUCE_PARTITION_RANGE} renders as {@code reduce_partition_range}.
 */
public enum StreamingLogKeys implements LogKey {
  /** The task attempt (id) that produced or consumed the streaming shuffle output. */
  ATTEMPT_ID,

  /** The reduce-side partition range a streaming read/transfer covers, e.g. {@code [0,8)}. */
  REDUCE_PARTITION_RANGE
}
