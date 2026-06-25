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

package org.apache.spark.shuffle

/**
 * Provides the opt-in, pluggable streaming shuffle data path for Spark Core.
 *
 * The streaming shuffle subsystem streams shuffle data directly from producer (map) tasks to
 * consumer (reduce) tasks through bounded in-memory buffers governed by a backpressure protocol,
 * eliminating the write-to-disk-then-fetch materialization barrier of sort-based shuffle.
 *
 * It is selected via `spark.shuffle.manager=streaming` and engages only when
 * `spark.shuffle.streaming.enabled=true`; otherwise it transparently delegates to the built-in
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]]. The subsystem coexists with the
 * sort-based shuffle (never replacing it) and automatically falls back to it on degradation
 * conditions.
 *
 * Key components:
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]]:
 *    SPI entry point and dispatch.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleWriter]]:
 *    producer-side buffered writer.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleReader]]:
 *    consumer-side streaming reader.
 *  - [[org.apache.spark.shuffle.streaming.BackpressureProtocol]]:
 *    token-bucket and heartbeat flow control.
 *  - [[org.apache.spark.shuffle.streaming.MemorySpillManager]]:
 *    utilization-driven disk spill.
 */
package object streaming
