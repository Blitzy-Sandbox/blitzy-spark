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
 * Streaming Shuffle: an opt-in, pluggable shuffle backend that pipelines map-side output directly
 * to reduce-side consumers through bounded in-memory buffers, eliminating the shuffle
 * materialization latency incurred by the default sort-based shuffle path.
 *
 * This package object is intentionally documentation-only: it carries the package-level Scaladoc
 * that makes the coexistence and isolation strategy discoverable at the package root. It declares
 * no types, holds no state, and adds no runtime dependency.
 *
 * ==Purpose==
 *
 * The streaming backend pipelines data from producer (map) executors to consumer (reduce)
 * executors through bounded in-memory buffers governed by a backpressure protocol. Buffered data
 * spills gracefully to disk under memory pressure, every block carries a CRC32C integrity
 * checksum for corruption detection, and any failure transparently degrades to the
 * production-stable sort-based shuffle so that correctness is never compromised. The goal is a
 * 30-50% end-to-end latency reduction for shuffle-heavy workloads with zero regression for
 * workloads that do not benefit from streaming.
 *
 * ==Dual Activation Gate==
 *
 * The streaming backend is active if and only if BOTH of the following are configured:
 *  - `spark.shuffle.manager=streaming` selects this backend through the factory short-name alias,
 *    and
 *  - `spark.shuffle.streaming.enabled=true` is the explicit opt-in feature flag.
 *
 * If either condition is not met, Spark uses the default sort-based shuffle. This
 * defense-in-depth gate reconciles the two configuration surfaces and prevents accidental
 * enablement. Configuration is immutable for the application lifetime; changes require an
 * executor restart (there is no dynamic reconfiguration in v1).
 *
 * ==Composition-Based Coexistence (Zero Regression)==
 *
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]] holds an inner
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] and delegates every non-streaming shuffle
 * handle and every fallback condition to it. The sort path remains the production-stable default
 * and is never modified, which guarantees that memory-bound and CPU-bound workloads observe no
 * performance regression. Fallback is decided by
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleFallbackPolicy]] and reverts to sort when
 * the consumer is sustained slower than the producer, when memory pressure risks an OOM, when the
 * network link saturates, or when a producer/consumer version mismatch is detected.
 *
 * ==Isolation (Zero Cross-Contamination)==
 *
 * All streaming production logic lives in this package and its
 * [[org.apache.spark.shuffle.streaming.network]] subpackage. The only modification to existing
 * shuffle code is a single short-name alias entry ("streaming") added to the factory map in
 * [[org.apache.spark.shuffle.ShuffleManager]]; no streaming logic is injected into any existing
 * shuffle code path, and the sort path is consumed unchanged by composition.
 *
 * ==Reuse of Existing SPIs (Least-Modification Principle)==
 *
 * The feature integrates exclusively through existing public Spark SPIs, redesigning no
 * subsystem:
 *  - [[org.apache.spark.shuffle.ShuffleManager]], [[org.apache.spark.shuffle.ShuffleWriter]], and
 *    [[org.apache.spark.shuffle.ShuffleReader]] provide the pluggable shuffle contract;
 *  - [[org.apache.spark.memory.MemoryConsumer]] provides execution-memory accounting for buffers;
 *  - [[org.apache.spark.storage.BlockManager]] and
 *    [[org.apache.spark.network.BlockTransferService]] provide block persistence and transfer;
 *  - [[org.apache.spark.MapOutputTracker]] resolves producer locations on the read path;
 *  - [[org.apache.spark.metrics.MetricsSystem]] and
 *    [[org.apache.spark.metrics.source.Source]] provide telemetry fan-out to all sinks;
 *  - [[org.apache.spark.rpc.RpcEndpoint]] provides the executor-only backpressure signaling; and
 *  - [[org.apache.spark.shuffle.MigratableResolver]] preserves decommission block migration.
 *
 * ==Component Overview==
 *
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]]: SPI entry point; dispatches
 *    streaming handles and delegates all fallback to the inner sort manager.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleHandle]]: shuffle handle carrying the
 *    per-shuffle streaming resource envelope (buffer, spill, and bandwidth settings).
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleWriter]]: map-side writer that buffers
 *    records per partition as a [[org.apache.spark.memory.MemoryConsumer]].
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleReader]]: reduce-side reader supporting
 *    in-progress reads and atomic partial-read invalidation.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleBlockResolver]]: in-memory and spilled
 *    block map that delegates migration to the sort resolver.
 *  - [[org.apache.spark.shuffle.streaming.StreamingBuffer]]: per-partition buffer with CRC32C and
 *    LRU access-time accounting.
 *  - [[org.apache.spark.shuffle.streaming.MemorySpillManager]]: utilization monitor that spills
 *    the largest and least-recently-used buffers to disk.
 *  - [[org.apache.spark.shuffle.streaming.BackpressureProtocol]]: token-bucket and heartbeat flow
 *    control with producer/consumer timeout detection.
 *  - [[org.apache.spark.shuffle.streaming.BackpressureRpcEndpoint]]: executor-only RPC endpoint
 *    that carries backpressure messages.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleFallbackPolicy]]: four-condition engine
 *    that decides when to revert to the sort path.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleMetrics]]: the streaming metric values
 *    (buffer utilization, spills, backpressure events, partial-read invalidations).
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleSource]]: metrics
 *    [[org.apache.spark.metrics.source.Source]] registered with the MetricsSystem.
 *  - [[org.apache.spark.shuffle.streaming.StreamingShuffleConfig]]: typed accessor for the
 *    `spark.shuffle.streaming.*` configuration keys.
 *
 * The [[org.apache.spark.shuffle.streaming.network]] subpackage provides the wire-level building
 * blocks:
 *  - [[org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter]]: a Guava RateLimiter
 *    wrapper enforcing the per-executor bandwidth cap.
 *  - [[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport]]: transport
 *    integration that reuses the executor [[org.apache.spark.network.BlockTransferService]].
 *  - [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]]: big-endian block
 *    header plus CRC32C payload framing.
 *
 * @since 4.2.0
 */
package object streaming {
}
