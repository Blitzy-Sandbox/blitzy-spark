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
 * The streaming shuffle backend: an opt-in, fully isolated implementation of the
 * [[org.apache.spark.shuffle.ShuffleManager]] service-provider interface (SPI).
 *
 * Rather than fully materializing map-side output to local disk before any fetch can
 * begin, the streaming backend pipelines intermediate data directly from producer
 * (map-side) executors to consumer (reduce-side) executors through bounded in-memory
 * buffers and the existing `org.apache.spark.network` transport layer. A consumer-to-
 * producer backpressure protocol throttles producers so consumers are never overwhelmed,
 * and buffered partitions spill to disk through the existing `BlockManager` when memory
 * pressure rises. The objective is to eliminate shuffle-materialization latency for
 * shuffle-heavy workloads while guaranteeing zero data loss under failure.
 *
 * ==Coexistence and fallback==
 *
 * This backend coexists with, and never modifies, the sort-based
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]], which remains its automatic
 * fallback. Whenever streaming is unsuitable (for example, a sustained slow consumer,
 * memory pressure that risks an OOM, network saturation, or a producer/consumer version
 * mismatch), the manager transparently delegates to a lazily-instantiated inner
 * sort-based manager, so memory-bound or otherwise unsuitable workloads incur zero
 * regression relative to the default path.
 *
 * Engaging the streaming path is strictly opt-in and requires BOTH of the following
 * configuration signals; either one alone leaves the cluster on the sort-based path:
 *
 *  - `spark.shuffle.manager=streaming` selects this manager through the factory alias.
 *  - `spark.shuffle.streaming.enabled=true` arms the streaming feature flag.
 *
 * Both settings default to off, so the default behavior of every existing Spark
 * deployment is byte-for-byte unchanged.
 *
 * ==Package component catalog==
 *
 * The subsystem is organized by concern:
 *
 *  - '''Shuffle SPI core''': `StreamingShuffleManager`, `StreamingShuffleHandle`,
 *    `StreamingShuffleWriter`, `StreamingShuffleReader`, and `StreamingShuffleBlockResolver`
 *    implement the manager, handle, writer, reader, and block-resolver contracts of the SPI.
 *  - '''Buffering and memory''': `StreamingBuffer` holds per-partition bytes with CRC32C and
 *    LRU access tracking, while `MemorySpillManager` spills the largest buffers to disk under
 *    memory pressure.
 *  - '''Backpressure and flow control''': `BackpressureProtocol`, the executor-only
 *    `BackpressureRpcEndpoint`, `network.TokenBucketRateLimiter`, and
 *    `StreamingShuffleFallbackPolicy` regulate producer/consumer flow and gate fallback.
 *  - '''Network wire''': `network.StreamingShuffleTransport` and
 *    `network.StreamingBlockEnvelope` frame and verify blocks on the wire.
 *  - '''Observability and configuration''': `StreamingShuffleMetrics`,
 *    `StreamingShuffleSource`, and `StreamingShuffleConfig` expose telemetry and typed,
 *    validated configuration accessors.
 *
 * The nested `network` subpackage holds the on-the-wire block envelope, the v1 logging-only
 * transport integration layer (the real data plane reuses the existing `BlockTransferService`),
 * and the token-bucket rate limiter.
 *
 * ==Operational invariants==
 *
 * The implementation upholds these high-level invariants: CRC32C block-level checksums; a
 * 2 MB block size; a 5 s connection timeout; a 10 s heartbeat interval; an 80% buffer-
 * utilization spill threshold honoring a 100 ms reclamation SLA; and token-bucket rate
 * limiting for per-executor bandwidth caps. Configuration is immutable for the application
 * lifetime in v1, so changing it requires an executor restart.
 */
package object streaming {
}
