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
 * Opt-in, fully isolated streaming shuffle backend for Spark Core.
 *
 * This package implements the [[org.apache.spark.shuffle.ShuffleManager]] service-provider
 * interface (SPI) as a low-latency alternative to the sort-based shuffle. Rather than fully
 * materializing map output to local disk before any fetch begins, map-side output is buffered in
 * bounded, per-partition in-memory buffers and pipelined directly to reduce-side consumers over
 * the existing Spark network transport. A consumer-to-producer backpressure protocol throttles
 * producers so consumers are never overwhelmed, and buffers spill to disk under memory pressure
 * so the executor memory footprint stays bounded.
 *
 * ==Coexistence and automatic fallback==
 *
 * The streaming backend coexists with, and automatically falls back to, the unchanged sort-based
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]]. The sort-based manager is composed as a
 * lazily instantiated inner manager and is never bypassed when a fallback condition trips.
 * Engaging the streaming path requires BOTH of the following configuration signals, each of which
 * defaults to off:
 *
 *   - `spark.shuffle.manager=streaming` selects this manager through the factory alias.
 *   - `spark.shuffle.streaming.enabled=true` is the opt-in feature flag.
 *
 * Because both default to off, the default behavior of every existing Spark deployment is
 * byte-for-byte unchanged. When streaming is disabled, or when a fallback condition (a sustained
 * slow consumer, memory pressure, network saturation, or a producer/consumer version mismatch) is
 * detected, all shuffle work is delegated to the inner sort-based manager.
 *
 * ==Component catalog==
 *
 * The subsystem is organized by concern:
 *
 *   - Shuffle SPI core: `StreamingShuffleManager`, `StreamingShuffleHandle`,
 *     `StreamingShuffleWriter`, `StreamingShuffleReader`, and `StreamingShuffleBlockResolver`.
 *   - Buffering and memory: `StreamingBuffer` (a per-partition in-memory buffer) and
 *     `MemorySpillManager` (threshold-driven disk spill and reclamation).
 *   - Backpressure and flow control: `BackpressureProtocol`, the executor-only
 *     `BackpressureRpcEndpoint`, `network.TokenBucketRateLimiter`, and
 *     `StreamingShuffleFallbackPolicy`.
 *   - Network wire: `network.StreamingShuffleTransport` and `network.StreamingBlockEnvelope`.
 *   - Observability and configuration: `StreamingShuffleMetrics`, `StreamingShuffleSource` (an
 *     `org.apache.spark.metrics.source.Source`), and `StreamingShuffleConfig`.
 *
 * ==Operational invariants==
 *
 * The implementation upholds the following high-level invariants:
 *
 *   - CRC32C block-level checksums protect every transferred block.
 *   - A fixed 2 MB block size frames streamed and spilled data identically.
 *   - A 5 s connection timeout bounds producer-liveness detection.
 *   - A 10 s heartbeat interval drives the backpressure liveness protocol.
 *   - An 80% buffer-utilization spill threshold reclaims memory within a 100 ms SLA.
 *   - Token-bucket rate limiting enforces per-executor bandwidth caps.
 *
 * ==The `network` subpackage==
 *
 * The `network` subpackage holds the on-the-wire `StreamingBlockEnvelope` (a 32-byte big-endian
 * header plus a CRC32C-validated payload), the v1 logging-only `StreamingShuffleTransport`
 * integration layer (which reuses the existing block transfer service for the actual data plane),
 * and the `TokenBucketRateLimiter` used for per-executor bandwidth enforcement.
 */
package object streaming {}
