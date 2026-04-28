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
 * Streaming-shuffle subpackage providing an opt-in alternative to
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] that pipelines map-side data directly
 * to reduce-side consumers with in-memory buffering, backpressure control, and graceful
 * disk-spill fallback.
 *
 * == Activation ==
 * Selected only when the user explicitly opts in via:
 *   - `spark.shuffle.manager=streaming` (short name registered in
 *     [[org.apache.spark.shuffle.ShuffleManager]] companion's `shortShuffleMgrNames` map), or
 *   - `spark.shuffle.manager=org.apache.spark.shuffle.streaming.StreamingShuffleManager`
 *     (FQCN fallback).
 *
 * The default `spark.shuffle.manager=sort` continues to use the production-stable
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] unchanged.
 *
 * == Components ==
 *   - `StreamingShuffleManager` - SPI implementation; owns lifecycle of the streaming
 *     components and a private `SortShuffleManager` for fallback delegation.
 *   - `StreamingShuffleHandle` - extends `BaseShuffleHandle` with streaming-specific
 *     configuration metadata.
 *   - `StreamingShuffleWriter` - map-side writer producing pipelined output blocks.
 *   - `StreamingShuffleReader` - reduce-side reader consuming in-progress blocks.
 *   - `BackpressureProtocol` - heartbeat-based flow control with token-bucket rate limiting.
 *   - `MemorySpillManager` - memory polling and disk-spill fallback.
 *   - `StreamingShuffleFallbackPolicy` - decision class for streaming-vs-sort fallback.
 *   - `StreamingShuffleMetrics` - Dropwizard `MetricSet` exposing observability counters.
 *   - `StreamingShuffleSource` - Spark `Source` registering metrics with `MetricsSystem`.
 *
 * == Coexistence ==
 * All code in this subpackage is fully isolated from the existing sort-shuffle implementation.
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* The single integration touchpoint
 * is the addition of a `"streaming"` -> `StreamingShuffleManager` entry in the existing
 * [[org.apache.spark.shuffle.ShuffleManager]] companion's dispatch map.
 *
 * == Shared Constants ==
 * The package object below holds package-private constants used by multiple components in
 * this subpackage. These constants are visibility-restricted via `private[streaming]` so they
 * are accessible to all sibling files in `org.apache.spark.shuffle.streaming` but invisible
 * outside the subpackage, preserving the "zero cross-contamination" boundary.
 */
package object streaming {

  /**
   * Producer-failure detection timeout in milliseconds.
   *
   * Used by `BackpressureProtocol` to scan for stale producer heartbeats and by
   * `StreamingShuffleReader` to time out producer connections before throwing
   * [[org.apache.spark.shuffle.FetchFailedException]] and triggering DAG-scheduler upstream
   * recomputation. The 5-second value is the contractual producer-failure detection window
   * specified for the streaming-shuffle feature.
   */
  private[streaming] val PRODUCER_TIMEOUT_MILLIS: Long = 5000L

  /**
   * Consumer-liveness heartbeat timeout in milliseconds.
   *
   * Used by `BackpressureProtocol` to scan for stale consumer acknowledgments and by
   * `StreamingShuffleWriter` to detect stalled consumers and trigger spill. The 10-second
   * value is the contractual consumer-liveness heartbeat interval specified for the
   * streaming-shuffle feature.
   */
  private[streaming] val CONSUMER_TIMEOUT_MILLIS: Long = 10000L

  /**
   * Pipelined block size in bytes (2 MB).
   *
   * Used by `StreamingShuffleWriter` as the per-flush boundary and as the floor for the
   * per-partition memory cap when buffer percentage divided by partition count produces a
   * smaller value. The 2 MB value reflects the "block size limited to 2MB for pipelining
   * efficiency" directive specified for the streaming-shuffle feature.
   *
   * Declared as `Int` because many JDK APIs accepting buffer sizes (`ByteArrayOutputStream`,
   * `ByteBuffer.allocate`) require `Int`, and 2 MB fits comfortably within the `Int` range.
   */
  private[streaming] val BLOCK_SIZE_BYTES: Int = 2 * 1024 * 1024

  /**
   * Initial capacity (in bytes) of each per-partition `ByteArrayOutputStream` constructed by
   * `StreamingShuffleWriter`.
   *
   * Per AAP Section 0.7.2.2, all streaming-shuffle buffer allocations must participate in
   * unified-memory accounting via
   * [[org.apache.spark.memory.TaskMemoryManager#acquireExecutionMemory]] before being
   * allocated. Pre-sizing each per-partition `ByteArrayOutputStream` to the full 2 MB block
   * size at construction time would allocate `BLOCK_SIZE_BYTES * numPartitions` bytes on the
   * JVM heap *before* `acquireExecutionMemory` is called, bypassing the unified-memory model
   * and risking executor OOM for high-partition-count shuffles (e.g. 200 partitions =
   * 400 MB, 1 000 partitions = 2 GB upfront).
   *
   * To honor unified-memory accounting, the writer constructs each `ByteArrayOutputStream`
   * with this small initial capacity (1 KB) and relies on `ByteArrayOutputStream`'s native
   * `Arrays.copyOf` doubling-growth to expand each buffer up to the per-partition cap as
   * records actually arrive. Aggregate growth is bounded by the `acquireExecutionMemory`
   * grant (`perPartitionBufferCap * numPartitions`) so the unified-memory ceiling is
   * respected throughout the writer's lifetime.
   *
   * The 1 KB value is small enough to make construction-time allocation negligible (e.g.
   * 200 partitions = 200 KB upfront) yet large enough to absorb a typical first-record
   * write without an immediate `Arrays.copyOf` reallocation.
   */
  private[streaming] val INITIAL_BAOS_CAPACITY: Int = 1024

  /**
   * Memory-utilization polling cadence in milliseconds.
   *
   * Used by `MemorySpillManager` for the daemon scheduler tick interval. The 100 ms value
   * reflects the "polling the existing MemoryManager at 100 ms intervals" specification and
   * the "buffer reclamation within 100 ms of consumer acknowledgment" requirement for the
   * streaming-shuffle feature.
   */
  private[streaming] val SPILL_POLL_INTERVAL_MILLIS: Long = 100L

  /**
   * Block-integrity checksum algorithm name.
   *
   * Used in log messages from `StreamingShuffleWriter` and `StreamingShuffleReader` to
   * identify the algorithm in operator-readable diagnostics. The actual implementation uses
   * `java.util.zip.CRC32C` (JDK 17 stdlib). Per the directive *"Checksum algorithm: CRC32C
   * only"*, this constant must remain the literal string `"CRC32C"` and no alternative
   * algorithm (MD5, SHA-1, SHA-256, xxHash) is permitted for the streaming-shuffle feature.
   */
  private[streaming] val CHECKSUM_ALGORITHM: String = "CRC32C"

}
