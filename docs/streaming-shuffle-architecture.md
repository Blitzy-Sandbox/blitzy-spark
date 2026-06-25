---
layout: global
title: "Streaming Shuffle Architecture"
displayTitle: "Streaming Shuffle Architecture"
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

Streaming Shuffle is an **opt-in, pluggable** shuffle subsystem that streams shuffle data
directly from producer (map) tasks to consumer (reduce) tasks through bounded in-memory buffers
governed by a backpressure protocol, eliminating the write-to-disk-then-fetch materialization
barrier of the default sort-based shuffle. Its goal is a **30–50% end-to-end latency
reduction** for shuffle-heavy workloads, with **zero regression** and **zero data loss**
guaranteed by automatic graceful degradation back to the sort-based shuffle. The feature is
delivered as a new `ShuffleManager` Service Provider Interface (SPI) implementation that
**coexists with — and never replaces —** the default `SortShuffleManager`. For
instructions on enabling it, see the [Streaming Shuffle Guide](streaming-shuffle-guide.html); for
tuning, see [Streaming Shuffle Tuning](streaming-shuffle-tuning.html); for day-to-day operations,
see [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html).

## Overview

In the default sort-based shuffle, each map task sorts and **materializes its output to local
disk**, and only then can reduce tasks fetch it over the network. This write-then-fetch sequence
is a hard materialization barrier: the entire map stage must finish writing before the reduce
stage can begin reading, which adds latency that dominates short, shuffle-heavy stages.

Streaming Shuffle removes that barrier. Instead of writing to disk and waiting, the producer
pipelines records through **per-partition in-memory buffers** toward the consumers as they are
produced, using **backpressure-based flow control** to keep fast producers from overwhelming
slower consumers. Data is spilled to disk **only under memory pressure** rather than on every
shuffle, so the common case never touches the disk-materialization path. By default, these buffers are capped at 20% of executor memory in aggregate and begin spilling to disk at 80% utilization.

The subsystem is strictly **opt-in** and is gated by a **dual-flag activation contract**. It
engages only when **both** of the following are set:

* `spark.shuffle.manager=streaming` — selects the streaming manager, and
* `spark.shuffle.streaming.enabled=true` — turns the streaming data path on (default `false`).

If either flag is unset, Spark behaves exactly as before: `spark.shuffle.manager` retains its
default of `sort`, and the sort-based shuffle remains the active path. A minimal opt-in looks like
this:

```bash
# Opt in to streaming shuffle. BOTH flags are required; the default is still "sort".
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  ...
```

The streaming-specific configuration keys are summarized below; full tuning guidance lives in the
[Streaming Shuffle Tuning](streaming-shuffle-tuning.html) page and the
[Spark Configuration — Shuffle Behavior](configuration.html#shuffle-behavior) reference.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Opt-in master switch for the streaming data path. Must be <code>true</code> <em>and</em>
    <code>spark.shuffle.manager</code> must be set to <code>streaming</code> for streaming shuffle
    to engage. When <code>false</code>, behavior is identical to the default sort-based shuffle.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Per-executor in-memory buffer budget expressed as a percentage of executor memory
    (valid range 1–50).
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer-utilization percentage at which the largest partitions are spilled to disk
    (valid range 50–95).
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0 (unlimited)</td>
  <td>
    Per-executor streaming rate limit in MB/s; <code>0</code> means unlimited. The effective rate
    is capped at 80% of this value to leave headroom for control traffic.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>
    Enables verbose streaming-shuffle debug logging.
  </td>
  <td>4.2.0</td>
</tr>
</table>

## SPI Coexistence

Spark selects a `ShuffleManager` implementation through the `spark.shuffle.manager` short-name
alias map. That map now resolves `"streaming"` to the streaming manager alongside the existing
`"sort"` and `"tungsten-sort"` aliases. The selected manager is **instantiated reflectively during
`SparkEnv` bootstrap**, so introducing the new alias requires **no scheduler or bootstrap changes**.

`StreamingShuffleManager` holds an inner `SortShuffleManager` **by composition** and delegates to
it for two purposes: as the automatic **fallback** path when streaming cannot proceed safely, and
for **block migration** during decommissioning. Both managers share the same
`IndexShuffleBlockResolver`, so migration and recovery behave identically regardless of which
manager is active.

The change is additive at the SPI boundary only. The following surfaces form a strict
**zero-modification boundary** and remain **unchanged**:

* user-facing RDD / DataFrame / Dataset APIs,
* the DAG scheduler,
* task scheduling and the task lifecycle,
* lineage tracking and fault recovery,
* `ShuffleExchangeExec`, and
* all Adaptive Query Execution (AQE) rules.

The *Streaming Shuffle SPI Coexistence* diagram below shows how the new manager plugs into the
unchanged dispatch boundary and coexists with the sort path.

```text
                          Streaming Shuffle SPI Coexistence

    +-----------------------------------------------------------------+
    |  User Code  (RDD / DataFrame / SQL)                  UNCHANGED  |
    +-----------------------------------------------------------------+
                                   |
                                   v
    +-----------------------------------------------------------------+
    |  ShuffleExchangeExec  +  Adaptive Query Execution    UNCHANGED  |
    +-----------------------------------------------------------------+
                                   |
                                   v
    +-----------------------------------------------------------------+
    |  SparkEnv bootstrap  ->  ShuffleManager.create (reflective)     |
    |                                                      UNCHANGED  |
    +-----------------------------------------------------------------+
                                   |
                                   v
    +-----------------------------------------------------------------+
    |  spark.shuffle.manager  short-name alias map                    |
    |  "sort"  /  "tungsten-sort"  /  "streaming" (NEW)               |
    +-----------------------------------------------------------------+
             |                                            |
     "sort" / "tungsten-sort"                     "streaming" (NEW)
             |                                            |
             v                                            v
    +------------------------+            +-------------------------------+
    |  SortShuffleManager    |  delegate  |  StreamingShuffleManager      |
    |  (default + fallback)  | <......... |  (NEW; holds an inner         |
    |                        |  fallback  |   SortShuffleManager)         |
    +------------------------+            +-------------------------------+
             |                                            |
             |  block index                               |  block migration
             v                                            v
    +-----------------------------------------------------------------+
    |  IndexShuffleBlockResolver  (shared by both managers)           |
    +-----------------------------------------------------------------+

Legend: solid arrows ( | v -> ) = active dispatch path;  dashed arrow ( <... ) = delegation / fallback.
```

## Core Components

The streaming subsystem is isolated in a dedicated package and is composed of the following
classes. Names are given exactly as they appear in the implementation.

| Component | Responsibility |
|-----------|----------------|
| `StreamingShuffleManager` | SPI entry point; performs dispatch and lifecycle management; composes an inner `SortShuffleManager` for delegation and fallback; registers metrics; performs an ordered shutdown (Backpressure → Spill → inner Sort). |
| `StreamingShuffleHandle` | A `BaseShuffleHandle` subtype that carries the tuning values (`bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps`); acts as the dispatch discriminator that distinguishes the streaming path from the sort path. |
| `StreamingShuffleWriter` | Producer-side writer (extends `MemoryConsumer`); maintains the per-partition in-memory buffers; pipelines data toward consumers; spills at the configured threshold; generates CRC32C checksums. |
| `StreamingShuffleReader` | Consumer-side reader; issues in-progress block requests; mirrors the existing `BlockStoreShuffleReader` read flow (honoring the aggregator, key ordering, and map-side combine); validates CRC32C; on producer failure invalidates partial reads and throws `FetchFailedException` to trigger recomputation. |
| `StreamingShuffleBlockResolver` | Maintains a 3-level block index; implements `MigratableResolver` by delegating to the shared `IndexShuffleBlockResolver`, preserving block-migration and decommission behavior. |
| `StreamingBuffer` | Per-partition buffer (a byte-array output stream) with CRC32C, LRU tracking, and atomic counters. |
| `BackpressureProtocol` | Consumer-to-producer token-bucket plus heartbeat flow control; performs a monotonic acknowledgment merge. |
| `BackpressureRpcEndpoint` | A `ThreadSafeRpcEndpoint` (executor-only; it refuses to register on the driver) that carries heartbeat, acknowledgment, rate, and timeout messages across executors. |
| `MemorySpillManager` | Polls buffer utilization every 100 ms; spills the largest partitions to disk (LRU) at the threshold via `BlockManager.putBytes(..., DISK_ONLY)`; reclaims buffers within 100 ms of acknowledgment; tracks metrics. |
| `TokenBucketRateLimiter` | Wraps the Guava `RateLimiter` (1 permit = 1 byte); caps throughput at 80% of link capacity. |
| `StreamingShuffleFallbackPolicy` | Evaluates the four automatic fallback conditions. |
| `StreamingShuffleMetrics` / `StreamingShuffleSource` | The four metrics under the `shuffle.streaming.` namespace, registered with the existing `MetricsSystem` through a `Source` named `streamingShuffle`. |
| `StreamingShuffleConfig` | Typed configuration accessors, range validation, and the effective-bandwidth (80% factor) computation. |
| `StreamingShuffleTransport` | A v1 **logging-only stub** that reuses the executor's existing `BlockTransferService`; the full Netty data-plane transport is deferred beyond v1. |
| `StreamingBlockEnvelope` | The self-describing wire envelope: a 32-byte big-endian header plus a payload ≤ 2 MiB, protected by CRC32C. |

## Producer-to-Consumer Data Flow

At run time, a map task hands its records to the `StreamingShuffleWriter`, which appends them to
the appropriate per-partition `StreamingBuffer`. As long as buffer utilization stays below the
spill threshold (80%), each buffered block (up to 2 MB) is pipelined toward the consumer through
the executor's `BlockTransferService`; if utilization reaches the threshold, the
`MemorySpillManager` first spills the largest partitions to disk (`DISK_ONLY`) and the block is
pipelined afterward. Every block passes through the `BackpressureProtocol` gate, which paces the
producer with token-bucket rate limiting and a heartbeat liveness signal. On the consumer side,
the `StreamingShuffleReader` validates each block's CRC32C and confirms the producer is still
alive. A valid block is acknowledged, allowing the producer-side buffer to be reclaimed; an
invalid or orphaned block invalidates the partial read and raises `FetchFailedException`, which
the existing DAG scheduler resolves by recomputing the upstream stage.

The *Streaming Shuffle Producer-to-Consumer Data Flow* diagram below traces this path, including
the spill and fallback branches.

```text
              Streaming Shuffle Producer-to-Consumer Data Flow

  Map task (producer)
        |
        v
  StreamingShuffleWriter  (extends MemoryConsumer)
        |
        v
  StreamingBuffer  (per-partition, CRC32C)
        |
        v
  ( utilization >= spillThreshold (80%) ? )
        |                          |
        | No                       | Yes
        v                          v
  pipeline block (<= 2 MB)    MemorySpillManager spills DISK_ONLY,
  via BlockTransferService    then pipelines the block
        |                          |
        +-------------+------------+
                      v
  BackpressureProtocol gate (token-bucket + heartbeat) ....> Revert to SortShuffleManager
        |                                                    (a fallback condition is met)
        v
  StreamingShuffleReader  (consumer)
        |
        v
  ( CRC32C valid AND producer alive (< 5 s) ? )
        |                          |
        | Yes                      | No
        v                          v
  acknowledge ->              invalidate partial read ->
  buffer reclaim (<= 100 ms)  throw FetchFailedException -> DAG recompute

Legend: solid ( | v -> ) = normal streaming flow;  dashed ( ....> ) = automatic fallback;
        "( ... ? )" = decision gate;  zero data loss is preserved via invalidation + recompute.
```

The per-partition buffer budget is derived from the executor memory and the configured buffer
percentage, divided evenly across the shuffle's partitions:

```text
perPartitionBudget = (executorMemory × bufferSizePercent / 100) / numPartitions
```

Blocks are pipelined at a 2 MB block size, so each partition's budget bounds how much in-flight
data it may hold before spilling.

## Wire Format

On-the-wire blocks are framed by the `StreamingBlockEnvelope`. Each envelope consists of a
**32-byte big-endian header** followed by a **payload of at most 2 MiB (≤ 2 MiB)**. The payload is protected
by a **CRC32C** checksum that the reader validates on a per-block basis. If the computed checksum
does not match the header, the block is treated as corrupt and **retransmitted**; a persistent
mismatch escalates to partial-read invalidation and recomputation (see
[Fault Tolerance and Zero Data Loss](#fault-tolerance-and-zero-data-loss)).

## Flow Control and Memory Management

Flow control and memory pressure are handled by two cooperating mechanisms.

**Backpressure.** The `BackpressureProtocol` applies token-bucket rate limiting that is capped at
**80% of link capacity**, leaving headroom for control traffic. Consumers emit a **5 s heartbeat**
liveness signal back to producers, and the protocol continuously monitors buffer-utilization
thresholds, performs priority arbitration across competing partitions, and emits telemetry for the
metrics described in [Observability](#observability).

**Memory management.** The `MemorySpillManager` **polls buffer utilization every 100 ms**. When
utilization reaches the **80% threshold**, it spills the **largest partitions** to disk in
**LRU** order via `BlockManager.putBytes(..., DISK_ONLY)`. Once a block has been acknowledged by
the consumer, the corresponding buffer is **reclaimed within 100 ms**, returning memory to the
executor for subsequent partitions.

## Fault Tolerance and Zero Data Loss

Streaming Shuffle is designed so that **no failure scenario can lose data**. Each failure mode maps
to a clean recovery that defers to Spark's existing mechanisms.

* **Producer failure** — detected on a **5 s connection timeout**. Any partial reads in flight
  are invalidated and a `FetchFailedException` is thrown, after which the **existing** DAG scheduler
  recomputes the upstream stage. No scheduler modification is involved.
* **Checksum mismatch** — the affected block is **retransmitted**. A persistent failure
  escalates to partial-read invalidation and recomputation, just like a producer failure.
* **Consumer failure** — detected via **missing acknowledgments over a 10 s window**, after
  which the producer-side buffers are reclaimed.

The resulting invariant is **zero data loss under all failure scenarios**: every failure path
either retransmits cleanly or invalidates the partial read and defers to Spark's existing
recomputation, never leaving a partially consumed result in place.

## Coexistence and Automatic Fallback

Streaming Shuffle never displaces the sort-based shuffle; it falls back to it automatically when
streaming cannot proceed safely. The `StreamingShuffleFallbackPolicy` reverts to the inner
`SortShuffleManager` when **any** of the following four conditions holds:

1. The consumer is sustained **2× slower** than the producer for **more than 60 s**.
2. **Memory pressure** prevents buffer allocation (OOM risk).
3. **Network saturation** exceeds **90% of link capacity**.
4. There is a **producer/consumer version mismatch**.

On any of these conditions, the subsystem reverts to the inner `SortShuffleManager` automatically
and transparently, preserving correctness with no user action required. Operational detail on
diagnosing and responding to fallbacks lives in the
[Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) page.

## Observability

Streaming Shuffle emits four metrics under the `shuffle.streaming.` namespace. They are registered
through the existing `MetricsSystem` via a `Source` named `streamingShuffle`, and are therefore
surfaced through the existing **JMX**, **Prometheus**, **CSV**, and **SLF4J** sinks without any new
endpoint.

| Metric | Type | Meaning |
|--------|------|---------|
| `bufferUtilizationPercent` | Gauge | Current buffer fill level, as a percentage. |
| `spillCount` | Counter | Number of disk spill events. |
| `backpressureEvents` | Counter | Number of backpressure activations. |
| `partialReadInvalidations` | Counter | Number of partial reads invalidated on producer failure. |

These metrics integrate with the broader Spark telemetry surfaces: see
[Monitoring and Instrumentation](monitoring.html) for the metrics system and sinks, and the
Stages tab in the [Web UI](web-ui.html#stages-tab) for native shuffle read/write/spill columns.
Full metric-by-metric interpretation and alerting guidance live in the
[Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) page.

## Related Documentation

* [Streaming Shuffle Guide](streaming-shuffle-guide.html)
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html)
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html)
* [Spark Configuration](configuration.html#shuffle-behavior)
* [Monitoring and Instrumentation](monitoring.html)
* [Web UI](web-ui.html)
* [RDD Programming Guide — Shuffle operations](rdd-programming-guide.html#shuffle-operations)
