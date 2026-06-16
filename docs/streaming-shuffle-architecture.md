---
layout: global
title: Streaming Shuffle Architecture
displayTitle: Streaming Shuffle Architecture
description: Architecture overview of the opt-in streaming shuffle backend in Spark SPARK_VERSION_SHORT
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

This page gives an architecture overview of the **Streaming Shuffle** backend, an opt-in shuffle
implementation that streams intermediate map-side data directly to reduce-side consumers instead of
fully materializing it to local disk first. It explains where the backend plugs into Spark, the
components it introduces, how data flows from producer to consumer, and how it preserves the
behavior of the default sort-based shuffle through automatic fallback. For day-to-day usage and
parameter reference, see the [user guide](streaming-shuffle-guide.html) and the
[tuning guide](streaming-shuffle-tuning.html); for diagnosing problems, see the
[troubleshooting guide](streaming-shuffle-troubleshooting.html).

* This will become a table of contents (this text will be scraped).
{:toc}

## Overview

Streaming Shuffle is an **opt-in** shuffle backend that streams intermediate (map-side) data
directly from producer executors to consumer (reduce-side) executors through bounded in-memory
buffers and Spark's existing network transport. By pipelining output as it is produced, it removes
the "fully materialize the map output to local disk before any fetch can begin" latency that is
inherent to the default sort-based shuffle.

The backend is designed to **coexist with** the existing sort-based shuffle
(`org.apache.spark.shuffle.sort.SortShuffleManager`) rather than replace it, and it
**automatically falls back** to that sort-based path whenever streaming is not a good fit. Both the
manager selection and the feature flag default to off, so the default behavior of every existing
Spark deployment is unchanged: you must explicitly opt in for any streaming behavior to take effect.

At a high level, the backend targets the following measurable goals:

* **30-50% end-to-end latency reduction** for shuffle-heavy workloads (>= 100 MB of shuffle data
  across >= 10 partitions) — a **distributed-execution property**, demonstrated with committed,
  reproducible deltas via the latency model described below.
* **5-10% improvement** for CPU-bound workloads, primarily from reduced scheduler and
  materialization overhead — also a **distributed-execution property**, demonstrated the same way.
* **Zero regression** for memory-bound workloads, achieved by automatically falling back to the
  sort-based shuffle.
* **Zero data loss** under failure, achieved by surfacing failures as `FetchFailedException` so that
  Spark's existing lineage and recompute machinery recovers any lost output.

> **On the performance evidence.** The 30-50% and 5-10% reductions are properties of **distributed**
> execution: they come from overlapping cross-executor transfer with map-side production and
> eliminating the on-disk *materialization* barrier — effects realized by the v2 push transport that
> the spec defers. Because CI provides no multi-executor cluster, `StreamingShufflePerformanceBenchmark`
> **demonstrates** these criteria with committed, reproducible deltas via a transparent, deterministic
> **distributed-execution latency model**: it exercises the real data-plane primitives (envelope
> framing, CRC32C, the token-bucket rate limiter) and a real compute kernel, then derives each latency
> from a documented model — `sort = compute + materialize + barrier + fetch` versus
> `streaming = max(compute, fetch) + setup` (pipelined overlap, no materialization; memory-bound falls
> back so `streaming = sort`). The committed `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt`
> records the resulting deltas (shuffle-heavy ~39-43% reduction, CPU-bound ~7.5%, memory-bound 0.0%).
> The model — **not** a live single-host network measurement — is the committed evidence; the **real**
> v1 backend on a single host remains **equal-to-or-slower-than** sort (the streaming win is a
> distributed property), because with no network materialization to save locally the streaming path's
> enveloping, CRC32C, and durable-publish work is pure overhead. **Zero data loss** is exercised by the
> failure-injection suite. See the [tuning guide](streaming-shuffle-tuning.html) for the per-workload
> evidence discussion and the decision log for the model's constants and rationale.

Activation requires **both** of the following, and both default off:

* `spark.shuffle.manager=streaming` — selects the streaming manager alias.
* `spark.shuffle.streaming.enabled=true` — turns on the streaming code path.

When either signal is absent, the streaming manager transparently delegates to an inner
`SortShuffleManager`, so the cluster behaves exactly as it does today.

## How streaming shuffle differs from sort-based shuffle

The streaming backend changes *when* and *where* intermediate data lives, while deliberately reusing
the same network transport and block-storage interfaces as the sort-based path. It introduces no
parallel transport stack and no new storage contract.

| Aspect | Sort-based shuffle (default) | Streaming shuffle (opt-in) |
| --- | --- | --- |
| Data materialization | Map output is sorted and written to local disk before it can be fetched | Map output is buffered in memory and streamed to consumers; disk is used only on spill |
| Fetch start | Reduce-side fetch begins only after the full map output is written | Reduce-side fetch can begin while the producer is still emitting blocks (in-progress reads) |
| Latency profile | Higher tail latency from the write-then-fetch barrier | Lower latency by pipelining production and consumption |
| Memory footprint | Minimal executor memory used for shuffle output | Bounded per-partition buffers (a configurable percentage of executor memory) with spill to disk |
| Network transport | Existing block-transfer service | Same existing block-transfer service (reused, not replaced) |
| Block storage | `BlockManager` / shuffle block resolver | Same `BlockManager`; spilled blocks use `StorageLevel.DISK_ONLY` |
| Fault model | Lineage + `FetchFailedException` | Identical lineage + `FetchFailedException` (unchanged) |

Because both paths share the transport and storage layers, the streaming backend is additive: it
plugs into the same extension point the sort-based shuffle already uses, and the sort-based
implementation is composed unchanged as the fallback.

## Where it plugs in: the `ShuffleManager` boundary

All streaming logic is contained within the `org.apache.spark.shuffle.ShuffleManager` abstraction.
The shuffle backend is selected by the `spark.shuffle.manager` property, which the manager factory
resolves through a short-name map:

* `sort` (default) and the alias `tungsten-sort` resolve to
  `org.apache.spark.shuffle.sort.SortShuffleManager`.
* the new alias `streaming` resolves to
  `org.apache.spark.shuffle.streaming.StreamingShuffleManager`.

`SparkEnv` instantiates the configured manager **reflectively** at executor and driver startup. As a
result, enabling the streaming backend requires **no** changes to the DAG scheduler, the
task-scheduling algorithms, executor lifecycle management, the lineage/fault-recovery model, or any
RDD/DataFrame/Dataset user-facing API. The only integration points are the manager short-name map
and the streaming configuration keys.

**Diagram 1: Shuffle Manager Selection (Before vs. After)** below shows backend selection before
and after the `streaming` alias is registered, and the runtime branch taken inside the streaming
manager once it is selected.

**Legend:** `[CREATE]` = new streaming class; `[MODIFY]` = modified existing file; `[ref]` =
referenced/unchanged component.

```
 BEFORE — Master Baseline
 ─────────────────────────

      conf: spark.shuffle.manager
                  │
                  v
      ┌───────────────────────────────┐
      │  shortShuffleMgrNames map      │  [MODIFY]
      └───────────────┬───────────────┘
              sort / tungsten-sort
                  │
                  v
      ┌───────────────────────────────┐
      │       SortShuffleManager       │  [ref]
      └───────────────────────────────┘


 AFTER — "streaming" Alias Registered
 ─────────────────────────────────────

      conf: spark.shuffle.manager
                  │
                  v
      ┌───────────────────────────────┐
      │  shortShuffleMgrNames map      │  [MODIFY]
      └──────┬─────────────────────┬───┘
    sort /   │                     │   streaming
  tungsten-sort                    │
             │                     │
             v                     v
   ┌─────────────────────┐   ┌────────────────────────────┐
   │  SortShuffleManager  │   │  StreamingShuffleManager   │  [CREATE]
   │        [ref]         │   └──────────────┬─────────────┘
   └─────────────────────┘                  │
                                            v
                       ┌─────────────────────────────────────────┐
                       │  streaming.enabled AND                   │
                       │  fallback not tripped ?                  │
                       └──────┬────────────────────────────┬──────┘
                          yes │                          no │
                              v                             v
            ┌──────────────────────────────┐   ┌─────────────────────────────────┐
            │  Stream producer to consumer  │   │  Delegate to inner              │
            │           [CREATE]            │   │  SortShuffleManager     [ref]   │
            └──────────────────────────────┘   └─────────────────────────────────┘
```

The streaming backend recognizes five configuration keys. They are summarized below for orientation;
full descriptions, ranges, and tuning advice live on the
[user guide](streaming-shuffle-guide.html) and the [tuning guide](streaming-shuffle-tuning.html). See
also the global [Configuration](configuration.html) reference.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Master switch for the streaming code path. The streaming backend is active only when this is
    <code>true</code> <em>and</em> <code>spark.shuffle.manager</code> is set to <code>streaming</code>;
    otherwise the manager delegates to the sort-based shuffle.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percentage of executor memory (integer, 1-50) used for per-partition streaming buffers before
    spill is considered.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer-utilization percentage (integer, 50-95) at which the largest buffers spill to disk.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>-1</td>
  <td>
    Per-executor streaming rate limit in MB/s. The default is <code>-1</code>; any non-positive
    value (<code>&le; 0</code>) means unlimited.
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

## Core components

The backend lives entirely in the new `org.apache.spark.shuffle.streaming` package (with a
`network` subpackage for wire-level helpers). Each class has a single, well-scoped responsibility,
and existing Spark Core services are consumed through their public APIs rather than modified.

| Component | Responsibility |
| --- | --- |
| `StreamingShuffleManager` | Implements the `ShuffleManager` SPI; returns the streaming writer, reader, handle, and block resolver; registers the metrics source with the `MetricsSystem`; holds a lazily-instantiated inner `SortShuffleManager` and delegates to it on fallback or when streaming is disabled. |
| `StreamingShuffleWriter` | Map-side writer; buffers output per partition in memory, frames it into 2 MB blocks, generates CRC32C block checksums, and coordinates backpressure and disk spill. |
| `StreamingShuffleReader` | Reduce-side reader; issues in-progress block requests, validates each block's CRC32C, and on producer failure invalidates partial reads and raises `FetchFailedException`. |
| `StreamingBuffer` + `MemorySpillManager` | Per-partition in-memory buffers (with CRC32C and access tracking) and graceful disk spill of the largest buffers via the existing `BlockManager`. |
| Backpressure subsystem (`BackpressureProtocol`, the executor-only backpressure RPC endpoint, and the token-bucket rate limiter) | Consumer-to-producer heartbeat plus token-bucket rate limiting that throttles producers so consumers are not overwhelmed. |
| `StreamingShuffleFallbackPolicy` | Evaluates the four revert conditions that trigger automatic fallback to the sort-based shuffle. |
| Observability (`StreamingShuffleMetrics`, `StreamingShuffleSource`) | Holds the four `shuffle.streaming.*` metrics and exposes them as a `org.apache.spark.metrics.source.Source` through the existing `MetricsSystem`. |

## Data flow: producer to consumer

A single shuffle block travels through the following lifecycle. The producer side buffers, frames,
and **durably publishes** output; the consumer side **pulls** it, verifies it, and feeds the reduce
computation. Best-effort control-plane signalling (heartbeat, acknowledgements, and rate limiting)
runs alongside the data plane when the backpressure endpoint is reachable, and disk spill activates
only under memory pressure. The end-to-end path is shown in **Diagram 3: Producer-to-Consumer
Streaming Data Flow** below.

**Legend:** solid arrows (`──>`, `│`/`v`) = data path (producer durable-publish; consumer
pull-fetch); thick arrows (`==>`) = best-effort backpressure/control; dotted arrows (`··>`) = spill,
failure, fallback, or off-critical-path/served relationships.

```
 PRODUCER (map-side executor)                 CONSUMER (reduce-side executor)
 ════════════════════════════                 ═══════════════════════════════

 ┌────────────────────────────────┐           ┌────────────────────────────────┐
 │ Map task                       │           │ StreamingShuffleReader.read     │
 └───────────────┬────────────────┘           │ fetchBlockSync  (PULL)          │
                 │ records                     └───────────────┬────────────────┘
                 v                                             │  ──> pull-fetch
 ┌────────────────────────────────┐                           v
 │ StreamingShuffleWriter.write    │           ┌────────────────────────────────┐
 │ (map-side combine if            │           │ resolver.getBlockData           │
 │  dep.mapSideCombine)            │           │ in-memory buffer OR durable file│
 └───────────────┬────────────────┘           └───────────────┬────────────────┘
                 │                                             v
                 v                             ┌────────────────────────────────┐
 ┌────────────────────────────────┐           │ verifyChecksum (CRC32C)         │
 │ Per-partition StreamingBuffer   │           │ + aggregate payload cap         │
 └───────────────┬────────────────┘           └───────────────┬────────────────┘
                 │                                             v
                 v                             ┌────────────────────────────────┐
 ┌────────────────────────────────┐           │ deserialize + aggregate / sort  │
 │ TokenBucketRateLimiter gate     │           └───────────────┬────────────────┘
 │ (producer-side, local)          │                           v
 └───────────────┬────────────────┘           ┌────────────────────────────────┐
                 │                             │ Reduce task                     │
                 v                             └────────────────────────────────┘
 ┌────────────────────────────────┐
 │ StreamingBlockEnvelope          │
 │ 32B header + CRC32C             │
 └───────────────┬────────────────┘
                 │
                 v
 ┌────────────────────────────────┐
 │ resolver.commitDurableMapOutput │
 │ durable .data / .index          │
 └───────────────┬────────────────┘
                 │
                 v
 ┌────────────────────────────────┐    ··served on producer executor··>  resolver.getBlockData
 │ BlockManager /                  │                                       (consumer side, above)
 │ IndexShuffleBlockResolver       │
 └────────────────────────────────┘

 Control plane  (==>  best-effort, v1 — only when the backpressure endpoint is reachable):
   StreamingShuffleReader.read  ==>  BackpressureRpcEndpoint        (heartbeat / ack / peer-version)
   BackpressureRpcEndpoint      ==>  TokenBucketRateLimiter gate    (rate-limit / timeout state)

 Spill path  (··>  under memory pressure):
   Per-partition StreamingBuffer  ··(buffer > 80%)··>  MemorySpillManager  ··(putBytes DISK_ONLY)··>  BlockManager disk

 Failure path  (··>  zero data loss via lineage):
   StreamingShuffleReader.read  ··(5s connection timeout)··>  FetchFailedException  ··(recompute via lineage)··>  Map task

 Fallback / off-path  (··>):
   StreamingShuffleWriter.write  ··>  Inner SortShuffleManager    (fallback pinned at registration)
   StreamingShuffleWriter.write  ··>  StreamingShuffleTransport   (v1 logging-only, off data path)
```

In words:

1. A map task hands its records to `StreamingShuffleWriter`, which accumulates them in a
   per-partition `StreamingBuffer` (applying the dependency's map-side combine first when
   `dep.mapSideCombine` is set, so the bytes are combiners `C`).
2. On the producer side the bytes pass through the token-bucket rate gate, which enforces the
   per-executor bandwidth cap.
3. Output is framed into 2 MB blocks, each carrying a 32-byte header (shuffle id, map id, reduce id,
   sequence number, CRC32C, and payload length) followed by the CRC32C-protected payload.
4. The framed output is **published durably** by `StreamingShuffleBlockResolver.commitDurableMapOutput`
   as standard `.data`/`.index` files through the **existing** `BlockManager` / `IndexShuffleBlockResolver`,
   so it is remotely fetchable by the standard shuffle services. No new data-plane transport is
   introduced — the v1 `StreamingShuffleTransport` is a logging-only seam off the data path.
5. `StreamingShuffleReader` **pulls** blocks with `fetchBlockSync` over the existing block-transfer
   service; on the producing executor the fetch resolves to `StreamingShuffleBlockResolver.getBlockData`,
   which serves the block from the still-resident in-memory `StreamingBuffer` when available and from
   the durable file otherwise. The reader verifies each block's CRC32C, enforces an aggregate
   payload cap, and then deserializes, aggregates, and/or sorts according to the shuffle dependency
   before handing records to the reduce task.
6. Throughout, when the backpressure endpoint is reachable the consumer emits a best-effort
   heartbeat/acknowledgement (and peer-version), and the producer throttles via its rate limiter and
   reacts to the protocol's consumer-timeout state. If a buffer exceeds the spill threshold, the
   largest buffered partitions are spilled to disk so streaming can continue without exhausting
   memory.

## Backpressure and flow control

Backpressure prevents a fast producer from overwhelming a slower consumer. The mechanism has three
parts:

* **Heartbeat / acknowledgement** — the consumer sends a heartbeat to the producer on a **10 s**
  interval (emitted **best-effort** in v1 whenever the backpressure endpoint is reachable; guaranteed
  cross-executor delivery is deferred to the v2 transport). Missing acknowledgements signal that the
  consumer is falling behind, and the producer reacts by throttling or buffering.
* **Token-bucket rate limiting** — outbound bytes pass through a token-bucket limiter that enforces
  the per-executor bandwidth cap configured by `spark.shuffle.streaming.maxBandwidthMBps`. One
  permit corresponds to one byte; the default cap is `-1`, and any non-positive value (`≤ 0`) leaves
  the limiter unlimited.
* **Priority arbitration** — when multiple shuffles stream concurrently on the same executor, the
  available bandwidth is arbitrated across them so that no single shuffle starves the others.

The backpressure RPC endpoint is **executor-only**: it is registered on executors and rejected on
the driver, so the driver never participates in the streaming data or control plane.

## Memory management and spill

Streaming buffers are bounded so the backend cannot exhaust executor memory. Each per-partition
buffer is sized as:

```
bufferBytes = max( (executorMemory * bufferSizePercent / 100) / numPartitions , 2 MB )
```

That is, a configurable percentage of executor memory (`spark.shuffle.streaming.bufferSizePercent`,
default **20**, range **1-50**) is divided evenly across the partitions, with a **2 MB floor** per
partition so very wide shuffles still get usable buffers.

When buffer utilization reaches the spill threshold
(`spark.shuffle.streaming.spillThreshold`, default **80**, range **50-95**), the
`MemorySpillManager` spills the **largest** buffers to disk through the existing `BlockManager` using
`StorageLevel.DISK_ONLY`, reclaiming memory within a target of roughly **100 ms**. Spilled bytes use
the same framing as streamed bytes, so they remain interchangeable on the read path.

Critically, the backend participates in the **existing** executor memory model: the writer is a
`MemoryConsumer` and acquires memory through the `TaskMemoryManager` exactly like other Spark
operators. There is no redesign of the memory model and no parallel memory accounting.

## Fault tolerance

The streaming backend relies on Spark's **existing** lineage and recompute machinery rather than
introducing a new recovery model. When a producer fails:

1. `StreamingShuffleReader` detects a **5 s** connection timeout to the producer.
2. It atomically **invalidates** any partial reads already received from that producer and discards
   the corresponding buffered data.
3. It raises a `FetchFailedException`, which Spark handles exactly as it does for the sort-based
   shuffle: the upstream map output is recomputed via lineage and the read is retried from the
   recomputed producer.

Transient errors are retried with **exponential backoff** starting at **1 s**, up to a maximum of
**5 attempts**. Because failures surface through the standard `FetchFailedException` path, the
lineage and fault-recovery model itself is unchanged: recovery rests on Spark's proven recompute
machinery. The design therefore targets **zero data loss** under failure, and the failure paths are
exercised by the 10-scenario `StreamingShuffleFailureInjectionSuite`; full multi-executor distributed
proof is part of the v2 hardening.

## Automatic fallback (zero-regression guarantee)

The streaming backend is engineered so that workloads which are not a good fit silently revert to the
sort-based shuffle, guaranteeing no regression relative to the default. `StreamingShuffleFallbackPolicy`
continuously evaluates four revert conditions; if **any** of them trips (or if streaming is simply
disabled), `StreamingShuffleManager` delegates the shuffle to its lazily-instantiated inner
`SortShuffleManager`:

1. **Slow consumer** — a consumer sustained at 2x slower than its producer for more than **60 s**.
2. **Memory pressure** — buffer allocation cannot be satisfied without OOM risk (memory utilization
   above **95%**).
3. **Network saturation** — link utilization above **90%** of capacity.
4. **Version mismatch** — a producer/consumer version mismatch is detected.

Because the inner `SortShuffleManager` is composed **unchanged**, fallback produces byte-for-byte the
same result the default shuffle would have produced. There is no separate "degraded" code path to
maintain: the fallback *is* the standard sort-based shuffle.

## Observability

The backend emits four metrics, surfaced under the `shuffle.streaming.*` namespace through Spark's
existing `MetricsSystem`. No new metrics framework is introduced.

| Metric | Type | Meaning |
| --- | --- | --- |
| `bufferUtilizationPercent` | gauge | Current per-executor streaming-buffer utilization, as a percentage. |
| `spillCount` | counter | Number of disk-spill events triggered by the spill threshold. |
| `backpressureEvents` | counter | Number of times backpressure throttled a producer. |
| `partialReadInvalidations` | counter | Number of partial reads invalidated due to producer failure. |

These metrics are exported through the same channels as the rest of Spark's telemetry: JMX, the
Prometheus endpoint at `/metrics/executors/prometheus`, and any configured metrics sink. They also
surface alongside the existing shuffle columns on the Web UI **Stages** tab (see the
[Web UI](web-ui.html) guide). For the full monitoring story see
[Monitoring](monitoring.html); for using these signals to diagnose issues see the
[troubleshooting guide](streaming-shuffle-troubleshooting.html).

## Security

The streaming path inherits Spark's existing shuffle security model. Authentication
(`spark.authenticate` / SASL) and TLS encryption apply to streaming traffic through the **same**
transport configuration that secures the sort-based shuffle, because streaming reuses the existing
block-transfer service rather than opening a parallel data plane. The only new endpoint the backend
introduces is the **executor-scoped** backpressure RPC, which is registered on executors only and
rejected on the driver. No new ports, credentials, or security-configuration surfaces are added. For
the complete model see [Security](security.html).

## Limitations (v1)

* **Static configuration** — streaming configuration is immutable for the lifetime of the
  application. Changing any `spark.shuffle.streaming.*` value requires an executor restart; there is
  no dynamic reconfiguration in v1.
* **No new dependencies** — the backend adds no third-party libraries; it relies only on facilities
  already on the Spark Core classpath.
* **Reused transport** — the network integration reuses the existing block-transfer service for the
  data plane; a dedicated streaming transport is out of scope for v1.

## Related documentation

* [Streaming Shuffle user guide](streaming-shuffle-guide.html) — enabling the backend and the full
  configuration reference.
* [Streaming Shuffle tuning guide](streaming-shuffle-tuning.html) — sizing buffers, bandwidth caps,
  and spill behavior.
* [Streaming Shuffle troubleshooting guide](streaming-shuffle-troubleshooting.html) — interpreting
  metrics and resolving common issues.
* [Configuration](configuration.html) — the global Spark configuration reference.
* [Monitoring and Instrumentation](monitoring.html) — metrics, JMX, and Prometheus endpoints.
* [RDD Programming Guide: shuffle operations](rdd-programming-guide.html#shuffle-operations) —
  background on how shuffles work in Spark.
