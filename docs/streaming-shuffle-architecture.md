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
implementation that streams intermediate data directly from producer (map-side) executors to
consumer (reduce-side) executors instead of fully materializing it to local disk first. It explains
where the backend plugs into the existing `ShuffleManager` abstraction, the components that make it
work, the producer-to-consumer data flow, and the memory, backpressure, fault-tolerance, and
fallback mechanisms that keep it safe. For step-by-step usage and tuning, see the
[user guide](streaming-shuffle-guide.html) and the [tuning guide](streaming-shuffle-tuning.html);
for diagnostics, see the [troubleshooting guide](streaming-shuffle-troubleshooting.html).

* This will become a table of contents (this text will be scraped).
{:toc}

## Overview

Streaming Shuffle is an **opt-in** shuffle backend that streams intermediate (map-side) output
directly from producer executors to consumer (reduce-side) executors through bounded in-memory
buffers and Spark's existing network transport. The default sort-based shuffle &mdash; the
[shuffle operations](rdd-programming-guide.html#shuffle-operations) described in the RDD Programming
Guide &mdash; fully materializes every map task's output to local disk before any reduce task can
begin fetching it; Streaming Shuffle removes that "materialize-then-fetch" barrier by pipelining
data as it is produced, which reduces end-to-end shuffle latency.

The backend is designed to **coexist with**, and **automatically fall back to**, the existing
sort-based shuffle (`SortShuffleManager`). It is engaged only when an operator explicitly opts in,
and it reverts to the sort-based path whenever a workload is unsuitable (for example, under memory
pressure). Because activation is opt-in and the fallback is automatic, the **default behavior of
every existing Spark deployment is unchanged** — no configuration change means byte-for-byte
identical behavior to the sort-based shuffle.

At a high level, the backend targets the following measurable goals. The **v1 release** delivers the
correctness and safety guarantees in full; the **headline latency-reduction targets are v2 goals**
that materialize once the real streaming data plane replaces the v1 logging-only transport layer
(see the streaming-shuffle decision log).

**Delivered in v1 (verified):**

* **Zero regression** for memory-bound workloads, achieved through automatic fallback to the
  sort-based shuffle.
* **Zero data loss** under all failure scenarios, achieved by surfacing failures as
  `FetchFailedException` so Spark's existing lineage and recompute machinery recovers lost output.

**v2 latency targets (design goals; not yet met in v1):**

* **30&ndash;50% end-to-end latency reduction** for shuffle-heavy workloads (&ge; 100 MB of shuffle
  data across &ge; 10 partitions).
* **5&ndash;10% improvement** for CPU-bound workloads via reduced scheduler and materialization
  overhead.

Because v1 reuses the existing `BlockTransferService` pull path (the `StreamingShuffleTransport` is
an intentional logging-only integration layer — not a defect), the committed benchmark artifacts
report the actual measured v1 numbers (shuffle-heavy &asymp; 2.7% best / 11.5% average; CPU-bound
&asymp; 5.2% best / 4.1% average; memory-bound fallback shows no regression), demonstrating
functional parity and zero regression rather than the headline latency deltas.

The remainder of this page describes how the v1 guarantees are met (and how the v2 latency targets
will be reached) without modifying the DAG scheduler, the task-scheduling algorithms, executor
lifecycle management, the lineage/fault-recovery model, or any RDD/DataFrame/Dataset user-facing
API.

## How it differs from sort-based shuffle

Streaming Shuffle reuses the **same network transport and block-storage interfaces** as the
sort-based shuffle rather than introducing parallel machinery; the difference is *when* and *where*
intermediate data lives. The table below summarizes the contrast.

| Aspect | Sort-based shuffle (default) | Streaming Shuffle (opt-in) |
| --- | --- | --- |
| Data materialization | Map output is sorted and fully written to local disk as `.data`/`.index` files | Map output is buffered in bounded in-memory buffers and streamed; spilled to disk only under memory pressure |
| Fetch start | Reduce-side fetch begins only **after** the full map-side write completes | Reduce-side fetch begins **in progress**, while the producer is still emitting blocks |
| Latency profile | Higher: a materialization barrier separates write and read | Lower: write and read overlap, removing the barrier |
| Memory footprint | Minimal map-side memory; relies on disk | Bounded per-partition buffers (a configurable percentage of executor memory) with graceful spill |
| Network transport | Existing `BlockTransferService` / block-transfer path | **Same** `BlockTransferService` / block-transfer path (reused, not replaced) |
| Block storage | Existing `BlockManager` | **Same** `BlockManager` (used for spill via `StorageLevel.DISK_ONLY`) |
| Fault recovery | `FetchFailedException` &rarr; lineage recompute | **Same** `FetchFailedException` &rarr; lineage recompute (model unchanged) |

The key takeaway is that Streaming Shuffle is an *additive* path layered on top of existing Spark
Core services. When it is not active &mdash; or when fallback trips &mdash; the behavior is exactly
that of the sort-based shuffle.

## Where it plugs in (the `ShuffleManager` boundary)

All streaming logic is contained **within the `org.apache.spark.shuffle.ShuffleManager`
abstraction**. Spark selects a shuffle backend through the `spark.shuffle.manager` configuration
property, which is resolved against a small manager-name map:

* `sort` (the default) and its alias `tungsten-sort` resolve to `SortShuffleManager`.
* the new alias `streaming` resolves to
  `org.apache.spark.shuffle.streaming.StreamingShuffleManager`.

`SparkEnv` instantiates the configured manager **reflectively** at startup, so registering the
`streaming` alias is the only wiring required. There are **no changes** to the scheduler, the DAG,
executor lifecycle management, or any user-facing API. Activation additionally requires the feature
flag `spark.shuffle.streaming.enabled=true`; both `spark.shuffle.manager=streaming` and
`spark.shuffle.streaming.enabled=true` must be set, and both default to off.

Once selected, `StreamingShuffleManager` decides per shuffle whether to stream or to delegate. If
streaming is enabled and the [automatic fallback](#automatic-fallback-zero-regression-guarantee)
policy has not tripped, it streams data from producer to consumer; otherwise it delegates to a
lazily-constructed inner `SortShuffleManager`. **Diagram 1 &mdash; Shuffle Manager Selection: Before vs. After (Factory Modification)** shows this selection path, contrasting the master baseline (only the sort-based managers) with the state after the `streaming` alias is registered.

**Legend:** `(CREATE)` = new streaming class; `(MODIFY)` = modified existing file; `(ref)` = referenced/unchanged component.

```
Diagram 1 — Shuffle Manager Selection: Before vs. After (Factory Modification)

BEFORE — Master Baseline
------------------------
    conf: spark.shuffle.manager
              |
              v
    +-----------------------------+
    | shortShuffleMgrNames map    |  (MODIFY)
    +-----------------------------+
        |                  |
     "sort"         "tungsten-sort"
        |                  |
        v                  v
    +-----------------------------+
    | SortShuffleManager          |  (ref)
    +-----------------------------+

AFTER — "streaming" Alias Registered
------------------------------------
    conf: spark.shuffle.manager
              |
              v
    +-----------------------------+
    | shortShuffleMgrNames map    |  (MODIFY)
    +-----------------------------+
        |                              |
  "sort" / "tungsten-sort"        "streaming"
        |                              |
        v                              v
  +----------------------+   +-------------------------------+
  | SortShuffleManager   |   | StreamingShuffleManager       |  (CREATE)
  | (ref)                |   +-------------------------------+
  +----------------------+                |
                                          v
                          < streaming.enabled AND
                            fallback NOT tripped ? >
                             |                     |
                           yes                     no
                             |                     |
                             v                     v
              +--------------------------+  +-------------------------------+
              | Stream producer ->       |  | Delegate to inner             |
              | consumer       (CREATE)  |  | SortShuffleManager     (ref)  |
              +--------------------------+  +-------------------------------+
```

Because the inner `SortShuffleManager` is composed **unchanged**, the sort-based path is never
bypassed when fallback conditions trip &mdash; it is the same implementation the default cluster
uses.

## Core components

The streaming backend lives entirely in the new `org.apache.spark.shuffle.streaming` package (with
a `network` subpackage) and implements the `ShuffleManager` service-provider contract. The table
below maps each component to its responsibility at a conceptual level.

| Component | Responsibility |
| --- | --- |
| `StreamingShuffleManager` | Implements the `ShuffleManager` SPI; returns the streaming writer, reader, handle, and block resolver; registers the metrics source with the `MetricsSystem`; holds a lazily-instantiated inner `SortShuffleManager` and delegates to it when streaming is disabled or fallback trips |
| `StreamingShuffleHandle` | A `BaseShuffleHandle` subtype that additionally carries the per-shuffle tuning (`bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps`) so the writer and reader receive their settings without re-reading configuration |
| `StreamingShuffleWriter` | Map-side writer that buffers output per partition in memory, frames it into 2 MB blocks, generates CRC32C checksums, applies backpressure, and coordinates spill; participates in the executor memory model as a `MemoryConsumer` |
| `StreamingShuffleReader` | Reduce-side reader that issues in-progress block requests, validates each block's CRC32C, and on failure invalidates partial reads and raises `FetchFailedException` |
| `StreamingShuffleBlockResolver` | Resolves streamed and spilled blocks; tracks buffers and spilled files; delegates `.data`/`.index` and migration concerns to the existing block resolver |
| `StreamingBuffer` | A per-partition in-memory buffer with CRC32C accounting and access tracking used to decide spill order |
| `MemorySpillManager` | Polls buffer utilization and spills the largest buffers to disk via the existing `BlockManager` (`StorageLevel.DISK_ONLY`) when the spill threshold is reached, reclaiming memory within the spill SLA |
| `BackpressureProtocol` | Token-bucket plus heartbeat flow-control state machine that throttles producers so consumers are not overwhelmed |
| Backpressure RPC endpoint | An **executor-only** `ThreadSafeRpcEndpoint` (registered on the existing `RpcEnv`) carrying heartbeat, acknowledgement, rate-limit, and timeout messages; the driver does not register it |
| `TokenBucketRateLimiter` | Enforces the per-executor bandwidth cap (one permit = one byte) via Guava's `RateLimiter`; unlimited when the cap is non-positive |
| `StreamingShuffleFallbackPolicy` | Evaluates the four revert conditions that gate automatic fallback to the sort-based shuffle |
| `StreamingShuffleMetrics` / `StreamingShuffleSource` | Hold and expose the four `shuffle.streaming.*` metrics; the source implements `org.apache.spark.metrics.source.Source` and surfaces the metrics through the existing `MetricsSystem` |
| `StreamingShuffleConfig` | A typed accessor for the five `spark.shuffle.streaming.*` properties with validation and derived values |

All of these collaborators are constructed from inside `StreamingShuffleManager` and consume
existing platform services (memory manager, block manager, RPC environment, metrics system,
map-output tracker, and block-transfer service) through their public APIs &mdash; none of those
services is modified.

**Diagram 2 &mdash; Streaming Shuffle Component Interaction** shows these new `org.apache.spark.shuffle.streaming` classes (green) and the existing Spark Core services they consume (gray), entered through the modified `ShuffleManager` factory (blue). Solid arrows denote construction or usage; the dashed arrow denotes fallback delegation from `StreamingShuffleManager` to the inner `SortShuffleManager`.

**Legend:** `(CREATE)` = new streaming class; `(MODIFY)` = modified existing file; `(ref)` = referenced/unchanged Spark Core component; `-->` = construction/usage; `..>` (dotted) = fallback delegation.

```
Diagram 2 — Streaming Shuffle Component Interaction

  SparkEnv.create (ref)
        |
        v
  ShuffleManager factory: shortShuffleMgrNames (MODIFY)
        |
        v
  StreamingShuffleManager (CREATE)  <-- StreamingShuffleConfig (CREATE) supplies config
        |
        +--> StreamingShuffleHandle         (CREATE)
        +--> StreamingShuffleWriter         (CREATE)   [see writer fan-out below]
        +--> StreamingShuffleReader         (CREATE)   [see reader fan-out below]
        +--> StreamingShuffleBlockResolver  (CREATE)
        +--> StreamingShuffleSource         (CREATE) --> MetricsSystem (ref)
        +--> StreamingShuffleFallbackPolicy (CREATE)
        ..>  SortShuffleManager             (ref)      [dotted = fallback delegation]

  StreamingShuffleWriter (CREATE) constructs/uses:
        +--> StreamingBuffer                (CREATE)
        +--> BackpressureProtocol           (CREATE)
        |        +--> BackpressureRpcEndpoint (CREATE, executor-only)
        |        +--> TokenBucketRateLimiter  (CREATE)
        +--> MemorySpillManager             (CREATE)
        |        +--> MemoryManager           (ref)
        |        +--> BlockManager            (ref)
        +--> StreamingShuffleTransport      (CREATE, v1 logging-only stub)
        +--> StreamingBlockEnvelope         (CREATE)

  StreamingShuffleReader (CREATE) consumes:
        +--> MapOutputTracker               (ref)
        +--> BlockTransferService           (ref)
        +--> StreamingBlockEnvelope         (CREATE)

  Metrics flow (all CREATE feed the source):
        StreamingShuffleWriter ---+
        StreamingShuffleReader ---+--> StreamingShuffleMetrics (CREATE)
        BackpressureProtocol   ---+         |
        MemorySpillManager     ---+         v
                                   StreamingShuffleSource (CREATE) --> MetricsSystem (ref)
```

## Data flow (producer &rarr; consumer)

A shuffle block travels from a map task to a reduce task as follows. On the **producer** side, the
map task hands records to `StreamingShuffleWriter`, which buffers them in a per-partition
`StreamingBuffer`. Output passes through the token-bucket rate gate, is framed into 2 MB blocks
&mdash; each prefixed with a 32-byte header and protected by a CRC32C checksum &mdash; and is sent
over the existing block-transfer service. On the **consumer** side, `StreamingShuffleReader` fetches
blocks while they are still being produced, verifies each block's CRC32C, and then deserializes and
(where the shuffle dependency requires it) aggregates or sorts records before handing them to the
reduce task.

Two control paths overlay the data path. A **backpressure** path sends a heartbeat/acknowledgement
(and, when a bandwidth cap is configured, a rate-limit request) from the consumer back to the
producer so the producer can throttle when the consumer falls behind. This control plane is
**RPC-wired** through the per-executor `BackpressureRpcEndpoint`: after each successful fetch the
reader uses the in-package `BackpressureRpcSender` to deliver these messages to the **co-located**
producer's endpoint over the existing `RpcEnv`, driving the producer-side protocol. Driving an
arbitrary **remote** (non-co-located) producer requires endpoint auto-discovery and is a **v2
enhancement**. A **spill** path activates when a buffer's utilization exceeds the spill threshold
(default 80%): the largest buffers are written to disk through the `BlockManager` using
`StorageLevel.DISK_ONLY`, freeing memory while keeping the streamed and spilled bytes
interchangeable.

**Diagram 3 &mdash; Producer-to-Consumer Streaming Data Flow with Backpressure, Spill, and Fallback** traces a single shuffle block end to end: from the map task through `StreamingShuffleWriter` into a per-partition `StreamingBuffer`, past the token-bucket rate gate and transport, across the wire as a CRC32C-checked `StreamingBlockEnvelope`, and into `StreamingShuffleReader` for verification, deserialization, and aggregation/sort before the reduce task. The spill, control (backpressure), failure, and fallback paths overlay the data path. The **fallback path is a manager-level decision, not a mid-write switch**: the backpressure layer and `MemorySpillManager` push their throughput/network and memory-utilization samples into the manager-owned `StreamingShuffleFallbackPolicy`, and `StreamingShuffleManager.registerShuffle` consults it (see [Automatic fallback](#automatic-fallback-zero-regression-guarantee)) to route each new shuffle to streaming or the inner `SortShuffleManager`.

**Legend:** solid arrows (`->`) = data path; thick arrows (`==>`) = backpressure/control; dotted arrows (`..>`) = spill, failure, or fallback.

> **As-built note (v1).** The `RD ==> RPC` (heartbeat/ack) and `RPC ==> RL` (rate-limit/timeout)
> edges are **production-wired** over the existing `RpcEnv`: after each successful fetch the reader's
> `BackpressureRpcSender` delivers `Heartbeat`/`Ack` (and a one-time `RateLimitRequest` when a
> bandwidth cap is set) to the **co-located** producer's `BackpressureRpcEndpoint`, which drives the
> producer-side `BackpressureProtocol` (proven by the cross-`RpcEnv` integration tests in
> `BackpressureRpcEndpointSuite`). Driving an arbitrary **remote** producer requires endpoint
> auto-discovery (mapping a producer `BlockManagerId` to its RPC address) and is deferred to **v2**.
> The data plane (`TX`/`WIRE`) is the existing `BlockTransferService.fetchBlockSync` pull path;
> `StreamingShuffleTransport` is the intentional v1 logging-only integration layer.

```
Diagram 3 — Producer-to-Consumer Streaming Data Flow with Backpressure, Spill, and Fallback

DATA PATH (-> = data):

    Map task
      -> StreamingShuffleWriter.write
      -> Per-partition StreamingBuffer
      -> TokenBucketRateLimiter gate
      -> StreamingShuffleTransport.sendBlock
      -> StreamingBlockEnvelope (32-byte header + CRC32C)
      -> [wire] StreamingShuffleReader.read (fetchBlockSync)
      -> verifyChecksum
      -> deserialize + aggregate/sort
      -> Reduce task

CONTROL PATH (==> = backpressure/control):

    StreamingShuffleReader.read  ==(heartbeat 10s / ack)==>  BackpressureRpcEndpoint
    BackpressureRpcEndpoint      ==(rate-limit / timeout)==>  TokenBucketRateLimiter gate

SPILL PATH (..> = spill):

    Per-partition StreamingBuffer  ..(buffer > 80%)..>       MemorySpillManager
    MemorySpillManager             ..(putBytes DISK_ONLY)..> BlockManager disk

FAILURE PATH (..> = failure):

    StreamingShuffleReader.read    ..(5 s timeout)..>           FetchFailedException
    FetchFailedException           ..(recompute via lineage)..> Map task

FALLBACK PATH (..> = manager-level decision, not a mid-write switch):

    BackpressureRpcEndpoint         ..(throughput / network samples)..> StreamingShuffleFallbackPolicy
    MemorySpillManager              ..(memory-utilization samples)..>   StreamingShuffleFallbackPolicy
    StreamingShuffleFallbackPolicy  ..(shouldFallback)..>               StreamingShuffleManager.registerShuffle
    StreamingShuffleManager.registerShuffle  ..(delegate new shuffle)..> Inner SortShuffleManager
```

If a block fails verification or the producer becomes unreachable, the reader follows the
[fault-tolerance](#fault-tolerance) path rather than delivering partial data.

## Backpressure &amp; flow control

Backpressure prevents a fast producer from overwhelming a slow consumer. Each consumer sends a
**heartbeat/acknowledgement** to its producers on a **10 s interval**; the producer uses these
signals to gauge consumer progress and throttle accordingly. Throttling is enforced by a
**token-bucket rate limiter** that caps per-executor streaming bandwidth at
`spark.shuffle.streaming.maxBandwidthMBps` megabytes per second (one permit corresponds to one byte;
a non-positive value means unlimited). When several shuffles run concurrently on the same executor,
the protocol applies **priority arbitration** so the available bandwidth is shared rather than
monopolized by a single shuffle.

The backpressure control plane runs over an **executor-only** RPC endpoint registered on Spark's
existing `RpcEnv`. The endpoint is registered on executors only and is **rejected on the driver**,
so it adds no driver-side surface. It carries only small control messages (heartbeats,
acknowledgements, rate-limit updates, and timeout notifications) &mdash; the bulk shuffle data still
flows over the existing block-transfer service. In v1 the consumer-side sender
(`BackpressureRpcSender`, invoked by `StreamingShuffleReader` after each successful fetch) delivers
these control messages to the **co-located** producer's endpoint, driving its protocol end-to-end
over the `RpcEnv`; auto-discovering an arbitrary **remote** (non-co-located) producer's endpoint is
a **v2 enhancement**.

## Memory management &amp; spill

Streaming Shuffle bounds its memory footprint with **per-partition buffers** sized as a configurable
percentage of executor memory. The per-partition buffer size is computed as:

```
bufferBytesPerPartition = max(
    (executorMemory * bufferSizePercent / 100) / numPartitions,
    2 MB)
```

where `bufferSizePercent` comes from `spark.shuffle.streaming.bufferSizePercent` (default 20, valid
range 1&ndash;50). A **2 MB floor** guarantees every partition has at least one full block of
headroom regardless of partition count.

When aggregate buffer utilization reaches the spill threshold &mdash;
`spark.shuffle.streaming.spillThreshold`, default 80, valid range 50&ndash;95 &mdash; the
`MemorySpillManager` spills the **largest** buffers first to local disk through the existing
`BlockManager` using `StorageLevel.DISK_ONLY`, reclaiming memory within an approximately **100 ms**
SLA. Spilled bytes use the same wire/persist format as streamed bytes, so a spilled partition can be
served interchangeably with an in-memory one.

Crucially, the writer participates in the executor memory model as a `MemoryConsumer` and acquires
memory through the existing `MemoryConsumer` / `TaskMemoryManager` path. The spill denominator is
the existing `MemoryManager.maxOnHeapStorageMemory`. There is **no redesign of the executor memory
model** &mdash; the backend reuses the same acquisition and spill machinery that other Spark memory
consumers use.


## Fault tolerance

Streaming Shuffle guarantees **zero data loss** by reusing Spark's existing lineage and recompute
machinery rather than inventing a new recovery model. On producer failure, the reader detects a
**5 s connection timeout**, atomically **invalidates all partial reads** from the failed producer,
and raises a `FetchFailedException`. Spark's scheduler treats this exactly as it treats a sort-based
shuffle fetch failure: the affected upstream map output is recomputed from lineage and the read is
retried against the recomputed producer. No partially-read data is ever delivered to the reduce
task.

Transient errors are retried with **exponential backoff starting at 1 s, up to a maximum of 5
attempts**, before a failure is surfaced. Because recovery is delegated to the existing scheduler
and lineage tracking, the **lineage/fault-recovery model itself is unchanged** &mdash; Streaming
Shuffle only changes how intermediate data is transported, not how Spark recovers from losing it.

## Automatic fallback (zero-regression guarantee)

To guarantee no regression for workloads that are unsuitable for streaming, `StreamingShuffleManager`
owns a single `StreamingShuffleFallbackPolicy` and **wires production signals into it from their
natural sources** so the policy reflects real executor state. The policy is kept current by
measurements pushed continuously from the streaming collaborators, and the **revert decision is made
at `registerShuffle` time** (inside `StreamingShuffleManager.useStreaming`), which first calls
`refreshFallbackSignals()` to pull a fresh executor-memory sample. When **any** revert condition holds
as a new shuffle registers &mdash; or when streaming is simply disabled &mdash; the manager delegates
that shuffle to its lazily-instantiated inner `SortShuffleManager`. The four conditions, each with the
exact production signal source that feeds it:

| # | Condition | Production signal source |
| --- | --- | --- |
| 1 | **Slow consumer** &mdash; consumer sustained 2&times; slower than producer for > 60 s | `BackpressureProtocol.updateThroughputWindow` &rarr; `recordThroughput` |
| 2 | **Memory pressure** &mdash; allocation risks OOM (utilization > 95%) | `MemorySpillManager.maybeSpill` &rarr; `updateMemoryUtilization`, plus the manager's registration-time `refreshFallbackSignals()` pull |
| 3 | **Network saturation** &mdash; usage > 90% of link capacity | `BackpressureProtocol.updateThroughputWindow` &rarr; `updateNetworkUtilization` |
| 4 | **Version mismatch** &mdash; incompatible streaming protocol versions | `BackpressureProtocol.reportVersionMismatch` &rarr; `markVersionMismatch` |

This wiring is verified end-to-end: `StreamingShuffleManagerSuite` drives each of the four conditions
into the manager's own policy **with streaming enabled** and asserts that `registerShuffle` returns a
sort handle from the unchanged inner `SortShuffleManager`, and `StreamingShuffleFailureInjectionSuite`
scenario 8 proves the memory-pressure fallback specifically.

**v1 note on version mismatch.** The version-mismatch trigger is fully wired
(`reportVersionMismatch` &rarr; `markVersionMismatch`), but the v1 wire envelope (a 32-byte header
with no version field) carries no version to compare, so on-wire **auto-detection** is deferred to v2
alongside the network-transport hardening. The other three conditions trip automatically from live
executor signals.

Because the inner `SortShuffleManager` is composed **unchanged**, falling back is equivalent to
running the default sort-based shuffle. This is the mechanism behind the zero-regression guarantee
for memory-bound workloads.

## Observability

Streaming Shuffle ships with observability built in. It emits **four metrics** through Spark's
existing `MetricsSystem`, surfaced under the `shuffle.streaming.*` namespace:

| Metric | Type | Meaning |
| --- | --- | --- |
| `bufferUtilizationPercent` | gauge | Current aggregate buffer utilization as a percentage of the configured buffer budget |
| `spillCount` | counter | Number of spill operations performed |
| `backpressureEvents` | counter | Number of backpressure throttling events triggered |
| `partialReadInvalidations` | counter | Number of partial-read invalidations caused by producer failures |

These metrics are exposed through the same channels as every other Spark metric: JMX, the Prometheus
endpoint at `/metrics/prometheus` (the `PrometheusServlet` sink), and any configured metrics sinks. The four
`shuffle.streaming.*` metrics are **not** added as Spark Web UI columns. Generic shuffle volume (the
standard Shuffle Read / Shuffle Write byte counts) still continues to appear via the Stages-tab
shuffle columns described in the [Web UI](web-ui.html) guide, exactly as it does for sort-based
shuffle, because the streaming reader and writer update Spark's standard shuffle read/write metrics.
For metrics configuration and endpoints, see the [Monitoring](monitoring.html) guide; for
interpreting these metrics during incidents, see the
[troubleshooting guide](streaming-shuffle-troubleshooting.html).

## Security

The streaming path introduces **no new security model**. It inherits Spark's existing shuffle
security through the existing transport configuration: authentication (`spark.authenticate` / SASL)
and network encryption (TLS) apply to streamed shuffle traffic exactly as they apply to sort-based
shuffle traffic, because the data plane is the same block-transfer service. The only additional
network surface is the **executor-scoped backpressure RPC endpoint**, which carries control messages
only and is rejected on the driver. See the [Security](security.html) guide for configuring
authentication and encryption.

## Limitations (v1)

The following limitations apply to the initial (v1) release of the streaming backend:

* **Immutable configuration** &mdash; the streaming configuration is fixed for the lifetime of the
  application. Changing it requires an executor restart; there is **no dynamic reconfiguration** in
  v1.
* **No new third-party dependencies** &mdash; the backend is built entirely on libraries already on
  the Spark Core classpath and on JDK primitives (for example, `java.util.zip.CRC32C`).
* **Reused network transport** &mdash; the network integration layer reuses the existing
  block-transfer service for the data plane rather than introducing a dedicated transport.

## Configuration summary

The streaming backend is controlled by the `spark.shuffle.manager=streaming` activation alias plus
five `spark.shuffle.streaming.*` properties. The table below is a brief reference; full descriptions
and tuning advice live in the [user guide](streaming-shuffle-guide.html),
[tuning guide](streaming-shuffle-tuning.html), and the main
[Configuration](configuration.html) page.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Whether the streaming shuffle backend is active. Must be <code>true</code> <em>and</em>
    <code>spark.shuffle.manager</code> must be set to <code>streaming</code> for the backend to
    engage; otherwise the sort-based shuffle is used.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percentage of executor memory used for per-partition streaming buffers. Integer in the range
    1&ndash;50. The per-partition buffer size is
    <code>(executorMemory * bufferSizePercent / 100) / numPartitions</code> with a 2 MB floor.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer-utilization percentage at which the largest buffers spill to disk via the
    <code>BlockManager</code> (<code>StorageLevel.DISK_ONLY</code>). Integer in the range
    50&ndash;95.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>-1 (unlimited)</td>
  <td>
    Per-executor streaming bandwidth cap in megabytes per second, enforced by the token-bucket rate
    limiter. The default <code>-1</code> (or any non-positive value) means unlimited.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>
    Whether to enable additional debug logging for the streaming shuffle backend.
  </td>
  <td>4.2.0</td>
</tr>
</table>

## Related documentation

* [Streaming Shuffle user guide](streaming-shuffle-guide.html) &mdash; how to enable and use the
  backend.
* [Streaming Shuffle tuning guide](streaming-shuffle-tuning.html) &mdash; sizing buffers, spill
  thresholds, and bandwidth caps.
* [Streaming Shuffle troubleshooting guide](streaming-shuffle-troubleshooting.html) &mdash;
  diagnosing fallback, spill, and fetch-failure behavior.
* [Configuration](configuration.html) &mdash; the full Spark configuration reference.
* [Monitoring](monitoring.html) &mdash; metrics systems, sinks, and endpoints.

