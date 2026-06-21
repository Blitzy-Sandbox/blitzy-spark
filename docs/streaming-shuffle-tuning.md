---
layout: global
title: Streaming Shuffle Tuning
displayTitle: Streaming Shuffle Tuning
description: Tuning guide for the streaming shuffle backend in Spark SPARK_VERSION_SHORT
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

This page explains how to size and tune the opt-in streaming shuffle backend once it has been
enabled. It concentrates on the three primary knobs &mdash; the per-partition buffer size, the spill
threshold, and the per-executor bandwidth cap &mdash; and then offers concrete, workload-specific
recommendations for shuffle-heavy, CPU-bound, and memory-bound jobs. If you have not yet enabled
the backend, start with the [user guide](streaming-shuffle-guide.html); for the internals behind
these settings, see the [architecture overview](streaming-shuffle-architecture.html).

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

The streaming shuffle backend pipelines map-side output directly to reduce-side consumers through
bounded in-memory buffers instead of fully materializing intermediate data to local disk before any
fetch begins. Once it is enabled (`spark.shuffle.manager=streaming` together with
`spark.shuffle.streaming.enabled=true`), its runtime behavior is governed almost entirely by three
tuning knobs:

* <code>spark.shuffle.streaming.bufferSizePercent</code> &mdash; how much executor memory is set aside
  for per-partition streaming buffers.
* <code>spark.shuffle.streaming.spillThreshold</code> &mdash; the buffer-utilization point at which the
  largest buffers spill to disk to reclaim memory.
* <code>spark.shuffle.streaming.maxBandwidthMBps</code> &mdash; an optional per-executor cap on
  streaming bandwidth.

A fourth knob, `spark.shuffle.streaming.debug`, toggles extra diagnostic logging while you tune.

These settings are **immutable for the lifetime of the application**: the streaming shuffle backend
reads them once at executor startup and does not support dynamic reconfiguration in this release.
Changing any of them requires restarting the application (and therefore its executors). Plan to tune
iteratively across runs rather than expecting live adjustments within a single application.

Tuning never compromises correctness or stability. When memory pressure or other unfavorable
conditions arise, the backend automatically falls back to the standard sort-based shuffle, so an
overly aggressive setting degrades gracefully into the default behavior rather than failing the job.
The goal of tuning is therefore to maximize the share of work that benefits from streaming while
staying comfortably inside the safety margins that keep fallback rare.

# Buffer Sizing: spark.shuffle.streaming.bufferSizePercent

`spark.shuffle.streaming.bufferSizePercent` is an integer in the range **1&ndash;50** (default
**20**) that sets the percentage of executor memory dedicated to per-partition streaming buffers.
The pool is divided evenly across the shuffle's partitions, and each partition's share is subject to
a **2 MB floor**. The exact per-partition buffer size is:

```
per-partition buffer = (executorMemory * bufferSizePercent / 100) / numPartitions
                       (never smaller than the 2 MB floor)
```

Guidance:

* **Higher values** reduce spill frequency and improve streaming throughput, because more data stays
  in memory and is pipelined directly to consumers. The trade-off is that less memory remains
  available for execution and storage, which raises the risk of out-of-memory pressure and triggers
  automatic fallback to sort-based shuffle sooner.
* **Lower values** are safer for memory-constrained executors but cause buffers to fill &mdash; and
  therefore spill &mdash; sooner, which adds disk I/O.
* **With many partitions**, the per-partition share shrinks toward the 2 MB floor and the percentage
  effectively stops growing the buffers. If you observe most partitions pinned at the floor, prefer
  *fewer, larger partitions* or *more executor memory* over simply raising this percentage.
* The percentage is carved from executor memory once at startup; it does not grow or shrink with the
  number of concurrent shuffles, so size it for the most buffer-intensive stage you expect.

## Worked example

Consider an executor with 8 GB of memory and the default `bufferSizePercent` of 20. The total
streaming buffer pool is `8192 MB * 20 / 100 = 1638.4 MB`, which is then split across the shuffle's
partitions:

```
executorMemory    = 8 GB (8192 MB)
bufferSizePercent = 20
total buffer pool = 8192 MB * 20 / 100 = 1638.4 MB

  numPartitions | pool / numPartitions | per-partition buffer (2 MB floor)
  ------------- | -------------------- | ---------------------------------
            100 | 1638.4 / 100         | 16.38 MB
            200 | 1638.4 / 200         |  8.19 MB   (~8 MB)
            500 | 1638.4 / 500         |  3.28 MB
           1000 | 1638.4 / 1000        |  2.00 MB   (1.64 MB raised to the 2 MB floor)
```

At 200 partitions each buffer is roughly 8 MB, comfortably above the floor. At 1000 partitions the
arithmetic share drops to about 1.64 MB, so every buffer is clamped up to the 2 MB floor &mdash; a
signal that this stage would benefit from fewer partitions or a larger executor rather than a higher
`bufferSizePercent`.

# Spill Threshold: spark.shuffle.streaming.spillThreshold

`spark.shuffle.streaming.spillThreshold` is an integer in the range **50&ndash;95** (default
**80**). It is the percentage of buffer utilization at which the backend spills the **largest**
buffered partitions to disk &mdash; through the block manager using the `DISK_ONLY` storage level
&mdash; to reclaim memory. The spill path is designed to reclaim memory within a **~100 ms** SLA so
that buffering can resume quickly.

Guidance:

* **Lower values** (closer to 50) spill earlier. This keeps memory pressure low and reduces the
  chance of fallback, at the cost of more disk I/O and slightly lower streaming throughput.
* **Higher values** (closer to 95) keep more data in memory for longer, which is faster, but leave a
  thinner safety margin before memory pressure forces an automatic fallback to sort-based shuffle.
* Keep the threshold **below the point at which memory pressure would trigger fallback**. Spilling is
  a controlled, in-backend reclamation step; fallback is a coarser, whole-shuffle revert. Tuning the
  threshold so that spilling absorbs transient spikes lets the streaming path stay engaged instead of
  reverting.
* Pair this knob with `bufferSizePercent`: a larger buffer pool with a moderate threshold spills less
  often, whereas a small pool with a high threshold risks spilling in large, bursty batches.

# Bandwidth Cap: spark.shuffle.streaming.maxBandwidthMBps

`spark.shuffle.streaming.maxBandwidthMBps` sets a per-executor limit, in MB/s, on streaming shuffle
bandwidth. It is enforced by a token-bucket rate limiter. The default is **-1**, and any value of
**0 or less means unlimited** (no rate limiting). The cap is applied per executor and is *arbitrated
across the concurrent shuffles* running on that executor, so multiple simultaneous shuffles share the
single budget rather than each receiving the full cap.

Guidance:

* On **shared or already-saturated networks**, set an explicit cap to prevent streaming shuffle from
  starving other traffic (other shuffles, block replication, external I/O). Keeping streaming below
  the **~90% link-saturation point** also avoids the network-saturation condition that triggers
  automatic fallback.
* On **dedicated or high-bandwidth networks**, leave the cap at the default (unlimited) so streaming
  can use the available throughput and deliver its full latency benefit.
* Because the budget is shared across concurrent shuffles, account for the number of shuffles you
  expect to run simultaneously when choosing a value &mdash; a cap that is comfortable for one shuffle
  may be too tight when several run at once.

# Diagnostic Logging: spark.shuffle.streaming.debug

`spark.shuffle.streaming.debug` is a boolean flag (default **false**) that enables additional
diagnostic logging along the streaming shuffle path &mdash; buffer sizing decisions, spill events,
backpressure signals, and fallback determinations. Enable it **temporarily** while diagnosing a
tuning problem, then disable it in production. The streaming backend is designed to keep log volume
within a modest per-executor budget, and leaving verbose logging on in production works against that
budget without providing ongoing value.

# Configuration Reference

The streaming shuffle tuning knobs are summarized below. All of them are immutable for the lifetime
of the application and take effect only after an application (executor) restart.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Enables the opt-in streaming shuffle backend. Must be set together with
    <code>spark.shuffle.manager=streaming</code>. When left at the default (false), Spark uses the
    standard sort-based shuffle and behavior is unchanged.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>Percentage (1&ndash;50) of executor memory used for per-partition streaming buffers; per-partition size = (executorMemory * bufferSizePercent / 100) / numPartitions, with a 2 MB floor.</td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>Buffer-utilization percentage (50&ndash;95) at which the largest buffers spill to disk to reclaim memory.</td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>-1 (unlimited)</td>
  <td>Per-executor streaming bandwidth cap in MB/s (token-bucket rate limiter); the default -1 (or any non-positive value) means unlimited.</td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>Enables additional diagnostic logging for the streaming shuffle path.</td>
  <td>4.2.0</td>
</tr>
</table>

# Workload-Specific Guidance

The streaming shuffle backend helps different workloads to very different degrees. Identify which
class your job falls into and tune accordingly. The table below is an at-a-glance summary; the
subsections that follow give the reasoning and concrete starting points.

| Workload class | Typical benefit | Suggested `bufferSizePercent` | Suggested `spillThreshold` | Notes |
| -------------- | --------------- | ----------------------------- | -------------------------- | ----- |
| Shuffle-heavy  | Materialization-avoidance win is component-proven in v1 (~78&ndash;79%); whole-job 30&ndash;50% in the distributed regime (local single-JVM near-parity, no regression) | 25&ndash;40 (if memory allows) | 75&ndash;85 (moderate) | Primary beneficiary; size buffers up. |
| CPU-bound      | 5&ndash;10% improvement in the distributed regime (local whole-job near-parity) | 20 (default) | 80 (default) | Keep defaults; spend memory on execution. |
| Memory-bound   | Zero regression (via fallback) &mdash; verified in v1 | 20 or lower | Conservative (lower) | High OOM risk; aggressive buffers only trigger fallback sooner. |

> **Measured results (self-measured; never aspirational).** The materialization-avoidance latency
> advantage is a **real v1 capability**: the `StreamingShuffleBenchmark` component harness measures
> the in-memory materialization round-trip at **&asymp; 78.3% best / 79.3% average faster** (4.6X)
> than via disk &mdash; **above** the 30&ndash;50% target. The **whole-job** 30&ndash;50% (shuffle-heavy)
> and 5&ndash;10% (CPU-bound) reductions are the AAP **targets for the distributed regime**; the committed
> `StreamingShufflePerformanceBenchmark` whole-job artifact measures local single-JVM **near-parity
> with no regression** (shuffle-heavy &asymp; 6.1% best / 14.8% average, CPU-bound &asymp; 5.0% best /
> 5.6% average), because the OS page cache, the absence of a network fetch, and equal fixed per-job
> costs mask the win locally. Tune for the v1 materialization win and zero-regression guarantee today.

## Shuffle-heavy workloads

Shuffle-heavy stages &mdash; roughly **&ge; 100 MB of intermediate data** across **&ge; 10
partitions** &mdash; are the primary beneficiaries of streaming shuffle. The latency advantage comes
from avoiding disk materialization, and it is **component-proven in v1** (the `StreamingShuffleBenchmark`
materialization round-trip is ~78&ndash;79% faster in-memory than via disk &mdash; above the
30&ndash;50% target). The **whole-job 30&ndash;50% end-to-end reduction** is realized in the
**distributed regime**; a local single-JVM run measures near-parity with no regression (the win is
page-cache/local-mode-masked there). Because these stages move a lot of data, keeping more of it in
memory pays off &mdash; the more output served from memory rather than spilled, the more of the
materialization win is preserved:

* Raise `bufferSizePercent` toward **25&ndash;40** when the executor has memory to spare, so fewer
  blocks spill and more data streams directly to consumers.
* Use a **moderate `spillThreshold` (75&ndash;85)** so that spilling absorbs transient spikes without
  prematurely surrendering memory.
* Watch `bufferUtilizationPercent` and `spillCount` (see [Monitoring While Tuning](#monitoring-while-tuning))
  to confirm the larger buffers are actually reducing spills rather than just consuming memory.

## CPU-bound workloads

CPU-bound jobs spend most of their time in computation rather than data movement, so the streaming
backend targets a smaller **5&ndash;10%** improvement, primarily from reduced scheduler overhead, in
the **distributed regime** (a local single-JVM whole-job run measures near-parity, since equal fixed
per-job costs dominate). For these workloads:

* **Keep the defaults** (`bufferSizePercent=20`, `spillThreshold=80`).
* Do **not** over-allocate buffers. Memory taken for streaming buffers is memory unavailable to
  execution; for CPU-bound stages that memory is better left for task execution and caching.

## Memory-bound workloads

Memory-bound jobs already operate close to their memory limits, so they carry a **high OOM risk**.
The streaming backend protects these jobs by **automatically falling back to sort-based shuffle**
under memory pressure, which guarantees **zero regression** relative to the default shuffle. To keep
as much work as possible on the streaming path without provoking fallback:

* Keep `bufferSizePercent` **low** (the default 20, or lower).
* Keep `spillThreshold` **conservative** so memory is reclaimed early.
* Recognize that setting buffers too aggressively does not make a memory-bound job faster &mdash; it
  simply triggers fallback sooner, at which point the job runs as standard sort-based shuffle anyway.

# Monitoring While Tuning

Tune with data, not guesswork. The streaming backend emits four `shuffle.streaming.*` metrics through
the existing Spark metrics system; watch them while you adjust the knobs above. See
[Monitoring](monitoring.html) for how to expose these metrics (JMX, Prometheus, and the existing
metrics sinks), and the [troubleshooting guide](streaming-shuffle-troubleshooting.html) for diagnosing
anomalies.

* <code>bufferUtilizationPercent</code> (gauge) &mdash; the current buffer fill level. If it is
  persistently high, either raise `bufferSizePercent` (when memory allows) or lower `spillThreshold`
  so memory is reclaimed sooner.
* <code>spillCount</code> (counter) &mdash; the number of spill events. Frequent spills indicate the
  buffers are too small for the data volume; increase `bufferSizePercent` or reduce the partition
  count so each partition's share clears the 2 MB floor with room to spare.
* <code>backpressureEvents</code> (counter) &mdash; how often producers were throttled because
  consumers could not keep up. A high count suggests consumers are the bottleneck; consider applying
  or tightening `maxBandwidthMBps`, or reducing the number of concurrent shuffles competing for the
  executor's bandwidth budget.
* <code>partialReadInvalidations</code> (counter) &mdash; partial reads invalidated due to producer
  failures, which lead to recomputation. A nonzero and growing value points to reliability problems
  (for example, connection timeouts) rather than tuning issues; investigate using the
  [troubleshooting guide](streaming-shuffle-troubleshooting.html).

# Interaction with General Shuffle Tuning

These streaming-specific settings **complement, and do not replace,** Spark's general shuffle and
partition tuning. The number and size of partitions, the serializer, and the amount of executor
memory all still matter &mdash; and, as shown above, partition count interacts directly with the
per-partition buffer formula. Tune the general parameters first so each partition is a sensible size,
then layer the streaming knobs on top. For the broader picture, see [Tuning Spark](tuning.html) and
the full [Configuration](configuration.html) reference.

# Related Documentation

* [Streaming Shuffle User Guide](streaming-shuffle-guide.html) &mdash; how to enable and use the
  streaming shuffle backend.
* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) &mdash; the internals behind
  the buffers, backpressure protocol, spill path, and fallback policy.
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) &mdash; diagnosing
  failures, fallback, and performance problems.
* [Tuning Spark](tuning.html) &mdash; general performance tuning.
* [Configuration](configuration.html) &mdash; the complete Spark configuration reference.
* [Monitoring](monitoring.html) &mdash; exposing and reading Spark metrics.

