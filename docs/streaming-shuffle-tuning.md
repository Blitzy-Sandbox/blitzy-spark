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

This page explains how to tune the opt-in streaming shuffle backend once it has been enabled. It
concentrates on the three primary tuning knobs &mdash; the buffer size, the spill threshold, and the
per-executor bandwidth cap &mdash; and offers workload-specific guidance for shuffle-heavy, CPU-bound,
and memory-bound jobs. If you have not enabled the streaming shuffle backend yet, start with the
[user guide](streaming-shuffle-guide.html); for background on how the backend pipelines data, see the
[architecture overview](streaming-shuffle-architecture.html).

* This will become a table of contents (this text will be scraped).
{:toc}

## Overview

The streaming shuffle backend pipelines map-side output directly to reduce-side consumers through
bounded in-memory buffers, falling back automatically to the sort-based shuffle when conditions are
unfavorable. Once the backend is enabled, the three settings below control how much memory it uses,
when it spills to disk, and how fast it transmits on the network. They are the primary levers for
trading memory for throughput and for keeping the backend inside its operating envelope:

* <code>spark.shuffle.streaming.bufferSizePercent</code> &mdash; the share of executor memory reserved
  for per-partition streaming buffers.
* <code>spark.shuffle.streaming.spillThreshold</code> &mdash; the buffer-utilization point at which the
  largest buffers spill to disk.
* <code>spark.shuffle.streaming.maxBandwidthMBps</code> &mdash; the per-executor streaming bandwidth cap.

These streaming-shuffle settings take effect only when the backend is actually active &mdash; that is,
when both <code>spark.shuffle.manager=streaming</code> and
<code>spark.shuffle.streaming.enabled=true</code>. Otherwise the manager transparently delegates to the
sort-based shuffle and these knobs have no effect. See the
[user guide](streaming-shuffle-guide.html) for how to enable the backend.

**Configuration is immutable for the lifetime of the application.** All streaming-shuffle settings are
read once at startup; the backend does not support dynamic reconfiguration in this release. Changing any
value requires restarting the application (and therefore its executors). Plan to tune iteratively across
runs: adjust a setting, restart, observe the
[metrics](#monitoring-while-tuning), and repeat.

## Buffer Sizing: `spark.shuffle.streaming.bufferSizePercent`

`spark.shuffle.streaming.bufferSizePercent` is the percentage of executor memory dedicated to the
streaming shuffle's per-partition in-memory buffers. It is an integer in the range 1&ndash;50 and
defaults to `20` (that is, 20% of executor memory). The amount of memory each partition's buffer
receives is derived from this percentage and the number of shuffle partitions:

```
per-partition buffer = (executorMemory * bufferSizePercent / 100) / numPartitions
```

A hard floor of **2 MB per partition** is always applied: if the formula yields less than 2 MB, the
buffer is raised to 2 MB. The pool is divided evenly across partitions, so the per-partition share
shrinks as the partition count grows.

Use the following guidance when choosing a value:

* **Higher values** reduce how often buffers fill and spill, improving streaming throughput, but leave
  less memory for execution and storage. Pushing this too high raises the risk of memory pressure, which
  in turn increases the chance the backend trips its automatic fallback to the sort-based shuffle.
* **Lower values** are safer on memory-constrained executors but cause buffers to fill and spill sooner,
  adding disk I/O.
* **With many partitions**, the per-partition share shrinks toward the 2 MB floor and most of the
  allocated pool is consumed by the floor rather than by the percentage. If you observe this, prefer
  fewer, larger partitions (for example, by tuning your shuffle partition count) or provision more
  executor memory, rather than simply raising `bufferSizePercent`.

### Worked example

Consider an executor with 8 GB (8192 MB) of memory and the default `bufferSizePercent` of `20`. The
total streaming buffer pool is `8192 * 20 / 100 = 1638.4` MB, which is then divided across the shuffle
partitions. The table below shows the per-partition buffer for several partition counts; the final
column shows the effective size after the 2 MB floor is applied:

```
executorMemory = 8192 MB,  bufferSizePercent = 20  =>  pool = 1638.4 MB

  numPartitions   computed per-partition   effective (>= 2 MB floor)
  -------------   ----------------------   -------------------------
            200          ~8.19 MB                    ~8.19 MB
            400          ~4.10 MB                    ~4.10 MB
            800          ~2.05 MB                    ~2.05 MB
           1000          ~1.64 MB                     2.00 MB  (floored)
```

At 200 partitions each buffer is roughly 8 MB. By 1000 partitions the computed share (~1.6 MB) drops
below the 2 MB floor and is raised to 2 MB, so additional partitions no longer shrink the per-partition
buffer &mdash; they only increase total memory demand. This is the point at which consolidating
partitions or adding executor memory is more effective than increasing the percentage.

## Spill Threshold: `spark.shuffle.streaming.spillThreshold`

`spark.shuffle.streaming.spillThreshold` is the buffer-utilization percentage at which the streaming
shuffle begins spilling its largest buffered partitions to disk in order to reclaim memory. It is an
integer in the range 50&ndash;95 and defaults to `80` (that is, spill once buffers reach 80% of the pool
configured by `bufferSizePercent`). Spilling is performed through the existing block manager using the
`DISK_ONLY` storage level, and the backend targets reclaiming the needed memory within a roughly 100 ms
service-level objective.

Use the following guidance when choosing a value:

* **Lower values** spill earlier, keeping memory pressure low at the cost of more frequent disk I/O.
  Choose a lower threshold when executors are memory-constrained or when you see the backend approaching
  fallback under load.
* **Higher values** keep more data resident in memory, which is faster, but operate closer to the point
  where memory pressure can trigger the automatic fallback to the sort-based shuffle.
* **Keep the threshold below the fallback memory-pressure point.** The backend reverts to sort-based
  shuffle when memory pressure prevents buffer allocation; a spill threshold that is too aggressive
  (too high) gives the spill manager too little headroom to reclaim memory before that point is reached.
  A moderate threshold leaves room for the ~100 ms reclamation to take effect.

Spilled data remains usable by the streaming path: spilled and streamed bytes share a common format, so
spilling relieves memory pressure without invalidating in-progress reads.

## Bandwidth Cap: `spark.shuffle.streaming.maxBandwidthMBps`

`spark.shuffle.streaming.maxBandwidthMBps` caps the per-executor streaming shuffle bandwidth, expressed
in megabytes per second and enforced by a token-bucket rate limiter. It defaults to `0`, which &mdash;
like any non-positive value &mdash; means **unlimited** (no rate limiting is applied). The cap is a
per-executor budget that is arbitrated across all concurrent shuffles running on that executor, so
adding more concurrent shuffles divides the same budget rather than multiplying it.

Use the following guidance when choosing a value:

* **Set a cap on shared or saturated networks.** Limiting streaming bandwidth prevents the shuffle from
  starving other traffic (for example, other applications, replication, or external I/O) and helps keep
  link utilization below the ~90% saturation point. Sustained network saturation is one of the
  conditions that causes the backend to fall back to the sort-based shuffle, so a sensible cap can keep
  the streaming path engaged.
* **Leave it unlimited on dedicated or fast networks.** When the network is not a bottleneck, a cap only
  adds overhead and throttles throughput needlessly; the default of `0` (unlimited) is appropriate.
* **Account for concurrency.** Because the cap is shared across concurrent shuffles on the executor,
  size it for the expected peak number of simultaneous shuffles, not for a single shuffle in isolation.

## Diagnostic Logging: `spark.shuffle.streaming.debug`

`spark.shuffle.streaming.debug` is a boolean flag (default `false`) that enables additional diagnostic
logging along the streaming shuffle path &mdash; buffer allocation and spill decisions, backpressure
events, and per-block transfer details. Enable it **temporarily** while tuning or diagnosing a specific
issue, then disable it in production. The streaming backend is designed to keep log volume within a
modest per-executor budget; leaving `debug` on in production defeats that budget and can itself add I/O
overhead. For routine, low-overhead visibility, rely on the streaming metrics described under
[Monitoring while tuning](#monitoring-while-tuning) instead of debug logging.

## Configuration Reference

The table below summarizes the streaming shuffle tuning properties. The activation flag
<code>spark.shuffle.streaming.enabled</code> is included for completeness; see the
[user guide](streaming-shuffle-guide.html) for how to enable the backend and the
[Configuration](configuration.html) page for the full Spark property reference. Remember that all of
these properties are immutable for the lifetime of the application and require a restart to change.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Enables the streaming shuffle backend. Takes effect only when
    <code>spark.shuffle.manager</code> is also set to <code>streaming</code>; otherwise the manager
    delegates to the sort-based shuffle. Opt-in; the default behavior is unchanged.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>Percentage (1&ndash;50) of executor memory used for per-partition streaming buffers; per-partition size = (executorMemory * bufferSizePercent / 100) / numPartitions, with a 2 MB floor.</td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>Buffer-utilization percentage (50&ndash;95) at which the largest buffers spill to disk to reclaim memory.</td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0 (unlimited)</td>
  <td>Per-executor streaming bandwidth cap in MB/s (token-bucket rate limiter); 0 or non-positive means unlimited.</td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>Enables additional diagnostic logging for the streaming shuffle path.</td>
  <td>4.1.0</td>
</tr>
</table>


## Workload-Specific Guidance

The right settings depend heavily on the shape of the workload. The table below summarizes starting
points for the three broad classes; the subsections that follow explain the reasoning. Treat these as
starting points and refine them using the [metrics](#monitoring-while-tuning).

| Workload class | Expected benefit | `bufferSizePercent` | `spillThreshold` | Notes |
|----------------|------------------|---------------------|------------------|-------|
| Shuffle-heavy (>= ~100 MB intermediate data, >= ~10 partitions) | 30&ndash;50% latency reduction | 25&ndash;40 (if memory allows) | 75&ndash;85 | Primary beneficiary; favor larger buffers and a moderate spill threshold |
| CPU-bound | 5&ndash;10% improvement | 20 (default) | 80 (default) | Keep defaults; spend memory on execution, not buffers |
| Memory-bound | ~0% (zero regression via fallback) | 20 or lower | conservative (lower) | High OOM risk; aggressive buffers only trigger fallback sooner |

### Shuffle-heavy workloads

Workloads that move large amounts of intermediate data across many partitions (roughly 100 MB or more
of shuffle data and at least about 10 partitions) are the primary beneficiaries of the streaming
backend, where it can deliver an end-to-end latency reduction in the 30&ndash;50% range. When executor
memory allows, favor a larger `bufferSizePercent` (for example, 25&ndash;40) to keep more data streaming
in memory and reduce spill frequency, paired with a moderate `spillThreshold` (for example, 75&ndash;85)
so the spill manager has headroom to reclaim memory before the backend approaches fallback. Verify the
chosen buffer size against the [worked example](#worked-example) so the per-partition share stays
comfortably above the 2 MB floor.

### CPU-bound workloads

Workloads dominated by computation rather than data movement see a smaller benefit, typically in the
5&ndash;10% range, coming mostly from reduced scheduler overhead. For these jobs, keep the defaults
(`bufferSizePercent = 20`, `spillThreshold = 80`). Do not over-allocate buffers: memory taken by
streaming buffers is memory unavailable to execution, and on CPU-bound jobs that trade rarely pays off.
Spend additional memory on execution and caching instead.

### Memory-bound workloads

Workloads that are already close to their memory limits carry a high risk of out-of-memory errors if
buffers are sized aggressively. Keep `bufferSizePercent` low (the default of `20`, or lower) and keep
`spillThreshold` conservative. Crucially, the streaming backend **automatically falls back to the
sort-based shuffle under memory pressure**, so memory-bound jobs are protected from regression: in the
worst case they run on the existing sort-based path with no loss of correctness or performance. Setting
buffers too aggressively on these workloads does not improve throughput &mdash; it simply trips the
fallback sooner, wasting the memory reserved for buffers. The safe default posture for memory-bound jobs
is therefore conservative settings combined with reliance on the automatic fallback.

## Monitoring While Tuning

Tuning is an iterative, metric-driven process: adjust a setting, restart the application (settings are
immutable for the application lifetime), and observe the effect. The streaming backend exposes four
metrics under the `shuffle.streaming.*` namespace through the standard Spark metrics system. Watch them
to guide each adjustment:

* **`bufferUtilizationPercent`** (gauge) &mdash; the current buffer fill level. If this sits
  persistently high, buffers are under pressure: raise `bufferSizePercent` (if memory allows) or lower
  `spillThreshold` so the backend reclaims memory earlier.
* **`spillCount`** (counter) &mdash; the number of spill events. Frequent spills indicate buffers are
  filling too often; increase `bufferSizePercent` or reduce the partition count so each partition's
  buffer is larger.
* **`backpressureEvents`** (counter) &mdash; how often producers were throttled because consumers could
  not keep up. A high count means the consumer side is the bottleneck; consider setting or lowering a
  `maxBandwidthMBps` cap, or reducing the number of concurrent shuffles competing for the executor's
  bandwidth budget.
* **`partialReadInvalidations`** (counter) &mdash; partial reads invalidated due to producer failures
  and subsequent recomputes. A rising count points to instability (failures or timeouts) rather than a
  tuning problem; investigate using the [troubleshooting guide](streaming-shuffle-troubleshooting.html).

These metrics are surfaced through the existing Spark metrics endpoints (for example, JMX and the
Prometheus endpoint). See [Monitoring](monitoring.html) for how to access them and the
[troubleshooting guide](streaming-shuffle-troubleshooting.html) for diagnosing anomalies.

## Interaction with General Shuffle Tuning

The streaming-shuffle settings on this page **complement, and do not replace, general shuffle and
partition tuning.** The number and size of shuffle partitions, serialization, compression, and the other
shuffle properties continue to apply and directly affect the per-partition buffer math described above
(fewer, larger partitions raise the per-partition share; many small partitions push it toward the 2 MB
floor). Tune the general shuffle settings first so partitions are reasonably sized, then apply the
streaming-specific knobs on top. See [Tuning Spark](tuning.html) for general performance guidance and
[Configuration](configuration.html) for the complete list of shuffle properties.

## Related Documentation

* [Streaming Shuffle User Guide](streaming-shuffle-guide.html) &mdash; how to enable and use the backend.
* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) &mdash; how the backend pipelines, buffers, and spills data.
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) &mdash; diagnosing failures, fallback, and performance issues.
* [Tuning Spark](tuning.html) &mdash; general Spark performance and memory tuning.
* [Configuration](configuration.html) &mdash; the complete Spark configuration reference.
* [Monitoring](monitoring.html) &mdash; accessing metrics, including the `shuffle.streaming.*` metrics.

