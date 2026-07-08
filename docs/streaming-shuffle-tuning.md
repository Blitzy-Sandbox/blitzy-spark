---
layout: global
displayTitle: Streaming Shuffle Tuning
title: Streaming Shuffle Tuning
description: Tuning the opt-in streaming shuffle backend in Apache Spark SPARK_VERSION_SHORT
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
* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

This page is a practical tuning guide for Spark's opt-in **streaming shuffle** backend. Streaming
shuffle pipelines map-output data directly to reduce tasks through bounded in-memory buffers instead
of first materializing shuffle files to local disk, which can reduce end-to-end latency for
shuffle-heavy workloads. For how to *enable* the backend (the dual activation gate of
`spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`), see the
[Streaming Shuffle Guide](streaming-shuffle-guide.html). For the authoritative property definitions,
see [Configuration &#8594; Shuffle Behavior](configuration.html#shuffle-behavior).

Streaming shuffle exposes five `spark.shuffle.streaming.*` keys, all available **since version
4.2.0**. Of these, three are true tuning knobs — `bufferSizePercent`, `spillThreshold`, and
`maxBandwidthMBps` — while `enabled` is the opt-in gate and `debug` is a diagnostics switch.

<table class="spark-config">
<thead><tr><th>Property</th><th>Default</th><th>Valid range</th><th>Role in tuning</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td><code>false</code></td>
  <td><code>true</code> / <code>false</code></td>
  <td>Master opt-in gate (paired with <code>spark.shuffle.manager=streaming</code>); not a sizing knob.</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td><code>20</code></td>
  <td><code>[1, 50]</code></td>
  <td>Percent of executor memory budgeted for per-partition streaming buffers.</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td><code>80</code></td>
  <td><code>[50, 95]</code></td>
  <td>Buffer-utilization percent at which buffered partitions spill to disk.</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td><code>0</code> (unlimited)</td>
  <td><code>0</code> or a positive integer</td>
  <td>Per-executor streaming rate limit, in MB/s.</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td><code>false</code></td>
  <td><code>true</code> / <code>false</code></td>
  <td>Diagnostics only; elevates the streaming logger to <code>DEBUG</code>.</td>
</tr>
</table>

**Configuration is immutable for the lifetime of the application.** There is no dynamic
reconfiguration in this version, so changing any `spark.shuffle.streaming.*` value requires an
**executor restart** to take effect. Treat tuning as an offline, iterative loop: change a value,
restart the executors, and re-measure (see [A tuning workflow](#a-tuning-workflow) below).

Every recommendation on this page is tied back to an observable metric in the `streamingShuffle`
namespace so the guidance is actionable — see the [Monitoring](monitoring.html) guide for the full
metric list and the [structured-logging](monitoring.html) schema.

# Buffer sizing (`spark.shuffle.streaming.bufferSizePercent`)

`spark.shuffle.streaming.bufferSizePercent` is an integer percent in the range `[1, 50]` with a
default of `20`. It controls how much of an executor's memory is budgeted for per-partition
streaming buffers. The per-partition buffer is sized approximately by the formula:

`(executorMemory × bufferPercent) / numPartitions`

In practice the percent is applied as a fraction of executor memory (i.e. `bufferPercent / 100`), and
each per-partition buffer is subject to a **2&nbsp;MB floor** — no partition's buffer drops below
2&nbsp;MB even when the arithmetic would produce a smaller value. Because the memory budget is
divided across partitions, **a large partition count shrinks each per-partition buffer**, which is
exactly the situation the 2&nbsp;MB floor protects against.

**Guidance.**

* **Raise** `bufferSizePercent` when you observe frequent spilling — a rising `spillCount` counter
  and a persistently high `bufferUtilizationPercent` gauge — **and** you have spare executor memory
  to give. Larger buffers hold more in-flight data, reduce spill frequency, and keep more of the
  shuffle on the fast in-memory path.
* **Lower** `bufferSizePercent` if streaming buffers are crowding out execution or storage memory
  (for example, you see increased GC pressure or execution-memory spilling elsewhere). Streaming
  buffers draw from the same executor memory pool, so an over-large budget can starve the rest of
  the task.
* Keep the partition count in mind: the more partitions a shuffle has, the smaller each buffer
  becomes for a given `bufferSizePercent`, so very wide shuffles may need a higher percent (or fewer
  partitions) to avoid running every buffer at the 2&nbsp;MB floor.

# Spill threshold (`spark.shuffle.streaming.spillThreshold`)

`spark.shuffle.streaming.spillThreshold` is an integer percent in the range `[50, 95]` with a
default of `80`. When overall buffer utilization exceeds this threshold, the streaming backend spills
the **largest / least-recently-used** buffered partitions to disk using the `DISK_ONLY` storage
level, relieving memory pressure while **continuing to stream**.

## Spilling is not the same as fallback

This distinction is the single most common source of confusion, so make it unmistakable:

* **Spilling** happens at `spillThreshold` (default `80`). The largest / least-recently-used
  partitions are written to disk to free memory, but **the shuffle remains on the streaming path**.
  Spilling is simply streaming operating under memory pressure. It shows up as a rising `spillCount`
  counter.
* **Fallback** is a separate, higher memory-pressure condition — buffer utilization approaching
  **~95%** with a genuine risk of out-of-memory — at which the shuffle **abandons streaming
  entirely and reverts to the default sort-based shuffle** for that shuffle. Fallback is a full
  switch back to sort-based shuffle, not a spill.

In short: **spilling is streaming under memory pressure; fallback is a full switch back to
sort-based shuffle.** Memory-pressure fallback is one of several automatic fallback conditions; the
complete list is documented in the [Streaming Shuffle Guide](streaming-shuffle-guide.html).

**Guidance.** Tune `spillThreshold` **down** (toward `50`) to spill **earlier and more
conservatively**, trading more disk I/O for a wider safety margin before the ~95% fallback point.
Tune it **up** (toward `95`) to spill **later**, keeping more data in memory at the cost of a
smaller margin. If you see fallback occurring under memory pressure and want to stay on the streaming
path, lowering `spillThreshold` (so spilling relieves pressure sooner) is usually more effective than
raising it.

# Bandwidth and rate limiting (`spark.shuffle.streaming.maxBandwidthMBps`)

`spark.shuffle.streaming.maxBandwidthMBps` is an integer rate limit in MB/s. The default `0` means
**unlimited** (no rate limiting). When set to a positive value, the streaming backend applies
token-bucket rate limiting, and the effective per-shuffle refill rate follows:

`Refill rate = maxBandwidthMBps / numConcurrentShuffles`

That is, the configured per-executor budget is divided among the shuffles running concurrently on the
executor, so no single shuffle can monopolize the link. Rate limiting also applies a **link-capacity
safety factor of roughly 80%** of capacity, leaving headroom so streaming traffic does not fully
saturate the network.

**Guidance.**

* Set a **positive** `maxBandwidthMBps` on **shared or constrained networks** to prevent streaming
  shuffle from saturating links that other workloads (or other executors) depend on. Aggressive rate
  limiting shows up as a rising `backpressureEvents` counter, which indicates the token bucket is
  throttling producers.
* Leave `maxBandwidthMBps` at `0` (unlimited) when the **network is not the bottleneck** — for
  example on a dedicated or high-bandwidth interconnect — so streaming can pipeline data as fast as
  producers and consumers allow.

# Block size (fixed at 2 MB)

Streaming frames data into blocks of **at most 2&nbsp;MB** for pipelining efficiency, and each block
is protected by a **CRC32C** checksum for corruption detection and retransmission. The block size is
an **internal constant and is not user-configurable**. It is documented here only so operators can
reason about behavior: **smaller blocks improve pipelining latency** (a consumer can begin working on
a completed block sooner) while **bounding the cost of retransmission** — if a block fails its CRC32C
check, only that single ≤2&nbsp;MB block must be resent rather than a large span of data.

# Timeouts, heartbeats, and retries

The following operational values are **fixed internal constants** — they are **not user-configurable**
in this version — and are documented so operators can interpret streaming-shuffle behavior:

* **Producer connection timeout: 5 seconds.** If a consumer cannot reach a producer within
  5&nbsp;seconds, the reader atomically invalidates the partial read and triggers upstream
  recomputation through the standard `FetchFailedException` path (the same fault path the sort-based
  shuffle uses). Each such event increments the `partialReadInvalidations` counter.
* **Consumer heartbeat interval / liveness: 10 seconds.** Consumers signal liveness on a
  10&nbsp;second cadence so producers can detect stalled or dead consumers.
* **Backpressure scan interval: 1 second.** The backpressure protocol scans flow-control state once
  per second.
* **Memory-spill polling and acknowledgment-driven reclamation: 100&nbsp;ms.** Buffer utilization is
  polled every 100&nbsp;ms, and buffer memory is reclaimed within **100&nbsp;ms** of a consumer
  acknowledgment.
* **Retry policy: exponential backoff starting at 1 second, with a maximum of 5 attempts.** Transient
  transfer failures are retried with exponential backoff (first retry after ~1&nbsp;second) up to 5
  attempts before the read is treated as failed.
* **TCP keepalive: enabled at a 5-second interval** to keep idle producer/consumer connections alive
  and detect broken links promptly.

# Debug logging (`spark.shuffle.streaming.debug`)

`spark.shuffle.streaming.debug` is a boolean with a default of `false`. Enable it **only for
diagnostics**: it elevates the streaming-shuffle logger to `DEBUG` and substantially increases log
volume. Per-executor streaming log volume is designed to stay under **~10&nbsp;MB/hour**, but verbose
debug logging will push toward that ceiling, so leave it off in steady-state production. When you do
enable it, correlate the emitted log lines using the MDC keys `shuffle_id`, `map_id`,
`reduce_partition_range`, and `attempt_id`. For a symptom-driven walkthrough of interpreting these
logs and metrics, see the
[Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) guide.

# A tuning workflow

Because configuration is immutable at runtime, tuning is an **offline, iterative loop**. Repeat the
following until the workload meets its latency and stability goals:

1. **Observe.** Collect the `streamingShuffle` metrics through Spark's `MetricsSystem` (JMX,
   Prometheus, CSV, or Slf4j sinks, or the `/metrics/executors/prometheus` endpoint) — see
   [Monitoring](monitoring.html). The four metrics are `bufferUtilizationPercent`, `spillCount`,
   `backpressureEvents`, and `partialReadInvalidations`.
2. **Adjust** one knob at a time, guided by the [metric-to-setting map](#metric-to-setting-map)
   below.
3. **Restart executors** so the new `spark.shuffle.streaming.*` values take effect (they are immutable
   for the application lifetime).
4. **Re-measure** the same metrics and end-to-end latency, and iterate.

Telemetry overhead for these metrics is designed to stay **under 1% CPU**, so you can keep them
enabled continuously in production to support this loop.

## Metric-to-setting map

Use the observed `streamingShuffle` metric to decide which knob to change and in which direction:

| Symptom (metric) | Setting to adjust | Direction |
| --- | --- | --- |
| High `bufferUtilizationPercent`, rising `spillCount`, spare memory available | `bufferSizePercent` | Increase (up to `50`) |
| Streaming buffers crowding out execution/storage memory | `bufferSizePercent` | Decrease (down to `1`) |
| Frequent fallback under memory pressure; want to spill sooner | `spillThreshold` | Decrease (toward `50`) |
| Plenty of memory headroom; want to keep more data in memory | `spillThreshold` | Increase (toward `95`) |
| Network saturation; high `backpressureEvents` on a shared link | `maxBandwidthMBps` | Set a positive limit |
| Network is not the bottleneck | `maxBandwidthMBps` | Leave `0` (unlimited) |

# Related pages

See also:

* [Streaming Shuffle Guide](streaming-shuffle-guide.html) — enabling the backend and the dual
  activation gate.
* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) — design and internals.
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) — diagnosing metrics
  and logs.
* [Configuration &#8594; Shuffle Behavior](configuration.html#shuffle-behavior) — the canonical
  property reference.
* [Monitoring](monitoring.html) — metrics and structured-logging details.
