---
layout: global
displayTitle: Streaming Shuffle Guide
title: Streaming Shuffle Guide
description: How to enable and use the opt-in streaming shuffle backend in Apache Spark SPARK_VERSION_SHORT
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

Streaming shuffle is an **opt-in** shuffle backend that pipelines map-output data directly to
reduce tasks through bounded in-memory buffers, instead of first materializing shuffle files to
local disk. By overlapping production and consumption, it removes the shuffle write/read round trip
from the common path and can reduce end-to-end shuffle latency for shuffle-heavy workloads. It
**coexists with** the default sort-based shuffle, which remains the production-stable path and the
automatic fallback: whenever streaming is not enabled, or whenever a fallback condition is detected
at runtime, Spark transparently uses sort-based shuffle, so there is no behavior change unless you
explicitly opt in. For the design and internals, see the
[Streaming Shuffle Architecture](streaming-shuffle-architecture.html) guide.

# Enabling Streaming Shuffle

Streaming shuffle is guarded by a **dual activation gate**: it is active **if and only if both** of
the following properties are set. This is the single most important thing to get right.

* `spark.shuffle.manager=streaming`
* `spark.shuffle.streaming.enabled=true`

If **only one** of the two is set, Spark silently uses the standard sort-based shuffle — there is no
error and no streaming. Requiring both properties is a deliberate, defense-in-depth opt-in that
prevents streaming shuffle from being enabled by accident (for example, by setting the manager alias
alone, or by flipping the `enabled` flag while another shuffle manager is in effect).

## Enabling from the command line

Pass both properties to `spark-submit` (or `spark-shell` / `pyspark`) with `--conf`:

```
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  ...
```

## Enabling in spark-defaults.conf

Alternatively, set both properties in `conf/spark-defaults.conf` so they apply to every application
submitted from that client:

```
spark.shuffle.manager           streaming
spark.shuffle.streaming.enabled true
```

## Configuration is immutable at runtime

**Configuration changes require an executor restart.** There is no dynamic reconfiguration in this
version: every `spark.shuffle.streaming.*` property is read once at executor startup and is
immutable for the lifetime of the application. To change any streaming-shuffle setting, update the
configuration and start a new application (or restart the executors).

# Configuration Reference

Streaming shuffle is controlled by the five properties below. All of them are available **since
version 4.2.0** and are only used when the [dual activation gate](#enabling-streaming-shuffle) is
satisfied. These values are the authoritative reference for streaming shuffle; the same keys also
appear in the canonical [Shuffle Behavior](configuration.html#shuffle-behavior) table.

| Property | Default | Meaning | Since |
| --- | --- | --- | --- |
| `spark.shuffle.streaming.enabled` | `false` | Master opt-in flag for the streaming shuffle backend. Takes effect **only** when combined with `spark.shuffle.manager=streaming` (the dual activation gate). When it is `false`, or the manager is not `streaming`, Spark uses the default sort-based shuffle. | 4.2.0 |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | Percent of executor memory used for per-partition streaming buffers, as an integer in the range `[1, 50]`. Each partition's buffer is sized approximately as `(executorMemory * bufferSizePercent / 100) / numPartitions`. | 4.2.0 |
| `spark.shuffle.streaming.spillThreshold` | `80` | Buffer-utilization percent, as an integer in the range `[50, 95]`, at which the largest / least-recently-used buffered partitions are spilled to disk (`DISK_ONLY`) to relieve memory pressure. Spilling keeps the shuffle on the streaming path; it is **not** a fallback to sort-based shuffle. | 4.2.0 |
| `spark.shuffle.streaming.maxBandwidthMBps` | `0` | Per-executor streaming rate limit in MB/s. The default `0` means **unlimited**. When set to a positive value, the effective per-shuffle refill rate is `maxBandwidthMBps / numConcurrentShuffles`. | 4.2.0 |
| `spark.shuffle.streaming.debug` | `false` | Elevates the streaming-shuffle logger to `DEBUG`. Disabled by default; enable only for diagnostics, as it increases log volume. | 4.2.0 |

For deeper sizing guidance on `bufferSizePercent`, `spillThreshold`, and `maxBandwidthMBps`, see the
[Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide.

# When to Use It

Streaming shuffle is designed for **shuffle-heavy** workloads — those that move a large volume of
intermediate data between stages, for example **100&nbsp;MB+** of shuffle data per stage across
**10 or more partitions**. For these workloads the design targets a **30–50% end-to-end latency
reduction**. CPU-bound workloads, where shuffle is not the bottleneck, see more modest gains in the
range of **5–10%**, primarily from reduced scheduler overhead.

These figures are **design targets, not guarantees** — realized improvement depends on data volume,
partition count, cluster hardware, and network conditions. Workloads that are memory-bound, or whose
shuffles are small, are unlikely to benefit and will typically stay on (or automatically fall back
to) the sort-based path. When in doubt, benchmark your own workload with and without streaming
shuffle enabled.

# Expected Latency Benefits and Fallback Behavior

In the common path, streaming shuffle avoids writing map output to local disk and re-reading it on
the reduce side. Producers pipeline data through bounded in-memory buffers directly to consumers,
which overlaps the map and reduce phases and removes the disk round trip, reducing shuffle latency.

## Automatic fallback (zero regression)

To guarantee **zero regression**, the engine continuously monitors each streaming shuffle and
automatically reverts the affected shuffle to the sort-based path when **any** of the following
conditions holds:

* the consumer is sustained at **2&times; or more slower** than the producer for **more than 60
  seconds**;
* **memory pressure** would risk an out-of-memory condition when allocating buffers;
* **network saturation** is high (approaching link capacity); or
* a **producer/consumer version mismatch** is detected by the compatibility check.

Because fallback is automatic and transparent, memory-bound workloads — and any workload that is not
a good fit — see **no regression** relative to the default sort-based shuffle.

## Fallback is not the same as spilling

Do not confuse **fallback** with **spilling**. Spilling to disk happens when buffer utilization
reaches `spark.shuffle.streaming.spillThreshold` (default `80`): the largest / least-recently-used
partitions are written to disk to relieve memory pressure, but the shuffle **remains on the
streaming path**. Fallback, by contrast, abandons streaming for that shuffle entirely and uses
sort-based shuffle instead. In short: spilling is streaming under memory pressure; fallback is a full
switch back to sort-based shuffle.

# Verifying It Is Active

Because enabling streaming shuffle is silent when the gate is not fully satisfied, confirm it is
actually running before you rely on it:

* **Metrics.** When streaming shuffle is active, executors emit metrics under the `streamingShuffle`
  namespace through Spark's standard `MetricsSystem` (JMX, Prometheus, CSV, and Slf4j sinks, plus the
  `/metrics/executors/prometheus` endpoint), exposed as
  `<application>.<executorId>.streamingShuffle.<metricName>`. A non-zero
  `bufferUtilizationPercent` gauge is a clear signal that data is flowing through the streaming
  buffers. These metrics are emitted **only** when the dual activation gate is satisfied, so their
  mere presence confirms the backend is active. See the [Monitoring](monitoring.html) guide for the
  full metric list.
* **Logs.** Streaming-shuffle components emit structured logs tagged with the MDC correlation-ID keys
  `shuffle_id`, `map_id`, `reduce_partition_range`, and `attempt_id`, which let you correlate log
  lines across the producer (map) and consumer (reduce) boundaries for a single shuffle. Set
  `spark.shuffle.streaming.debug=true` for verbose diagnostics. See the [Monitoring](monitoring.html)
  guide for how to surface these keys in your log layout.

If you do not see these metrics or logs, double-check that **both** `spark.shuffle.manager=streaming`
and `spark.shuffle.streaming.enabled=true` are set on the executors, and consult the
[Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) guide.

# Limitations

* **No dynamic reconfiguration.** In this version, `spark.shuffle.streaming.*` properties are fixed
  for the lifetime of the application; changing any of them requires restarting the executors.
* **Debug logging is off by default.** Streaming-shuffle debug logging is disabled unless you set
  `spark.shuffle.streaming.debug=true`, which increases log volume.
* **No new Web UI pages.** Streaming shuffle does not add any Spark Web UI pages or tabs; observe it
  through the existing metrics, the Stages tab, and structured logs described in
  [Verifying It Is Active](#verifying-it-is-active).

# Related Pages

See also:

* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) — design and internals.
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) — sizing buffers, spill threshold, and
  bandwidth.
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) — diagnosing metrics
  and logs.
* [Configuration → Shuffle Behavior](configuration.html#shuffle-behavior) — the canonical property
  reference.
* [Monitoring](monitoring.html) — metrics and structured-logging details.
