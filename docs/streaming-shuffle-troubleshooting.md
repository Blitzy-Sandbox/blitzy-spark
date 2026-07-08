---
layout: global
displayTitle: Streaming Shuffle Troubleshooting
title: Streaming Shuffle Troubleshooting
description: Diagnosing and troubleshooting the streaming shuffle backend in Apache Spark SPARK_VERSION_SHORT
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

Streaming shuffle is an **opt-in** shuffle backend that pipelines map-output data directly to reduce
tasks through bounded in-memory buffers, instead of first materializing shuffle files to local disk.
It **coexists with** the default sort-based shuffle, which remains the production-stable path and the
automatic fallback. You observe and diagnose streaming shuffle entirely through Spark's existing
`MetricsSystem` and structured logs — it adds **no new Spark Web UI pages or tabs**.

This page is a set of **symptom &#8594; cause &#8594; action** diagnostics: how to interpret the
streaming-shuffle metrics and logs, and how to resolve the issues they surface. Every diagnostic ties
a concrete metric back to the configuration knob that addresses it, cross-linking the
[Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide for sizing guidance.

Before troubleshooting a *behavioral* problem, first confirm the backend is even active. Streaming
shuffle requires a **dual activation gate** and silently uses sort-based shuffle when the gate is not
fully satisfied — so "streaming does nothing" is most often a configuration issue, not a runtime one.
If you are unsure it is running, start with [Common Misconfigurations](#common-misconfigurations).

Related references:

* metrics namespace and the structured-logging (MDC) schema — [Monitoring](monitoring.html);
* enabling the backend (the dual activation gate) — [Streaming Shuffle Guide](streaming-shuffle-guide.html);
* sizing buffers, the spill threshold, and bandwidth — [Streaming Shuffle Tuning](streaming-shuffle-tuning.html).

# Interpreting the Metrics

When streaming shuffle is active, each executor emits four metrics under the `streamingShuffle`
namespace through Spark's standard `MetricsSystem`. They surface automatically through every
configured sink (JMX, Prometheus, CSV, Slf4j) and on the `/metrics/executors/prometheus` endpoint,
exposed as `<application>.<executorId>.streamingShuffle.<metricName>`. See the
[Monitoring](monitoring.html) guide for the full metric list. Because these metrics are emitted
**only** when the [dual activation gate](#common-misconfigurations) is satisfied, their mere presence
confirms the backend is active.

| Metric | Type | Meaning | What sustained / rising values indicate |
| --- | --- | --- | --- |
| `bufferUtilizationPercent` | Gauge (0&ndash;100) | Current per-executor streaming buffer utilization. | Approaching the spill threshold; spilling is imminent. |
| `spillCount` | Counter | Number of buffered partitions spilled to disk. | Memory pressure on the streaming buffers. |
| `backpressureEvents` | Counter | Number of backpressure throttle/timeout events. | The consumer or the network cannot keep up with the producer. |
| `partialReadInvalidations` | Counter | Number of in-progress reads invalidated on producer failure. | Producer-side failures or timeouts. |

Use each metric as the entry point to the matching diagnostic below:

* `bufferUtilizationPercent` (gauge, 0&ndash;100) — current per-executor streaming buffer
  utilization; **sustained high values precede spilling**. See
  [Diagnosing Frequent Spills](#diagnosing-frequent-spills).
* `spillCount` (counter) — buffered partitions spilled to disk; **rising values mean memory
  pressure**. See [Diagnosing Frequent Spills](#diagnosing-frequent-spills).
* `backpressureEvents` (counter) — throttle/timeout events; **rising values mean the consumer or
  network can't keep up with the producer**. See
  [Diagnosing Backpressure Events](#diagnosing-backpressure-events).
* `partialReadInvalidations` (counter) — in-progress reads invalidated on producer failure;
  **nonzero values indicate producer-side failures/timeouts**. See
  [Diagnosing Partial-Read Invalidations and Fetch Failures](#diagnosing-partial-read-invalidations-and-fetch-failures).

# Reading the Logs (MDC Correlation IDs)

Streaming-shuffle components log through `org.apache.spark.internal.Logging` and tag every line with
four MDC (Mapped Diagnostic Context) correlation-ID keys. These let you follow a single shuffle
across the producer (map) and consumer (reduce) executor boundaries:

* `shuffle_id` — the shuffle identifier;
* `map_id` — the producing map task identifier;
* `reduce_partition_range` — the half-open range of reduce partitions being read, formatted
  `[start, end)` (a single reduce partition appears as the degenerate range `[p, p+1)`);
* `attempt_id` — the task attempt identifier.

As with all Spark MDC keys, these are **not shown in plain-text logs by default**. To surface them,
either:

* add the keys to your log4j2 `PatternLayout` — for example `%X{shuffle_id}`, `%X{map_id}`,
  `%X{reduce_partition_range}`, and `%X{attempt_id}`; or
* enable [structured logging](configuration.html#structured-logging) by setting
  `spark.log.structuredLogging.enabled=true`, which emits JSON containing **all** MDC fields (ideal
  for querying logs at scale).

For verbose diagnostics, set `spark.shuffle.streaming.debug=true`. At manager construction this
elevates the `org.apache.spark.shuffle.streaming` logger to `DEBUG` via the log4j2 `Configurator`,
so the diagnostics the streaming components gate behind the flag are actually emitted. If the
active logging backend is not log4j2, the flag still gates those debug calls and you can raise that
logger to `DEBUG` through your own logging configuration instead. It is **off by default** and
increases log volume, so enable it only while investigating an issue and disable it afterward. See
the [Monitoring](monitoring.html) guide for the full MDC schema. Like every
`spark.shuffle.streaming.*` key, changing `debug` requires an **executor restart** to take effect.

# Diagnosing Frequent Spills

**Symptom.** A steadily rising `spillCount` counter together with a `bufferUtilizationPercent` gauge
that sits near or above the configured spill threshold
(`spark.shuffle.streaming.spillThreshold`, default `80`).

**Cause.** The per-partition buffers are too small for the volume of data being streamed. This
usually comes from one of: buffers sized too small for the data volume, too many partitions dividing
the memory budget into tiny per-partition buffers, or simply insufficient executor memory. Recall
that each buffer is sized approximately as
`(executorMemory × bufferSizePercent / 100) / numPartitions`, so a high partition count shrinks every
buffer. Note that spilling is **not** a failure or a fallback — it is streaming operating under
memory pressure, and the shuffle stays on the streaming path. It becomes a concern only when it is
frequent enough to erode the latency benefit.

**Action.**

* Increase `spark.shuffle.streaming.bufferSizePercent` (valid range `[1, 50]`, default `20`) when you
  have spare executor memory — larger buffers hold more in-flight data and spill less often.
* Reduce the shuffle partition count so each partition receives a larger share of the buffer budget.
* Add executor memory so the same `bufferSizePercent` yields larger absolute buffers.
* Optionally adjust `spark.shuffle.streaming.spillThreshold` (valid range `[50, 95]`, default `80`):
  lower it to spill earlier and more conservatively, or raise it to keep more data in memory.

For detailed sizing guidance and a metric-to-setting map, see the
[Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide. Remember that all
`spark.shuffle.streaming.*` changes require an **executor restart** to take effect.

# Diagnosing Backpressure Events

**Symptom.** A rising `backpressureEvents` counter.

**Cause.** The producer is being throttled because the downstream cannot keep up. The usual causes
are a consumer sustained slower than the producer, a `spark.shuffle.streaming.maxBandwidthMBps` rate
limit set too low, or genuine network saturation on a shared link.

**Action.**

* If you have set a rate limit, raise or clear `spark.shuffle.streaming.maxBandwidthMBps`
  (`0` = unlimited). On a dedicated or high-bandwidth network, leaving it at `0` lets streaming
  pipeline data as fast as producers and consumers allow.
* Investigate consumer-side slowness or skew — a few slow reduce tasks (data skew, GC pressure, CPU
  contention) will throttle their producers and show up here.
* Check network utilization. If the link is genuinely saturated, backpressure is doing its job;
  sustained saturation approaching **~90%** of link capacity is also one of the automatic
  [fallback](#confirming-and-understanding-automatic-fallback) conditions.

See the [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide for bandwidth guidance.
Changes require an **executor restart**.

# Diagnosing Partial-Read Invalidations and Fetch Failures

**Symptom.** A nonzero `partialReadInvalidations` counter, frequently accompanied by
`FetchFailedException` entries in the driver and executor logs.

**Cause.** A producer failed, or did not respond within the **5&nbsp;second** producer connection
timeout. When that happens the reader **atomically invalidates** the partial read it had accumulated
and throws a standard `FetchFailedException`, which the DAG scheduler handles by recomputing the
upstream (map) stage.

**This is the normal, safe fault path — not a bug and not data loss.** It is exactly the same fault
path the default sort-based shuffle uses. The partial (and now discarded) read is never surfaced to
the reduce task, and the recomputed upstream output replaces it. Streaming shuffle atomically discards
partial reads precisely to guarantee **zero data loss** under producer failure. A small, occasional
`partialReadInvalidations` count during an otherwise healthy run (for example, an executor lost to a
spot-instance reclaim) is expected and harmless.

**Action.** Treat a *persistently* rising count as a signal to investigate the **producer** side, not
the streaming backend itself:

* Look for producer-executor crashes, long GC pauses (a pause exceeding the 5&nbsp;second timeout will
  trip it), or network partitions between executors.
* Correlate the failing shuffle using the MDC keys `shuffle_id`, `map_id`, `reduce_partition_range`,
  and `attempt_id` (see [Reading the Logs](#reading-the-logs-mdc-correlation-ids)) to pinpoint
  the failing producer.
* Repeated failures for the same producer usually indicate an infrastructure problem (an unhealthy
  node, a flaky network) rather than a streaming-shuffle defect.

The two liveness mechanisms are **directional**: the **5&nbsp;second** producer connection timeout
(above) detects a failed *producer*, while separately the consumer emits a heartbeat on a
**10&nbsp;second** cadence so *producers* can detect a stalled or dead *consumer*.

# Version-Mismatch Fallback

**Symptom.** A shuffle silently runs on the **sort-based** path even though streaming shuffle is
enabled, and the logs report a producer/consumer protocol **version mismatch**.

**Cause.** Executors are running mixed Spark (or streaming-protocol) versions, so the producer and
consumer cannot agree on the wire protocol. The compatibility check detects the mismatch and, rather
than risk an incompatible transfer, reverts that shuffle to the sort-based path.

**Action.** Ensure a **uniform Spark version across the entire cluster** — the driver and all
executors. A mismatch most often appears during a rolling upgrade, or when a node is provisioned from
a stale image. A version mismatch is one of the
[fallback policy](#confirming-and-understanding-automatic-fallback) conditions that will govern
streaming in v2; in this release every shuffle already runs on the sort path regardless of version,
so aligning versions is about forward-compatibility rather than re-enabling streaming today.

# Confirming and Understanding Automatic Fallback

To guarantee **zero regression**, the streaming backend chooses between the streaming and the
production-stable **sort-based** path when each shuffle is *registered*. In this release (v1) that
decision is unconditional: because the wire transport is a **logging-only stub** that puts no bytes
on the wire (`StreamingShuffleTransport.isWireTransferAvailable` is `false`), the manager routes
**every** shuffle to the sort path. Zero regression therefore holds *by construction* — no workload
is ever placed on an incomplete streaming data path.

The four conditions below are the **fallback policy** (`StreamingShuffleFallbackPolicy`) that will
govern the streaming-versus-sort choice once real wire transfer lands (v2). The policy already
exists and is consulted at registration, but in v1 the transport-capability gate above forces the
sort path before any of these can take effect:

1. **Slow consumer** — the consumer is sustained at **2&times; or more slower** than the producer for
   **more than 60&nbsp;seconds**.
2. **Memory pressure** — buffer memory pressure approaches an out-of-memory risk (around **~95%**
   utilization), beyond the point where spilling can relieve it.
3. **Network saturation** — network utilization approaches **~90%** of link capacity.
4. **Version mismatch** — a producer/consumer protocol version mismatch is detected (see
   [Version-Mismatch Fallback](#version-mismatch-fallback)).

Once streaming is active (v2), only the affected shuffle falls back on any trigger; the switch is
transparent and guarantees the workload is never slower than the default sort-based shuffle. This is
why memory-bound and poorly-fitting workloads see **no regression**.

**Do not confuse fallback with spilling.** Spilling (at `spark.shuffle.streaming.spillThreshold`,
default `80`) writes buffered partitions to disk but keeps the shuffle **on the streaming path**;
fallback abandons streaming for that shuffle entirely. See the
[Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide for the full distinction.

**How to confirm the sort path is in use.** When a shuffle runs on the sort path, its
`streamingShuffle` metrics **flatline** — `bufferUtilizationPercent` stays at `0` and the counters
do not advance — while the ordinary sort-shuffle metrics (shuffle read/write size, spill, fetch wait
time) populate in the **Stages tab** of the Spark Web UI. Seeing sort-shuffle activity with no
accompanying streaming metrics is the clearest confirmation that the shuffle is running on the
sort-based path. **In v1 this is the expected steady state for every shuffle**, since the stub
transport forces the sort path.

# Common Misconfigurations

In practice these are the most frequent reasons streaming shuffle "does nothing." Start here whenever
you expected streaming but observe none.

## Only one of the two gate properties is set (most common)

Streaming shuffle requires a **dual activation gate**: it is active **if and only if both**
`spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true` are set. If only one
is set — for example `spark.shuffle.streaming.enabled=true` while the manager is left at its default,
or `spark.shuffle.manager=streaming` without flipping the `enabled` flag — Spark **silently uses the
standard sort-based shuffle**. There is no error; streaming simply never activates and **no
`streamingShuffle` metrics are emitted**. This is by far the most common real-world issue. The
quickest check is metric presence: if you see no `streamingShuffle` metrics at all, verify that
**both** properties are set on the executors. See
[Enabling Streaming Shuffle](streaming-shuffle-guide.html) for details.

## Out-of-range configuration values

Streaming configuration values are validated at startup; out-of-range values are **rejected** so the
application fails fast rather than running with an invalid setting:

* `spark.shuffle.streaming.bufferSizePercent` must be within `[1, 50]` (default `20`);
* `spark.shuffle.streaming.spillThreshold` must be within `[50, 95]` (default `80`);
* `spark.shuffle.streaming.maxBandwidthMBps` must be **&ge; 0**, where `0` means unlimited
  (default `0`).

If an executor fails to start with a configuration error mentioning one of these keys, correct the
value into its valid range.

## Expecting live configuration changes

None of the `spark.shuffle.streaming.*` keys can be changed at runtime — there is **no dynamic
reconfiguration** in this version. Each key is read once at executor startup and is immutable for the
lifetime of the application. **Restart the executors** (or start a new application) to apply any
change. If a configuration edit "had no effect," an un-restarted executor is the usual explanation.

# Related Pages

See also:

* [Streaming Shuffle Guide](streaming-shuffle-guide.html) — enabling the backend and the dual
  activation gate.
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) — sizing buffers, the spill threshold,
  and bandwidth.
* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) — design and internals.
* [Monitoring](monitoring.html) — the streaming metric list and structured-logging schema.
* [Configuration &#8594; Shuffle Behavior](configuration.html#shuffle-behavior) — the canonical
  property reference.
