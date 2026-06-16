---
layout: global
title: Streaming Shuffle Troubleshooting
displayTitle: Streaming Shuffle Troubleshooting
description: Troubleshooting guide for the streaming shuffle backend in Spark SPARK_VERSION_SHORT
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

This page helps you diagnose problems when running with the opt-in streaming shuffle backend
enabled, and explains how to use its metrics and logs to find the root cause. Before you start,
keep one design principle in mind: the streaming shuffle backend is built to **fall back to the
sort-based shuffle automatically** whenever conditions are unfavorable, so many of the "problems"
you may observe are in fact expected, safe degradations rather than errors. For background on how
the backend works, see the [architecture overview](streaming-shuffle-architecture.html); for how
to enable and configure it, see the [user guide](streaming-shuffle-guide.html) and the
[tuning guide](streaming-shuffle-tuning.html).

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

The streaming shuffle backend is an **opt-in** alternative to the default sort-based shuffle. It
streams map-side output directly to reduce-side consumers through bounded in-memory buffers and the
existing network transport, governed by a backpressure protocol, instead of fully materializing
shuffle data to local disk before any fetch begins. It is engaged only when an operator sets both
`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`; both default to off,
so the default behavior of every existing deployment is unchanged.

Because the backend is designed for **zero regression**, it composes the existing sort-based
shuffle (`SortShuffleManager`) unchanged and automatically reverts to it whenever a fallback
condition trips. As a result, the most common observations on this page — fallback to sort-based
shuffle, occasional disk spills, and producer throttling — are normal, non-fatal behaviors that
protect the job rather than fail it. The guidance below explains how to recognize each situation,
which metric to inspect, and what (if anything) to change. When a genuine fault occurs, such as a
producer failure, the backend surfaces a standard `FetchFailedException` so Spark's existing
lineage and recompute machinery recovers the lost output with **zero data loss**.

# Diagnostic metrics

The backend emits four metrics under the `shuffle.streaming.*` namespace. They are registered with
Spark's existing `MetricsSystem` and are therefore exposed through the same channels as every other
Spark metric: via **JMX**, via the **Prometheus** endpoint `/metrics/executors/prometheus`, and
through the shuffle columns on the Web UI **Stages** tab. These four metrics are your primary
diagnostic signal — most symptoms in this guide are diagnosed by reading one or two of them. See
[Monitoring](monitoring.html) for how to enable and scrape Spark metrics in general.

| Metric (`shuffle.streaming.*`) | Type | What it measures | How to use it in diagnosis |
|--------------------------------|------|------------------|----------------------------|
| `bufferUtilizationPercent` | Gauge | Current fill level of the in-memory per-partition buffers, as a percent. | A value that is persistently high (close to the configured `spillThreshold`) indicates memory pressure and imminent spill. Sustained values near 100% precede memory-pressure fallback. |
| `spillCount` | Counter | Number of disk spills performed since the executor started. | Steadily rising values mean the buffers are too small for the workload. Correlate with `bufferUtilizationPercent`. |
| `backpressureEvents` | Counter | Number of times producers were throttled by the flow-control protocol. | High or rapidly increasing values mean consumers cannot keep up with producers; the leading indicator of slow-consumer fallback. |
| `partialReadInvalidations` | Counter | Number of partial reads invalidated because a producer failed or timed out. | Increases in lock-step with `FetchFailedException` and the recomputes that follow. Frequent increments point to flaky executors or networks. |

For deeper, per-shuffle diagnosis, set `spark.shuffle.streaming.debug=true` to enable additional
diagnostic logging. The structured log lines carry correlation keys — shuffle id, map id, reduce
partition range, and attempt id — so you can trace a single shuffle across producer and consumer
executors (see [Logging](#logging) below).

# Symptom: the backend fell back to sort-based shuffle

The streaming backend continuously evaluates four fallback conditions and **transparently reverts
to the sort-based shuffle** when any of them trips. This is the zero-regression safety mechanism, so
fallback is **non-fatal**: there is no data loss and no job failure — only a change of shuffle
implementation for the affected work. If throughput or latency is not improving as expected, the
most likely explanation is that one of the conditions below is keeping the job on the sort-based
path. Diagnose which one using the streaming metrics, then decide whether to tune or simply accept
the fallback.

### Slow consumer

The consumer was sustained **2× slower than the producer for more than 60 seconds**, so the backend
reverts rather than let producers stall.

* **Diagnose:** a high or climbing `backpressureEvents` count is the signature — producers were
  repeatedly throttled before the fallback tripped.
* **Mitigate:** cap producer throughput with `spark.shuffle.streaming.maxBandwidthMBps`, reduce the
  number of concurrent shuffles, or simply accept the fallback if the workload is consumer-bound.

### Memory pressure (OOM risk, > 95%)

Buffer allocation could not proceed because executor memory utilization exceeded **95%**, so the
backend reverts to avoid an out-of-memory failure.

* **Diagnose:** a persistently high `bufferUtilizationPercent` together with a rising `spillCount`
  indicates the executor was under memory pressure right before fallback.
* **Mitigate:** lower `spark.shuffle.streaming.bufferSizePercent`, lower
  `spark.shuffle.streaming.spillThreshold`, increase executor memory, or repartition to reduce
  per-partition buffer size. See the [tuning guide](streaming-shuffle-tuning.html) for sizing
  guidance.

### Network saturation (> 90% link capacity)

The network link was running above **90% of capacity**, so streaming additional shuffle data would
have degraded the whole executor's traffic.

* **Diagnose:** correlate with cluster/host network metrics; streaming throughput plateaus while
  `backpressureEvents` rises.
* **Mitigate:** set a `spark.shuffle.streaming.maxBandwidthMBps` cap to leave headroom, and
  investigate competing traffic on the same links.

### Producer/consumer version mismatch

A producer and consumer reported incompatible streaming-protocol versions, so the backend declines
to stream between them.

* **Diagnose:** check that every executor is running the **same Spark version**; this is common
  during rolling upgrades.
* **Mitigate:** ensure all executors run an identical Spark version before relying on the streaming
  backend.

# Symptom: `FetchFailedException` / partial reads

A `FetchFailedException` originating from the streaming backend is the **normal, safe recovery
path** for a producer failure — not a defect in the streaming implementation. When a producer
becomes unreachable, the reader follows this sequence:

```
1. Connection timeout            reader waits up to 5 s for the producer; on timeout it
                                 treats the producer as failed.
2. Invalidate partial reads      all partially-read blocks from the failed producer are
                                 atomically invalidated (increments partialReadInvalidations)
                                 and the buffered data is discarded.
3. Raise FetchFailedException    a standard FetchFailedException is surfaced to Spark.
4. Lineage recompute             Spark's existing DAG/lineage machinery recomputes the
                                 upstream map tasks that produced the lost output.
5. Retry from recomputed output  the read is retried against the recomputed producer using
                                 exponential backoff (1 s initial delay, maximum 5 attempts).
```

Because the partial output is discarded and the upstream work is recomputed before the retry —
through Spark's unchanged lineage/recompute machinery — this flow is designed for **zero data loss**,
and the failure paths are exercised by the 10-scenario `StreamingShuffleFailureInjectionSuite`. An
occasional `FetchFailedException` under load is expected and is handled transparently.

When `FetchFailedException` is **frequent**, the streaming backend is usually the messenger, not the
cause — investigate flaky executors or an unstable network instead. Use the stage and task error
details in the [Web UI](web-ui.html) to identify which executors are failing, and correlate the
failure rate with the `partialReadInvalidations` counter. If invalidations are concentrated on a few
hosts, focus remediation there rather than on the streaming configuration.

# Symptom: frequent disk spills / high disk I/O

Disk spills are a built-in safety valve, not an error. When buffer utilization reaches the
configured `spillThreshold` (default **80%**), the backend spills the largest buffered partitions to
disk through the existing block manager using the `DISK_ONLY` storage level, reclaiming the memory
within roughly **100 ms**. Spilled and streamed bytes are interchangeable, so a spill never changes
results — it only trades memory for disk I/O.

* **Diagnose:** a rising `spillCount` combined with a `bufferUtilizationPercent` that frequently
  approaches `spillThreshold` confirms the workload is spilling. Frequent spills typically show up as
  elevated disk I/O on the executors.
* **Mitigate:** increase `spark.shuffle.streaming.bufferSizePercent` if executor memory allows,
  reduce the partition count so each per-partition buffer is larger, or simply accept spilling when
  memory is the binding constraint. See the [tuning guide](streaming-shuffle-tuning.html) for how to
  balance buffer size, spill threshold, and partition count.

# Symptom: producers throttled / backpressure

A high or rapidly increasing `backpressureEvents` count means the consumer-to-producer flow-control
protocol is **throttling producers**. The protocol combines a periodic heartbeat (interval **10 s**)
from consumers — emitted **best-effort** in v1 when the backpressure endpoint is reachable — with
token-bucket rate limiting on producers (always active locally), so that producers cannot overwhelm
consumers that are reading more slowly than data is generated.

* **Causes:** slow or overloaded consumers, an overly low
  `spark.shuffle.streaming.maxBandwidthMBps` cap that limits producers below what consumers can
  actually absorb, or too many concurrent shuffles competing for the per-executor bandwidth budget.
* **Mitigate:** raise or clear the `spark.shuffle.streaming.maxBandwidthMBps` cap, scale out or
  speed up the consumers, or reduce the number of concurrent shuffles. Note that sustained
  backpressure (consumer 2× slower than producer for more than 60 s) eventually triggers
  [slow-consumer fallback](#symptom-the-backend-fell-back-to-sort-based-shuffle), which is the
  expected outcome for a persistently consumer-bound workload.

# Symptom: streaming shuffle does not appear to be active

If you expected the streaming backend to engage but the job behaves exactly like the sort-based
shuffle, work through this checklist:

1. **Both activation flags are set.** The backend requires **both**
   `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`. Both default to
   off, and setting only one has no effect.
2. **The application was restarted after the change.** Streaming-shuffle configuration is
   **immutable for the lifetime of the application** in this version — there is no dynamic
   reconfiguration. Changing the flags requires restarting the application (and its executors) to
   take effect.
3. **The metrics are non-zero.** Inspect the `shuffle.streaming.*` metrics described above. If all
   four remain at their initial values across a shuffle-heavy stage, the streaming path is not being
   exercised — re-check the flags and the restart.
4. **Enable debug logging.** Set `spark.shuffle.streaming.debug=true` and look for the
   streaming-shuffle startup and per-shuffle log lines to confirm the backend initialized.

See the [user guide](streaming-shuffle-guide.html) for the full activation procedure.

# Logging

The streaming backend uses Spark's existing SLF4J/Log4j2 logging stack and adds streaming-specific
**structured logging with correlation IDs**. Each streaming-shuffle log line carries correlation
keys — shuffle id, map id, reduce partition range, and attempt id — so you can follow a single
shuffle across the producer and consumer executors that participate in it.

Setting `spark.shuffle.streaming.debug=true` raises the verbosity of these logs, which is useful
while diagnosing a problem. **Disable it again in production** to respect the backend's log-volume
budget; verbose streaming logs can otherwise dominate executor log output. For general log
configuration — log levels, layouts, and file rotation — see
[Configuration](configuration.html#configuring-logging).

# Configuration reference

The configuration keys referenced throughout this guide are summarized below. All keys are
**immutable for the lifetime of the application**; change them only between application runs. Tuning
guidance for each lives in the [tuning guide](streaming-shuffle-tuning.html).

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Opt-in feature flag for the streaming shuffle backend. Must be set together with
    <code>spark.shuffle.manager=streaming</code> for the backend to engage. When false (the default),
    the sort-based shuffle is used.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percent of executor memory (range 1&ndash;50) used for streaming shuffle buffers. Raising it
    reduces spills at the cost of memory headroom; lowering it relieves memory pressure.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer utilization percent (range 50&ndash;95) at which the largest buffers spill to disk
    (<code>DISK_ONLY</code>). Lower it to spill earlier and reduce out-of-memory risk.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0 (unlimited)</td>
  <td>
    Per-executor streaming bandwidth cap, in MB/s, enforced by token-bucket rate limiting. A value
    of 0 (or any non-positive value) means unlimited. Use a cap to leave network headroom or to
    throttle producers ahead of slow consumers.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>
    Enables additional streaming-shuffle diagnostic logging with correlation IDs. Enable while
    troubleshooting; disable in production to respect the log-volume budget.
  </td>
  <td>4.1.0</td>
</tr>
</table>

Note that `spark.shuffle.manager=streaming` is the manager alias that selects the backend; it is the
second flag required for activation alongside `spark.shuffle.streaming.enabled=true`.

# Quick reference: symptom &rarr; metric &rarr; action

The table below ties the guide together: find your symptom, read the indicated metric to confirm the
cause, and apply the recommended action.

| Symptom | Key metric(s) | Likely cause | Recommended action |
|---------|---------------|--------------|--------------------|
| Fell back to sort-based shuffle | `backpressureEvents`, `bufferUtilizationPercent` | Slow consumer, memory pressure (> 95%), network saturation (> 90%), or version mismatch | Diagnose the specific [fallback condition](#symptom-the-backend-fell-back-to-sort-based-shuffle); cap bandwidth, tune buffers, or align Spark versions. Fallback is safe — accepting it is valid. |
| `FetchFailedException` / partial reads | `partialReadInvalidations` | Producer failure or 5 s connection timeout | Normal recovery (zero data loss). If frequent, investigate flaky executors/network via the [Web UI](web-ui.html). |
| Frequent disk spills / high disk I/O | `spillCount`, `bufferUtilizationPercent` | Buffers too small for the workload | Increase `bufferSizePercent` (if memory allows), reduce partition count, or accept spilling. |
| Producers throttled / backpressure | `backpressureEvents` | Consumers can't keep up; cap too low; too many concurrent shuffles | Raise or clear `maxBandwidthMBps`, scale/speed up consumers, reduce concurrency. |
| High buffer utilization / imminent spill | `bufferUtilizationPercent` | Memory pressure approaching `spillThreshold` | Lower `spillThreshold` or `bufferSizePercent`, or add executor memory. |
| Streaming shuffle not active | All four metrics stay at zero | Activation flags not both set, or no restart | Set `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`, then restart the application. |

# Related documentation

* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) — how the backend works internally.
* [Streaming Shuffle User Guide](streaming-shuffle-guide.html) — enabling and using the backend.
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) — sizing buffers, spill threshold, and bandwidth.
* [Monitoring and Instrumentation](monitoring.html) — enabling and scraping Spark metrics (JMX and Prometheus).
* [Configuration](configuration.html) — all Spark configuration properties, including [logging](configuration.html#configuring-logging).
* [Web UI](web-ui.html) — inspecting stages, tasks, and shuffle read/write columns.

