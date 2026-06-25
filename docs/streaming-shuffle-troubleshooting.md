---
layout: global
title: "Streaming Shuffle Troubleshooting"
displayTitle: "Streaming Shuffle Troubleshooting"
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

This page helps operators diagnose and resolve issues with Spark's opt-in streaming shuffle. The central guarantee is **zero data loss** under all failure scenarios: every failure path invalidates partial work cleanly and defers to Spark's existing stage recomputation, so correctness is never at risk. In addition, **automatic fallback** to the default sort-based shuffle preserves both correctness and progress whenever streaming is not viable for a given workload or cluster. This page assumes streaming shuffle is already enabled as described in the [Streaming Shuffle Guide](streaming-shuffle-guide.html); if streaming never engages, start with [Streaming not engaging at all](#common-operational-issues) below.

## Automatic Fallback Conditions

The `StreamingShuffleFallbackPolicy` continuously evaluates four conditions during execution. If **any** condition is met, the subsystem transparently reverts to the inner `SortShuffleManager` for the affected shuffles. Fallback incurs **no data loss** and requires **no user action** — the sort-based shuffle remains the default path and the always-available safety net.

| Condition | Trigger | Result |
|-----------|---------|--------|
| **Slow consumer** | Consumer sustained 2× slower than the producer for > 60 s | Affected shuffles revert to the sort-based shuffle |
| **Memory pressure** | Buffer allocation would risk an out-of-memory (OOM) condition | Affected shuffles revert to the sort-based shuffle |
| **Network saturation** | Link utilization exceeds 90% of capacity | Affected shuffles revert to the sort-based shuffle |
| **Version mismatch** | Producer/consumer streaming-protocol version mismatch | Affected shuffles revert to the sort-based shuffle |

Occasional fallback is expected and healthy. However, **frequent** fallback indicates that the workload or cluster is not a good fit for streaming shuffle (for example, memory-bound jobs or congested networks). In that case, review the [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide, or simply keep the sort-based shuffle for the affected jobs.

## Failure Scenarios

The streaming shuffle subsystem is validated against a failure-injection suite covering ten scenarios. **All ten preserve data integrity** — each either recovers in place or defers to Spark's existing recomputation, so no scenario can lose data. The table below summarizes how each failure is detected and handled.

| # | Scenario | Detection | Response |
|---|----------|-----------|----------|
| 1 | Producer (map task) connection timeout | Producer unreachable for 5 s | Partial reads are invalidated; a `FetchFailedException` is thrown and Spark's existing DAG scheduler recomputes the upstream stage. |
| 2 | Consumer (reduce task) crash | Missing acknowledgments detected at 10 s | Buffers held for the failed consumer are reclaimed. |
| 3 | CRC32C checksum mismatch on a block | Block-level checksum verification fails | The block is retransmitted; if the mismatch persists, the read is invalidated and the stage is recomputed. |
| 4 | Memory pressure prevents buffer allocation (OOM risk) | Buffer allocation would risk OOM | Automatic fallback to the sort-based shuffle. |
| 5 | Network saturation | Link utilization > 90% of capacity | Automatic fallback to the sort-based shuffle. |
| 6 | Slow consumer | Consumer sustained 2× slower than the producer for > 60 s | Automatic fallback to the sort-based shuffle. |
| 7 | Producer/consumer version mismatch | Streaming-protocol version mismatch | Automatic fallback to the sort-based shuffle. |
| 8 | Buffer utilization reaches the 80% spill threshold | Buffer utilization reaches 80% | `MemorySpillManager` spills the largest partitions to disk (LRU, `DISK_ONLY`) within 100 ms. |
| 9 | Transient transport error | Transport I/O error | Retried with exponential backoff (starting at 1 s, up to 5 attempts); on exhaustion the read is invalidated and the stage is recomputed. |
| 10 | Backpressure heartbeat loss / RPC endpoint unreachable | Heartbeat (5 s) missed | `backpressureEvents` increments; the consumer is treated as slow/failed and a fallback evaluation is triggered. |

## Metrics to Watch

The streaming shuffle registers four metrics under the `shuffle.streaming.` namespace via a metrics `Source` named `streamingShuffle`. These are surfaced through Spark's **existing** JMX, Prometheus, CSV, and SLF4J sinks — no new metrics endpoint is introduced. For sink configuration, see [Monitoring and Instrumentation](monitoring.html); aggregate shuffle read/write/spill columns also appear on the Stages tab of the [Web UI](web-ui.html).

| Metric | Type | What it indicates |
|--------|------|-------------------|
| `bufferUtilizationPercent` | Gauge | Current per-partition buffer fill level. Sustained high values precede spills. |
| `spillCount` | Counter | Number of disk spill events. Rising values mean buffers are too small for the workload. |
| `backpressureEvents` | Counter | Number of backpressure activations. High values indicate a slow consumer or a saturated link. |
| `partialReadInvalidations` | Counter | Number of partial reads invalidated on producer failure. Each invalidation triggers an upstream recomputation. |

When the JMX sink is enabled, each metric is registered under an `ObjectName` that follows Spark's standard convention:

```text
metrics:name=<app>.<executor-id>.streamingShuffle.shuffle.streaming.<metric>
```

When the Prometheus servlet is enabled (gated by the Prometheus UI flag), the same metrics are scrapable from the existing executors endpoint:

```text
/metrics/executors/prometheus/
```

## Timeouts and Timing Reference

The streaming shuffle uses several distinct timers. They are non-overlapping; the table below documents each value and its role so the timers are not confused with one another.

| Timer | Value | Role |
|-------|-------|------|
| Producer connection timeout | 5 s | Producer-failure detection → partial-read invalidation |
| Backpressure heartbeat | 5 s | Flow-control liveness signal |
| Consumer liveness / missing-ack | 10 s | Consumer-failure detection |
| Retry backoff | Start 1 s, up to 5 attempts (exponential) | Transient transport retry |
| Spill poll / reclaim | 100 ms | Buffer-utilization polling and buffer-reclamation SLA |

## Common Operational Issues

Use the symptom → likely cause → resolution entries below to triage the most common situations. All resolutions reference the real streaming-shuffle configuration keys; see [Spark Configuration](configuration.html#shuffle-behavior) for their full definitions and ranges.

- **Frequent spills (`spillCount` rising).** Likely cause: per-partition buffers are too small, or partitions are too large for the configured buffer budget. Resolution: increase `spark.shuffle.streaming.bufferSizePercent` if executor memory allows, or raise `spark.shuffle.streaming.spillThreshold`. See the [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) guide for sizing guidance.
- **Frequent fallback to the sort-based shuffle.** Likely cause: one of the four fallback conditions — memory pressure, network saturation above 90%, a consumer sustained 2× slower than the producer for more than 60 s, or a producer/consumer version mismatch. Resolution: check executor memory headroom and network utilization, confirm that all executors run the same Spark version, and consider keeping the sort-based shuffle for memory-bound jobs.
- **High `backpressureEvents`.** Likely cause: slow consumers or a saturated network link. Resolution: cap per-executor bandwidth with `spark.shuffle.streaming.maxBandwidthMBps`, scale out consumers, or investigate data skew that overloads specific reduce tasks.
- **Rising `partialReadInvalidations`.** Likely cause: producer instability or timeouts that force upstream recomputation. Resolution: investigate executor stability and the 5 s producer connection timeout. Recomputation preserves correctness (zero data loss) but costs latency, so reducing producer churn directly improves end-to-end time.
- **Streaming not engaging at all.** Likely cause: the **dual-flag** activation contract is not satisfied. Resolution: ensure that **both** `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true` are set **before the application starts**. Streaming-shuffle configuration is immutable for the application lifetime and cannot be changed at runtime. See the [Streaming Shuffle Guide](streaming-shuffle-guide.html) for the full activation contract.

To capture verbose diagnostics while reproducing any of the issues above, enable debug logging with `spark.shuffle.streaming.debug=true`. A typical opt-in plus diagnostics configuration looks like this:

```bash
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.debug=true \
  ...
```

## Related Documentation

* [Streaming Shuffle Guide](streaming-shuffle-guide.html)
* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html)
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html)
* [Monitoring and Instrumentation](monitoring.html)
* [Web UI](web-ui.html)
* [Spark Configuration](configuration.html#shuffle-behavior)
