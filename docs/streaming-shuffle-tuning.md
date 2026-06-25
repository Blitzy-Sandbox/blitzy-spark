---
layout: global
title: "Streaming Shuffle Tuning"
displayTitle: "Streaming Shuffle Tuning"
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

This page covers tuning Spark's opt-in streaming shuffle for your workload. It assumes the feature is already enabled as described in the [Streaming Shuffle Guide](streaming-shuffle-guide.html) — that is, both activation flags are set: `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`. The guiding principle is **performance with safety**: the subsystem pursues its latency target only where the in-memory buffers fit, and otherwise automatically falls back to the default sort-based shuffle, so tuning can never cause data loss or a hard regression. Note that streaming-shuffle configuration is immutable for the lifetime of the application — set every property described below *before* launch, as none of them can be reconfigured dynamically while the application is running.

## Tuning Parameters at a Glance

The streaming shuffle exposes three tuning knobs. Each takes effect at application start and is versioned `4.2.0`.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percentage of executor memory (range 1&ndash;50) reserved for per-partition streaming buffers.
    Larger values reduce spills and improve pipelining at the cost of higher memory pressure.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer-utilization percentage (range 50&ndash;95) at which the largest partitions spill to disk.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0</td>
  <td>
    Per-executor streaming bandwidth limit in MB/s; <code>0</code> means unlimited. The effective
    rate is always capped at 80% of link capacity regardless of the configured value.
  </td>
  <td>4.2.0</td>
</tr>
</table>

For the full streaming-shuffle configuration surface — including the activation flag `spark.shuffle.streaming.enabled` and the diagnostic flag `spark.shuffle.streaming.debug` — see the [Streaming Shuffle Guide](streaming-shuffle-guide.html) and the [Shuffle Behavior](configuration.html#shuffle-behavior) section of [Spark Configuration](configuration.html#shuffle-behavior).

## Buffer Sizing (bufferSizePercent)

`spark.shuffle.streaming.bufferSizePercent` controls how much of each executor's memory is set aside for the in-memory streaming buffers, expressed as a percentage. It defaults to `20` and accepts values in the range `1`&ndash;`50`.

That percentage is shared across the partitions a task writes, giving the following per-partition budget:

```text
perPartitionBudget = (executorMemory × bufferSizePercent / 100) / numPartitions
```

Within each per-partition buffer, streamed blocks are sized up to 2 MB before they are pipelined toward the consumer.

**Tradeoffs.** Larger values reserve more memory for buffering: they reduce the frequency of disk spills and improve pipelining for shuffle-heavy stages, but they increase memory pressure and therefore the risk of an out-of-memory condition or an automatic fallback to the sort-based shuffle. Smaller values are safer on memory-constrained executors but cause buffers to spill sooner. Start at the default of 20% and increase it only when you observe a high `spillCount` or frequent `backpressureEvents` *and* your executors have memory headroom to spare.

```bash
# Reserve 30% of executor memory for per-partition streaming buffers
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=30 \
  ...
```

## Spill Threshold (spillThreshold)

`spark.shuffle.streaming.spillThreshold` is the buffer-utilization percentage at which the streaming subsystem begins spilling buffered data to disk. It defaults to `80` and accepts values in the range `50`&ndash;`95`.

**The 80% spill threshold.** When a per-partition buffer's utilization reaches the threshold, the `MemorySpillManager` spills the largest partitions to disk in least-recently-used (LRU) order using `DISK_ONLY` storage. The manager polls buffer utilization every 100 ms, and it reclaims spilled buffers within 100 ms of receiving the consumer's acknowledgment. This keeps memory bounded while data continues to stream.

**Tradeoffs.** A lower threshold spills earlier: it incurs more disk I/O but keeps memory usage safer. A higher threshold keeps more data resident in memory, lowering latency but raising the risk of an out-of-memory condition. Keep the threshold at `80` unless you have a specific reason to change it — lower it (toward `50`) if you observe memory pressure, or raise it (up to `95`) if your executors have ample memory and you want to avoid spills.

## Bandwidth Limiting (maxBandwidthMBps)

`spark.shuffle.streaming.maxBandwidthMBps` sets a per-executor limit, in MB/s, on the rate at which streaming data is sent. It defaults to `0`, which means unlimited. The limit is enforced by a token-bucket rate limiter in which one permit corresponds to one byte.

**The 80% bandwidth cap.** Even when you configure an explicit limit, the *effective* streaming rate is capped at 80% of the link capacity. This headroom is deliberately reserved for control traffic (heartbeats and acknowledgments) and for other workloads sharing the link, and it prevents the streaming path from saturating the network. Sustained network saturation above 90% link utilization is one of the conditions that triggers an automatic fallback to the sort-based shuffle, so the 80% cap helps keep the streaming path inside its safe operating envelope.

On shared clusters, set an explicit limit to prevent a single application from saturating the network and starving co-located jobs. On dedicated links, you can leave the value at `0` (unlimited) and rely on the built-in 80% cap.

```bash
# Cap streaming shuffle bandwidth at 500 MB/s per executor
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.maxBandwidthMBps=500 \
  ...
```

## Performance Targets

The streaming shuffle is designed to meet the following targets. These are the goals validated by the project's benchmark suite, not guarantees for every workload:

* **Shuffle-heavy workloads** (≥ 100 MB of shuffle data across ≥ 10 partitions): **30–50%** end-to-end latency reduction compared with the sort-based shuffle.
* **CPU-bound workloads**: **5–10%** improvement.
* **Memory-bound workloads**: **zero regression**, achieved by automatically falling back to the sort-based shuffle when the buffers cannot fit.
* **Telemetry overhead**: **< 1%** of executor CPU and **< 10 MB/hour/executor** of additional log volume.
* **Spill / reclaim SLA**: **100 ms** in both directions (spill when the threshold is reached, reclaim once an acknowledgment is received).

## A Suggested Tuning Workflow

Tune iteratively against a representative job rather than changing several knobs at once:

1. **Enable streaming on a representative shuffle-heavy job.** Set the two activation flags (`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`) and start from the default tuning values.
2. **Observe the streaming metrics.** Watch `bufferUtilizationPercent`, `spillCount`, `backpressureEvents`, and `partialReadInvalidations` through the configured metrics sinks and the Spark UI. See [Monitoring and Instrumentation](monitoring.html) for how to access them.
3. **If spilling or backpressure is frequent and memory allows**, raise `bufferSizePercent` (and, optionally, `spillThreshold`) to keep more data resident in memory.
4. **If executors approach out-of-memory or fall back frequently**, lower `bufferSizePercent` (and/or `spillThreshold`) so buffers spill earlier and memory pressure is reduced.
5. **On shared networks**, set `maxBandwidthMBps` to prevent the application from saturating the link.
6. **Compare end-to-end latency against the sort baseline** to confirm that the change is a net improvement.

If you encounter persistent fallback, repeated `partialReadInvalidations`, or other anomalies, see [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html).

## Benchmarking

The project commits benchmark baselines for the streaming shuffle. You can regenerate them by setting the environment variable `SPARK_GENERATE_BENCHMARK_FILES=1` when you run the streaming-shuffle benchmark:

```bash
SPARK_GENERATE_BENCHMARK_FILES=1 \
  build/sbt "core/Test/runMain org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
```

The regenerated result files (written under `core/benchmarks/`) let you compare streaming-shuffle latency against the sort-based baseline on your own hardware. Refer to those committed baselines rather than relying on any single number quoted here.

## Related Documentation

* [Streaming Shuffle Guide](streaming-shuffle-guide.html)
* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html)
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html)
* [Spark Configuration](configuration.html#shuffle-behavior)
* [Tuning Spark](tuning.html)
* [Monitoring and Instrumentation](monitoring.html)
