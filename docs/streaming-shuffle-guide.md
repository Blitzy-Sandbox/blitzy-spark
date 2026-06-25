---
layout: global
title: "Streaming Shuffle Guide"
displayTitle: "Streaming Shuffle Guide"
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

Streaming Shuffle is an opt-in shuffle implementation that streams shuffle data directly from
producer (map) tasks to consumer (reduce) tasks through bounded in-memory buffers, targeting a
30–50% end-to-end latency reduction for shuffle-heavy workloads. It **coexists with** the
default sort-based shuffle and **automatically falls back** to it under adverse conditions, so
enabling it carries no correctness risk. For the internal design, see the
[Streaming Shuffle Architecture](streaming-shuffle-architecture.html).

## Enabling Streaming Shuffle

Streaming Shuffle engages **only when both** of the following properties are set:

* `spark.shuffle.manager=streaming`
* `spark.shuffle.streaming.enabled=true`

This is a deliberate dual-flag activation contract. If either flag is missing or `false`, Spark
uses the default sort-based shuffle (`spark.shuffle.manager` defaults to `sort`). In particular,
setting only `spark.shuffle.manager=streaming` **without** `spark.shuffle.streaming.enabled=true`
still results in the manager delegating to the inner sort path; **both** flags are required to
activate streaming.

You can supply the configuration in any of the standard ways.

Using `spark-submit` on the command line:

```bash
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80 \
  --class com.example.MyApp myapp.jar
```

Programmatically through `SparkConf`:

```scala
val conf = new SparkConf()
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
```

In `conf/spark-defaults.conf`:

```text
spark.shuffle.manager            streaming
spark.shuffle.streaming.enabled  true
```

**Important:** the streaming shuffle configuration is **immutable for the application lifetime** —
there is no dynamic reconfiguration in this version. Set these properties **before** the
application starts. See [Submitting Applications](submitting-applications.html) and
[Spark Configuration](configuration.html#dynamically-loading-spark-properties) for the available
mechanisms to load properties at startup.

## Configuration

The streaming shuffle adds the configuration keys listed below, alongside the existing
`spark.shuffle.manager` selector. All five `spark.shuffle.streaming.*` keys are new in this
release; range-checked values are validated when the application starts.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.manager</code></td>
  <td>sort</td>
  <td>
    Which shuffle manager to use. Available options are <code>sort</code>,
    <code>tungsten-sort</code>, and <code>streaming</code>. Set to <code>streaming</code>
    (together with <code>spark.shuffle.streaming.enabled=true</code>) to enable the streaming
    shuffle.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Opt-in master switch for the streaming shuffle. Must be <code>true</code> <em>and</em>
    <code>spark.shuffle.manager=streaming</code> for streaming to activate; otherwise the
    sort-based shuffle is used.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percentage of executor memory (1–50) used for per-partition streaming buffers.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer-utilization percentage (50–95) at which the largest partitions spill to disk.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0</td>
  <td>
    Per-executor rate limit in MB/s for streamed shuffle data; <code>0</code> means unlimited.
    The effective rate is capped at 80% of link capacity.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>
    Enables verbose debug logging for the streaming shuffle subsystem.
  </td>
  <td>4.2.0</td>
</tr>
</table>

Detailed tuning guidance for `spark.shuffle.streaming.bufferSizePercent`,
`spark.shuffle.streaming.spillThreshold`, and `spark.shuffle.streaming.maxBandwidthMBps` lives in
[Streaming Shuffle Tuning](streaming-shuffle-tuning.html).

## When to Use Streaming Shuffle

* **Best fit — shuffle-heavy workloads.** Jobs that shuffle at least **100 MB** of data across at
  least **10 partitions** benefit the most, where streaming shuffle targets a **30–50%**
  end-to-end latency reduction versus the sort-based shuffle.
* **Modest benefit — CPU-bound workloads.** Jobs dominated by computation rather than data
  movement typically see roughly a **5–10%** improvement.
* **No regression — memory-bound workloads.** When executors lack the memory headroom for
  streaming buffers, the subsystem automatically falls back to sort, yielding **zero regression**
  relative to the default.
* **When not to use / caveats.** Very small shuffles (little data, few partitions) gain little
  from streaming. If your executors are memory-constrained, the subsystem will frequently fall
  back, so the sort default is preferable and simpler to operate. Streaming shuffle is
  **complementary to — not a replacement for** — push-based shuffle
  (`spark.shuffle.push.enabled`), which targets large, long-running YARN jobs; streaming shuffle
  targets latency-sensitive jobs.

## How It Coexists With and Falls Back to Sort

The streaming shuffle is strictly additive. The `streaming` manager wraps an inner
`SortShuffleManager` by composition: the default sort-based shuffle path is fully preserved, and
the same on-disk block resolver is shared between the two managers, so block migration and
executor decommissioning continue to work exactly as before.

When streaming is active, the subsystem continuously monitors the data path and **automatically
reverts to the sort-based shuffle — transparently and without data loss** — under any of the
following four conditions:

1. The consumer is sustained at **2×** slower than the producer for more than **60 s**.
2. Memory pressure prevents buffer allocation (OOM risk).
3. Network saturation exceeds **90%** of link capacity.
4. A producer/consumer version mismatch is detected.

Because fallback reuses the unchanged sort path and Spark's existing recomputation machinery,
correctness is preserved in every failure scenario. See
[Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) for the full
failure-handling behavior and the metrics that surface each condition.

## Verifying Streaming Shuffle Is Active

To confirm that streaming shuffle is actually running for your application:

* **Watch the streaming metrics.** The subsystem emits metrics under the `shuffle.streaming.`
  namespace — for example `bufferUtilizationPercent`, `spillCount`, `backpressureEvents`, and
  `partialReadInvalidations`. These are exposed through the existing metrics sinks (such as JMX
  and Prometheus); see [Monitoring and Instrumentation](monitoring.html) for how to enable and
  scrape them.
* **Check the Spark UI.** The shuffle read/write/spill columns on the Stages tab reflect streaming
  activity through the same channels as the sort-based shuffle; see the
  [Web UI](web-ui.html) guide.
* **Enable debug logging.** Set `spark.shuffle.streaming.debug=true` to emit verbose logs from the
  streaming shuffle subsystem, which record activation, spill, backpressure, and fallback events.

If you see no `shuffle.streaming.` metrics and no streaming log lines, verify that **both**
`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true` are set, since the
application otherwise runs the default sort-based shuffle.

## Related Documentation

* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html)
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html)
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html)
* [Spark Configuration](configuration.html#shuffle-behavior)
* [Tuning Spark](tuning.html)
* [Submitting Applications](submitting-applications.html)
