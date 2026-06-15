---
layout: global
title: Streaming Shuffle User Guide
displayTitle: Streaming Shuffle User Guide
description: User guide for enabling and using the streaming shuffle backend in Spark SPARK_VERSION_SHORT
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

Streaming Shuffle is an opt-in shuffle backend for Spark Core that streams map-side output directly to
reduce-side consumers through in-memory buffers and Spark's existing network transport, reducing the
end-to-end latency of shuffle-heavy workloads. It coexists with the default sort-based shuffle and
automatically falls back to it whenever conditions are unfavorable, so enabling it never changes a
job's results and never regresses workloads that are not a good fit. This guide explains how to enable
the feature, what each configuration key does, when it helps, and how to confirm it is active. For the
internal design, see the [architecture overview](streaming-shuffle-architecture.html).

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

The streaming shuffle backend pipelines intermediate shuffle data from producer (map-side) executors
to consumer (reduce-side) executors as it is produced, buffering it in bounded in-memory buffers and
sending it over Spark's existing network transport instead of fully materializing every map output to
local disk before any fetch begins. For shuffle-heavy jobs this removes much of the
shuffle-materialization latency from the critical path. The backend is fully opt-in: it is engaged only
when you explicitly request it, and even then it automatically falls back to the standard sort-based
shuffle when a workload is not a good fit. Because both activation signals default to off, the default
behavior of every existing Spark deployment is unchanged.

The streaming backend is implemented entirely within the `ShuffleManager` abstraction as
`StreamingShuffleManager`. It composes the existing `SortShuffleManager` as its fallback and reuses
Spark's memory manager, block manager, network transport, and metrics system rather than introducing
parallel machinery. See the [architecture overview](streaming-shuffle-architecture.html) for how the
pieces fit together, and the [RDD Programming Guide](rdd-programming-guide.html#shuffle-operations) for
background on shuffle operations in general.

# Enabling Streaming Shuffle

Activating the streaming shuffle backend requires **both** of the following configuration signals. If
either one is missing, Spark uses the default sort-based shuffle:

* `spark.shuffle.manager=streaming` &mdash; selects `StreamingShuffleManager` as the shuffle manager
  (the `streaming` value is an alias for the streaming backend).
* `spark.shuffle.streaming.enabled=true` &mdash; the feature flag that turns the streaming code path on.

Both default to off: `spark.shuffle.manager` defaults to `sort`, and
`spark.shuffle.streaming.enabled` defaults to `false`. Setting only one of them is not enough &mdash;
for example, selecting `spark.shuffle.manager=streaming` while leaving
`spark.shuffle.streaming.enabled=false` causes the manager to delegate straight to the sort-based
shuffle. You must set **both** to engage streaming.

Spark properties can be supplied in any of the usual ways. The following examples enable the streaming
backend with a 20% buffer.

On the `spark-submit` command line with `--conf` flags:

```bash
./bin/spark-submit \
  --class com.example.MyApp \
  --master spark://master-host:7077 \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80 \
  myApp.jar
```

In `conf/spark-defaults.conf` (one `key value` pair per line, separated by whitespace):

```properties
spark.shuffle.manager                        streaming
spark.shuffle.streaming.enabled              true
spark.shuffle.streaming.bufferSizePercent    20
spark.shuffle.streaming.spillThreshold       80
```

Programmatically on a `SparkConf` before creating the `SparkContext`:

```scala
val conf = new SparkConf()
  .setAppName("MyApp")
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
  .set("spark.shuffle.streaming.bufferSizePercent", "20")
val sc = new SparkContext(conf)
```

The streaming shuffle configuration is **immutable for the lifetime of the application**. There is no
dynamic reconfiguration in this version: changing any of these properties requires restarting the
application (and therefore its executors) for the new values to take effect.

# Configuration reference

The streaming shuffle backend adds the following five `spark.shuffle.streaming.*` properties. They are
read once when the shuffle manager is created and are validated against the ranges shown below.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    When <code>true</code> (and <code>spark.shuffle.manager</code> is set to <code>streaming</code>), enables the
    opt-in streaming shuffle backend. When <code>false</code>, the manager delegates to the sort-based shuffle.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percentage (1&ndash;50) of executor memory used for per-partition streaming buffers. The per-partition
    buffer size is computed as <code>(executorMemory * bufferSizePercent / 100) / numPartitions</code> with a 2 MB floor.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer-utilization percentage (50&ndash;95) at which the largest buffered partitions spill to disk via the
    block manager to reclaim memory.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0 (unlimited)</td>
  <td>
    Per-executor streaming bandwidth cap in MB/s enforced by the token-bucket rate limiter. A value of
    <code>0</code> (or any non-positive value) means unlimited.
  </td>
  <td>4.1.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>
    When <code>true</code>, enables additional diagnostic logging for the streaming shuffle path.
  </td>
  <td>4.1.0</td>
</tr>
</table>

In addition to the keys above, the existing `spark.shuffle.manager` property (default `sort`) must be
set to `streaming` to select this backend. `spark.shuffle.manager` is the standard shuffle-manager
selector documented in the [Configuration](configuration.html) page; `streaming` is simply a registered
alias that resolves to `StreamingShuffleManager`. Setting `spark.shuffle.manager=streaming` alone does
not enable streaming &mdash; you must also set `spark.shuffle.streaming.enabled=true`, as described in
[Enabling Streaming Shuffle](#enabling-streaming-shuffle). For guidance on choosing buffer sizes and
spill thresholds for your workload, see the [tuning guide](streaming-shuffle-tuning.html).

# When to use it

The streaming shuffle backend helps some workloads more than others. Use the following guidance to
decide whether to enable it:

* **Shuffle-heavy workloads** &mdash; the primary target. Jobs that move a substantial amount of
  intermediate data (roughly &ge; 100 MB) across a reasonable number of partitions (roughly &ge; 10)
  benefit the most, with an expected **30&ndash;50% reduction in end-to-end shuffle latency** because
  reduce-side consumers begin receiving data before map output is fully materialized.
* **CPU-bound workloads** &mdash; expect a more modest **5&ndash;10% improvement** from reduced
  scheduler and materialization overhead, rather than from data-movement savings.
* **Memory-bound workloads** &mdash; it is safe to leave streaming enabled. When buffers cannot be
  allocated without risking memory exhaustion, the backend automatically falls back to the sort-based
  shuffle, so there is **zero regression**. Such workloads may simply see no benefit from streaming.

If you are unsure, enabling streaming is low risk because of the automatic fallback described below.
For help sizing the per-partition buffers (`spark.shuffle.streaming.bufferSizePercent`) and the spill
threshold (`spark.shuffle.streaming.spillThreshold`) for your cluster, see the
[tuning guide](streaming-shuffle-tuning.html).

# Automatic fallback behavior

Even when streaming is enabled, the backend continuously evaluates whether streaming is still the right
choice and **automatically reverts to the sort-based shuffle** when any of the following four conditions
trips:

1. **Slow consumer** &mdash; a consumer is sustained at 2&times; slower than its producer for more than
   60 seconds.
2. **Memory pressure** &mdash; memory pressure prevents buffer allocation (OOM risk), i.e. utilization
   exceeds 95%.
3. **Network saturation** &mdash; network utilization exceeds 90% of link capacity.
4. **Version mismatch** &mdash; the producer and consumer report incompatible streaming-protocol
   versions.

When a fallback condition trips, `StreamingShuffleManager` delegates to the composed, unchanged
`SortShuffleManager`. The fallback is **transparent**: there is no job failure and no user action is
required &mdash; the shuffle simply proceeds on the sort-based path. This is the feature's
zero-regression guarantee: workloads that are not a good fit for streaming behave exactly as they would
with the default shuffle.

# Verifying it is active

Because the backend can fall back transparently, it is useful to confirm whether streaming shuffle is
actually engaged for a given application. There are several complementary ways to check:

* **Streaming shuffle metrics** &mdash; the backend emits four `shuffle.streaming.*` metrics through
  Spark's existing metrics system: a buffer-utilization gauge plus counters for spills, backpressure
  events, and partial-read invalidations. Non-zero activity on these metrics indicates the streaming
  path is in use. They are available wherever you already consume Spark metrics, including JMX and the
  Prometheus endpoint at `/metrics/executors/prometheus`.
* **Web UI Stages tab** &mdash; the shuffle read/write columns on the Stages tab reflect shuffle
  activity for streaming-enabled stages just as they do for sort-based shuffle.
* **Debug logging** &mdash; set `spark.shuffle.streaming.debug=true` to emit additional diagnostic log
  lines for the streaming code path, which make it easy to see when streaming is selected and when a
  fallback occurs. Leave this off in production to keep log volume low.

See the [Monitoring](monitoring.html) page for how to configure metric sinks and the Prometheus
endpoint, and the [troubleshooting guide](streaming-shuffle-troubleshooting.html) if streaming does not
appear to be active when you expect it.

# Failure handling (summary)

The streaming backend is designed for **zero data loss** under failure. On a producer failure, the
reduce-side reader detects a connection timeout (5 seconds), atomically invalidates any partial reads
from the failed producer, discards the affected buffered data, and surfaces a `FetchFailedException`.
Spark's existing lineage and fault-recovery machinery then recomputes the lost map output and retries
the fetch, exactly as it does for the sort-based shuffle. No streaming-specific recovery action is
required from the user. For the full producer- and consumer-failure protocols and how to diagnose
fetch failures, see the [troubleshooting guide](streaming-shuffle-troubleshooting.html).

# Compatibility and limitations

* **Backend-only change** &mdash; streaming shuffle is a Spark Core change confined to the
  `ShuffleManager` abstraction. It does not change RDD/DataFrame/Dataset APIs, the DAG scheduler, the
  task model, or the lineage and fault-recovery model.
* **No new dependencies** &mdash; the backend reuses libraries and components already on the Spark Core
  classpath; enabling it does not add any third-party dependency to your application.
* **Security reuse** &mdash; the streaming data path inherits Spark's existing shuffle security. Network
  authentication/SASL (`spark.authenticate`) and TLS apply to streaming shuffle exactly as they do to
  the sort-based path; no separate security configuration is introduced.
* **Immutable configuration** &mdash; as noted in [Enabling Streaming Shuffle](#enabling-streaming-shuffle),
  the streaming configuration is fixed for the lifetime of the application; changing it requires an
  application/executor restart.
* **Default behavior unchanged** &mdash; when the activation flags are off (the default), the existing
  sort-based shuffle is used and is completely unaffected by the presence of the streaming backend.

# Related documentation

* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) &mdash; internal design,
  components, and data flow.
* [Streaming Shuffle Tuning Guide](streaming-shuffle-tuning.html) &mdash; sizing buffers, spill
  thresholds, and bandwidth caps.
* [Streaming Shuffle Troubleshooting Guide](streaming-shuffle-troubleshooting.html) &mdash; diagnosing
  fallbacks, fetch failures, and performance issues.
* [Configuration](configuration.html) &mdash; the full list of Spark properties, including
  `spark.shuffle.manager` and other shuffle settings.
* [Monitoring](monitoring.html) &mdash; metrics sinks, JMX, and the Prometheus endpoints used to observe
  the `shuffle.streaming.*` metrics.
