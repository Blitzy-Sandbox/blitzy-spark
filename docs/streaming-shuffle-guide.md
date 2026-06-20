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

Streaming Shuffle is an **opt-in** shuffle backend for Spark Core that streams map-side output
directly to reduce-side consumers through in-memory buffers and Spark's existing network transport,
reducing the latency of shuffle-heavy workloads. It is disabled by default and coexists with the
standard sort-based shuffle, automatically falling back to it whenever streaming is unsuitable, so
the default behavior of every existing Spark application is unchanged. This guide explains how to
enable streaming shuffle, the configuration keys that control it, when it helps, and how the
automatic fallback keeps your jobs safe.

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Streaming Shuffle is an alternative implementation of Spark's `ShuffleManager` service-provider
interface. Instead of fully materializing every map task's output to local disk before any reduce
task begins fetching, the streaming backend buffers map output in memory and pipelines it to
reduce-side consumers over Spark's existing network transport layer. For shuffle-heavy workloads
this removes the shuffle-materialization barrier and can substantially reduce end-to-end latency.

The backend is delivered as a self-contained, opt-in capability that **coexists with** the default
sort-based shuffle (`SortShuffleManager`). When streaming is not enabled, or when runtime conditions
make streaming unsuitable, Spark transparently uses the sort-based path instead. This is the
zero-regression guarantee: turning the feature on can only help eligible workloads, and it never
changes the behavior of workloads that are left on the default path. For the internal design and
component interactions, see the [architecture overview](streaming-shuffle-architecture.html).

# Enabling Streaming Shuffle

Activating streaming shuffle requires **both** of the following configuration signals. If either one
is missing, Spark uses the default sort-based shuffle:

* `spark.shuffle.manager=streaming` &mdash; selects `StreamingShuffleManager` as the shuffle backend
  (this is an existing Spark property whose default value is `sort`).
* `spark.shuffle.streaming.enabled=true` &mdash; the feature flag that turns the streaming code path
  on inside the manager.

Both signals default to off. Setting only `spark.shuffle.manager=streaming` (with the feature flag
left at its default `false`) still results in the sort-based shuffle, because `StreamingShuffleManager`
delegates to its inner `SortShuffleManager` until the feature flag is explicitly enabled. Requiring
both signals makes activation deliberate and keeps the default cluster behavior byte-for-byte
unchanged.

Spark properties can be supplied in any of the usual ways. The examples below mirror the patterns in
the [Configuration](configuration.html) guide.

## Using spark-submit

Pass the flags on the `spark-submit` command line with `--conf`:

```bash
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80 \
  --class com.example.MyApp \
  myApp.jar
```

## Using spark-defaults.conf

Add the properties to `conf/spark-defaults.conf`, one key-value pair per line separated by
whitespace, so they apply to every application submitted from that client:

```properties
spark.shuffle.manager                       streaming
spark.shuffle.streaming.enabled             true
spark.shuffle.streaming.bufferSizePercent   20
spark.shuffle.streaming.spillThreshold      80
spark.shuffle.streaming.maxBandwidthMBps    0
```

## Using SparkConf programmatically

Set the properties on a `SparkConf` before creating the `SparkContext` (or `SparkSession`):

```scala
val conf = new SparkConf()
  .setAppName("MyApp")
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
  .set("spark.shuffle.streaming.bufferSizePercent", "20")
  .set("spark.shuffle.streaming.spillThreshold", "80")
val sc = new SparkContext(conf)
```

Streaming shuffle configuration is **immutable for the lifetime of the application**. There is no
dynamic reconfiguration in this version: to change any `spark.shuffle.streaming.*` value (or to turn
the feature on or off), stop the application and restart it &mdash; which restarts the executors &mdash;
with the new settings.

# Configuration reference

The streaming backend is selected with the existing `spark.shuffle.manager` property, which defaults
to `sort`. Set it to `streaming` to choose `StreamingShuffleManager`; any other value selects the
corresponding built-in manager and the streaming keys below have no effect. The five
`spark.shuffle.streaming.*` properties tune the streaming backend once it is selected and enabled:

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

For guidance on choosing values for `bufferSizePercent`, `spillThreshold`, and `maxBandwidthMBps`,
see the [tuning guide](streaming-shuffle-tuning.html).

# When to use it

Streaming shuffle targets workloads where the cost of materializing intermediate shuffle data
dominates end-to-end latency. Use the following guidance to decide whether to enable it:

* **Shuffle-heavy workloads** &mdash; jobs that move a substantial amount of intermediate data
  (roughly &ge; 100 MB) across a reasonable number of partitions (roughly &ge; 10) benefit the most.
  For these workloads you can expect a 30&ndash;50% reduction in end-to-end shuffle latency.
* **CPU-bound workloads** &mdash; jobs whose runtime is dominated by computation rather than data
  movement see a more modest 5&ndash;10% improvement, primarily from reduced scheduler overhead.
* **Memory-bound workloads** &mdash; jobs that are already close to their memory limits may see no
  benefit, because the backend automatically falls back to sort-based shuffle when buffers cannot be
  allocated safely. It is still safe to leave streaming enabled for these jobs: the automatic
  fallback guarantees zero regression (see [Automatic fallback behavior](#automatic-fallback-behavior)).

Because activation is global to the application, a practical approach is to enable streaming shuffle
for applications dominated by large shuffles and rely on the automatic fallback for the stages that
do not benefit. See the [tuning guide](streaming-shuffle-tuning.html) for sizing buffers and the
spill threshold to match your workload and executor memory.

# Automatic fallback behavior

Even when streaming shuffle is enabled, the backend continuously evaluates whether streaming remains
the right choice and **automatically reverts to the sort-based shuffle** when any one of the
following four conditions trips:

1. **Slow consumer** &mdash; the consumer (reduce side) is sustained at 2&times; slower than the
   producer (map side) for more than 60 seconds.
2. **Memory pressure** &mdash; available memory is insufficient to allocate buffers safely (OOM risk,
   that is, utilization above 95%).
3. **Network saturation** &mdash; the network link is more than 90% of its capacity.
4. **Version mismatch** &mdash; the producer and consumer report incompatible streaming-protocol
   versions.

When any condition trips, the backend delegates to the existing `SortShuffleManager` with no job
failure and no user action required. The fallback is transparent: affected stages simply use the
sort-based path, while stages that remain eligible continue to stream. This is the **zero-regression
guarantee** &mdash; enabling streaming shuffle cannot make an unsuitable workload slower than the
default sort-based shuffle.

# Verifying it is active

To confirm that streaming shuffle is engaged for a running application, use the standard Spark
observability surfaces:

* **Metrics** &mdash; the backend emits four `shuffle.streaming.*` metrics (a buffer-utilization
  gauge plus counters for spills, backpressure events, and partial-read invalidations). They are
  exposed through Spark's existing metrics endpoints, including JMX and the Prometheus endpoint at
  `/metrics/executors/prometheus`. Non-zero streaming metrics confirm the path is active. See
  [Monitoring](monitoring.html) for how to scrape and view these metrics.
* **Web UI** &mdash; the Stages tab shuffle columns (Shuffle Read / Shuffle Write) reflect shuffle
  activity for stages handled by the streaming backend, just as they do for sort-based shuffle.
* **Debug logging** &mdash; set `spark.shuffle.streaming.debug=true` to emit additional diagnostic
  log lines from the streaming path, which is useful when confirming activation or diagnosing
  fallback.

If the streaming metrics remain at zero while the flags are set, the backend is most likely on the
automatic fallback path; the [troubleshooting guide](streaming-shuffle-troubleshooting.html)
explains how to interpret the metrics and identify which fallback condition was triggered.

# Failure handling

Streaming shuffle preserves Spark's existing zero-data-loss guarantees. On a producer failure, the
reader detects a connection timeout (5 seconds), atomically invalidates all partial reads from the
failed producer, and raises a `FetchFailedException`. Spark's existing lineage and fault-recovery
machinery then recomputes the lost upstream output and retries the read &mdash; exactly as it does
for the sort-based shuffle &mdash; so no committed data is lost. The full producer- and
consumer-failure flows, including the buffering and retransmission behavior, are documented in the
[troubleshooting guide](streaming-shuffle-troubleshooting.html).

# Compatibility and limitations

* **Backend-only change** &mdash; streaming shuffle is implemented entirely within the
  `ShuffleManager` abstraction in Spark Core. It introduces no changes to the RDD/DataFrame/Dataset
  APIs, the DAG scheduler, executor lifecycle, or the lineage and fault-recovery model.
* **No new dependencies** &mdash; the backend reuses libraries already on the Spark classpath; adding
  it requires no additional artifacts in your deployment.
* **Security** &mdash; the streaming path inherits Spark's existing shuffle security, including
  authentication (SASL) and TLS, and introduces no new externally reachable network endpoints.
* **Immutable configuration** &mdash; all streaming settings are fixed for the lifetime of the
  application; changing them requires an application/executor restart (no dynamic reconfiguration in
  this version).
* **Default behavior unchanged** &mdash; when the activation flags are off, the existing sort-based
  shuffle is used and is completely unaffected by the presence of the streaming backend.

# Related documentation

* [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) &mdash; internal design,
  components, and data flow.
* [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) &mdash; sizing buffers, the spill
  threshold, and the bandwidth cap.
* [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) &mdash; diagnosing
  fallback, failures, and performance issues.
* [Configuration](configuration.html) &mdash; the full Spark configuration reference.
* [Monitoring and Instrumentation](monitoring.html) &mdash; metrics endpoints and dashboards.
* [RDD Programming Guide](rdd-programming-guide.html#shuffle-operations) &mdash; background on Spark
  shuffle operations.
