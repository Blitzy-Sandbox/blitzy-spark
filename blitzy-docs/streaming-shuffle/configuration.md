<!--
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
-->

# Streaming Shuffle Configuration

This page is the operator-facing reference for the five new `spark.shuffle.streaming.*` configuration keys introduced by the Streaming Shuffle feature. All five keys are read once at executor startup during `SparkEnv` construction and are treated as immutable for the application lifetime — configuration changes therefore require executor restart (no dynamic reconfiguration is supported in v1).

## Configuration Properties

The five keys below are registered in `core/src/main/scala/org/apache/spark/internal/config/package.scala` via the typed `ConfigBuilder` DSL, immediately following the existing `SHUFFLE_MANAGER` block. The default values keep streaming shuffle fully opt-in: a vanilla Spark deployment with no `spark.shuffle.*` overrides continues to use the production-stable `SortShuffleManager`.

| Property Name | Default | Range | Since Version | Description |
|---------------|---------|-------|---------------|-------------|
| `spark.shuffle.streaming.enabled` | `false` | (boolean) | `4.2.0` | Opt-in flag for streaming shuffle. When true and `spark.shuffle.manager=streaming`, enables pipelined map-to-reduce data transfer with in-memory buffering. |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | 1–50 | `4.2.0` | Percent of executor execution memory reserved for streaming shuffle buffers. |
| `spark.shuffle.streaming.spillThreshold` | `80` | 50–95 | `4.2.0` | Buffer utilization percent that triggers spill to disk via existing `BlockManager` path. |
| `spark.shuffle.streaming.maxBandwidthMBps` | `-1` | -1 (unlimited) or any positive integer | `4.2.0` | Per-executor outbound bandwidth cap in MB/s for streaming shuffle, enforced via token bucket. -1 means unlimited. |
| `spark.shuffle.streaming.debug` | `false` | (boolean, internal) | `4.2.0` | Enable verbose debug tracing for streaming shuffle. Disabled by default to honor the <10 MB/hour log-volume budget. |

Notes on the table values:

- **`spark.shuffle.streaming.enabled`** governs whether the streaming code path is permitted to operate; it does NOT by itself cause `StreamingShuffleManager` to be loaded. The manager is loaded by `spark.shuffle.manager=streaming` (see [Activation](#activation) below). When the manager is loaded but `spark.shuffle.streaming.enabled=false`, the manager transparently delegates every shuffle to its private `SortShuffleManager` collaborator.
- **`spark.shuffle.streaming.bufferSizePercent`** is enforced at registration time: the per-partition buffer is `(executorExecutionMemory × bufferSizePercent / 100) / numPartitions`. Allocations are acquired through `MemoryConsumer.acquireMemory(...)` so they participate in unified-memory accounting.
- **`spark.shuffle.streaming.spillThreshold`** is sampled by `MemorySpillManager` at 100 ms intervals; the LRU-selected partition is persisted via `BlockManager.putBytes` with `StorageLevel.DISK_ONLY` and a `ShuffleBlockId(shuffleId, mapId, reduceId)` key.
- **`spark.shuffle.streaming.maxBandwidthMBps`** is enforced by a per-`BackpressureProtocol`-instance token bucket whose refill rate is `maxBandwidthMBps / numConcurrentShuffles`. The default `-1` disables the rate limiter entirely; any non-negative integer N enables the cap at N MB/s.
- **`spark.shuffle.streaming.debug`** is registered with `.internal()` in the `ConfigBuilder` DSL, signalling diagnostic-only intent. Operators may still set it, but it is hidden from public configuration tables. INFO/DEBUG log lines are gated on this flag to honor the 10 MB/hour/executor log-volume budget.

## Activation

To enable streaming shuffle, set `spark.shuffle.manager=streaming` in your `spark-defaults.conf` or pass `--conf spark.shuffle.manager=streaming` to `spark-submit` / `spark-shell`. The short alias `streaming` is registered in the `getShuffleManagerClassName` companion-object method of `org.apache.spark.shuffle.ShuffleManager` alongside the existing `sort` (default) and `tungsten-sort` aliases. Setting `spark.shuffle.streaming.enabled=true` without changing `spark.shuffle.manager` will NOT activate the streaming path; both flags work in tandem.

```bash
# Example activation
bin/spark-shell \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80
```

For users who prefer fully-qualified class names, the equivalent invocation is:

```bash
bin/spark-shell \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.streaming.StreamingShuffleManager \
  --conf spark.shuffle.streaming.enabled=true
```

Both forms are equivalent; the short alias is purely a convenience supplied by the `shortShuffleMgrNames` lookup table.

## Configuration Immutability

Configuration changes require executor restart (no dynamic reconfiguration in v1). The `StreamingShuffleManager` reads its configuration once during `SparkEnv` construction and is treated as an immutable singleton for the application lifetime. To change any `spark.shuffle.streaming.*` value, restart the affected executors.

This immutability discipline applies uniformly to all five keys above. Setting a `spark.shuffle.streaming.*` value via `SparkContext.setLocalProperty` or after the application has started has no effect on shuffle behavior — the manager has already captured its configuration. For long-running applications that need to adjust streaming-shuffle parameters, use Spark's existing dynamic-allocation or executor-restart mechanisms rather than attempting in-place reconfiguration.

## Coexistence with `SortShuffleManager`

Streaming shuffle coexists with the production-stable `SortShuffleManager`. The default value of `spark.shuffle.manager` remains `sort`. Setting it to `streaming` selects the new `StreamingShuffleManager`, which holds a private `SortShuffleManager` instance for transparent fallback when the `StreamingShuffleFallbackPolicy` triggers (consumer sustained 2x slower than producer for >60 seconds, memory pressure preventing buffer allocation, network saturation >90% link capacity, or producer/consumer version mismatch). No existing application is affected unless `spark.shuffle.manager` is explicitly set to `streaming`.

The fallback delegation is intentionally invisible to the rest of Spark: the `StreamingShuffleManager` returns the same `MapStatus` payload, registers shuffles through the same `MapOutputTracker` path, and emits the same `ShuffleReadMetricsReporter` / `ShuffleWriteMetricsReporter` calls regardless of which inner manager handled the shuffle. Operators see only an increment in the appropriate `shuffle.streaming.*` counter when fallback occurs (see [Observability](observability.md)).

## See Also

- [Feature overview](index.md)
- [Architecture and Mermaid diagrams](architecture.md)
- [Decision log and traceability matrix](decision-log.md)
- [Observability — metrics, MDC, dashboard, runbook](observability.md)
- [Executive summary slide deck](executive-summary.html)
