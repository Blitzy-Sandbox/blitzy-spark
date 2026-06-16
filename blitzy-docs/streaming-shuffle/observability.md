# Observability

Streaming Shuffle **ships observability with the implementation** rather than bolting monitoring on afterward. Wherever Spark already provides a capability the backend **reuses** it as-is — the SLF4J/Log4j2 logging stack, the executor `MetricsSystem`, the JMX and Prometheus endpoints, the executor health surface, and the existing shuffle security (authentication/SASL and TLS) — and on top of that foundation it **adds** exactly four streaming-specific metrics, structured logging with correlation IDs, and a ready-to-import Grafana dashboard template (`dashboard.json`, in this folder). This page is the explicit **reused-vs-added ledger** for the feature.

## Reused

The following existing Spark infrastructure is **reused as-is** — the streaming backend integrates with it rather than replacing or duplicating it:

- **SLF4J / Log4j2 logging stack** — the backend logs through Spark's existing SLF4J facade and Log4j2 backend; no new logging framework is introduced.
- **Executor `MetricsSystem` (Dropwizard/Codahale Metrics)** — the four streaming metrics are registered with the same executor-side metrics registry Spark already runs; no parallel metrics pipeline is created.
- **Existing metrics endpoints — JMX and the Prometheus servlet sink** — the streaming metrics surface through the `MetricsSystem` sinks Spark already exposes: **JMX** (via `JmxSink`) and the **Prometheus servlet sink** (`PrometheusServlet`, default path **`/metrics/prometheus`**) when that sink is enabled in `metrics.properties`. No new metrics endpoint is added. (Note: the built-in executor-summary endpoint `/metrics/executors/prometheus` is *not* a `MetricsSystem` sink — it is served by `status.api.v1.PrometheusResource` from the `AppStatusStore` and exposes only Spark's fixed per-executor summary metrics, so it does not carry these custom source metrics; see the [endpoint note](#which-endpoint-exposes-the-custom-metrics) below.)
- **Executor health surface for readiness** — readiness is observed through Spark's existing executor health surface; the backend does not add a separate health/readiness probe.
- **Existing shuffle security — authentication/SASL and TLS** — the streaming data path inherits Spark's existing shuffle security (authentication/**SASL** and **TLS**) via the existing transport configuration. It introduces **no new network endpoints** beyond the executor-scoped backpressure RPC, so existing security controls apply unchanged.

Nothing in the metrics framework itself is modified: the backend only **registers** a new source with the existing `MetricsSystem` and uses the established logging and security surfaces — the frameworks are consumed through their public integration points, not altered.

## Added

On top of the reused infrastructure above, Streaming Shuffle adds three things: four streaming-specific metrics, structured logging with correlation IDs, and a Grafana dashboard template.

### The four `shuffle.streaming.*` metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `bufferUtilizationPercent` | gauge | Current aggregate buffer utilization, as a percentage (0–100), across the per-partition in-memory buffers. |
| `spillCount` | counter | Number of disk-spill events triggered when buffer utilization reaches the spill threshold. |
| `backpressureEvents` | counter | Number of producer-throttling (backpressure) events applied to slow consumers down the producer. |
| `partialReadInvalidations` | counter | Number of partial reads invalidated on producer failure/timeout (each surfaces a `FetchFailedException`). |

These four metrics are emitted under the **`shuffle.streaming.*`** namespace via a **`StreamingShuffleSource`** — an implementation of **`org.apache.spark.metrics.source.Source`** — that is **registered with the executor `MetricsSystem`**. Because the source plugs into the existing metrics registry, the metrics surface through **every configured `MetricsSystem` sink**: notably **JMX** (via `JmxSink`) and the **Prometheus servlet sink** (`PrometheusServlet`, default path `/metrics/prometheus`) when it is enabled in `metrics.properties`. This is the same `StreamingShuffleMetrics → StreamingShuffleSource → MetricsSystem` path shown in the component-interaction diagram on the [Architecture](architecture.md) page.

#### Which endpoint exposes the custom metrics

The four `shuffle.streaming.*` values are a **custom `MetricsSystem` `Source`**, so they appear wherever the `MetricsSystem` reports — i.e. through the **enabled sinks** (JMX and the `PrometheusServlet` sink at `/metrics/prometheus`). They do **not** appear on the built-in **`/metrics/executors/prometheus`** endpoint: that endpoint is served by `org.apache.spark.status.api.v1.PrometheusResource` directly from the `AppStatusStore` and emits only Spark's **fixed per-executor summary** metrics (`rddBlocks`, `memoryUsed`, `totalShuffleRead`/`totalShuffleWrite`, peak-memory, GC, etc.) — it does not read the `MetricsSystem` registry, so no custom source metric (streaming or otherwise) is exposed there. Likewise, the Web UI **Stages** tab shows the **standard** shuffle read/write byte columns (overall shuffle activity), not these four custom source metrics. To scrape `shuffle.streaming.*`, enable the `PrometheusServlet` sink (see [`metrics.properties.template`](../../core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template)) and read `/metrics/prometheus`, or read them over JMX.

### Structured logging with correlation IDs

Streaming-specific events are emitted as **structured logging** through the reused SLF4J/Log4j2 stack, carrying **MDC (Mapped Diagnostic Context) correlation keys** so a single shuffle can be traced end-to-end across producer and consumer executors. MDC key strings are the **lowercased names** of their `LogKey` — Spark lowercases each `LogKey.name` when it populates the MDC. The backend emits exactly the four correlation keys the cross-cutting Observability rule names — **`shuffle_id`**, **`map_id`**, **`reduce_partition_range`**, and **`attempt_id`**:

- **`shuffle_id`** (`LogKeys.SHUFFLE_ID`) — the shuffle the log line belongs to.
- **`map_id`** (`LogKeys.MAP_ID`) — the producing (map-side) task.
- **`reduce_partition_range`** (`StreamingLogKeys.REDUCE_PARTITION_RANGE`) — the consuming (reduce-side) **partition range**, formatted `[start,end)` (start inclusive, end exclusive); emitted on the reader's read path, the producer-side writer, and `openConsumerStream`.
- **`attempt_id`** (`StreamingLogKeys.ATTEMPT_ID`) — the task attempt, distinguishing retries.
- **`reduce_id`** (`LogKeys.REDUCE_ID`) — the single reduce partition (not a range), on per-block log lines such as checksum-mismatch and partial-read-invalidation lines.

> **How the four required keys are emitted byte-exact.** The cross-cutting Observability rule names the keys `shuffle_id`, `map_id`, `reduce_partition_range`, and `attempt_id`. `shuffle_id` and `map_id` already exist in the central `org.apache.spark.internal.LogKeys` enum and are **reused unchanged** (`SHUFFLE_ID`, `MAP_ID`). The other two have no canonical equivalent, so they are provided through Spark's **documented custom-`LogKey` extension mechanism** (see `org.apache.spark.internal.Logging`): a small enum, **`StreamingLogKeys`** (`core/src/main/java/org/apache/spark/shuffle/streaming/StreamingLogKeys.java`), declares `REDUCE_PARTITION_RANGE` and `ATTEMPT_ID` **inside this feature's own package**. This emits the **exact** key names the rule requires while keeping all streaming logic inside the streaming package (zero cross-contamination) and leaving the **shared, frozen `LogKeys.java` untouched** — so the integration footprint remains the two surgical edits in `ShuffleManager.scala` and `internal/config/package.scala`. The spill, fallback, and backpressure paths additionally reuse existing keys such as `partition_id`, `block_id`, `num_bytes`, `count`, `duration`, `threshold`, and `reason`.

Setting **`spark.shuffle.streaming.debug=true`** raises log verbosity for diagnostics. It is off by default and should remain off in production to stay within the log-volume budget described under [Constraints](#constraints); see [Configuration](configuration.md) for the flag's definition.

### Grafana dashboard template

**`dashboard.json`** (in this same folder) is the provided **Grafana dashboard template**: a **2×2 grid of four panels**, one panel per `shuffle.streaming.*` metric, importable against a **Prometheus datasource**. It visualizes **`bufferUtilizationPercent`** as a **gauge with the 80% spill threshold marked**, alongside the three counters — **`spillCount`**, **`backpressureEvents`**, and **`partialReadInvalidations`**. Its panels query the metrics by name (`__name__=~".*shuffle.streaming.<metric>.*"`), so it works against any Prometheus datasource that scrapes the `PrometheusServlet` sink (`/metrics/prometheus`); enabling that sink (above) is the only metrics-configuration prerequisite.

## Constraints

The added telemetry is governed by strict budget invariants so that observability never becomes a performance liability:

- **Telemetry overhead < 1% executor CPU** — metric collection and structured logging together must consume less than one percent of executor CPU.
- **Log volume < 10 MB/hour/executor** — structured logging must stay under ten megabytes per hour per executor. Keep **`spark.shuffle.streaming.debug` off in production**, since debug verbosity is intended for short diagnostic windows, not steady-state operation.

## Verifying metric emission

Confirming that the four `shuffle.streaming.*` metrics actually emit in a **local development run** is part of the observability acceptance for this feature. To verify locally:

1. **Enable streaming** — set **both** activation signals so the streaming backend (not the sort-based fallback) is engaged:

   ```properties
   spark.shuffle.manager=streaming
   spark.shuffle.streaming.enabled=true
   ```

2. **Run a small shuffle job** — execute any job that performs a shuffle (for example, a `groupBy`/`reduceByKey` over a small dataset) so the backend allocates buffers and produces shuffle traffic.
3. **Enable a sink and scrape the metrics** — confirm the four `shuffle.streaming.*` metrics appear through a configured `MetricsSystem` sink (they do **not** appear on `/metrics/executors/prometheus` — see the [endpoint note](#which-endpoint-exposes-the-custom-metrics) above):
   - the **Prometheus servlet sink** at **`/metrics/prometheus`** — enable `PrometheusServlet` in `metrics.properties` (see [`metrics.properties.template`](../../core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template)), then `curl http://<driver-or-executor-metrics-ui>/metrics/prometheus | grep -i shuffle_streaming`, or
   - **JMX** (via the executor's registered MBeans, when `JmxSink` is enabled).

Seeing `bufferUtilizationPercent` move and the counters increment confirms the `StreamingShuffleSource → MetricsSystem` wiring is live. This local-dev metric-emission verification is a required part of the observability acceptance, not an optional check.

## See also

- [Architecture](architecture.md) — the component-interaction diagram shows the `StreamingShuffleMetrics → StreamingShuffleSource → MetricsSystem` registration path described above.
- [Configuration](configuration.md) — the five `spark.shuffle.streaming.*` keys, including `spark.shuffle.streaming.debug`, and the activation alias.
- Back to the [overview](index.md).
