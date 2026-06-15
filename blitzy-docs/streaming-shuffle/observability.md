# Observability

Streaming Shuffle **ships observability with the implementation** rather than bolting monitoring on afterward. Wherever Spark already provides a capability the backend **reuses** it as-is — the SLF4J/Log4j2 logging stack, the executor `MetricsSystem`, the JMX and Prometheus endpoints, the executor health surface, and the existing shuffle security (authentication/SASL and TLS) — and on top of that foundation it **adds** exactly four streaming-specific metrics, structured logging with correlation IDs, and a ready-to-import Grafana dashboard template (`dashboard.json`, in this folder). This page is the explicit **reused-vs-added ledger** for the feature.

## Reused

The following existing Spark infrastructure is **reused as-is** — the streaming backend integrates with it rather than replacing or duplicating it:

- **SLF4J / Log4j2 logging stack** — the backend logs through Spark's existing SLF4J facade and Log4j2 backend; no new logging framework is introduced.
- **Executor `MetricsSystem` (Dropwizard/Codahale Metrics)** — the four streaming metrics are registered with the same executor-side metrics registry Spark already runs; no parallel metrics pipeline is created.
- **Existing metrics endpoints — JMX and Prometheus** — the streaming metrics surface through the channels Spark already exposes, including the executor Prometheus endpoint **`/metrics/executors/prometheus`**. No new metrics endpoint is added.
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

These four metrics are emitted under the **`shuffle.streaming.*`** namespace via a **`StreamingShuffleSource`** — an implementation of **`org.apache.spark.metrics.source.Source`** — that is **registered with the executor `MetricsSystem`**. Because the source plugs into the existing metrics registry, the metrics surface automatically through **JMX** and the **Prometheus** endpoint (`/metrics/executors/prometheus`), as well as via the Web UI **Stages-tab shuffle columns**. This is the same `StreamingShuffleMetrics → StreamingShuffleSource → MetricsSystem` path shown in the component-interaction diagram on the [Architecture](architecture.md) page.

### Structured logging with correlation IDs

Streaming-specific events are emitted as **structured logging** through the reused SLF4J/Log4j2 stack, carrying **MDC (Mapped Diagnostic Context) correlation keys** so a single shuffle can be traced end-to-end across producer and consumer executors. The correlation keys are exactly:

- **`shuffle_id`** — the shuffle the log line belongs to.
- **`map_id`** — the producing (map-side) task.
- **`reduce_partition_range`** — the consuming (reduce-side) partition range being read.
- **`attempt_id`** — the task attempt, distinguishing retries.

Setting **`spark.shuffle.streaming.debug=true`** raises log verbosity for diagnostics. It is off by default and should remain off in production to stay within the log-volume budget described under [Constraints](#constraints); see [Configuration](configuration.md) for the flag's definition.

### Grafana dashboard template

**`dashboard.json`** (in this same folder) is the provided **Grafana dashboard template**: a **2×2 grid of four panels**, one panel per `shuffle.streaming.*` metric, importable against a **Prometheus datasource**. It visualizes **`bufferUtilizationPercent`** as a **gauge with the 80% spill threshold marked**, alongside the three counters — **`spillCount`**, **`backpressureEvents`**, and **`partialReadInvalidations`**. Because it scrapes the same Prometheus endpoint the metrics already surface through, importing `dashboard.json` requires no change to the cluster's metrics configuration.

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
3. **Scrape and inspect the metrics** — confirm the four `shuffle.streaming.*` metrics appear through any of the reused endpoints:
   - the **Prometheus** endpoint **`/metrics/executors/prometheus`**,
   - **JMX** (via the executor's registered MBeans), or
   - the Web UI **Stages-tab shuffle columns**.

Seeing `bufferUtilizationPercent` move and the counters increment confirms the `StreamingShuffleSource → MetricsSystem` wiring is live. This local-dev metric-emission verification is a required part of the observability acceptance, not an optional check.

## See also

- [Architecture](architecture.md) — the component-interaction diagram shows the `StreamingShuffleMetrics → StreamingShuffleSource → MetricsSystem` registration path described above.
- [Configuration](configuration.md) — the five `spark.shuffle.streaming.*` keys, including `spark.shuffle.streaming.debug`, and the activation alias.
- Back to the [overview](index.md).
