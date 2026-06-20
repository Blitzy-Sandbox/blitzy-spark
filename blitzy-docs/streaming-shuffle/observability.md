# Observability

Streaming shuffle ships its observability **with the implementation** rather than bolting it on afterward. It **reuses** Spark's existing logging and metrics infrastructure unchanged, and on top of that it **adds** a small, streaming-specific layer: four `shuffle.streaming.*` metrics, structured logging with correlation IDs, and a ready-to-import Grafana dashboard template. This page is the explicit **reused-vs-added ledger** for the feature — read it alongside the [Architecture](architecture.md) and [Configuration](configuration.md) pages, or return to the section [overview](index.md).

## Reused

The streaming backend follows a least-modification approach and consumes Spark's existing observability stack **as-is**. None of the following is re-implemented or forked — the feature simply plugs into it:

- **SLF4J / Log4j2 logging stack** — all streaming log output flows through Spark's existing SLF4J facade and Log4j2 backend, honoring the cluster's configured log levels, appenders, and layouts.
- **Executor `MetricsSystem`** — the Dropwizard/Codahale Metrics registry already running inside every executor. Streaming metrics register with it exactly like any other Spark metric source.
- **Existing metrics endpoints — JMX and Prometheus** — the streaming metrics are exposed through the same surfaces as every other Spark metric, including the executor Prometheus endpoint at **`/metrics/executors/prometheus`**. No new metrics endpoint is added.
- **Executor health surface** — readiness is reported through Spark's existing executor health surface; the streaming backend adds no separate health probe.
- **Shuffle security — authentication/SASL and TLS** — authentication (`spark.authenticate`/SASL) and TLS are inherited via the existing transport configuration. The streaming path introduces **no new network endpoints** beyond the executor-scoped backpressure RPC, so it sits behind the same security surfaces as sort-based shuffle.

Nothing in the metrics framework itself is modified. The streaming backend only *registers a source* with the existing `MetricsSystem`; it does not change `MetricsSystem`, its sinks, or the existing endpoints.

## Added

On top of the reused stack, the feature adds exactly three observability artifacts: the streaming metrics, streaming-specific structured logging, and the Grafana dashboard template.

### The four `shuffle.streaming.*` metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `bufferUtilizationPercent` | gauge | Current aggregate in-memory buffer utilization, expressed as a percentage (0–100) of the buffer budget. |
| `spillCount` | counter | Number of disk-spill events triggered when buffer utilization reached the spill threshold. |
| `backpressureEvents` | counter | Number of producer-throttling (backpressure) events raised by the flow-control protocol. |
| `partialReadInvalidations` | counter | Number of partial reads invalidated on producer failure or connection timeout. |

These four metrics are emitted under the **`shuffle.streaming.*`** namespace by a **`StreamingShuffleSource`** — an implementation of `org.apache.spark.metrics.source.Source` — that `StreamingShuffleManager` registers with the executor **`MetricsSystem`** (registration is gated on a live `SparkEnv` for local-mode safety). Because they register as a standard metric source, the values surface automatically through the reused `MetricsSystem` endpoints: **JMX** and the Prometheus endpoint **`/metrics/executors/prometheus`** (plus any other configured metrics sink). The backend registers a `MetricsSystem` **source only** — it does **not** add Spark Web UI Stages-tab columns — so consume `shuffle.streaming.*` via JMX, the Prometheus endpoint, or the Grafana dashboard below.

### Structured logging with correlation IDs

Streaming-specific log lines are **structured** and tagged with MDC (Mapped Diagnostic Context) correlation keys, so a single shuffle can be traced end-to-end across producer (map-side) and consumer (reduce-side) executors. The backend emits the **exact MDC correlation keys** required by the Observability rule — **`shuffle_id`**, **`map_id`**, **`reduce_partition_range`**, and **`attempt_id`**. It reuses the canonical `org.apache.spark.internal.LogKeys` where Spark already defines a matching key (`SHUFFLE_ID` → `shuffle_id`, `MAP_ID` → `map_id`) and defines two additional in-package `LogKey`s in **`StreamingShuffleLogKeys`** for the dimensions Spark has no canonical key for (`ATTEMPT_ID` → `attempt_id`, `REDUCE_PARTITION_RANGE` → `reduce_partition_range`). The MDC string key is always the `LogKey` name lower-cased (`Locale.ROOT`), so the keys actually emitted are:

| MDC key | Identifies |
|---------|-----------|
| `shuffle_id` | The shuffle being executed (emitted on writer and reader log lines). |
| `map_id` | The producer (map-side) task (emitted on writer log lines and the reader's per-block / failure log lines). |
| `reduce_partition_range` | The consumer (reduce-side) partition range a reduce task reads, formatted `[startPartition,endPartition)` (emitted on the reader's summary log line). |
| `attempt_id` | The task attempt, distinguishing retries from originals (emitted on the reader and writer summary log lines). |

The MDC context map is populated when Spark's structured-logging framework is enabled (the standard production logging layout); when it is disabled the same key/value pairs render inline in the message text. Either way the four correlation dimensions are present on the streaming log lines.

Setting **`spark.shuffle.streaming.debug=true`** raises log verbosity for diagnostics; it is `false` by default (see the [Configuration](configuration.md) page). Keep it off in production to stay within the logging budget described under [Constraints](#constraints).

### Grafana dashboard template

**`dashboard.json`** — located in this same folder — is the provided **Grafana dashboard template**. It is a **2×2 grid of four panels**, one panel per `shuffle.streaming.*` metric, and is importable as-is against a Prometheus datasource that scrapes the executor Prometheus endpoint. The panels visualize:

- `bufferUtilizationPercent` rendered as a **gauge**, with the **80% spill threshold** marked so operators can see how close buffers are to spilling.
- `spillCount`, `backpressureEvents`, and `partialReadInvalidations` rendered as counter time series.

Import `dashboard.json` into Grafana and point it at your Prometheus datasource to get the streaming-shuffle view without building panels by hand.

## Constraints

The telemetry is designed to be effectively free at runtime. Two budget invariants must hold:

- **Telemetry overhead < 1% executor CPU** — metric updates and structured logging together must not consume more than one percent of an executor's CPU.
- **Log volume < 10 MB/hour/executor** — keep `spark.shuffle.streaming.debug` **off** in production so log output stays under ten megabytes per hour per executor.

## Verifying metric emission

Confirming that the four metrics actually emit in a local development run is part of the observability acceptance for this feature. A minimal verification:

1. **Enable streaming.** Set both activation signals: `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`. (Both default off, so streaming is strictly opt-in.)
2. **Run a small shuffle job.** Any job with a shuffle stage — for example a `groupBy` or `reduceByKey` over a modest dataset — exercises the streaming writer and reader.
3. **Scrape or inspect the metrics.** Confirm the four `shuffle.streaming.*` values appear via any reused `MetricsSystem` surface:
    - the Prometheus endpoint **`/metrics/executors/prometheus`**, or
    - a **JMX** console attached to the executor.

    (The backend registers a `MetricsSystem` source only; it does not add Spark Web UI Stages-tab columns.)

Seeing a non-null `bufferUtilizationPercent` gauge and incrementing counters (`spillCount`, `backpressureEvents`, and `partialReadInvalidations`, as the relevant conditions occur) confirms the `StreamingShuffleSource → MetricsSystem` path is wired correctly.

## See also

- [Architecture](architecture.md) — the component-interaction diagram shows the `StreamingShuffleMetrics → StreamingShuffleSource → MetricsSystem` registration path described above.
- [Configuration](configuration.md) — the `spark.shuffle.streaming.*` keys, including `debug`, referenced throughout this page.
- [Overview](index.md) — back to the Streaming Shuffle documentation overview.
