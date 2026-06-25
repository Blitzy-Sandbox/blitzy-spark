# Streaming Shuffle Observability

This page is the telemetry reference for the opt-in **streaming shuffle** subsystem. The guiding principle is **reuse first**: streaming shuffle does not introduce a new metrics endpoint, a new sink, or any new Spark Web UI asset. Instead it binds to the observability surfaces Spark already ships — `org.apache.spark.internal.Logging` (log4j2 with MDC), the Dropwizard `MetricsSystem`, the Prometheus servlet, and the JMX, CSV, and SLF4J sinks — and adds only **four metrics** plus a documented MDC correlation schema. Operators who already run Spark's metrics stack therefore gain streaming-shuffle visibility with no new infrastructure to stand up. The configuration keys that govern the subsystem are described in [configuration.md](configuration.md), and the component and protocol overview is in [architecture.md](architecture.md).

## Metrics

The streaming shuffle emits four metrics under the `shuffle.streaming.` namespace:

| Metric | Type | Meaning |
|--------|------|---------|
| `bufferUtilizationPercent` | Gauge (`AtomicInteger`) | Current buffer fill level (0–100%). |
| `spillCount` | Counter (`LongAdder`) | Number of disk spill events. |
| `backpressureEvents` | Counter (`LongAdder`) | Number of backpressure activations. |
| `partialReadInvalidations` | Counter (`LongAdder`) | Partial reads invalidated on producer failure. |

These metrics are defined by `StreamingShuffleMetrics` (F-112) and registered with the existing Dropwizard `MetricsSystem` through `StreamingShuffleSource` (F-113), which implements `org.apache.spark.metrics.source.Source`. Registration happens **only when `SparkEnv.get != null`** — that is, on a live executor (or driver) with an initialized environment — so unit tests and embedded usages that run without a `SparkEnv` neither register the source nor incur any telemetry cost. The source is registered under the source name `streamingShuffle`, and every metric it exposes is qualified by the `shuffle.streaming.` namespace shown above.

## Structured logging and the MDC schema

All streaming-shuffle components mix in `org.apache.spark.internal.Logging`, which routes through log4j2 and supports a Mapped Diagnostic Context (MDC). Streaming shuffle populates the MDC (via the typed `LogKeys`) with four correlation fields that thread a single logical shuffle through the producer→consumer path:

| Field | Meaning |
|-------|---------|
| `shuffle_id` | Identifier of the shuffle this log line belongs to. |
| `map_id` | Identifier of the producing (map) task that wrote the block. |
| `reduce_partition_range` | The consumer-side reduce partition range being read. |
| `attempt_id` | Task attempt identifier, distinguishing retries of the same task. |

Because these fields are attached as structured MDC entries rather than embedded in free-text messages, they can be extracted and correlated by any log4j2-aware aggregation pipeline. Distributed-tracing-style correlation **across executor boundaries** is carried by the `BackpressureRpcEndpoint` (F-108): its heartbeat and acknowledgment messages propagate the shuffle/attempt identity between the producer and consumer executors, so a flow-control event observed on one executor can be tied back to the originating task on another. Verbose tracing of the streaming path can be turned on with `spark.shuffle.streaming.debug` (see [configuration.md](configuration.md)).

## JMX exposition

When the JMX sink is enabled, the four metrics are exposed as JMX MBeans. The `ObjectName` follows Spark's existing convention, with the source registered under the name `streamingShuffle`:

```
metrics:name=<app>.<executor-id>.streamingShuffle.shuffle.streaming.<metric>
```

For example, the buffer-utilization gauge for executor `3` of application `app-20240601120000-0001` is exposed as:

```
metrics:name=app-20240601120000-0001.3.streamingShuffle.shuffle.streaming.bufferUtilizationPercent
```

The same pattern applies to `spillCount`, `backpressureEvents`, and `partialReadInvalidations`, substituting the metric leaf name for `<metric>`.

## Prometheus exposition

Streaming-shuffle metrics surface through Spark's **existing** Prometheus servlet — no new endpoint is added. Executor metrics are exposed at:

```
/metrics/executors/prometheus
```

This endpoint is gated by `UI_PROMETHEUS_ENABLED` (the `spark.ui.prometheus.enabled` configuration); when it is enabled, the four metrics appear in the executor metrics scrape alongside Spark's built-in series. The `PrometheusServlet` renders dotted metric names with underscores, so the four streaming metrics match the following regular expression:

```
.*_streamingShuffle_shuffle_streaming_(bufferUtilizationPercent|spillCount|backpressureEvents|partialReadInvalidations)
```

The expression is intentionally unanchored at the end so that any per-type suffix the exporter appends (for example a gauge `_Value` or a counter `_Count` segment) is still matched. These four leaf names are **byte-identical** to the panel expressions in [dashboard.json](dashboard.json), so a scrape configured with this regex produces exactly the series the dashboard plots.

Operators who want to scrape only the streaming-shuffle metrics can drop everything else with a `keep` action on `__name__`:

```yaml
scrape_configs:
  - job_name: 'spark-streaming-shuffle'
    metrics_path: '/metrics/executors/prometheus'
    static_configs:
      - targets: ['<executor-host>:<ui-port>']
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: '.*_streamingShuffle_shuffle_streaming_(bufferUtilizationPercent|spillCount|backpressureEvents|partialReadInvalidations)'
        action: keep
```

## Sink reuse

Telemetry routes entirely through the **existing** Dropwizard `MetricsSystem` (Dropwizard/Codahale Metrics 4.2.37) and surfaces through the sinks Spark already provides — **JMX, Prometheus, CSV, and SLF4J**. No new sink and no new endpoint are introduced; directing the streaming-shuffle metrics to a given sink is a matter of the operator's existing `metrics.properties` configuration. A ready-to-copy template ships with the subsystem at:

```
core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template
```

(F-118). It documents how to route the `streamingShuffle` source to each of the reused sinks.

## Telemetry budget and local verification

The telemetry surface is deliberately lightweight. Its overhead is budgeted at **less than 1% of executor CPU** and **less than 10 MB/hour/executor** of log volume, so that observability never erodes the latency gains the streaming path is designed to deliver. Per the Observability rule, all telemetry must be **verified to work in the local development environment**: bring up a local Spark application with streaming shuffle enabled, confirm the four metrics register and update through the configured sink(s), confirm the MDC fields appear on log4j2 output, and confirm the Prometheus endpoint exports the four series before relying on them in any higher environment.

## Dashboard and the Spark Web UI

A ready-to-import Grafana dashboard template ships alongside this page as [dashboard.json](dashboard.json). It is laid out as a **2×2 grid of four panels**, one per metric:

- **Buffer Utilization (%)** — a gauge for `bufferUtilizationPercent`, with thresholds stepping to a warning near 70 and a critical band at 80 (the spill threshold), so sustained readings near 80 visibly flag imminent disk spills.
- **Spill Count** — a stat panel summing `spillCount`.
- **Backpressure Events** — a stat panel summing `backpressureEvents`.
- **Partial Read Invalidations** — a stat panel summing `partialReadInvalidations`.

Each panel keys on the same `_streamingShuffle_shuffle_streaming_<leaf>` series matched by the regex above, so the dashboard plots precisely the metrics this page documents.

Beyond the dashboard, streaming-shuffle read/write/spill activity **also** surfaces through the **existing Spark Web UI Stages tab** shuffle columns (driven by `AppStatusListener`). No new Spark Web UI page, tab, or static asset is added by this feature — operators read streaming-shuffle behavior through the same Stages-tab columns they already use for the sort-based path.

## See also

- [index.md](index.md) — streaming shuffle documentation landing page.
- [architecture.md](architecture.md) — component and protocol overview, including the metrics-emitting `MemorySpillManager` and `BackpressureProtocol`.
- [configuration.md](configuration.md) — the five `spark.shuffle.streaming.*` keys and the dual-flag activation contract.
- [decision-log.md](decision-log.md) — architecture decisions and the requirement-to-source-to-test traceability matrix.
- [dashboard.json](dashboard.json) — the Grafana dashboard template visualizing the four metrics.
