# Streaming Shuffle Observability

Streaming shuffle is observable **by reuse** of Spark's existing telemetry
facilities rather than through any bespoke pipeline or new user interface. When
the streaming backend is active it emits metrics through Spark's `MetricsSystem`
as a registered metrics `Source`, and it emits MDC-tagged structured logs
through the standard `org.apache.spark.internal.Logging` framework. **No new
Spark Web UI** page, tab, or static asset is added — operators observe
streaming-shuffle internals through the pre-existing metrics endpoints, the
existing Stages tab shuffle columns, and structured logs (observability-by-reuse).

See also: [Configuration](configuration.md) and the external
[Grafana Dashboard](dashboard.json).

## Metrics

A `StreamingShuffleSource` (an `org.apache.spark.metrics.source.Source` whose
`sourceName = "streamingShuffle"`) is registered with the `MetricsSystem` when
the `StreamingShuffleManager` is constructed. Because registration flows through
the standard `Source` SPI, **all** configured sinks — JMX, Prometheus, CSV, and
Slf4j — pick these metrics up automatically, with no sink-specific wiring. The
metrics surface under the executor instance in the `streamingShuffle` namespace,
so each fully-qualified metric name has the form:

```
<application>.<executorId>.streamingShuffle.<metric>
```

The source exposes four metrics:

| Metric | Type | Description |
|---|---|---|
| `bufferUtilizationPercent` | Gauge | Current per-executor streaming buffer utilization (0–100). Sustained high values precede spilling. |
| `spillCount` | Counter | Number of buffered partitions spilled to disk. Rising values indicate memory pressure. |
| `backpressureEvents` | Counter | Number of backpressure throttle/timeout events. Rising values mean the consumer or network can't keep up with the producer. |
| `partialReadInvalidations` | Counter | Number of in-progress reads invalidated on producer failure. Nonzero values indicate producer-side failures/timeouts. |

These metrics are emitted **only when the streaming backend is active** — that
is, only under the dual activation gate `spark.shuffle.manager=streaming` **and**
`spark.shuffle.streaming.enabled=true`. Under the default sort-based shuffle the
source is never registered and no series are produced. The counters are backed
by lock-free Dropwizard primitives and the gauge by a single atomic read, so the
telemetry overhead is designed to stay **below 1% CPU**.

## Structured logging (MDC correlation IDs)

Streaming-shuffle components log through Spark's standard logging framework
(`org.apache.spark.internal.Logging`), tagging each event with the following MDC
(Mapped Diagnostic Context) keys. These keys act as **cross-boundary correlation
IDs**: they link the producer (map) and consumer (reduce) log lines that belong
to a single shuffle, across executor boundaries.

| MDC Key | Meaning |
|---|---|
| `shuffle_id` | The shuffle identifier |
| `map_id` | The producing map task identifier |
| `reduce_partition_range` | The reduce partition range being consumed |
| `attempt_id` | The task attempt identifier |

As with all Spark MDC keys, these fields are not shown in plain-text logs by
default. There are two ways to surface them:

- **Plain-text logs** — add the fields to the log4j2 `PatternLayout`, for
  example `%X{shuffle_id}` (and likewise `%X{map_id}`,
  `%X{reduce_partition_range}`, `%X{attempt_id}`).
- **Structured (JSON) logs** — set `spark.log.structuredLogging.enabled=true` to
  emit JSON log events that include **all** MDC fields automatically, with no
  pattern edits required.

DEBUG-level streaming output is gated by `spark.shuffle.streaming.debug=true` and
is **off by default**. Per-executor streaming log volume is capped at under
**10&nbsp;MB/hour**, keeping the logging overhead bounded even under heavy shuffle
activity.

## Prometheus scraping

Streaming-shuffle metrics reach Prometheus through Spark's `PrometheusServlet`
sink and/or the pre-existing `/metrics/executors/prometheus` endpoint, which is
gated by `spark.ui.prometheus.enabled` (the `UI_PROMETHEUS_ENABLED` config).
Because the metrics register through the standard `Source` SPI, no
sink-specific configuration is required for them to appear in the exposition.

**Naming convention.** When exporting to Prometheus, Spark normalizes each
metric key to `metrics_<key>_`, replacing every non-alphanumeric character with
an underscore (`_`). It then appends a type-dependent suffix: `Number` or
`Value` for gauges, and `Count` for counters. Applied to the `streamingShuffle`
source, the exported time series therefore look like:

```
metrics_<application>_<executorId>_streamingShuffle_bufferUtilizationPercent_Value   # gauge
metrics_<application>_<executorId>_streamingShuffle_spillCount_Count                 # counter
```

Because the `<application>` and `<executorId>` prefixes vary from run to run and
executor to executor, robust dashboards should **match on `__name__` with a
regular expression** rather than pinning a fully-qualified series name.

## Prometheus regex / PromQL per Grafana panel

The following PromQL queries back the four panels of the external Grafana
dashboard [`dashboard.json`](dashboard.json). The metric names and query shapes
are kept **byte-identical** to that file so the documentation and the dashboard
never diverge.

| Panel | Metric | PromQL |
|---|---|---|
| Streaming Buffer Utilization (%) | `bufferUtilizationPercent` (gauge) | `max by (instance) ({__name__=~"metrics_.*_streamingShuffle_bufferUtilizationPercent_(Value|Number)"})` |
| Spill Count | `spillCount` (counter) | `sum by (instance) ({__name__=~"metrics_.*_streamingShuffle_spillCount_Count"})` |
| Backpressure Events | `backpressureEvents` (counter) | `sum by (instance) ({__name__=~"metrics_.*_streamingShuffle_backpressureEvents_Count"})` |
| Partial-Read Invalidations | `partialReadInvalidations` (counter) | `sum by (instance) ({__name__=~"metrics_.*_streamingShuffle_partialReadInvalidations_Count"})` |

The gauge query uses `max by (instance)` to report peak utilization per executor;
the counter queries use `sum by (instance)`. For counters, operators often prefer
a per-second **rate** over the raw cumulative total — for example, spill events
per second over a five-minute window:

```promql
sum by (instance) (rate({__name__=~"metrics_.*_streamingShuffle_spillCount_Count"}[5m]))
```

## Scrape topologies

Two common Prometheus scrape setups expose these metrics; both are
vendor-neutral and require no changes to the streaming code:

1. **Direct executor scrape.** Point Prometheus at each executor's (and the
   driver's) `/metrics/executors/prometheus` endpoint — or scrape the
   driver-aggregated executor metrics. This is the simplest option for Standalone
   and YARN deployments and only requires `spark.ui.prometheus.enabled=true`.
2. **`PrometheusServlet` sink via `metrics.properties`.** Configure the sink
   centrally in the metrics system so metrics are exposed on the Spark UI's
   metrics servlet path and scraped there. This is useful when the metrics system
   is configured centrally for the whole cluster:

    ```properties
    *.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
    *.sink.prometheusServlet.path=/metrics/prometheus
    ```

On Kubernetes, a Prometheus Operator `ServiceMonitor` or `PodMonitor` targets the
very same endpoints; no streaming-specific configuration is needed.

## Spark-native interpretation of the Observability rule

Apache Spark is a distributed data engine, not an HTTP microservice, so the
project's Observability rule is satisfied using **Spark-native facilities**
rather than a service-style `/health` endpoint or a bespoke tracing stack. This
mapping is a deliberate, logged decision (see the [Decision Log](decision-log.md)):

- **Metrics endpoint** → the `MetricsSystem` fan-out to its configured sinks
  (JMX / Prometheus / CSV / Slf4j).
- **Liveness / readiness** → existing executor heartbeats and `BlockManager`
  status; a stalled executor is already detected and its work rescheduled through
  the standard mechanisms.
- **Cross-boundary tracing** → MDC-tagged structured logs, using the
  shuffle/map/attempt identifiers above as correlation IDs across the
  producer→consumer boundary.
- **Dashboard** → the external Grafana template [`dashboard.json`](dashboard.json).

No new Spark Web UI page or tab is added. Streaming-shuffle activity also appears
in the **existing Stages tab** shuffle columns (read/write size, records, and
spill), fed by the standard `AppStatusListener`, exactly as sort-based shuffle
data does.

## Related pages

- [Configuration](configuration.md) — the `spark.shuffle.streaming.*` keys and the dual activation gate.
- [Architecture](architecture.md) — how the streaming subsystem is structured and how it coexists with sort-based shuffle.
- [Grafana Dashboard](dashboard.json) — the importable dashboard whose four panels these queries back.
- [Decision Log](decision-log.md) — the rationale behind the Spark-native Observability interpretation and other design decisions.
