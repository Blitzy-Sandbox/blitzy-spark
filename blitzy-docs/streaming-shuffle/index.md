# Streaming Shuffle for Apache Spark

Streaming Shuffle is an **opt-in** shuffle backend (`StreamingShuffleManager`) that
pipelines map-output data directly from producer (map) executors to consumer
(reduce) executors through **bounded in-memory buffers governed by a backpressure
protocol**, eliminating shuffle-materialization latency. It **coexists with** the
default sort-based shuffle (`SortShuffleManager`), which remains the
production-stable default and the automatic fallback whenever streaming is not
selected or a fallback condition is met.

## Objective

The items below are **design targets, not guarantees**: streaming shuffle aims to
deliver them for the workloads described and automatically falls back to
sort-based shuffle rather than regress.

- **30–50% end-to-end latency reduction** for shuffle-heavy workloads
  (100&nbsp;MB+ data, 10+ partitions).
- **5–10% improvement** for CPU-bound workloads through reduced scheduler overhead.
- **Zero performance regression** for memory-bound workloads, via automatic sort
  fallback.
- **Zero data loss** under all failure scenarios, including producer crashes,
  consumer failures, and network partitions.
- **Memory-exhaustion prevention** through an 80% buffer-utilization spill trigger
  with a <100&nbsp;ms response time.

## Opt-in and the dual activation gate

Streaming shuffle is active **if and only if BOTH** `spark.shuffle.manager=streaming`
**AND** `spark.shuffle.streaming.enabled=true` are set. If either is unset, Spark
uses the standard **sort-based** shuffle — no error is raised and no streaming
occurs. Requiring both properties is deliberate defense-in-depth against
accidental enablement.

```
spark.shuffle.manager=streaming
spark.shuffle.streaming.enabled=true
```

Configuration is **immutable for the application lifetime**: changing any
`spark.shuffle.streaming.*` value requires an **executor restart**. There is no
dynamic reconfiguration in v1.

## Contents

- [Configuration](configuration.md) — the five `spark.shuffle.streaming.*` keys,
  their defaults and ranges, and the dual activation gate.
- [Architecture](architecture.md) — Mermaid diagrams of the current (sort-only) and
  target (streaming-coexists) states, plus the integration points.
- [Observability](observability.md) — the four `streamingShuffle` metrics, the MDC
  logging schema, and Prometheus/PromQL guidance.
- [Decision Log](decision-log.md) — the architectural decision record: alternatives,
  rationale, and risks.
- [Executive Summary](executive-summary.html) — a self-contained reveal.js
  presentation.
- [Grafana Dashboard](dashboard.json) — an importable Grafana dashboard template for
  the streaming metrics.

## At a glance

- **Backend:** a new `ShuffleManager` selected by `spark.shuffle.manager=streaming`;
  it coexists with sort by **composition**, holding an inner `SortShuffleManager`
  for every non-streaming handle and every fallback.
- **Manager class:** `org.apache.spark.shuffle.streaming.StreamingShuffleManager`;
  the default manager remains `sort`.
- **Isolation:** all new code lives in the package
  `org.apache.spark.shuffle.streaming` (plus a `network/` subpackage); only two
  existing Scala files are surgically modified.
- **Since version:** **4.2.0**.
