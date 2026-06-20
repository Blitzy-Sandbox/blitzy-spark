# Streaming Shuffle

Streaming Shuffle is an **opt-in** shuffle backend for Apache Spark Core (`blitzy-spark` fork, `spark-core_2.13`) that **eliminates shuffle-materialization latency** by streaming intermediate (map-side) data **directly from producer executors to consumer (reduce-side) executors** through **bounded in-memory buffers** and the **existing `org.apache.spark.network` transport layer**, governed by a **backpressure protocol**. It is designed to coexist with — and **automatically fall back to** — the existing sort-based shuffle (`SortShuffleManager`), which is composed unchanged and is never bypassed when a fallback condition trips. The backend is **opt-in and off by default**: unless it is explicitly enabled, default cluster behavior is **byte-for-byte unchanged**.

## Scope and value

The streaming backend is designed to meet the following measurable success criteria:

- **30–50% end-to-end latency reduction** for **shuffle-heavy** workloads (≥ 100 MB shuffled data, ≥ 10 partitions).
- **5–10% improvement** for **CPU-bound** workloads, via reduced scheduler overhead.
- **Zero regression** for **memory-bound** workloads, through automatic fallback to the sort-based shuffle.
- **Zero data loss** under all failure scenarios.
- **Memory-exhaustion prevention** via an **80% buffer-utilization spill trigger** with a **< 100 ms** response time.

## Core capabilities

1. **Producer→consumer streaming** — map output is buffered in memory and pipelined to reduce-side consumers via the existing network transport, instead of being fully materialized to local disk before any fetch begins.
2. **Bounded in-memory buffering** — per-partition buffers are limited to a configurable percentage of executor memory (**default 20%, range 1–50%**), sized `(executorMemory * bufferSizePercent / 100) / numPartitions` with a **2 MB floor**.
3. **Backpressure flow control** — a consumer→producer **heartbeat** and **token-bucket** rate-limiting protocol throttles producers so consumers are not overwhelmed, with per-executor bandwidth caps.
4. **Graceful disk spill** — when buffer utilization reaches the spill threshold (**default 80%**), the largest buffered partitions spill to disk via the existing `BlockManager`, reclaiming memory within a **~100 ms** SLA.
5. **Partial-read invalidation on failure** — on producer failure (a **5 s connection timeout**), the reader invalidates partial reads and raises a **`FetchFailedException`**, letting Spark's existing lineage/recompute machinery recover the lost output with zero data loss.

## Activation

Streaming engages **only** when **both** of the following signals are set; otherwise the sort-based shuffle is used:

```properties
spark.shuffle.manager=streaming
spark.shuffle.streaming.enabled=true
```

Both signals default to **off**, so a cluster that does not set them continues to run the existing sort-based shuffle unchanged. See **[Configuration](configuration.md)** for the full configuration surface — the five `spark.shuffle.streaming.*` keys and the `streaming` activation alias.

## Documentation

This section is organized into the following pages:

- **[Configuration](configuration.md)** — the five `spark.shuffle.streaming.*` keys and the activation alias.
- **[Architecture](architecture.md)** — component-interaction, before/after factory, and data-flow Mermaid diagrams.
- **[Observability](observability.md)** — metrics, structured logging, and the Grafana dashboard.
- **[Decision Log](decision-log.md)** — design decisions, rationale, and the requirement traceability matrix.
- **[Executive Summary](executive-summary.html)** — a self-contained reveal.js slide deck for leadership.
- **`dashboard.json`** (in this folder) — the Grafana dashboard template referenced by the [Observability](observability.md) page. It is a JSON template, not a rendered page.

## Operator guides

Operator-facing user guides are published on the separate **Jekyll** documentation site (the `docs/` tree) and are provided for cluster operators, while this TechDocs section is the engineering and cross-cutting-deliverable home for the feature. The Jekyll guides are:

- `docs/streaming-shuffle-guide.md` — user guide (enabling and using the backend).
- `docs/streaming-shuffle-architecture.md` — architecture overview.
- `docs/streaming-shuffle-tuning.md` — tuning guide.
- `docs/streaming-shuffle-troubleshooting.md` — troubleshooting guide.
