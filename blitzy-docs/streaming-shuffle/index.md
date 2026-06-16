# Streaming Shuffle

Streaming Shuffle is an **opt-in** shuffle backend for Apache Spark Core (`blitzy-spark` fork, `spark-core_2.13`) that **eliminates shuffle-materialization latency** by streaming intermediate (map-side) data **directly from producer executors to consumer (reduce-side) executors** through **bounded in-memory buffers** and the **existing `org.apache.spark.network` transport layer**, governed by a **backpressure protocol**. It **preserves the sort-based shuffle (`SortShuffleManager`) as an automatic fallback**, so memory-bound or otherwise unsuitable workloads revert to the existing path transparently. The backend is **opt-in and off by default**: when it is not explicitly enabled, default cluster behavior is **byte-for-byte unchanged**.

## Scope and value

Streaming Shuffle is **designed against** the following measurable success criteria:

- **30–50% end-to-end latency reduction** for **shuffle-heavy** workloads (≥ 100 MB shuffled data, ≥ 10 partitions) — a **distributed-cluster target**.
- **5–10% improvement** for **CPU-bound** workloads (via reduced scheduler overhead) — a **distributed-cluster target**.
- **Zero regression** for **memory-bound** workloads (through automatic fallback to sort-based shuffle) — **validated** locally (fallback runs at sort-equivalent latency).
- **Zero data loss** under failure — by design (failures recover via Spark's lineage/recompute), exercised by the failure-injection suite.
- **Memory-exhaustion prevention** via an **80% buffer-utilization spill trigger** with a **< 100 ms** response time.

> The 30–50% / 5–10% latency figures are **distributed-cluster targets** that arise from avoiding cross-executor materialization latency; they are **not** reproducible on a single host. The committed single-host benchmarks instead validate component overheads and confirm the sort-equivalent memory-bound fallback. See the [decision log](decision-log.md) traceability row and the [architecture](architecture.md) page.

## Core capabilities

1. **Producer→consumer streaming** — map output is buffered in memory and pipelined to reduce-side consumers via the existing network transport instead of being fully materialized to local disk first.
2. **Bounded in-memory buffering** — per-partition buffers are limited to a configurable percentage of executor memory (**default 20%, range 1–50%**), sized `(executorMemory * bufferSizePercent / 100) / numPartitions` with a **2 MB floor**.
3. **Backpressure flow control** — a consumer→producer **heartbeat** plus **token-bucket** rate-limiting protocol throttles producers, with per-executor bandwidth caps.
4. **Graceful disk spill** — at the spill threshold (**default 80%**) the largest buffered partitions spill to disk via the existing `BlockManager`, reclaiming memory within a **~100 ms** SLA.
5. **Partial-read invalidation on failure** — on producer failure (5 s connection timeout) the reader invalidates partial reads and raises a **`FetchFailedException`**, letting Spark's existing lineage/recompute machinery recover the lost output (zero data loss).

## Activation

The streaming backend engages **only** when **both** signals below are set; otherwise the sort-based shuffle is used:

```properties
spark.shuffle.manager=streaming
spark.shuffle.streaming.enabled=true
```

Both flags **default to off**, so a cluster that does not opt in behaves exactly as it does today. See [Configuration](configuration.md) for the full configuration surface — the five `spark.shuffle.streaming.*` keys and the activation alias.

## Documentation

This section is the engineering and cross-cutting-deliverable home for the streaming shuffle backend:

- [Configuration](configuration.md) — the five `spark.shuffle.streaming.*` keys and the activation alias.
- [Architecture](architecture.md) — component-interaction, before/after factory, and data-flow Mermaid diagrams.
- [Observability](observability.md) — metrics, structured logging, and the Grafana dashboard.
- [Decision Log](decision-log.md) — design decisions, rationale, and the requirement traceability matrix.
- [Executive Summary](executive-summary.html) — a self-contained reveal.js slide deck for leadership.
- `dashboard.json` (in this folder) — the Grafana dashboard template referenced by the [Observability](observability.md) page; it is a JSON template for import into Grafana, not a rendered documentation page.

## Operator guides

Operator-facing user guides are published on the separate **Jekyll docs site** (the `docs/` tree of the Spark distribution), not within this TechDocs section. They are provided for operators enabling, tuning, and supporting the backend in production, while this TechDocs section remains the engineering / cross-cutting-deliverable home. The guides are:

- `docs/streaming-shuffle-guide.md` — user guide (enabling & using).
- `docs/streaming-shuffle-architecture.md` — architecture overview.
- `docs/streaming-shuffle-tuning.md` — tuning guide.
- `docs/streaming-shuffle-troubleshooting.md` — troubleshooting guide.
