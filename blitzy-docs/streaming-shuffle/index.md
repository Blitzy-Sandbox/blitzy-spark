# Streaming Shuffle for Apache Spark

Streaming shuffle is an **opt-in**, pluggable `ShuffleManager` that streams shuffle data directly from producer (map) tasks to consumer (reduce) tasks through bounded in-memory buffers governed by a backpressure protocol, eliminating the write-to-disk-then-fetch materialization barrier of the default sort-based shuffle. It is designed to deliver a **30–50% end-to-end latency reduction** for shuffle-heavy workloads (**≥ 100 MB** of shuffle data across **≥ 10 partitions**) and a **5–10% improvement** for CPU-bound workloads, while guaranteeing **zero regression** for memory-bound workloads through automatic graceful degradation back to the sort-based path.

The subsystem is built around two load-bearing invariants. **Zero data loss** holds under all failure scenarios: every failure path — producer timeout, checksum mismatch, or consumer crash — invalidates cleanly and defers to Spark's existing recomputation machinery rather than attempting an in-band repair. **Memory-exhaustion prevention** is enforced by an **80% buffer-utilization spill threshold** that offloads the largest partitions to disk with a **sub-100 ms** response and reclaims buffers within 100 ms of consumer acknowledgment.

## Activation

Streaming shuffle engages **only under a dual-flag activation contract** — both of the following must be set:

- `spark.shuffle.manager=streaming`
- `spark.shuffle.streaming.enabled=true`

If either flag is absent, the default `SortShuffleManager` handles all shuffle exactly as before. The default of `spark.shuffle.manager` remains **`sort`** and is **unchanged** by this feature, so existing applications observe no behavioral difference unless they explicitly opt in. See [configuration.md](configuration.md) for the full configuration surface — the five `spark.shuffle.streaming.*` keys plus the manager selector.

## SPI coexistence topology

**Diagram 0.2-A — Streaming Shuffle SPI Coexistence Topology** shows how the new manager plugs into the unchanged dispatch boundary and coexists with the sort path. The user-facing API, `ShuffleExchangeExec` and the AQE rules, the `SparkEnv` bootstrap, and the reflective `ShuffleManager.create` factory are all unchanged; only the `shortShuffleMgrNames` alias map gains the `"streaming"` short name, and the new `StreamingShuffleManager` holds an inner `SortShuffleManager` for delegation and fallback.

```mermaid
flowchart TB
    title["Diagram 0.2-A: Streaming Shuffle SPI Coexistence Topology"]
    UserCode["User Code (RDD / DataFrame / SQL)<br/>UNCHANGED"]
    Exchange["ShuffleExchangeExec + AQE rules<br/>UNCHANGED (Tech Spec 5.2.4)"]
    SparkEnvBoot["SparkEnv bootstrap<br/>ShuffleManager.create (reflective, L226)<br/>UNCHANGED"]
    Factory{"shortShuffleMgrNames alias map<br/>ShuffleManager.scala L112-L114<br/>MODIFY: add 'streaming'"}
    Sort["SortShuffleManager<br/>(default + fallback)<br/>UNCHANGED"]
    Streaming["StreamingShuffleManager (F-101)<br/>NEW — holds inner SortShuffleManager"]
    SharedResolver["IndexShuffleBlockResolver<br/>(shared, via delegation)"]
    UserCode --> Exchange --> SparkEnvBoot --> Factory
    Factory -->|"'sort' / 'tungsten-sort'"| Sort
    Factory -->|"'streaming' (NEW)"| Streaming
    Streaming -. "delegate / fallback" .-> Sort
    Streaming -. "block migration delegation" .-> SharedResolver
    Sort --> SharedResolver
    legend["Legend: solid = active dispatch path; dashed = delegation/fallback;<br/>'UNCHANGED' = zero-modification surface; 'NEW'/'MODIFY' = in-scope edits"]
```

The solid edges are the active dispatch path: `"sort"` / `"tungsten-sort"` resolve to the unchanged `SortShuffleManager`, while the new `"streaming"` alias resolves to `StreamingShuffleManager`. The dashed edges show that the streaming manager delegates to — and falls back to — the inner `SortShuffleManager`, and delegates block-migration calls to the shared `IndexShuffleBlockResolver`. The full component and protocol overview, including the integration-touchpoint and data-flow diagrams, lives in [architecture.md](architecture.md).

## Failure Scenarios (Zero Data Loss)

The following **ten** failure-injection scenarios — exercised by `StreamingShuffleFailureInjectionSuite` (F-121) — are the user-facing proof of the zero-data-loss guarantee. Each scenario resolves either by invalidating the partial read and deferring to the DAG scheduler's existing recomputation, or by degrading gracefully to the sort-based path; no scenario loses or corrupts shuffle data.

| # | Failure scenario | Expected behavior (zero data loss) |
|---|------------------|------------------------------------|
| 1 | **Producer crash mid-stream** | Consumer partial-read invalidation → `FetchFailedException` → DAG recomputation. |
| 2 | **Producer connection timeout (5 s)** | Partial-read invalidation → recompute; increments `partialReadInvalidations`. |
| 3 | **Consumer crash / missing-ack timeout (10 s)** | Producer reclaims buffers; no leak. |
| 4 | **CRC32C checksum mismatch on a ≤ 2 MiB block** | Retransmission; on repeated failure → invalidate + recompute. |
| 5 | **Memory pressure / buffer-allocation OOM risk** | Fallback condition #2 → revert to `SortShuffleManager` (no data loss). |
| 6 | **Network saturation > 90% link capacity** | Fallback condition #3 → revert to sort. |
| 7 | **Producer/consumer version mismatch** | Fallback condition #4 → revert to sort. |
| 8 | **Consumer sustained 2× slower than producer for > 60 s** | Fallback condition #1 (backpressure) → revert to sort. |
| 9 | **Buffer utilization ≥ 80% spill threshold** | `MemorySpillManager` spills the largest partitions to disk (`BlockManager.putBytes(DISK_ONLY)`), reclaim within 100 ms; increments `spillCount`. |
| 10 | **Backpressure activation under load** (token-bucket throttling, heartbeat 5 s) | Flow control engaged; increments `backpressureEvents`; no loss. |

Scenarios 5–8 correspond to the four automatic fallback conditions documented in [decision-log.md](decision-log.md); scenarios 1–4 and 9–10 are handled within the streaming data path itself.

## Documentation

In this section:

- [configuration.md](configuration.md) — configuration reference: the five `spark.shuffle.streaming.*` keys (plus the `spark.shuffle.manager` selector) and the dual-flag activation contract.
- [architecture.md](architecture.md) — component and protocol overview, including the three Mermaid architecture diagrams.
- [observability.md](observability.md) — metrics, the MDC correlation schema, JMX/Prometheus exposition, and the dashboard.
- [decision-log.md](decision-log.md) — the architecture decision (ADR) table and the requirement → source → test traceability matrix.
- [executive-summary.html](executive-summary.html) — the reveal.js executive presentation deck.
- [dashboard.json](dashboard.json) — the Grafana dashboard template.
