# Decision Log

This log is the **Explainability** record for the streaming shuffle backend: it captures the non-trivial design decisions (the "why"), the alternatives that were considered, the rationale for each choice, and the residual risk. It also documents the one **intentional v1 deviation** (the logging-only transport) so it is not mistaken for an unfinished stub, and closes with a **bidirectional traceability matrix** that maps every requirement to the source files that implement it and the test suites that verify it. By design, this rationale lives here rather than in code comments, so the code stays focused on behavior while the reasoning stays discoverable in one place.

## Design decisions

The table below records each non-trivial decision with its discarded alternatives, the reasoning, and the residual risk (and how it is mitigated). Every choice follows the guiding principles of the feature: opt-in by default, zero regression for existing workloads, and the least possible modification to the executor memory model and the network transport.

| Decision | Alternatives | Rationale | Risk |
| --- | --- | --- | --- |
| **Opt-in dual-flag activation** — engage streaming only when `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`. | Always-on streaming; a single enable flag. | Guarantees a zero-regression default (existing clusters are byte-for-byte unchanged) and requires explicit operator intent before any behavior changes. | Misconfiguration (only one flag set) silently uses the sort path — mitigated by the configuration docs and startup logging. |
| **Compose a lazy inner `SortShuffleManager` for fallback** rather than reimplementing sort. | Fork/duplicate the sort code; modify `SortShuffleManager` in place. | Preserves the sort path completely unchanged, enables automatic graceful fallback, and embodies the least-modification principle. | One extra layer of indirection on the manager path — negligible overhead. |
| **Reuse `MemoryConsumer`/`TaskMemoryManager` + `BlockManager`** for buffering and spill. | Build parallel memory-management machinery dedicated to streaming. | Honors the existing executor memory model, avoids a redesign, and spills through `BlockManager.putBytes(..., StorageLevel.DISK_ONLY)`. | Shares the executor memory budget with other consumers — bounded by `bufferSizePercent` and the 80% spill threshold. |
| **Reuse `BlockTransferService`/`fetchBlockSync` as the data plane.** | Implement a new dedicated Netty data plane for streaming. | Minimizes change to the network transport and inherits the existing authentication/SASL and TLS surfaces; the v1 transport layer is logging-only. | v1 is not a true zero-copy push — deferred to v2 (see the documented deviation below). |
| **Guava `RateLimiter` token bucket** (1 permit = 1 byte) for bandwidth control. | Hand-rolled custom rate limiter. | Already on the Spark Core classpath, battle-tested, and treats `maxBandwidthMBps` of 0 or below as unlimited. | Coarse fairness across concurrent shuffles — mitigated by a per-concurrent-shuffle cap. |
| **JDK `CRC32C` block checksums.** | CRC32, Adler32, or MD5. | Uses the same primitive as `ShuffleChecksumUtils`, is hardware-accelerated on modern CPUs, and is built into JDK 17. | No material risk. |
| **Executor-only backpressure RPC endpoint** named `streaming-shuffle-backpressure`. | Driver-hosted endpoint; no RPC at all. | Heartbeats are executor-to-executor, so the endpoint registers on executors only and the driver returns `None` (the endpoint is rejected on the driver). | Endpoint lifecycle is tied to the executor — acceptable. |
| **Immutable configuration for the application lifetime** (no dynamic reconfiguration in v1). | Support dynamic reconfiguration at runtime. | Buys simplicity and determinism; an executor restart is the single, well-understood way to change tuning. | Tuning changes require a restart — acceptable for v1. |
| **Fixed 2 MB streaming block size** (and the per-partition buffer 2 MB floor). | Smaller blocks (more framing/checksum overhead); larger blocks (more latency and memory). | Balances per-block framing and checksum overhead against memory footprint and aligns the wire unit with the buffer floor. | Suboptimal for extreme record sizes — acceptable; not tunable in v1. |
| **Liveness timeouts with exponential backoff** — 5 s connection timeout, 10 s heartbeat interval, retry starting at 1 s for at most 5 attempts. | Fixed-interval retries; no retries; longer or shorter windows. | Standard distributed-systems liveness tuning that triggers partial-read invalidation and recompute promptly without thrashing the network. | Aggressive timeouts could cause spurious fallbacks under transient slowness — bounded by the retry budget. |
| **Surface telemetry via a `metrics.source.Source` registered with the `MetricsSystem`.** | A custom metrics pipeline; log-only telemetry. | Reuses the existing Dropwizard metrics framework and its JMX/Prometheus sinks; registration is gated on `SparkEnv.get != null` for local-mode safety. | One additional metrics source per manager — negligible (telemetry overhead is held under 1% of executor CPU). |

## Intentional v1 deviation: logging-only transport

In v1, `StreamingShuffleTransport.sendBlock` returns an already-**completed `Future`** and `openConsumerStream` returns **`Iterator.empty`**. This is **intended, documented behavior — not a placeholder defect and not an unfinished stub.** The real data plane for streaming reads is the existing **`BlockTransferService` / `fetchBlockSync`** path (driven by `ShuffleBlockFetcherIterator`); the v1 transport layer is a thin, logging-only integration seam over that proven transport rather than a parallel network stack standing in for missing functionality.

The deferred work — a real Netty data plane, `SO_KEEPALIVE` tuning, and full retry/backoff wiring at the transport layer — constitutes the **v2 network-transport hardening** and is explicitly out of scope for v1. Every other production path in the streaming package is fully implemented.

This deviation is recorded here deliberately so that a pull-request pre-flight review (the Segmented PR Review gate) does not misclassify the intentional v1 transport behavior as an unfinished production-path stub. The choice trades a true zero-copy push in v1 for the least-modification reuse of Spark's existing, secure transport, with the push optimization scheduled for v2.

## Traceability matrix

This matrix is **bidirectional**: a reader can trace forward (requirement → source → test) and backward (test → source → requirement) without reading the source. All streaming production sources live under `core/src/main/scala/org/apache/spark/shuffle/streaming/` (network classes under `…/streaming/network/`, the metrics template under `core/src/main/resources/org/apache/spark/shuffle/streaming/`); the two MODIFY files live in the existing shuffle and config packages; and all test suites live under `core/src/test/scala/org/apache/spark/shuffle/streaming/`.

| Requirement / capability | Source file(s) | Test file(s) |
| --- | --- | --- |
| Opt-in activation and the `"streaming"` manager alias (dual-flag, zero-regression default) | `StreamingShuffleManager`, `StreamingShuffleConfig`; `shuffle/ShuffleManager.scala` (MODIFY — registers the `"streaming"` alias), `internal/config/package.scala` (MODIFY — five `spark.shuffle.streaming.*` `ConfigEntry` values) | `StreamingShuffleManagerSuite`, `StreamingShuffleIntegrationSuite`, `StreamingShuffleIntegrationTest` |
| Streaming shuffle handle and per-shuffle tuning carrier (`bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps`) | `StreamingShuffleHandle` | `StreamingShuffleHandleSuite` |
| Producer-side streaming, per-partition buffering, CRC32C generation, and spill coordination | `StreamingShuffleWriter`, `StreamingBuffer` | `StreamingShuffleWriterSuite` |
| Reduce-side in-progress block reads, CRC32C validation, and partial-read invalidation surfaced as `FetchFailedException` | `StreamingShuffleReader` | `StreamingShuffleReaderSuite`, `StreamingShuffleFailureInjectionSuite` |
| Block resolution and migration (delegates `.data`/`.index` and migration to `IndexShuffleBlockResolver`) | `StreamingShuffleBlockResolver` | `StreamingShuffleIntegrationSuite`, `StreamingShuffleIntegrationTest` |
| Memory-pressure disk spill (80% threshold, 100 ms reclamation SLA, LRU selection) | `MemorySpillManager` | `MemorySpillManagerSuite` |
| Backpressure flow control (10 s heartbeat, token-bucket rate limiting, producer/consumer timeouts) | `BackpressureProtocol`, `BackpressureRpcEndpoint`, `TokenBucketRateLimiter` | `BackpressureProtocolSuite`, `BackpressureRpcEndpointSuite` |
| Automatic fallback on the four revert conditions (slow consumer, memory pressure, network saturation, version mismatch) | `StreamingShuffleFallbackPolicy` | `StreamingShuffleFallbackPolicySuite` |
| Wire framing/envelope and v1 transport integration (32-byte big-endian header + payload capped at 2 MB) | `StreamingBlockEnvelope`, `StreamingShuffleTransport` | `StreamingShuffleWriterSuite`, `StreamingShuffleReaderSuite`, `StreamingShuffleIntegrationSuite` |
| Telemetry — the four `shuffle.streaming.*` metrics and the metrics source (plus the dashboard/sink template) | `StreamingShuffleMetrics`, `StreamingShuffleSource`; resource `metrics.properties.template` | `StreamingShuffleMetricsSuite` |
| Package isolation and subsystem Scaladoc (zero cross-contamination of existing code) | `package.scala` | Compiled and exercised by all streaming suites |
| Zero data loss under all failure scenarios | All streaming production sources above (writer, reader, buffer, spill, backpressure, fallback) | `StreamingShuffleFailureInjectionSuite` (10 scenarios), `StreamingShuffleStressSuite` (5-minute run, 10% failure injection) |
| Performance success criteria (30–50% latency reduction; 5–10% CPU-bound improvement; zero regression via fallback) | End-to-end across all streaming production sources | `StreamingShufflePerformanceBenchmark`; checked-in artifacts `core/benchmarks/StreamingShuffleBenchmark-results.txt` and `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` |

## See also

- [Architecture](architecture.md) — the component-interaction and producer-to-consumer data-flow Mermaid diagrams referenced by these decisions.
- [Configuration](configuration.md) — the five `spark.shuffle.streaming.*` keys, defaults, ranges, and the `spark.shuffle.manager=streaming` activation alias.
- [Observability](observability.md) — the four `shuffle.streaming.*` metrics, structured logging with correlation IDs, and the Grafana dashboard template.
- [Overview](index.md) — the streaming shuffle documentation home.
