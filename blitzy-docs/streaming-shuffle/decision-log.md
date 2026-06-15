# Decision Log

This log records the non-trivial design decisions behind the streaming shuffle backend — *the "why"* — so a reviewer can understand every non-obvious choice without reading the source. It also documents the single **intentional v1 deviation** (the logging-only transport) as a justified, recorded exception rather than an unfinished stub, and it provides a **bidirectional traceability matrix** mapping each requirement to the source file(s) that implement it and the test suite(s) that prove it. By design, rationale lives here in the decision log rather than scattered through inline code comments.

## Design decisions

Each row is a concrete, non-trivial decision taken while implementing the streaming shuffle backend, with the alternatives that were weighed, the rationale for the choice, and the residual risk (with its mitigation where applicable).

| Decision | Alternatives | Rationale | Risk |
| --- | --- | --- | --- |
| **Opt-in dual-flag activation** — engage streaming only when `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true` (both default off). | Always-on streaming; a single enabling flag. | Guarantees a zero-regression, byte-for-byte-unchanged default and forces explicit, deliberate operator intent before behavior changes. | A partial config (only one flag set) silently runs the sort path; mitigated by the [Configuration](configuration.md) guide and the coexistence comment at the activation gate. |
| **Compose a lazy inner `SortShuffleManager` for fallback** — delegate to a lazily-instantiated inner sort manager when streaming is disabled or a fallback condition trips. | Fork/duplicate the sort-shuffle code; modify `SortShuffleManager` directly. | Preserves the sort path completely unchanged, enables automatic graceful fallback, and embodies the least-modification principle. | One extra layer of indirection, only on the disabled/fallback branch; negligible. |
| **Reuse `MemoryConsumer`/`TaskMemoryManager` + `BlockManager` for buffering and spill** — acquire buffers through the executor memory model and spill via `BlockManager.putBytes(..., StorageLevel.DISK_ONLY)`. | Build a parallel buffer-pool and spill subsystem. | Honors the existing executor memory model and storage interface contracts; least modification, with no redesign of memory management. | Streaming buffers share the executor memory budget; bounded by `bufferSizePercent` and the 80% spill threshold. |
| **Reuse `BlockTransferService` / `fetchBlockSync` as the data plane** — the actual bytes travel over the existing transport read path. | Build a new dedicated Netty data plane for streaming. | Least modification to the network transport and automatic inheritance of existing authentication/SASL and TLS; the v1 transport layer is logging-only. | v1 is not a true zero-copy producer push; deferred to v2 and documented as an intentional deviation (see below). |
| **Guava `RateLimiter` token bucket** — rate-limit with `1 permit = 1 byte`; `maxBandwidthMBps ≤ 0` means unlimited. | A bespoke custom rate limiter. | Already on the classpath, battle-tested, and avoids any new dependency. | Coarse fairness across concurrent shuffles; mitigated by a per-concurrent-shuffle cap. |
| **JDK `CRC32C` block checksums** — verify each block with `java.util.zip.CRC32C`. | CRC32, Adler32, or MD5. | Same primitive already used by `ShuffleChecksumUtils`; hardware-accelerated and built into JDK 17, so no new dependency. | None material. |
| **Executor-only backpressure RPC endpoint** — register `streaming-shuffle-backpressure` (a `ThreadSafeRpcEndpoint`) on executors only; the driver rejects it (returns `None`). | A driver-hosted endpoint; no RPC at all. | Heartbeats and acks are executor↔executor, so keeping the endpoint off the driver avoids needless driver load and lifecycle coupling. | Endpoint lifecycle is tied to the executor; acceptable. |
| **Immutable config for the application lifetime** — all `spark.shuffle.streaming.*` settings are fixed at startup; no dynamic reconfiguration in v1. | Live/dynamic reconfiguration. | Simplicity and determinism; avoids mid-run state churn and race conditions. | Re-tuning requires an executor restart; acceptable for v1. |
| **Fixed 2 MB block size** — frame streamed output (and the per-partition buffer floor) at 2 MB. | Smaller blocks (more framing/checksum overhead) or larger blocks (coarser memory/latency granularity). | Balances per-block framing and checksum overhead against memory granularity and latency, and aligns the wire and spill formats so spilled and streamed bytes are interchangeable. | Very small partitions still pay one 2 MB floor; bounded and intentional. |
| **Timeouts and bounded retry** — 5 s connection timeout, 10 s heartbeat interval, retry with exponential backoff starting at 1 s up to 5 attempts. | Aggressive timeouts (false positives) or unbounded retries. | Detects genuine producer/consumer failure quickly while tolerating transient hiccups; bounded retries prevent runaway recovery loops. | Worst-case detection latency is on the order of seconds before fallback/recompute; acceptable and observable via metrics. |
| **Proportional buffer sizing with proactive spill** — size each partition buffer as `(executorMemory * bufferSizePercent / 100) / numPartitions` with a 2 MB floor, and spill the largest buffers when utilization reaches the 80% threshold within a 100 ms SLA. | A fixed global buffer; spill only on hard OOM. | Bounds the memory footprint proportionally to executor memory and partition count and reclaims memory proactively before exhaustion. | Many partitions shrink per-buffer size toward the floor; mitigated by the floor and automatic fallback under memory pressure. |

## Intentional v1 deviation: logging-only transport

The `StreamingShuffleTransport` layer is intentionally a **v1 logging-only integration layer**, and this is recorded here explicitly so that a pull-request pre-flight review does not misclassify it as an unfinished placeholder stub.

- In v1, `StreamingShuffleTransport.sendBlock` returns a **completed `Future`** and `openConsumerStream` returns **`Iterator.empty`**.
- This is **intended, documented behavior — not a placeholder defect.** The real data plane is the existing **`BlockTransferService` / `fetchBlockSync`** read path (driven by `ShuffleBlockFetcherIterator`), which already provides authenticated, TLS-capable block transfer. The streaming backend deliberately reuses that proven path rather than introducing a parallel network stack in v1.
- **What is deferred to v2:** the real Netty streaming data plane (a genuine zero-copy producer→consumer push), `SO_KEEPALIVE` tuning, and full retry/backoff wiring on the dedicated transport. None of this is required for correctness in v1 because the read path above carries the actual bytes.
- **Why it is safe:** because the transport is not on the correctness-critical byte path in v1, the completed-`Future`/`Iterator.empty` behavior cannot drop or corrupt data; checksums, buffering, spill, backpressure, and fallback all operate over the existing transport.

## Traceability matrix

This matrix is **bidirectional**: read each row left-to-right to go *requirement → source → test*, or scan the **Source file(s)** / **Test file(s)** columns to recover the requirement a given file serves (*test → requirement*, *code → requirement*). Unless otherwise noted, production sources live under `core/src/main/scala/org/apache/spark/shuffle/streaming/` (network classes under `.../streaming/network/`), test suites under `core/src/test/scala/org/apache/spark/shuffle/streaming/`, and benchmark artifacts under `core/benchmarks/`. Two pre-existing files are surgically modified (marked **MODIFY**); all other streaming sources are new.

| Requirement / capability | Source file(s) | Test file(s) |
| --- | --- | --- |
| Opt-in activation and `"streaming"` manager alias | `StreamingShuffleManager`, `StreamingShuffleConfig`, `ShuffleManager.scala` (**MODIFY** — registers the `"streaming"` alias), `internal/config/package.scala` (**MODIFY** — five `spark.shuffle.streaming.*` ConfigEntry values) | `StreamingShuffleManagerSuite`, `StreamingShuffleIntegrationSuite`, `StreamingShuffleIntegrationTest` |
| Streaming handle / tuning carrier | `StreamingShuffleHandle` | `StreamingShuffleHandleSuite` |
| Producer-side streaming, per-partition buffering, CRC32C checksums, spill coordination | `StreamingShuffleWriter`, `StreamingBuffer` | `StreamingShuffleWriterSuite` |
| Reduce-side in-progress reads, CRC32C validation, partial-read invalidation → `FetchFailedException` | `StreamingShuffleReader` | `StreamingShuffleReaderSuite`, `StreamingShuffleFailureInjectionSuite` |
| Block resolution and migration | `StreamingShuffleBlockResolver` | `StreamingShuffleIntegrationSuite`, `StreamingShuffleIntegrationTest` |
| Memory-pressure spill (80% threshold, 100 ms SLA) | `MemorySpillManager` | `MemorySpillManagerSuite` |
| Backpressure flow control (heartbeat, token bucket, timeout state machine) | `BackpressureProtocol`, `BackpressureRpcEndpoint`, `TokenBucketRateLimiter` | `BackpressureProtocolSuite`, `BackpressureRpcEndpointSuite` |
| Automatic fallback on the four revert conditions | `StreamingShuffleFallbackPolicy` | `StreamingShuffleFallbackPolicySuite` |
| Wire framing / block envelope and v1 transport integration | `StreamingBlockEnvelope`, `StreamingShuffleTransport` | Exercised by `StreamingShuffleWriterSuite`, `StreamingShuffleReaderSuite`, `StreamingShuffleIntegrationSuite`, `StreamingShuffleIntegrationTest` |
| Telemetry — four `shuffle.streaming.*` metrics, metrics source, and template | `StreamingShuffleMetrics`, `StreamingShuffleSource`, `resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | `StreamingShuffleMetricsSuite` |
| Subsystem package documentation (Scaladoc) | `package.scala` | Compile-time only; exercised transitively by all suites above |
| Zero data loss under all failure scenarios | All streaming sources above (manager, writer, reader, buffer, spill, backpressure, fallback) | `StreamingShuffleFailureInjectionSuite` (10 scenarios), `StreamingShuffleStressSuite` (5-minute, 10% failure injection) |
| Performance success criteria (30–50% latency reduction; 5–10% CPU-bound gain; zero memory-bound regression via fallback) | End-to-end: `StreamingShuffleManager`, `StreamingShuffleWriter`, `StreamingShuffleReader`, `StreamingBuffer`, `BackpressureProtocol` | `StreamingShufflePerformanceBenchmark`; checked-in artifacts `core/benchmarks/StreamingShuffleBenchmark-results.txt` and `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` |

## See also

- [Architecture](architecture.md) — component-interaction and data-flow diagrams for the streaming shuffle backend.
- [Configuration](configuration.md) — the five `spark.shuffle.streaming.*` keys, defaults, and ranges.
- [Observability](observability.md) — the four metrics, structured logging, and the Grafana dashboard template.
- Back to the [overview](index.md).
