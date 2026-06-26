# Streaming Shuffle — Architecture Decision Log

This log is the single, authoritative home for the **design rationale** behind the streaming
shuffle subsystem. Per the project Explainability rule (R2), every non-trivial design decision is
recorded here with its context, the alternatives that were weighed, and its consequences — rather
than as narrative prose embedded in production source comments. Source files carry concise,
functional Scaladoc describing *what* a component does and its contract; this log explains *why*
the design is the way it is, and points back to the affected files.

It also carries an explicit **deviations log** and a bidirectional
**requirement → source → test traceability matrix** so that every feature requirement, every
code-review finding resolved at this checkpoint, and every intentional deviation maps to the code
that implements it and the test that proves it.

## How to read this log

- **ADR table** — one row per decision, with a stable `ADR-NN` identifier. The `Drivers`
  column references the feature id (`F-1xx`, from the Feature Catalog) and/or the code-review
  finding id it satisfies.
- **Detailed rationale** — fuller narrative for the decisions whose reasoning previously lived in
  source comments. These sections are the canonical text; the corresponding Scaladoc was trimmed
  to a one-line summary plus a pointer to the relevant `ADR-NN`.
- **Deviations** — anything that intentionally departs from a literal reading of the plan; in this
  feature there is exactly one, the v1 logging-only network transport.
- **Traceability matrix** — requirement ⇄ source ⇄ test, used for acceptance.

---

## Architecture Decision Records

| ADR | Decision | Rationale (summary) | Alternatives Considered | Consequences | Drivers | Status |
|-----|----------|---------------------|--------------------------|--------------|---------|--------|
| ADR-01 | **Compose, never replace, the sort manager.** `StreamingShuffleManager` holds an inner `SortShuffleManager` and forwards to it whenever streaming is not engaged. | Coexistence is a first-class constraint: the default and the fallback path must remain provably identical to plain sort-based shuffle. | Subclass or replace `SortShuffleManager`, or register streaming as the global default. Rejected: either breaks coexistence and the zero-regression guarantee. | Zero behavioral change when streaming is off; one extra object per executor. | F-101 | Accepted |
| ADR-02 | **Dual-flag activation via the `"streaming"` alias only.** Streaming engages iff `spark.shuffle.manager=streaming` (resolved through the short-name alias) **and** `spark.shuffle.streaming.enabled=true`. FQCN selection leaves the path disengaged. | Matches the published activation contract exactly; avoids a second, undocumented activation route through fully-qualified-class-name selection. | Single-flag activation (manager alias only), or activation via fully-qualified-class-name selection. Rejected: a single flag removes the opt-in safety valve, and FQCN selection creates a second, undocumented activation route. | Selecting the manager by FQCN instantiates it but delegates every shuffle to sort. | F-101, F-114, Finding M9 | Accepted |
| ADR-03 | **Writer extends `ShuffleWriter` and composes a private `MemoryConsumer`.** | Both base types are concrete classes and Scala forbids extending two; the object returned by `getWriter` must be a `ShuffleWriter[K, V]`. Composition mirrors how the sort path layers `Spillable` over a `MemoryConsumer`. | Extend `MemoryConsumer` directly (impossible: `ShuffleWriter` is also a class and Scala has no multiple inheritance), or skip cooperative spilling. Rejected: `getWriter` must return a `ShuffleWriter[K, V]` and the writer must honor cooperative spilling. | The writer participates in cooperative spilling through the inner consumer rather than by inheritance. | F-103 | Accepted |
| ADR-04 | **Publish through `IndexShuffleBlockResolver`; do not "pipeline" bytes in v1.** At commit the writer frames each partition, writes the frames to one temp data file, and commits the file plus a per-partition index atomically. | The v1 data plane is a logging-only stub (ADR-15), so the only fetchable channel is the existing `MapOutputTracker` + `BlockTransferService` path. Publishing through the shared index resolver makes every advertised byte fetchable and keeps block migration/decommission unified. | Build a bespoke streaming block store, or pipeline bytes over a new Netty channel in v1. Rejected: a bespoke store duplicates migration/decommission machinery, and the v1 data plane is an intentional stub (ADR-15), so the existing fetch path is the only channel. | `MapStatus` describes real on-disk framed sizes; a reducer fetches via the standard path. | F-103, F-105, F-115, Findings C1, C2 | Accepted |
| ADR-05 | **Fold spilled bytes back into published output (oldest-first ⧺ resident).** A spilled buffer's segments are concatenated in spill order ahead of the final resident snapshot. | A partition buffer is one continuous serialization stream; concatenating spilled segments in spill order then the resident tail reconstructs the exact original byte stream — so spilling never drops records. | Re-sort/merge spilled segments by key, or drop spilled bytes and recompute. Rejected: a buffer is one continuous serialization stream, so order-preserving concatenation is exact and cheaper; dropping bytes would lose records. | Zero data loss across the spill path; the spill ledger preserves order. | F-103, F-109, Finding C1 | Accepted |
| ADR-06 | **Reset (release) a buffer's heap immediately after a successful, reader-visible spill; `stop()` resets all still-registered buffers before clearing the registry.** | The spill manager exists to *reclaim* memory; registering a spill without releasing the heap would defeat its purpose. | Keep buffers resident after registering a spill (lazy free), or rely on the GC to reclaim. Rejected: registering a spill without releasing the heap defeats the spill manager's purpose of returning memory at the threshold. | Memory is actually returned at the 80% threshold; the per-key ledger retains the bytes' identity for commit. | F-109, Finding M1 | Accepted |
| ADR-07 | **Wire the spill manager into the runtime: the writer registers each buffer; a consumer acknowledgment routed from the reader triggers `reclaim` within 100 ms.** | A poller over an empty registry cannot protect anything; registration + ack-driven reclaim makes the 100 ms reclaim SLA observable in real execution. | A poll-only spill manager with no buffer registration, or reclaim on a fixed timer. Rejected: an empty registry protects nothing, and a timer cannot meet the 100 ms ack-driven reclaim SLA. | The spill monitor sees live buffers; reader acks free producer memory. | F-109, Findings M2, M11 | Accepted |
| ADR-08 | **Preserve serializer/compression symmetry: the writer wraps its output with `serializerManager.wrapStream(ShuffleBlockId(...), out)`; the reader unwraps symmetrically.** | `spark.shuffle.compress` defaults to `true` (LZ4). The reconstructed `spilled-oldest-first ⧺ resident` byte stream must be the exact contiguous compressed stream the reader decodes, or deserialization corrupts silently. | Store raw uncompressed bytes, or compress each spill segment independently. Rejected: `spark.shuffle.compress` defaults to true and the reader decodes one contiguous wrapped stream, so asymmetric wrapping corrupts deserialization silently. | Round-trip is byte-for-byte; spill segments must be drained in the same wrapped stream. | F-103, F-104 | Accepted |
| ADR-09 | **Enforce the 5 s producer timeout with a bounded async fetch.** Replace the `Duration.Inf` `fetchBlockSync` with `fetchBlocks` + a `Promise`, awaited with `ThreadUtils.awaitResult` bounded by the remaining time to the deadline; on expiry invalidate immediately. | A blocking infinite wait can never reach the deadline check, so a hung fetch would never invalidate and the zero-data-loss timeout could not be honored. | Keep `fetchBlockSync` with `Duration.Inf`, or busy-wait poll for completion. Rejected: an infinite blocking wait can never reach the deadline check, so the 5 s timeout (and thus zero data loss) could not be honored. | The 5 s timeout is enforceable; expiry throws `FetchFailedException` so the DAG scheduler recomputes. | F-104, Finding C3 | Accepted |
| ADR-10 | **Decode frame-by-frame with an upfront budget guard; never allocate the whole fetched buffer first.** Validate the 32-byte header (magic/version/`payloadLength ≤ 2 MiB`), then read exactly one payload, then CRC32C-verify. On a CRC32C mismatch the reader re-fetches the block (bounded retransmission with exponential backoff) within the producer deadline, and invalidates only on a *persistent* mismatch or a structural decode error. A pre-read budget guard rejects a block whose transport size grossly exceeds the published partition size. | A corrupt or non-streaming block must not be able to force an unbounded allocation before the per-envelope guard runs; a *transient* corruption must be recoverable per the AAP "validate checksums with retransmission" contract without forcing a stage recompute. | Allocate the whole fetched buffer first and then validate, or invalidate immediately on any CRC32C mismatch with no retransmission. Rejected: a single upfront allocation is unbounded under corruption, and immediate invalidation would violate the AAP "validate checksums with retransmission" contract; bounded retransmission within the deadline (invalidating only on persistent corruption) is preferred. | Largest single allocation is one header + one ≤ 2 MiB frame; transient corruption is recovered by bounded re-fetch, while a persistent CRC32C mismatch or a structural decode error invalidates and defers to recomputation. | F-104, F-116, Finding M7 | Accepted |
| ADR-11 | **Key the acknowledgment watermark per stream.** `StreamKey(shuffleId, reducePartitionId, consumerAttemptId, consumerExecutorId)` → its own `AtomicLong`, advanced by a CAS loop. | A single global watermark let a stale/duplicate/out-of-scope ack corrupt unrelated flow-control state. Per-stream keying contains any such ack to its own (possibly unknown) key. | A single global acknowledgment watermark, or no watermark (accept every ack). Rejected: a global watermark lets a stale, duplicate, or out-of-scope ack corrupt unrelated flow-control state. | Acks are isolated per stream; monotonic merge is preserved per key. | F-107, Finding M3 | Accepted |
| ADR-12 | **Add a 10 s consumer-liveness / missing-ack detector, distinct from the 5 s flow-control heartbeat.** | The timing model defines two separate timers; only the 5 s heartbeat existed. The 10 s detector is the consumer-failure signal feeding fallback/invalidation. | Reuse the 5 s flow-control heartbeat as the consumer-failure signal, or add no consumer-failure detector. Rejected: the timing model defines two distinct timers; conflating them loses the consumer-failure signal that feeds fallback/invalidation. | A silent consumer is detected at 10 s and synthesizes a consumer-lag signal for the fallback policy. | F-107, Finding M4 | Accepted |
| ADR-13 | **Validate and sanitize every backpressure RPC message; route acks to per-stream state.** Reject negative ids/seq/bytes, empty executor id, and out-of-scope identities; bound/sanitize the free-text `reason` (`MAX_REASON_LENGTH = 256`). | The endpoint advanced flow-control state and logged free text from unvalidated messages — a state-corruption and unbounded-log risk. | Trust message contents and log the raw free-text `reason`. Rejected: unvalidated messages corrupt flow-control state, and unbounded free text is a log-flooding risk. | Malformed/out-of-scope messages are rejected and counted; reasons are length-bounded. | F-108, Finding M6 | Accepted |
| ADR-14 | **Decide automatic fallback once per shuffle at registration, not per map task mid-flight.** `registerShuffle` evaluates `StreamingShuffleFallbackPolicy`; on any triggered reason the whole shuffle registers on the inner sort manager. | One `StreamingShuffleHandle` drives both writer and reader; switching only one side mid-flight would desynchronize the wire format and break zero-data-loss. | Decide fallback per map task mid-flight, or switch only one side (writer or reader). Rejected: one `StreamingShuffleHandle` drives both sides, so a mid-flight or one-sided switch desynchronizes the wire format and breaks zero data loss. | Fallback is consistent end-to-end for a shuffle; signals are gathered on the driver at registration. | F-101, F-111, Finding M10 | Accepted |
| ADR-15 | **Ship the v1 network transport as the single, intentional logging-only stub.** `StreamingShuffleTransport.send` logs and returns `Unit`; `fetch` delegates to the existing `BlockTransferService`; no new transport context/port/Netty bootstrap. | The full Netty data plane is deferred; the existing fetch path is sufficient for a correct, end-to-end implementation now. This is the only permitted production-path stub. | Ship the full Netty data plane in v1, or block the feature until it is ready. Rejected: the existing fetch path is sufficient for a correct end-to-end v1, and deferring the data plane lets the feature land now without a later protocol change (this is the single permitted production stub). | Correctness does not depend on the stub; the data plane can land later without protocol change. | F-115 | Accepted (deviation — see Deviations) |
| ADR-16 | **Expose the inner sort manager's `IndexShuffleBlockResolver` from `shuffleBlockResolver`, not the streaming resolver.** | Spark internals (e.g. `BlockManager.diagnoseShuffleBlockCorruption`) cast `ShuffleManager.shuffleBlockResolver` directly to `IndexShuffleBlockResolver`; exposing the shared index resolver preserves that contract and unifies migration/decommission state. | Expose the streaming resolver from `shuffleBlockResolver`. Rejected: Spark internals cast that field directly to `IndexShuffleBlockResolver`, so exposing the streaming resolver would break the contract and split migration/decommission state. | The streaming in-memory index is held separately by `StreamingShuffleBlockResolver` over the same shared resolver. | F-101, F-105 | Accepted |
| ADR-17 | **Spill registry uses a Guava `Cache`; the per-key spill ledger uses a `ConcurrentLinkedQueue`.** | Guava is already on the classpath for `IndexShuffleBlockResolver`; its weakly-consistent `asMap()` iteration lets the poll thread scan while producers register/reclaim without `ConcurrentModificationException`. Per-key appends are already serialized under the buffer monitor. | A synchronized `HashMap` registry, or add a new concurrency dependency. Rejected: Guava is already on the classpath and its weakly-consistent `asMap()` lets the poll thread scan lock-free, so no new dependency is warranted. | No new dependency; lock-free registry scan and ledger append. | F-109, F-106 | Accepted |
| ADR-18 | **Token bucket caps the effective rate at 80 % of link capacity; 1 permit = 1 byte (Guava `RateLimiter`).** | Reuses a standard, well-tested primitive already available; the 80 % cap is the safety envelope that keeps the link from saturating. | A hand-rolled rate limiter, or no rate cap at all. Rejected: Guava `RateLimiter` is a standard, well-tested primitive already available, and an uncapped link can saturate (the 80 % cap is the safety envelope). | No new dependency; rate limiting is byte-accurate. | F-110, F-107 | Accepted |
| ADR-19 | **Emit four metrics through a Dropwizard `Source` registered with the existing `MetricsSystem`; add no new endpoint.** Gauge `bufferUtilizationPercent`, counters `spillCount`, `backpressureEvents`, `partialReadInvalidations` under `shuffle.streaming.`. | Reuse Spark's telemetry infrastructure (JMX/Prometheus/CSV/SLF4J sinks) rather than inventing a parallel surface. | A new bespoke metrics endpoint/HTTP server, or no metrics. Rejected: reusing the existing `MetricsSystem` surfaces the four metrics through every existing sink (JMX/Prometheus/CSV/SLF4J) with no new infrastructure. | Metrics surface through every existing sink; source name `streamingShuffle`. | F-112, F-113 | Accepted |
| ADR-20 | **Structured MDC logging via the typed `LogKeys`; map two documented field names to the existing keys** (`attempt_id → task_attempt_id` / `TASK_ATTEMPT_ID`, `reduce_partition_range → range` / `RANGE`). | The shared `LogKeys` Java module is out of scope (additive-MiMa surface); reusing existing keys avoids modifying it. Correlation fields are attached only where the identifier is in scope. | Add new keys to the shared `LogKeys` Java module, or embed correlation ids in free-text messages. Rejected: `LogKeys` is an out-of-scope additive-MiMa surface, and free-text ids cannot be extracted by a log4j2-aware aggregation pipeline. | Runtime logs carry shuffle/map/reduce/attempt correlation where applicable; the observability doc names the keys actually emitted. | R1 (observability) | Accepted |

---

## Detailed rationale

The following sections are the canonical, fuller statements of reasoning for the decisions whose
"why" narrative previously lived in source comments. The production Scaladoc now carries a concise
functional summary and a pointer to the relevant `ADR-NN`.

### ADR-03 — Writer single-inheritance design

`org.apache.spark.shuffle.ShuffleWriter` and `org.apache.spark.memory.MemoryConsumer` are both
concrete (abstract) classes, and Scala forbids extending two classes. The object returned by
`StreamingShuffleManager.getWriter` must be a `ShuffleWriter[K, V]`, so `StreamingShuffleWriter`
extends `ShuffleWriter` and **composes** a private inner `MemoryConsumer`
(`StreamingShuffleMemoryConsumer`) to participate in Spark's cooperative spilling protocol. This
deliberately mirrors how the sort path layers `org.apache.spark.util.collection.Spillable` over a
`MemoryConsumer`. The composed consumer's `spill` is invoked synchronously on the same task thread
by the `TaskMemoryManager`; the writer is single-threaded with respect to `write`.

### ADR-04 / ADR-05 — Publication and zero-data-loss assembly

Because the on-the-wire data plane is a logging-only stub in v1 (ADR-15), the writer publishes its
output through the same fetchable channel the consumer already reads from. At commit it frames
every partition into canonical ≤ 2 MiB `StreamingBlockEnvelope` records (each with its per-block
CRC32C), writes those frames sequentially to a single temporary data file, and commits the file and
its per-partition index atomically via the shared `IndexShuffleBlockResolver`. A reducer then
fetches a partition through the standard `MapOutputTracker` + `BlockTransferService` path and
decodes the frames.

Records are never silently dropped: each record is routed to exactly one partition buffer, and at
commit each partition's complete byte stream is reconstructed by concatenating its spilled segments
(in spill order) ahead of its final resident bytes. Buffers and all granted execution memory are
released on both the success and failure paths in `stop`'s `finally` block, and transient spill
blocks are always removed. On the failure path any committed output is removed so the DAG
scheduler's normal recomputation regenerates it cleanly. The `MapStatus` returned from `stop`
therefore describes the actual on-disk framed per-partition sizes, the map-task location, and an
aggregate CRC32C over all serialized bytes.

### ADR-06 / ADR-07 / ADR-17 — Spill block identity, reclamation, and registry

Each spill of a buffer drains its resident bytes to a fresh `TempLocalBlockId` (stored via
`BlockManager.putBytes(..., DISK_ONLY)`) and appends a `SpilledSegment` (block id, length, CRC32C)
to that key's ordered ledger. Insertion order is the on-the-wire byte order: because a buffer is
backed by a single continuous serialization stream, concatenating its spilled segments in spill
order and then appending the final resident snapshot reconstructs the original partition byte
stream exactly. The producing writer reads the ordered segments back via `spilledSegmentsFor` at
commit time.

After a successful, reader-visible spill the buffer is **reset** so its heap is released
immediately; on `stop()` any still-registered buffer is reset before the registry is cleared. The
registry is a Guava `Cache` (already on the classpath) for weakly-consistent `asMap()` iteration,
and the per-key ledger is a `ConcurrentLinkedQueue` so the poll thread can append while the writer
drains, without external locking (per-key appends are serialized under the buffer monitor inside
`StreamingBuffer#spillUnderLock`). Writer-created buffers are registered with the manager, and a
consumer acknowledgment routed from the reader triggers reclamation within the 100 ms SLA.

### ADR-11 / ADR-12 / ADR-13 — Per-stream acknowledgment, consumer liveness, message validation

Acknowledgment high-water marks are kept **per stream**, keyed by
`StreamKey(shuffleId, reducePartitionId, consumerAttemptId, consumerExecutorId)`, each advanced
under its own CAS loop so it is monotonically non-decreasing even under concurrent, out-of-order,
or duplicate acknowledgments. Keying per stream guarantees that a stale, duplicated, or out-of-scope
ack is contained to its own (possibly unknown) key and cannot corrupt the watermark of any
unrelated stream.

A separate 10 s consumer-liveness / missing-ack detector (`CONSUMER_LIVENESS_TIMEOUT_MS = 10000`)
tracks the most recent consumer signal per stream and is distinct from the single 5 s flow-control
heartbeat. The `BackpressureRpcEndpoint` validates message identity (non-negative ids/sequence
numbers/byte counts, non-empty executor id, in-scope identities) and bounds/sanitizes the free-text
`reason` before logging, then routes acks to the per-stream merge. Messages that fail validation
are rejected and counted rather than mutating flow-control state.

### ADR-14 — Registration-time, per-shuffle fallback

Automatic fallback (F-111) is decided once, per shuffle, at registration. One
`StreamingShuffleHandle` drives both the writer and the reader; switching only one side mid-flight
would desynchronize the on-the-wire format and break the zero-data-loss invariant. When
`registrationFallbackReason` reports a triggered condition the entire shuffle is registered on the
inner `SortShuffleManager` and is deliberately not tracked as streaming, so `getWriter` and
`getReader` consistently route it to the sort path. The four policy conditions are wired to the
signals observable on the driver at registration:

- **Memory pressure** — from the active memory manager's on-heap storage budget.
- **Consumer lag** — from the backpressure protocol's 10 s consumer-liveness detector (a missing
  tracked stream synthesizes a sustained-slowness sample so the lag predicate fires).
- **Network saturation** — reported as *unsaturated* in v1 because the stub transport has no
  measurable link utilization; the condition stays wired for the future data plane (a direct
  consequence of the v1 transport stub — see deviation D-1).
- **Version mismatch** — compares the producer/consumer streaming versions; v1 ships a single
  build so they match in practice and the predicate therefore never fires (exactly as the plan
  intends), while staying wired so a future rolling-version deployment degrades gracefully.

---

## Deviations

Per R2, intentional departures from a literal reading of the plan are logged explicitly. There is
exactly **one**, and the plan explicitly permits it: the v1 logging-only network transport (F-115).
Items recorded as separate deviations in earlier checkpoints are no longer departures and have been
removed: bounded CRC32C retransmission is now implemented (ADR-10), so the reader matches the AAP
"validate checksums with retransmission" contract; the MDC field-name mapping is the documented
ADR-20 design decision (reuse the existing typed `LogKeys`) and the observability doc names exactly
the emitted keys; and the version-mismatch predicate firing only on a future rolling upgrade is the
plan's fallback condition operating correctly under a single-build v1 (ADR-14).

| # | Deviation | Why it is acceptable | Reference |
|---|-----------|----------------------|-----------|
| D-1 | **v1 network transport is a logging-only stub.** `StreamingShuffleTransport.send` logs without transmitting; `fetch` delegates to the existing `BlockTransferService`; no new transport context, port, or Netty bootstrap is created. As a direct consequence, the network-saturation fallback predicate reports *unsaturated* in v1 (the stub exposes no measurable link-utilization signal to threshold); its wiring stays in place to activate unchanged once the real data plane reports utilization. | This is the single documented, intentional stub permitted by the plan (F-115). Correctness does not depend on it: the writer publishes through `IndexShuffleBlockResolver` and the reader fetches through the standard map-output path (ADR-04), so the subsystem is correct end-to-end today and the Netty data plane can land later without a protocol change. | ADR-15, F-115 |

---

## Traceability matrix

This matrix is **bidirectional**: left-to-right it maps each requirement to the exact source file
that implements it and the exact test suite that proves it; right-to-left, every source file and
every suite below traces back to a requirement, and the **Findings / deviation** column maps each
review finding (or the single F-115 deviation) to the row that resolves it. All paths are
repository-relative and exact. Feature ids are from the Feature Catalog; finding ids are from the
CP2 and CP3 code reviews.

| Requirement / Feature | Source file (exact repo path) | Test suite (exact repo path) | Findings / deviation |
|-----------------------|-------------------------------|------------------------------|----------------------|
| F-101 Manager: compose inner sort, dispatch on handle type, dual-flag activation, registration-time fallback, deterministic ordered `stop` | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | M9, M10, M11; CP3 manager dispatch/lifecycle |
| F-102 Dispatch handle carrying tuning vals | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala` | &mdash; |
| F-103 Writer: bounded per-partition buffers, spill, CRC32C, publish fetchable `MapStatus` | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | C1, C2, m2 |
| F-104 Reader: sort-reader parity (aggregation, map-side combine, key ordering), bounded fetch + 5 s timeout, frame decode, CRC32C validation with bounded retransmission | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | C3, M7, m1, M2; CP3 reader parity + retransmission |
| F-105 Block resolver: 3-level index, `MigratableResolver` by delegation to `IndexShuffleBlockResolver` | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolverSuite.scala` (dedicated resolver unit tests), `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` (resolver exposure), `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` (publication), `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala` and `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` (active publish&rarr;fetch round-trip) | &mdash; |
| F-106 Per-partition buffer: CRC32C, finalize/spill under lock, heap reset | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingBuffer.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | M1 (reset) |
| F-107 Backpressure: token bucket, 5 s heartbeat, per-stream monotonic ack, 10 s liveness | `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | M3, M4 |
| F-108 RPC endpoint: executor-only, validated routing | `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpoint.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpointSuite.scala` | M6 |
| F-109 Spill manager: 100 ms poll/reclaim, `DISK_ONLY` spill, buffer reset, runtime-wired | `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | M1, M2, M11 |
| F-110 Token-bucket rate limiter (80 % cap, 1 permit = 1 byte) | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | &mdash; |
| F-111 Fallback policy: four exact predicates | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicySuite.scala` | M10 (wiring) |
| F-112 Four metrics under `shuffle.streaming.` (see per-metric rows below) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala` | &mdash; |
| F-113 Metrics `Source` (`streamingShuffle`) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala` | &mdash; |
| F-114 Typed config + dual-flag accessors + range validation | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` (activation/dual-flag), `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicySuite.scala` (validation) | M9 |
| F-115 v1 logging-only transport stub (single intentional deviation) | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` (silent-transport model) | D-1 |
| F-116 Wire envelope: 32-byte header, &le; 2 MiB payload, CRC32C | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` (decode/checksum), `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` (encode), `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` (corruption) | M7 |
| F-118 Package doc + metrics template | `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala`, `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | &mdash; | &mdash; |
| Config `spark.shuffle.manager=streaming` (manager alias) | `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` (alias map), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` (`STREAMING_MANAGER_ALIAS`) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | M9 |
| Config `spark.shuffle.streaming.enabled` (Boolean, default `false`) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` (`ConfigEntry`), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` (`enabled`) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | M9 |
| Config `spark.shuffle.streaming.bufferSizePercent` (Int, default `20`, [1, 50]) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` (`ConfigEntry` + range), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` (`bufferSizePercent`) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | &mdash; |
| Config `spark.shuffle.streaming.spillThreshold` (Int, default `80`, [50, 95]) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` (`ConfigEntry` + range), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` (`spillThreshold`) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | &mdash; |
| Config `spark.shuffle.streaming.maxBandwidthMBps` (Int, default `0` = unlimited) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` (`ConfigEntry`), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` (`maxBandwidthMBps`, `effectiveBandwidthMBps`) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | &mdash; |
| Config `spark.shuffle.streaming.debug` (Boolean, default `false`) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` (`ConfigEntry`), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` (`debug`) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` (config construction) | &mdash; |
| Metric `bufferUtilizationPercent` (Gauge) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala`, registered via `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala` | &mdash; |
| Metric `spillCount` (Counter) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala`, registered via `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | &mdash; |
| Metric `backpressureEvents` (Counter) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala`, registered via `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | &mdash; |
| Metric `partialReadInvalidations` (Counter) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala`, registered via `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` | &mdash; |
| Active streaming output parity vs sort (&ge; 10 partitions, &ge; 2 shuffle shapes) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala`, `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala`, `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala`, `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | CP3 active parity |
| Zero data loss across the ten failure scenarios (true 5 s timeout, end-to-end recompute) | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala`, `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` | CP3 zero data loss |
| 5-minute active-streaming stress, ~10 % failure injection, no retained heap | full subsystem under `core/src/main/scala/org/apache/spark/shuffle/streaming/` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleStressSuite.scala` | CP3 stress |
| Latency benchmark (sort vs streaming) via `BenchmarkBase`; no result file unless requested | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` (+ data path) | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | CP3 benchmark run path |
| R1 Observability: MDC correlation (`shuffle_id`, `map_id`, `range`, `task_attempt_id`) + doc accuracy | all six runtime components under `core/src/main/scala/org/apache/spark/shuffle/streaming/`; `blitzy-docs/streaming-shuffle/observability.md` | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala` + compile/suite logs | M8, R1 |
| R2 Explainability: rationale lives in this decision log, not in code comments | `blitzy-docs/streaming-shuffle/decision-log.md` (this file) | &mdash; | R2; CP3 decision-log |
| R3 Visual architecture docs accurate (Diagram 0.2-A byte-identical across docs) | `blitzy-docs/streaming-shuffle/architecture.md`, `blitzy-docs/streaming-shuffle/index.md` | &mdash; | M5, R3 |
| R4 Executive presentation (16-slide reveal.js deck, accurate MDC fields) | `blitzy-docs/streaming-shuffle/executive-summary.html` | &mdash; | CP3 deck accuracy |
| R5 Segmented PR review ledger advanced to CP3 with final verdict | `CODE_REVIEW.md` | &mdash; | R5 (governance) |

---

## Configuration reference

| Config key | Type | Default | Range | ADR |
|------------|------|---------|-------|-----|
| `spark.shuffle.manager` | String | `sort` | `sort` / `tungsten-sort` / `streaming` | ADR-01, ADR-02 |
| `spark.shuffle.streaming.enabled` | Boolean | `false` | — | ADR-02 |
| `spark.shuffle.streaming.bufferSizePercent` | Int | `20` | 1–50 | ADR-03 |
| `spark.shuffle.streaming.spillThreshold` | Int | `80` | 50–95 | ADR-05, ADR-06 |
| `spark.shuffle.streaming.maxBandwidthMBps` | Int | `0` (unlimited) | — | ADR-18 |
| `spark.shuffle.streaming.debug` | Boolean | `false` | — | — |

## References

- Architecture overview and diagrams: [architecture.md](architecture.md)
- Configuration details: [configuration.md](configuration.md)
- Observability (metrics, MDC schema, dashboard): [observability.md](observability.md)
