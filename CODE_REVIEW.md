# Code Review — Streaming Shuffle Backend

> **Segmented PR Review Artifact.** This is the mandated Segmented PR Review deliverable required by the project's review rule (AAP §0.6.2), listed in §0.4.1 (Group 9) and §0.5.1. It lives at the **repository root** (`CODE_REVIEW.md`) and is the authoritative, multi-phase review of the **Streaming Shuffle** feature change set for the Apache Spark `blitzy-spark` fork. It contains no executable code; it is a review record only.

> **Checkpoint scope (read this first).** This is a **Checkpoint 1 (CP1)** review: *Documentation Deliverables, Integration Wiring & Foundation/Primitive Production Classes*. The Streaming Shuffle feature is planned across multiple checkpoints. **25 of the 48 planned files are delivered at CP1**; the remaining **23 files** (the manager/writer/reader/block-resolver/spill/backpressure/transport production classes, 13 test suites, and 2 benchmark result files) are **deferred to CP2/CP3** and are therefore **out of scope for this review** — they are inventoried as *NOT YET DELIVERED* in §2.2 and §5.11, not reviewed or approved. This artifact reviews **only the 25 delivered files** and does **not** claim the full feature is complete.

---

## Status Banner

| Field | Value |
| ------- | ------- |
| **Review status** | CHECKPOINT 1 — delivered surface reviewed; feature in progress |
| **Overall verdict** | **CP1 DELIVERED SURFACE: APPROVED** · **FULL FEATURE: INCOMPLETE (not merge-ready as a whole)** |
| **Pre-flight gate** | **GREEN over the 25 delivered CP1 files** (build/static-analysis clean; full-feature test/coverage/benchmark gates deferred — see §3) |
| **Current phase** | Final Re-Verification (CP1 closed) |
| **Target artifact** | `spark-core_2.13` under `spark-parent_2.13:4.2.0-SNAPSHOT` |
| **Feature** | Opt-in Streaming Shuffle backend in `org.apache.spark.shuffle.streaming` (+ `…/network`) |
| **Files planned (full feature)** | **48** (2 modified existing + 46 newly created) |
| **Files delivered & reviewed at CP1** | **25** (2 modified + 23 newly created) |
| **Files deferred to CP2/CP3 (not reviewed)** | **23** (8 production classes + 13 test suites + 2 benchmark artifacts) |
| **Domain review phases** | 6 sequential phases over delivered files, each resolving to `APPROVED` or `BLOCKED` |
| **Review date** | 2026-06-15 |

### Commit cadence (explicit)

Per the Segmented PR Review rule, this artifact is committed on a defined cadence so its state is always visible in version control:

1. **Committed at the checkpoint.** `CODE_REVIEW.md` is committed with the CP1 pre-flight gate and per-phase verdicts recorded for the delivered surface.
2. **Re-committed at every phase transition.** As each domain phase closes (and the next opens), the status banner and the completed phase's verdict are updated and re-committed.
3. **Re-committed for each checkpoint verdict.** When subsequent checkpoints (CP2/CP3) land the deferred files, this artifact is re-reviewed and re-committed with the expanded scope and updated verdicts.
4. **Present in the pull request's final commit.** This artifact is guaranteed to be part of the PR's final commit so the delivered review state ships with the change set.

---

## 1. Feature Summary

The change set adds an **opt-in Streaming Shuffle backend** to Spark Core that is designed to eliminate shuffle-materialization latency by streaming intermediate data directly from producer (map-side) executors to consumer (reduce-side) executors through bounded in-memory buffers and the existing `org.apache.spark.network` transport, governed by a backpressure protocol, while preserving the existing sort-based shuffle as an automatic fallback. The implementation is delivered as a self-contained, isolated package, `org.apache.spark.shuffle.streaming` (with a `network/` subpackage), that is designed to implement the `ShuffleManager` service-provider contract and compose — never bypass — the existing `SortShuffleManager`.

The feature is **additive and opt-in**. Exactly **two** existing source files are modified (a one-line factory alias and five new configuration entries); everything else is newly created. Activation requires **both** `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`; because both default to off, the default behavior of every existing Spark deployment is byte-for-byte unchanged.

> **CP1 delivery note.** At Checkpoint 1, the two integration edits, the configuration and foundation/primitive classes, the observability primitives, the documentation set, and one handle test suite are delivered. The runtime classes that actually engage the streaming data path — `StreamingShuffleManager` (and its fallback delegation), `StreamingShuffleWriter`, `StreamingShuffleReader`, `StreamingShuffleBlockResolver`, `MemorySpillManager`, `BackpressureProtocol`, `BackpressureRpcEndpoint`, and the `network/StreamingShuffleTransport` integration layer — are **not yet delivered** and land in CP2/CP3. Consequently, end-to-end streaming, fallback delegation, spill, backpressure, and the full test/benchmark gates are **not exercisable at CP1** and are explicitly deferred below.

---

## 2. Review Scope

This review partitions **every delivered CP1 file** into **exactly one** sequential domain phase and records an explicit `APPROVED`/`BLOCKED` verdict per phase. Deferred CP2/CP3 files are inventoried separately (§2.2, §5.11) and are **not** assigned a phase or a verdict at this checkpoint.

### 2.1 Delivered & reviewed at CP1 (25)

- **Modified existing source (2):** the `ShuffleManager` factory alias and the internal config registry.
- **New production Scala (9):** 7 classes in `…/streaming/` (`StreamingShuffleHandle`, `StreamingShuffleConfig`, `StreamingShuffleMetrics`, `StreamingShuffleSource`, `StreamingBuffer`, `StreamingShuffleFallbackPolicy`, `package.scala`) and 2 in `…/streaming/network/` (`StreamingBlockEnvelope`, `TokenBucketRateLimiter`).
- **New resource (1):** the metrics configuration template.
- **New tests (1):** `StreamingShuffleHandleSuite`.
- **New documentation (11):** 7 TechDocs under `blitzy-docs/streaming-shuffle/` and 4 Jekyll guides under `docs/`.
- **This artifact (1):** `CODE_REVIEW.md`.

### 2.2 Deferred to CP2/CP3 — NOT YET DELIVERED, not reviewed (23)

These files are planned by the AAP but are absent from the repository at CP1. They are listed here for traceability and are **explicitly excluded** from every phase verdict and the coverage matrix's exact-once partition (which covers delivered files only).

- **Production Scala — Backend (7):** `StreamingShuffleManager.scala`, `StreamingShuffleWriter.scala`, `StreamingShuffleReader.scala`, `StreamingShuffleBlockResolver.scala`, `MemorySpillManager.scala`, `BackpressureProtocol.scala`, `network/StreamingShuffleTransport.scala`.
- **Production Scala — Security surface (1):** `BackpressureRpcEndpoint.scala`.
- **Tests (13):** `BackpressureProtocolSuite`, `BackpressureRpcEndpointSuite`, `MemorySpillManagerSuite`, `StreamingShuffleFailureInjectionSuite`, `StreamingShuffleFallbackPolicySuite`, `StreamingShuffleIntegrationSuite`, `StreamingShuffleIntegrationTest`, `StreamingShuffleManagerSuite`, `StreamingShuffleMetricsSuite`, `StreamingShufflePerformanceBenchmark`, `StreamingShuffleReaderSuite`, `StreamingShuffleStressSuite`, `StreamingShuffleWriterSuite`.
- **Benchmark artifacts (2):** `core/benchmarks/StreamingShuffleBenchmark-results.txt`, `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt`.

### 2.3 Out of scope / absolute preservation (verified untouched)

RDD/DataFrame/Dataset user-facing APIs; the DAG scheduler and task-scheduling algorithms; executor lifecycle management; lineage tracking and the fault-recovery model; the existing `SortShuffleManager` implementation (to be composed unchanged as the fallback); deployment infrastructure and external dependencies; BlockManager storage interface contracts; and task serialization/deserialization protocols. (Confirmed by `git diff --stat` against the baseline: only the two intended source edits, plus the additive new files, are present.)

---

## 3. Pre-Flight Gate

> The pre-flight gate runs **first**, before any domain phase. It is **scoped to the 25 delivered CP1 files**. Full-feature gates that depend on not-yet-delivered code (the complete test catalog, the > 85% coverage bar, failure-injection/stress, and the benchmark deltas) **cannot be evaluated at CP1** and are recorded as **DEFERRED**, not PASS. **Result: GREEN over the delivered surface; full-feature gates deferred.**

### 3.1 Pre-flight checklist

- [x] **Delivered CP1 deliverables present at their specified paths** — the 25-file CP1 inventory (§5) is present at the paths the AAP specifies. The 23 deferred files (§2.2) are **not** present and are not claimed to be.
- [x] **Zero-error / zero-warning build of delivered files** — `./build/mvn -pl core -am -DskipTests compile` and `./build/mvn -pl core -o test-compile` complete with zero errors and zero warnings (Scala compiler runs with `-Wconf:any:e`, i.e. warnings-as-errors). The delivered foundation/primitive classes and the two integration edits compile clean.
- [x] **Delivered test passes** — `StreamingShuffleHandleSuite` passes. (The remaining 13 suites and 2 benchmark artifacts are **deferred**; see §3.4.)
- [x] **Static analysis clean on delivered files** — Scalastyle (`scalastyle-config.xml`), Checkstyle (`dev/checkstyle.xml`), and MiMa (additive-only) report zero violations for the delivered files. (Scalafmt is enforced by `dev/lint-scala` only for the Spark Connect modules, not `core`; the delivered `core` files are nonetheless ASCII-clean and within the project line-length convention.)
- [x] **No production-path placeholder stubs among delivered files** — none. The previously-claimed v1 transport stub belongs to `network/StreamingShuffleTransport.scala`, which is **not yet delivered** (deferred to CP2); see §3.4.

### 3.2 Pre-flight results (delivered surface)

| # | Gate | Command / evidence | Result |
| --- | ------ | -------------------- | -------- |
| 1 | Delivered deliverables present | Inventory cross-check against AAP §0.2.3 / §0.5.1 (see §5 coverage matrix) — 25/25 present | **PASS** |
| 2 | Zero-error/zero-warning build | `./build/mvn -pl core -am -DskipTests compile` then `./build/mvn -pl core -o test-compile` | **PASS** |
| 3 | Delivered test passes | `-DwildcardSuites=org.apache.spark.shuffle.streaming` (the only present suite is `StreamingShuffleHandleSuite`) | **PASS** |
| 4 | Scalastyle | `scalastyle-config.xml` | **PASS** |
| 5 | Checkstyle | `dev/checkstyle.xml` | **PASS** |
| 6 | MiMa (additive-only) | binary-compatibility check — additions only, no signature changes | **PASS** |
| 7 | No undocumented stubs (delivered) | source scan of the delivered `streaming` package | **PASS** (no stubs among delivered files) |

### 3.3 Full-feature gates — DEFERRED at CP1

| # | Gate | Why deferred | Status |
| --- | ------ | -------------- | -------- |
| 8 | Full test catalog (14 suites) | 13 suites not yet delivered (CP2/CP3) | **DEFERRED** |
| 9 | Unit line coverage > 85% | depends on not-yet-delivered manager/writer/reader/spill/backpressure code and their suites | **DEFERRED** |
| 10 | Zero data loss (10-scenario failure injection) | `StreamingShuffleFailureInjectionSuite` not yet delivered | **DEFERRED** |
| 11 | Zero retained heap (5-min stress) | `StreamingShuffleStressSuite` not yet delivered | **DEFERRED** |
| 12 | Performance deltas (latency/CPU) | `StreamingShufflePerformanceBenchmark` + `*-results.txt` not yet delivered | **DEFERRED** |

### 3.4 v1 transport behavior — a DEFERRED CP2 item (not a present whitelisted stub)

`core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` is **not delivered at CP1** — it is part of the CP2 backend set (§2.2). Its planned **v1 logging-only** behavior (returning a completed `Future` from `sendBlock` and `Iterator.empty` from `openConsumerStream`, because the real data plane is the existing `BlockTransferService` / `fetchBlockSync` path) is recorded in `blitzy-docs/streaming-shuffle/decision-log.md` as an intended, justified deviation. **When the file lands in CP2, the pre-flight gate will whitelist that documented v1 behavior so it is not misclassified as `BLOCKED`.** At CP1 there is no such file on the production path, so there is nothing to whitelist now and no production-path placeholder among the delivered files.

---

## 4. Sequential Domain Review Phases

Every **delivered** CP1 file is partitioned into **exactly one** of the phases below. The allowed domains are Infrastructure/DevOps, Security, Backend Architecture, QA/Test Integrity, Business/Domain, Frontend, and Other SME. **Frontend is not applicable** — this is a backend-only Spark Core change with no Web UI/static-asset surface. Observability is reviewed under **Other SME (Observability/SRE)** to remain within the allowed-domain list. Phases run in sequence; each carries an explicit `APPROVED` or `BLOCKED` verdict for its **delivered** files. The exact-once coverage of all 25 delivered files is proven by the matrix in §5.

| Phase | Domain | Delivered files owned | Deferred to CP2/CP3 | Verdict (delivered scope) |
| ------- | -------- | ----------------------: | --------------------: | --------- |
| 1 | Infrastructure/DevOps | 0 (negative verification) | 0 | **APPROVED** |
| 2 | Security | 0 | 1 (BackpressureRpcEndpoint) | **APPROVED** (negative verification; security review deferred with the endpoint) |
| 3 | Backend Architecture | 9 | 7 | **APPROVED** |
| 4 | Other SME — Observability/SRE | 5 | 0 | **APPROVED** |
| 5 | QA/Test Integrity | 1 | 15 | **APPROVED** (delivered suite only; full QA bar **DEFERRED**) |
| 6 | Business/Domain — Documentation | 10 | 0 | **APPROVED** |
| — | **Total** | **25** | **23** | — |

---

### Review Phase 1 — Infrastructure/DevOps

**Domain intent.** Confirm the change introduces **no** build, CI, or dependency modifications, upholding the "no dependency changes" guarantee (AAP §0.3.1) and the least-modification discipline.

**Files owned:** none by design. This is a **negative-verification** phase: it asserts that specific infrastructure files were *not* touched. Every changed file is owned by another phase; this phase guards the boundary.

**Findings.**

- [x] No changes to dependency manifests — the root `pom.xml` and `core/pom.xml` are unchanged. Every library the feature relies on (Guava `RateLimiter`, Netty via `BlockTransferService`, Dropwizard/Codahale metrics, JDK `CRC32C`) is already on the Spark Core classpath.
- [x] No changes to CI workflows under `.github/`.
- [x] No changes to build/lint config under `dev/` (`dev/checkstyle.xml`, `dev/.scalafmt.conf`) or `scalastyle-config.xml`.
- [x] No changes to docs site config — the new Jekyll docs are additive Markdown files only.
- [x] Build/runtime baseline unchanged: Scala 2.13.18, JDK 17 (build), Maven 3.9.12 via the `./build/mvn` wrapper, artifact `spark-core_2.13` under `spark-parent_2.13:4.2.0-SNAPSHOT`.

**Verdict: `APPROVED`.** No infrastructure, CI, or dependency drift. The delivered change is purely additive at the source level with two surgical edits owned by Phase 3.

---

### Review Phase 2 — Security

**Domain intent.** Review any new network-facing surface and confirm the streaming path reuses Spark's existing shuffle security model and introduces no new attack surface.

**Delivered files owned (0).** The only security-relevant file — `BackpressureRpcEndpoint.scala`, the executor-scoped backpressure RPC endpoint — is **not yet delivered** (deferred to CP2, §2.2). At CP1 there is therefore **no new network endpoint, listening socket, or RPC surface** in the delivered code. This phase is a **negative-verification** phase for CP1.

**Findings (CP1).**

- [x] **No new endpoint delivered.** No `setupEndpoint(...)` call and no new RPC endpoint exist in the delivered files; the backpressure RPC surface arrives in CP2 and will be reviewed then (executor-only registration, driver rejection, SASL/TLS reuse, minimal typed message surface).
- [x] **No secrets in delivered source or templates.** The delivered config accessor (`StreamingShuffleConfig`) and the `metrics.properties.template` contain no embedded credentials, tokens, or keys.
- [x] **No new external dependencies** that would expand the attack surface (Phase 1).

**Verdict: `APPROVED`** for the delivered surface (negative verification). **Security review of the backpressure RPC endpoint is DEFERRED to CP2** when `BackpressureRpcEndpoint.scala` lands.

---

### Review Phase 3 — Backend Architecture

**Domain intent.** Review the delivered shuffle-SPI foundation and primitives — the two surgical integration edits, the typed configuration accessor, the per-partition buffer primitive, the fallback-policy primitive, the package object, and the wire-envelope and rate-limiter network primitives — verifying correct contract shape, isolation, protocol invariants, and the dual-channel byte-layout invariant.

**Delivered files owned (9):**

| # | File | Role |
| --- | ------ | ------ |
| 1 | `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` *(MODIFY)* | Adds `"streaming" -> "org.apache.spark.shuffle.streaming.StreamingShuffleManager"` to `shortShuffleMgrNames` (factory alias) |
| 2 | `core/src/main/scala/org/apache/spark/internal/config/package.scala` *(MODIFY)* | Registers five `spark.shuffle.streaming.*` `ConfigEntry` values after `SHUFFLE_MANAGER` |
| 3 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` | Typed configuration accessor with validation and derived values (block size, envelope header bytes, effective bandwidth) |
| 4 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | `BaseShuffleHandle` subtype carrying `bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps` |
| 5 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingBuffer.scala` | Per-partition in-memory buffer with CRC32C, atomic counters, LRU access tracking; emits canonical envelope frames |
| 6 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` | Evaluates the four revert conditions to gate fallback |
| 7 | `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | Package-level Scaladoc for the streaming subsystem |
| 8 | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala` | Wraps Guava `RateLimiter` (1 permit = 1 byte); per-shuffle cap; unlimited when bandwidth ≤ 0 |
| 9 | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala` | 32-byte big-endian header (shuffleId, mapId, reduceId, sequenceNumber, CRC32C, payloadLength) + ≤ 2 MB payload |

**Findings.**

- [x] **Factory edit is surgical and annotated.** `ShuffleManager.shortShuffleMgrNames` gains exactly one entry pointing at the (CP2) `StreamingShuffleManager` FQCN; the existing `create`/`getShuffleManagerClassName` logic and the `config.SHUFFLE_MANAGER` lookup are reused unchanged. The edit carries a coexistence comment as the rules direct. `SparkEnv.create()` reflectively instantiates the configured manager with no scheduler/environment change.
- [x] **Config registry edit is additive.** Five new `ConfigEntry` values (`spark.shuffle.streaming.enabled`, `…bufferSizePercent`, `…spillThreshold`, `…maxBandwidthMBps`, `…debug`) are registered immediately after `SHUFFLE_MANAGER` via the existing `ConfigBuilder` DSL, each with `.version("4.2.0")`; the existing `SHUFFLE_MANAGER` entry is untouched. Defaults: `enabled=false`, `bufferSizePercent=20` (range 1–50), `spillThreshold=80` (range 50–95), `maxBandwidthMBps=unlimited`, `debug=false`.
- [x] **Typed config accessor correct.** `StreamingShuffleConfig` exposes validated, derived values (2 MB block size, 32-byte envelope header, the 80%-factored effective bandwidth) consumed by the primitives; ranges are enforced.
- [x] **Dual-channel byte-layout invariant verified (delivered primitives).** `StreamingBuffer.toChunkedByteBuffer` and `toByteArray` emit **canonical `StreamingBlockEnvelope` frames** (32-byte big-endian header + payload) that are **byte-identical** to what the envelope produces on the wire, satisfying the invariant declared in `StreamingBlockEnvelope`'s contract ("spilled and streamed bytes are interchangeable"). The per-block payload accessors (`readBlock`/`checksumOf`/`blockWithChecksum`) expose the raw payload and CRC; the frame accessors (`envelopeOf`/`toChunkedByteBuffer`/`toByteArray`) emit the enveloped form. CRC equivalence holds because `CRC32C.getValue() ∈ [0, 2^32)`, so the buffer's stored checksum narrowed via `& 0xFFFFFFFFL).toInt` equals `StreamingBlockEnvelope.computeCrc32c(samePayload)`; `verifyChecksum` returns true for every framed block. Verified by a dual-channel round-trip check (spill bytes parse via `StreamingBlockEnvelope.parse`, checksums validate, payloads round-trip byte-for-byte, single-block spill equals `envelopeOf(0).toByteArray`, and multi-block concatenation preserves order).
- [x] **Wire envelope correct.** `StreamingBlockEnvelope` defines the 32-byte big-endian header and a payload capped at 2 MB, with canonical CRC32C verification; `HEADER_BYTES`/`MAX_PAYLOAD_BYTES` are sourced from `StreamingShuffleConfig`. It is a plain `private[spark] class` (not a `case class`) to avoid `Array` structural-equality pitfalls.
- [x] **Rate limiter correct.** `TokenBucketRateLimiter` wraps Guava `RateLimiter` at 1 permit = 1 byte, applies a per-concurrent-shuffle cap, and is unlimited when `maxBandwidthMBps ≤ 0`.
- [x] **Fallback-policy primitive present.** `StreamingShuffleFallbackPolicy` evaluates the four revert conditions (slow consumer > 60 s, memory pressure/OOM risk, network saturation, version mismatch) with lock-free counters. (Its *consumption* by `StreamingShuffleManager` is deferred to CP2.)
- [x] **Isolation upheld.** All delivered streaming logic lives in the new package; there is zero cross-contamination of existing classes beyond the two surgical, comment-annotated edits.

**Deferred (not reviewed here):** `StreamingShuffleManager`, `StreamingShuffleWriter`, `StreamingShuffleReader`, `StreamingShuffleBlockResolver`, `MemorySpillManager`, `BackpressureProtocol`, and `network/StreamingShuffleTransport` — the classes that wire the primitives into a running data path — land in CP2/CP3.

**Verdict: `APPROVED`** for the delivered foundation and primitives. The two edits are minimal/additive/annotated; the configuration, buffer, envelope, rate-limiter, and fallback-policy primitives implement their contracts correctly; the dual-channel byte-layout invariant holds. The runtime SPI classes that compose these primitives are **DEFERRED to CP2/CP3**.

---

### Review Phase 4 — Other SME (Observability / SRE)

**Domain intent.** Review the delivered telemetry surface — metrics holder, metrics source, configuration template, observability documentation, and the dashboard template — confirming the four `shuffle.streaming.*` metrics are defined with correct types and surfaced through existing endpoints.

**Delivered files owned (5):**

| # | File | Role |
| --- | ------ | ------ |
| 1 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | The four metrics: `bufferUtilizationPercent` (gauge), `spillCount` / `backpressureEvents` / `partialReadInvalidations` (counters) |
| 2 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | `org.apache.spark.metrics.source.Source` exposing the metrics via JMX and configured sinks |
| 3 | `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | Metrics configuration template |
| 4 | `blitzy-docs/streaming-shuffle/observability.md` | Observability guide (reused-vs-added inventory, MDC keys, verification) |
| 5 | `blitzy-docs/streaming-shuffle/dashboard.json` | Grafana dashboard template (2×2 grid, four panels) |

**Findings.**

- [x] **Four metrics, correct types.** `bufferUtilizationPercent` is a gauge; `spillCount`, `backpressureEvents`, and `partialReadInvalidations` are counters. `StreamingShuffleSource` implements `org.apache.spark.metrics.source.Source` so that, once registered by the (CP2) manager with `metricsSystem.registerSource(...)`, the metrics surface via JMX and the Prometheus endpoint with no change to the metrics framework.
- [x] **Source integration shape correct.** The source exposes the metric registry; registration is designed to be gated on `SparkEnv.get != null` (local-mode safe) by the consuming manager in CP2.
- [x] **Dashboard template valid.** `dashboard.json` is a self-contained Grafana template (2×2 grid of four panels) provisioned externally; it adds no Web UI surface to Spark itself. (JSON validity confirmed.)
- [x] **Reused-vs-added documented.** `observability.md` records what is reused (SLF4J/Log4j2, `MetricsSystem`, executor health surface, Prometheus endpoint) versus what is added (the four metrics, MDC keys `shuffle_id`/`map_id`/`reduce_partition_range`/`attempt_id`, the dashboard).

**Checkpoint limitation (documented, not a defect).** Live metric *emission* and the structured-logging MDC path are produced by the writer/reader/backpressure/spill classes, which are deferred to CP2. `observability.md`'s "verify metric emission in the local development environment" step is therefore a forward-looking instruction that becomes executable once those producers land. This is one of the two known minor cross-reference caveats for CP1.

**Verdict: `APPROVED`.** The delivered telemetry primitives define exactly the four specified metrics with correct types via a standard `Source`, and ship a valid dashboard template and an accurate reused-vs-added ledger. Live-emission verification is **DEFERRED to CP2** with its producers.

---

### Review Phase 5 — QA/Test Integrity

**Domain intent.** Verify the delivered test artifacts, and record the status of the full feature merge bar (> 85% coverage, zero data loss under failure injection, zero retained heap under stress, reproducible performance deltas).

**Delivered files owned (1):**

| # | File | Role |
| --- | ------ | ------ |
| 1 | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala` | Handle field propagation (`bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps`) |

**Findings.**

- [x] **Delivered suite passes.** `StreamingShuffleHandleSuite` compiles and passes under `-DwildcardSuites=org.apache.spark.shuffle.streaming`.
- [x] **Naming/location conventions** match Spark's test layout (mirrored package, `*Suite` naming).

**Checkpoint limitation — full QA merge bar DEFERRED (NOT met at CP1).** The following 13 suites and 2 benchmark artifacts are **not yet delivered** (§2.2) and the gates that depend on them are **not verifiable** at CP1:

| Deferred gate | Owning artifact (CP2/CP3) | Status |
| --------------- | --------------------------- | -------- |
| Unit line coverage > 85% | full streaming test catalog | **DEFERRED** |
| All suites green (14/14) | 13 additional suites | **DEFERRED** |
| Zero data loss (10 scenarios) | `StreamingShuffleFailureInjectionSuite` | **DEFERRED** |
| Zero retained heap (5-min stress) | `StreamingShuffleStressSuite` | **DEFERRED** |
| Reproducible perf deltas | `StreamingShufflePerformanceBenchmark` + `core/benchmarks/StreamingShuffle*-results.txt` | **DEFERRED** |

**Verdict: `APPROVED`** for the single delivered suite. The **full QA/Test merge bar is explicitly DEFERRED** — it is **not** claimed to be met at CP1 and must be satisfied when CP2/CP3 land the remaining suites and benchmark artifacts.

---

### Review Phase 6 — Business/Domain (Documentation)

**Domain intent.** Review the delivered documentation set — TechDocs, Jekyll guides, the decision log, the executive presentation — plus this review artifact, confirming completeness, Mermaid usage, and accuracy against the **delivered** state.

**Delivered files owned (10):**

| # | File | Role |
| --- | ------ | ------ |
| 1 | `blitzy-docs/streaming-shuffle/index.md` | TechDocs landing page |
| 2 | `blitzy-docs/streaming-shuffle/configuration.md` | The five `spark.shuffle.streaming.*` keys + activation |
| 3 | `blitzy-docs/streaming-shuffle/architecture.md` | Mermaid architecture diagrams (before/after, component, data-flow) |
| 4 | `blitzy-docs/streaming-shuffle/decision-log.md` | Explainability decision log + traceability matrix; records the v1 transport deviation |
| 5 | `blitzy-docs/streaming-shuffle/executive-summary.html` | Self-contained reveal.js executive presentation |
| 6 | `docs/streaming-shuffle-architecture.md` | Jekyll architecture guide (Mermaid diagrams) |
| 7 | `docs/streaming-shuffle-guide.md` | Jekyll user guide |
| 8 | `docs/streaming-shuffle-troubleshooting.md` | Jekyll troubleshooting guide |
| 9 | `docs/streaming-shuffle-tuning.md` | Jekyll tuning guide |
| 10 | `CODE_REVIEW.md` | **This** Segmented PR Review artifact (self-referential) |

**Findings.**

- [x] **Visual architecture uses Mermaid.** Both `blitzy-docs/streaming-shuffle/architecture.md` and the Jekyll `docs/streaming-shuffle-architecture.md` carry the before/after factory diagram, the component-interaction diagram, and the producer-to-consumer data-flow diagram as fenced `mermaid` blocks — each titled, with a legend, and referenced by name in the prose. (All three diagrams render error-free.)
- [x] **Decision log present** as a Markdown table capturing decision/alternatives/rationale/risk per non-trivial choice, including a requirement→source→test traceability matrix; the intended v1 transport-stub behavior is recorded as an explicit, justified deviation.
- [x] **Executive presentation** is a single self-contained `executive-summary.html` (reveal.js) for non-technical leadership, covering scope, business value, the architectural change, risks/mitigations, and onboarding; it targets 16 slides, pins CDN versions (reveal.js 5.1.0, Mermaid 11.4.0, Lucide 0.460.0), embeds two Mermaid diagrams, uses Lucide SVG icons (no emoji), and gives every slide a non-text visual. (Rendered and verified in a browser.)
- [x] **Jekyll guides** (`docs/streaming-shuffle-*.md`) are additive Markdown and consistent with the configuration keys, invariants, and fallback semantics described in the delivered code and the AAP.
- [x] **This review artifact** (`CODE_REVIEW.md`) is at the repository root, partitions every **delivered** file exactly once, inventories the deferred files separately, and records the commit cadence.

**Checkpoint limitation (documented, not a defect).** The decision-log traceability matrix maps some requirements to **source/test files that are deferred** (CP2/CP3); those rows are forward-looking until the files land. This is the second of the two known minor cross-reference caveats for CP1.

**Verdict: `APPROVED`.** The delivered documentation set is complete for CP1, uses Mermaid for all visual architecture, ships the decision log and executive presentation, and is accurate to the delivered implementation (with the two documented forward-looking caveats above).

---

## 5. File-to-Phase Coverage Matrix (delivered files)

This matrix proves the partition over **delivered CP1 files** is exhaustive and disjoint: **every** delivered file maps to **exactly one** domain phase (no omissions, no double-counts). Total delivered: **25** = 0 (Infra) + 0 (Security) + 9 (Backend) + 5 (Observability) + 1 (QA) + 10 (Docs). The 23 deferred files are inventoried in §5.11 and are **not** part of this exact-once partition.

### 5.1 Modified existing source (2)

| File | Mode | Phase |
| ------ | ------ | ------- |
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | MODIFY | Backend Architecture |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | MODIFY | Backend Architecture |

### 5.2 New production Scala — `…/shuffle/streaming/` delivered (5)

| File | Phase |
| ------ | ------- |
| `StreamingShuffleConfig.scala` | Backend Architecture |
| `StreamingShuffleHandle.scala` | Backend Architecture |
| `StreamingBuffer.scala` | Backend Architecture |
| `StreamingShuffleFallbackPolicy.scala` | Backend Architecture |
| `package.scala` | Backend Architecture |

### 5.3 New production Scala — `…/shuffle/streaming/` observability (2)

| File | Phase |
| ------ | ------- |
| `StreamingShuffleMetrics.scala` | Other SME — Observability |
| `StreamingShuffleSource.scala` | Other SME — Observability |

### 5.4 New production Scala — `…/shuffle/streaming/network/` delivered (2)

| File | Phase |
| ------ | ------- |
| `TokenBucketRateLimiter.scala` | Backend Architecture |
| `StreamingBlockEnvelope.scala` | Backend Architecture |

### 5.5 New resource (1)

| File | Phase |
| ------ | ------- |
| `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | Other SME — Observability |

### 5.6 New tests delivered (1)

| File | Phase |
| ------ | ------- |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala` | QA/Test Integrity |

### 5.7 New documentation — TechDocs `blitzy-docs/streaming-shuffle/` (7)

| File | Phase |
| ------ | ------- |
| `index.md` | Business/Domain — Documentation |
| `configuration.md` | Business/Domain — Documentation |
| `architecture.md` | Business/Domain — Documentation |
| `observability.md` | Other SME — Observability |
| `decision-log.md` | Business/Domain — Documentation |
| `executive-summary.html` | Business/Domain — Documentation |
| `dashboard.json` | Other SME — Observability |

### 5.8 New documentation — Jekyll `docs/` (4)

| File | Phase |
| ------ | ------- |
| `docs/streaming-shuffle-architecture.md` | Business/Domain — Documentation |
| `docs/streaming-shuffle-guide.md` | Business/Domain — Documentation |
| `docs/streaming-shuffle-troubleshooting.md` | Business/Domain — Documentation |
| `docs/streaming-shuffle-tuning.md` | Business/Domain — Documentation |

### 5.9 Review artifact (1)

| File | Phase |
| ------ | ------- |
| `CODE_REVIEW.md` | Business/Domain — Documentation |

### 5.10 Partition tally (delivered)

| Phase | Count |
| ------- | ------: |
| Infrastructure/DevOps (negative verification, owns 0) | 0 |
| Security (negative verification, owns 0) | 0 |
| Backend Architecture | 9 |
| Other SME — Observability | 5 |
| QA/Test Integrity | 1 |
| Business/Domain — Documentation | 10 |
| **Total delivered** | **25** |

> **Coverage proof (delivered).** 0 + 0 + 9 + 5 + 1 + 10 = **25**, equal to the delivered CP1 inventory. No delivered file appears in more than one phase, and no delivered file is unassigned.

### 5.11 Deferred to CP2/CP3 — inventory only (23, not partitioned/reviewed)

| File | Planned phase (future) |
| ------ | ------- |
| `…/streaming/StreamingShuffleManager.scala` | Backend Architecture (CP2) |
| `…/streaming/StreamingShuffleWriter.scala` | Backend Architecture (CP2) |
| `…/streaming/StreamingShuffleReader.scala` | Backend Architecture (CP2) |
| `…/streaming/StreamingShuffleBlockResolver.scala` | Backend Architecture (CP2) |
| `…/streaming/MemorySpillManager.scala` | Backend Architecture (CP2) |
| `…/streaming/BackpressureProtocol.scala` | Backend Architecture (CP2) |
| `…/streaming/BackpressureRpcEndpoint.scala` | Security (CP2) |
| `…/streaming/network/StreamingShuffleTransport.scala` | Backend Architecture (CP2) |
| `…/test/…/streaming/BackpressureProtocolSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/BackpressureRpcEndpointSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/MemorySpillManagerSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleFailureInjectionSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleFallbackPolicySuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleIntegrationSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleIntegrationTest.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleManagerSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleMetricsSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShufflePerformanceBenchmark.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleReaderSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleStressSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `…/test/…/streaming/StreamingShuffleWriterSuite.scala` | QA/Test Integrity (CP2/CP3) |
| `core/benchmarks/StreamingShuffleBenchmark-results.txt` | QA/Test Integrity (CP3) |
| `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` | QA/Test Integrity (CP3) |

> 25 delivered + 23 deferred = **48** planned files.

---

## 6. Final Re-Verification & Verdict

A final reviewer re-verified the **delivered CP1 state** after the six domain phases resolved over their delivered files:

- [x] **Pre-flight GREEN over delivered files** — the 25 delivered deliverables are present; the delivered files build with zero errors and zero warnings; `StreamingShuffleHandleSuite` passes; Scalastyle/Checkstyle/MiMa are clean. Full-feature test/coverage/benchmark gates are **DEFERRED** (§3.3), not claimed PASS.
- [x] **Delivered-scope domain phases `APPROVED`** — Infrastructure/DevOps, Security (negative verification), Backend Architecture, Other SME (Observability), QA/Test Integrity (delivered suite only), and Business/Domain (Documentation) each resolved to `APPROVED` for their **delivered** files.
- [x] **Delivered coverage complete** — the §5 matrix confirms all **25 delivered** files are partitioned into exactly one phase (no omissions, no double-counts); the 23 deferred files are inventoried separately and not reviewed.
- [x] **Absolute-preservation honored** — RDD/DataFrame/Dataset APIs, the DAG scheduler, executor lifecycle, lineage/fault-recovery, `SortShuffleManager`, deployment infra, BlockManager storage contracts, and task ser/de are all untouched.
- [x] **Isolation & coexistence** — delivered streaming logic is fully isolated in the new package; only two surgical, comment-annotated edits touch existing code; both activation flags default off, so default cluster behavior is byte-for-byte unchanged.

### Overall verdict

**CHECKPOINT 1 — DELIVERED SURFACE: `APPROVED`. FULL FEATURE: INCOMPLETE — NOT merge-ready as a whole.**

The 25 delivered CP1 files (the two integration edits, the configuration and foundation/primitive classes, the observability primitives, the documentation set, and one handle test) meet the checkpoint bar: they build clean with warnings-as-errors, pass static analysis, uphold isolation and the dual-channel byte-layout invariant, and ship the rule-mandated documentation deliverables (Mermaid architecture, decision log, executive presentation, this artifact). **However, the feature is not complete:** the runtime SPI classes that engage the streaming data path (`StreamingShuffleManager`, `StreamingShuffleWriter`, `StreamingShuffleReader`, `StreamingShuffleBlockResolver`, `MemorySpillManager`, `BackpressureProtocol`, `BackpressureRpcEndpoint`, `network/StreamingShuffleTransport`), the 13 remaining test suites, and the 2 benchmark artifacts are **deferred to CP2/CP3**. The full quality bar — > 85% coverage, all suites green, zero data loss, zero retained heap, and reproducible performance deltas — is **explicitly DEFERRED** and must be satisfied before the feature as a whole can be approved for merge.

**`CODE_REVIEW.md` is present in the pull request's final commit.** This artifact is committed at the checkpoint with the delivered-scope verdicts and will be re-reviewed and re-committed as CP2/CP3 land the deferred files (see the Commit Cadence in the Status Banner).

---

## 7. Appendices

### 7.1 Protocol & operational invariants (design targets; realization spans CP1–CP3)

| Invariant | Value | Realized in delivered CP1 primitives? |
| ----------- | ------- | --- |
| Block-level checksum | CRC32C | Yes — `StreamingBlockEnvelope`, `StreamingBuffer` |
| Block size | 2 MB | Yes — `StreamingShuffleConfig`, `StreamingBlockEnvelope` (≤ 2 MB payload) |
| Wire envelope header | 32-byte big-endian (shuffleId, mapId, reduceId, sequenceNumber, CRC32C, payloadLength) | Yes — `StreamingBlockEnvelope` |
| Rate limiting | token-bucket (1 permit = 1 byte; unlimited when `maxBandwidthMBps ≤ 0`) | Yes — `TokenBucketRateLimiter` |
| Fallback conditions | slow consumer > 60 s, memory pressure, network saturation, version mismatch | Policy primitive yes (`StreamingShuffleFallbackPolicy`); consumption deferred (CP2) |
| Connection timeout | 5 s | Deferred — reader (CP2) |
| Heartbeat interval | 10 s | Deferred — backpressure (CP2) |
| Retry/backoff | exponential, 1 s start, max 5 attempts | Deferred — transport/reader (CP2) |
| Spill/reclaim SLA | 100 ms | Deferred — `MemorySpillManager` (CP2) |
| Telemetry overhead | < 1% executor CPU | Deferred — verified with producers (CP2) |
| Log volume | < 10 MB/hour/executor | Deferred — verified with producers (CP2) |
| Reconfiguration | immutable for application lifetime (executor restart required in v1) | Config delivered; enforced by design |

### 7.2 Configuration keys (delivered)

| Key | Type | Default | Range / notes |
| ----- | ------ | --------- | --------------- |
| `spark.shuffle.manager` | String | `sort` | set to `streaming` to select the backend (factory alias) |
| `spark.shuffle.streaming.enabled` | Boolean | `false` | opt-in feature flag |
| `spark.shuffle.streaming.bufferSizePercent` | Integer | `20` | percent of executor memory, 1–50 |
| `spark.shuffle.streaming.spillThreshold` | Integer | `80` | percent buffer utilization, 50–95 |
| `spark.shuffle.streaming.maxBandwidthMBps` | Integer | unlimited | per-executor rate cap |
| `spark.shuffle.streaming.debug` | Boolean | `false` | verbose diagnostics |

> Activation requires **both** `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`.

### 7.3 Quality gates (merge bar) — CP1 status

| Gate | Target | CP1 Result |
| ------ | -------- | -------- |
| Build (delivered files) | zero errors, zero warnings | **PASS** |
| Static analysis (delivered files) | Scalastyle, Checkstyle, MiMa (additive-only) | **PASS** |
| Delivered test | `StreamingShuffleHandleSuite` green | **PASS** |
| All streaming suites | 14/14 pass | **DEFERRED** (1/14 delivered) |
| Unit line coverage | > 85% | **DEFERRED** |
| Zero data loss | 10-scenario failure injection | **DEFERRED** |
| Zero retained heap | 5-min stress, 10% failure | **DEFERRED** |
| Latency reduction | 30–50% (shuffle-heavy) | **DEFERRED** |
| CPU-bound improvement | 5–10% | **DEFERRED** |
| Memory-bound regression | zero (via fallback) | **DEFERRED** |

> The **DEFERRED** gates depend on production and test files that land in CP2/CP3 (§2.2). They are **not** claimed to be met at CP1.

### 7.4 Absolute-preservation list (verified untouched)

RDD/DataFrame/Dataset user-facing APIs · DAG scheduler and task-scheduling algorithms · executor lifecycle management · lineage tracking and the fault-recovery model · the existing `SortShuffleManager` implementation (to be composed unchanged as fallback) · deployment infrastructure and external dependencies · BlockManager storage interface contracts · task serialization/deserialization protocols.

### 7.5 Dependency posture

**No dependency changes.** The feature adds, updates, or removes **nothing** in `pom.xml` or `core/pom.xml`. Reused, pre-existing libraries/APIs: Guava `RateLimiter` (rate limiting); Netty via `BlockTransferService` / `TransportContext` (network, CP2); Dropwizard/Codahale Metrics via `MetricsSystem` + `metrics.source.Source` (telemetry); JDK 17 `java.util.zip.CRC32C` (checksums); and internal Spark Core APIs (`RpcEnv`/`ThreadSafeRpcEndpoint`, `ThreadUtils`, `ConfigBuilder`, `MemoryConsumer`/`TaskMemoryManager`). Test dependencies (ScalaTest, ScalaCheck, Mockito, JUnit Jupiter) are already present.

---

*End of review.*
