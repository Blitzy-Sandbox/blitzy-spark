# Code Review — Streaming Shuffle Subsystem (Checkpoint CP2 — FULL PRODUCTION RUNTIME)

This document is the **Segmented PR Review** ledger for the new **opt-in streaming shuffle subsystem** added to Apache Spark (`spark-parent_2.13`, `4.2.0-SNAPSHOT`). It records the multi-phase, file-partitioned review that governs the feature's merge into `master`.

This revision of the ledger reflects the **CP2 — FULL PRODUCTION RUNTIME** checkpoint. CP2 delivers the complete runtime/control-plane surface on top of the CP1 foundation: the manager and dispatch handle, the producer writer and consumer reader, the backpressure protocol and its RPC endpoint, the memory spill manager, the fallback policy, the metrics source, the v1 network-transport stub, the nine present unit/component test suites, and the architecture, observability, and decision-log TechDocs. The advanced integration / failure-injection / stress / performance-benchmark suites, the second benchmark baseline, the landing page (`index.md`), and the executive deck (`executive-summary.html`) are **deferred to CP3** and are tracked here as `DEFERRED`.

The streaming shuffle is delivered as a **new `ShuffleManager` Service Provider Interface (SPI) implementation** that *coexists with* — and never replaces — the default `SortShuffleManager`. The change is **strictly additive**: the default `spark.shuffle.manager` value remains `"sort"`, the reflective `SparkEnv` factory call site is untouched, and streaming engages only under an explicit dual-flag activation contract (`spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`). All out-of-scope surfaces — `DAGScheduler`, `TaskScheduler`, lineage / fault recovery, `ShuffleExchangeExec`, every Adaptive Query Execution (AQE) rule, the existing `SortShuffleManager`, and block-manager storage contracts — are preserved verbatim.

## Review Metadata

| Field | Value |
|-------|-------|
| Target branch | `master` |
| Base version | `4.2.0-SNAPSHOT` |
| Feature | Opt-in streaming shuffle subsystem |
| Checkpoint | CP2 — FULL PRODUCTION RUNTIME |
| Activation contract | `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true` |
| Default behavior | Unchanged — `spark.shuffle.manager` remains `"sort"` |
| New runtime dependencies | None (all primitives already declared in the build) |
| Review model | Segmented PR Review — 8 sequential domain phases |
| Review ledger | `CODE_REVIEW.md` (this file, repository root) |

### Scope Summary

The counts below distinguish files **present and reviewed at CP2** from files **deferred** to CP3. Only present files receive an `APPROVED`/`BLOCKED` verdict; deferred files are recorded as `DEFERRED` and are not approved here.

| Category | Present at CP2 | Deferred (CP3) | Detail |
|----------|---------------:|---------------:|--------|
| Existing files **MODIFIED** | 2 | 0 | SPI alias map + internal config registry (both additive) |
| Production classes **CREATED** | 16 | 0 | All runtime/control-plane classes (F-101–F-116) now present and reviewed |
| Production resources **CREATED** | 2 | 0 | `package.scala` Scaladoc + `metrics.properties.template` (F-118) |
| Test suites **CREATED** | 9 | 5 | 9 unit/component suites present; integration, integration-test, failure-injection, stress, and performance-benchmark suites (F-121) deferred to CP3 |
| Benchmark result files **CREATED** | 1 | 1 | `StreamingShuffleBenchmark-results.txt` present; `StreamingShufflePerformanceBenchmark-results.txt` deferred |
| Documentation artifacts **CREATED** | 9 | 2 | Present: 5 TechDocs (`configuration`, `dashboard.json`, `architecture`, `observability`, `decision-log`) + 4 Jekyll pages. Deferred: `index.md`, `executive-summary.html` |
| Governance artifact **CREATED** | 1 | 0 | this `CODE_REVIEW.md` review ledger |

At CP2, **40 changed files** are present and reviewed; the remaining **8 feature files** are `DEFERRED` to CP3. The union across all checkpoints is the full 48-file feature change set.

### Review Lifecycle

The Segmented PR Review follows a strict commit cadence so the review ledger is always an accurate, version-controlled reflection of review state:

1. **Pre-flight gate** — Before any domain phase begins, the pre-flight gate (below) is evaluated for the checkpoint's scope. `CODE_REVIEW.md` exists at the repository root and is committed **before the first review phase**.
2. **Commit before Phase 1** — The ledger, with the pre-flight gate recorded, is committed prior to opening the first domain phase.
3. **Re-commit on every phase transition** — As each domain phase resolves, the ledger is updated with that phase's verdict and **re-committed** before the next phase opens.
4. **Checkpoint verdict commit** — Once the checkpoint's in-scope files resolve, the **Final Verdict** for the checkpoint is recorded and the ledger is committed again.

**Pre-flight gate criteria (CP2 scope, stated verbatim):**

- All CP2 FULL-PRODUCTION-RUNTIME deliverables exist at their specified paths. CP3 deliverables are tracked as `DEFERRED` and are not asserted to exist.
- No production-path method in the CP2 source set returns a placeholder stub. The v1 network transport (`StreamingShuffleTransport`, F-115) is the single documented, intentional logging-only stub exception **by design**, recorded in the decision log (ADR-15, deviation D-1).
- `CODE_REVIEW.md` is present at the repository root.

## Pre-Flight Gate

**Status: `PASSED` (CP2 scope)** — every criterion applicable to the CP2 full-production-runtime set is satisfied.

| Check | Status | Notes |
|-------|--------|-------|
| All CP2 deliverable paths exist | ✅ PASSED | The 40 CP2 files in the [Changed-File Inventory](#changed-file-inventory) are present; CP3 files are listed as `DEFERRED`, not asserted present. |
| No production placeholder stubs in the CP2 source set | ✅ PASSED | The runtime classes implement complete logic; the writer publishes fetchable output via `IndexShuffleBlockResolver`, the reader fetches with a bounded timeout, the spill manager reclaims real heap, and fallback delegates to sort. The v1 logging-only transport stub (F-115) is the single documented exception (decision log ADR-15 / D-1). |
| `CODE_REVIEW.md` present at repository root | ✅ PASSED | This file. |
| Build compiles — zero errors, zero warnings (CP2 sources) | ✅ PASSED | `./build/mvn -pl core -DskipTests compile` with `-Wconf:any:e` (warnings-as-errors), `-Wunused:imports`, `-release 17`. |
| Tests compile and the present suites pass | ✅ PASSED | `./build/mvn -pl core test-compile` succeeds; the nine present streaming suites pass (`Tests: succeeded …, failed 0`). |
| Scalastyle clean (`maxColumn=98`) | ✅ PASSED | `git diff --check` clean; no streaming production line exceeds 98 characters (import lines exempt per `ignoreImports`). |
| Apache RAT license headers (CP2 files) | ✅ PASSED | Every new CP2 **source** file carries the ASF header; the benchmark result file is excluded through `dev/.rat-excludes`. Prose Markdown intentionally omits the header per repository convention. |
| MiMa binary compatibility | ➖ ADDITIVE BY CONSTRUCTION (CI gate) | New public symbols are additive only — new `ConfigEntry` vals and new `private[spark]` classes; no existing signature is changed. |

## Review Phases

The feature's changed files are partitioned into **eight sequential domain phases**. Every changed file appears in **exactly one** phase — including this ledger, which is reviewed in Phase 8. At CP2, each phase reviews the files in its domain that are **present**, resolving them to `APPROVED`; files in the domain that remain uncreated are listed as `DEFERRED` and carry no approval.

### Phase 1 — SPI & Configuration Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | MODIFIED | F-117 (SPI alias) | APPROVED |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | MODIFIED | F-117 (config keys) | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` | CREATE | F-114 | APPROVED |

**Review criteria** — Edits must be additive-only; the default `spark.shuffle.manager` must remain `"sort"`; range checks must hold; the config accessor must encode the dual-flag activation contract.

**Findings** — The `shortShuffleMgrNames` map gains exactly one `"streaming"` entry and the `SHUFFLE_MANAGER` default is unchanged (CP1 regression check: no CP2 diff). `StreamingShuffleConfig` was extended at CP2 with `managerSelected` (true only when `spark.shuffle.manager` resolves to the `"streaming"` alias) and the composite `active` accessor, so the manager enforces both halves of the dual-flag contract (resolving finding M9). Configuration remains immutable for the application lifetime. Rationale moved to the decision log (ADR-02).

**CP2 Verdict: `APPROVED`** (3 of 3 files present and reviewed).

### Phase 2 — Manager & Dispatch Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | CREATE | F-101 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | CREATE | F-102 | APPROVED |

**Review criteria** — The manager must compose the inner `SortShuffleManager`, dispatch by handle type, enforce dual-flag activation, act on the fallback policy, wire the spill manager, and stop deterministically; the handle is a `BaseShuffleHandle` subtype carrying tuning vals.

**Findings (CP2 fixes)** — `streamingActive` now enforces both halves of the dual-flag contract via `StreamingShuffleConfig.active`; selection by fully-qualified class name leaves the path disengaged (resolves M9). `registerShuffle` now evaluates `registrationFallbackReason()` against `StreamingShuffleFallbackPolicy` and, on any triggered reason, registers the whole shuffle on the inner `SortShuffleManager` so `getWriter`/`getReader` route it consistently to sort (resolves M10); the previous "memory pressure logs but still builds a streaming writer" gap is removed. `getWriter` now injects the shared `IndexShuffleBlockResolver` and the `MemorySpillManager` into the writer so runtime buffers are registered and reclaimed (resolves M11). `StreamingShuffleHandle` is unchanged (CP1, pure dispatch discriminator). Rationale moved to the decision log (ADR-01, ADR-02, ADR-14, ADR-16).

**CP2 Verdict: `APPROVED`** (2 of 2 files present and reviewed).

### Phase 3 — Data Path Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | CREATE | F-103 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | CREATE | F-104 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | CREATE | F-105 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingBuffer.scala` | CREATE | F-106 | APPROVED |

**Review criteria** — Writer output must be fetchable before `MapStatus` is returned; spilled bytes must not be dropped; the reader must enforce the 5 s producer timeout, validate checksums, and invalidate partial reads; the block resolver and buffer must preserve their CP1 contracts.

**Findings (CP2 fixes)** — `StreamingShuffleWriter` now frames each partition into ≤ 2 MiB `StreamingBlockEnvelope` records, writes them to one temp data file, and commits the file plus a per-partition index atomically through the shared `IndexShuffleBlockResolver`, so a reducer can fetch the output through the standard `MapOutputTracker` + `BlockTransferService` path (resolves C1/data availability). Spilled segments are folded back (oldest-first, ahead of resident bytes) into the published output rather than discarded, and the output stream is wrapped with `serializerManager.wrapStream` so the reader can decode it symmetrically (resolves C2/data loss). `StreamingShuffleReader` replaces the unbounded `fetchBlockSync` with a bounded async fetch awaited against the 5 s deadline, invalidating immediately on expiry (resolves C3); it decodes envelopes frame-by-frame behind an upfront budget guard so a corrupt block cannot force an unbounded allocation (resolves M7), removes the trailing-blank-line style issue (resolves m1), and routes consumer acknowledgments to `MemorySpillManager.reclaim` (closes M2 on the reader side). `StreamingBuffer` gained race-safe `spillUnderLock`/`finalizeForCommit` and a `reset` that releases heap. `StreamingShuffleBlockResolver` is unchanged (CP1). Rationale moved to the decision log (ADR-03–ADR-10).

**CP2 Verdict: `APPROVED`** (4 of 4 files present and reviewed).

### Phase 4 — Flow Control & Memory Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | CREATE | F-107 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpoint.scala` | CREATE | F-108 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | CREATE | F-109 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala` | CREATE | F-110 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` | CREATE | F-111 | APPROVED |

**Review criteria** — Acknowledgment state must be scoped per stream; messages must be validated and sanitized; a 10 s consumer-liveness detector must exist; the spill manager must release heap after a successful spill, be runtime-wired, and reclaim on ack; the fallback policy predicates must be exact.

**Findings (CP2 fixes)** — `BackpressureProtocol` replaces the single global ack watermark with per-stream watermarks keyed by `StreamKey(shuffleId, partitionId, attemptId, executorId)`, so a stale/out-of-scope ack cannot corrupt another stream (resolves M3), and adds a 10 s consumer-liveness / missing-ack detector distinct from the 5 s heartbeat, wired into fallback evaluation (resolves M4). `BackpressureRpcEndpoint` now validates message identity (non-negative ids/sequence/bytes, non-empty executor id, in-scope identity), bounds and sanitizes the free-text `reason`, and routes acks to the per-stream merge (resolves M6). `MemorySpillManager` resets each buffer after a successful, reader-visible spill and resets all still-registered buffers on `stop()` before clearing the registry (resolves M1); writer buffers are now registered and reclaimed from the runtime path (resolves M2/M11). `TokenBucketRateLimiter` is unchanged (CP1). `StreamingShuffleFallbackPolicy` predicates are exact and are now acted upon by the manager (Phase 2). Rationale moved to the decision log (ADR-06, ADR-07, ADR-11–ADR-14, ADR-17).

**CP2 Verdict: `APPROVED`** (5 of 5 files present and reviewed).

### Phase 5 — Observability Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | CREATE | F-112 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | CREATE | F-113 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | CREATE | F-118 | APPROVED |
| `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | CREATE | F-118 | APPROVED |

**Review criteria** — Exactly four metric state fields under `shuffle.streaming.`; a `Source` named `streamingShuffle` registering them with the existing `MetricsSystem`; runtime logs must populate the documented MDC correlation fields where applicable.

**Findings** — `StreamingShuffleMetrics` defines the four required fields (`bufferUtilizationPercent`, `spillCount`, `backpressureEvents`, `partialReadInvalidations`). `StreamingShuffleSource` (now present) implements `Source` with `sourceName = "streamingShuffle"` and exposes those four gauges (review PASS). For R1, the six runtime components were converted from plain string interpolation to structured `log"…"` logging that attaches correlation values to the MDC under the typed `LogKeys` (`SHUFFLE_ID`, `MAP_ID`, `REDUCE_ID`, `TASK_ATTEMPT_ID`, `RANGE`, etc.) where the identifier is in scope. `package.scala` and `metrics.properties.template` are unchanged (CP1).

**CP2 Verdict: `APPROVED`** (4 of 4 files present and reviewed).

### Phase 6 — Wire Transport Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` | CREATE | F-115 (v1 stub) | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala` | CREATE | F-116 | APPROVED |

**Review criteria** — The transport must be the single documented v1 logging-only stub, reusing the existing `BlockTransferService` and adding no new `TransportContext`; the envelope must enforce the 32-byte header + ≤ 2 MiB CRC32C payload invariant on every path.

**Findings** — `StreamingShuffleTransport` is the **single, intentional logging-only stub** in the subsystem: `send` performs structured logging and returns `Unit`, `fetch` delegates to the executor's existing `BlockTransferService`, and no new transport context, port, or Netty bootstrap is introduced (review PASS; recorded in decision log ADR-15 / deviation D-1). Its Scaladoc now points to the decision log for the deferral rationale. `StreamingBlockEnvelope` enforces the documented invariant on every construction path. Because the data plane is a stub, the writer/reader use the published-block fetch path (Phase 3), so the subsystem is correct end-to-end today.

**CP2 Verdict: `APPROVED`** (2 of 2 files present and reviewed).

### Phase 7 — Test & Benchmark Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `StreamingShuffleManagerSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleHandleSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleWriterSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleReaderSuite.scala` | CREATE | F-121 | APPROVED |
| `BackpressureProtocolSuite.scala` | CREATE | F-121 | APPROVED |
| `BackpressureRpcEndpointSuite.scala` | CREATE | F-121 | APPROVED |
| `MemorySpillManagerSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleFallbackPolicySuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleMetricsSuite.scala` | CREATE | F-121 | APPROVED |
| `core/benchmarks/StreamingShuffleBenchmark-results.txt` | CREATE | F-121 | APPROVED |
| Integration / IntegrationTest / FailureInjection / Stress / PerformanceBenchmark suites | CREATE | F-121 | DEFERRED (CP3) |
| `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` | CREATE | F-121 | DEFERRED (CP3) |

**Review criteria** — Suites must compile and provide meaningful coverage of the runtime components, including the highest-risk paths the CP2 fixes addressed; the benchmark result file follows the Spark console format and is RAT-excluded.

**Findings (CP2 fixes)** — The nine present suites compile and pass. Coverage was strengthened for the highest-risk paths surfaced by the review: `StreamingShuffleWriterSuite` now asserts a reducer can fetch the writer's output through the real reader path and that spilled bytes are included (and fixes the L22 over-length import, m2); `StreamingShuffleReaderSuite` (new) covers the bounded 5 s timeout, frame-by-frame bounded decode (over-budget and out-of-range), checksum-mismatch invalidation, a happy-path round-trip, and ack→reclaim; `MemorySpillManagerSuite` adds reset-after-spill, reclaim, and stop-resets regressions; `StreamingShuffleManagerSuite` (new) covers dual-flag activation and fallback-to-sort. The advanced integration/failure-injection/stress/performance suites and the second benchmark baseline are **DEFERRED** to CP3, where the coverage, failure-injection, and stress SLAs are validated.

**CP2 Verdict: `APPROVED`** for the nine present suites and `StreamingShuffleBenchmark-results.txt`; the five advanced suites and the second benchmark baseline **DEFERRED**.

### Phase 8 — Documentation & Governance Domain

**Files**

| File | Mode | Feature | CP2 Status |
|------|------|---------|------------|
| `blitzy-docs/streaming-shuffle/index.md` | CREATE | F-119 | DEFERRED (CP3) |
| `blitzy-docs/streaming-shuffle/configuration.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/architecture.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/observability.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/decision-log.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/executive-summary.html` | CREATE | F-119 | DEFERRED (CP3) |
| `blitzy-docs/streaming-shuffle/dashboard.json` | CREATE | F-119 | APPROVED |
| `docs/streaming-shuffle-architecture.md` | CREATE | F-120 | APPROVED |
| `docs/streaming-shuffle-guide.md` | CREATE | F-120 | APPROVED |
| `docs/streaming-shuffle-troubleshooting.md` | CREATE | F-120 | APPROVED |
| `docs/streaming-shuffle-tuning.md` | CREATE | F-120 | APPROVED |
| `CODE_REVIEW.md` | CREATE | governance ledger | APPROVED |

**Review criteria** — Present documentation must accurately reflect the implemented CP2 semantics, every cross-link must resolve, the decision log must hold the design rationale plus a requirement→source→test traceability matrix, and this ledger must be CP2-accurate and assign every CP2 changed file to exactly one phase.

**Findings (CP2 fixes)** — `architecture.md` was corrected to match the implementation: the writer is described as a `ShuffleWriter` composing a private `MemoryConsumer`; the data-flow narrative and Diagram 0.5-A now describe the real publish-via-`IndexShuffleBlockResolver`-then-fetch path with the v1 stub noted, registration-time fallback, per-stream ack keying, the 10 s liveness detector, endpoint validation, and real spill/reset/reclaim; all three Mermaid diagrams remain titled, legended, and referenced (resolves M5/R3). `observability.md` was aligned to the emitted `LogKeys`: the documented fields are renamed to `range` and `task_attempt_id`, a "where applicable" clause is added, context-specific keys are listed, plain lifecycle logs are noted as by-design, and the cross-executor correlation claim is made precise (resolves M8/R1 doc side; deviation D-4). `decision-log.md` (new) holds a 20-row ADR table, an explicit deviations log, and a requirement→source→test traceability matrix, and is the home for the design rationale trimmed from production Scaladoc (resolves R2). `configuration.md`, `dashboard.json`, and the four Jekyll pages are unchanged (CP1). This ledger is reviewed in this phase and is CP2-accurate (resolves R5). `index.md` and `executive-summary.html` (the executive deck, R4) are **DEFERRED** to CP3.

**CP2 Verdict: `APPROVED`** for `configuration.md`, `architecture.md`, `observability.md`, `decision-log.md`, `dashboard.json`, the four Jekyll pages, and this ledger; `index.md` and `executive-summary.html` **DEFERRED**.

## Rules Compliance (CP2)

| Rule | Status | Evidence |
|------|--------|----------|
| R1 — Observability | ✅ APPROVED | Four metrics via `StreamingShuffleSource` through the existing `MetricsSystem`; runtime logs emit MDC correlation fields via typed `LogKeys` where in scope; `observability.md` documents the fields actually emitted. |
| R2 — Explainability | ✅ APPROVED | `decision-log.md` holds the 20-row ADR table, deviations log, and requirement→source→test matrix; design rationale was trimmed from production Scaladoc to concise functional docs with ADR pointers. |
| R3 — Visual Architecture Documentation | ✅ APPROVED | `architecture.md` carries the coexistence, integration, and data-flow Mermaid diagrams — each titled, legended, and referenced — and now matches the implementation. |
| R4 — Executive Presentation | ➖ DEFERRED (CP3) | `executive-summary.html` is out of scope for CP2. |
| R5 — Segmented PR Review | ✅ APPROVED | This ledger is advanced to CP2, maps every changed file to exactly one domain phase with a verdict, and documents F-115 as the single intentional stub. |

## Changed-File Inventory

This table lists every changed file across the whole feature, its mode, its feature ID, the single domain phase that owns it, and its CP2 status. Files present at CP2 carry an `APPROVED` verdict; files not yet created carry `DEFERRED` and are not approved at this checkpoint.

| File Path | Mode | Feature ID | Review Phase | CP2 Status |
|-----------|------|-----------|--------------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | MODIFY | F-117 (SPI alias) | Phase 1 | APPROVED |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | MODIFY | F-117 (config keys) | Phase 1 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` | CREATE | F-114 | Phase 1 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | CREATE | F-101 | Phase 2 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | CREATE | F-102 | Phase 2 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | CREATE | F-103 | Phase 3 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | CREATE | F-104 | Phase 3 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | CREATE | F-105 | Phase 3 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingBuffer.scala` | CREATE | F-106 | Phase 3 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | CREATE | F-107 | Phase 4 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpoint.scala` | CREATE | F-108 | Phase 4 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | CREATE | F-109 | Phase 4 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala` | CREATE | F-110 | Phase 4 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` | CREATE | F-111 | Phase 4 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | CREATE | F-112 | Phase 5 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | CREATE | F-113 | Phase 5 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | CREATE | F-118 | Phase 5 | APPROVED |
| `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | CREATE | F-118 | Phase 5 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` | CREATE | F-115 (v1 stub) | Phase 6 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala` | CREATE | F-116 | Phase 6 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandleSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpointSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicySuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala` | CREATE | F-121 | Phase 7 | DEFERRED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | CREATE | F-121 | Phase 7 | DEFERRED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` | CREATE | F-121 | Phase 7 | DEFERRED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleStressSuite.scala` | CREATE | F-121 | Phase 7 | DEFERRED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | CREATE | F-121 | Phase 7 | DEFERRED |
| `core/benchmarks/StreamingShuffleBenchmark-results.txt` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` | CREATE | F-121 | Phase 7 | DEFERRED |
| `blitzy-docs/streaming-shuffle/index.md` | CREATE | F-119 | Phase 8 | DEFERRED |
| `blitzy-docs/streaming-shuffle/configuration.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/architecture.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/observability.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/decision-log.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/executive-summary.html` | CREATE | F-119 | Phase 8 | DEFERRED |
| `blitzy-docs/streaming-shuffle/dashboard.json` | CREATE | F-119 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-architecture.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-guide.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-troubleshooting.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-tuning.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `CODE_REVIEW.md` | CREATE | governance ledger | Phase 8 | APPROVED |

> **Traceability:** The full requirement→source→test traceability matrix (Explainability rule) is maintained in [`blitzy-docs/streaming-shuffle/decision-log.md`](blitzy-docs/streaming-shuffle/decision-log.md), mapping each requirement to its implementing class and its covering test suite.

## Final Verdict

**`APPROVED` (CP2 — FULL PRODUCTION RUNTIME); overall feature `IN PROGRESS`.**

The CP2 full-production-runtime set is accepted: the 40 present files resolve to `APPROVED` in their owning phases, with no `BLOCKED` finding outstanding. Every CP2 review finding is resolved at its root cause — the writer publishes fetchable output through `IndexShuffleBlockResolver` and folds spilled segments back into it (C1/C2); the reader enforces the 5 s producer timeout with a bounded fetch and decodes frames behind a budget guard (C3/M7); the manager enforces dual-flag activation and acts on the fallback policy, delegating to the inner `SortShuffleManager` (M9/M10); the spill manager releases heap on spill and is runtime-wired with ack-driven reclaim (M1/M2/M11); the backpressure protocol keys ack state per stream and adds a 10 s liveness detector (M3/M4); the RPC endpoint validates and sanitizes messages (M6); the TechDocs match the implementation, the decision log holds the rationale and traceability matrix, and this ledger is advanced to CP2 (M5/M8/R1/R2/R3/R5). The change remains strictly additive — the default path is provably unchanged (`spark.shuffle.manager` remains `"sort"`; the reflective `SparkEnv` factory and every AQE/scheduler surface are untouched). The CP2 source set compiles with zero errors and zero warnings, the present suites pass, `git diff --check` is clean, and no streaming production line exceeds the column limit.

This verdict covers **CP2 only**. The advanced integration / failure-injection / stress / performance-benchmark suites (F-121), the second benchmark baseline, the landing page (`index.md`), and the executive deck (`executive-summary.html`, R4) are **DEFERRED** to CP3. The feature as a whole is therefore **not yet cleared to merge**; the v1 logging-only network-transport stub (F-115) remains the single documented, intentional placeholder and is recorded in the decision log (ADR-15, deviation D-1).

### Verdict History

The table below shows the commit/re-commit cadence across checkpoints, satisfying the requirement that `CODE_REVIEW.md` be committed before the first review phase and re-committed on every phase transition and on the checkpoint verdict.

| Commit point | Ledger state | Verdict |
|--------------|--------------|---------|
| CP1 · Pre-flight gate | Created at repository root; gate evaluated for CP1 scope | — |
| CP1 · Checkpoint verdict | CP1 foundation present files `APPROVED`; runtime/control plane `DEFERRED` | `APPROVED` (CP1); feature `IN PROGRESS` |
| CP2 · Pre-flight gate | Ledger advanced to CP2 scope; gate re-evaluated | — |
| CP2 · Phase 1 → 2 | SPI & Configuration — dual-flag accessor reviewed | APPROVED (CP2) |
| CP2 · Phase 2 → 3 | Manager & Dispatch — activation, fallback, spill-wiring reviewed | APPROVED (CP2) |
| CP2 · Phase 3 → 4 | Data Path — publication, spill fold-in, bounded reader reviewed | APPROVED (CP2) |
| CP2 · Phase 4 → 5 | Flow Control & Memory — per-stream ack, liveness, validation, reclaim reviewed | APPROVED (CP2) |
| CP2 · Phase 5 → 6 | Observability — metrics source + MDC correlation reviewed | APPROVED (CP2) |
| CP2 · Phase 6 → 7 | Wire Transport — v1 stub discipline + envelope invariant reviewed | APPROVED (CP2) |
| CP2 · Phase 7 → 8 | Test & Benchmark — present suites + first baseline reviewed | APPROVED (CP2); advanced suites + 2nd baseline DEFERRED |
| CP2 · Checkpoint verdict | CP2 present files `APPROVED`; CP3 items `DEFERRED` | **`APPROVED` (CP2); feature `IN PROGRESS`** |
