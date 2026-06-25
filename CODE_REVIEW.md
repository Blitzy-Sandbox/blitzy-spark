# Code Review — Streaming Shuffle Subsystem (Checkpoint CP3 — VERIFICATION & CAPSTONE)

This document is the **Segmented PR Review** ledger for the new **opt-in streaming shuffle subsystem** added to Apache Spark (`spark-parent_2.13`, `4.2.0-SNAPSHOT`). It records the multi-phase, file-partitioned review that governs the feature's merge into `master`.

This revision of the ledger reflects the **CP3 — VERIFICATION & CAPSTONE** checkpoint, the final checkpoint of the feature. CP3 delivers the verification layer and the capstone artifacts on top of the CP1 foundation and the CP2 full-production runtime: the advanced **integration**, **integration-test**, **failure-injection**, **stress**, and **performance-benchmark** suites (completing the 14-suite F-121 set), the landing page (`index.md`), and the executive deck (`executive-summary.html`). It also lands the CP3 remediation of the runtime — bounded CRC32C retransmission in the reader, deterministic ordered teardown seams in the manager, and a latent import-order fix in the RPC endpoint — and the capstone documentation polish (decision-log alternatives/traceability, deck accuracy/accessibility, and a single consistent checksum narrative across all docs). **No feature file remains `DEFERRED`** at CP3; the only feature artifact intentionally not committed is the regenerable `StreamingShufflePerformanceBenchmark-results.txt` (see the Test & Benchmark phase).

The streaming shuffle is delivered as a **new `ShuffleManager` Service Provider Interface (SPI) implementation** that *coexists with* — and never replaces — the default `SortShuffleManager`. The change is **strictly additive**: the default `spark.shuffle.manager` value remains `"sort"`, the reflective `SparkEnv` factory call site is untouched, and streaming engages only under an explicit dual-flag activation contract (`spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true`). All out-of-scope surfaces — `DAGScheduler`, `TaskScheduler`, lineage / fault recovery, `ShuffleExchangeExec`, every Adaptive Query Execution (AQE) rule, the existing `SortShuffleManager`, and block-manager storage contracts — are preserved verbatim.

## Review Metadata

| Field | Value |
|-------|-------|
| Target branch | `master` |
| Base version | `4.2.0-SNAPSHOT` |
| Feature | Opt-in streaming shuffle subsystem |
| Checkpoint | CP3 — VERIFICATION & CAPSTONE (final) |
| Activation contract | `spark.shuffle.manager=streaming` **and** `spark.shuffle.streaming.enabled=true` |
| Default behavior | Unchanged — `spark.shuffle.manager` remains `"sort"` |
| New runtime dependencies | None (all primitives already declared in the build) |
| Review model | Segmented PR Review — 8 sequential domain phases |
| Review ledger | `CODE_REVIEW.md` (this file, repository root) |

### Scope Summary

At CP3 the full feature change set is present and reviewed. The only feature artifact intentionally **not committed** is the second benchmark result file, which is regenerable on demand and is out of scope as a committed artifact (the benchmark *object* is present and its run path is green).

| Category | Present at CP3 | Not committed | Detail |
|----------|---------------:|--------------:|--------|
| Existing files **MODIFIED** | 2 | 0 | SPI alias map + internal config registry (both additive) |
| Production classes **CREATED** | 16 | 0 | All runtime/control-plane classes (F-101–F-116) present and reviewed |
| Production resources **CREATED** | 2 | 0 | `package.scala` Scaladoc + `metrics.properties.template` (F-118) |
| Test suites **CREATED** | 14 | 0 | All 14 F-121 suites present — 9 unit/component + integration, integration-test, failure-injection, stress, and performance-benchmark |
| Benchmark result files | 1 | 1 | `StreamingShuffleBenchmark-results.txt` present; `StreamingShufflePerformanceBenchmark-results.txt` **not committed** (regenerable via `SPARK_GENERATE_BENCHMARK_FILES=1`; out of scope as a committed file) |
| Documentation artifacts **CREATED** | 11 | 0 | 7 TechDocs (`index`, `configuration`, `architecture`, `observability`, `decision-log`, `executive-summary.html`, `dashboard.json`) + 4 Jekyll pages |
| Governance artifact **CREATED** | 1 | 0 | this `CODE_REVIEW.md` review ledger |

At CP3, **47 changed files** are present and reviewed; the single remaining feature artifact (`StreamingShufflePerformanceBenchmark-results.txt`) is intentionally not committed. The union across all checkpoints is the full 48-entry feature change set.

### Review Lifecycle

The Segmented PR Review follows a strict commit cadence so the review ledger is always an accurate, version-controlled reflection of review state:

1. **Pre-flight gate** — Before any domain phase begins, the pre-flight gate (below) is evaluated for the checkpoint's scope. `CODE_REVIEW.md` exists at the repository root and is committed **before the first review phase**.
2. **Commit before Phase 1** — The ledger, with the pre-flight gate recorded, is committed prior to opening the first domain phase.
3. **Re-commit on every phase transition** — As each domain phase resolves, the ledger is updated with that phase's verdict and **re-committed** before the next phase opens.
4. **Checkpoint verdict commit** — Once the checkpoint's in-scope files resolve, the **Final Verdict** for the checkpoint is recorded and the ledger is committed again.

**Pre-flight gate criteria (CP3 scope, stated verbatim):**

- All CP3 VERIFICATION & CAPSTONE deliverables exist at their specified paths. The only feature artifact not committed is `StreamingShufflePerformanceBenchmark-results.txt`, which is regenerable on demand and is intentionally out of scope as a committed file (the benchmark object is present and runs).
- No production-path method in the source set returns a placeholder stub. The v1 network transport (`StreamingShuffleTransport`, F-115) is the single documented, intentional logging-only stub exception **by design**, recorded in the decision log (ADR-15, deviation D-1).
- `CODE_REVIEW.md` is present at the repository root and advanced to CP3.

## Pre-Flight Gate

**Status: `PASSED` (CP3 scope)** — every criterion applicable to the CP3 verification & capstone set is satisfied.

| Check | Status | Notes |
|-------|--------|-------|
| All CP3 deliverable paths exist | ✅ PASSED | The 47 files in the [Changed-File Inventory](#changed-file-inventory) are present; only `StreamingShufflePerformanceBenchmark-results.txt` is intentionally not committed (regenerable; out of scope as a file). |
| No production placeholder stubs | ✅ PASSED | Runtime classes implement complete logic; the writer publishes fetchable output via `IndexShuffleBlockResolver`, the reader fetches with a bounded timeout and now performs bounded CRC32C retransmission, the spill manager reclaims real heap, and fallback delegates to sort. The v1 logging-only transport stub (F-115) is the single documented exception (decision log ADR-15 / D-1). |
| `CODE_REVIEW.md` present at repository root | ✅ PASSED | This file, advanced to CP3. |
| Build compiles — zero errors, zero warnings | ✅ PASSED | `./build/mvn -pl core -DskipTests compile` with `-Wconf:any:e` (warnings-as-errors), `-Wunused:imports`, `-release 17`. |
| Tests compile and all 14 streaming suites pass | ✅ PASSED | `./build/mvn -pl core test-compile` succeeds; the full 14-suite F-121 set runs green (`failed 0`). |
| Scalastyle clean (`maxColumn=98`) | ✅ PASSED | SBT `core/scalastyle` + `core/Test/scalastyle` report 0 errors. CP3 fixed the `StreamingShuffleReaderSuite` import-order + non-ASCII + line-length blockers and a latent `BackpressureRpcEndpoint` import-order error, so the benchmark `core/Test/runMain` path is no longer gated. |
| Apache RAT license headers | ✅ PASSED | Every new **source** file carries the ASF header; the benchmark result file is excluded through `dev/.rat-excludes`. Prose Markdown intentionally omits the header per repository convention. |
| MiMa binary compatibility | ➖ ADDITIVE BY CONSTRUCTION (CI gate) | New public symbols are additive only — new `ConfigEntry` vals and new `private[spark]` classes; no existing signature is changed. |

## Review Phases

The feature's changed files are partitioned into **eight sequential domain phases**. Every changed file appears in **exactly one** phase — including this ledger, which is reviewed in Phase 8. At CP3, each phase reviews every file in its domain (all are now present), resolving each to `APPROVED`; no file remains `DEFERRED`.

### Phase 1 — SPI & Configuration Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | MODIFIED | F-117 (SPI alias) | APPROVED |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | MODIFIED | F-117 (config keys) | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` | CREATE | F-114 | APPROVED |

**Review criteria** — Edits must be additive-only; the default `spark.shuffle.manager` must remain `"sort"`; range checks must hold; the config accessor must encode the dual-flag activation contract.

**Findings** — The `shortShuffleMgrNames` map gains exactly one `"streaming"` entry and the `SHUFFLE_MANAGER` default is unchanged. `StreamingShuffleConfig` carries `managerSelected` (true only when `spark.shuffle.manager` resolves to the `"streaming"` alias) and the composite `active` accessor, so the manager enforces both halves of the dual-flag contract (M9). Configuration remains immutable for the application lifetime. **CP3 regression check:** no CP3 diff to these three files; the CP3 dual-flag and parity tests in Phase 7 exercise this accessor live. Rationale lives in the decision log (ADR-01, ADR-02).

**CP3 Verdict: `APPROVED`** (3 of 3 files reviewed).

### Phase 2 — Manager & Dispatch Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | CREATE | F-101 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | CREATE | F-102 | APPROVED |

**Review criteria** — The manager must compose the inner `SortShuffleManager`, dispatch by handle type, enforce dual-flag activation, act on the fallback policy, wire the spill manager, and **stop deterministically and idempotently**; the handle is a `BaseShuffleHandle` subtype carrying tuning vals.

**Findings (CP3 fixes)** — `StreamingShuffleManager.stop()` was refactored into four ordered, overridable teardown seams — `stopBackpressureEndpoint` → `stopSpillManager` → `stopInnerSortManager` → `clearStreamingState` — so shutdown is **observably ordered (Backpressure → Spill → Sort → state)** and **idempotent** (a second `stop()` is a safe no-op). This makes the lifecycle directly testable by `StreamingShuffleManagerSuite` (Phase 7) without weakening any runtime behavior. The CP2 dispatch/activation/fallback/spill-wiring logic (M9/M10/M11) is unchanged and re-verified by the new CP3 dispatch tests. `StreamingShuffleHandle` is unchanged (pure dispatch discriminator). Rationale lives in the decision log (ADR-01, ADR-02, ADR-14, ADR-16).

**CP3 Verdict: `APPROVED`** (2 of 2 files reviewed).

### Phase 3 — Data Path Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | CREATE | F-103 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | CREATE | F-104 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | CREATE | F-105 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingBuffer.scala` | CREATE | F-106 | APPROVED |

**Review criteria** — Writer output must be fetchable before `MapStatus` is returned; spilled bytes must not be dropped; the reader must enforce the 5 s producer timeout, validate checksums **with retransmission per the AAP contract**, and invalidate partial reads; the block resolver and buffer must preserve their contracts.

**Findings (CP3 fixes)** — `StreamingShuffleReader` now implements **bounded CRC32C retransmission** to satisfy the AAP "validate checksums with retransmission" contract: a transient CRC32C mismatch (modelled as `RetransmittableBlockException`) and transient transport errors trigger a re-fetch of the affected block with exponential backoff, all within the shared 5 s producer deadline and capped at `MAX_RETRANSMIT_ATTEMPTS`; only a **persistent** checksum mismatch, a **structural decode error** (non-retransmittable), or an exceeded deadline invalidates the partial read, increments `partialReadInvalidations`, and throws `FetchFailedException` so the DAG scheduler recomputes. This removes the prior "invalidate immediately, no retransmission" behavior (closing the former deviation D-5) and is covered by the new transient-recovery and failure tests in Phase 7. The CP2 publication path (writer frames ≤ 2 MiB `StreamingBlockEnvelope` records and commits via the shared `IndexShuffleBlockResolver`; spilled segments fold back into the published output — C1/C2), the bounded fetch (C3), and the budget-guarded frame decode (M7) are unchanged. `StreamingBuffer` and `StreamingShuffleBlockResolver` are unchanged. Rationale lives in the decision log (ADR-03–ADR-10; ADR-10 now records bounded-retransmit-then-invalidate).

**CP3 Verdict: `APPROVED`** (4 of 4 files reviewed).

### Phase 4 — Flow Control & Memory Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | CREATE | F-107 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpoint.scala` | CREATE | F-108 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | CREATE | F-109 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala` | CREATE | F-110 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` | CREATE | F-111 | APPROVED |

**Review criteria** — Acknowledgment state must be scoped per stream; messages must be validated and sanitized; a 10 s consumer-liveness detector must exist; the spill manager must release heap after a successful spill, be runtime-wired, and reclaim on ack; the fallback policy predicates must be exact; **all sources must satisfy the Scalastyle import-order gate.**

**Findings (CP3 fixes)** — `BackpressureRpcEndpoint` had a latent Scalastyle import-order violation (`LogKeys` ordered before `Logging` in the `org.apache.spark.internal` import group) that gated the SBT `core/Test/scalastyle` run and, transitively, the benchmark `core/Test/runMain` path; the import group was reordered to `{Logging, LogKeys, MessageWithContext}`, clearing the gate. The CP2 behavioral fixes are unchanged: `BackpressureProtocol` keys ack state per `StreamKey` and runs a 10 s liveness detector (M3/M4); the endpoint validates and sanitizes messages (M6); `MemorySpillManager` resets buffers after a reader-visible spill and on `stop()` (M1/M2/M11). `TokenBucketRateLimiter` and `StreamingShuffleFallbackPolicy` are unchanged. Rationale lives in the decision log (ADR-06, ADR-07, ADR-11–ADR-14, ADR-17).

**CP3 Verdict: `APPROVED`** (5 of 5 files reviewed).

### Phase 5 — Observability Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | CREATE | F-112 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` | CREATE | F-113 | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | CREATE | F-118 | APPROVED |
| `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | CREATE | F-118 | APPROVED |

**Review criteria** — Exactly four metric state fields under `shuffle.streaming.`; a `Source` named `streamingShuffle` registering them with the existing `MetricsSystem`; runtime logs must populate the documented MDC correlation fields where applicable.

**Findings** — `StreamingShuffleMetrics` defines the four required fields (`bufferUtilizationPercent`, `spillCount`, `backpressureEvents`, `partialReadInvalidations`); `StreamingShuffleSource` implements `Source` with `sourceName = "streamingShuffle"` and exposes the four gauges. Runtime components emit structured `log"…"` MDC correlation values under typed `LogKeys` where the identifier is in scope. **CP3 regression check:** no CP3 diff to these four files; the emitted MDC field names (`range`, `task_attempt_id`) are now consistent across `observability.md`, the decision log, and the executive deck (R1, see Phase 8). `package.scala` and `metrics.properties.template` are unchanged.

**CP3 Verdict: `APPROVED`** (4 of 4 files reviewed).

### Phase 6 — Wire Transport Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` | CREATE | F-115 (v1 stub) | APPROVED |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala` | CREATE | F-116 | APPROVED |

**Review criteria** — The transport must be the single documented v1 logging-only stub, reusing the existing `BlockTransferService` and adding no new `TransportContext`; the envelope must enforce the 32-byte header + ≤ 2 MiB CRC32C payload invariant on every path.

**Findings** — `StreamingShuffleTransport` remains the **single, intentional logging-only stub** (decision log ADR-15 / deviation D-1): `send` logs and returns `Unit`, `fetch` delegates to the existing `BlockTransferService`, and no new transport context/port/Netty bootstrap is introduced. `StreamingBlockEnvelope` enforces the documented invariant on every construction path and is exercised by the CP3 failure-injection corruption and reader decode/checksum tests (Phase 7). **CP3 regression check:** no CP3 diff to these two files.

**CP3 Verdict: `APPROVED`** (2 of 2 files reviewed).

### Phase 7 — Test & Benchmark Domain

**Files**

| File | Mode | Feature | CP3 Status |
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
| `StreamingShuffleIntegrationSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleIntegrationTest.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleFailureInjectionSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShuffleStressSuite.scala` | CREATE | F-121 | APPROVED |
| `StreamingShufflePerformanceBenchmark.scala` | CREATE | F-121 | APPROVED |
| `core/benchmarks/StreamingShuffleBenchmark-results.txt` | CREATE | F-121 | APPROVED |
| `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` | CREATE | F-121 | NOT COMMITTED (regenerable; out of scope) |

**Review criteria** — Suites must compile, pass, and prove the AAP verification semantics: active-streaming output parity vs sort across ≥ 2 shuffle shapes at ≥ 10 partitions; exactly ten zero-data-loss failure scenarios with true 5 s-timeout and end-to-end recompute semantics; a 5-minute active-streaming stress run with ~10 % failure injection and no retained heap; a `BenchmarkBase` object whose run path is green and that writes no result file unless requested.

**Findings (CP3 fixes)** —

- **`StreamingShuffleReaderSuite`** — fixed the Scalastyle blockers (import order: `SharedSparkContext` before `ShuffleDependency`; replaced non-ASCII em-dashes with ASCII; wrapped the over-length Scaladoc line ≤ 98). `newReader` is now configurable for `aggregator`, `keyOrdering`, and `mapSideCombine`, and the suite adds **semantic parity tests** asserting the streaming reader matches sort-reader semantics for aggregation (`combineValuesByKey`), map-side combine (`combineCombinersByKey`), and key ordering, plus a **transient-checksum-recovery** test (corrupt-then-valid → read succeeds via retransmission). Resolves R-a, R-b, R-c, R-d.
- **`StreamingShuffleManagerSuite`** — adds `getWriter`/`getReader` dispatch tests (streaming handle → `StreamingShuffleWriter`/`StreamingShuffleReader`; sort handle → delegated sort writer/`BlockStoreShuffleReader`), an executor-mode (`isDriver=false`) collaborator-gating test (spill manager + backpressure endpoint present; `stop()` cleans up), and a `stop()` idempotence + ordered-teardown test (Backpressure → Spill → Sort) using the new manager seams. Resolves M.
- **`StreamingShuffleIntegrationSuite` / `StreamingShuffleIntegrationTest`** — the handle-only "active" checks are replaced with **real active dual-flag parity**: `reduceByKey` and `groupByKey`/`join` at ≥ 10 partitions, asserting exact equality to a sort baseline; the integration test runs the large/chained/skew/config cases under active streaming across ≥ 2 shuffle shapes. The inaccurate "streaming active cannot move data" Scaladoc is corrected. Resolves I, IT.
- **`StreamingShuffleFailureInjectionSuite`** — the producer-timeout scenario now models a **silent producer** (transport never answers) with a small injected `producerTimeoutMs`, asserting a timeout-driven `FetchFailedException` and a `partialReadInvalidations` increment; the recomputation scenario is now an **end-to-end active-streaming Spark job** with a deterministic one-time fetch failure, proving DAG recomputation completes with exact full output. The checksum scenario reflects bounded-retransmission-then-invalidation. The suite holds **exactly ten** scenarios. Resolves F1, F2.
- **`StreamingShuffleStressSuite`** — the sustained correctness/failure-injection loop now runs with **both flags enabled (active streaming)**, injects streaming-specific failure/fallback/retry conditions, and asserts correctness, no data loss, and no retained heap. Resolves S.
- **`StreamingShufflePerformanceBenchmark`** — extends `BenchmarkBase` correctly; with the Scalastyle gate cleared, the canonical `core/Test/runMain …StreamingShufflePerformanceBenchmark` path runs both the sort and streaming cases green under the test JVM (which carries the JDK17 `--add-opens java.base/java.nio=ALL-UNNAMED` required by Kryo) and writes **no** result file unless `SPARK_GENERATE_BENCHMARK_FILES=1`. The over-length run-instruction Scaladoc line was wrapped ≤ 98. Resolves B1, B2.
- **`StreamingShufflePerformanceBenchmark-results.txt`** — intentionally **not committed**: it is regenerable on demand and is out of scope as a committed artifact; the review confirms no result file is generated by the green run path, so there is no scope creep.

All 14 suites compile and pass; coverage now exercises the highest-risk semantic paths (reader parity, manager dispatch/lifecycle, active parity, true timeout/recompute, active stress) that previously undermined the > 85 % coverage claim.

**CP3 Verdict: `APPROVED`** (14 suites + the first benchmark baseline reviewed; the second baseline is intentionally not committed).

### Phase 8 — Documentation & Governance Domain

**Files**

| File | Mode | Feature | CP3 Status |
|------|------|---------|------------|
| `blitzy-docs/streaming-shuffle/index.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/configuration.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/architecture.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/observability.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/decision-log.md` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/executive-summary.html` | CREATE | F-119 | APPROVED |
| `blitzy-docs/streaming-shuffle/dashboard.json` | CREATE | F-119 | APPROVED |
| `docs/streaming-shuffle-architecture.md` | CREATE | F-120 | APPROVED |
| `docs/streaming-shuffle-guide.md` | CREATE | F-120 | APPROVED |
| `docs/streaming-shuffle-troubleshooting.md` | CREATE | F-120 | APPROVED |
| `docs/streaming-shuffle-tuning.md` | CREATE | F-120 | APPROVED |
| `CODE_REVIEW.md` | CREATE | governance ledger | APPROVED |

**Review criteria** — Documentation must accurately reflect the implemented semantics; every cross-link must resolve; the decision log must hold the design rationale, a per-decision alternatives column, a single intentional deviation, and a complete bidirectional traceability matrix; the executive deck must be accurate and accessible; this ledger must be CP3-accurate and assign every changed file to exactly one phase.

**Findings (CP3 fixes)** —

- **`decision-log.md`** — the ADR table gained an **Alternatives Considered** column (every one of the 20 rows now records concrete rejected alternatives); the deviations log was reduced to the **single intentional F-115 stub (D-1)** (network-saturation folded into D-1 as a v1-stub consequence; the former D-2..D-5 removed — version-mismatch is the fallback policy operating correctly, MDC naming is the ADR-20 design decision, and checksum retransmission is now implemented per ADR-10); and the requirement→source→test **traceability matrix** was rewritten to use exact repository paths and exact suite names, made bidirectional, and extended with per-metric rows for all four metrics and rows for every config key (resolves R2 / decision-log findings).
- **`executive-summary.html`** — slide 11 MDC fields corrected to `range` and `task_attempt_id` (consistent with `observability.md` and the decision log, resolving the R1/R4 inconsistency); decorative Lucide icons marked `aria-hidden="true"` and the three Mermaid diagram containers given `role="img"` + descriptive `aria-label` (accessibility); `crossorigin="anonymous"` added to the four static CDN tags with a documented SRI posture (integrity hashes attached at deploy time from the serving CDN; the Mermaid ESM `import` cannot carry a standard `integrity` attribute); and the Lucide UMD script moved before the Mermaid ESM module so `window.lucide` is defined deterministically when the module's icon pass runs. 16 slides and exact pinned CDN versions are preserved (resolves R4 deck findings).
- **`index.md`** — the ten landing-page failure scenarios were rewritten to mirror the ten `StreamingShuffleFailureInjectionSuite` tests one-to-one in order, and the checksum narrative aligned to bounded-retransmit-then-invalidate; Diagram 0.2-A remains byte-identical to `architecture.md` and all six sibling links resolve (resolves the index findings; R3 preserved).
- **`architecture.md`** — the checksum-handling prose (data-flow narrative, F-104 component row, and failure summary) was aligned to bounded-retransmit-then-invalidate so the entire doc set tells one consistent story; the three Mermaid diagrams remain titled, legended, and referenced (R3).
- **`docs/streaming-shuffle-troubleshooting.md`** and the other Jekyll pages already described retransmission-then-invalidation and are consistent; no CP3 diff was required.
- **`CODE_REVIEW.md`** — this ledger is advanced to CP3: every changed file is mapped to exactly one phase, all phases resolve to `APPROVED`, the previously `DEFERRED` items are now reviewed, and the Final Verdict and Verdict History are updated (resolves the CODE_REVIEW.md / R5 finding).
- **`configuration.md`** and **`dashboard.json`** are unchanged and consistent.

**CP3 Verdict: `APPROVED`** (12 of 12 files reviewed).

## Rules Compliance (CP3)

| Rule | Status | Evidence |
|------|--------|----------|
| R1 — Observability | ✅ APPROVED | Four metrics via `StreamingShuffleSource` through the existing `MetricsSystem`; runtime logs emit MDC correlation fields via typed `LogKeys` where in scope; `observability.md`, the decision log, and the executive deck now use the **same** emitted field names (`range`, `task_attempt_id`). |
| R2 — Explainability | ✅ APPROVED | `decision-log.md` holds the 20-row ADR table **with an Alternatives Considered column**, a **single** intentional deviation (F-115 / D-1), and a complete **bidirectional** requirement→source→test matrix with exact paths/suites and per-metric rows. |
| R3 — Visual Architecture Documentation | ✅ APPROVED | `architecture.md` carries the coexistence, integration, and data-flow Mermaid diagrams — each titled, legended, and referenced; Diagram 0.2-A is byte-identical between `architecture.md` and `index.md`. |
| R4 — Executive Presentation | ✅ APPROVED | `executive-summary.html` is a single self-contained 16-slide reveal.js deck with exact pinned CDNs (`reveal.js@5.1.0`, `mermaid@11.4.0`, `lucide@0.460.0`), accurate MDC fields, accessibility labels, `crossorigin` on static CDN tags with a documented SRI posture, and a deterministic Reveal → Lucide → Mermaid init order. |
| R5 — Segmented PR Review | ✅ APPROVED | This ledger is advanced to **CP3**, maps every changed file to exactly one of eight domain phases with an `APPROVED` verdict, documents F-115 as the single intentional stub, and records the full commit cadence in the Verdict History. |

## Changed-File Inventory

This table lists every changed file across the whole feature, its mode, its feature ID, the single domain phase that owns it, and its CP3 status. Every present file carries an `APPROVED` verdict; the one regenerable benchmark result file is marked `NOT COMMITTED`.

| File Path | Mode | Feature ID | Review Phase | CP3 Status |
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
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleStressSuite.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/benchmarks/StreamingShuffleBenchmark-results.txt` | CREATE | F-121 | Phase 7 | APPROVED |
| `core/benchmarks/StreamingShufflePerformanceBenchmark-results.txt` | CREATE | F-121 | Phase 7 | NOT COMMITTED (regenerable; out of scope) |
| `blitzy-docs/streaming-shuffle/index.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/configuration.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/architecture.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/observability.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/decision-log.md` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/executive-summary.html` | CREATE | F-119 | Phase 8 | APPROVED |
| `blitzy-docs/streaming-shuffle/dashboard.json` | CREATE | F-119 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-architecture.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-guide.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-troubleshooting.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `docs/streaming-shuffle-tuning.md` | CREATE | F-120 | Phase 8 | APPROVED |
| `CODE_REVIEW.md` | CREATE | governance ledger | Phase 8 | APPROVED |

> **Traceability:** The full requirement→source→test traceability matrix (Explainability rule) is maintained in [`blitzy-docs/streaming-shuffle/decision-log.md`](blitzy-docs/streaming-shuffle/decision-log.md), mapping each requirement to its implementing class and its covering test suite.

## Final Verdict

**`APPROVED` (CP3 — VERIFICATION & CAPSTONE); overall feature `APPROVED` — cleared to merge.**

The full feature change set is accepted: all **47 present files** resolve to `APPROVED` in their owning phases, with **no `BLOCKED`** finding outstanding and **no file `DEFERRED`**. Every CP3 review finding is resolved at its root cause:

- **Reader semantics & retransmission (Phase 3 / Phase 7)** — the reader now performs bounded CRC32C retransmission within the 5 s producer deadline, invalidating only on persistent corruption, a structural decode error, or timeout; the reader suite proves sort-reader parity for aggregation, map-side combine, and key ordering, plus transient-checksum recovery (R-a..R-d).
- **Manager dispatch & lifecycle (Phase 2 / Phase 7)** — `getWriter`/`getReader` dispatch, executor-mode collaborator gating, and idempotent ordered teardown (Backpressure → Spill → Sort) are tested against the new manager seams (M).
- **Active-streaming parity (Phase 7)** — integration suites prove active dual-flag output equals the sort baseline across ≥ 2 shuffle shapes at ≥ 10 partitions (I, IT).
- **Zero data loss (Phase 7)** — exactly ten failure scenarios with a true silent-producer 5 s timeout and an end-to-end DAG-recompute proof (F1, F2); the 5-minute stress run is active streaming with ~10 % failure injection and no retained heap (S).
- **Benchmark run path (Phase 4 / Phase 7)** — the Scalastyle gate (reader suite + a latent RPC-endpoint import-order error) is cleared, so the `BenchmarkBase` run path is green for both sort and streaming cases and writes no result file unless requested (B1, B2).
- **Capstone docs & governance (Phase 8)** — the decision log gains an alternatives column, a single F-115 deviation, and a complete bidirectional traceability matrix; the executive deck is accurate (MDC fields), accessible, and hardened (crossorigin/SRI posture, deterministic init order); the landing page scenarios match the suite; the checksum narrative is consistent across every doc; and this ledger is advanced to CP3 (R1, R2, R3, R4, R5).

The change remains strictly additive — the default path is provably unchanged (`spark.shuffle.manager` remains `"sort"`; the reflective `SparkEnv` factory and every AQE/scheduler surface are untouched). The source set compiles with zero errors and zero warnings, all 14 streaming suites pass, and SBT `core/scalastyle` + `core/Test/scalastyle` report zero violations.

The v1 logging-only network-transport stub (F-115) remains the single documented, intentional placeholder, recorded in the decision log (ADR-15, deviation D-1); it does not affect correctness because the writer publishes fetchable output through `IndexShuffleBlockResolver` and the reader fetches through the standard map-output path. The only feature artifact not committed is the regenerable `StreamingShufflePerformanceBenchmark-results.txt`, which is out of scope as a committed file.

### Verdict History

The table below shows the commit/re-commit cadence across checkpoints, satisfying the requirement that `CODE_REVIEW.md` be committed before the first review phase and re-committed on every phase transition and on the checkpoint verdict.

| Commit point | Ledger state | Verdict |
|--------------|--------------|---------|
| CP1 · Pre-flight gate | Created at repository root; gate evaluated for CP1 scope | — |
| CP1 · Checkpoint verdict | CP1 foundation present files `APPROVED`; runtime/control plane `DEFERRED` | `APPROVED` (CP1); feature `IN PROGRESS` |
| CP2 · Pre-flight gate | Ledger advanced to CP2 scope; gate re-evaluated | — |
| CP2 · Checkpoint verdict | CP2 present files `APPROVED`; CP3 items `DEFERRED` | `APPROVED` (CP2); feature `IN PROGRESS` |
| CP3 · Pre-flight gate | Ledger advanced to CP3 scope; gate re-evaluated for verification & capstone set | — |
| CP3 · Phase 1 → 2 | SPI & Configuration — additive alias/config re-verified live by CP3 parity tests | APPROVED (CP3) |
| CP3 · Phase 2 → 3 | Manager & Dispatch — ordered/idempotent teardown seams reviewed and tested | APPROVED (CP3) |
| CP3 · Phase 3 → 4 | Data Path — bounded CRC32C retransmission reviewed | APPROVED (CP3) |
| CP3 · Phase 4 → 5 | Flow Control & Memory — RPC-endpoint import-order Scalastyle fix reviewed | APPROVED (CP3) |
| CP3 · Phase 5 → 6 | Observability — MDC field-name consistency across docs/deck reviewed | APPROVED (CP3) |
| CP3 · Phase 6 → 7 | Wire Transport — v1 stub discipline + envelope invariant re-verified | APPROVED (CP3) |
| CP3 · Phase 7 → 8 | Test & Benchmark — active parity, true timeout/recompute, active stress, green benchmark path; all 14 suites reviewed | APPROVED (CP3); 2nd baseline NOT COMMITTED |
| CP3 · Phase 8 (governance) | Documentation & Governance — decision-log alternatives/traceability, accurate/accessible deck, aligned scenarios, ledger advanced to CP3 | APPROVED (CP3) |
| CP3 · Checkpoint verdict | All present files `APPROVED`; no `DEFERRED`; F-115 single documented stub | **`APPROVED` (CP3); feature `APPROVED` — cleared to merge** |
