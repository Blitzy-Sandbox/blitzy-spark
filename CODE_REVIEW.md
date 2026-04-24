---
feature: "Streaming Shuffle (F-001) — Apache Spark 4.2.0-SNAPSHOT"
aap_version: "0.5 — Technical Implementation"
created: "YYYY-MM-DD"
last_updated: "YYYY-MM-DD"
pr_status: "NOT_OPEN"
principal_reviewer_verdict: "PENDING"
phases:
  - id: 1
    name: "Infrastructure/DevOps"
    persona: "DevOps-Persona (SRE / Build Engineer)"
    status: "OPEN"
    handoff_from: null
    handoff_to: 2
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
  - id: 2
    name: "Security"
    persona: "SecOps-Persona (AppSec Reviewer)"
    status: "OPEN"
    handoff_from: 1
    handoff_to: 3
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
  - id: 3
    name: "Backend Architecture"
    persona: "BackendArch-Persona (Scala/JVM Architect)"
    status: "OPEN"
    handoff_from: 2
    handoff_to: 4
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
  - id: 4
    name: "QA/Test Integrity"
    persona: "QA-Persona (Test Strategy Lead)"
    status: "OPEN"
    handoff_from: 3
    handoff_to: 5
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
  - id: 5
    name: "Business/Domain"
    persona: "Domain-Persona (Shuffle Subsystem SME)"
    status: "OPEN"
    handoff_from: 4
    handoff_to: 6
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
  - id: 6
    name: "Frontend (Not Applicable)"
    persona: "Frontend-Persona (UX/Accessibility Reviewer — documented closure)"
    status: "OPEN"
    handoff_from: 5
    handoff_to: 7
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
  - id: 7
    name: "Principal Reviewer"
    persona: "Principal-Persona (Staff Engineer / Gate Keeper)"
    status: "OPEN"
    handoff_from: 6
    handoff_to: null
    started_at: null
    completed_at: null
    findings_count: 0
    open_issues: 0
    resolved_issues: 0
---

# CODE_REVIEW.md — Segmented PR Review Ledger

**Feature:** Streaming Shuffle (F-001) — opt-in, coexisting alternative to the production-stable `SortShuffleManager` on Apache Spark 4.2.0-SNAPSHOT.

**Governing Agent Action Plan:** AAP §0.5 "Technical Implementation" is authoritative on file-by-file scope. AAP §0.7.7 "Segmented PR Review Rule" is authoritative on this file's structure and lifecycle. AAP §0.7.8 "Non-Negotiable Invariants" is authoritative on the terminal checklist.

## Purpose

This file is the single source of truth for the multi-phase code review that must complete before any pull request opens for the Streaming Shuffle feature. It tracks seven sequential review phases, each assigned to a named Expert Agent persona, each with an explicit status lifecycle (`OPEN` → `IN_REVIEW` → `APPROVED` | `BLOCKED`). Handoffs between phases are logged in place. The Principal Reviewer (Phase 7) consolidates findings from Phases 1-6, verifies alignment between the implemented code and the AAP, and records the final verdict that unlocks PR opening.

Per AAP §0.7.8 non-negotiable invariant: `CODE_REVIEW.md` must reach a Principal Reviewer `APPROVED` verdict before a pull request opens.

## Phase 1 — Infrastructure/DevOps

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

DevOps-Persona (SRE / Build Engineer)

### Scope

- Verify `pom.xml`, `core/pom.xml`, `common/network-common/pom.xml`, `common/network-shuffle/pom.xml`, and `common/utils/pom.xml` are untouched (zero new Maven coordinates introduced per AAP §0.3.1).
- Confirm CI workflow compatibility (`.github/workflows/build_and_test.yml`, `.github/workflows/build_infra.yml`, `.github/workflows/maven_test.yml`) with the new test suites under the existing Java 17.0.11 + Scala 2.13.18 matrix.
- Validate `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` is well-formed and loadable by the existing `MetricsSystem` parser.
- Validate `blitzy-docs/streaming-shuffle-dashboard-template.json` parses as JSON and conforms to the Grafana dashboard schema.
- Run RAT license check: `./build/sbt rat` — zero license violations.
- Run MiMa binary compatibility gate: `./build/sbt -mem 5632 mimaReportBinaryIssues` — zero new issues, zero new entries in `project/MimaExcludes.scala`.
- Run Scalastyle: `./build/sbt scalastyle` — zero violations in the new `org.apache.spark.shuffle.streaming.*` sub-package.
- Run Checkstyle: `./build/mvn -B -pl core checkstyle:check` — zero violations.
- Run documentation build: `./build/sbt doc` — zero errors; Scaladoc for new public types renders cleanly.
- Confirm `dev/sparktestsupport/modules.py` requires no edits (the existing `core` module transparently discovers the new suites).

### Entry Criteria

- All streaming shuffle source files exist at their expected paths per AAP §0.5.1.
- Metrics template and Grafana dashboard template files are committed.
- A clean checkout of the feature branch builds via `./build/mvn -B -pl core install -DskipTests`.

### Exit Criteria

- `./build/mvn -B -pl core install -DskipTests` succeeds.
- `./build/sbt -mem 5632 mimaReportBinaryIssues` reports zero new issues.
- `./build/sbt scalastyle` reports zero violations.
- `./build/mvn -B -pl core checkstyle:check` reports zero violations.
- `./build/sbt rat` reports zero license violations.
- `./build/sbt doc` completes without errors.
- `blitzy-docs/streaming-shuffle-dashboard-template.json` parses as valid JSON.
- `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` parses as a valid properties file.
- Every Maven POM referenced in AAP §0.3.2.2 is bit-for-bit unchanged.

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->

## Phase 2 — Security

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

SecOps-Persona (AppSec Reviewer)

### Scope

- Verify streaming transport inherits the existing transport security envelope: `spark.authenticate`, `spark.authenticate.secret`, SASL, and `spark.network.crypto.enabled` flow through automatically because streaming classes use the `TransportContext` obtained from `SparkEnv`, not a newly-constructed context.
- Confirm zero new secrets, credentials, or cryptographic primitives are introduced.
- Audit CRC32C usage in `StreamingBlockEnvelope` and `StreamingShuffleWriter` — must be integrity-only; must not be used as an authentication code or message-authentication primitive.
- Verify `BackpressureRpcEndpoint` registration is guarded to executors only via a `SparkEnv.get.executorId != SparkContext.DRIVER_IDENTIFIER` check; driver-side instantiation of the endpoint is prohibited.
- Verify the five new `spark.shuffle.streaming.*` config keys are non-sensitive and do not require additions to `SparkConf.redact`.
- Confirm no new network port, TLS certificate, or credential store is introduced.
- Confirm log output under `spark.shuffle.streaming.debug=true` contains no payload bytes, checksum secret material, or authentication tokens.

### Entry Criteria

- Phase 1 `APPROVED`.
- `StreamingShuffleTransport`, `StreamingBlockEnvelope`, `BackpressureProtocol`, and `BackpressureRpcEndpoint` implementations are present and compile.
- The five new config keys are visible in `ConfigEntrySuite` or equivalent config registry test.

### Exit Criteria

- Streaming transport demonstrably obtains `TransportContext` from `SparkEnv` (no `new TransportContext(...)` call inside the streaming sub-package).
- SASL and TLS handshake traces confirmed in an authenticated `local-cluster` integration run.
- CRC32C is scoped to per-block payload integrity validation; no usage as MAC or authentication token.
- Driver-side `BackpressureRpcEndpoint` setup throws an explicit `IllegalStateException` (or equivalent) with a clear diagnostic message.
- Zero new keys added to `SparkConf.redact`.
- Log sampling under debug mode confirms zero secret or payload leakage.

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->

## Phase 3 — Backend Architecture

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

BackendArch-Persona (Scala/JVM Architect)

### Scope

- Verify `StreamingShuffleManager` implements all six methods of the `ShuffleManager` trait with public-surface parity to `SortShuffleManager`: `registerShuffle`, `getWriter`, `getReader`, `unregisterShuffle`, `shuffleBlockResolver`, `stop`.
- Verify full isolation in the `org.apache.spark.shuffle.streaming` and `org.apache.spark.shuffle.streaming.network` sub-packages; zero streaming imports reach `org.apache.spark.shuffle.sort.*` except the documented `SortShuffleManager` delegate-reference in `StreamingShuffleManager`.
- Verify zero modification to DAG scheduler, task lifecycle, executor lifecycle, RDD/DataFrame/Dataset user-facing APIs, lineage tracking, task serialization, or `BlockManager` interface contracts (AAP §0.6.2 Explicitly Out of Scope).
- Verify zero modification to `SortShuffleManager` internals, `SortShuffleWriter`, `UnsafeShuffleWriter`, `BypassMergeSortShuffleWriter`, `ShuffleExternalSorter`, `ShuffleInMemorySorter`, `PackedRecordPointer`, `IndexShuffleBlockResolver`, or the existing `ShuffleDataIO` plug-in contract surface.
- Verify the executor memory model is consumed through public surfaces only: `MemoryManager.acquireExecutionMemory` and `MemoryManager.releaseExecutionMemory`; no internal restructuring of `UnifiedMemoryManager`, `TaskMemoryManager`, or `MemoryConsumer`.
- Verify atomic metadata commit on spill is delegated to `IndexShuffleBlockResolver.writeMetadataFileAndCommit`; no new commit protocol is invented (ADR-002).
- Verify Netty OOM backoff: `NettyUtils.freeDirectMemory()` checked before envelope `ByteBuf` allocation, and the global `isNettyOOMOnShuffle` `AtomicBoolean` is honored (ADR-004).
- Verify push-based shuffle mutual exclusion per active shuffle: when `spark.shuffle.push.enabled=true`, the `StreamingShuffleFallbackPolicy` selects sort-based routing (ADR-005).
- Verify every new class in the streaming sub-package is `private[spark]` or resides in a new sub-package so no existing public signature changes; no `@DeveloperApi` additions in v1.
- Verify the three edits to existing files (`ShuffleManager.scala`, `internal/config/package.scala`, `LogKey.scala`) are append-only and do not rename, reorder, or remove any existing entry.
- Confirm `project/MimaExcludes.scala` is unchanged; no new exclusions are required.

### Entry Criteria

- Phases 1-2 `APPROVED`.
- All classes in `core/src/main/scala/org/apache/spark/shuffle/streaming/` and `core/src/main/scala/org/apache/spark/shuffle/streaming/network/` compile and pass the module build.
- The three modified files (`ShuffleManager.scala`, `internal/config/package.scala`, `LogKey.scala`) compile.

### Exit Criteria

- `ShuffleManager` trait implementation verified by instantiating `StreamingShuffleManager` and confirming all six methods dispatch correctly.
- Isolation audit confirms zero `import org.apache.spark.shuffle.sort.*` in streaming sub-package classes except the documented `SortShuffleManager` delegate reference in `StreamingShuffleManager`, which must carry an inline comment explaining the coexistence strategy.
- Absolute preservation list in AAP §0.1.2 and §0.6.2 reconciled file-by-file with the git diff.
- Memory manager consumption audit confirms zero access to private members; only `acquireExecutionMemory` / `releaseExecutionMemory` are called.
- Atomic commit delegation to `IndexShuffleBlockResolver.writeMetadataFileAndCommit` confirmed for the spill path.
- Push-based shuffle fallback path verified by a unit test asserting `SortShuffleManager` delegation when `spark.shuffle.push.enabled=true`.
- All new classes carry `private[spark]` visibility unless the AAP explicitly specifies otherwise.
- Edits to `ShuffleManager.scala`, `internal/config/package.scala`, and `LogKey.scala` are append-only diffs.
- MiMa passes with zero new exclusions in `project/MimaExcludes.scala`.

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->


## Phase 4 — QA/Test Integrity

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

QA-Persona (Test Strategy Lead)

### Scope

- Verify the ten new test files exist at their expected paths per AAP §0.5.1.3 and extend `SparkFunSuite`: `StreamingShuffleManagerSuite`, `StreamingShuffleWriterSuite`, `BackpressureProtocolSuite`, `StreamingShuffleReaderSuite`, `MemorySpillManagerSuite`, `StreamingShuffleFallbackPolicySuite`, `StreamingShuffleIntegrationTest`, `StreamingShuffleFailureInjectionSuite`, `StreamingShuffleStressSuite`, and `StreamingShufflePerformanceBenchmark`.
- Verify unit-test coverage greater than 85% for the new `org.apache.spark.shuffle.streaming.*` sub-package (AAP §0.7.6 quality gate).
- Verify all unit suites pass with zero failures and zero flakiness under 100 consecutive runs of the integration test.
- Verify `StreamingShuffleIntegrationTest` exercises all five user-specified scenarios: 100 MB / 10-partition shuffle with 30% latency reduction assertion; producer failure mid-shuffle with partial-read invalidation; consumer 50% slowdown with automatic spill; network partition with timeout and fallback; 5 concurrent shuffles with buffer allocation arbitration.
- Verify `StreamingShuffleFailureInjectionSuite` exercises all ten failure scenarios and asserts zero data loss: producer crash, consumer crash, network partition, memory exhaustion, disk failure, checksum mismatch, connection timeout, executor JVM pause (GC), multiple concurrent producer failures, consumer reconnect after extended downtime.
- Verify `StreamingShuffleStressSuite` runs a 5-minute continuous workload with 10 concurrent tasks and 5 concurrent shuffles; asserts less than 5% throughput degradation; forces full GC and confirms heap analysis shows zero retained shuffle objects.
- Verify deterministic fault injection: thread interrupts, closed sockets, `System.gc()`, truncated `ByteBuf` payloads, and explicit `Thread.sleep` for simulated consumer slowdown — never time-based flakes.
- Verify no new test tag class is introduced; all new suites run under the default untagged CI set.
- Verify every invocation of `ShuffleReadMetricsReporter` (17 methods) and `ShuffleWriteMetricsReporter` (5 methods) in the streaming writer/reader is covered by test assertions (F-009 parity).
- Verify the benchmark regenerates its golden file via `SPARK_GENERATE_BENCHMARK_FILES=1` and the committed file matches.

### Entry Criteria

- Phases 1-3 `APPROVED`.
- All ten test files and the benchmark compile.
- A local run of the new unit suites completes in under 10 minutes cumulative (excluding the 5-minute stress suite).

### Exit Criteria

- All unit suites report zero failures under `./build/mvn -B -pl core -Dtest=none -Dsuites="org.apache.spark.shuffle.streaming.*" test`.
- Coverage tool reports greater than 85% line coverage and greater than 85% branch coverage for `org.apache.spark.shuffle.streaming.*`.
- 100 consecutive integration test runs all pass (zero flakiness).
- Five integration scenarios each pass individually and in aggregate.
- Ten failure-injection scenarios each assert zero data loss via post-run result validation.
- Stress suite 5-minute run shows less than 5% throughput degradation vs. a measured first-minute baseline.
- Post-stress heap analysis confirms zero retained shuffle objects (forced full GC).
- F-009 reporter parity: every reporter method invoked by `BlockStoreShuffleReader` and `SortShuffleWriter` has an equivalent call-site covered by a test assertion in the streaming path.
- Benchmark golden file reproducible via `SPARK_GENERATE_BENCHMARK_FILES=1`.

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->

## Phase 5 — Business/Domain

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

Domain-Persona (Shuffle Subsystem SME)

### Scope

- Verify all five user-specified success criteria have dedicated test coverage:
  - 30-50% end-to-end latency reduction for shuffle-heavy workloads (100 MB+ data, 10+ partitions).
  - 5-10% improvement for CPU-bound workloads through reduced scheduler overhead.
  - Zero performance regression for memory-bound workloads, validated by automatic fallback engagement.
  - Zero data loss under all failure scenarios including producer crashes, consumer failures, and network partitions.
  - Memory exhaustion prevention through 80% threshold spill trigger with less than 100 ms response time.
- Verify all four user-specified fallback conditions are correctly evaluated in `StreamingShuffleFallbackPolicy`:
  - Consumer sustained 2x slower than producer for greater than 60 seconds.
  - Memory pressure prevents buffer allocation (OOM risk).
  - Network saturation exceeds 90% link capacity.
  - Producer/consumer version mismatch (compatibility check).
- Verify the per-partition buffer sizing formula `(executorMemory * bufferPercent) / numPartitions` is implemented faithfully in `StreamingShuffleWriter`.
- Verify the token-bucket refill formula `maxBandwidthMBps / numConcurrentShuffles` is implemented faithfully in `TokenBucketRateLimiter`.
- Verify the user's operational constraints are preserved: block size less than or equal to 2 MB, heartbeat interval 10 s, connection timeout 5 s, TCP keepalive 5 s interval, CRC32C algorithm, exponential backoff starting 1 s with max 5 attempts, telemetry overhead less than 1% CPU, log volume less than 10 MB/hour/executor.
- Verify the Shuffle-Preservation Gate compatibility: when `spark.dynamicAllocation.enabled=true` is set, documentation clearly states that streaming shuffle does not claim reliable storage by default and that operators must separately enable ESS, shuffleTracking, decommission, or a reliable `ShuffleDataIO` plug-in (AAP §0.7.2).
- Verify the user's Failure Handling Protocol flows are implemented exactly as specified (producer-failure detection flow and consumer-failure detection flow from AAP §0.1.2).
- Verify documentation updates in `docs/configuration.md`, `docs/tuning.md`, `docs/core-migration-guide.md`, `blitzy-docs/streaming-shuffle.md`, and `blitzy-docs/index.md` accurately reflect the implemented behavior.
- Verify the four new `LogKey` entries (`BUFFER_UTILIZATION_PERCENT`, `SPILL_COUNT`, `BACKPRESSURE_EVENTS`, `PARTIAL_READ_INVALIDATIONS`) are emitted at the functionally correct observation points.

### Entry Criteria

- Phases 1-4 `APPROVED`.
- `blitzy-docs/streaming-shuffle.md`, `blitzy-docs/streaming-shuffle-decision-log.md`, and `blitzy-docs/streaming-shuffle-traceability.md` are present.
- Documentation updates to `docs/configuration.md`, `docs/tuning.md`, and `docs/core-migration-guide.md` are present.

### Exit Criteria

- All five success criteria are tied to a passing test with an explicit quantitative assertion.
- All four fallback conditions independently trigger delegation to `SortShuffleManager` in deterministic unit tests.
- Per-partition buffer and token-bucket refill formulas match the user-specified expressions exactly.
- User operational constraints (block size, timeouts, algorithm choices, overhead budgets) each map to a config entry or code constant verified by inspection.
- Shuffle-Preservation Gate compatibility documented in `docs/core-migration-guide.md` and `docs/configuration.md`.
- Failure Handling Protocol flows traced end-to-end in the failure-injection suite.
- Documentation review confirms accuracy of every claim about behavior.
- Decision log rows exist for every non-trivial choice (Option A vs. Option B, token-bucket vs. leaky-bucket, CRC32C vs. Murmur3, RPC heartbeat vs. piggy-back acknowledgment, and similar).

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->

## Phase 6 — Frontend (Not Applicable)

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

Frontend-Persona (UX/Accessibility Reviewer — documented closure)

### Scope

- Document closure: Streaming Shuffle is a backend-only performance feature with no Spark UI, HTML, JavaScript, CSS, or React component additions.
- Verify zero file additions or modifications in `core/src/main/resources/org/apache/spark/ui/` and `core/src/main/scala/org/apache/spark/ui/`.
- Confirm the new `shuffle.streaming.*` Dropwizard instruments surface through the pre-existing JMX, Prometheus, and Graphite sinks automatically; no new sink, servlet, or handler is added.
- Confirm the Spark UI Stages page's "Shuffle Read" / "Shuffle Write" columns continue to render correctly for streaming-mode shuffles because streaming writer/reader invoke the existing `ShuffleReadMetricsReporter` and `ShuffleWriteMetricsReporter` methods (F-009 parity).
- Confirm the Grafana dashboard template `blitzy-docs/streaming-shuffle-dashboard-template.json` is an operator artefact for external dashboards; it is not a Spark UI page and is not imported by the running Spark process.
- Record documented closure: this phase is `APPROVED` once the non-applicability is verified by inspection, with no UX/accessibility review work required.

### Entry Criteria

- Phases 1-5 `APPROVED`.
- Grafana dashboard template committed.

### Exit Criteria

- `git diff` over the feature branch shows zero modifications under `core/src/main/resources/org/apache/spark/ui/`.
- `git diff` shows zero modifications under `core/src/main/scala/org/apache/spark/ui/`.
- `git diff` shows zero additions of `.html`, `.js`, `.css`, or `.tsx` files.
- Spark UI smoke test (local-cluster run with `spark.shuffle.manager=streaming`) confirms the Stages page renders shuffle metrics identically to a sort-mode run.
- Documented closure rendered and `APPROVED` without ceremony.

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->



## Phase 7 — Principal Reviewer

### Status

**OPEN** — Initial state. Last transition timestamp: to-be-set on first review.

### Assigned Persona

Principal-Persona (Staff Engineer / Gate Keeper)

### Scope

- Consolidate findings from Phases 1-6 into an executive summary; confirm zero open items remain.
- Validate alignment between the implemented code and the AAP file-by-file: every CREATE / MODIFY action in AAP §0.5.1 is reflected in the git diff, and nothing outside AAP §0.6.1 is modified.
- Spot-check the bidirectional traceability matrix in `blitzy-docs/streaming-shuffle-traceability.md`: confirm 100% coverage by sampling three randomly selected user requirements and verifying each traces to an implementing class, method, and test.
- Review the decision log in `blitzy-docs/streaming-shuffle-decision-log.md`: confirm every non-trivial implementation choice has a row with columns Decision / Alternatives Considered / Rationale / Risks.
- Verify `blitzy-docs/streaming-shuffle-executive-summary.html` opens, renders in a browser, contains 12-18 slides (target 16), and every slide carries at least one non-text visual (Mermaid diagram, KPI card, styled table, or Lucide SVG icon).
- Verify Observability Rule compliance (AAP §0.7.7): structured logging with correlation IDs via `SparkLogger` and `MDC`; four `shuffle.streaming.*` metrics visible via JMX and Prometheus; Grafana dashboard template shipped; unit and integration tests assert metric counters advance under exercise.
- Verify Explainability Rule compliance (AAP §0.7.7): decision log complete; traceability matrix 100%.
- Verify Visual Architecture Documentation Rule (AAP §0.7.7): before/after Mermaid diagrams present in `blitzy-docs/streaming-shuffle.md`; titles and legends carried.
- Verify Executive Presentation Rule (AAP §0.7.7): reveal.js 5.1.0 HTML file self-contained; Blitzy brand palette (`#5B39F3` primary, `#2D1C77` dark, `#94FAD5` teal, `#1A105F` navy); Inter / Space Grotesk / Fira Code typography; zero emoji; Mermaid 11.4.0 with `startOnLoad: false` and `mermaid.run()` on `ready` and `slidechanged`; Reveal.js config with `hash: true`, `transition: 'slide'`, `controlsTutorial: false`, `width: 1920`, `height: 1080`; all CDN versions pinned.
- Verify the MiMa binary compatibility gate passes with zero new entries in `project/MimaExcludes.scala`.
- Verify RAT, Scalastyle, Checkstyle, and `./build/sbt doc` are all green.
- Render final verdict: `principal_reviewer_verdict: APPROVED` and update top-level `pr_status: READY_FOR_PR`.

### Entry Criteria

- Phases 1-6 all `APPROVED`.
- `blitzy-docs/streaming-shuffle-traceability.md`, `blitzy-docs/streaming-shuffle-decision-log.md`, `blitzy-docs/streaming-shuffle.md`, `blitzy-docs/streaming-shuffle-dashboard-template.json`, and `blitzy-docs/streaming-shuffle-executive-summary.html` are all present and pass per-file linting.

### Exit Criteria

- Zero open findings from Phases 1-6.
- File-by-file git diff reconciliation with AAP §0.5.1 shows every CREATE / MODIFY entry reflected and zero out-of-scope modifications.
- Traceability matrix spot-check passes for three sampled requirements.
- Decision log contains a row for every non-trivial decision.
- Executive summary opens, renders, slide count is between 12 and 18 inclusive, every slide carries a non-text visual, zero emoji.
- Observability, Explainability, Visual Architecture Documentation, Executive Presentation, and Segmented PR Review rules all satisfied.
- MiMa, RAT, Scalastyle, Checkstyle, and SBT doc all green.
- Final verdict recorded: `principal_reviewer_verdict: APPROVED` in YAML frontmatter; `pr_status: READY_FOR_PR`.

### Findings

| ID | Severity | Description | Evidence (file:line) | Status | Resolution |
|----|----------|-------------|----------------------|--------|------------|
<!-- no findings yet -->

### Handoff Log

<!-- no handoff events recorded yet -->

## Global Quality Gates

Each of these gates is reproduced from AAP §0.7.6 and must be checked before the Principal Reviewer renders `APPROVED`.

- [ ] Unit test coverage greater than 85% for all new components.
- [ ] All unit tests pass with zero failures.
- [ ] All integration tests pass with zero flakiness.
- [ ] Failure injection tests validate zero data loss under all scenarios.
- [ ] Memory leak validation: zero retained heap after stress test completion.
- [ ] Code compiles without errors or warnings.
- [ ] Static analysis passes with zero critical issues.
- [ ] Scalastyle: `build/sbt scalastyle` reports zero violations.
- [ ] Java style: `build/mvn checkstyle:check` reports zero violations.
- [ ] MiMa: `build/sbt -mem 5632 mimaReportBinaryIssues` reports zero new issues.
- [ ] RAT: `build/sbt rat` reports zero license violations.
- [ ] Documentation build: `build/sbt doc` completes without errors.

## Non-Negotiable Invariants

Each invariant is reproduced from AAP §0.7.8 and must be validated by the Principal Reviewer before `APPROVED` is granted.

- [ ] `spark.shuffle.manager=sort` (default) behavior is bit-for-bit unchanged; MiMa passes; existing test suites pass unchanged.
- [ ] `spark.shuffle.manager=streaming` activates the new path; all 5 success criteria are validated by dedicated tests.
- [ ] Zero files outside the Exhaustively In Scope list of AAP §0.6.1 are modified.
- [ ] Zero new third-party dependencies added to any `pom.xml`.
- [ ] Zero entries added to `project/MimaExcludes.scala`.
- [ ] Every non-trivial decision is entered into `blitzy-docs/streaming-shuffle-decision-log.md`.
- [ ] `blitzy-docs/streaming-shuffle-traceability.md` achieves 100% coverage.
- [ ] `blitzy-docs/streaming-shuffle-executive-summary.html` opens, renders, and contains 12-18 slides with every slide carrying a non-text visual.
- [ ] `CODE_REVIEW.md` reaches a Principal Reviewer `APPROVED` verdict before PR open.

## Referenced Artifacts

This table maps every AAP-mandated deliverable to its repository path, its initial review status, and the owning review phase. Status values follow the lifecycle defined in the Lifecycle Rules section and begin as `PENDING_REVIEW` at file creation time; they transition to `APPROVED` as the owning phase records its findings resolution.

| Artifact | Path | Status | Owning Phase |
|----------|------|--------|--------------|
| Streaming shuffle manager | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming shuffle handle | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming shuffle writer | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming shuffle reader | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Backpressure protocol | `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Backpressure RPC endpoint | `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpoint.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Memory spill manager | `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Fallback policy | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming shuffle metrics source | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming block envelope | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming shuffle transport | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Token-bucket rate limiter | `core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Metrics properties template | `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | PENDING_REVIEW | Phase 1 — Infrastructure/DevOps |
| Manager short-name registration (modified) | `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Config entries (modified) | `core/src/main/scala/org/apache/spark/internal/config/package.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Log keys (modified) | `common/utils/src/main/scala/org/apache/spark/internal/LogKey.scala` | PENDING_REVIEW | Phase 3 — Backend Architecture |
| Streaming shuffle manager suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Streaming shuffle writer suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Backpressure protocol suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Streaming shuffle reader suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Memory spill manager suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Fallback policy suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicySuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Streaming shuffle integration test | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Streaming shuffle failure-injection suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Streaming shuffle stress suite | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleStressSuite.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Streaming shuffle performance benchmark | `core/benchmarks/StreamingShufflePerformanceBenchmark.scala` | PENDING_REVIEW | Phase 4 — QA/Test Integrity |
| Configuration docs (modified) | `docs/configuration.md` | PENDING_REVIEW | Phase 5 — Business/Domain |
| Tuning docs (modified) | `docs/tuning.md` | PENDING_REVIEW | Phase 5 — Business/Domain |
| Core migration guide (modified) | `docs/core-migration-guide.md` | PENDING_REVIEW | Phase 5 — Business/Domain |
| Architectural write-up | `blitzy-docs/streaming-shuffle.md` | PENDING_REVIEW | Phase 5 — Business/Domain |
| Decision log | `blitzy-docs/streaming-shuffle-decision-log.md` | PENDING_REVIEW | Phase 5 — Business/Domain |
| Traceability matrix | `blitzy-docs/streaming-shuffle-traceability.md` | PENDING_REVIEW | Phase 7 — Principal Reviewer |
| Grafana dashboard template | `blitzy-docs/streaming-shuffle-dashboard-template.json` | PENDING_REVIEW | Phase 1 — Infrastructure/DevOps |
| Executive summary presentation | `blitzy-docs/streaming-shuffle-executive-summary.html` | PENDING_REVIEW | Phase 7 — Principal Reviewer |
| Docs index registration (modified) | `blitzy-docs/index.md` | PENDING_REVIEW | Phase 5 — Business/Domain |
| Segmented PR review ledger (this file) | `CODE_REVIEW.md` | PENDING_REVIEW | Phase 7 — Principal Reviewer |

## Lifecycle Rules

The following rules govern phase transitions and the overall gate. They are normative; any violation requires escalation to the Principal Reviewer.

- Phase transitions must occur in order: Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7. Skipping a phase is not allowed.
- A phase can be in exactly one of four states at any time: `OPEN`, `IN_REVIEW`, `APPROVED`, or `BLOCKED`.
- Valid state transitions: `OPEN → IN_REVIEW`, `IN_REVIEW → APPROVED`, `IN_REVIEW → BLOCKED`, and `BLOCKED → IN_REVIEW` (after remediation). Any other transition is a process violation.
- A phase may be set to `BLOCKED` only if at least one finding with `Status: OPEN` exists and that finding has a proposed remediation path. If no remediation path exists, the phase escalates to the Principal Reviewer for a governance decision.
- Phase 7 (Principal Reviewer) cannot enter `IN_REVIEW` until Phases 1-6 are all `APPROVED`.
- When the Principal Reviewer records `principal_reviewer_verdict: APPROVED`, update the top-level `pr_status` key from `NOT_OPEN` to `READY_FOR_PR`.
- Every state transition must update the corresponding phase's `started_at` / `completed_at` field in the YAML frontmatter and must be logged as a row in that phase's Handoff Log section with an ISO-8601 timestamp.
- A phase is not complete until its Exit Criteria are fully satisfied and no finding with `Status: OPEN` remains.
