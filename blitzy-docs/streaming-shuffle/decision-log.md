<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Streaming Shuffle Decision Log

Per AAP §0.7.3.7, every non-trivial implementation decision for the streaming-shuffle feature is documented here. A decision is non-trivial if a competent engineer could reasonably have chosen differently. **Rationale MUST NOT be embedded in code comments. The decision log is the single source of truth for *why* decisions.**

This page also includes the bidirectional traceability matrix (AAP §0.7.3.7) mapping each user-prompt requirement to its implementing source file(s) — 100% coverage, no gaps.

## Document Purpose and Audience

This decision log serves three distinct audiences:

1. **Reviewers and auditors** — verify that every non-trivial decision was explicitly considered, that alternatives were weighed, and that residual risk is acknowledged. The log is the artifact a Principal Reviewer consults during the segmented PR review (AAP §0.7.7) before issuing the binary APPROVED / BLOCKED verdict.
2. **Future maintainers** — understand *why* a particular implementation choice was made before changing it. A decision recorded here may be revisited later, but only with explicit awareness of the original rationale and risk profile.
3. **Operators and integrators** — verify that the streaming-shuffle feature delivers what the user requested, by following the bidirectional traceability matrix from any user-prompt clause to the source file(s) that implement it.

A decision is "non-trivial" if any of the following apply:
- A competent engineer could reasonably have chosen a different alternative.
- The chosen path imposes a constraint that future work must respect (e.g., singleton invariant, classpath dependency).
- The chosen path materially affects performance, correctness, or observability.
- The choice deviates from upstream Spark idiom or from the literal user prompt.

## Decision Methodology

Each row in the Decision Log table follows a four-column structure: **Decision**, **Alternatives**, **Rationale**, **Risk**. The methodology used to populate each row is:

- **Decision** — a one-phrase summary of the chosen path. Concrete enough that a reader can identify it in source code without ambiguity.
- **Alternatives** — at least one viable alternative the team considered and rejected. If no viable alternative exists (e.g., user mandates a specific algorithm), the row records the rejected hypothetical alternatives so the reader sees the full design space.
- **Rationale** — the reason for the chosen path. Cites the AAP section and, where applicable, the user-prompt clause that drove the choice. Rationale is descriptive (states *why* the choice was made) rather than prescriptive (states *what* must be done elsewhere).
- **Risk** — the residual risk of the chosen path. Categorized as **None**, **Low**, **Medium**, or **High**. A rating above **Low** would require an explicit risk-mitigation plan; no decision in the table below carries a risk rating above **Low**.

## Decision Log

The following table captures every non-trivial implementation choice made while authoring the streaming-shuffle feature. The set of eight rows extends beyond the AAP-required minimum of five entries (decisions 1–5) to include three additional implementation-strategy choices (decisions 6–8) flagged during the assigned-folder review. Each row names the decision, the alternatives considered, the rationale for the chosen path, and the residual risk.

| # | Decision | Alternatives | Rationale | Risk |
|---|----------|--------------|-----------|------|
| 1 | Streaming via `ShuffleManager` SPI | Implementing a new `ShuffleDataIO` plugin instead | User prompt explicitly specifies `StreamingShuffleManager`; AAP §0.1.1 requires class name `StreamingShuffleManager` registered under short name `streaming`. The `ShuffleManager` is the higher-abstraction extension point; the `ShuffleDataIO` plugin operates below it and would not satisfy the user's specification. | Low — `ShuffleManager` is a stable SPI used by the existing `SortShuffleManager`; reusing it carries no novel API risk. |
| 2 | Token-bucket location: per-`BackpressureProtocol` instance | Global cross-shuffle rate limiter | User directive (AAP §0.7.2.3) requires "priority arbitration across concurrent shuffles"; per-instance buckets enable per-shuffle priority allocation. A single global limiter would treat all shuffles identically and violate the priority-arbitration directive. | Low — per-instance state is small (a `long` for tokens plus a refill timer); memory overhead is negligible. |
| 3 | CRC32C checksum algorithm | xxHash, MD5, SHA-1, SHA-256 | User specification (AAP §0.7.2.4) explicitly mandates "Checksum algorithm: CRC32C only (no MD5, SHA-1, SHA-256, xxHash, or alternative algorithm)." JDK 17 ships `java.util.zip.CRC32C` natively — no external dependency. | None — algorithm is mandated by user; alternative cryptographic hashes (MD5, SHA-*) are slower and overkill for integrity checks. |
| 4 | 100 ms spill polling interval | Event-driven memory-pressure callback | User specification (AAP §0.7.2.2) is explicit: "Memory release MUST occur within 100 ms of consumer acknowledgment." A 100 ms polling timer is simple, deterministic, and meets the budget. Event-driven callbacks would also satisfy timing but deviate from the explicit spec and introduce coupling to `MemoryManager` listener APIs. | Low — 100 ms wakeups have negligible CPU cost; well under the 1% telemetry-overhead budget per AAP §0.7.2.5. |
| 5 | Guava `CacheBuilder` for LRU partition selection | Custom LRU implementation (LinkedHashMap-based) | Guava is already on the `core` module classpath via the parent POM (no new dependency); `recordStats()` provides built-in metrics integration that aligns with the observability rule (AAP §0.7.4). A custom `LinkedHashMap`-based LRU would duplicate working library code. | None — Guava is a stable, vetted dependency at production-grade quality. |
| 6 | Hold private `SortShuffleManager` instance for fallback | Re-instantiate `SortShuffleManager` on each fallback decision | The `ShuffleManager` is a singleton per `SparkEnv` (AAP §0.1.2 cites `SparkEnv._shuffleManager` is a volatile, lazily initialized singleton). Re-instantiation would violate the SPI-singleton invariant and risk inconsistent shuffle-state across the executor lifetime. Holding a private instance preserves the invariant and eliminates re-initialization overhead. | Low — adds one collaborator field to `StreamingShuffleManager`; both managers share the same `SparkConf`. |
| 7 | Existing `Utils.instantiateSerializerOrShuffleManager` reflection | New factory pattern (e.g., `ShuffleManagerFactory.create`) | Adding a factory pattern would require modifying `SparkEnv.create` and the boot path — disallowed by the user directive "Make only changes necessary to implement streaming shuffle capability within `ShuffleManager` abstraction boundary" (AAP §0.7.1). Reusing the existing reflective instantiation is purely additive. | None — reflection contract is stable and tested for the existing `SortShuffleManager`. |
| 8 | Emit metrics via `MetricsSystem` `Source` | Extend the typed `ExecutorMetrics` array | Extending `ExecutorMetrics` would touch `core/src/main/scala/org/apache/spark/executor/ExecutorMetrics.scala` — a cross-cutting change that violates the "zero cross-contamination" directive (AAP §0.1.2). The `MetricsSystem` `Source` pattern is purely additive: a new `StreamingShuffleSource` class registers in one location without altering the existing typed-metric array schema. | Low — `MetricsSystem` is the established Dropwizard registration surface; new sources appear automatically in JMX/CSV/Slf4j sinks. |

### Decision Detail Notes

The following supplementary notes expand on the table rows where a single sentence is insufficient to capture the design intent. They are intentionally non-normative — the table above remains the canonical source of truth.

**Decision 1 — Why `ShuffleManager` and not `ShuffleDataIO`.** The `ShuffleDataIO` SPI exists to plug in alternative storage backends (e.g., local disk, remote object store) for the existing sort-based shuffle layout. It does not control how blocks are produced or consumed; it only controls where they are persisted. Streaming shuffle changes the producer/consumer dataflow itself — pipelining instead of barrier-synchronizing — which is fundamentally a `ShuffleManager`-level concern. Selecting `ShuffleDataIO` would have required modifying the existing `SortShuffleManager` to differentiate streaming from sort dataflow, violating the "zero cross-contamination" directive.

**Decision 2 — Why per-instance and not per-executor token buckets.** Two shuffles running on the same executor with the same priority configuration receive equal share of the `maxBandwidthMBps` budget. Two shuffles with different priorities receive proportionally different shares. A global executor-level bucket cannot enforce different rates per shuffle without an out-of-band priority controller, which would itself require a new component beyond the user-specified five.

**Decision 3 — Why CRC32C is sufficient.** CRC32C is a non-cryptographic integrity check, suitable for detecting transmission errors and bit-flips in network or disk media. It is **not** suitable for adversarial tampering detection. The user-specified threat model is integrity validation against incidental corruption (network noise, GC-induced bit corruption, transient I/O errors), not malicious modification, so CRC32C is appropriate.

**Decision 4 — Why polling and not event subscription.** The `MemoryManager` interface does not expose a public listener API in the version of Spark targeted by this feature. Adding such an API would itself be a cross-cutting change. A 100 ms polling timer in a single daemon thread is the minimum-modification path that delivers the required SLA.

**Decision 5 — Why Guava's `CacheBuilder` for LRU.** The standard library `LinkedHashMap` with `accessOrder=true` provides LRU semantics, but does not provide thread safety or built-in eviction-statistics. A custom wrapper would duplicate logic that Guava already provides at production-grade quality. The `core` module's parent POM transitively brings Guava onto the classpath, so no dependency change is required.

**Decision 6 — Why composition over re-instantiation.** Composition preserves the singleton invariant Spark relies on for `SparkEnv._shuffleManager`. It also eliminates a class of subtle bugs where two `SortShuffleManager` instances on the same executor would each register their own `ShuffleBlockResolver` in the `BlockManager`, causing block-ID collisions on fallback.

**Decision 7 — Why reuse the reflection path.** The existing `Utils.instantiateSerializerOrShuffleManager` machinery already validates the constructor signature, handles `ClassNotFoundException`, surfaces meaningful error messages, and is exercised by existing tests for the `SortShuffleManager` and `tungsten-sort` aliases. Reusing it for `streaming` is the literal embodiment of the "minimum modification" directive.

**Decision 8 — Why a `Source` and not a typed-metrics array entry.** The typed `ExecutorMetrics` array is a fixed-shape struct intended for memory and GC metrics that every executor reports on every heartbeat. Streaming-shuffle metrics are sparse — they are only meaningful when streaming is enabled, and many executors will never produce non-zero values. A `Source` is a better fit because Dropwizard sinks gracefully handle sources that emit infrequently.

## Bidirectional Traceability Matrix

Every user-prompt requirement maps to one or more implementing source file(s) below. This matrix has 100% coverage of the user prompt with no gaps. Reverse traversal (source file → requirement) is implicit — every source file in `core/src/main/scala/org/apache/spark/shuffle/streaming/` exists to satisfy at least one requirement listed here.

The matrix is organized by user-prompt requirement. Each row names the requirement, cites the AAP section(s) where the requirement is recorded in detail, and lists the source file(s) where the requirement is implemented. Test files are listed alongside production files for requirements that include explicit verification criteria (e.g., performance thresholds, failure scenarios).

| # | User-Prompt Requirement | AAP Reference | Implementing Source File(s) |
|---|-------------------------|---------------|------------------------------|
| 1 | Streaming Shuffle Manager registered as opt-in alias `streaming` | AAP §0.1.3 / §0.5.1.1 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` (new), `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` (modified) |
| 2 | `StreamingShuffleHandle` carries buffer-size, spill-threshold, max-bandwidth metadata | AAP §0.1.3 / §0.5.1.1 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` (new) |
| 3 | `StreamingShuffleWriter` with buffer allocation, partition memory tracking, spill at 80%, CRC32C | AAP §0.5.1.2 / §0.7.2.2 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` (new) |
| 4 | `StreamingShuffleReader` with in-progress block requests, partial-read invalidation, `FetchFailedException` | AAP §0.5.1.2 / §0.4.3.1 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` (new) |
| 5 | `BackpressureProtocol` with heartbeat, token-bucket, priority arbitration | AAP §0.5.1.3 / §0.7.2.3 | `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` (new) |
| 6 | `MemorySpillManager` with 100 ms polling, LRU eviction, spill via `BlockManager.putBytes`, reclamation within 100 ms | AAP §0.5.1.3 / §0.7.2.2 | `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` (new) |
| 7 | Five configuration keys with ranges (`enabled`, `bufferSizePercent` 1–50, `spillThreshold` 50–95, `maxBandwidthMBps`, `debug`) | AAP §0.3.1.3 / §0.5.1.5 / §0.7.3.6 | `core/src/main/scala/org/apache/spark/internal/config/package.scala` (modified) |
| 8 | Four telemetry metrics under `shuffle.streaming.*` (bufferUtilizationPercent, spillCount, backpressureEvents, partialReadInvalidations) | AAP §0.5.1.4 / §0.7.4 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` (new), `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleSource.scala` (new) |
| 9 | Fallback policy with four conditions (slow consumer >60s, memory pressure, network saturation >90%, version mismatch) | AAP §0.5.1.1 / §0.1.2 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala` (new) |
| 10 | Coexistence with `SortShuffleManager` — held privately as fallback collaborator | AAP §0.1.2 / §0.7.2.1 | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` (new), `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala` (held by composition, NOT modified) |
| 11 | Failure handling for all 10 enumerated failure scenarios | AAP §0.1.2 / §0.5.1.6 | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFailureInjectionSuite.scala` (new) |
| 12 | Quality gate: >85% unit test coverage | AAP §0.7.2.6 | All `*Suite.scala` files under `core/src/test/scala/org/apache/spark/shuffle/streaming/` (new) |
| 13 | Performance target 30–50% latency reduction validation | AAP §0.1.2 / §0.5.1.6 | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` (new), `core/benchmarks/StreamingShuffleBenchmark-results.txt` (new) |
| 14 | Stress test: 5-minute continuous workload with 10% failure injection | AAP §0.1.2 / §0.5.1.6 | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleStressSuite.scala` (new) |
| 15 | Integration test end-to-end (100 MB, 10 partitions, ≥30% latency reduction) | AAP §0.5.1.6 | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationSuite.scala` (new) |
| 16 | Documentation: feature overview, configuration, architecture, decision log, observability | AAP §0.5.1.7 / §0.7.3.7 / §0.7.5 | `blitzy-docs/streaming-shuffle/index.md`, `configuration.md`, `architecture.md`, `decision-log.md`, `observability.md` (all new — this folder) |
| 17 | Executive presentation (reveal.js) per project rule | AAP §0.7.6 | `blitzy-docs/streaming-shuffle/executive-summary.html` (new — this folder) |
| 18 | Grafana dashboard JSON template | AAP §0.7.4 fifth bullet | `blitzy-docs/streaming-shuffle/dashboard.json` (new — this folder) |
| 19 | Apache 2.0 license header on every new source file | AAP §0.7.3.2 | All new files (Scala, Java, Markdown, HTML — this folder and `core/src/main/scala/org/apache/spark/shuffle/streaming/`) |
| 20 | MiMa binary compatibility preservation | AAP §0.7.3.4 | `project/MimaExcludes.scala` (modified, additive) |
| 21 | Documentation in upstream Jekyll docs | AAP §0.5.1.7 / §0.7.3.6 | `docs/configuration.md` (modified — handled by `docs/` folder agent) |
| 22 | TechDocs navigation entries | AAP §0.5.1.7 | `mkdocs.yml` (modified — handled by root-folder agent) |

### Coverage Verification

The matrix above is verified to satisfy the "100% coverage, no gaps" criterion of AAP §0.7.3.7 by the following audit:

- **User directives (verbatim, AAP §0.7.1)** — five directives. Each directive is satisfied by at least one row above:
  - "Make only changes necessary to implement streaming shuffle capability within `ShuffleManager` abstraction boundary" → rows 1, 7 (existing SPI reuse), 20 (MiMa preservation).
  - "Preserve existing sort-based shuffle as production-stable fallback. Never modify DAG scheduler, task lifecycle, or user-facing APIs" → row 10 (composition over modification).
  - "When implementation choices exist, select approach requiring least modification to executor memory model and network transport layer" → rows 3, 5, 6 (collaborate with `MemoryManager` and reuse `TransportContext`).
  - "Isolate streaming logic in dedicated classes with zero cross-contamination into existing shuffle code paths" → rows 1, 5, 8, 10 (all new code under `org.apache.spark.shuffle.streaming.*`).
  - "Document all integration points with clear comments explaining coexistence strategy" → rows 16, 17, 19 (documentation set).

- **Functional requirements (AAP §0.1.1 enhanced-clarity bullets)** — eleven bullet points. Each is satisfied by at least one row above:
  - Streaming shuffle path / new manager → rows 1, 2.
  - Coexistence guarantee with `SortShuffleManager` → row 10.
  - Zero modification to user-facing APIs → row 7 (factory reuse) plus the explicit out-of-scope documentation.
  - Five new core components → rows 3, 4, 5, 6, 9.
  - Memory management discipline → rows 3, 6.
  - Backpressure protocol → row 5.
  - Failure handling → rows 4, 11.
  - Integrity validation → row 3 (CRC32C in writer), row 4 (verification in reader).
  - Telemetry parity → row 8.
  - Quality gates → rows 11, 12, 13, 14, 15.

- **Configuration requirements (AAP §0.7.2.2 / §0.7.2.5)** — five `spark.shuffle.streaming.*` keys → row 7.
- **Operational requirements (AAP §0.7.4)** — observability, logging, metrics, dashboard → rows 8, 16, 17, 18.
- **Project engineering rules (AAP §0.7.3)** — license headers, style/lint, MiMa, configuration registry, decision log → rows 19, 20, plus this document.

No user-prompt clause remains unmapped. Conversely, every row above traces back to at least one user-prompt clause (no over-engineering / scope creep).

### Traceability Detail Notes

The following per-row notes expand on the most operationally significant matrix entries. They clarify the contract between the user-prompt requirement and the implementing source file(s), and identify the test file that verifies each contract.

**Row 1 — Streaming Shuffle Manager registration.** Two files cooperate to satisfy this requirement: the new `StreamingShuffleManager.scala` provides the manager implementation, and the modified `ShuffleManager.scala` registers the short-name `"streaming"` in the `shortShuffleMgrNames` map. The cooperation is asymmetric — `ShuffleManager.scala` references `StreamingShuffleManager` only by FQCN string (`classOf[…].getName`), so there is no compile-time dependency from the existing SPI on the new package. Verified by `StreamingShuffleManagerSuite`.

**Row 3 — Streaming Writer with CRC32C and spill at 80%.** The single file `StreamingShuffleWriter.scala` carries three sub-concerns: buffer allocation via `TaskMemoryManager`, CRC32C computation per block, and spill triggering at the configurable `spillThreshold`. The writer collaborates with `MemorySpillManager` (row 6) and `BackpressureProtocol` (row 5) but does not own their state. Verified by `StreamingShuffleWriterSuite`.

**Row 4 — Streaming Reader with `FetchFailedException` propagation.** The reader detects producer connection timeout (5 seconds) by polling for in-progress blocks. On timeout, it discards all partial data from the failed producer and throws the existing `FetchFailedException`, which the upstream `DAGScheduler.handleTaskCompletion` consumes to drive upstream-stage recomputation. No new exception type is introduced. Verified by `StreamingShuffleReaderSuite`.

**Row 6 — Memory spill manager with 100 ms reclamation.** The `MemorySpillManager` uses a `ScheduledExecutorService` with a fixed 100 ms tick. On each tick, it polls the `MemoryManager` for current utilization, evicts the LRU partition if utilization ≥ `spillThreshold`, and persists the evicted bytes via `BlockManager.putBytes` with `StorageLevel.DISK_ONLY`. Memory release on consumer acknowledgment also runs on this same scheduler, ensuring the 100 ms SLA is met in both directions. Verified by `MemorySpillManagerSuite`.

**Row 8 — Four telemetry metrics.** The four metric names are user-mandated verbatim: `shuffle.streaming.bufferUtilizationPercent`, `shuffle.streaming.spillCount`, `shuffle.streaming.backpressureEvents`, `shuffle.streaming.partialReadInvalidations`. The first is a `Gauge[Int]`, the remaining three are `Counter`s. They are emitted via the standard `MetricsSystem` pipeline so that the JMX, CSV, Slf4j, and Prometheus sinks pick them up automatically — no sink-specific configuration is required.

**Row 10 — Coexistence with `SortShuffleManager`.** The `StreamingShuffleManager` constructor instantiates a private `SortShuffleManager` field at construction time, using the same reflective machinery that `SparkEnv.create` would use. When `StreamingShuffleFallbackPolicy.shouldFallback` returns `true` for a given handle, the manager delegates `getWriter` and `getReader` calls to the held `SortShuffleManager`. The fallback decision is per-shuffle, so a single executor may run some shuffles via the streaming path and other shuffles via the sort path concurrently.

**Row 11 — Failure-injection coverage.** The `StreamingShuffleFailureInjectionSuite` contains one named test method per enumerated failure scenario: producer crash, consumer crash, network partition, memory exhaustion, disk failure, checksum mismatch, connection timeout, executor JVM pause, multiple concurrent producer failures, and consumer reconnect after extended downtime. Each test asserts zero data loss against a known-good reference output computed by the sort-based path.

**Row 13 — Performance benchmark.** The benchmark uses `BenchmarkBase` and `runBenchmark("StreamingShuffleVsSort")` to compare sort and streaming on identical input (100 MB synthetic dataset, 10 partitions, `groupByKey` pattern). The committed golden file `core/benchmarks/StreamingShuffleBenchmark-results.txt` is regenerable via `SPARK_GENERATE_BENCHMARK_FILES=1`, ensuring the benchmark numbers can be reproduced on the same hardware/JVM combination.

## Deviations from Literal User Prompt

Per AAP §0.7.3.7: "Any deviation from a literal interpretation of the user's requirements MUST have an explicit entry in the decision log." The list below captures every deliberate deviation, with rationale.

- **No deviations.** Every user-prompt requirement is implemented as literally specified, with the chosen approach captured in the Decision Log table above. The five non-mandatory decisions (decision rows 1, 6, 7, 8 plus the Guava choice in row 5) reflect implementation-strategy choices that satisfy the literal requirements without altering their semantics. Decisions 2, 3, and 4 record the *means* by which the user's literal requirements ("priority arbitration across concurrent shuffles", "Checksum algorithm: CRC32C", "Memory release MUST occur within 100 ms") are met — not deviations from them.

### Deviation-Adjacent Clarifications

The following items are **not** deviations, but warrant explicit clarification because a casual reader might mistake them for one:

- **Five core components vs. user-listed "five new core components".** The user prompt enumerates `StreamingShuffleManager`, `StreamingShuffleWriter`, `BackpressureProtocol`, `StreamingShuffleReader`, and `MemorySpillManager`. The implementation adds two supporting classes: `StreamingShuffleHandle` (extends `BaseShuffleHandle`, required by the `ShuffleManager` SPI to carry per-shuffle metadata) and `StreamingShuffleFallbackPolicy` (implements the four fallback conditions). Both are mechanical consequences of the user's other directives — fallback conditions and SPI compliance — and were named in the AAP §0.5.1.1 expansion of the user prompt. They are not deviations; they are the minimum surface required to deliver the five named components.
- **`StreamingShuffleSource` and `package.scala` (package object).** Two additional Scala source files exist under `core/src/main/scala/org/apache/spark/shuffle/streaming/` for purely housekeeping reasons: `StreamingShuffleSource` is the Dropwizard `Source` adapter that registers `StreamingShuffleMetrics` with the `MetricsSystem` (a one-line bridge), and `package.scala` holds shared constants (block size, timeout durations, polling interval, checksum algorithm name) so that no constant is duplicated across multiple files. These are mechanical scaffolding and do not represent additional user-visible features.
- **Test file count of ten.** The user prompt does not enumerate test files; it specifies coverage criteria (>85%) and lists scenarios. The ten test files (`*Suite.scala` and `*Benchmark.scala`) are organized one-per-component plus integration, failure-injection, stress, and benchmark suites — the minimum granularity to maintain readability while achieving coverage. This is implementation strategy, not a deviation.

## Document History

| Date | Change | Author |
|------|--------|--------|
| Initial commit | Created the decision log with 8 decision rows and 22 traceability rows; supplementary notes section added; coverage verification section added. | Streaming Shuffle Feature Team |

When the streaming-shuffle feature is revised in a future Spark release, new rows MUST be appended (never overwritten) to preserve the decision history. Each new row should follow the same four-column structure for the Decision Log and three-column structure for the Traceability Matrix.

## Glossary

The following terms are used throughout this decision log and the rest of the streaming-shuffle documentation. The glossary is descriptive — it does not introduce new concepts, only points readers at the canonical definition.

- **AAP** — Agent Action Plan. The comprehensive specification authored at the start of the streaming-shuffle implementation. Cited throughout as `AAP §X.Y.Z`.
- **Backpressure** — flow control mechanism that signals upstream producers to slow down when downstream consumers cannot keep up. In streaming shuffle, implemented as a heartbeat-based protocol with token-bucket rate limiting per `BackpressureProtocol` instance.
- **CRC32C** — Cyclic Redundancy Check using the Castagnoli polynomial (0x1EDC6F41). Provides 32-bit non-cryptographic integrity validation. Available in JDK 17 as `java.util.zip.CRC32C`.
- **Fallback** — Transparent delegation from `StreamingShuffleManager` to the held private `SortShuffleManager` when one of four conditions is met (consumer-2x-slow, memory pressure, network saturation, version mismatch). Fallback is per-shuffle, not global.
- **Handle** — A serializable token (`StreamingShuffleHandle`, extending `BaseShuffleHandle`) created by `registerShuffle` and consumed by `getWriter` / `getReader`. Carries per-shuffle metadata (buffer size, spill threshold, max bandwidth) needed by the writer and reader.
- **LRU** — Least Recently Used eviction policy. Applied by `MemorySpillManager` to select which partition buffer to spill to disk when memory pressure exceeds the threshold. Implemented via Guava `CacheBuilder`.
- **MDC** — Mapped Diagnostic Context. SLF4J facility for adding correlation IDs (shuffle ID, map ID, reduce-partition range) to every log line emitted from the streaming-shuffle path.
- **MetricsSystem** — Spark's Dropwizard-based metrics registration and emission subsystem. Streaming-shuffle metrics register via a new `StreamingShuffleSource`; emission flows automatically to JMX, CSV, Slf4j, and Prometheus sinks.
- **MiMa** — Migration Manager for Scala. Tool that detects binary incompatibilities between Scala library versions. New public symbols may need exclusion entries in `project/MimaExcludes.scala`.
- **SPI** — Service Provider Interface. The `ShuffleManager` trait is the SPI extension point; implementations are loaded reflectively via `Utils.instantiateSerializerOrShuffleManager`.
- **Spill** — Persisting an in-memory partition buffer to local disk via `BlockManager.putBytes` with `StorageLevel.DISK_ONLY`. Triggered when buffer utilization ≥ `spillThreshold` (default 80%).
- **Token Bucket** — Rate-limiting algorithm where tokens accumulate in a bucket at a fixed refill rate, and each transmission consumes one token. Enforces a long-term average rate while allowing short bursts up to bucket capacity.

## See Also

- [Feature overview](index.md)
- [Configuration reference](configuration.md)
- [Architecture diagrams](architecture.md)
- [Observability](observability.md)
- [Executive summary slide deck](executive-summary.html)
