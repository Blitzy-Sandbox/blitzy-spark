# Streaming Shuffle — Bidirectional Traceability Matrix

## Purpose

This matrix provides **100% coverage** mapping between user-stated requirements
and implementing code artifacts for the Streaming Shuffle feature added to
Apache Spark 4.2.0-SNAPSHOT. It satisfies the Explainability Rule
(AAP §0.7.7) and the non-negotiable invariant in AAP §0.7.8
("The traceability matrix achieves 100% coverage").

- **Forward matrix**: User requirement → Implementing class / method / test.
- **Reverse matrix**: Implementation class / method → Requirement IDs satisfied.

Both matrices are expressed as GitHub-flavored Markdown tables. Each cell is
independently populated; there are no merged cells and no empty cells. Where a
requirement is satisfied by preservation rather than by new implementation
(AP-1 through AP-8), the row explicitly records `(preservation — no new
implementation)` with validation pointing at MiMa and the pre-existing test
suites that must continue to pass.

Sibling documents:

- [streaming-shuffle.md](./streaming-shuffle.md) — architectural reference with
  coexistence topology, runtime wiring, failure-handling flows.
- [streaming-shuffle-decision-log.md](./streaming-shuffle-decision-log.md) —
  rationale and alternatives for every non-trivial decision.
- [streaming-shuffle-dashboard-template.json](./streaming-shuffle-dashboard-template.json) —
  Grafana dashboard template for the four `shuffle.streaming.*` metrics.
- [streaming-shuffle-executive-summary.html](./streaming-shuffle-executive-summary.html) —
  reveal.js executive summary.

## Coverage Summary

| Requirement Category                                     | Total | Mapped | Coverage |
|----------------------------------------------------------|-------|--------|----------|
| Success Criteria (SC)                                    | 5     | 5      | 100%     |
| Component Responsibilities — StreamingShuffleManager (CR-M)   | 4 | 4 | 100% |
| Component Responsibilities — StreamingShuffleWriter (CR-W)    | 5 | 5 | 100% |
| Component Responsibilities — BackpressureProtocol (CR-B)      | 5 | 5 | 100% |
| Component Responsibilities — StreamingShuffleReader (CR-R)    | 4 | 4 | 100% |
| Component Responsibilities — MemorySpillManager (CR-S)        | 5 | 5 | 100% |
| Implementation Constraints (IC)                          | 17    | 17     | 100%     |
| Absolute Preservation Invariants (AP)                    | 8     | 8      | 100%     |
| Implementation Discipline Directives (ID)                | 5     | 5      | 100%     |
| Failure Handling — Producer Flow (FH-P)                  | 5     | 5      | 100%     |
| Failure Handling — Consumer Flow (FH-C)                  | 5     | 5      | 100%     |
| Fallback Conditions (FB)                                 | 4     | 4      | 100%     |
| **Total Requirements**                                   | **72** | **72** | **100%** |

### Requirement-ID Namespace

| Prefix | Category                                       | Source                  |
|--------|------------------------------------------------|-------------------------|
| SC     | Success Criteria                               | AAP §0.1.1              |
| CR-M   | Component Responsibility — StreamingShuffleManager    | AAP §0.1.1       |
| CR-W   | Component Responsibility — StreamingShuffleWriter     | AAP §0.1.1       |
| CR-B   | Component Responsibility — BackpressureProtocol       | AAP §0.1.1       |
| CR-R   | Component Responsibility — StreamingShuffleReader     | AAP §0.1.1       |
| CR-S   | Component Responsibility — MemorySpillManager         | AAP §0.1.1       |
| IC     | Implementation Constraint (verbatim)           | AAP §0.1.2              |
| AP     | Absolute Preservation Invariant                | AAP §0.1.2              |
| ID     | Implementation Discipline Directive            | AAP §0.1.2              |
| FH-P   | Failure Handling — Producer Failure Flow Step  | AAP §0.1.2              |
| FH-C   | Failure Handling — Consumer Failure Flow Step  | AAP §0.1.2              |
| FB     | Automatic Fallback Condition                   | AAP §0.1.2              |

## Forward Matrix (Requirement → Implementation)

Columns:

- **Requirement ID** — stable identifier (see namespace above).
- **User Requirement (verbatim)** — quoted exactly from AAP §0.1.1 / §0.1.2.
- **Implementing Class** — class(es) in `org.apache.spark.shuffle.streaming.*`
  that carry the logic, or `(preservation — no new implementation)` for
  AP-1…AP-8.
- **Implementing Method(s)** — specific method(s) / field(s) / integration
  point(s) where the requirement is realized.
- **Validating Test(s)** — test suite file names referenced by the AAP
  §0.2.3.5 catalogue (T1–T10) plus any complementary existing suites.

### Success Criteria (SC)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| SC-1 | 30-50% end-to-end latency reduction for shuffle-heavy workloads (100MB+ data, 10+ partitions) | `StreamingShuffleWriter`, `StreamingShuffleReader`, `StreamingShuffleTransport` | `write(records)`, `read()`, `sendBlock(target, envelope)`, `openConsumerStream(producer, shuffleId, reduceRange)` | `StreamingShuffleIntegrationTest` (100 MB / 10 partitions / 30% latency scenario), `StreamingShufflePerformanceBenchmark` (baseline sort vs. streaming on `groupByKey`) |
| SC-2 | 5-10% improvement for CPU-bound workloads through reduced scheduler overhead | `StreamingShuffleManager`, `BackpressureProtocol` (lock-free metrics), `StreamingShuffleMetrics` | `getWriter[K,V](handle, mapId, context, metrics)`, `getReader[K,C](handle, startMap, endMap, startPart, endPart, context, metrics)`, `acquirePermission(blockSize)` backed by `AtomicLong.getAndIncrement()` | `StreamingShufflePerformanceBenchmark` (CPU-bound variant), `StreamingShuffleStressSuite` (telemetry-overhead assertion) |
| SC-3 | Zero performance regression for memory-bound workloads (automatic fallback validation) | `StreamingShuffleFallbackPolicy`, `StreamingShuffleManager` | `shouldFallback(shuffleId, dep)`, `registerShuffle(shuffleId, dep)` delegation to held `SortShuffleManager` | `StreamingShuffleFallbackPolicySuite`, `StreamingShuffleIntegrationTest` (memory pressure scenario), existing `SortShuffleManagerSuite` (regression parity) |
| SC-4 | Zero data loss under all failure scenarios including producer crashes, consumer failures, network partitions | `StreamingShuffleWriter`, `StreamingShuffleReader`, `BackpressureProtocol`, `MemorySpillManager` | `write(records)`, `read()`, `acknowledgeReceipt(blockId, consumerPos)`, `invalidatePartialReads(producerId)`, `retransmit(blockIds)` | `StreamingShuffleFailureInjectionSuite` (all 10 user-specified scenarios), `StreamingShuffleStressSuite` (5-minute continuous workload) |
| SC-5 | Memory exhaustion prevention through 80% threshold spill trigger with <100ms response time | `MemorySpillManager` | `pollMemory()` (100 ms `ScheduledExecutorService`), `spillLargestPartition()`, `reclaimMemory(blockId)` | `MemorySpillManagerSuite` (timing validation), `StreamingShuffleWriterSuite` (spill trigger at 80% threshold) |

### Component Responsibilities — StreamingShuffleManager (CR-M)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| CR-M-1 | Implements `org.apache.spark.shuffle.ShuffleManager` | `StreamingShuffleManager` | All 6 trait methods: `registerShuffle`, `getWriter`, `getReader`, `unregisterShuffle`, `shuffleBlockResolver`, `stop` | `StreamingShuffleManagerSuite` (trait conformance), MiMa gate (public shuffle SPI unchanged) |
| CR-M-2 | Instantiated via `spark.shuffle.manager=streaming` | `ShuffleManager` object (modified), `StreamingShuffleManager` | `shortShuffleMgrNames` map entry `"streaming" -> classOf[StreamingShuffleManager].getName`; reflective `Class.forName(...).getConstructor(classOf[SparkConf]).newInstance(conf)` | `StreamingShuffleManagerSuite` (short-name resolution test + FQCN resolution test) |
| CR-M-3 | Factory returns `StreamingShuffleWriter` and `StreamingShuffleReader` | `StreamingShuffleManager`, `StreamingShuffleHandle` | `getWriter[K,V](...)` returns `new StreamingShuffleWriter`; `getReader[K,C](...)` returns `new StreamingShuffleReader`; dispatch via `StreamingShuffleHandle` type-match | `StreamingShuffleManagerSuite` (factory dispatch tests) |
| CR-M-4 | Coexists with `SortShuffleManager` | `StreamingShuffleManager`, `StreamingShuffleFallbackPolicy` | Held `sortShuffleManager: SortShuffleManager` delegate field; `registerShuffle` routes to delegate when `shouldFallback()` returns true | `StreamingShuffleManagerSuite` (fallback delegation), `StreamingShuffleFallbackPolicySuite`, existing `SortShuffleManagerSuite` (parity) |

### Component Responsibilities — StreamingShuffleWriter (CR-W)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| CR-W-1 | Per-partition memory buffers bounded to 20% executor memory | `StreamingShuffleWriter`, `MemorySpillManager` | `allocatePartitionBuffer(reduceId)`, `computeBufferSize()` → `(executorMemory × bufferSizePercent) / numPartitions` (user-specified formula) | `StreamingShuffleWriterSuite` (buffer allocation and partition-level memory tracking) |
| CR-W-2 | Direct network pipelining to consumers | `StreamingShuffleWriter`, `StreamingShuffleTransport`, `StreamingBlockEnvelope` | `write(records)` enqueues envelopes; `sendBlock(BlockManagerId, env)` flushes via `ChannelHandlerContext.writeAndFlush` | `StreamingShuffleWriterSuite` (pipelining validation), `StreamingShuffleIntegrationTest` |
| CR-W-3 | Spill at 80% buffer threshold | `StreamingShuffleWriter`, `MemorySpillManager` | `checkBufferThreshold()` (comparing to `spillThreshold`), `triggerSpill()` invokes `MemorySpillManager.spillLargestPartition()` | `StreamingShuffleWriterSuite` (spill trigger at 80% threshold with timing validation), `MemorySpillManagerSuite` |
| CR-W-4 | Integrates with block manager for disk persistence | `StreamingShuffleWriter`, `MemorySpillManager` | `BlockManager.putBytes(ShuffleBlockId(shuffleId, mapId, reduceId), bytes, StorageLevel.DISK_ONLY)` delegation on spill | `MemorySpillManagerSuite` (block manager integration), `StreamingShuffleWriterSuite` (spill-to-disk test) |
| CR-W-5 | Generates block-level checksums | `StreamingShuffleWriter`, `StreamingBlockEnvelope` | `computeChecksum(payload)` via `java.util.zip.CRC32C`; envelope's `checksum: Int` field populated per block | `StreamingShuffleWriterSuite` (CRC32C checksum generation for integrity validation), `StreamingShuffleReaderSuite` (end-to-end validation) |

### Component Responsibilities — BackpressureProtocol (CR-B)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| CR-B-1 | Heartbeat-based consumer→producer signaling with 5-second timeout | `BackpressureProtocol`, `BackpressureRpcEndpoint` | `registerProducer(producerId)`, `unregisterProducer(producerId)`, scheduled heartbeat timer (5 s), `HeartbeatMessage` handler in RPC endpoint, `TimeoutMessage` on expiry | `BackpressureProtocolSuite` (timeout detection and failure signaling) |
| CR-B-2 | Per-executor token-bucket rate limiting capped at 80% link capacity | `BackpressureProtocol`, `TokenBucketRateLimiter` | `acquirePermission(blockSize)`, `setRate(maxBandwidthMBps * 1024 * 1024 / numConcurrentShuffles)` (user-specified refill formula); 80% cap applied before rate set | `BackpressureProtocolSuite` (rate limiting enforcement via token bucket validation) |
| CR-B-3 | Threshold monitoring | `BackpressureProtocol`, `MemorySpillManager`, `StreamingShuffleMetrics` | `checkThreshold()` reads buffer utilization from `MemorySpillManager`; emits `BUFFER_UTILIZATION_PERCENT` `LogKey` and `bufferUtilizationPercent` gauge | `BackpressureProtocolSuite`, `MemorySpillManagerSuite` (threshold tests) |
| CR-B-4 | Priority arbitration by partition count and data volume | `BackpressureProtocol` | `arbitratePriority(partitionCount, dataVolume)` returns ordered allocation across concurrent shuffles | `BackpressureProtocolSuite` (priority arbitration under concurrent shuffle load), `StreamingShuffleIntegrationTest` (5-concurrent-shuffle memory pressure scenario) |
| CR-B-5 | Telemetry emission | `BackpressureProtocol`, `StreamingShuffleMetrics` | `emitBackpressureEvent()` invokes `backpressureEvents.inc()` and logs with `BACKPRESSURE_EVENTS` `LogKey` | `BackpressureProtocolSuite` (telemetry counter validation) |

### Component Responsibilities — StreamingShuffleReader (CR-R)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| CR-R-1 | Polls producer for in-progress blocks before shuffle completion | `StreamingShuffleReader`, `StreamingShuffleTransport` | `read()` constructs iterator over `openConsumerStream(producer, shuffleId, reduceRange)`; envelopes yield `Product2[K, C]` lazily | `StreamingShuffleReaderSuite` (in-progress block request and partial data consumption) |
| CR-R-2 | Detects producer failure via connection timeout | `StreamingShuffleReader`, `StreamingShuffleTransport` | Connection-watchdog thread monitoring channel state; `ChannelOption.CONNECT_TIMEOUT_MILLIS = 5000` on client bootstrap | `StreamingShuffleReaderSuite` (producer failure detection via connection timeout), `StreamingShuffleFailureInjectionSuite` (connection-timeout scenario) |
| CR-R-3 | Sends acknowledgment-based buffer reclamation signals | `StreamingShuffleReader`, `BackpressureProtocol`, `BackpressureRpcEndpoint` | `acknowledgeReceipt(blockId, consumerPos)` sends `AcknowledgmentMessage` via RPC; dispatched on `streaming-shuffle-ack-dispatch` executor | `StreamingShuffleReaderSuite`, `BackpressureProtocolSuite` (consumer acknowledgment processing and buffer reclamation) |
| CR-R-4 | Validates checksums and requests retransmission on corruption | `StreamingShuffleReader`, `StreamingBlockEnvelope` | `validateChecksum(envelope)` recomputes CRC32C and compares; `requestRetransmission(blockId)` sends `RetransmitMessage` | `StreamingShuffleReaderSuite` (checksum validation and retransmission request), `StreamingShuffleFailureInjectionSuite` (checksum-mismatch scenario) |

### Component Responsibilities — MemorySpillManager (CR-S)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| CR-S-1 | 100ms polling of memory manager | `MemorySpillManager` | `pollMemory()` scheduled at 100 ms via `ScheduledExecutorService` named `streaming-shuffle-memory-poll`; reads `MemoryManager.executionMemoryUsed(...)` | `MemorySpillManagerSuite` (100 ms polling cadence validation) |
| CR-S-2 | LRU-based eviction of largest buffered partitions at 80% threshold | `MemorySpillManager` | `spillLargestPartition()` selects via LRU-ordered priority queue of partition buffers; invoked when utilization ≥ `spillThreshold` | `MemorySpillManagerSuite` (LRU eviction of largest buffered partition) |
| CR-S-3 | 100ms buffer reclamation after consumer acknowledgment | `MemorySpillManager`, `BackpressureProtocol` | `reclaimMemory(blockId)` within 100 ms of ACK receipt via `MemoryManager.releaseExecutionMemory(numBytes, taskAttemptId, MemoryMode.ON_HEAP)` | `MemorySpillManagerSuite` (100 ms reclamation latency validation) |
| CR-S-4 | Integration with block manager disk storage | `MemorySpillManager` | `BlockManager.putBytes(ShuffleBlockId(shuffleId, mapId, reduceId), bytes, StorageLevel.DISK_ONLY)` | `MemorySpillManagerSuite` (block manager integration) |
| CR-S-5 | Spill metrics tracking | `MemorySpillManager`, `StreamingShuffleMetrics` | `recordSpillMetrics(bytes, latency)` → `spillCount.inc()`, spill latency histogram; `SPILL_COUNT` `LogKey` | `MemorySpillManagerSuite` (spill metrics correctness) |

### Implementation Constraints (IC)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| IC-1 | Streaming buffers limited to 20% executor memory (configurable 1-50%) | `StreamingShuffleWriter`, `MemorySpillManager`, `internal.config.package` | `computeBufferSize()` = `(executorMemory * bufferSizePercent) / numPartitions`; `SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.checkValue(v => v >= 1 && v <= 50, "must be between 1 and 50")`; default 20 | `StreamingShuffleWriterSuite` (buffer sizing), `ConfigEntrySuite` (range validation for `spark.shuffle.streaming.bufferSizePercent`) |
| IC-2 | Spill trigger enforced at 80% utilization (configurable 50-95%) | `MemorySpillManager`, `StreamingShuffleWriter`, `internal.config.package` | `checkBufferThreshold()` compares to `SHUFFLE_STREAMING_SPILL_THRESHOLD`; `.checkValue(v => v >= 50 && v <= 95, "must be between 50 and 95")`; default 80 | `MemorySpillManagerSuite` (80% threshold monitoring), `ConfigEntrySuite` (range validation for `spark.shuffle.streaming.spillThreshold`) |
| IC-3 | Zero memory leaks under failure scenarios (validated via unit test with simulated failure injection) | `StreamingShuffleWriter`, `StreamingShuffleReader`, `MemorySpillManager`, `BackpressureProtocol` | `stop(success=false)` release paths; `cleanup()` in `finally` blocks; `unregisterProducer(producerId)` releases ACK tables | `StreamingShuffleFailureInjectionSuite` (all 10 scenarios with memory-leak assertion), `StreamingShuffleStressSuite` (heap-analysis leak detection post-run) |
| IC-4 | Leverage existing `org.apache.spark.network.TransportContext` for streaming | `StreamingShuffleTransport` | Obtains `TransportContext` via `SparkEnv.get.rpcEnv` and existing `TransportConf`; no new context constructed; `createClientFactory()` / `createServer(...)` re-used | `StreamingShuffleManagerSuite` (transport reuse), `StreamingShuffleIntegrationTest` (end-to-end transport path), existing transport security suites unchanged |
| IC-5 | QoS prioritization: Shuffle traffic priority over speculative task execution | `StreamingShuffleTransport`, `BackpressureProtocol`, `TokenBucketRateLimiter` | `ChannelOption.IP_TOS` set for shuffle channels; `arbitratePriority(partitionCount, dataVolume)` favors shuffle over speculative; `acquirePermission()` gate | `StreamingShuffleIntegrationTest` (5-concurrent-shuffle scenario), `BackpressureProtocolSuite` (priority arbitration) |
| IC-6 | TCP keepalive enabled with 5-second interval for failure detection | `StreamingShuffleTransport` | `ChannelOption.SO_KEEPALIVE = true`; OS-level keepalive interval 5 s (TCP_KEEPIDLE / TCP_KEEPINTVL) | `StreamingShuffleIntegrationTest` (network partition scenario), `StreamingShuffleFailureInjectionSuite` (network-partition test) |
| IC-7 | Block size limited to 2MB for pipelining efficiency | `StreamingBlockEnvelope`, `StreamingShuffleWriter` | `StreamingBlockEnvelope` constructor enforces `require(payload.length <= 2 * 1024 * 1024, "block exceeds 2 MB cap")`; writer chunks payload into ≤ 2 MB blocks | `StreamingShuffleWriterSuite` (block-size boundary test) |
| IC-8 | Connection timeout: 5 seconds for producer failure detection | `StreamingShuffleReader`, `StreamingShuffleTransport` | `ChannelOption.CONNECT_TIMEOUT_MILLIS = 5000` on client bootstrap; connection-watchdog thread enforces | `StreamingShuffleReaderSuite` (producer failure detection via connection timeout) |
| IC-9 | Heartbeat interval: 10 seconds for consumer liveness monitoring | `BackpressureProtocol`, `BackpressureRpcEndpoint` | Scheduled heartbeat at 10-second interval via `ScheduledExecutorService` named `streaming-shuffle-heartbeat`; `HeartbeatMessage` dispatched | `BackpressureProtocolSuite` (heartbeat tests) |
| IC-10 | Checksum algorithm: CRC32C for block integrity validation | `StreamingBlockEnvelope`, `StreamingShuffleWriter`, `StreamingShuffleReader` | `java.util.zip.CRC32C` (JDK 17 built-in) used in `computeChecksum()` / `validateChecksum()`; `checksum: Int` field in envelope | `StreamingShuffleWriterSuite`, `StreamingShuffleReaderSuite`, `StreamingShuffleFailureInjectionSuite` (checksum-mismatch scenario) |
| IC-11 | Retry policy: Exponential backoff starting 1 second, max 5 attempts | `StreamingShuffleReader`, `BackpressureProtocol` | `retryWithExponentialBackoff(initialDelay=1s, maxAttempts=5, factor=2)` wraps reader fetch and retransmission request | `StreamingShuffleFailureInjectionSuite` (retry backoff scenarios) |
| IC-12 | Partial read invalidation: Atomic discard of all blocks from failed producer | `StreamingShuffleReader`, `StreamingShuffleMetrics` | `invalidatePartialReads(producerId)` under `ReentrantLock`; emits `PARTIAL_READ_INVALIDATIONS` `LogKey` and increments `partialReadInvalidations` counter | `StreamingShuffleReaderSuite` (partial read invalidation and upstream recomputation trigger), `StreamingShuffleFailureInjectionSuite` (producer crash scenario) |
| IC-13 | Configuration changes require executor restart (no dynamic reconfiguration in v1) | `StreamingShuffleManager`, `SparkEnv.initializeShuffleManager()` (read-only reference) | Config values captured in immutable fields in constructor; `Preconditions.checkState(null == _shuffleManager)` enforces single-bind at `SparkEnv` | `StreamingShuffleManagerSuite` (immutable-config test asserting post-start mutation has no effect) |
| IC-14 | Telemetry overhead limited to <1% CPU utilization | `StreamingShuffleMetrics`, `BackpressureProtocol`, `MemorySpillManager` | Lock-free `AtomicLong.getAndIncrement()`, `AtomicLongArray`; Dropwizard `Counter`/`Gauge` updates off the hot path | `StreamingShuffleStressSuite` (CPU-utilization measurement with <1% telemetry overhead assertion) |
| IC-15 | Log volume capped at <10MB/hour per executor for streaming events | `StreamingShuffleManager` (logger configuration) | Default `INFO` level for `org.apache.spark.shuffle.streaming`; per-shuffle `TRACE` only when `spark.shuffle.streaming.debug=true`; structured `LogKey` entries dedupe | `StreamingShuffleStressSuite` (log-volume cap validation) |
| IC-16 | JMX metrics exposed for external monitoring integration | `StreamingShuffleMetrics`, `metrics.properties.template` | Dropwizard `Source` registered via `SparkEnv.get.metricsSystem.registerSource(...)`; template enables `*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink` | `StreamingShuffleManagerSuite` (JMX source registration test) |
| IC-17 | Debug logging disabled by default (enable via `spark.shuffle.streaming.debug=true`) | `StreamingShuffleManager`, `internal.config.package` | `SHUFFLE_STREAMING_DEBUG.booleanConf.createWithDefault(false)`; conditional `setLevel(DEBUG)` gated by config | `StreamingShuffleManagerSuite` (debug-flag toggle test), `ConfigEntrySuite` (default-value validation) |

### Absolute Preservation Invariants (AP)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| AP-1 | Zero modification to RDD / DataFrame / Dataset user-facing APIs | (preservation — no new implementation) | (N/A — invariant verification) | MiMa binary compatibility gate (`build/sbt -mem 5632 mimaReportBinaryIssues`); existing `RDDSuite`, `DataFrameSuite`, `DatasetSuite`, `SparkSessionSuite` continue to pass unchanged |
| AP-2 | Zero modification to the DAG scheduler and task-scheduling algorithms | (preservation — no new implementation) | (N/A — invariant verification) | MiMa gate; existing `DAGSchedulerSuite`, `TaskSchedulerImplSuite`, `TaskSetManagerSuite` continue to pass unchanged |
| AP-3 | Zero modification to executor lifecycle management | (preservation — no new implementation) | (N/A — invariant verification) | MiMa gate; existing `CoarseGrainedExecutorBackendSuite`, `ExecutorSuite`, `HeartbeatReceiverSuite` continue to pass unchanged |
| AP-4 | Zero modification to lineage tracking and the fault-recovery model | (preservation — no new implementation) | (N/A — invariant verification) | MiMa gate; existing `DAGSchedulerSuite` lineage tests (FetchFailed recovery, stage resubmission) continue to pass unchanged |
| AP-5 | Zero modification to the existing `SortShuffleManager` implementation | (preservation — no new implementation) | (N/A — invariant verification) | Existing `SortShuffleManagerSuite`, `SortShuffleWriterSuite`, `BypassMergeSortShuffleWriterSuite`, `UnsafeShuffleWriterSuite` continue to pass unchanged; MiMa gate; `git diff` verification in `CODE_REVIEW.md` Phase 3 |
| AP-6 | Zero modification to deployment infrastructure or external dependencies | (preservation — no new implementation) | (N/A — invariant verification) | Zero edits to `.github/workflows/*.yml`, `pom.xml`, `dev/*.sh`, `bin/*.sh`, `sbin/*.sh`, `kubernetes/dockerfiles/*`; `CODE_REVIEW.md` Phase 1 Infrastructure/DevOps audit |
| AP-7 | Zero modification to block-manager storage interface contracts | (preservation — consumed via public API only) | (N/A — invariant verification) | MiMa gate; existing `BlockManagerSuite`, `DiskBlockManagerSuite`, `MemoryStoreSuite`, `DiskStoreSuite` continue to pass unchanged |
| AP-8 | Zero modification to task serialization/deserialization protocols | (preservation — no new implementation) | (N/A — invariant verification) | MiMa gate; existing `TaskSerializerSuite`, `KryoSerializerSuite`, `JavaSerializerSuite`, `TaskResultGetterSuite` continue to pass unchanged |

### Implementation Discipline Directives (ID)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| ID-1 | Make only changes necessary to implement streaming shuffle capability within `ShuffleManager` abstraction boundary | (scope boundary — enforced across codebase review) | (N/A — review-gate verification against AAP §0.6 In-Scope list) | `CODE_REVIEW.md` Phase 3 Backend Architecture; `git diff` audit against AAP §0.6.1 file list; MiMa gate confirming no public SPI changes |
| ID-2 | Preserve existing sort-based shuffle as production-stable fallback. Never modify DAG scheduler, task lifecycle, or user-facing APIs | `StreamingShuffleManager`, `StreamingShuffleFallbackPolicy` | `StreamingShuffleManager.sortShuffleManager: SortShuffleManager` delegate field; `shouldFallback()` redirects `registerShuffle` to delegate | `StreamingShuffleFallbackPolicySuite`, `StreamingShuffleManagerSuite` (fallback delegation), MiMa gate, existing `SortShuffleManagerSuite` unchanged |
| ID-3 | When implementation choices exist, select approach requiring least modification to executor memory model and network transport layer | (architectural discipline — consumer of public API only) | `MemoryManager.acquireExecutionMemory()` / `releaseExecutionMemory()` re-used; `TransportContext` re-used via `SparkEnv` | `streaming-shuffle-decision-log.md` entries (Option A vs. Option B); MiMa gate; zero edits in `core/src/main/scala/org/apache/spark/memory/` or `common/network-common/` (`CODE_REVIEW.md` Phase 3 audit) |
| ID-4 | Isolate streaming logic in dedicated classes with zero cross-contamination into existing shuffle code paths | New sub-package `org.apache.spark.shuffle.streaming.*` | All new classes in isolated package tree; existing `org.apache.spark.shuffle.sort.*` untouched; no imports of `shuffle.streaming.*` from outside the sub-package | `CODE_REVIEW.md` Phase 3; package-isolation grep (`grep -r "org.apache.spark.shuffle.streaming" core/src/main/scala/org/apache/spark/shuffle/sort/`) verifies zero cross-imports |
| ID-5 | Document all integration points with clear comments explaining coexistence strategy | Scaladoc headers on each new class | `StreamingShuffleManager`, `MemorySpillManager`, `BackpressureRpcEndpoint`, `StreamingShuffleFallbackPolicy` Scaladoc with "Coexistence Strategy" notes | `CODE_REVIEW.md` Phase 3 documentation audit; `blitzy-docs/streaming-shuffle.md` cross-references; `scaladoc` build passes |

### Failure Handling — Producer Failure Detection Flow (FH-P)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| FH-P-1 | StreamingShuffleReader detects connection timeout (5 seconds) | `StreamingShuffleReader`, `StreamingShuffleTransport` | Connection-watchdog thread; `ChannelOption.CONNECT_TIMEOUT_MILLIS = 5000`; `TimeoutMessage` dispatched from `BackpressureRpcEndpoint` | `StreamingShuffleReaderSuite` (producer failure detection via connection timeout), `StreamingShuffleFailureInjectionSuite` (producer crash) |
| FH-P-2 | Invalidates all partial reads from failed producer | `StreamingShuffleReader`, `StreamingShuffleMetrics` | `invalidatePartialReads(producerId)` under `ReentrantLock`; emits `PARTIAL_READ_INVALIDATIONS` `LogKey`; increments `partialReadInvalidations` counter | `StreamingShuffleReaderSuite` (partial read invalidation), `StreamingShuffleFailureInjectionSuite` |
| FH-P-3 | Notifies DAG scheduler to recompute upstream tasks | `StreamingShuffleReader` (integration via existing `MapOutputTracker`) | `throw FetchFailedException(...)` — delegates to DAG scheduler's existing FetchFailed recovery path (AP-2 preserved) | `StreamingShuffleFailureInjectionSuite` (producer crash scenario — asserts upstream recompute via existing `DAGSchedulerSuite` pattern) |
| FH-P-4 | Discards buffered data from failed shuffle attempt | `StreamingShuffleReader`, `MemorySpillManager` | `cleanup()` releases partition buffers via `MemoryManager.releaseExecutionMemory()` and purges associated disk spill entries via `BlockManager.removeBlock()` | `StreamingShuffleFailureInjectionSuite` (memory-leak validation on producer failure), `MemorySpillManagerSuite` (cleanup on producer failure) |
| FH-P-5 | Retries read from recomputed producer shuffle | `StreamingShuffleReader`, existing DAG-scheduler re-attempt cycle | Re-invocation of `StreamingShuffleManager.getReader(...)` at new attempt number following DAG scheduler re-dispatch | `StreamingShuffleFailureInjectionSuite` (retry scenario), `StreamingShuffleIntegrationTest` (producer failure mid-shuffle with successful recompute) |

### Failure Handling — Consumer Failure Detection Flow (FH-C)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| FH-C-1 | StreamingShuffleWriter detects missing acknowledgments (10 seconds) | `StreamingShuffleWriter`, `BackpressureProtocol`, `BackpressureRpcEndpoint` | Heartbeat-timer check on pending-ACK map; 10-second window per-block; `TimeoutMessage` emitted on expiry | `BackpressureProtocolSuite` (timeout detection), `StreamingShuffleWriterSuite` (missing-ACK scenario), `StreamingShuffleFailureInjectionSuite` (consumer crash) |
| FH-C-2 | Buffers unacknowledged data in memory | `StreamingShuffleWriter` | Per-partition buffers retained in memory until `acknowledgeReceipt` or spill trigger; `pendingAcks: ConcurrentHashMap[BlockId, ByteBuf]` | `StreamingShuffleWriterSuite` (buffer retention test), `StreamingShuffleFailureInjectionSuite` (consumer crash scenario) |
| FH-C-3 | Triggers disk spill if buffer exceeds 80% threshold | `MemorySpillManager`, `StreamingShuffleWriter` | `checkBufferThreshold()` → `triggerSpill()` → `MemorySpillManager.spillLargestPartition()` when `bufferUtilization >= spillThreshold` | `MemorySpillManagerSuite` (80% threshold), `StreamingShuffleWriterSuite` (spill trigger), `StreamingShuffleIntegrationTest` (consumer slowdown scenario) |
| FH-C-4 | Resumes streaming when consumer reconnects | `StreamingShuffleWriter`, `BackpressureRpcEndpoint` | Reconnect handler on endpoint re-registration; `registerProducer(producerId)` called again on consumer-side `onStart()` | `StreamingShuffleFailureInjectionSuite` (consumer reconnect after extended downtime scenario) |
| FH-C-5 | Retransmits unacknowledged blocks from spill or memory | `StreamingShuffleWriter`, `MemorySpillManager` | `retransmit(blockIds)` reads from `BlockManager.getBytes(ShuffleBlockId)` (if spilled) or in-memory `pendingAcks` map; resends via `StreamingShuffleTransport.sendBlock` | `StreamingShuffleFailureInjectionSuite` (retransmission scenario), `StreamingShuffleReaderSuite` (retransmission request reception) |

### Automatic Fallback Conditions (FB)

| Requirement ID | User Requirement (verbatim) | Implementing Class | Implementing Method(s) | Validating Test(s) |
|----------------|------------------------------|--------------------|------------------------|--------------------|
| FB-1 | Consumer sustained 2x slower than producer for >60 seconds | `StreamingShuffleFallbackPolicy`, `BackpressureProtocol` | `shouldFallback()` evaluates producer/consumer rate ratio over a 60-second sliding window; returns `FallbackReason.ConsumerSlowdown` when ratio ≥ 2.0 | `StreamingShuffleFallbackPolicySuite` (consumer-slowdown condition with deterministic mock timers) |
| FB-2 | Memory pressure prevents buffer allocation (OOM risk) | `StreamingShuffleFallbackPolicy`, `MemorySpillManager` | `shouldFallback()` checks `NettyUtils.freeDirectMemory()` and `MemoryManager` state; returns `FallbackReason.MemoryPressure` when allocation would fail | `StreamingShuffleFallbackPolicySuite` (memory-pressure condition), `StreamingShuffleIntegrationTest` (5-concurrent-shuffle memory pressure scenario) |
| FB-3 | Network saturation exceeds 90% link capacity | `StreamingShuffleFallbackPolicy`, `BackpressureProtocol`, `TokenBucketRateLimiter` | `shouldFallback()` reads token-bucket saturation metric and compares to 90%; returns `FallbackReason.NetworkSaturation` | `StreamingShuffleFallbackPolicySuite` (network-saturation condition), `StreamingShuffleIntegrationTest` (network partition with timeout and fallback scenario) |
| FB-4 | Producer/consumer version mismatch (compatibility check) | `StreamingShuffleFallbackPolicy` | `shouldFallback()` compares `sparkVersion` reported on handshake between executors; returns `FallbackReason.VersionMismatch` on mismatch | `StreamingShuffleFallbackPolicySuite` (version-mismatch condition) |


## Reverse Matrix (Implementation → Requirement)

Columns:

- **Class** — fully qualified or canonical class name in
  `org.apache.spark.shuffle.streaming.*` (or modified existing class).
- **Method** — method / field / integration point within the class.
- **Purpose** — one-sentence summary of what the method does.
- **Requirement IDs Satisfied** — comma-separated list of Forward-matrix IDs
  the method helps realize. Missing an ID here for a method that realizes
  it breaks the 100% coverage invariant.

Class groupings mirror AAP §0.2.3 numbering (N1 … N12) for traceability
back to the new-file catalogue.

### N1 — `StreamingShuffleManager` (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleManager` | `this(conf: SparkConf)` (primary constructor) | Captures immutable config (bufferSizePercent, spillThreshold, maxBandwidthMBps, debug); wires delegate `SortShuffleManager`; configures logger level; obtains `TransportContext` reference | CR-M-2, IC-4, IC-13, IC-15, IC-17 |
| `StreamingShuffleManager` | `registerShuffle[K,V,C](shuffleId: Int, dependency: ShuffleDependency[K,V,C]): ShuffleHandle` | Returns `StreamingShuffleHandle` unless `StreamingShuffleFallbackPolicy.shouldFallback()` triggers, in which case delegates to held `SortShuffleManager` | CR-M-1, CR-M-4, SC-3, FB-1, FB-2, FB-3, FB-4, ID-2 |
| `StreamingShuffleManager` | `getWriter[K,V](handle, mapId, context, metrics)` | Constructs and returns a `StreamingShuffleWriter` bound to the handle's dependency; preserves the `SortShuffleManager` delegation branch when the handle is not a `StreamingShuffleHandle` | CR-M-1, CR-M-3, SC-2 |
| `StreamingShuffleManager` | `getReader[K,C](handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)` | Constructs and returns a `StreamingShuffleReader` for the partition range; delegates to `SortShuffleManager.getReader(...)` when handle is not streaming | CR-M-1, CR-M-3, SC-2 |
| `StreamingShuffleManager` | `unregisterShuffle(shuffleId: Int): Boolean` | Releases per-shuffle buffers, notifies `BackpressureProtocol`, removes from in-memory maps; delegates to `SortShuffleManager` for fallback shuffles | CR-M-1, IC-3 |
| `StreamingShuffleManager` | `shuffleBlockResolver: ShuffleBlockResolver` | Returns a streaming-aware resolver that merges in-memory partition-buffer lookups with on-disk spill lookups delegated to `IndexShuffleBlockResolver` | CR-M-1, CR-S-4 |
| `StreamingShuffleManager` | `stop(): Unit` | Tears down `BackpressureRpcEndpoint`, releases outstanding buffers, stops the polling `ScheduledExecutorService`, idempotent on repeated invocation | CR-M-1, IC-3 |

### N2 — `StreamingShuffleHandle` (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleHandle[K,V]` | `this(shuffleId: Int, dependency: ShuffleDependency[K,V,V])` | Extends `BaseShuffleHandle`; carries dependency metadata; type-match discriminator for dispatch between streaming and sort paths | CR-M-3, CR-M-4 |

### N3 — `StreamingShuffleWriter` (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleWriter[K,V]` | `write(records: Iterator[Product2[K,V]]): Unit` | Per-partition buffer allocation on first write, network pipelining via `StreamingShuffleTransport`, ≤ 2 MB block chunking, CRC32C checksum per envelope, spill trigger on threshold crossing, buffer retention until ACK | CR-W-1, CR-W-2, CR-W-3, CR-W-4, CR-W-5, IC-1, IC-2, IC-7, IC-10, SC-1, FH-C-2 |
| `StreamingShuffleWriter[K,V]` | `stop(success: Boolean): Option[MapStatus]` | Emits `MapStatus` with per-partition byte counts on success; releases all buffers and removes pending-ACK entries on failure | SC-4, IC-3 |
| `StreamingShuffleWriter[K,V]` | `allocatePartitionBuffer(reduceId: Int): ByteBuf` | Lazily allocates a per-partition `ByteBuf` on first record so empty partitions consume zero memory; sized per `computeBufferSize()` | CR-W-1, IC-1 |
| `StreamingShuffleWriter[K,V]` | `computeBufferSize(): Long` | Computes buffer bound as `(executorMemory * bufferSizePercent) / numPartitions` (user-specified formula) | CR-W-1, IC-1 |
| `StreamingShuffleWriter[K,V]` | `checkBufferThreshold(): Boolean` | Reads current buffer utilization and returns true when utilization reaches `spillThreshold` (default 80%) | CR-W-3, IC-2, FH-C-3 |
| `StreamingShuffleWriter[K,V]` | `triggerSpill(): Unit` | Invokes `MemorySpillManager.spillLargestPartition()`; increments `spillCount` counter | CR-W-3, IC-2, FH-C-3 |
| `StreamingShuffleWriter[K,V]` | `computeChecksum(payload: Array[Byte]): Int` | Computes CRC32C via `java.util.zip.CRC32C` over the envelope payload for integrity validation | CR-W-5, IC-10 |
| `StreamingShuffleWriter[K,V]` | `retransmit(blockIds: Seq[BlockId]): Unit` | Re-sends unacknowledged blocks from memory or from `BlockManager.getBytes(...)` on reconnect | FH-C-5 |
| `StreamingShuffleWriter[K,V]` | `missingAckWatchdog` (scheduled timer, 10 s) | Detects missing ACKs within 10-second window; retains unacknowledged data and triggers spill if threshold exceeded | FH-C-1 |

### N4 — `StreamingShuffleReader` (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleReader[K,C]` | `read(): Iterator[Product2[K,C]]` | Iterator-based poll of in-progress envelopes via `StreamingShuffleTransport.openConsumerStream(...)`; per-envelope CRC32C validation; invalidates partial reads on producer timeout; on failure throws `FetchFailedException` to DAG scheduler; retries with exponential backoff | CR-R-1, CR-R-2, CR-R-3, CR-R-4, SC-1, SC-4, IC-11, IC-12, FH-P-1, FH-P-3, FH-P-5 |
| `StreamingShuffleReader[K,C]` | `invalidatePartialReads(producerId: BlockManagerId): Unit` | Atomic discard of all buffered blocks from a failed producer under `ReentrantLock`; logs with `PARTIAL_READ_INVALIDATIONS` `LogKey`; increments `partialReadInvalidations` counter | IC-12, FH-P-2, CR-R-2 |
| `StreamingShuffleReader[K,C]` | `validateChecksum(envelope: StreamingBlockEnvelope): Boolean` | Recomputes CRC32C over the envelope payload and compares against the envelope's checksum field | CR-R-4, IC-10 |
| `StreamingShuffleReader[K,C]` | `requestRetransmission(blockId: BlockId): Unit` | Sends `RetransmitMessage` via `BackpressureProtocol` on checksum mismatch; wrapped in `retryWithExponentialBackoff(initial=1s, max=5)` | CR-R-4, IC-11 |
| `StreamingShuffleReader[K,C]` | `cleanup(): Unit` | Releases per-partition buffers and disk-spill entries on iterator exhaustion or producer failure | FH-P-4, IC-3 |
| `StreamingShuffleReader[K,C]` | `connectionWatchdog` (thread on client bootstrap) | 5-second connection-timeout monitoring; triggers `invalidatePartialReads` on channel inactivity | CR-R-2, IC-6, IC-8, FH-P-1 |

### N5 — `BackpressureProtocol` (`core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `BackpressureProtocol` | `acquirePermission(blockSize: Int): Boolean` | Token-bucket gate for outgoing blocks; delegates to `TokenBucketRateLimiter.tryAcquire(blockSize)`; favors shuffle over speculative via IP_TOS | CR-B-2, IC-5 |
| `BackpressureProtocol` | `acknowledgeReceipt(blockId: BlockId, consumerPos: Long): Unit` | Dispatched on `streaming-shuffle-ack-dispatch` executor; removes from pending-ACK map; triggers `MemorySpillManager.reclaimMemory(blockId)` | CR-B-1, CR-R-3, CR-S-3 |
| `BackpressureProtocol` | `registerProducer(producerId: BlockManagerId): Unit` | Adds producer to heartbeat-tracking map; resets heartbeat timer on reconnect | CR-B-1, FH-C-4 |
| `BackpressureProtocol` | `unregisterProducer(producerId: BlockManagerId): Unit` | Removes producer from heartbeat-tracking map; releases per-producer state and ACK tables | CR-B-1, IC-3 |
| `BackpressureProtocol` | `checkThreshold(): Unit` | Reads buffer utilization from `MemorySpillManager`; emits `BUFFER_UTILIZATION_PERCENT` `LogKey` and `bufferUtilizationPercent` gauge | CR-B-3, CR-S-1 |
| `BackpressureProtocol` | `arbitratePriority(partitionCount: Int, dataVolume: Long): Int` | Returns ordered allocation across concurrent shuffles; favors shuffle traffic over speculative tasks | CR-B-4, IC-5 |
| `BackpressureProtocol` | `emitBackpressureEvent(reason: String): Unit` | Invokes `backpressureEvents.inc()`; logs at INFO with `BACKPRESSURE_EVENTS` `LogKey` | CR-B-5 |
| `BackpressureProtocol` | `heartbeatTimer` (5 s producer timeout, 10 s consumer liveness) | Scheduled via `ScheduledExecutorService` named `streaming-shuffle-heartbeat`; emits `HeartbeatMessage` and detects expiry | CR-B-1, IC-9 |
| `BackpressureProtocol` | `retryWithExponentialBackoff[T](initialDelay: Duration, maxAttempts: Int, factor: Int)(op: => T): T` | Exponential backoff wrapper (initial 1 s, max 5 attempts, factor 2) | IC-11 |

### N6 — `BackpressureRpcEndpoint` (`core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureRpcEndpoint.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `BackpressureRpcEndpoint` | `onStart(): Unit` | Gate: registered only on executors (`SparkEnv.get.executorId != SparkContext.DRIVER_IDENTIFIER`); sets up heartbeat timers | CR-B-1 |
| `BackpressureRpcEndpoint` | `receive: PartialFunction[Any, Unit]` | Handles one-way RPC messages for ACK receipts and backpressure events | CR-B-1, CR-R-3 |
| `BackpressureRpcEndpoint` | `receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]` | Handles request-response RPC for heartbeat, rate-limit negotiation, and retransmission requests | CR-B-1, CR-R-3, CR-B-2 |
| `BackpressureRpcEndpoint` | `HeartbeatMessage` handler | Reply with liveness status; resets per-producer heartbeat window (5 s / 10 s) | CR-B-1, IC-9 |
| `BackpressureRpcEndpoint` | `AcknowledgmentMessage` handler | Invokes `BackpressureProtocol.acknowledgeReceipt(blockId, consumerPos)` | CR-B-1, CR-R-3 |
| `BackpressureRpcEndpoint` | `RateLimitMessage` handler | Updates `TokenBucketRateLimiter.setRate(...)` on consumer signal | CR-B-2 |
| `BackpressureRpcEndpoint` | `TimeoutMessage` handler | Dispatched on heartbeat expiry; triggers producer-side `invalidatePartialReads` and consumer-side missing-ACK workflow | CR-B-1, FH-P-1, FH-C-1 |
| `BackpressureRpcEndpoint` | `RetransmitMessage` handler | Routes retransmission request to `StreamingShuffleWriter.retransmit(blockIds)` | FH-C-5, CR-R-4 |

### N7 — `MemorySpillManager` (`core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `MemorySpillManager` | `pollMemory(): Unit` | Scheduled at 100 ms via `ScheduledExecutorService` named `streaming-shuffle-memory-poll`; lock-free via `AtomicLong` reads; emits `BUFFER_UTILIZATION_PERCENT` | CR-S-1, SC-5, IC-14 |
| `MemorySpillManager` | `spillLargestPartition(): Long` | LRU-ordered priority queue selects the largest buffered partition when utilization ≥ `spillThreshold`; returns bytes spilled | CR-S-2, CR-W-3, IC-2, FH-C-3 |
| `MemorySpillManager` | `reclaimMemory(blockId: BlockId): Unit` | Releases memory within 100 ms of ACK via `MemoryManager.releaseExecutionMemory(numBytes, taskAttemptId, MemoryMode.ON_HEAP)` | CR-S-3, SC-5 |
| `MemorySpillManager` | `persistToDisk(blockId: ShuffleBlockId, bytes: ByteBuffer): Unit` | Delegates to `BlockManager.putBytes(blockId, bytes, StorageLevel.DISK_ONLY)` preserving existing block-manager storage contracts | CR-S-4, CR-W-4 |
| `MemorySpillManager` | `recordSpillMetrics(bytes: Long, latencyMs: Long): Unit` | Increments `spillCount` counter, updates spill-latency histogram; logs with `SPILL_COUNT` `LogKey` | CR-S-5, CR-B-5 |

### N8 — `StreamingShuffleFallbackPolicy` (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleFallbackPolicy.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleFallbackPolicy` | `shouldFallback(shuffleId: Int, dependency: ShuffleDependency[_,_,_]): Option[FallbackReason]` | Evaluates the four user-specified fallback conditions (consumer 2× slower >60s, memory pressure, network saturation >90%, version mismatch); returns `None` to proceed with streaming | SC-3, FB-1, FB-2, FB-3, FB-4, ID-2 |
| `StreamingShuffleFallbackPolicy` | `FallbackReason` enum (`ConsumerSlowdown`, `MemoryPressure`, `NetworkSaturation`, `VersionMismatch`) | Typed fallback reason logged as a structured `reason` field on the `StreamingShuffleFallbackPolicy` logger; surfaces to `CODE_REVIEW.md` audit. The dedicated `LogKey` enum entry for this reason is introduced alongside `StreamingShuffleFallbackPolicy` in a later checkpoint; the four CP1 `LogKey` additions are `BUFFER_UTILIZATION_PERCENT`, `SPILL_COUNT`, `BACKPRESSURE_EVENTS`, and `PARTIAL_READ_INVALIDATIONS` only | ID-5, FB-1, FB-2, FB-3, FB-4 |

### N9 — `StreamingBlockEnvelope` (`core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingBlockEnvelope.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingBlockEnvelope` | `this(shuffleId: Int, mapId: Long, reduceId: Int, sequenceNumber: Long, checksum: Int, payload: Array[Byte])` | Constructor enforces `require(payload.length <= 2 * 1024 * 1024, "block exceeds 2 MB cap")` for pipelining efficiency | IC-7 |
| `StreamingBlockEnvelope` | `toByteBuf(alloc: ByteBufAllocator): ByteBuf` | Serializes envelope to Netty `ByteBuf` with CRC32C-checksummed payload; honors `isNettyOOMOnShuffle` global | IC-7, IC-10, CR-W-2 |
| `StreamingBlockEnvelope` | `fromByteBuf(buf: ByteBuf): StreamingBlockEnvelope` | Deserializes envelope, exposes checksum for reader validation | IC-7, IC-10, CR-R-4 |
| `StreamingBlockEnvelope` | `checksum: Int` (CRC32C) | Per-envelope integrity checksum populated by writer, validated by reader | IC-10, CR-W-5, CR-R-4 |

### N10 — `StreamingShuffleTransport` (`core/src/main/scala/org/apache/spark/shuffle/streaming/network/StreamingShuffleTransport.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleTransport` | `sendBlock(target: BlockManagerId, env: StreamingBlockEnvelope): Future[Unit]` | Rate-limited `ChannelHandlerContext.writeAndFlush(env.toByteBuf(...))` to target executor via shared `TransportClientFactory` | CR-W-2, IC-4, SC-1 |
| `StreamingShuffleTransport` | `openConsumerStream(producer: BlockManagerId, shuffleId: Int, reduceRange: Range): Iterator[StreamingBlockEnvelope]` | Opens streaming consumer channel; yields envelopes lazily via Netty `channelRead0` handler | CR-R-1, IC-4, SC-1 |
| `StreamingShuffleTransport` | `configureBootstrap(bootstrap: Bootstrap): Unit` | Sets `ChannelOption.SO_KEEPALIVE = true` (5-second interval); `ChannelOption.CONNECT_TIMEOUT_MILLIS = 5000`; `ChannelOption.IP_TOS` for QoS | IC-5, IC-6, IC-8, FH-P-1 |

### N11 — `TokenBucketRateLimiter` (`core/src/main/scala/org/apache/spark/shuffle/streaming/network/TokenBucketRateLimiter.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `TokenBucketRateLimiter` | `tryAcquire(blockSize: Int): Boolean` | Thin wrapper around `com.google.common.util.concurrent.RateLimiter.tryAcquire(blockSize)`; enforces 80% link-capacity cap | CR-B-2, IC-5 |
| `TokenBucketRateLimiter` | `setRate(maxBandwidthMBps: Double, numConcurrentShuffles: Int): Unit` | Dynamically adjusts rate via `RateLimiter.setRate(maxBandwidthMBps * 1024 * 1024 / numConcurrentShuffles)` (user-specified refill formula) | CR-B-2 |

### N12 — `StreamingShuffleMetrics` (`core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala`)

| Class | Method | Purpose | Requirement IDs Satisfied |
|-------|--------|---------|---------------------------|
| `StreamingShuffleMetrics` | `this()` | Dropwizard `Source` with `sourceName = "shuffle.streaming"`; registered against `SparkEnv.get.metricsSystem`; surfaced through JMX sink, Prometheus sink, and Graphite sink automatically | IC-16 |
| `StreamingShuffleMetrics` | `bufferUtilizationPercent: Gauge[Double]` | Exposes current buffer utilization across all concurrent streaming shuffles | CR-S-5, CR-B-3 |
| `StreamingShuffleMetrics` | `spillCount: Counter` | Incremented on every spill event by `MemorySpillManager.recordSpillMetrics` | CR-S-5 |
| `StreamingShuffleMetrics` | `backpressureEvents: Counter` | Incremented on every throttle / timeout by `BackpressureProtocol.emitBackpressureEvent` | CR-B-5 |
| `StreamingShuffleMetrics` | `partialReadInvalidations: Counter` | Incremented on every `StreamingShuffleReader.invalidatePartialReads` call | CR-R-2, IC-12, FH-P-2 |

### Modified Existing Files

| Class / File | Method / Location | Purpose | Requirement IDs Satisfied |
|--------------|-------------------|---------|---------------------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | Companion `object ShuffleManager` — `shortShuffleMgrNames` map, append-only | Adds `"streaming" -> classOf[StreamingShuffleManager].getName`; `"sort"` and `"tungsten-sort"` entries preserved unchanged | CR-M-2 |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | `SHUFFLE_STREAMING_ENABLED` `ConfigBuilder` (appended after `SHUFFLE_MANAGER` at line 1748) | `private[spark] val SHUFFLE_STREAMING_ENABLED = ConfigBuilder("spark.shuffle.streaming.enabled").booleanConf.createWithDefault(false)`; opt-in flag | CR-M-2, IC-13 |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | `SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT` `ConfigBuilder` | Integer 1-50, default 20, `.checkValue(v => v >= 1 && v <= 50, ...)`; `version("4.2.0")` | IC-1, CR-W-1 |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | `SHUFFLE_STREAMING_SPILL_THRESHOLD` `ConfigBuilder` | Integer 50-95, default 80, `.checkValue(v => v >= 50 && v <= 95, ...)`; `version("4.2.0")` | IC-2, CR-S-2, CR-W-3 |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | `SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS` `ConfigBuilder` | Integer, default unlimited; consumed by `TokenBucketRateLimiter.setRate` | IC-5, CR-B-2 |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | `SHUFFLE_STREAMING_DEBUG` `ConfigBuilder` | Boolean, default `false`; consumed by `StreamingShuffleManager` constructor to gate DEBUG log level | IC-17 |
| `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java` (per setup note; AAP pointed to `common/utils/src/main/scala/.../LogKey.scala`, but in this repository the enum entries live in `LogKeys.java` — the sibling `LogKey.java` file defines only the base interface) | `BUFFER_UTILIZATION_PERCENT` enum entry | Structured logging key emitted by `BackpressureProtocol.checkThreshold` and `MemorySpillManager.pollMemory` | CR-B-3, CR-S-1 |
| `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java` | `SPILL_COUNT` enum entry | Structured logging key emitted by `MemorySpillManager.recordSpillMetrics` | CR-S-5 |
| `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java` | `BACKPRESSURE_EVENTS` enum entry | Structured logging key emitted by `BackpressureProtocol.emitBackpressureEvent` | CR-B-5 |
| `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java` | `PARTIAL_READ_INVALIDATIONS` enum entry | Structured logging key emitted by `StreamingShuffleReader.invalidatePartialReads` | IC-12, FH-P-2, CR-R-2 |

### Operator-Facing Artifacts (Not Runtime Code)

| Artifact | Location | Purpose | Requirement IDs Satisfied |
|----------|----------|---------|---------------------------|
| `metrics.properties.template` | `core/src/main/resources/org/apache/spark/shuffle/streaming/metrics.properties.template` | Template entries enabling `*.source.shuffle.streaming.class`, `*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink`, and Prometheus servlet sink | IC-16 |
| `streaming-shuffle-dashboard-template.json` | `blitzy-docs/streaming-shuffle-dashboard-template.json` | Grafana dashboard consuming the four `shuffle.streaming.*` metrics (Observability Rule deliverable) | IC-16, CR-B-5, CR-S-5, CR-R-2 |

### Preservation Surfaces (Absolute Invariants)

AP-1 through AP-8 correspond to *absence* of changes; the rows below record
the artifacts and gates that verify this absence.

| Class / Gate | Method / Check | Purpose | Requirement IDs Satisfied |
|--------------|----------------|---------|---------------------------|
| MiMa Binary Compatibility Gate | `build/sbt -mem 5632 mimaReportBinaryIssues` | Reports zero new issues against baseline Spark 4.0.0; fails PR on any new public-SPI signature change | AP-1, AP-2, AP-3, AP-4, AP-5, AP-7, AP-8 |
| Existing RDD/DataFrame/Dataset Suites | `RDDSuite`, `DataFrameSuite`, `DatasetSuite`, `SparkSessionSuite` | Must continue to pass unchanged; any regression signals a violation of AP-1 | AP-1 |
| Existing DAG/Task Scheduler Suites | `DAGSchedulerSuite`, `TaskSchedulerImplSuite`, `TaskSetManagerSuite` | Must continue to pass unchanged; any regression signals a violation of AP-2/AP-4 | AP-2, AP-4 |
| Existing Executor Lifecycle Suites | `CoarseGrainedExecutorBackendSuite`, `ExecutorSuite`, `HeartbeatReceiverSuite` | Must continue to pass unchanged; any regression signals a violation of AP-3 | AP-3 |
| Existing Sort-Path Shuffle Suites | `SortShuffleManagerSuite`, `SortShuffleWriterSuite`, `BypassMergeSortShuffleWriterSuite`, `UnsafeShuffleWriterSuite` | Must continue to pass unchanged; any regression signals a violation of AP-5 | AP-5 |
| Deployment-Infrastructure Git Diff | `git diff` audit of `.github/workflows/*.yml`, `pom.xml`, `dev/*.sh`, `bin/*.sh`, `sbin/*.sh`, `kubernetes/dockerfiles/*` | Expected zero-line diff for this PR; any non-empty diff signals a violation of AP-6 | AP-6 |
| Existing Block-Manager Suites | `BlockManagerSuite`, `DiskBlockManagerSuite`, `MemoryStoreSuite`, `DiskStoreSuite` | Must continue to pass unchanged; any regression signals a violation of AP-7 | AP-7 |
| Existing Serializer Suites | `TaskSerializerSuite`, `KryoSerializerSuite`, `JavaSerializerSuite`, `TaskResultGetterSuite` | Must continue to pass unchanged; any regression signals a violation of AP-8 | AP-8 |

### Implementation Discipline Surfaces

ID-1 through ID-5 are enforced by code-review gates and cross-cutting
artifacts rather than by single runtime methods.

| Class / Gate | Method / Check | Purpose | Requirement IDs Satisfied |
|--------------|----------------|---------|---------------------------|
| `CODE_REVIEW.md` Phase 3 (Backend Architecture) | Scope-boundary audit against AAP §0.6.1 In-Scope list | Confirms only files listed as In-Scope are modified; flags any stray edits | ID-1, ID-4 |
| `StreamingShuffleManager.sortShuffleManager` field | Held `SortShuffleManager` delegate for fallback routing | Preserves sort path as production-stable fallback without modification | ID-2 |
| `streaming-shuffle-decision-log.md` | Option A vs. Option B decisions; CRC32C vs. Murmur3; token-bucket vs. leaky-bucket; RPC heartbeat vs. piggy-back ACK | Records every non-trivial decision and rationale | ID-3, ID-5 |
| Package-isolation verification | `grep -r "org.apache.spark.shuffle.streaming" core/src/main/scala/org/apache/spark/shuffle/sort/` returns zero hits | Confirms zero cross-contamination from streaming into sort paths | ID-4 |
| Scaladoc on new classes | "Coexistence Strategy" section in `StreamingShuffleManager`, `MemorySpillManager`, `BackpressureRpcEndpoint`, `StreamingShuffleFallbackPolicy` | Documents integration points with clear comments | ID-5 |


## Coverage Verification

The four verification checks below are mechanical, reproducible, and
collectively prove the 100% coverage invariant mandated by AAP §0.7.8.

### 1. Every Requirement ID Appears in the Forward Matrix

The Forward matrix section contains one row per Requirement ID in the
namespace (SC, CR-M, CR-W, CR-B, CR-R, CR-S, IC, AP, ID, FH-P, FH-C, FB).
The 72-row total matches the sum of the category counts in the Coverage
Summary table.

### 2. Every Requirement ID Appears in the Reverse Matrix

The table below tokenizes the `Requirement IDs Satisfied` column across all
Reverse matrix rows and asserts at least one row covers each ID. `✓`
indicates the ID is cited by at least one Reverse-matrix row; the
"Representative Row" column names one example class/method that carries
the citation.

| Requirement ID | Covered | Representative Reverse-Matrix Row |
|----------------|---------|-----------------------------------|
| SC-1 | ✓ | `StreamingShuffleWriter.write(records)`; `StreamingShuffleTransport.sendBlock(...)` |
| SC-2 | ✓ | `StreamingShuffleManager.getWriter(...)`; `StreamingShuffleManager.getReader(...)` |
| SC-3 | ✓ | `StreamingShuffleFallbackPolicy.shouldFallback(...)`; `StreamingShuffleManager.registerShuffle(...)` |
| SC-4 | ✓ | `StreamingShuffleWriter.stop(success)`; `StreamingShuffleReader.read()` |
| SC-5 | ✓ | `MemorySpillManager.pollMemory()`; `MemorySpillManager.reclaimMemory(blockId)` |
| CR-M-1 | ✓ | `StreamingShuffleManager.registerShuffle / getWriter / getReader / unregisterShuffle / shuffleBlockResolver / stop` |
| CR-M-2 | ✓ | `StreamingShuffleManager.this(conf)`; `ShuffleManager.shortShuffleMgrNames` (modified) |
| CR-M-3 | ✓ | `StreamingShuffleManager.getWriter / getReader`; `StreamingShuffleHandle` constructor |
| CR-M-4 | ✓ | `StreamingShuffleManager.registerShuffle(...)`; `StreamingShuffleHandle` constructor |
| CR-W-1 | ✓ | `StreamingShuffleWriter.write / allocatePartitionBuffer / computeBufferSize` |
| CR-W-2 | ✓ | `StreamingShuffleWriter.write(...)`; `StreamingShuffleTransport.sendBlock(...)` |
| CR-W-3 | ✓ | `StreamingShuffleWriter.checkBufferThreshold / triggerSpill`; `MemorySpillManager.spillLargestPartition` |
| CR-W-4 | ✓ | `StreamingShuffleWriter.write(...)`; `MemorySpillManager.persistToDisk` |
| CR-W-5 | ✓ | `StreamingShuffleWriter.write / computeChecksum`; `StreamingBlockEnvelope.checksum` |
| CR-B-1 | ✓ | `BackpressureProtocol.acknowledgeReceipt / registerProducer / heartbeatTimer`; `BackpressureRpcEndpoint.HeartbeatMessage / AcknowledgmentMessage / TimeoutMessage` |
| CR-B-2 | ✓ | `BackpressureProtocol.acquirePermission`; `TokenBucketRateLimiter.tryAcquire / setRate`; `BackpressureRpcEndpoint.RateLimitMessage / receiveAndReply` |
| CR-B-3 | ✓ | `BackpressureProtocol.checkThreshold`; `StreamingShuffleMetrics.bufferUtilizationPercent`; `LogKey.BUFFER_UTILIZATION_PERCENT` |
| CR-B-4 | ✓ | `BackpressureProtocol.arbitratePriority(...)` |
| CR-B-5 | ✓ | `BackpressureProtocol.emitBackpressureEvent`; `MemorySpillManager.recordSpillMetrics`; `StreamingShuffleMetrics.backpressureEvents`; `LogKey.BACKPRESSURE_EVENTS` |
| CR-R-1 | ✓ | `StreamingShuffleReader.read()`; `StreamingShuffleTransport.openConsumerStream(...)` |
| CR-R-2 | ✓ | `StreamingShuffleReader.read / invalidatePartialReads / connectionWatchdog`; `StreamingShuffleMetrics.partialReadInvalidations` |
| CR-R-3 | ✓ | `BackpressureProtocol.acknowledgeReceipt`; `BackpressureRpcEndpoint.AcknowledgmentMessage / receive / receiveAndReply / RetransmitMessage` |
| CR-R-4 | ✓ | `StreamingShuffleReader.validateChecksum / requestRetransmission`; `StreamingBlockEnvelope.fromByteBuf / checksum`; `BackpressureRpcEndpoint.RetransmitMessage` |
| CR-S-1 | ✓ | `MemorySpillManager.pollMemory()`; `BackpressureProtocol.checkThreshold`; `LogKey.BUFFER_UTILIZATION_PERCENT` |
| CR-S-2 | ✓ | `MemorySpillManager.spillLargestPartition()`; `SHUFFLE_STREAMING_SPILL_THRESHOLD` config entry |
| CR-S-3 | ✓ | `MemorySpillManager.reclaimMemory(blockId)`; `BackpressureProtocol.acknowledgeReceipt` |
| CR-S-4 | ✓ | `MemorySpillManager.persistToDisk`; `StreamingShuffleManager.shuffleBlockResolver` |
| CR-S-5 | ✓ | `MemorySpillManager.recordSpillMetrics`; `StreamingShuffleMetrics.spillCount / bufferUtilizationPercent`; `LogKey.SPILL_COUNT` |
| IC-1 | ✓ | `StreamingShuffleWriter.computeBufferSize / allocatePartitionBuffer`; `SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT` config entry |
| IC-2 | ✓ | `StreamingShuffleWriter.checkBufferThreshold / triggerSpill`; `MemorySpillManager.spillLargestPartition`; `SHUFFLE_STREAMING_SPILL_THRESHOLD` config entry |
| IC-3 | ✓ | `StreamingShuffleWriter.stop(false)`; `StreamingShuffleReader.cleanup`; `StreamingShuffleManager.unregisterShuffle / stop`; `BackpressureProtocol.unregisterProducer` |
| IC-4 | ✓ | `StreamingShuffleTransport.sendBlock / openConsumerStream`; `StreamingShuffleManager.this(conf)` |
| IC-5 | ✓ | `BackpressureProtocol.acquirePermission / arbitratePriority`; `TokenBucketRateLimiter.tryAcquire`; `StreamingShuffleTransport.configureBootstrap`; `SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS` |
| IC-6 | ✓ | `StreamingShuffleTransport.configureBootstrap`; `StreamingShuffleReader.connectionWatchdog` |
| IC-7 | ✓ | `StreamingBlockEnvelope.this / toByteBuf / fromByteBuf`; `StreamingShuffleWriter.write(...)` |
| IC-8 | ✓ | `StreamingShuffleTransport.configureBootstrap`; `StreamingShuffleReader.connectionWatchdog` |
| IC-9 | ✓ | `BackpressureProtocol.heartbeatTimer`; `BackpressureRpcEndpoint.HeartbeatMessage` |
| IC-10 | ✓ | `StreamingShuffleWriter.computeChecksum`; `StreamingShuffleReader.validateChecksum`; `StreamingBlockEnvelope.toByteBuf / fromByteBuf / checksum` |
| IC-11 | ✓ | `BackpressureProtocol.retryWithExponentialBackoff`; `StreamingShuffleReader.requestRetransmission / read` |
| IC-12 | ✓ | `StreamingShuffleReader.invalidatePartialReads / read`; `StreamingShuffleMetrics.partialReadInvalidations`; `LogKey.PARTIAL_READ_INVALIDATIONS` |
| IC-13 | ✓ | `StreamingShuffleManager.this(conf)`; `SHUFFLE_STREAMING_ENABLED` config entry |
| IC-14 | ✓ | `MemorySpillManager.pollMemory()` (lock-free `AtomicLong`); `StreamingShuffleMetrics` counters |
| IC-15 | ✓ | `StreamingShuffleManager.this(conf)` (logger configuration) |
| IC-16 | ✓ | `StreamingShuffleMetrics.this()`; `metrics.properties.template`; `streaming-shuffle-dashboard-template.json` |
| IC-17 | ✓ | `StreamingShuffleManager.this(conf)`; `SHUFFLE_STREAMING_DEBUG` config entry |
| AP-1 | ✓ | MiMa Gate; Existing RDD/DataFrame/Dataset Suites |
| AP-2 | ✓ | MiMa Gate; Existing DAG/Task Scheduler Suites |
| AP-3 | ✓ | MiMa Gate; Existing Executor Lifecycle Suites |
| AP-4 | ✓ | MiMa Gate; Existing DAG Scheduler Suites (lineage tests) |
| AP-5 | ✓ | MiMa Gate; Existing Sort-Path Shuffle Suites |
| AP-6 | ✓ | Deployment-Infrastructure Git Diff (zero-line change audit) |
| AP-7 | ✓ | MiMa Gate; Existing Block-Manager Suites |
| AP-8 | ✓ | MiMa Gate; Existing Serializer Suites |
| ID-1 | ✓ | `CODE_REVIEW.md` Phase 3 scope-boundary audit |
| ID-2 | ✓ | `StreamingShuffleManager.sortShuffleManager` field; `StreamingShuffleFallbackPolicy.shouldFallback` |
| ID-3 | ✓ | `streaming-shuffle-decision-log.md`; zero edits to `memory/` or `common/network-common/` |
| ID-4 | ✓ | `CODE_REVIEW.md` Phase 3 scope-boundary audit; Package-isolation grep verification |
| ID-5 | ✓ | Scaladoc on new classes; `streaming-shuffle-decision-log.md`; `StreamingShuffleFallbackPolicy.FallbackReason` enum logging |
| FH-P-1 | ✓ | `StreamingShuffleReader.read / connectionWatchdog`; `StreamingShuffleTransport.configureBootstrap`; `BackpressureRpcEndpoint.TimeoutMessage` |
| FH-P-2 | ✓ | `StreamingShuffleReader.invalidatePartialReads`; `StreamingShuffleMetrics.partialReadInvalidations`; `LogKey.PARTIAL_READ_INVALIDATIONS` |
| FH-P-3 | ✓ | `StreamingShuffleReader.read()` (throws `FetchFailedException`) |
| FH-P-4 | ✓ | `StreamingShuffleReader.cleanup()` |
| FH-P-5 | ✓ | `StreamingShuffleReader.read()` (re-invocation path) |
| FH-C-1 | ✓ | `StreamingShuffleWriter.missingAckWatchdog`; `BackpressureRpcEndpoint.TimeoutMessage` |
| FH-C-2 | ✓ | `StreamingShuffleWriter.write(records)` |
| FH-C-3 | ✓ | `StreamingShuffleWriter.checkBufferThreshold / triggerSpill`; `MemorySpillManager.spillLargestPartition` |
| FH-C-4 | ✓ | `BackpressureProtocol.registerProducer` |
| FH-C-5 | ✓ | `StreamingShuffleWriter.retransmit`; `BackpressureRpcEndpoint.RetransmitMessage` |
| FB-1 | ✓ | `StreamingShuffleFallbackPolicy.shouldFallback / FallbackReason.ConsumerSlowdown`; `StreamingShuffleManager.registerShuffle` |
| FB-2 | ✓ | `StreamingShuffleFallbackPolicy.shouldFallback / FallbackReason.MemoryPressure`; `StreamingShuffleManager.registerShuffle` |
| FB-3 | ✓ | `StreamingShuffleFallbackPolicy.shouldFallback / FallbackReason.NetworkSaturation`; `StreamingShuffleManager.registerShuffle` |
| FB-4 | ✓ | `StreamingShuffleFallbackPolicy.shouldFallback / FallbackReason.VersionMismatch`; `StreamingShuffleManager.registerShuffle` |

**Result**: Every one of the 72 Requirement IDs is cited by at least one
Reverse matrix row; the set difference (Forward-matrix IDs − Reverse-matrix
IDs) is empty. **Coverage invariant satisfied.**

### 3. No Orphan Requirements

Formally: `∀ id ∈ ForwardMatrixIDs, id ∈ ReverseMatrixIDs`. This is the
contrapositive restatement of Check 2 above and is validated by the same
73-row coverage table.

### 4. No Orphan Implementations

Every new class listed in AAP §0.2.3.1–0.2.3.4 (N1 through N12) has at
least one row in the Reverse matrix:

| New File ID | Class | Covered in Reverse Matrix |
|-------------|-------|---------------------------|
| N1 | `StreamingShuffleManager` | ✓ (7 method rows) |
| N2 | `StreamingShuffleHandle` | ✓ (constructor row) |
| N3 | `StreamingShuffleWriter` | ✓ (9 method rows) |
| N4 | `StreamingShuffleReader` | ✓ (6 method rows) |
| N5 | `BackpressureProtocol` | ✓ (9 method rows) |
| N6 | `BackpressureRpcEndpoint` | ✓ (8 method rows) |
| N7 | `MemorySpillManager` | ✓ (5 method rows) |
| N8 | `StreamingShuffleFallbackPolicy` | ✓ (2 method rows) |
| N9 | `StreamingBlockEnvelope` | ✓ (4 method rows) |
| N10 | `StreamingShuffleTransport` | ✓ (3 method rows) |
| N11 | `TokenBucketRateLimiter` | ✓ (2 method rows) |
| N12 | `StreamingShuffleMetrics` | ✓ (5 method rows) |

Every modified existing file listed in AAP §0.4.1.1 has at least one row:

| Modified File | Covered in Reverse Matrix |
|---------------|---------------------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | ✓ (shortShuffleMgrNames map row) |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | ✓ (5 ConfigBuilder rows) |
| `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java` | ✓ (4 enum-entry rows) |

**Result**: Every implementation artifact traces back to at least one user
requirement. **No orphan-implementation invariant satisfied.**

## Notes on Coverage

- **The Forward matrix is the "proof" that every user demand is mapped.**
  Every numbered requirement in AAP §0.1.1 (core objectives, success
  criteria, component responsibilities) and §0.1.2 (implementation
  constraints, preservation invariants, failure flows, fallback conditions,
  implementation discipline directives) has a dedicated Forward-matrix row
  with exact quotation of the user's language.
- **The Reverse matrix is the "proof" that every line of new code traces
  back to a user demand.** Every class enumerated in AAP §0.2.3.1–0.2.3.4
  and every modified location described in AAP §0.4.1.1 has one or more
  Reverse-matrix rows naming the specific methods that realize the
  requirements.
- **Preservation invariants (AP-1 through AP-8) are mapped to verification
  gates rather than to new implementation artifacts.** The MiMa gate is
  the primary mechanical check; unchanged existing test suites are the
  secondary check. A single failure of either gate signals a violation of
  the "zero modification" contract.
- **Implementation discipline directives (ID-1 through ID-5) are mapped to
  review gates and cross-cutting artifacts.** The `CODE_REVIEW.md` ledger
  (AAP §0.7.7 Segmented PR Review Rule) and the
  `streaming-shuffle-decision-log.md` are the authoritative audit surfaces
  for these directives.
- **Test-suite references use the exact names from AAP §0.2.3.5.** The
  ten test/benchmark files T1–T10 are cited throughout:
    - T1 `StreamingShuffleManagerSuite`
    - T2 `StreamingShuffleWriterSuite`
    - T3 `BackpressureProtocolSuite`
    - T4 `StreamingShuffleReaderSuite`
    - T5 `MemorySpillManagerSuite`
    - T6 `StreamingShuffleFallbackPolicySuite`
    - T7 `StreamingShuffleIntegrationTest`
    - T8 `StreamingShuffleFailureInjectionSuite`
    - T9 `StreamingShuffleStressSuite`
    - T10 `StreamingShufflePerformanceBenchmark`
- **Consumers of this document**:
  1. The Principal Reviewer in the Segmented PR Review phase
     (AAP §0.7.7) uses this matrix to confirm the implemented code
     matches the AAP.
  2. Future maintainers tracing intent when modifying streaming-shuffle
     code consult this matrix to locate the requirement behind any
     method or test.
  3. Audit trails for compliance / governance reviews use the
     Requirement-ID namespace as the canonical reference point.

## References

- **AAP §0.1.1** — Core feature objective, component responsibilities,
  success criteria.
- **AAP §0.1.2** — Implementation constraints (IC-1 through IC-17),
  preservation invariants (AP-1 through AP-8), failure flows (FH-P-1
  through FH-P-5, FH-C-1 through FH-C-5), fallback conditions (FB-1
  through FB-4), implementation discipline directives (ID-1 through ID-5).
- **AAP §0.2.3** — New file catalogue (N1 through N12 + operator-facing
  artifacts).
- **AAP §0.4.1.1** — Direct modifications to existing files (three files).
- **AAP §0.5.1** — File-by-file execution plan with method-level
  references.
- **AAP §0.7.7** — Explainability Rule (mandates this document) and
  Segmented PR Review Rule (consumes this document).
- **AAP §0.7.8** — Non-negotiable invariant: the traceability matrix
  achieves 100% coverage.
- **Sibling document**:
  [streaming-shuffle.md](./streaming-shuffle.md) — architectural reference.
- **Sibling document**:
  [streaming-shuffle-decision-log.md](./streaming-shuffle-decision-log.md)
  — decision log with alternatives and rationale for every non-trivial
  implementation choice.

