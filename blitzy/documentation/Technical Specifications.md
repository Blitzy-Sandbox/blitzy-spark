# Technical Specification

# 0. Agent Action Plan

## 0.1 Intent Clarification

Based on the prompt, the Blitzy platform understands that the new feature requirement is to implement a **Streaming Shuffle capability for Apache Spark** that fundamentally changes how shuffle data flows between map and reduce tasks by eliminating the traditional full-materialization pattern in favor of an incremental streaming approach with sophisticated backpressure and fault tolerance mechanisms.

### 0.1.1 Core Feature Objectives

The feature introduces the following primary capabilities:

- **StreamingShuffleManager**: A new pluggable `ShuffleManager` implementation that coexists with the existing `SortShuffleManager`, activated via `spark.shuffle.manager=streaming` configuration. This manager orchestrates the complete streaming shuffle lifecycle including writer/reader instantiation, buffer management, and graceful fallback to sort-based shuffle.

- **StreamingShuffleWriter**: A memory-efficient shuffle writer that streams serialized partition data directly to consumer executors rather than materializing complete shuffle files. It manages per-partition buffers constrained to 20% of executor memory, implements CRC32C checksum generation for block integrity, and coordinates with the block manager for disk spill when memory pressure exceeds the 80% threshold.

- **StreamingShuffleReader**: A reduce-side reader capable of consuming in-progress shuffle blocks before the map task completes. It implements partial read invalidation for failed producers, acknowledgment-based buffer reclamation, and checksum validation with retransmission requests on corruption.

- **BackpressureProtocol**: A flow control mechanism using heartbeat-based consumer-to-producer signaling with 5-second timeouts, per-executor rate limiting at 80% link capacity via token bucket algorithm, and priority arbitration for concurrent shuffles based on partition count and data volume.

- **MemorySpillManager**: A component that monitors memory utilization at 100ms intervals, triggers automatic disk spill when buffer utilization exceeds 80%, and implements LRU-based partition eviction with sub-100ms response time for buffer reclamation.

### 0.1.2 Implicit Requirements Detected

Based on the feature specification, the following implicit requirements have been identified:

- **Configuration Keys**: New Spark configuration entries must be added to the internal config package following existing patterns (e.g., `spark.shuffle.streaming.enabled`, `spark.shuffle.streaming.bufferSizePercent`, `spark.shuffle.streaming.spillThreshold`, `spark.shuffle.streaming.maxBandwidthMBps`)

- **Metrics Integration**: The streaming shuffle components must integrate with Spark's existing metrics system via `ShuffleReadMetricsReporter` and `ShuffleWriteMetricsReporter` interfaces, adding new metrics like `shuffle.streaming.bufferUtilizationPercent`, `shuffle.streaming.spillCount`, `shuffle.streaming.backpressureEvents`, and `shuffle.streaming.partialReadInvalidations`

- **Handle Types**: New `ShuffleHandle` subclass(es) for streaming shuffle mode to enable proper writer/reader instantiation based on shuffle registration

- **Network Message Types**: New RPC message types for streaming protocol communication (acknowledgments, backpressure signals, heartbeats)

- **Fallback Logic**: Automatic detection and graceful degradation to `SortShuffleManager` under specified conditions (consumer 2x slower than producer for >60s, OOM risk, network saturation >90%, version mismatch)

### 0.1.3 Feature Dependencies and Prerequisites

- **Existing Infrastructure**: The implementation must leverage Spark's existing `TransportContext` for network streaming, `BlockManager` for disk spill coordination, `MemoryManager` for buffer allocation tracking, and `MapOutputTracker` for shuffle metadata propagation

- **Network Layer Compatibility**: TCP keepalive with 5-second intervals requires integration with existing `NettyBlockTransferService` configuration

- **Serialization Compatibility**: Streaming buffers must work with Spark's existing `SerializerManager` for compression and encryption wrapping

### 0.1.4 Special Instructions and Constraints

**Critical Directives from User Specification:**

- **Absolute Preservation Zone**: Zero modifications to RDD/DataFrame/Dataset APIs, DAG scheduler, task scheduling algorithms, executor lifecycle management, lineage tracking, existing `SortShuffleManager` implementation, deployment infrastructure, block manager storage interface contracts, or task serialization protocols

- **Isolation Requirement**: Streaming logic must be isolated in dedicated classes with zero cross-contamination into existing shuffle code paths

- **Coexistence Strategy**: All integration points must be documented with clear comments explaining how streaming shuffle coexists with sort-based shuffle

- **Minimal Modification Principle**: When implementation choices exist, select the approach requiring least modification to executor memory model and network transport layer

### 0.1.5 Technical Interpretation

These feature requirements translate to the following technical implementation strategy:

- To **implement StreamingShuffleManager**, we will create a new class extending `ShuffleManager` trait in package `org.apache.spark.shuffle.streaming`, register it in the `ShuffleManager` companion object's name mapping, and implement all required trait methods (`registerShuffle`, `getWriter`, `getReader`, `unregisterShuffle`, `shuffleBlockResolver`, `stop`)

- To **implement StreamingShuffleWriter**, we will create a new class extending `ShuffleWriter[K, V]` that buffers records in memory-mapped regions, streams them to consumers via Netty channels, and coordinates with block manager for spill operations when memory thresholds are exceeded

- To **implement BackpressureProtocol**, we will create a companion class with token bucket rate limiting, heartbeat-based liveness detection, and priority queue arbitration for concurrent shuffle memory allocation

- To **implement StreamingShuffleReader**, we will create a new class implementing `ShuffleReader[K, C]` that fetches in-progress blocks, validates checksums, sends acknowledgments, and handles producer failure by invalidating partial reads and notifying the DAG scheduler

- To **implement MemorySpillManager**, we will create a utility class that polls memory manager at 100ms intervals, selects largest partitions for LRU eviction, and coordinates with `DiskBlockManager` for persistence

- To **add configuration support**, we will extend `core/src/main/scala/org/apache/spark/internal/config/package.scala` with new `ConfigEntry` definitions for all streaming shuffle parameters

- To **add metrics telemetry**, we will extend `ShuffleReadMetricsReporter` and `ShuffleWriteMetricsReporter` or create new streaming-specific reporter traits that integrate with Spark's existing metrics system

- To **implement failure tolerance**, we will add connection timeout detection (5s), heartbeat monitoring (10s), CRC32C checksum validation, exponential backoff retry (1s base, 5 max attempts), and atomic partial read invalidation


## 0.2 Repository Scope Discovery

### 0.2.1 Comprehensive File Analysis

The following comprehensive analysis identifies all existing files requiring modification and all new files requiring creation for the streaming shuffle implementation.

#### Existing Modules to Modify

**Core Shuffle Interface Files:**

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | ShuffleManager factory | Add "streaming" alias mapping |
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleHandle.scala` | Base shuffle handle | Reference only (no modification) |
| `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` | Shuffle metrics reporters | Add streaming-specific metrics methods |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Configuration keys | Add streaming shuffle config entries |
| `core/src/main/scala/org/apache/spark/SparkEnv.scala` | Environment initialization | No modification needed (uses factory) |

**Memory Management Integration:**

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/main/scala/org/apache/spark/memory/MemoryManager.scala` | Memory pool management | Reference only for buffer tracking |
| `core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala` | Execution memory | Reference for streaming buffer allocation |
| `core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala` | Unified memory policy | Reference for spill threshold monitoring |

**Storage Layer Integration:**

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/main/scala/org/apache/spark/storage/BlockManager.scala` | Block storage | Reference for spill coordination |
| `core/src/main/scala/org/apache/spark/storage/DiskBlockManager.scala` | Disk persistence | Reference for spill file management |
| `core/src/main/scala/org/apache/spark/storage/BlockId.scala` | Block identifiers | Add StreamingShuffleBlockId variant |
| `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` | Shuffle fetch | Reference only (coexists) |

**Network Transport Layer:**

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala` | Netty transport | Reference for streaming transfer |
| `core/src/main/scala/org/apache/spark/network/netty/NettyBlockRpcServer.scala` | RPC handler | May require streaming protocol handlers |
| `core/src/main/scala/org/apache/spark/network/BlockTransferService.scala` | Transfer abstraction | Reference only |
| `common/network-common/src/main/java/org/apache/spark/network/**/*.java` | Transport infrastructure | Reference only |
| `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/*.java` | Shuffle network | May require streaming message types |

**Executor and Metrics:**

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala` | Write metrics | Add streaming metrics fields |
| `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala` | Read metrics | Add streaming metrics fields |
| `core/src/main/scala/org/apache/spark/InternalAccumulator.scala` | Accumulator keys | Add streaming shuffle accumulator names |

**Test Infrastructure:**

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/test/scala/org/apache/spark/SparkFunSuite.scala` | Test base class | Reference only |
| `core/src/test/scala/org/apache/spark/shuffle/**/*.scala` | Shuffle test suites | Pattern reference for new tests |
| `core/src/test/scala/org/apache/spark/ShuffleSuite.scala` | Shuffle integration | Reference for test patterns |

#### Integration Point Discovery

**API Endpoints Connecting to the Feature:**

- `ShuffleManager.getWriter()` - Called by `ShuffleWriteProcessor` to obtain writer instance
- `ShuffleManager.getReader()` - Called by `SortShuffleManager` pattern to obtain reader instance
- `MapOutputTracker.registerMapOutput()` - Called to register completed shuffle outputs
- `MapOutputTracker.getMapSizesByExecutorId()` - Called by reader to locate shuffle blocks
- `BlockManager.putBlockData()` - Used for spill persistence
- `BlockManager.getLocalBlockData()` - Used for reading spilled blocks

**Database Models/Migrations Affected:**

- None - Spark shuffle does not use persistent database storage

**Service Classes Requiring Updates:**

- `ShuffleWriteProcessor` - May need awareness of streaming mode for push notification
- `ContextCleaner` - May need streaming shuffle cleanup hooks

**Middleware/Interceptors Impacted:**

- `SecurityManager` - Authentication for streaming connections (reference only)
- RPC authentication bootstraps in `NettyBlockTransferService`

### 0.2.2 New File Requirements

**New Source Files to Create:**

| File Path | Purpose |
|-----------|---------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | Main shuffle manager implementing `ShuffleManager` trait |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | Streaming variant of `ShuffleWriter` with buffer management |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | In-progress block reader with partial invalidation |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | Handle type for streaming shuffle registration |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | Flow control with token bucket and heartbeats |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | Threshold monitoring and LRU spill coordination |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | Block resolution for streaming shuffle |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | Streaming-specific metrics source |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | Package-level documentation and utilities |

**New Test Files to Create:**

| File Path | Purpose |
|-----------|---------|
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | Unit tests for buffer allocation, spill trigger, checksums |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | Unit tests for acknowledgments, rate limiting, timeouts |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | Unit tests for partial reads, failure detection, validation |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | Integration scenarios with failure injection |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | Performance benchmarks for latency comparison |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | Manager lifecycle and registration tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | Spill coordination and threshold tests |

**New Configuration Files:**

- No new YAML/JSON configuration files required - all configuration via SparkConf

**New Documentation Files:**

| File Path | Purpose |
|-----------|---------|
| `docs/streaming-shuffle-guide.md` | User guide for streaming shuffle configuration and usage |

### 0.2.3 Web Search Research Conducted

Based on the implementation requirements, the following research areas would benefit implementation:

- **Best practices for implementing flow control in distributed shuffle systems** - Token bucket algorithms, backpressure patterns in stream processing (Flink, Storm)
- **Library recommendations for checksum validation** - CRC32C native implementations in Java/Scala, performance comparison with Adler32/CRC32
- **Common patterns for graceful degradation in shuffle systems** - Fallback trigger heuristics, version compatibility detection
- **Security considerations for streaming block transfer** - Authentication during long-lived streaming connections, encryption at rest vs in-flight for partial blocks

### 0.2.4 Existing Code Patterns to Follow

Based on analysis of the existing `SortShuffleManager` implementation:

```scala
// Pattern from SortShuffleManager for handle registration
override def registerShuffle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
  // StreamingShuffleManager will follow same signature
}
```

```scala
// Pattern from ShuffleWriter for writer lifecycle
def write(records: Iterator[Product2[K, V]]): Unit
def stop(success: Boolean): Option[MapStatus]
```

The streaming implementation will extend these patterns while maintaining API compatibility.


## 0.3 Dependency Inventory

### 0.3.1 Private and Public Packages

The streaming shuffle implementation leverages existing Spark dependencies with no new external library requirements. All functionality is built upon the established dependency stack.

**Core Internal Dependencies (Already in Spark Core):**

| Package Registry | Name | Version | Purpose |
|------------------|------|---------|---------|
| Apache Maven | spark-core_2.13 | 4.2.0-SNAPSHOT | Host module for streaming shuffle |
| Apache Maven | spark-network-common_2.13 | 4.2.0-SNAPSHOT | Netty transport infrastructure |
| Apache Maven | spark-network-shuffle_2.13 | 4.2.0-SNAPSHOT | Shuffle network protocols |
| Apache Maven | spark-unsafe_2.13 | 4.2.0-SNAPSHOT | Memory-efficient buffer operations |
| Apache Maven | spark-kvstore_2.13 | 4.2.0-SNAPSHOT | Local storage abstractions |

**External Dependencies (Pre-existing in spark-core):**

| Package Registry | Name | Version | Purpose for Streaming Shuffle |
|------------------|------|---------|-------------------------------|
| Maven Central | netty-all | 4.1.x (managed) | Network transport for streaming |
| Maven Central | guava | 33.x (shaded) | ConcurrentHashMap, buffer utilities |
| Maven Central | metrics-core | 4.x (managed) | Dropwizard metrics for telemetry |
| Maven Central | slf4j-api | 2.x (managed) | Logging infrastructure |
| Maven Central | kryo-shaded | 4.x (managed) | Serialization for streaming buffers |
| Maven Central | lz4-java | 1.8.x (managed) | Compression for spilled data |
| Maven Central | zstd-jni | 1.5.x (managed) | Alternative compression codec |

**Test Dependencies (Pre-existing):**

| Package Registry | Name | Version | Purpose |
|------------------|------|---------|---------|
| Maven Central | mockito-core | 5.x (managed) | Mocking for unit tests |
| Maven Central | scalatest_2.13 | 3.x (managed) | Test framework |
| Maven Central | byte-buddy | 1.x (managed) | Mockito support |

### 0.3.2 Dependency Updates Required

**No new external dependencies are required.** The streaming shuffle implementation uses exclusively existing Spark infrastructure and Java standard library classes for:

- **CRC32C Checksums**: `java.util.zip.CRC32C` (Java 9+ standard library)
- **Token Bucket Rate Limiting**: Custom implementation using `System.nanoTime()` and atomic operations
- **Heartbeat Scheduling**: `java.util.concurrent.ScheduledExecutorService` (standard library)
- **Buffer Management**: `java.nio.ByteBuffer` and Spark's existing `MemoryManager`
- **Network Transport**: Existing Netty-based `NettyBlockTransferService`

### 0.3.3 Import Updates

**Files Requiring Import Updates:**

The following patterns identify files that will need import statement additions for streaming shuffle classes:

- `core/src/main/scala/org/apache/spark/shuffle/**/*.scala` - Add imports for streaming types
- `core/src/test/scala/org/apache/spark/shuffle/**/*.scala` - Add imports for streaming test fixtures
- `core/src/main/scala/org/apache/spark/internal/config/package.scala` - Add streaming config imports

**Import Transformation Rules:**

For new streaming shuffle components, imports will follow existing patterns:

```scala
// Pattern for shuffle implementations
import org.apache.spark.shuffle.streaming.StreamingShuffleManager
import org.apache.spark.shuffle.streaming.StreamingShuffleWriter
import org.apache.spark.shuffle.streaming.StreamingShuffleReader
```

No existing imports will be modified - only additions for new streaming components.

### 0.3.4 External Reference Updates

**Configuration Files:**

| File Pattern | Update Required |
|--------------|-----------------|
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Add streaming config entries |
| `docs/configuration.md` | Document new streaming shuffle configurations |

**Documentation:**

| File Pattern | Update Required |
|--------------|-----------------|
| `docs/tuning.md` | Add streaming shuffle tuning recommendations |
| `docs/configuration.md` | Add streaming shuffle configuration reference |

**Build Files:**

No POM or build file modifications required - all code lives in existing `spark-core` module.

### 0.3.5 Java/Scala Version Compatibility

The implementation targets:

- **Java Version**: 17 (as specified in `pom.xml` property `java.version`)
- **Scala Version**: 2.13 (as specified in module naming conventions)

Key Java 17+ features utilized:
- `java.util.zip.CRC32C` for hardware-accelerated checksum computation
- `java.lang.ProcessHandle` descendants for resource tracking (existing usage in codebase)
- Enhanced switch expressions and pattern matching (if used)

### 0.3.6 Configuration Key Definitions

The following configuration entries will be added to `core/src/main/scala/org/apache/spark/internal/config/package.scala`:

```scala
// Primary streaming shuffle configuration
private[spark] val SHUFFLE_STREAMING_ENABLED =
  ConfigBuilder("spark.shuffle.streaming.enabled")
    .doc("Enable streaming shuffle for reduced latency...")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(false)

private[spark] val SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT =
  ConfigBuilder("spark.shuffle.streaming.bufferSizePercent")
    .doc("Percent of executor memory for streaming buffers...")
    .version("4.2.0")
    .intConf
    .checkValue(v => v >= 1 && v <= 50, "Must be 1-50")
    .createWithDefault(20)
```

Similar patterns for:
- `spark.shuffle.streaming.spillThreshold` (default: 80)
- `spark.shuffle.streaming.maxBandwidthMBps` (default: unlimited)
- `spark.shuffle.streaming.connectionTimeout` (default: 5s)
- `spark.shuffle.streaming.heartbeatInterval` (default: 10s)
- `spark.shuffle.streaming.debug` (default: false)


## 0.4 Integration Analysis

### 0.4.1 Existing Code Touchpoints

**Direct Modifications Required:**

| File | Location | Change Description |
|------|----------|-------------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | Lines 111-118 (companion object) | Add "streaming" alias to `shortShuffleMgrNames` map pointing to `StreamingShuffleManager` class |
| `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` | End of file | Add streaming-specific metrics methods to `ShuffleReadMetricsReporter` and `ShuffleWriteMetricsReporter` traits |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Shuffle config section (~lines 1500-1800) | Add `SHUFFLE_STREAMING_*` configuration entries |
| `core/src/main/scala/org/apache/spark/InternalAccumulator.scala` | Internal accumulator keys | Add streaming shuffle accumulator key constants |
| `core/src/main/scala/org/apache/spark/storage/BlockId.scala` | BlockId hierarchy | Add `StreamingShuffleBlockId` case class for streaming block identification |

**Dependency Injections:**

| File | Integration Point | Purpose |
|------|-------------------|---------|
| `core/src/main/scala/org/apache/spark/SparkEnv.scala` | `initializeShuffleManager()` method | No modification needed - uses `ShuffleManager.create()` factory which will auto-detect streaming manager |
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala` | `write()` method | Reference point for writer lifecycle - streaming writer integrates via same interface |
| `core/src/main/scala/org/apache/spark/ContextCleaner.scala` | Shuffle cleanup hooks | Add streaming shuffle cleanup registration via existing `registerShuffleForCleanup` pattern |

**Database/Schema Updates:**

No database or schema updates required - Spark shuffle uses file-based persistence only.

### 0.4.2 Memory Manager Integration

The streaming shuffle integrates with Spark's unified memory management through the following touchpoints:

**Buffer Allocation Flow:**

```
StreamingShuffleWriter
    └── SparkEnv.get.memoryManager
        └── acquireExecutionMemory(taskAttemptId, numBytes, ...)
            └── ExecutionMemoryPool (shared with other tasks)
```

**Integration Contract:**

- Streaming buffers request memory via `MemoryManager.acquireExecutionMemory()`
- Memory is tracked against the task's execution memory allocation
- Spill triggers when buffer utilization exceeds configurable threshold (default 80%)
- Memory released via `MemoryManager.releaseExecutionMemory()` after consumer acknowledgment

**Memory Calculation:**

```scala
// Per-partition buffer size calculation
val maxStreamingBufferBytes = 
  (executorMemory * conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) / 100)
val perPartitionBuffer = maxStreamingBufferBytes / numPartitions
```

### 0.4.3 Network Transport Integration

**Existing Transport Layer Usage:**

The streaming shuffle leverages Spark's existing `NettyBlockTransferService` without modification:

| Component | Usage |
|-----------|-------|
| `TransportContext` | Shared Netty context for streaming connections |
| `TransportClient` | Client-side streaming connections to consumers |
| `TransportServer` | Server-side handling of streaming requests |
| `RpcHandler` | Protocol handler for streaming shuffle messages |

**New Protocol Messages:**

New message types will be added to the shuffle network protocol:

| Message Type | Direction | Purpose |
|--------------|-----------|---------|
| `StreamingBlockRequest` | Consumer → Producer | Request in-progress block data |
| `StreamingBlockData` | Producer → Consumer | Streaming block payload with checksum |
| `StreamingAck` | Consumer → Producer | Acknowledgment for buffer reclamation |
| `BackpressureSignal` | Consumer → Producer | Flow control notification |
| `StreamingHeartbeat` | Bidirectional | Liveness detection |

### 0.4.4 Block Manager Integration

**Spill Coordination:**

| Operation | BlockManager Method | Purpose |
|-----------|---------------------|---------|
| Spill to disk | `putBlockData()` | Persist buffered partition data |
| Read spilled | `getLocalBlockData()` | Retrieve spilled data for streaming |
| Remove spilled | `removeBlock()` | Clean up after consumer acknowledgment |

**BlockId Registration:**

```scala
// New BlockId type for streaming shuffle blocks
case class StreamingShuffleBlockId(
    shuffleId: Int, 
    mapId: Long, 
    reduceId: Int,
    chunkIndex: Int  // For streaming chunk identification
) extends BlockId {
  override def name: String = 
    s"streaming_shuffle_${shuffleId}_${mapId}_${reduceId}_${chunkIndex}"
}
```

### 0.4.5 MapOutputTracker Integration

**Shuffle Metadata Flow:**

| Phase | MapOutputTracker Method | Streaming Behavior |
|-------|------------------------|-------------------|
| Registration | `registerMapOutput()` | Register streaming shuffle with partial completion status |
| Location query | `getMapSizesByExecutorId()` | Return locations for in-progress shuffles |
| Invalidation | `unregisterMapOutput()` | Handle partial read invalidation on producer failure |

**Status Tracking Enhancement:**

The streaming shuffle extends `MapStatus` to include streaming-specific metadata:

- `isStreamingComplete: Boolean` - Whether streaming has finished
- `acknowledgedBytes: Long` - Bytes confirmed received by consumers
- `spilledBytes: Long` - Bytes persisted to disk due to backpressure

### 0.4.6 Failure Detection Integration

**Connection Timeout Detection (5 seconds):**

```
StreamingShuffleReader
    └── TransportClient.sendRpc() with timeout
        └── IOException on timeout
            └── Mark producer as failed
                └── DAGScheduler.taskFailed()
```

**Heartbeat Monitoring (10 seconds):**

```
StreamingShuffleWriter
    └── ScheduledExecutorService (heartbeat thread)
        └── Check lastAckTimestamp for each consumer
            └── If stale > 10s, buffer for reconnect
                └── If buffer > 80%, trigger spill
```

### 0.4.7 Metrics System Integration

**Existing Metrics Infrastructure:**

The streaming shuffle integrates with Spark's Dropwizard-based metrics system:

| Metric Type | Existing Pattern | Streaming Adaptation |
|-------------|------------------|---------------------|
| Gauges | `ExecutorSource` gauges | Buffer utilization percentage |
| Counters | `ShuffleWriteMetrics` counters | Spill count, backpressure events |
| Histograms | Task timing histograms | Streaming latency distribution |

**New Metrics Registration:**

```scala
// Integration with existing MetricsSystem
class StreamingShuffleMetricsSource extends Source {
  override val sourceName = "StreamingShuffle"
  override val metricRegistry = new MetricRegistry()
  
  // Gauges
  metricRegistry.register("bufferUtilizationPercent", ...)
  
  // Counters
  metricRegistry.register("spillCount", ...)
  metricRegistry.register("backpressureEvents", ...)
  metricRegistry.register("partialReadInvalidations", ...)
}
```

### 0.4.8 Fallback Integration

**Automatic Fallback Triggers:**

| Condition | Detection Method | Fallback Action |
|-----------|------------------|-----------------|
| Consumer 2x slower for >60s | Rate tracking in BackpressureProtocol | Switch to SortShuffleWriter |
| OOM risk | MemoryManager allocation failure | Spill all buffers, disable streaming |
| Network saturation >90% | Bandwidth monitoring | Reduce streaming rate, may fallback |
| Version mismatch | Handshake protocol | Fallback on unsupported consumers |

**Graceful Degradation Flow:**

```
StreamingShuffleWriter.write()
    └── detectFallbackCondition()
        └── if (shouldFallback) {
              spillAllBuffers()
              createSortShuffleWriter()
              delegateRemainingRecords()
            }
```

This ensures zero data loss during fallback by persisting all in-memory data before switching to sort-based shuffle.


## 0.5 Technical Implementation

### 0.5.1 File-by-File Execution Plan

**CRITICAL: Every file listed below MUST be created or modified as specified.**

#### Group 1 - Core Streaming Shuffle Components

| Action | File Path | Implementation Details |
|--------|-----------|------------------------|
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | Implement `ShuffleManager` trait with streaming-specific registration, writer/reader factory methods, and fallback detection logic |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | Implement `ShuffleWriter[K, V]` with memory buffer management, streaming to consumers, checksum generation, and spill coordination |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | Implement `ShuffleReader[K, C]` with in-progress block fetching, partial read invalidation, acknowledgment protocol, and checksum validation |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | Define `StreamingShuffleHandle` case class extending `ShuffleHandle` with streaming metadata |

#### Group 2 - Supporting Infrastructure

| Action | File Path | Implementation Details |
|--------|-----------|------------------------|
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | Token bucket rate limiting, heartbeat-based flow control, priority arbitration for concurrent shuffles |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | 100ms polling interval, 80% threshold monitoring, LRU partition eviction, block manager coordination |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | Implement `ShuffleBlockResolver` for streaming block resolution and merged data support |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | Metrics source with buffer utilization gauge, spill counter, backpressure event counter |
| CREATE | `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | Package object with utilities, constants, and documentation |

#### Group 3 - Configuration and Registry Updates

| Action | File Path | Implementation Details |
|--------|-----------|------------------------|
| MODIFY | `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | Add "streaming" → `StreamingShuffleManager` mapping in companion object |
| MODIFY | `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Add `SHUFFLE_STREAMING_*` configuration entries |
| MODIFY | `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` | Add streaming-specific methods to metrics reporter traits |
| MODIFY | `core/src/main/scala/org/apache/spark/storage/BlockId.scala` | Add `StreamingShuffleBlockId` case class |
| MODIFY | `core/src/main/scala/org/apache/spark/InternalAccumulator.scala` | Add streaming shuffle accumulator key constants |

#### Group 4 - Test Suite Implementation

| Action | File Path | Implementation Details |
|--------|-----------|------------------------|
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | Buffer allocation, spill trigger at 80%, checksum generation, producer failure cleanup |
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | Acknowledgment processing, rate limiting, timeout detection, priority arbitration |
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | In-progress block request, producer failure detection, partial read invalidation |
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | 100MB shuffle, producer/consumer failure injection, memory pressure, network partition |
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | Latency comparison with sort-based shuffle, memory utilization, spill frequency |
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | Manager lifecycle, handle registration, writer/reader instantiation |
| CREATE | `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | Threshold monitoring, LRU eviction, reclamation timing |

#### Group 5 - Documentation Updates

| Action | File Path | Implementation Details |
|--------|-----------|------------------------|
| CREATE | `docs/streaming-shuffle-guide.md` | User guide with configuration, tuning, monitoring, and troubleshooting |
| MODIFY | `docs/configuration.md` | Add streaming shuffle configuration reference section |
| MODIFY | `docs/tuning.md` | Add streaming shuffle tuning recommendations |

### 0.5.2 Implementation Approach per File

## StreamingShuffleManager.scala

```scala
// Core implementation pattern
private[spark] class StreamingShuffleManager(conf: SparkConf) 
    extends ShuffleManager with Logging {
  
  private val spillManager = new MemorySpillManager(conf)
  private val backpressureProtocol = new BackpressureProtocol(conf)
  
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Check if streaming is beneficial
    new StreamingShuffleHandle(shuffleId, dependency)
  }
  
  override def getWriter[K, V](...): ShuffleWriter[K, V] = {
    new StreamingShuffleWriter(handle, mapId, context, 
      metrics, spillManager, backpressureProtocol)
  }
}
```

## StreamingShuffleWriter.scala

```scala
// Core write path with buffer management
private[spark] class StreamingShuffleWriter[K, V](...)
    extends ShuffleWriter[K, V] {
  
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Buffer records in memory
    // Stream to consumers as buffers fill
    // Coordinate spill when thresholds exceeded
  }
}
```

## BackpressureProtocol.scala

```scala
// Flow control implementation
private[spark] class BackpressureProtocol(conf: SparkConf) {
  private val tokenBucket = new TokenBucket(
    conf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS))
  
  def shouldThrottle(consumerId: String): Boolean = {
    !tokenBucket.tryAcquire(1)
  }
  
  def recordAcknowledgment(consumerId: String, bytes: Long): Unit = {
    tokenBucket.release(bytes)
  }
}
```

### 0.5.3 Implementation Sequence

**Phase 1: Foundation (Core Components)**
1. Create `StreamingShuffleHandle` with basic metadata
2. Create `StreamingShuffleManager` skeleton implementing trait
3. Add configuration entries to `package.scala`
4. Add "streaming" alias to `ShuffleManager` companion

**Phase 2: Writer Path**
1. Implement `MemorySpillManager` with threshold monitoring
2. Implement `StreamingShuffleWriter` with buffer management
3. Add checksum generation using CRC32C
4. Integrate spill coordination with BlockManager

**Phase 3: Network Protocol**
1. Implement `BackpressureProtocol` with token bucket
2. Add streaming message types to network layer
3. Implement heartbeat-based liveness detection
4. Add acknowledgment protocol

**Phase 4: Reader Path**
1. Implement `StreamingShuffleReader` for in-progress reads
2. Add partial read invalidation logic
3. Implement checksum validation
4. Add retransmission request handling

**Phase 5: Integration**
1. Implement `StreamingShuffleBlockResolver`
2. Add `StreamingShuffleMetrics` source
3. Implement fallback detection and graceful degradation
4. Add `StreamingShuffleBlockId` to BlockId hierarchy

**Phase 6: Testing**
1. Create unit test suites for each component
2. Create integration test with failure injection
3. Create performance benchmark suite
4. Validate all 10 failure scenarios

**Phase 7: Documentation**
1. Create streaming shuffle user guide
2. Update configuration reference
3. Update tuning guide

### 0.5.4 Critical Implementation Patterns

**Buffer Allocation Pattern:**

```scala
// Request memory through unified memory manager
val acquired = memoryManager.acquireExecutionMemory(
  taskAttemptId, 
  numBytesNeeded,
  MemoryMode.ON_HEAP
)
if (acquired < numBytesNeeded) {
  spillManager.triggerSpill(acquired)
}
```

**Checksum Generation Pattern:**

```scala
// CRC32C checksum for each block
val checksum = new CRC32C()
checksum.update(blockData)
val checksumValue = checksum.getValue()
```

**Failure Detection Pattern:**

```scala
// Producer failure detection (5s timeout)
val result = client.sendRpcSync(request, 5000) // 5 second timeout
if (result == null) {
  invalidatePartialReads(producerId)
  notifyDagScheduler(producerFailure)
}
```

**Graceful Fallback Pattern:**

```scala
// Fallback to sort-based shuffle
if (shouldFallback()) {
  spillAllBuffers()
  val sortWriter = new SortShuffleWriter(handle, mapId, context, metrics)
  sortWriter.write(remainingRecords)
  return sortWriter.stop(success)
}
```

### 0.5.5 User Interface Design

Not applicable - this feature is a backend shuffle optimization with no user interface components. All interaction is through Spark configuration properties.


## 0.6 Scope Boundaries

### 0.6.1 Exhaustively In Scope

**Streaming Shuffle Source Files (New):**

| Pattern | Description |
|---------|-------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/**/*.scala` | All new streaming shuffle implementation files |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | Main shuffle manager |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | Streaming writer |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | Streaming reader |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | Handle type |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | Flow control |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | Spill coordination |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | Block resolution |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetrics.scala` | Metrics source |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/package.scala` | Package utilities |

**Streaming Shuffle Test Files (New):**

| Pattern | Description |
|---------|-------------|
| `core/src/test/scala/org/apache/spark/shuffle/streaming/**/*.scala` | All streaming shuffle test files |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | Writer unit tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | Flow control tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | Reader unit tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | Integration tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | Performance tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | Manager tests |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | Spill tests |

**Integration Points (Modifications):**

| File | Lines/Section | Modification |
|------|---------------|--------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | Lines 111-118 | Add streaming alias |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Shuffle config section | Add streaming configs |
| `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` | Reporter traits | Add streaming methods |
| `core/src/main/scala/org/apache/spark/storage/BlockId.scala` | BlockId hierarchy | Add StreamingShuffleBlockId |
| `core/src/main/scala/org/apache/spark/InternalAccumulator.scala` | Accumulator keys | Add streaming keys |

**Configuration Files:**

| Pattern | Description |
|---------|-------------|
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Streaming shuffle ConfigEntry definitions |

**Documentation:**

| Pattern | Description |
|---------|-------------|
| `docs/streaming-shuffle-guide.md` | New user guide for streaming shuffle |
| `docs/configuration.md` | Configuration reference updates |
| `docs/tuning.md` | Tuning recommendations |

### 0.6.2 Explicitly Out of Scope

**Absolute Preservation - ZERO MODIFICATIONS:**

| Component | Reason |
|-----------|--------|
| `core/src/main/scala/org/apache/spark/rdd/**/*.scala` | RDD APIs preserved per specification |
| `core/src/main/scala/org/apache/spark/sql/**/*.scala` | DataFrame/Dataset APIs preserved |
| `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala` | DAG scheduler preserved |
| `core/src/main/scala/org/apache/spark/scheduler/TaskScheduler*.scala` | Task scheduling preserved |
| `core/src/main/scala/org/apache/spark/executor/Executor.scala` | Executor lifecycle preserved |
| `core/src/main/scala/org/apache/spark/Dependency.scala` | Lineage tracking preserved |
| `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala` | Existing shuffle preserved |
| `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala` | Existing writer preserved |
| `core/src/main/scala/org/apache/spark/network/BlockDataManager.scala` | Block interface preserved |
| `core/src/main/scala/org/apache/spark/serializer/**/*.scala` | Serialization protocols preserved |

**Functionality Explicitly Excluded:**

| Feature | Reason |
|---------|--------|
| DAG optimization heuristics | Out of scope per specification |
| Query planning modifications | Out of scope per specification |
| Executor memory model redesign | Out of scope per specification |
| External system integrations | Out of scope per specification |
| Dynamic reconfiguration | Deferred to future version |
| Adaptive query execution integration | Not required for initial implementation |
| Push-based shuffle modifications | Coexists separately |

**Performance Optimizations Excluded:**

| Optimization | Reason |
|--------------|--------|
| JIT compilation hints | Beyond feature requirements |
| NUMA-aware allocation | Beyond feature requirements |
| Custom memory allocators | Uses existing MemoryManager |
| Network protocol compression | Uses existing compression |

**Refactoring Excluded:**

| Area | Reason |
|------|--------|
| Existing shuffle code cleanup | Not specified |
| Legacy API deprecation | Not specified |
| Code style updates | Not specified |
| Unrelated test improvements | Not specified |

### 0.6.3 Boundary Conditions

**Feature Activation Boundary:**

- Streaming shuffle activates ONLY when `spark.shuffle.streaming.enabled=true` AND `spark.shuffle.manager=streaming`
- Default behavior remains `SortShuffleManager` with zero impact on existing workloads

**Fallback Boundary:**

Automatic reversion to sort-based shuffle when:
- Consumer sustained 2x slower than producer for >60 seconds
- Memory pressure prevents buffer allocation (OOM risk)
- Network saturation exceeds 90% link capacity
- Producer/consumer version mismatch detected

**Memory Boundary:**

- Streaming buffers limited to configurable percentage (default 20%, max 50%) of executor memory
- Per-partition buffer calculated as: `(executorMemory * bufferPercent) / numPartitions`
- Spill triggered at configurable threshold (default 80%, range 50-95%)

**Network Boundary:**

- Block size limited to 2MB for pipelining efficiency
- Rate limiting via token bucket at configurable bandwidth cap
- TCP keepalive at 5-second interval for failure detection

**Telemetry Boundary:**

- Telemetry overhead limited to <1% CPU utilization
- Log volume capped at <10MB/hour per executor
- Debug logging disabled by default

### 0.6.4 Coexistence Strategy

**SortShuffleManager Preservation:**

The streaming shuffle implementation coexists with `SortShuffleManager` through:

1. **Separate Package**: All streaming code in `org.apache.spark.shuffle.streaming`
2. **Factory Pattern**: `ShuffleManager.create()` selects based on config
3. **Independent Handles**: `StreamingShuffleHandle` distinct from `BaseShuffleHandle`
4. **Shared Infrastructure**: Both managers use same `BlockManager`, `MemoryManager`, `MapOutputTracker`
5. **Fallback Path**: Streaming can delegate to sort-based on degradation

**No Cross-Contamination:**

| Isolation Requirement | Implementation |
|----------------------|----------------|
| No modifications to SortShuffleManager | Streaming in separate package |
| No modifications to SortShuffleWriter | Streaming writer is separate class |
| No changes to BlockStoreShuffleReader | Streaming reader is separate class |
| No changes to existing tests | New test suites in streaming package |

### 0.6.5 Version Compatibility

**Minimum Spark Version:** 4.2.0 (implements in main branch)

**Forward Compatibility:**
- Configuration keys follow established patterns
- Metrics integrate with existing infrastructure
- Fallback ensures graceful degradation

**Backward Compatibility:**
- Zero impact on `spark.shuffle.manager=sort` (default)
- Existing applications unchanged
- Configuration restart required (no hot reload in v1)


## 0.7 Rules for Feature Addition

### 0.7.1 Implementation Discipline Rules

The user has explicitly emphasized the following implementation constraints that must be strictly followed:

**Rule 1: Minimal Modification Principle**
> "Make only changes necessary to implement streaming shuffle capability within `ShuffleManager` abstraction boundary."

- All streaming code must be self-contained within `org.apache.spark.shuffle.streaming` package
- Modifications to existing files limited to registration/configuration additions
- No changes to execution paths when streaming is disabled

**Rule 2: Fallback Preservation**
> "Preserve existing sort-based shuffle as production-stable fallback."

- `SortShuffleManager` must remain unchanged and fully functional
- Streaming shuffle must automatically fallback under degradation conditions
- Zero data loss during fallback transitions

**Rule 3: API Preservation**
> "Never modify DAG scheduler, task lifecycle, or user-facing APIs."

- No changes to RDD, DataFrame, or Dataset APIs
- No changes to DAGScheduler scheduling logic
- No changes to task execution lifecycle
- No changes to SparkContext public methods

**Rule 4: Isolation Requirement**
> "Isolate streaming logic in dedicated classes with zero cross-contamination into existing shuffle code paths."

- Streaming classes in separate `streaming` subpackage
- No modifications to existing shuffle writer/reader classes
- Streaming handle types distinct from existing handle hierarchy
- Test suites isolated in streaming test package

**Rule 5: Documentation Requirement**
> "Document all integration points with clear comments explaining coexistence strategy."

- Each integration point (ShuffleManager factory, config registration) must have explanatory comments
- Coexistence behavior documented in code and user documentation
- Fallback conditions explicitly documented

### 0.7.2 Performance Requirements

**Latency Targets:**

| Workload Type | Target Improvement | Validation Method |
|---------------|-------------------|-------------------|
| Shuffle-heavy (100MB+, 10+ partitions) | 30-50% latency reduction | `StreamingShufflePerformanceBenchmark` |
| CPU-bound workloads | 5-10% improvement | Scheduler overhead measurement |
| Memory-bound workloads | Zero regression | Automatic fallback validation |

**Memory Constraints:**

| Constraint | Value | Enforcement |
|------------|-------|-------------|
| Buffer allocation limit | 20% executor memory (default) | MemorySpillManager |
| Spill trigger threshold | 80% buffer utilization (default) | Configurable 50-95% |
| Buffer reclamation response | <100ms | Unit test validation |
| Memory leak tolerance | Zero retained heap | Stress test validation |

**Operational Limits:**

| Metric | Limit | Purpose |
|--------|-------|---------|
| Telemetry CPU overhead | <1% utilization | Minimal impact |
| Log volume | <10MB/hour per executor | Storage management |
| Connection timeout | 5 seconds | Failure detection |
| Heartbeat interval | 10 seconds | Liveness monitoring |

### 0.7.3 Failure Tolerance Requirements

**Data Integrity:**
> "Zero data loss under all failure scenarios including producer crashes, consumer failures, network partitions."

The implementation must handle all 10 specified failure scenarios:

| # | Scenario | Required Behavior |
|---|----------|-------------------|
| 1 | Producer crash during shuffle write | Buffer spill, reader invalidation, upstream recomputation |
| 2 | Consumer crash during shuffle read | Buffer retention, reconnection handling |
| 3 | Network partition between producer/consumer | Timeout detection, graceful fallback |
| 4 | Memory exhaustion during buffer allocation | Immediate spill trigger, no OOM |
| 5 | Disk failure during spill operation | Escalate to task failure, lineage recomputation |
| 6 | Checksum mismatch on block receive | Retransmission request |
| 7 | Connection timeout during streaming | Producer failure notification |
| 8 | Executor JVM pause (GC) during shuffle | Heartbeat tolerance, no false failures |
| 9 | Multiple concurrent producer failures | Parallel invalidation handling |
| 10 | Consumer reconnect after extended downtime | Buffer/spill data availability |

**Validation Requirements:**

| Validation Type | Coverage Target | Enforcement |
|-----------------|-----------------|-------------|
| Unit test coverage | >85% for all new components | CI validation |
| Integration tests | All 10 failure scenarios | Failure injection testing |
| Memory leak detection | Zero retained heap | Heap analysis after stress test |
| Static analysis | Zero critical issues | Build validation |

### 0.7.4 Security Requirements

**Authentication:**
- Streaming connections must respect existing `spark.authenticate` configuration
- Authentication bootstraps inherited from `NettyBlockTransferService`

**Encryption:**
- Streaming data encrypted when `spark.network.crypto.enabled=true`
- Spilled data encryption follows existing `spark.io.encryption.enabled` setting

**No New Security Surface:**
- No new ports or protocols requiring firewall changes
- Uses existing RPC transport layer security

### 0.7.5 Coding Standards

**Scala Style:**
- Follow existing Spark Scalastyle rules defined in `scalastyle-config.xml`
- Use Spark logging patterns (`Logging` trait, MDC keys from `LogKeys`)
- Configuration via `SparkConf` and `ConfigBuilder` pattern

**Testing Standards:**
- Use `SparkFunSuite` base class for all tests
- Follow existing Mockito mocking patterns
- Include proper test fixtures and cleanup
- Use `LocalSparkContext` for integration tests

**Documentation Standards:**
- Scaladoc for all public/private[spark] APIs
- Configuration entries include `.doc()` documentation
- Integration comments at all touchpoints

### 0.7.6 Quality Gates

**Pre-Merge Requirements:**

| Gate | Requirement |
|------|-------------|
| Compilation | Zero errors, zero warnings |
| Unit Tests | All pass, >85% coverage |
| Integration Tests | All pass, zero flakiness |
| Failure Tests | All 10 scenarios validated |
| Memory Tests | Zero leaks after stress test |
| Static Analysis | Zero critical issues |
| Performance | Meet latency targets in benchmarks |

**Autonomous Validation:**
All tests must be executable via standard Spark test infrastructure:
```bash
./build/mvn test -pl core -Dtest=StreamingShuffle*
```


## 0.8 References

### 0.8.1 Files and Folders Searched

The following files and folders were comprehensively searched across the codebase to derive conclusions for this Agent Action Plan:

**Root Level:**
- `pom.xml` - Maven parent POM with version and dependency information
- `README.md` - Project overview and build instructions
- `scalastyle-config.xml` - Scala style rules

**Core Module - Shuffle Implementation:**
- `core/src/main/scala/org/apache/spark/shuffle/` - Complete shuffle package
  - `ShuffleManager.scala` - Pluggable shuffle manager trait and factory
  - `ShuffleWriter.scala` - Abstract writer contract
  - `ShuffleReader.scala` - Abstract reader contract
  - `ShuffleHandle.scala` - Base handle definition
  - `BaseShuffleHandle.scala` - Concrete handle implementation
  - `ShuffleBlockResolver.scala` - Block resolution contract
  - `IndexShuffleBlockResolver.scala` - Sort-based resolver
  - `ShuffleWriteProcessor.scala` - Write orchestration
  - `ShuffleBlockPusher.scala` - Push-based shuffle support
  - `ShuffleChecksumUtils.scala` - Checksum utilities
  - `metrics.scala` - Metrics reporter interfaces
  - `sort/SortShuffleManager.scala` - Sort-based manager implementation
  - `sort/SortShuffleWriter.scala` - Sort-based writer implementation

**Core Module - Memory Management:**
- `core/src/main/scala/org/apache/spark/memory/` - Memory subsystem
  - `MemoryManager.scala` - Abstract memory manager
  - `MemoryPool.scala` - Pool abstraction
  - `ExecutionMemoryPool.scala` - Execution memory
  - `StorageMemoryPool.scala` - Storage memory
  - `UnifiedMemoryManager.scala` - Unified memory policy

**Core Module - Storage:**
- `core/src/main/scala/org/apache/spark/storage/` - Storage subsystem
  - `BlockManager.scala` - Central block management
  - `BlockId.scala` - Block identifier hierarchy
  - `DiskBlockManager.scala` - Disk persistence
  - `ShuffleBlockFetcherIterator.scala` - Shuffle fetch pipeline

**Core Module - Network:**
- `core/src/main/scala/org/apache/spark/network/` - Network abstractions
  - `BlockTransferService.scala` - Transfer service contract
  - `BlockDataManager.scala` - Block data interface
  - `netty/NettyBlockTransferService.scala` - Netty implementation
  - `netty/NettyBlockRpcServer.scala` - RPC handler
  - `netty/SparkTransportConf.scala` - Transport configuration

**Core Module - Configuration:**
- `core/src/main/scala/org/apache/spark/internal/config/` - Config registry
  - `package.scala` - Main configuration keys including shuffle configs

**Core Module - Executor and Metrics:**
- `core/src/main/scala/org/apache/spark/executor/` - Executor runtime
  - `ShuffleReadMetrics.scala` - Read metrics container
  - `ShuffleWriteMetrics.scala` - Write metrics container
  - `TaskMetrics.scala` - Per-task metrics facade

**Core Module - Environment:**
- `core/src/main/scala/org/apache/spark/SparkEnv.scala` - Runtime environment
- `core/src/main/scala/org/apache/spark/SparkConf.scala` - Configuration container
- `core/src/main/scala/org/apache/spark/InternalAccumulator.scala` - Accumulator keys

**Test Infrastructure:**
- `core/src/test/scala/org/apache/spark/` - Test root
  - `SparkFunSuite.scala` - Test base class
  - `shuffle/` - Shuffle test suites
  - `ShuffleSuite.scala` - Integration tests

**Common Modules:**
- `common/network-shuffle/` - Network shuffle module
  - `src/main/java/org/apache/spark/network/shuffle/` - Shuffle transport
- `common/network-common/` - Common network infrastructure

**Documentation:**
- `docs/` - Documentation root
  - `configuration.md` - Configuration reference
  - `tuning.md` - Tuning guide

### 0.8.2 Attachments Provided

No file attachments were provided by the user for this feature implementation.

### 0.8.3 Figma Screens Provided

No Figma screens or URLs were provided - this feature is a backend optimization with no user interface components.

### 0.8.4 External References

**Apache Spark Documentation:**
- Spark Shuffle Architecture: Internal design documents
- Spark Configuration Reference: Configuration patterns
- Spark Tuning Guide: Performance optimization patterns

**Technical Standards Referenced:**
- CRC32C Checksum: RFC 3720 (hardware-accelerated in Java 9+)
- Token Bucket Algorithm: Standard rate limiting pattern
- Backpressure Protocols: Reactive Streams specification patterns

### 0.8.5 Related Spark JIRAs and Design Documents

While not explicitly provided, the following existing Spark work relates to this feature:

- **SPARK-4550**: Serialized shuffle sort optimization (reference for serialization patterns)
- **SPARK-7081**: Tungsten shuffle optimizations (reference for memory management)
- **Push-Based Shuffle**: Existing implementation for reference on network protocol patterns

### 0.8.6 Version Information

| Component | Version |
|-----------|---------|
| Apache Spark | 4.2.0-SNAPSHOT |
| Java | 17 |
| Scala | 2.13 |
| Netty | 4.1.x (managed) |
| Dropwizard Metrics | 4.x (managed) |

### 0.8.7 Search Tracking Summary

| Search Type | Count | Coverage |
|-------------|-------|----------|
| Folder explorations | 15+ | Core shuffle, memory, network, storage, executor, config packages |
| File retrievals | 10+ | Key interface and implementation files |
| Pattern searches | 5+ | Configuration patterns, test patterns |

**Depth Coverage:**
- Level 0 (root): Explored
- Level 1 (core, common, docs): Explored
- Level 2 (src/main/scala/org/apache/spark): Explored
- Level 3+ (shuffle, memory, network, storage): Fully explored

**Key Files Retrieved and Analyzed:**
1. `ShuffleManager.scala` - Complete interface understanding
2. `ShuffleWriter.scala` - Writer contract
3. `ShuffleReader.scala` - Reader contract
4. `SortShuffleManager.scala` - Implementation reference (lines 1-220)
5. `SparkEnv.scala` - Initialization flow (lines 1-250)
6. `metrics.scala` - Metrics interfaces
7. `package.scala` (config) - Configuration patterns (grep for SHUFFLE)
8. `MemoryManager.scala` - Memory management integration
9. Network transport files - Transport layer understanding

All searches followed the Comprehensive Search Strategy with systematic deep exploration of relevant branches and targeted broad searches for specific implementations.


