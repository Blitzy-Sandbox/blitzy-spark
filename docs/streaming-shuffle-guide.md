---
layout: global
title: Streaming Shuffle Guide
displayTitle: Streaming Shuffle Guide
description: Guide to using Spark's streaming shuffle for reduced shuffle latency SPARK_VERSION_SHORT
license: |
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
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Streaming shuffle is an alternative shuffle implementation that streams serialized partition data
directly to consumer executors instead of materializing complete shuffle files on disk. This
approach fundamentally changes how shuffle data flows between map and reduce tasks, eliminating
the traditional full-materialization pattern in favor of an incremental streaming approach with
sophisticated backpressure and fault tolerance mechanisms.

## How It Works

In the default sort-based shuffle, map tasks write all shuffle data to disk files before reduce
tasks can begin reading. This creates a synchronization barrier where the reduce phase cannot
start until all map tasks complete their writes.

Streaming shuffle removes this barrier by:

1. **Streaming Data Directly**: Map tasks stream serialized partition data to consumer executors
   as soon as data is available, rather than waiting for full materialization.
2. **Memory-Efficient Buffering**: Per-partition buffers are maintained in memory with configurable
   limits, automatically spilling to disk when memory pressure exceeds thresholds.
3. **Backpressure Protocol**: A token bucket-based flow control mechanism prevents fast producers
   from overwhelming slow consumers.
4. **Acknowledgment-Based Reclamation**: Buffers are reclaimed only after consumers acknowledge
   receipt, ensuring data integrity.

## Key Benefits

- **30-50% latency reduction** for shuffle-heavy workloads with 100MB+ data and 10+ partitions
- **Reduced disk I/O** by streaming data directly to consumers when possible
- **Earlier reduce task progress** as data becomes available incrementally
- **Memory-efficient operation** with automatic spill when thresholds are exceeded

## When to Use Streaming Shuffle

Streaming shuffle is recommended when:

- Shuffle data volume exceeds **100MB**
- Number of partitions is **10 or more**
- Network bandwidth is sufficient for streaming
- Workloads are shuffle-intensive (joins, aggregations, repartitions)

Streaming shuffle may not be beneficial for:

- Very small shuffles (<100MB) where sort-based overhead is minimal
- CPU-bound workloads where shuffle is not the bottleneck
- Memory-constrained environments where spill overhead exceeds streaming benefits

## Coexistence with Sort-Based Shuffle

Streaming shuffle coexists with the default sort-based shuffle implementation. When streaming
shuffle is enabled:

- The default sort-based shuffle remains available as a fallback
- Automatic fallback occurs under degradation conditions (see [Automatic Fallback](#automatic-fallback))
- No changes are required to existing application code
- Both shuffle modes share the same BlockManager and MemoryManager infrastructure

# Quick Start

To enable streaming shuffle, set the following configuration properties:

{% highlight scala %}
val conf = new SparkConf()
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
val sc = new SparkContext(conf)
{% endhighlight %}

Or via `spark-submit`:

```sh
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  myApp.jar
```

Or in `conf/spark-defaults.conf`:

```
spark.shuffle.manager           streaming
spark.shuffle.streaming.enabled true
```

With these settings, all shuffle operations will use the streaming shuffle implementation,
with automatic fallback to sort-based shuffle under degradation conditions.

# Configuration Options

The following configuration properties control streaming shuffle behavior:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Description</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.shuffle.streaming.enabled</code></td>
  <td>false</td>
  <td>
    Enable streaming shuffle for reduced latency. When set to <code>true</code> and
    <code>spark.shuffle.manager</code> is set to <code>streaming</code>, shuffle data will be
    streamed directly to consumer executors instead of materializing complete shuffle files.
    This can reduce shuffle latency by 30-50% for shuffle-heavy workloads.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
  <td>20</td>
  <td>
    Percentage of executor memory allocated for streaming shuffle buffers. Valid range is 1-50.
    Higher values allow more data to be buffered before spilling to disk, which can improve
    performance for large shuffles but increases memory pressure. The actual per-partition
    buffer size is calculated as: <code>(executorMemory * bufferSizePercent / 100) / numPartitions</code>.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.spillThreshold</code></td>
  <td>80</td>
  <td>
    Buffer utilization percentage at which to trigger disk spill. Valid range is 50-95.
    When the streaming shuffle buffer utilization exceeds this threshold, the system triggers
    automatic spill of the largest partitions using LRU eviction. Lower values provide more
    headroom for memory pressure but may increase spill frequency.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
  <td>0 (unlimited)</td>
  <td>
    Maximum bandwidth in megabytes per second for streaming shuffle transfers. When set to 0,
    no bandwidth limit is applied. Setting a limit can prevent network saturation when multiple
    shuffles are running concurrently. The backpressure protocol uses token bucket rate limiting
    at 80% of this configured capacity to allow burst handling.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.connectionTimeout</code></td>
  <td>5s</td>
  <td>
    Timeout for streaming shuffle connections. If a producer fails to respond within this
    duration, the connection is considered failed and the consumer will invalidate partial reads
    and notify the DAG scheduler. Lower values enable faster failure detection but may cause
    false positives during GC pauses or temporary network congestion.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.heartbeatInterval</code></td>
  <td>10s</td>
  <td>
    Interval between heartbeat signals in the streaming shuffle protocol. Heartbeats are used
    for liveness detection of both producers and consumers. If no heartbeat or acknowledgment
    is received within twice this interval, the peer is considered failed. This should be set
    higher than <code>connectionTimeout</code> to avoid premature failure detection.
  </td>
  <td>4.2.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.debug</code></td>
  <td>false</td>
  <td>
    Enable debug logging for streaming shuffle operations. When enabled, detailed logs are
    written for buffer allocation, spill operations, backpressure events, and acknowledgment
    processing. This can significantly increase log volume and should only be enabled for
    troubleshooting. Log volume at default settings is &lt;10MB/hour per executor.
  </td>
  <td>4.2.0</td>
</tr>
</table>

For a complete list of Spark configuration properties, see the
[Configuration Guide](configuration.html).

# Memory Management

Streaming shuffle implements sophisticated memory management to balance performance with
resource constraints.

## Buffer Allocation

Streaming shuffle allocates memory buffers for each partition being written. The total
buffer allocation is calculated as:

```
maxStreamingBufferBytes = executorMemory * spark.shuffle.streaming.bufferSizePercent / 100
perPartitionBuffer = maxStreamingBufferBytes / numPartitions
```

For example, with 4GB executor memory, 20% buffer allocation, and 100 partitions:

- Maximum streaming buffer: 4GB × 0.20 = 800MB
- Per-partition buffer: 800MB / 100 = 8MB

Memory is requested from Spark's unified MemoryManager, which tracks execution memory
across all tasks. If insufficient memory is available, the system triggers immediate spill.

## Memory Monitoring

The streaming shuffle monitors memory utilization at **100ms intervals** using a dedicated
monitoring thread. This polling approach ensures rapid response to memory pressure without
excessive overhead (telemetry overhead is limited to <1% CPU utilization).

The monitoring thread tracks:

- Current buffer utilization percentage across all partitions
- Individual partition buffer sizes for LRU eviction decisions
- Memory manager's reported execution memory availability
- Trend analysis for predictive spill triggering

## Spill Triggers

Automatic disk spill is triggered when buffer utilization exceeds the configured threshold
(default 80%). The spill process:

1. Identifies partitions for eviction using LRU (Least Recently Used) ordering
2. Selects the largest LRU partitions to meet the spill target
3. Persists partition data to disk via BlockManager's DiskBlockManager
4. Releases memory back to the MemoryManager pool
5. Updates partition metadata for reader access

Spill operations target **sub-100ms response time** for buffer reclamation to prevent
out-of-memory conditions.

## Integration with Spark Memory Management

Streaming shuffle buffers are allocated from Spark's execution memory pool, which is shared
with other operations (sorts, joins, aggregations). The unified memory management ensures:

- Execution can evict streaming buffers when needed for other operations
- Streaming buffers can expand into storage memory when caching is not in use
- Memory is properly released when tasks complete or fail
- No memory leaks occur under any failure scenario

For detailed information on Spark's memory management, see the
[Memory Tuning](tuning.html#memory-management-overview) section of the Tuning Guide.

# Backpressure Protocol

The streaming shuffle implements a flow control mechanism to prevent fast producers from
overwhelming slow consumers. This is essential for maintaining system stability under
heterogeneous workload conditions.

## Token Bucket Rate Limiting

The backpressure protocol uses a token bucket algorithm for rate limiting:

- Each consumer maintains a token bucket with capacity based on 80% of link capacity
- Tokens are replenished at a configurable rate (via `spark.shuffle.streaming.maxBandwidthMBps`)
- Each streaming operation consumes tokens proportional to the data size
- When insufficient tokens are available, the producer pauses until tokens are replenished

This approach allows for burst handling (up to 20% over average rate) while maintaining
average throughput limits.

## Heartbeat-Based Liveness Detection

The protocol uses heartbeat signals for detecting peer failures:

- **Heartbeat Interval**: Configurable via `spark.shuffle.streaming.heartbeatInterval` (default 10s)
- **Timeout Detection**: Connection timeout at `spark.shuffle.streaming.connectionTimeout` (default 5s)
- **TCP Keepalive**: TCP keepalive at 5-second intervals for additional failure detection

If no heartbeat or data is received within twice the heartbeat interval, the peer is
considered failed and appropriate recovery actions are initiated.

## Consumer-to-Producer Flow Control

Flow control signals flow from consumers to producers:

1. **Acknowledgment Messages**: Consumers send acknowledgments for received data chunks
2. **Backpressure Signals**: Consumers signal producers to slow down when buffers are full
3. **Rate Adjustment**: Producers dynamically adjust sending rate based on feedback
4. **Priority Arbitration**: When multiple shuffles compete for resources, priority is
   determined by partition count and data volume

## Acknowledgment-Based Buffer Reclamation

Producer buffers are only reclaimed after explicit consumer acknowledgment:

1. Producer streams data chunk to consumer with CRC32C checksum
2. Consumer validates checksum and processes data
3. Consumer sends acknowledgment with byte count
4. Producer marks acknowledged bytes as reclaimable
5. Memory is released back to pool when all consumers acknowledge

This ensures zero data loss even under partial failures.

# Failure Handling

Streaming shuffle is designed to handle all failure scenarios gracefully with zero data loss.
This section documents the 10 supported failure scenarios and their handling mechanisms.

## Failure Scenario 1: Producer Crash During Shuffle Write

**Situation**: A map task crashes while actively streaming shuffle data to consumers.

**Detection**: Consumers detect producer failure through connection timeout (5 seconds) or
heartbeat absence (10 seconds).

**Recovery**:
1. Consumers receive connection failure notification
2. Partial reads are atomically invalidated
3. DAG scheduler is notified of the producer failure
4. Upstream RDD partitions are recomputed via lineage
5. New producer resumes from the beginning of the partition

**Data Integrity**: Zero data loss - partial data is discarded and recomputed.

## Failure Scenario 2: Consumer Crash During Shuffle Read

**Situation**: A reduce task crashes while receiving streaming shuffle data.

**Detection**: Producer detects consumer failure through acknowledgment timeout or connection loss.

**Recovery**:
1. Producer receives connection failure notification
2. Unacknowledged buffer data is retained (not reclaimed)
3. If consumer executor recovers, data is re-sent from retained buffers
4. If consumer is rescheduled to different executor, data is retrieved from buffer or spill

**Data Integrity**: Zero data loss - unacknowledged data remains available for retry.

## Failure Scenario 3: Network Partition Between Producer/Consumer

**Situation**: Network connectivity is lost between producer and consumer executors.

**Detection**: Both sides detect partition through connection timeout (5 seconds).

**Recovery**:
1. Connection timeout triggers failure handling on both sides
2. Producer retains buffered data and attempts reconnection with exponential backoff
3. Consumer invalidates partial reads and waits for producer recovery
4. If partition persists beyond retry limits (5 attempts, 1s base), graceful fallback to sort-based shuffle

**Data Integrity**: Zero data loss - either reconnection succeeds or full fallback occurs.

## Failure Scenario 4: Memory Exhaustion During Buffer Allocation

**Situation**: MemoryManager cannot allocate requested buffer memory.

**Detection**: Memory allocation failure returned from MemoryManager.

**Recovery**:
1. Immediate spill trigger for existing buffers
2. LRU eviction of largest partitions to disk
3. Retry allocation with reduced buffer size
4. If repeated failures, automatic fallback to sort-based shuffle

**Data Integrity**: Zero data loss - spill ensures all data is persisted.

## Failure Scenario 5: Disk Failure During Spill Operation

**Situation**: Disk write fails during buffer spill to DiskBlockManager.

**Detection**: IOException during BlockManager.putBlockData() call.

**Recovery**:
1. Spill failure is logged and reported
2. Task failure is escalated to the DAG scheduler
3. Entire task is retried on potentially different executor
4. Lineage-based recomputation produces new shuffle data

**Data Integrity**: Zero data loss - task retry ensures data is recomputed.

## Failure Scenario 6: Checksum Mismatch on Block Receive

**Situation**: Consumer detects CRC32C checksum mismatch on received data block.

**Detection**: Checksum validation failure after receiving complete block.

**Recovery**:
1. Corrupted block is discarded
2. Consumer sends retransmission request to producer
3. Producer resends block from buffer or spilled data
4. Process repeats until valid checksum or retry limit reached
5. If retry limit exceeded, task failure with recomputation

**Data Integrity**: Zero data loss - corruption is detected and corrected.

## Failure Scenario 7: Connection Timeout During Streaming

**Situation**: TCP connection times out during active data streaming.

**Detection**: Socket timeout exception after `connectionTimeout` duration.

**Recovery**:
1. Consumer marks producer as potentially failed
2. Exponential backoff retry initiated (1s base, max 5 attempts)
3. If retry succeeds, streaming resumes from last acknowledged position
4. If all retries fail, producer failure notification to DAG scheduler
5. Upstream recomputation via lineage

**Data Integrity**: Zero data loss - retry or recomputation ensures completeness.

## Failure Scenario 8: Executor JVM Pause (GC) During Shuffle

**Situation**: Long GC pause causes temporary unresponsiveness.

**Detection**: Heartbeat monitoring with tolerance for GC pauses.

**Recovery**:
1. Heartbeat timeout is set higher than typical GC pauses (10s default)
2. Connection timeout (5s) allows for short pauses
3. TCP keepalive maintains connection during pause
4. After GC completes, normal operation resumes
5. Only extended pauses (>2× heartbeat interval) trigger failure handling

**Data Integrity**: Zero data loss - GC pauses are tolerated within timeout bounds.

## Failure Scenario 9: Multiple Concurrent Producer Failures

**Situation**: Several map tasks fail simultaneously (e.g., executor death).

**Detection**: Multiple connection failures detected by consumers.

**Recovery**:
1. Each consumer independently invalidates partial reads from failed producers
2. DAG scheduler receives multiple failure notifications
3. Parallel task retry scheduling for all affected partitions
4. Independent lineage recomputation for each failed partition
5. System continues with surviving producers

**Data Integrity**: Zero data loss - parallel recovery ensures all data is recomputed.

## Failure Scenario 10: Consumer Reconnect After Extended Downtime

**Situation**: Consumer reconnects after being unavailable for an extended period.

**Detection**: Reconnection request from previously failed consumer.

**Recovery**:
1. Producer checks if buffer data is still available
2. If in memory: resend from buffer
3. If spilled: retrieve from disk via BlockManager
4. If expired: notify consumer to re-fetch from lineage
5. Consumer resumes from last acknowledged position

**Data Integrity**: Zero data loss - data available from buffer, spill, or recomputation.

## Summary of Failure Handling Mechanisms

| Mechanism | Purpose | Configuration |
|-----------|---------|---------------|
| CRC32C Checksums | Data corruption detection | Always enabled |
| Connection Timeout | Fast failure detection | `connectionTimeout` (5s default) |
| Heartbeat Monitoring | Liveness detection | `heartbeatInterval` (10s default) |
| Exponential Backoff | Transient failure recovery | 1s base, 5 max attempts |
| Buffer Retention | Consumer failure recovery | Until acknowledgment |
| Disk Spill | Memory pressure handling | `spillThreshold` (80% default) |
| Partial Read Invalidation | Producer failure recovery | Automatic |

# Automatic Fallback

Streaming shuffle automatically falls back to sort-based shuffle under certain conditions
to ensure system stability. Fallback is designed to be graceful with zero data loss.

## Fallback Conditions

The following conditions trigger automatic fallback to sort-based shuffle:

### Consumer 2x Slower Than Producer for >60 Seconds

**Detection**: The backpressure protocol tracks producer and consumer rates. If a consumer
sustains less than 50% of the producer's rate for 60 continuous seconds, fallback is triggered.

**Rationale**: Severely mismatched rates indicate streaming is not beneficial and may cause
excessive buffer pressure.

### OOM Risk Detected

**Detection**: Memory allocation failure from MemoryManager after spill attempts.

**Rationale**: If memory pressure persists despite spilling, sort-based shuffle's bounded
memory usage is safer.

### Network Saturation Exceeds 90%

**Detection**: Bandwidth monitoring detects sustained network utilization above 90% of
configured maximum.

**Rationale**: Network saturation can cause cascading failures; sort-based shuffle reduces
network pressure.

### Producer/Consumer Version Mismatch

**Detection**: Protocol handshake detects incompatible streaming shuffle versions.

**Rationale**: Version mismatches can cause data corruption; fallback ensures compatibility.

## Graceful Degradation Process

When fallback is triggered:

1. **Spill All Buffers**: All in-memory streaming buffers are spilled to disk
2. **Notify Consumers**: Consumers are notified to switch to sort-based fetch
3. **Switch Writer**: StreamingShuffleWriter delegates to SortShuffleWriter
4. **Continue Processing**: Remaining records processed via sort-based shuffle
5. **Metrics Update**: Fallback event recorded in metrics

The entire process ensures:

- **Zero data loss**: All buffered data is persisted before switching
- **No recomputation**: Already-processed data is preserved
- **Transparent to application**: No application code changes required

## Monitoring Fallback

Fallback events are recorded in metrics:

- `StreamingShuffle.fallbackCount` - Number of fallback events
- `StreamingShuffle.fallbackReason` - Most recent fallback reason

Check these metrics to identify whether streaming shuffle is providing benefit for your
workload.

# Monitoring and Metrics

Streaming shuffle integrates with Spark's MetricsSystem to provide observability into
shuffle operations.

## Available Metrics

The following metrics are exposed via Spark's metrics infrastructure:

<table class="spark-config">
<thead><tr><th>Metric Name</th><th>Type</th><th>Description</th></tr></thead>
<tr>
  <td><code>StreamingShuffle.bufferUtilizationPercent</code></td>
  <td>Gauge</td>
  <td>
    Current percentage of allocated streaming buffer memory in use. Values approaching
    <code>spillThreshold</code> indicate memory pressure. Monitor this metric to tune
    <code>bufferSizePercent</code> appropriately.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.spillCount</code></td>
  <td>Counter</td>
  <td>
    Total number of spill operations triggered by memory pressure. Frequent spills may
    indicate <code>bufferSizePercent</code> is too low or <code>spillThreshold</code> is
    too aggressive. Some spilling is normal and healthy.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.backpressureEvents</code></td>
  <td>Counter</td>
  <td>
    Number of backpressure events where producers were throttled due to slow consumers.
    High counts may indicate consumer processing bottlenecks or network bandwidth limitations.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.partialReadInvalidations</code></td>
  <td>Counter</td>
  <td>
    Number of partial read invalidations due to producer failures. This metric indicates
    how often failure recovery is invoked. Non-zero values are expected under failures.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.bytesStreamed</code></td>
  <td>Counter</td>
  <td>
    Total bytes successfully streamed directly to consumers without disk materialization.
    Compare with total shuffle bytes to assess streaming efficiency.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.bytesSpilled</code></td>
  <td>Counter</td>
  <td>
    Total bytes spilled to disk due to memory pressure or backpressure. High values relative
    to <code>bytesStreamed</code> may indicate memory constraints.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.fallbackCount</code></td>
  <td>Counter</td>
  <td>
    Number of times streaming shuffle fell back to sort-based shuffle. Non-zero values
    indicate conditions where streaming is not optimal.
  </td>
</tr>
<tr>
  <td><code>StreamingShuffle.checksumValidationFailures</code></td>
  <td>Counter</td>
  <td>
    Number of CRC32C checksum validation failures detected. Non-zero values indicate
    data corruption during transfer that was successfully detected and recovered.
  </td>
</tr>
</table>

## Accessing Metrics

### JMX

Metrics are exposed via JMX under the `metrics` domain:

```
metrics:name=StreamingShuffle.bufferUtilizationPercent
metrics:name=StreamingShuffle.spillCount
...
```

Enable JMX metrics with:

```
spark.metrics.conf.*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
```

### REST API

Metrics are available through the Spark REST API at:

```
http://<driver>:4040/api/v1/applications/<app-id>/executors
```

The executor endpoint includes streaming shuffle metrics when available.

### Spark UI

The Spark UI Environment tab displays streaming shuffle configuration, and the Stages tab
shows shuffle read/write metrics including streaming-specific statistics when streaming
shuffle is active.

## Log Volume

At default settings (`spark.shuffle.streaming.debug=false`), streaming shuffle generates
**less than 10MB/hour per executor** of log data. This includes:

- Spill event notifications (WARN level)
- Fallback event notifications (WARN level)
- Connection failure notifications (ERROR level)

With debug logging enabled (`spark.shuffle.streaming.debug=true`), log volume increases
significantly and includes:

- Buffer allocation/deallocation events
- Acknowledgment processing details
- Backpressure signal details
- Memory monitoring snapshots

Debug logging should only be enabled for troubleshooting specific issues.

# Tuning Recommendations

This section provides guidelines for tuning streaming shuffle for different workload types.

## Workload-Specific Recommendations

### Small Shuffles (<100MB)

For small shuffles, the overhead of streaming setup may outweigh benefits:

```
spark.shuffle.streaming.enabled=false
```

Consider using streaming shuffle only if:
- Many small shuffles occur frequently (amortizes setup cost)
- Latency is more important than throughput

### Medium Shuffles (100MB - 1GB)

Default settings are optimized for medium shuffles:

```
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.bufferSizePercent=20
spark.shuffle.streaming.spillThreshold=80
```

Monitor `spillCount` and `bufferUtilizationPercent` to verify defaults work well.

### Large Shuffles (>1GB)

Large shuffles may benefit from increased buffer allocation:

```
spark.shuffle.streaming.bufferSizePercent=30
spark.shuffle.streaming.spillThreshold=85
```

This allows more data to accumulate before spilling, improving streaming efficiency.
Monitor memory metrics to ensure adequate headroom.

### Memory-Constrained Environments

When executor memory is limited:

```
spark.shuffle.streaming.bufferSizePercent=10
spark.shuffle.streaming.spillThreshold=70
```

Lower buffer allocation and earlier spill triggers prevent OOM:
- Reduces memory footprint
- Trades off some streaming efficiency for stability

### Network-Limited Environments

When network bandwidth is constrained:

```
spark.shuffle.streaming.maxBandwidthMBps=100
spark.shuffle.streaming.spillThreshold=75
```

Bandwidth limiting prevents network saturation:
- Prevents streaming from dominating network
- Lower spill threshold accommodates backpressure

### Latency-Sensitive Workloads

For workloads requiring fast failure detection:

```
spark.shuffle.streaming.connectionTimeout=2s
spark.shuffle.streaming.heartbeatInterval=5s
```

Aggressive timeouts detect failures quickly:
- Faster recovery from failures
- May cause false positives during GC or network hiccups

## Tuning Process

1. **Start with Defaults**: Begin with default configuration and measure baseline metrics
2. **Monitor Key Metrics**: Watch `bufferUtilizationPercent`, `spillCount`, `backpressureEvents`
3. **Identify Bottlenecks**:
   - High buffer utilization → Consider increasing `bufferSizePercent`
   - Frequent spills → Consider increasing `bufferSizePercent` or lowering partition count
   - Backpressure events → Check consumer processing speed, network bandwidth
4. **Iterate Carefully**: Make one change at a time and measure impact
5. **Test at Scale**: Verify tuning with production-scale data volumes

## Configuration Interactions

Be aware of interactions between configuration options:

| Setting A | Setting B | Interaction |
|-----------|-----------|-------------|
| High `bufferSizePercent` | Low `spillThreshold` | Frequent spills despite large buffers |
| Low `connectionTimeout` | Long GC pauses | False failure detection |
| High `maxBandwidthMBps` | Limited network | Saturation, then backpressure |
| High partition count | Fixed buffer memory | Very small per-partition buffers |

For general Spark tuning guidance, see the [Tuning Guide](tuning.html).

# Troubleshooting

This section addresses common issues and their solutions.

## High Spill Rates

**Symptoms**:
- `StreamingShuffle.spillCount` increasing rapidly
- High disk I/O on executors
- Shuffle performance not improving vs. sort-based

**Causes**:
- `bufferSizePercent` too low for shuffle volume
- Too many partitions creating many small buffers
- Memory pressure from other operations

**Solutions**:
1. Increase `spark.shuffle.streaming.bufferSizePercent` (up to 50)
2. Reduce partition count via `spark.sql.shuffle.partitions`
3. Increase executor memory
4. If persistent, consider disabling streaming shuffle for this workload

## Excessive Backpressure Events

**Symptoms**:
- `StreamingShuffle.backpressureEvents` increasing
- Producers frequently throttled
- Higher than expected shuffle latency

**Causes**:
- Slow consumer processing (CPU-bound reduce tasks)
- Network bandwidth insufficient for shuffle volume
- Consumer memory pressure causing slow acknowledgments

**Solutions**:
1. Check consumer task CPU utilization - optimize reduce-side processing
2. Set appropriate `spark.shuffle.streaming.maxBandwidthMBps`
3. Increase consumer executor resources
4. Consider if sort-based shuffle would be more efficient

## Connection Timeouts

**Symptoms**:
- Frequent connection timeout errors in logs
- `StreamingShuffle.partialReadInvalidations` increasing
- Task retries due to shuffle failures

**Causes**:
- Network issues between executors
- `connectionTimeout` too aggressive for environment
- Executor overload causing slow responses

**Solutions**:
1. Verify network connectivity between executors
2. Increase `spark.shuffle.streaming.connectionTimeout` (try 10s or 15s)
3. Increase `spark.shuffle.streaming.heartbeatInterval` proportionally
4. Check for executor overload (CPU, memory pressure)

## Frequent Fallback to Sort-Based Shuffle

**Symptoms**:
- `StreamingShuffle.fallbackCount` non-zero
- Log messages indicating fallback conditions
- Performance similar to sort-based shuffle

**Causes**:
- Consumer/producer rate mismatch (2x slower for >60s)
- Memory pressure triggering OOM protection
- Network saturation exceeding 90%

**Solutions**:
1. Review fallback reason in logs
2. For rate mismatch: optimize consumer processing or accept sort-based for this workload
3. For OOM: increase `bufferSizePercent` or executor memory
4. For network saturation: set `maxBandwidthMBps` below saturation threshold

## Checksum Validation Failures

**Symptoms**:
- `StreamingShuffle.checksumValidationFailures` non-zero
- Retransmission requests in logs
- Slightly elevated shuffle latency

**Causes**:
- Network corruption (rare)
- Memory corruption (very rare)
- Hardware issues

**Solutions**:
1. Single occurrences are recovered automatically - no action needed
2. Persistent failures indicate hardware issues - check network and memory
3. Run memory diagnostics on affected executors
4. Consider replacing hardware showing repeated failures

## Debugging Steps

For issues not covered above:

1. **Enable Debug Logging**:
   ```
   spark.shuffle.streaming.debug=true
   ```
   Capture logs for detailed analysis (remember to disable after troubleshooting)

2. **Check Metrics**:
   - Review all StreamingShuffle.* metrics
   - Compare with historical baselines
   - Identify anomalous patterns

3. **Examine Task Timelines**:
   - Use Spark UI Stages tab
   - Look for shuffle-related delays
   - Identify slow tasks or executors

4. **Network Analysis**:
   - Monitor network utilization during shuffle
   - Check for packet loss or high latency
   - Verify executor connectivity

5. **Memory Analysis**:
   - Review executor memory utilization
   - Check GC logs for long pauses
   - Verify unified memory pool health

# Best Practices

Follow these best practices for optimal streaming shuffle performance:

## Start with Defaults

The default configuration is tuned for common workloads:

- `bufferSizePercent=20` - Balanced memory allocation
- `spillThreshold=80` - Room for pressure handling
- `connectionTimeout=5s` - Fast failure detection
- `heartbeatInterval=10s` - Reliable liveness

Only tune after measuring baseline performance.

## Monitor Before Tuning

Always collect metrics before making changes:

1. Run representative workloads
2. Collect all StreamingShuffle.* metrics
3. Establish baseline performance
4. Compare streaming vs. sort-based shuffle

## Test with Representative Workloads

Validate configuration with production-like data:

- Use realistic data volumes
- Include typical partition counts
- Test failure scenarios
- Measure end-to-end latency

## Consider Fallback Behavior

Plan for automatic fallback in production:

- Fallback is a feature, not a failure
- Monitor `fallbackCount` for patterns
- Some workloads are better suited for sort-based
- Don't force streaming for incompatible workloads

## Use Sort-Based for Very Small Shuffles

Streaming overhead may exceed benefits for small data:

- Shuffles <100MB often better with sort-based
- Setup cost amortized over larger volumes
- Consider disabling for known small shuffles

## Right-Size Partitions

Partition count significantly impacts streaming efficiency:

- Too many partitions → tiny per-partition buffers
- Too few partitions → limited parallelism
- Target: 100MB-1GB per partition for streaming
- Adjust `spark.sql.shuffle.partitions` accordingly

## Monitor Memory Holistically

Streaming buffers share execution memory pool:

- Other operations compete for same memory
- High cache usage reduces streaming headroom
- Balance `spark.memory.fraction` and `spark.memory.storageFraction`

## Plan for Failures

Design applications with failure tolerance:

- Streaming shuffle handles all failures gracefully
- Zero data loss under all scenarios
- Task retry overhead is acceptable
- Lineage enables full recomputation if needed

# Compatibility

## Version Requirements

Streaming shuffle requires:

- **Apache Spark**: 4.2.0 or later
- **Java**: 17 or later (for CRC32C hardware acceleration)
- **Scala**: 2.13

Attempting to use streaming shuffle on earlier versions will result in configuration
errors at startup.

## Application Code Compatibility

No changes are required to existing application code:

- Same RDD, DataFrame, and Dataset APIs
- Shuffle operations automatically use streaming when configured
- Transparent to application logic
- Same semantics and guarantees as sort-based shuffle

## Configuration Restart Requirement

Streaming shuffle configuration cannot be changed at runtime:

- Configuration is read at SparkContext creation
- Changes require application restart
- Hot reload is not supported in the initial release

## External Shuffle Service Compatibility

Streaming shuffle is compatible with the external shuffle service:

- Spilled data is accessible via external shuffle service
- Fallback to sort-based uses external shuffle service normally
- No additional external shuffle service configuration required

When external shuffle service is enabled:

1. Streaming data flows directly between executors (not through ESS)
2. Spilled data is registered with BlockManager
3. On executor loss, spilled data is retrievable via ESS
4. Fallback shuffle data uses ESS as normal

## Push-Based Shuffle Compatibility

Streaming shuffle coexists with push-based shuffle:

- Push-based shuffle (`spark.shuffle.push.enabled`) operates independently
- Both optimizations can be enabled simultaneously
- Push-based handles shuffle file consolidation
- Streaming handles data flow between map and reduce

Configuration for both:

```
spark.shuffle.manager=streaming
spark.shuffle.streaming.enabled=true
spark.shuffle.push.enabled=true
```

Note: Push-based shuffle primarily benefits external shuffle service scenarios, while
streaming shuffle benefits direct executor-to-executor scenarios.

## Kubernetes and YARN Compatibility

Streaming shuffle works with all supported cluster managers:

- **Kubernetes**: Full support with standard networking
- **YARN**: Full support with all schedulers
- **Standalone**: Full support
- **Local**: Full support for development/testing

No cluster manager-specific configuration is required.

## Encryption Compatibility

Streaming shuffle respects Spark's encryption settings:

- **Network encryption**: Enabled via `spark.network.crypto.enabled`
- **Spill encryption**: Enabled via `spark.io.encryption.enabled`

When encryption is enabled:

1. Streaming data is encrypted in transit
2. Spilled data is encrypted at rest
3. Performance overhead similar to sort-based shuffle

## Authentication Compatibility

Streaming shuffle respects Spark's authentication settings:

- Authentication enabled via `spark.authenticate`
- All streaming connections authenticate using Spark's mechanisms
- No additional authentication configuration required

## Known Limitations

Current limitations in the initial release:

1. **No dynamic reconfiguration**: Configuration requires restart
2. **Memory overhead**: Token bucket and monitoring threads consume some memory
3. **Network-bound workloads**: May not benefit if already network-saturated
4. **Very small shuffles**: Overhead may exceed benefits for <100MB shuffles

These limitations may be addressed in future releases.
