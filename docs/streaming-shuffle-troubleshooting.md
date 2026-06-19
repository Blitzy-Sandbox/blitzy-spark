---
layout: global
title: Streaming Shuffle Troubleshooting
displayTitle: Streaming Shuffle Troubleshooting
description: Troubleshooting guide for Spark Streaming Shuffle SPARK_VERSION_SHORT
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

The streaming shuffle feature provides an opt-in alternative shuffle mechanism that streams data directly 
from map (producer) tasks to reduce (consumer) tasks, targeting 30-50% latency reduction for shuffle-heavy 
workloads. While streaming shuffle offers significant performance benefits, it introduces new operational 
considerations that require monitoring and troubleshooting.

This guide covers common issues that may arise when using streaming shuffle, how to interpret streaming 
shuffle telemetry, debugging procedures, and understanding the automatic fallback behavior.

## When Streaming Shuffle Issues Occur

Streaming shuffle issues typically manifest in the following scenarios:

- **High memory pressure**: When buffer allocation exceeds available executor memory
- **Consumer-producer imbalance**: When consumers cannot keep up with producer data rates
- **Network instability**: When network conditions cause timeouts or data corruption
- **Resource contention**: When multiple concurrent shuffles compete for limited resources

## General Debugging Approach

When troubleshooting streaming shuffle issues, follow this systematic approach:

1. **Check executor logs** for streaming shuffle-related warnings and errors
2. **Review Spark UI metrics** for streaming shuffle telemetry
3. **Verify configuration** settings match workload characteristics
4. **Enable debug logging** for detailed operational visibility
5. **Monitor fallback events** to identify automatic degradation triggers

# Common Issues

## Memory Pressure Issues

### Symptoms

- High GC (garbage collection) activity on executors
- OutOfMemoryError exceptions in executor logs
- Frequent disk spills indicated by `shuffle.streaming.spillCount` metric
- Task failures with memory-related error messages
- Degraded shuffle performance despite streaming shuffle enabled

### Causes

Memory pressure in streaming shuffle typically occurs due to:

- **Buffer size too large**: The `spark.shuffle.streaming.bufferSizePercent` setting allocates too much memory
- **Too many concurrent partitions**: High partition counts multiply buffer memory requirements
- **Large record sizes**: Individual records consume significant buffer space
- **Insufficient executor memory**: Base executor memory allocation too small for workload
- **Multiple concurrent shuffles**: Several shuffle operations compete for buffer space

### Solutions

<table class="spark-config">
  <thead>
    <tr>
      <th>Solution</th>
      <th>Configuration Change</th>
      <th>Expected Impact</th>
    </tr>
  </thead>
  <tr>
    <td>Reduce buffer allocation</td>
    <td><code>spark.shuffle.streaming.bufferSizePercent=10</code> (reduce from default 20)</td>
    <td>Less memory for buffers, more frequent spills, but prevents OOM</td>
  </tr>
  <tr>
    <td>Lower spill threshold</td>
    <td><code>spark.shuffle.streaming.spillThreshold=70</code> (reduce from default 80)</td>
    <td>Earlier spills to disk, preserving memory headroom</td>
  </tr>
  <tr>
    <td>Increase executor memory</td>
    <td><code>spark.executor.memory=8g</code> (increase based on workload)</td>
    <td>More absolute memory available for streaming buffers</td>
  </tr>
  <tr>
    <td>Reduce partition count</td>
    <td><code>spark.sql.shuffle.partitions=100</code> (reduce if over-partitioned)</td>
    <td>Fewer buffers to manage, more memory per partition</td>
  </tr>
</table>

**Example configuration for memory-constrained environments:**

```properties
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.bufferSizePercent=10
spark.shuffle.streaming.spillThreshold=65
spark.executor.memory=8g
spark.sql.shuffle.partitions=100
```

## Slow Consumer Issues

### Symptoms

- Backpressure warnings in executor logs: `StreamingShuffleWriter: Backpressure signal received from consumer`
- High `shuffle.streaming.backpressureEvents` metric count
- Automatic fallback to sort-based shuffle triggered
- Producer tasks completing significantly faster than consumer tasks
- Increasing buffer utilization over time

### Causes

Slow consumer issues occur when reduce (consumer) tasks cannot process data as fast as map (producer) 
tasks generate it:

- **Consumer-side computation**: Complex aggregations or joins in reduce phase
- **Data skew**: Uneven partition sizes causing some consumers to process more data
- **Consumer resource constraints**: Insufficient CPU or memory on consumer executors
- **Network asymmetry**: Consumer executors on slower network segments
- **GC pauses**: Long garbage collection pauses on consumer executors

### Solutions

<table class="spark-config">
  <thead>
    <tr>
      <th>Solution</th>
      <th>Action</th>
      <th>Expected Impact</th>
    </tr>
  </thead>
  <tr>
    <td>Optimize consumer operations</td>
    <td>Review and optimize reduce-side computations</td>
    <td>Faster consumer processing, reduced backpressure</td>
  </tr>
  <tr>
    <td>Address data skew</td>
    <td>Use salting or adaptive query execution to balance partitions</td>
    <td>Even distribution of work across consumers</td>
  </tr>
  <tr>
    <td>Increase consumer resources</td>
    <td>Add more executor cores or memory for consumer tasks</td>
    <td>Higher consumer throughput capacity</td>
  </tr>
  <tr>
    <td>Limit producer bandwidth</td>
    <td><code>spark.shuffle.streaming.maxBandwidthMBps=100</code></td>
    <td>Throttles producers to match consumer capacity</td>
  </tr>
  <tr>
    <td>Reduce partition count</td>
    <td>Decrease <code>spark.sql.shuffle.partitions</code></td>
    <td>Fewer concurrent consumers, more data per consumer</td>
  </tr>
</table>

**Identifying data skew:**

Check the Spark UI stages page for partition size distribution. If some partitions are significantly 
larger than others, consider using adaptive query execution:

```properties
spark.sql.adaptive.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
```

## Network Timeout Issues

### Symptoms

- Connection timeout errors in executor logs: `StreamingShuffleReader: Connection timeout after 5000ms`
- `FetchFailedException` with streaming shuffle block references
- Increasing `shuffle.streaming.retransmissions` metric
- Intermittent task failures followed by retries
- Executor loss events during shuffle operations

### Causes

Network timeout issues in streaming shuffle are caused by:

- **Network congestion**: High network utilization across the cluster
- **Executor GC pauses**: Long garbage collection pauses preventing heartbeat responses
- **Network infrastructure issues**: Faulty switches, cables, or network configuration
- **Cross-datacenter shuffles**: High latency between executor locations
- **Firewall or security settings**: Network policies blocking shuffle traffic

### Solutions

<table class="spark-config">
  <thead>
    <tr>
      <th>Solution</th>
      <th>Configuration Change</th>
      <th>Expected Impact</th>
    </tr>
  </thead>
  <tr>
    <td>Increase connection timeout</td>
    <td><code>spark.shuffle.streaming.connectionTimeout=10s</code> (increase from default 5s)</td>
    <td>More tolerance for temporary network delays</td>
  </tr>
  <tr>
    <td>Adjust heartbeat interval</td>
    <td><code>spark.shuffle.streaming.heartbeatInterval=15s</code> (increase from default 10s)</td>
    <td>Less sensitive to brief executor pauses</td>
  </tr>
  <tr>
    <td>Increase retry attempts</td>
    <td><code>spark.shuffle.streaming.maxRetries=10</code> (increase from default 5)</td>
    <td>More retry opportunities before failure</td>
  </tr>
  <tr>
    <td>Adjust retry wait time</td>
    <td><code>spark.shuffle.streaming.retryWait=2s</code> (increase from default 1s)</td>
    <td>Longer backoff between retries for network recovery</td>
  </tr>
  <tr>
    <td>Limit bandwidth usage</td>
    <td><code>spark.shuffle.streaming.maxBandwidthMBps=500</code></td>
    <td>Reduces network congestion from shuffle traffic</td>
  </tr>
</table>

**Example configuration for unreliable networks:**

```properties
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.connectionTimeout=15s
spark.shuffle.streaming.heartbeatInterval=20s
spark.shuffle.streaming.maxRetries=10
spark.shuffle.streaming.retryWait=3s
```

**Diagnosing network issues:**

1. Check cluster network utilization using infrastructure monitoring tools
2. Review executor GC logs for long pause times (`-XX:+PrintGCDetails -XX:+PrintGCTimeStamps`)
3. Verify network connectivity between executor hosts using ping/traceroute
4. Check for network timeouts in Netty transport logs

## Checksum Validation Failures

### Symptoms

- Retransmission request logs: `StreamingShuffleReader: Checksum mismatch, requesting retransmission`
- High `shuffle.streaming.retransmissions` metric without network timeout errors
- Data corruption warnings in executor logs
- Increased shuffle data transfer volume due to retries
- Occasional task failures with data validation errors

### Causes

Checksum validation failures indicate data corruption during transfer:

- **Network bit errors**: Faulty network hardware causing data corruption
- **Memory corruption**: Hardware memory errors affecting buffer contents
- **Disk issues**: Faulty storage affecting spilled data
- **Software bugs**: Rare edge cases in serialization or compression
- **Concurrent modification**: Race conditions in buffer management

### Solutions

<table class="spark-config">
  <thead>
    <tr>
      <th>Solution</th>
      <th>Action</th>
      <th>Expected Impact</th>
    </tr>
  </thead>
  <tr>
    <td>Verify network health</td>
    <td>Run network diagnostics, check switch error counters</td>
    <td>Identify and fix network hardware issues</td>
  </tr>
  <tr>
    <td>Check disk health</td>
    <td>Review disk SMART data, check for I/O errors in system logs</td>
    <td>Identify and replace failing storage devices</td>
  </tr>
  <tr>
    <td>Test memory integrity</td>
    <td>Run memory diagnostics on executor hosts</td>
    <td>Identify hosts with memory hardware issues</td>
  </tr>
  <tr>
    <td>Enable compression</td>
    <td><code>spark.shuffle.compress=true</code></td>
    <td>Compression may help detect corruption earlier</td>
  </tr>
  <tr>
    <td>Blacklist problematic hosts</td>
    <td>Use executor blacklisting to avoid faulty hardware</td>
    <td>Prevents scheduling on known-bad hosts</td>
  </tr>
</table>

**Investigating checksum failures:**

1. Correlate checksum failures with specific executor hosts
2. Check if failures cluster on specific network paths
3. Review system logs (dmesg, syslog) for hardware errors
4. Monitor `shuffle.streaming.retransmissions` for trends over time

# Telemetry Interpretation

Streaming shuffle exposes several metrics that help diagnose operational issues. These metrics are 
available through the Spark UI, REST API, and metrics sinks (JMX, Prometheus, etc.).

## Streaming Shuffle Metrics

<table class="spark-config">
  <thead>
    <tr>
      <th>Metric Name</th>
      <th>Type</th>
      <th>Description</th>
      <th>Healthy Range</th>
    </tr>
  </thead>
  <tr>
    <td><code>shuffle.streaming.bufferUtilizationPercent</code></td>
    <td>Gauge</td>
    <td>Current percentage of allocated streaming buffer memory in use. High values indicate 
    memory pressure and potential spill risk.</td>
    <td>30-70%</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.spillCount</code></td>
    <td>Counter</td>
    <td>Total number of times streaming buffers have been spilled to disk due to memory 
    pressure. Frequent spills indicate buffer sizing or memory issues.</td>
    <td>&lt;1 per shuffle stage</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.spillBytes</code></td>
    <td>Counter</td>
    <td>Total bytes spilled to disk from streaming buffers. Large values indicate significant 
    disk I/O overhead from memory pressure.</td>
    <td>Context-dependent</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.backpressureEvents</code></td>
    <td>Counter</td>
    <td>Number of backpressure signals sent from consumers to producers. Indicates consumer 
    unable to keep pace with producer data rates.</td>
    <td>&lt;10 per shuffle stage</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.partialReads</code></td>
    <td>Counter</td>
    <td>Number of partial read operations where consumers received data before shuffle 
    completion. Higher values indicate effective streaming overlap.</td>
    <td>Higher is better for streaming</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.retransmissions</code></td>
    <td>Counter</td>
    <td>Number of block retransmission requests due to checksum failures or network issues. 
    Non-zero values indicate data integrity or network problems.</td>
    <td>0</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.bytesStreamed</code></td>
    <td>Counter</td>
    <td>Total bytes transferred via streaming shuffle (excluding retransmissions). Used to 
    calculate streaming efficiency.</td>
    <td>Context-dependent</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.fallbackCount</code></td>
    <td>Counter</td>
    <td>Number of times streaming shuffle automatically fell back to sort-based shuffle. 
    Non-zero indicates streaming shuffle eligibility issues.</td>
    <td>0</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.connectionTimeouts</code></td>
    <td>Counter</td>
    <td>Number of connection timeout events during streaming shuffle. Indicates network 
    reliability issues or executor health problems.</td>
    <td>0</td>
  </tr>
  <tr>
    <td><code>shuffle.streaming.avgLatencyMs</code></td>
    <td>Gauge</td>
    <td>Average latency in milliseconds for streaming shuffle data transfer. Lower values 
    indicate effective streaming performance.</td>
    <td>&lt;100ms for local, &lt;500ms for remote</td>
  </tr>
</table>

## Interpreting Metric Patterns

### Healthy Streaming Shuffle

A healthy streaming shuffle operation shows:

- `bufferUtilizationPercent` stable between 30-70%
- `spillCount` at 0 or very low
- `backpressureEvents` at 0 or very low
- `partialReads` higher than 0 (indicates effective streaming)
- `retransmissions` at 0
- `fallbackCount` at 0

### Memory Pressure Pattern

Memory pressure is indicated by:

- `bufferUtilizationPercent` consistently above 80%
- `spillCount` increasing over time
- `spillBytes` significantly high relative to shuffle data size
- Possible increase in `fallbackCount`

### Consumer Slowdown Pattern

Consumer slowdown shows:

- `backpressureEvents` increasing steadily
- `bufferUtilizationPercent` rising on producer side
- `spillCount` may increase as producers buffer unacknowledged data
- Potential `fallbackCount` increase if sustained for >60 seconds

### Network Issues Pattern

Network problems appear as:

- `connectionTimeouts` > 0
- `retransmissions` > 0
- `avgLatencyMs` significantly higher than baseline
- Possible `fallbackCount` increase

## Accessing Metrics

### Spark UI

Navigate to the Spark UI and select the **Executors** tab to view per-executor streaming shuffle 
metrics. The **Stages** tab shows aggregate streaming shuffle metrics for each stage.

### REST API

Query streaming shuffle metrics via the REST API:

```bash
# Get executor metrics including streaming shuffle stats
curl http://<driver-node>:4040/api/v1/applications/<app-id>/executors

# Get stage metrics with streaming shuffle details
curl http://<driver-node>:4040/api/v1/applications/<app-id>/stages/<stage-id>
```

### JMX

Streaming shuffle metrics are exposed via JMX under the namespace:

```
org.apache.spark.executor.streaming.shuffle.*
```

Example JMX access:

```bash
# Using jconsole or JMX client
jconsole <executor-host>:<jmx-port>
# Navigate to: org.apache.spark.executor.streaming.shuffle
```

### Prometheus/Metrics Sinks

Configure Spark metrics sink to export streaming shuffle metrics:

```properties
# In metrics.properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
```

# Debugging Procedures

## Enabling Debug Logging

For detailed operational visibility into streaming shuffle operations, enable debug logging:

### Method 1: Spark Configuration

Add to your Spark application configuration:

```properties
spark.shuffle.streaming.debug=true
```

Or via spark-submit:

```bash
spark-submit \
  --conf spark.shuffle.streaming.debug=true \
  --conf spark.shuffle.streaming.enabled=true \
  your-application.jar
```

### Method 2: Log4j Configuration

Add to your `log4j2.properties` file:

```properties
# Enable debug logging for streaming shuffle components
logger.streaming_shuffle.name = org.apache.spark.shuffle.streaming
logger.streaming_shuffle.level = debug

# For more verbose output including network details
logger.streaming_shuffle_writer.name = org.apache.spark.shuffle.streaming.StreamingShuffleWriter
logger.streaming_shuffle_writer.level = trace

logger.streaming_shuffle_reader.name = org.apache.spark.shuffle.streaming.StreamingShuffleReader
logger.streaming_shuffle_reader.level = trace

logger.backpressure.name = org.apache.spark.shuffle.streaming.BackpressureProtocol
logger.backpressure.level = debug
```

### Debug Log Output Examples

With debug logging enabled, you'll see entries like:

```
DEBUG StreamingShuffleWriter: Allocated 209715200 bytes for streaming buffers (20% of executor memory)
DEBUG StreamingShuffleWriter: Partition 42: buffered 1048576 bytes, buffer utilization 15%
DEBUG BackpressureProtocol: Sending heartbeat to consumer executor-1, RTT: 12ms
DEBUG StreamingShuffleReader: Received partial data for partition 42: 524288 bytes (50% complete)
WARN  StreamingShuffleWriter: Backpressure signal received from consumer executor-1, throttling
DEBUG MemorySpillManager: Memory threshold 80% reached, initiating spill for partitions [42, 87, 103]
INFO  StreamingShuffleManager: Fallback triggered: consumer 2.5x slower than producer for 65 seconds
```

## Using Spark UI for Diagnosis

### Stages Tab Analysis

1. Navigate to the **Stages** tab in Spark UI
2. Select a shuffle stage to view details
3. Look for the **Streaming Shuffle Metrics** section showing:
   - Total bytes streamed vs. spilled
   - Backpressure event count
   - Retransmission count
   - Fallback occurrence (if any)

### Executors Tab Analysis

1. Navigate to the **Executors** tab
2. Review per-executor streaming shuffle statistics:
   - Buffer utilization percentage
   - Active streaming connections
   - Spill statistics
3. Identify executors with anomalous metrics (high spills, timeouts)

### Tasks Tab Analysis

1. From a stage, click on the **Tasks** tab
2. Sort by shuffle write/read time to identify slow tasks
3. Look for tasks with:
   - High shuffle write time (producer issues)
   - High shuffle read time (consumer issues)
   - Failed attempts with streaming shuffle errors

## Checking Executor Logs

### Log Locations

Executor logs containing streaming shuffle information are typically found at:

- **YARN**: `yarn logs -applicationId <app-id>`
- **Kubernetes**: `kubectl logs <executor-pod>`
- **Standalone**: `$SPARK_HOME/work/<app-id>/<executor-id>/stderr`

### Key Log Patterns to Search

```bash
# Search for streaming shuffle initialization
grep "StreamingShuffleManager" executor-*.log

# Search for memory spill events
grep "MemorySpillManager" executor-*.log

# Search for backpressure events
grep -i "backpressure" executor-*.log

# Search for fallback events
grep -i "fallback" executor-*.log | grep -i "streaming"

# Search for timeout errors
grep -i "timeout" executor-*.log | grep -i "shuffle"

# Search for retransmission requests
grep -i "retransmission\|checksum" executor-*.log
```

### Example Log Analysis Session

```bash
# 1. Get application logs
yarn logs -applicationId application_1234567890_0001 > app.log

# 2. Check for streaming shuffle activation
grep "StreamingShuffleManager: Streaming shuffle enabled" app.log

# 3. Look for memory issues
grep "MemorySpillManager\|OutOfMemory\|GC" app.log | head -50

# 4. Check for fallback events
grep "Fallback triggered" app.log

# 5. Summarize backpressure events
grep -c "Backpressure signal" app.log
```

## Analyzing MapOutputTracker

The MapOutputTracker maintains shuffle block location information. For streaming shuffle debugging:

### Accessing MapOutputTracker Status

Via Spark driver logs:

```bash
grep "MapOutputTracker" driver.log | grep -i "streaming"
```

### Key Information

- **Block locations**: Where streaming shuffle blocks are being served from
- **Shuffle registration**: Confirmation that streaming shuffle handle was registered
- **Block availability**: When partial blocks become available for streaming reads

### Debug via SparkContext

For interactive debugging in spark-shell:

```scala
// Get MapOutputTracker reference
val tracker = sc.env.mapOutputTracker

// Check shuffle status (requires internal access)
// Note: This is for debugging only, not production use
import org.apache.spark.MapOutputTrackerMaster
val master = tracker.asInstanceOf[MapOutputTrackerMaster]

// View registered shuffles
master.shuffleStatuses.keys.foreach(println)
```

# Fallback Behavior Explanation

Streaming shuffle includes automatic fallback mechanisms to ensure reliability and prevent performance 
degradation in adverse conditions. Understanding fallback behavior is essential for optimizing 
streaming shuffle deployments.

## Automatic Fallback Triggers

Streaming shuffle automatically falls back to sort-based shuffle under the following conditions:

<table class="spark-config">
  <thead>
    <tr>
      <th>Condition</th>
      <th>Threshold</th>
      <th>Rationale</th>
    </tr>
  </thead>
  <tr>
    <td>Consumer sustained slowdown</td>
    <td>Consumer 2x slower than producer for &gt;60 seconds</td>
    <td>Streaming benefits are negated when consumers cannot keep pace; sort-based shuffle 
    allows consumers to read at their own rate after materialization</td>
  </tr>
  <tr>
    <td>Memory pressure</td>
    <td>Unable to allocate minimum buffer memory</td>
    <td>Prevents OutOfMemoryError by falling back to disk-based shuffle that uses less memory</td>
  </tr>
  <tr>
    <td>Network saturation</td>
    <td>&gt;90% network link capacity utilization</td>
    <td>Streaming shuffle adds latency under severe congestion; sort-based shuffle better 
    utilizes available bandwidth in bursts</td>
  </tr>
  <tr>
    <td>Version mismatch</td>
    <td>Producer/consumer protocol version incompatible</td>
    <td>Ensures correctness in mixed-version deployments during rolling upgrades</td>
  </tr>
  <tr>
    <td>Serializer incompatibility</td>
    <td>Non-relocatable serializer detected</td>
    <td>Some serializers are incompatible with streaming shuffle's buffer management</td>
  </tr>
</table>

## Identifying Fallback Events

### Via Logs

Fallback events are logged at INFO level:

```
INFO  StreamingShuffleManager: Fallback triggered for shuffle 5: consumer 2.1x slower than producer for 62 seconds
INFO  StreamingShuffleManager: Fallback triggered for shuffle 8: memory pressure - unable to allocate buffer
INFO  StreamingShuffleManager: Fallback triggered for shuffle 12: network saturation at 94%
INFO  StreamingShuffleManager: Fallback triggered for shuffle 15: version mismatch detected
```

### Via Metrics

Monitor the `shuffle.streaming.fallbackCount` metric:

```bash
# Via REST API
curl http://<driver>:4040/api/v1/applications/<app-id>/executors | jq '.[].streamingShuffleMetrics.fallbackCount'
```

### Via Spark UI

In the Stages tab, stages that experienced fallback show:

- "Streaming Shuffle: Fallback to Sort" indicator
- Fallback reason in stage details
- Metrics comparison between streaming and sort-based portions

## Preventing Unnecessary Fallbacks

To maximize streaming shuffle benefits while avoiding unnecessary fallbacks:

### Tuning for Consumer Slowdown Prevention

```properties
# Increase tolerance for consumer lag
spark.shuffle.streaming.maxBandwidthMBps=200
# Throttle producers to prevent overwhelming consumers

# Or optimize consumer side
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
# Adaptive execution can reduce consumer workload
```

### Tuning for Memory Pressure Prevention

```properties
# Conservative buffer allocation
spark.shuffle.streaming.bufferSizePercent=15

# Earlier spill threshold
spark.shuffle.streaming.spillThreshold=70

# Adequate executor memory
spark.executor.memory=12g
spark.executor.memoryOverhead=2g
```

### Tuning for Network Stability

```properties
# Limit bandwidth to prevent saturation
spark.shuffle.streaming.maxBandwidthMBps=500
# Adjust based on network capacity

# Longer timeouts for higher-latency networks
spark.shuffle.streaming.connectionTimeout=10s
spark.shuffle.streaming.heartbeatInterval=15s
```

## Fallback Behavior During Shuffle Operations

When fallback is triggered mid-shuffle:

1. **In-flight data is preserved**: Data already buffered is written to disk
2. **Consumers switch to disk reads**: Remaining data is read from materialized shuffle files
3. **No data loss occurs**: The fallback mechanism ensures all records are processed
4. **Metrics reflect hybrid operation**: Both streaming and sort-based metrics are reported

### Recovery After Fallback

Fallback is determined per-shuffle, not globally. Subsequent shuffles may still use streaming 
shuffle if conditions improve. To force streaming shuffle re-evaluation:

- Monitor the `shuffle.streaming.fallbackCount` metric for trends
- Review fallback reasons in logs to address root causes
- Adjust configuration based on observed fallback triggers

## Disabling Automatic Fallback (Advanced)

In rare cases where you want to prevent automatic fallback (not recommended for production):

```properties
# Force streaming shuffle without fallback (use with caution)
# This may result in failures instead of graceful degradation
spark.shuffle.streaming.fallback.enabled=false
```

**Warning**: Disabling automatic fallback may cause job failures under adverse conditions. 
Only use this setting for debugging or in controlled environments.

# Additional Resources

- [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) - Design documentation
- [Streaming Shuffle Tuning](streaming-shuffle-tuning.html) - Performance optimization guide
- [Configuration Reference](configuration.html#streaming-shuffle) - Complete configuration options
- [Monitoring and Instrumentation](monitoring.html) - General Spark monitoring guide
- [Tuning Guide](tuning.html) - General Spark performance tuning
