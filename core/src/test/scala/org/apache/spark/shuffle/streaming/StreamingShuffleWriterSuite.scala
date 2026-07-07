/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.streaming

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyBoolean, eq => meq}
import org.mockito.Mockito.{atLeastOnce, reset, verify, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.memory.{MemoryTestingUtils, UnifiedMemoryManager}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter
import org.apache.spark.storage.{BlockManager, StorageLevel}
import org.apache.spark.util.Utils

/**
 * Unit suite for [[StreamingShuffleWriter]], the streaming map-side shuffle writer selected by
 * `spark.shuffle.manager=streaming`. The suite is modeled on
 * `org.apache.spark.shuffle.sort.SortShuffleWriterSuite`: it mocks the [[BlockManager]] and the
 * [[ShuffleDependency]], drives the writer with an anonymous [[Partitioner]], and obtains a real
 * [[org.apache.spark.memory.TaskMemoryManager]] through
 * [[org.apache.spark.memory.MemoryTestingUtils.fakeTaskContext]] (the writer composes an inner
 * [[org.apache.spark.memory.MemoryConsumer]] that requires one).
 *
 * The tests assert the writer's PUBLIC contract rather than its private internals: the shuffle SPI
 * (`write` / `stop` / `getPartitionLengths`),
 * the visible `MemoryConsumer` spill surface `spill(size, trigger): Long`, and the published
 * per-partition buffer capacity `perPartitionBufferCapacityBytes` (which encodes the
 * `(execution memory * bufferPercent / 100) / numPartitions` budget with a 2 MB floor). Because the
 * v1 transport is a logging-only stub that puts no bytes on the wire, a dedicated test also proves
 * the [[StreamingShuffleManager]] delegates to the durable sort path while the transport is
 * stubbed, so a reported map status always corresponds to reducer-fetchable data.
 *
 * All collaborators are the real same-package production classes referenced (never redefined);
 * only the two Spark-owned integration points ([[BlockManager]] and [[ShuffleDependency]]) are
 * mocked, exactly as the sort-path template does.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  private val shuffleId = 0

  // Reduce-partition fan-out backing the mocked dependency's partitioner. It is a `var` because
  // individual tests vary it to drive the per-partition buffer-sizing floor: a large fan-out pushes
  // the raw per-partition share below 2 MB, while a single partition leaves it above the floor. The
  // anonymous partitioner reads this field on every call, so mutating it before building the writer
  // changes the number of partitions the writer sees at construction.
  private var numReducePartitions = 5

  private val partitioner = new Partitioner() {
    def numPartitions: Int = numReducePartitions
    def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numReducePartitions)
  }

  /**
   * The executor memory basis the writer sizes per-partition buffers against (finding M2). This
   * mirrors `StreamingShuffleWriter.executorMemoryBytes` exactly: the writer budgets against the
   * unified heap execution-memory pool ([[UnifiedMemoryManager.maxHeapMemory]]) and only falls back
   * to `maxOnHeapStorageMemory` for a non-unified manager. Computing the expected buffer capacity
   * from the same basis keeps the public-contract assertions on `perPartitionBufferCapacityBytes`
   * exact and independent of the JVM heap the suite happens to run under.
   */
  private def executionMemoryBytes: Long = sc.env.memoryManager match {
    case unified: UnifiedMemoryManager => unified.maxHeapMemory
    case other => other.maxOnHeapStorageMemory
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
  }

  /**
   * Re-stub the mocked [[ShuffleDependency]] so the writer resolves a valid partitioner and
   * serializer. Called from [[newWriter]] (rather than `beforeEach`) so the partitioner reflects
   * the [[numReducePartitions]] value the current test selected. Mirrors the sort-path template's
   * `resetDependency`, minus the row-based-checksum stubs the streaming writer never reads.
   */
  private def resetDependency(): Unit = {
    reset(dependency)
    when(dependency.partitioner).thenReturn(partitioner)
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
  }

  /**
   * Build a fully wired [[StreamingShuffleWriter]] with real streaming collaborators. Returns the
   * writer together with the [[StreamingShuffleMetrics]] instance it was given (so spill telemetry
   * can be asserted) and the [[org.apache.spark.TaskContext]] backing it (so its
   * shuffle-write metrics reporter can be inspected). The daemon-bearing collaborators
   * (backpressure protocol, spill manager) are intentionally left un-started: the writer only
   * uses their synchronous surfaces, so starting their background threads would add nothing but
   * flakiness to a pure writer unit test.
   */
  private def newWriter(
      mapId: Long,
      bufferSizePercent: Int,
      spillThreshold: Int = 80,
      maxBandwidthMBps: Int = 0) = {
    resetDependency()
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, bufferSizePercent, spillThreshold, maxBandwidthMBps)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val streamingConf = new StreamingShuffleConfig(conf)
    val metrics = new StreamingShuffleMetrics()
    val transport = new StreamingShuffleTransport(streamingConf)
    val rateLimiter = new TokenBucketRateLimiter(0, 1)
    val backpressure = new BackpressureProtocol(streamingConf, metrics, rateLimiter)
    val spillManager = new MemorySpillManager(streamingConf, blockManager, metrics)
    val writer = new StreamingShuffleWriter[Int, Int, Int](
      handle,
      mapId,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      blockManager,
      transport,
      spillManager,
      backpressure,
      metrics,
      streamingConf)
    (writer, metrics, context)
  }

  test("write empty iterator produces zero-length partitions") {
    numReducePartitions = 5
    val (writer, _, context) = newWriter(mapId = 1L, bufferSizePercent = 20)
    writer.write(Iterator.empty)
    val status = writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    val partitionLengths = writer.getPartitionLengths()
    assert(partitionLengths.length === numReducePartitions)
    assert(partitionLengths.forall(_ == 0L))
    assert(writeMetrics.recordsWritten === 0L)
    assert(writeMetrics.bytesWritten === 0L)
    // write() always publishes a MapStatus (even for an empty iterator), so a successful stop
    // returns Some(mapStatus); a failed stop returns None (covered by a dedicated test below).
    assert(status.isDefined)
  }

  test("write some records populates partition lengths and the records-written metric") {
    numReducePartitions = 5
    val (writer, _, context) = newWriter(mapId = 2L, bufferSizePercent = 20)
    val records = List((1, 10), (2, 20), (3, 30), (4, 40))
    writer.write(records.iterator)
    val status = writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    val partitionLengths = writer.getPartitionLengths()
    assert(partitionLengths.length === numReducePartitions)
    // v1 transport is a logging stub with no bytes on the wire, so assert the writer's own
    // accounting rather than any on-disk data file: every record contributes serialized bytes to
    // its partition length, and each record increments the write reporter's record count.
    assert(partitionLengths.sum > 0L)
    assert(writeMetrics.recordsWritten === records.size.toLong)
    assert(status.isDefined)
  }

  test("v1 StreamingShuffleManager delegates to the sort path while the transport is stubbed") {
    numReducePartitions = 5
    resetDependency()
    // Finding M11 / C1: a successful streaming write must never report a MapStatus for data that
    // was not durably materialized. The v1 wire transport is a logging-only stub
    // (StreamingShuffleTransport.isWireTransferAvailable == false), so even with streaming fully
    // requested the manager must delegate every shuffle to the inner SortShuffleManager, whose
    // writer/reader path produces durable, reducer-fetchable output. Streaming is enabled here so
    // the ONLY thing forcing the fallback is the stub transport, then prove both the registered
    // handle and the constructed writer come from the sort path (never the streaming components).
    // Use sc.getConf (not the suite's raw conf): it is a live clone carrying spark.app.id, which
    // the inner SortShuffleManager needs when it lazily loads its executor shuffle components on
    // the delegated getWriter call. Enable streaming on the clone so the sole cause of the fallback
    // is the stub transport.
    val streamingConf = sc.getConf.set("spark.shuffle.streaming.enabled", "true")
    val manager = new StreamingShuffleManager(streamingConf, isDriver = true)
    try {
      val handle = manager.registerShuffle(shuffleId, dependency)
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "v1 must delegate registration to the sort path while the wire transport is a stub")

      val context = MemoryTestingUtils.fakeTaskContext(sc.env)
      val writeMetrics = context.taskMetrics().shuffleWriteMetrics
      val writer = manager.getWriter[Int, Int](handle, 5L, context, writeMetrics)
      assert(!writer.isInstanceOf[StreamingShuffleWriter[_, _, _]],
        "v1 must serve writes from the durable sort path, not the stub streaming writer")
    } finally {
      manager.stop()
    }
  }

  test("per-partition buffer capacity applies the execution-memory budget and the 2 MB floor") {
    val twoMB = 2L * 1024L * 1024L
    val execMem = executionMemoryBytes

    // Below-floor case: a large fan-out drives the raw per-partition share under 2 MB, so the
    // writer must clamp the buffer capacity up to the floor. The fan-out is derived from the live
    // executor memory so the test holds regardless of the JVM heap the suite runs under.
    // The public `perPartitionBufferCapacityBytes` accessor (finding M10) is asserted directly
    // rather than reaching a private field.
    val belowPct = 20
    val belowBudget = math.max(0L, execMem * belowPct / 100L)
    numReducePartitions = math.max(2, (belowBudget / twoMB).toInt + 2)
    val belowRaw = belowBudget / numReducePartitions
    assert(belowRaw < twoMB, s"test setup must drive the raw per-partition share below the " +
      s"floor, but raw=$belowRaw for numPartitions=$numReducePartitions")
    val (belowWriter, _, _) = newWriter(mapId = 10L, bufferSizePercent = belowPct)
    assert(belowWriter.perPartitionBufferCapacityBytes === twoMB)
    belowWriter.stop(success = false)

    // Above-floor case: a single partition at the maximum buffer percent leaves the computed share
    // comfortably above 2 MB (Spark requires systemMemory >= 450 MB, so execMem/2 >> 2 MB), so the
    // writer must publish the computed budget unchanged rather than the floor.
    val abovePct = 50
    numReducePartitions = 1
    val aboveBudget = math.max(0L, execMem * abovePct / 100L)
    val aboveExpected = math.max(aboveBudget / numReducePartitions, twoMB)
    assert(aboveExpected > twoMB, s"test setup must leave the computed share above the floor, " +
      s"but expected=$aboveExpected")
    val (aboveWriter, _, _) = newWriter(mapId = 11L, bufferSizePercent = abovePct)
    val aboveActual = aboveWriter.perPartitionBufferCapacityBytes
    assert(aboveActual === aboveExpected)
    assert(aboveActual > twoMB)
    aboveWriter.stop(success = false)
  }

  test("spill persists buffered partitions to disk and returns freed bytes") {
    numReducePartitions = 5
    // Report a successful disk persist so a spill actually completes and its reclaimed bytes are
    // counted. The writer persists via BlockManager.putBytes(blockId, bytes, DISK_ONLY); match that
    // erased signature, whose trailing implicit ClassTag and defaulted `tellMaster` become two
    // extra positional arguments (any() / anyBoolean()).
    when(blockManager.putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), anyBoolean())(any()))
      .thenReturn(true)
    val (writer, metrics, _) = newWriter(mapId = 3L, bufferSizePercent = 20)
    writer.write(List((1, 10), (2, 20), (3, 30), (4, 40)).iterator)
    val spillsBefore = metrics.spillCounter.getCount

    // Drive the writer's PUBLIC MemoryConsumer spill contract directly (findings M1 and M10):
    // `spill(size, trigger): Long` is the visible, testable surface the AAP requires. The inner
    // MemoryConsumer the memory manager triggers under pressure simply forwards to this same
    // method, so this drives the identical production spill path. `trigger` is documented-unused
    // (the writer always spills its own buffers), so `null` is passed.
    val freed = writer.spill(Long.MaxValue, null)

    // spill(size, trigger) returns the number of bytes it reclaimed. Its contract only requires a
    // non-negative result, but buffered data was present here so the reclaim is strictly positive,
    // the spill counter advances, and every block is persisted at DISK_ONLY.
    assert(freed >= 0L)
    assert(freed > 0L)
    assert(metrics.spillCounter.getCount > spillsBefore)
    verify(blockManager, atLeastOnce()).putBytes(
      any(), any(), meq(StorageLevel.DISK_ONLY), anyBoolean())(any())
    writer.stop(success = false)
  }

  test("stop(success = false) returns None and retains no buffered memory") {
    numReducePartitions = 5
    val (writer, _, _) = newWriter(mapId = 4L, bufferSizePercent = 20)
    writer.write(List((1, 10), (2, 20), (3, 30), (4, 40)).iterator)
    val status = writer.stop(success = false)
    assert(status.isEmpty)

    // releaseAllResources() runs in stop()'s finally regardless of outcome: every partition buffer
    // is reset, unregistered, and nulled, and all execution memory reserved through the inner
    // MemoryConsumer is freed. The public MemoryConsumer spill contract (finding M10) is the
    // observable proof that no buffered memory is retained: with every buffer already released, a
    // maximal spill request finds no partition to persist and so reclaims exactly zero bytes.
    assert(writer.spill(Long.MaxValue, null) === 0L)
  }
}
