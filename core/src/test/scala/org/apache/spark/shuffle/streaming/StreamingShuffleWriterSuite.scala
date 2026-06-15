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

import scala.concurrent.Future

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{atLeastOnce, mock, verify, when}
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{HashPartitioner, SharedSparkContext, ShuffleDependency, SparkConf, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport

/**
 * Unit tests for [[StreamingShuffleWriter]], the map-side writer of the opt-in streaming shuffle
 * backend. The suite mirrors the sort-based `org.apache.spark.shuffle.sort.SortShuffleWriterSuite`
 * harness - `@Mock(answer = RETURNS_SMART_NULLS)` collaborators initialized through
 * `MockitoAnnotations.openMocks` in [[beforeEach]], a [[MemoryTestingUtils.fakeTaskContext]] over a
 * real [[org.apache.spark.memory.TaskMemoryManager]], and data-driven test tables - because the
 * writer participates in the executor memory model (via a composed inner
 * [[org.apache.spark.memory.MemoryConsumer]]) and builds its [[MapStatus]] from the live
 * [[org.apache.spark.SparkEnv]] provided by [[SharedSparkContext]].
 *
 * The tests assert the writer's signature behaviors:
 *  - the per-partition buffer sizing formula and its hard 2 MB floor
 *    ([[StreamingShuffleConfig.perPartitionBufferBytes]]);
 *  - 2 MB block framing with CRC32C checksums (through [[StreamingBuffer]]);
 *  - backpressure permit acquisition and spill coordination through the mocked collaborators
 *    rather than real network/disk;
 *  - the `stop(true) -> Some(MapStatus)` / `stop(false) -> None` contract with metric rollback;
 *  - the defensive `getPartitionLengths` copy; and
 *  - that every code path (success, mid-write failure, and asynchronous send failure) releases all
 *    accounted execution memory so the task leaves no leak (the suite runs under
 *    `spark.unsafe.exceptionOnMemoryLeak=true`).
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester {

  /** Default reduce-partition count used by the integer-record tests. */
  private val numPartitions = 7

  /** Map id every writer under test produces output for. */
  private val mapId = 0L

  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _
  @Mock(answer = RETURNS_SMART_NULLS)
  private var transport: StreamingShuffleTransport = _
  @Mock(answer = RETURNS_SMART_NULLS)
  private var backpressure: BackpressureProtocol = _
  @Mock(answer = RETURNS_SMART_NULLS)
  private var spillManager: MemorySpillManager = _
  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockResolver: StreamingShuffleBlockResolver = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
    when(dependency.partitioner).thenReturn(new HashPartitioner(numPartitions))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    // The v1 transport is logging-only and completes its future synchronously; the smart-null
    // default would instead yield a null future that the writer's awaitResult would dereference.
    when(transport.sendBlock(any(), any())).thenReturn(Future.unit)
  }

  /**
   * Builds an integer-record streaming writer over the shared `@Mock` collaborators and the given
   * task context. The shared `dependency` is stubbed with a [[HashPartitioner]] in [[beforeEach]].
   */
  private def intWriter(
      context: TaskContext,
      config: StreamingShuffleConfig = new StreamingShuffleConfig(conf))
      : StreamingShuffleWriter[Int, Int] = {
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = 0, dependency, bufferSizePercent = 20, spillThreshold = 80,
      maxBandwidthMBps = -1)
    new StreamingShuffleWriter[Int, Int](
      handle,
      mapId,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      config,
      new StreamingShuffleMetrics,
      backpressure,
      spillManager,
      transport,
      blockResolver)
  }

  /**
   * Builds a byte-array-record streaming writer with a freshly mocked dependency partitioned into
   * `numParts` reduce partitions. Byte-array payloads let a single record exceed a 2 MB block so
   * the in-loop drain, framing, and spill paths are exercised without producing many records.
   */
  private def bytesWriter(
      context: TaskContext,
      config: StreamingShuffleConfig,
      numParts: Int): StreamingShuffleWriter[Int, Array[Byte]] = {
    val dep = mock(classOf[ShuffleDependency[Int, Array[Byte], Array[Byte]]])
    when(dep.partitioner).thenReturn(new HashPartitioner(numParts))
    when(dep.serializer).thenReturn(new JavaSerializer(conf))
    val handle = new StreamingShuffleHandle[Int, Array[Byte], Array[Byte]](
      shuffleId = 0, dep, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)
    new StreamingShuffleWriter[Int, Array[Byte]](
      handle,
      mapId,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      config,
      new StreamingShuffleMetrics,
      backpressure,
      spillManager,
      transport,
      blockResolver)
  }

  /** A payload larger than one 2 MB block so a single record forces an in-loop block drain. */
  private def largePayload(): Array[Byte] =
    new Array[Byte](StreamingShuffleConfig.BLOCK_SIZE_BYTES + 64 * 1024)

  /**
   * An iterator that yields `throwAfter` full-block records and then fails, modelling a record
   * source that breaks mid-write after the writer has already buffered and accounted memory.
   */
  private def boomIterator(throwAfter: Int): Iterator[Product2[Int, Array[Byte]]] =
    new Iterator[Product2[Int, Array[Byte]]] {
      private var emitted = 0
      override def hasNext: Boolean = true
      override def next(): Product2[Int, Array[Byte]] = {
        if (emitted >= throwAfter) throw new RuntimeException("blitzy-boom")
        emitted += 1
        (0, largePayload())
      }
    }

  /** The exception messages along a throwable's cause chain, root first, skipping null messages. */
  private def messageChain(t: Throwable): List[String] = {
    def loop(e: Throwable, acc: List[String]): List[String] =
      if (e == null) acc.reverse
      else loop(e.getCause, Option(e.getMessage).map(_ :: acc).getOrElse(acc))
    loop(t, Nil)
  }

  // ---------------------------------------------------------------------------------------------
  // Per-partition buffer sizing: the canonical formula with a hard 2 MB floor. These cases are
  // pure (no SparkEnv) and assert both the computed value (> 2 MB) and the clamp UP to the floor.
  // ---------------------------------------------------------------------------------------------
  private val bufferSizingCases: Seq[(Long, Int, Long)] = Seq(
    // (executorMemoryBytes, numPartitions, expectedPerPartitionBytes)
    (1024L * 1024 * 1024, 1, 214748364L),   // 1 GiB * 20% / 1 -> ~204 MiB, well above the floor
    (1024L * 1024 * 1024, 10, 21474836L),   // 1 GiB * 20% / 10 -> ~20 MiB, still above the floor
    (8L * 1024 * 1024, 64, StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES), // tiny budget -> floor
    (0L, 1, StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES)) // no memory budget -> floor

  bufferSizingCases.foreach { case (execMem, parts, expected) =>
    test(s"perPartitionBufferBytes(mem=$execMem, parts=$parts) honors the 2 MB floor") {
      val cfg = new StreamingShuffleConfig(new SparkConf(false))
      val sized = cfg.perPartitionBufferBytes(execMem, parts)
      assert(sized === expected)
      assert(sized >= StreamingShuffleConfig.MIN_BUFFER_SIZE_BYTES)
    }
  }

  test("write empty iterator then stop(true) returns Some(MapStatus) with zero-length partitions") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = intWriter(context)
    writer.write(Iterator.empty)
    val status: Option[MapStatus] = writer.stop(success = true)
    assert(status.isDefined)
    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numPartitions)
    assert(lengths.sum === 0L)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.recordsWritten === 0L)
    assert(writeMetrics.bytesWritten === 0L)
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }

  test("write records buffers per partition, frames blocks, and stop(true) returns a MapStatus") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = intWriter(context)
    val n = 100
    writer.write((0 until n).iterator.map(i => (i, i)))
    val status: Option[MapStatus] = writer.stop(success = true)
    assert(status.isDefined)
    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numPartitions)
    assert(lengths.sum > 0L)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.recordsWritten === n.toLong)
    // partitionLengths is the per-partition written byte count, so it must equal bytesWritten.
    assert(lengths.sum === writeMetrics.bytesWritten)
    // The writer registers and tracks each lazily-allocated buffer, frames its bytes into a
    // checksummed envelope, gates the send through backpressure, and hands it to the transport.
    verify(spillManager, atLeastOnce()).register(any[StreamingBuffer]())
    verify(blockResolver, atLeastOnce()).trackBuffer(any[StreamingBuffer]())
    verify(backpressure, atLeastOnce()).acquireSendPermit(any(), anyInt())
    verify(transport, atLeastOnce()).sendBlock(any(), any())
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }

  test("stop(false) returns None, rolls back write metrics, and unregisters the buffers") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = intWriter(context)
    writer.write((0 until 50).iterator.map(i => (i, i)))
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.recordsWritten === 50L)
    assert(writeMetrics.bytesWritten > 0L)
    val status = writer.stop(success = false)
    assert(status.isEmpty)
    // The discarded attempt rolls the incrementally-committed metrics back to zero.
    assert(writeMetrics.recordsWritten === 0L)
    assert(writeMetrics.bytesWritten === 0L)
    verify(spillManager, atLeastOnce()).unregister(any[MemorySpillManager.BufferKey]())
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }

  test("backpressure permits are acquired for the blocks streamed during a non-empty write") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = intWriter(context)
    writer.write((0 until 20).iterator.map(i => (i, i)))
    writer.stop(success = true)
    verify(backpressure, atLeastOnce()).acquireSendPermit(any(), anyInt())
  }

  test("buffer pressure on a timed-out consumer triggers spill coordination") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    // A timed-out consumer plus a buffer over the spill threshold persists the unacked data so it
    // can never be lost; force the 2 MB floor (bufferSizePercent=1 over many partitions) so the
    // single 4 MB write deterministically drives utilization to 100% regardless of executor memory.
    when(backpressure.isConsumerTimedOut(any())).thenReturn(true)
    val cfg = new StreamingShuffleConfig(
      new SparkConf(false).set("spark.shuffle.streaming.bufferSizePercent", "1"))
    val writer = bytesWriter(context, cfg, numParts = 200)
    val payload = new Array[Byte](2 * StreamingShuffleConfig.BLOCK_SIZE_BYTES)
    writer.write(Iterator((0, payload)))
    writer.stop(success = true)
    verify(spillManager, atLeastOnce()).spillBuffer(any[MemorySpillManager.BufferKey]())
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }

  test("no execution memory is leaked after a successful write and stop(true)") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = intWriter(context)
    writer.write((0 until 200).iterator.map(i => (i, i)))
    val status = writer.stop(success = true)
    assert(status.isDefined)
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }

  test("StreamingBuffer seals full 2 MB blocks, computes a CRC32C, and clears") {
    val blockSize = StreamingShuffleConfig.BLOCK_SIZE_BYTES
    val buffer = new StreamingBuffer(0, 0L, 0, 8L * blockSize)
    // One full 2 MB block is sealed; the trailing 100 bytes remain a pending (unsealed) block, so
    // the eagerly-streamed sealed count and the finalizing total count differ by the pending tail.
    buffer.append(new Array[Byte](blockSize + 100))
    buffer.numSealedBlocks mustBe 1
    buffer.numBlocks mustBe 2
    // Reading the sealed block back exercises the canonical 2 MB framing and its CRC32C checksum.
    val (blockBytes, checksum) = buffer.blockWithChecksum(0)
    blockBytes.length mustBe blockSize
    checksum mustBe buffer.checksumOf(0)
    buffer.clear()
    buffer.numSealedBlocks mustBe 0
    buffer.numBlocks mustBe 0
  }

  test("getPartitionLengths returns a defensive copy that cannot mutate writer state") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = intWriter(context)
    writer.write((0 until 30).iterator.map(i => (i, i)))
    writer.stop(success = true)
    val first = writer.getPartitionLengths()
    val second = writer.getPartitionLengths()
    assert(first ne second)
    assert(first.sum > 0L)
    val p = first.indexWhere(_ > 0L)
    val original = first(p)
    first(p) = original + 987654321L
    assert(writer.getPartitionLengths()(p) === original)
  }

  test("a mid-write failure releases all execution memory without masking the original error") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = bytesWriter(context, new StreamingShuffleConfig(conf), numParts = 1)
    val ex = intercept[RuntimeException](writer.write(boomIterator(throwAfter = 3)))
    assert(ex.getMessage === "blitzy-boom")
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }

  test("a failed asynchronous block send propagates to the map task and leaks no memory") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    when(transport.sendBlock(any(), any()))
      .thenReturn(Future.failed(new RuntimeException("blitzy-send-boom")))
    val writer = bytesWriter(context, new StreamingShuffleConfig(conf), numParts = 1)
    val ex = intercept[Exception](
      writer.write(Iterator((0, largePayload()), (0, largePayload()))))
    assert(messageChain(ex).exists(_.contains("blitzy-send-boom")))
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask === 0L)
  }
}
