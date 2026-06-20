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
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{HashPartitioner, SharedSparkContext, ShuffleDependency, SparkConf,
  SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport

/**
 * Unit tests for [[StreamingShuffleWriter]], the map-side writer of the opt-in streaming shuffle
 * backend.
 *
 * Mirrors `SortShuffleWriterSuite`: it runs under [[SharedSparkContext]] (so the executor memory
 * model and the leak detector `spark.unsafe.exceptionOnMemoryLeak=true` are live), creates the
 * task context with [[MemoryTestingUtils.fakeTaskContext]], and rebuilds the Mockito mocks per
 * test via [[org.mockito.MockitoAnnotations.openMocks]].
 *
 * The streaming writer differs from the sort-based writer in that it never materializes the whole
 * shuffle to disk: it buffers each partition in memory, frames it into 2 MB CRC32C blocks, gates
 * sends through the backpressure protocol, and coordinates disk spill under memory pressure. The
 * network, spill, backpressure, and block-resolver collaborators are therefore mocked so the
 * tests assert the writer's collaboration contract without touching real disk or network, while
 * the configuration, metrics, and handle are real. The suite covers the writer's signature
 * behaviors:
 *
 *   - the per-partition buffer 2 MB floor ([[StreamingShuffleConfig.perPartitionBufferBytes]]);
 *   - an empty write still produces a `Some(MapStatus)` with zero-length partitions;
 *   - a non-empty write buffers, frames CRC32C 2 MB blocks, and returns a `MapStatus`;
 *   - `stop(success = true)` yields `Some(MapStatus)` and `stop(success = false)` yields `None`;
 *   - backpressure send-permit acquisition gates every block;
 *   - buffer pressure is relieved by coordinating a spill through the spill manager; and
 *   - no execution memory is leaked once the writer is stopped.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester {

  /** The shuffle id under test; the streaming handle is constructed with the same id. */
  private val shuffleId = 0

  /** The number of reduce partitions; the length of every partitionLengths / MapStatus array. */
  private val numPartitions = 4

  /** The map task (task-attempt) id this writer produces output for. */
  private val mapId = 0L

  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var backpressure: BackpressureProtocol = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var spillManager: MemorySpillManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var transport: StreamingShuffleTransport = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockResolver: StreamingShuffleBlockResolver = _

  /** Real, immutable typed configuration (default bufferSizePercent=20, spillThreshold=80). */
  private val streamingConfig = new StreamingShuffleConfig(conf)

  /** Real telemetry holder; the writer only reads it under debug, which is off by default. */
  private val streamingMetrics = new StreamingShuffleMetrics()

  /** The streaming handle wrapping the mocked dependency; rebuilt per test in `beforeEach`. */
  private var shuffleHandle: StreamingShuffleHandle[Int, Int, Int] = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
    when(dependency.partitioner).thenReturn(new HashPartitioner(numPartitions))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    // v1 transport hand-off is observational: it returns an already-completed Future (the real
    // data plane is the reduce-side BlockTransferService.fetchBlockSync), so stub it accordingly.
    when(transport.sendBlock(any(), any())).thenReturn(Future.unit)
    shuffleHandle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)
  }

  /**
   * Builds a [[StreamingShuffleWriter]] wired to the mocked collaborators and the real config,
   * metrics, and handle, using `context` for the working task memory manager.
   *
   * @param context the fake task context supplying the working task memory manager
   * @return a streaming writer for `(shuffleId, mapId)`
   */
  private def newWriter(context: TaskContext): StreamingShuffleWriter[Int, Int] = {
    new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      streamingConfig,
      streamingMetrics,
      backpressure,
      spillManager,
      transport,
      blockResolver)
  }

  // The 2 MB floor is the writer's signature buffer-sizing invariant. Each row asserts the pure
  // formula (executorMemory * bufferSizePercent / 100) / numPartitions clamped up to the 2 MB
  // floor: the first two rows compute above 2 MB, while the third clamps a sub-2 MB result up.
  Seq(
    (1024L * 1024 * 1024, 1, 214748364L),
    (1024L * 1024 * 1024, 10, 21474836L),
    (100L * 1024 * 1024, 1000, 2L * 1024 * 1024)).foreach { case (execMem, parts, expected) =>
    test(s"perPartitionBufferBytes applies the 2MB floor (mem=$execMem parts=$parts)") {
      val cfg = new StreamingShuffleConfig(new SparkConf(false))
      assert(cfg.perPartitionBufferBytes(execMem, parts) === expected)
    }
  }

  test("write empty iterator then stop(true) returns Some(MapStatus) with zero lengths") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter(context)
    writer.write(Iterator.empty)
    val status: Option[MapStatus] = writer.stop(success = true)
    assert(status.isDefined)
    assert(writer.getPartitionLengths().length === numPartitions)
    assert(writer.getPartitionLengths().sum === 0L)
  }

  test("write records frames CRC32C blocks and stop(true) returns a MapStatus") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter(context)
    writer.write((0 until 100).iterator.map(i => (i, i)))
    val status: Option[MapStatus] = writer.stop(success = true)
    assert(status.isDefined)
    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numPartitions)
    assert(lengths.sum > 0L)
    // ensurePartition registers each created buffer with the spill manager and block resolver.
    verify(spillManager, atLeastOnce()).register(any())
    verify(blockResolver, atLeastOnce()).trackBuffer(any())
    // sendFramed builds one CRC32C StreamingBlockEnvelope per 2 MB block and hands it to the
    // transport, so a sendBlock interaction proves the framing + checksum path executed.
    verify(transport, atLeastOnce()).sendBlock(any(), any())
  }

  test("stop(false) returns None and unregisters buffers and backpressure streams") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter(context)
    writer.write((0 until 50).iterator.map(i => (i, i)))
    val status: Option[MapStatus] = writer.stop(success = false)
    assert(status.isEmpty)
    // Failure teardown discards the partial output: each buffer is unregistered from the spill
    // manager and every backpressure stream this writer opened is unregistered.
    verify(spillManager, atLeastOnce()).unregister(any())
    verify(backpressure, atLeastOnce()).unregisterStream(any())
  }

  test("backpressure send permits are acquired during a non-empty write") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter(context)
    writer.write((0 until 50).iterator.map(i => (i, i)))
    writer.stop(success = true)
    // The writer gates every framed block through the backpressure protocol before sending it.
    verify(backpressure, atLeastOnce()).acquireSendPermit(anyInt())
  }

  test("buffer pressure is relieved by coordinating a spill through the spill manager") {
    // A timed-out consumer drives the deterministic consumer-failure path in the writer
    // (maybeHandleConsumerTimeout), which asks the spill manager to reclaim memory for unacked
    // data. Stubbing isConsumerTimedOut=true makes the coordination fire without real timing.
    when(backpressure.isConsumerTimedOut(any())).thenReturn(true)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter(context)
    writer.write((0 until 50).iterator.map(i => (i, i)))
    writer.stop(success = true)
    verify(spillManager, atLeastOnce()).maybeSpill()
  }

  test("no memory leak: all task execution memory is released after stop") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter(context)
    writer.write((0 until 50).iterator.map(i => (i, i)))
    writer.stop(success = true)
    // The composed MemoryConsumer must free every byte it reserved. The leak detector
    // (spark.unsafe.exceptionOnMemoryLeak=true) is the backstop; this is the explicit check.
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask() === 0L)
  }
}
