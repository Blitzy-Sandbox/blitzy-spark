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

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

/**
 * Unit tests for [[StreamingShuffleWriter]] (feature F-103), the producer-side writer of the
 * streaming shuffle data path that buffers each map task's output per reduce partition, spills
 * to disk under memory pressure, and returns a `MapStatus` from `stop(true)`.
 *
 * Because the writer composes a `MemoryConsumer`, it needs a [[TaskContext]] backed by a real
 * task memory manager. The suite therefore extends [[SharedSparkContext]] and builds the context
 * with `MemoryTestingUtils.fakeTaskContext` rather than `TaskContext.empty()`. The shuffle
 * dependency is a Mockito mock stubbed with the serializer, partitioner, and (empty) aggregator
 * and key ordering that the writer reads when it is constructed.
 *
 * Executor memory is pinned to a small, deterministic value via `spark.testing.memory` so the
 * per-partition buffer budget is tiny and the spill-on-threshold path is fast to exercise.
 */
class StreamingShuffleWriterSuite extends SparkFunSuite with SharedSparkContext {

  // Pin executor memory to 64 MiB and disable the testing reserved-memory floor so that
  // `maxOnHeapStorageMemory` (the denominator of the streaming buffer budget) is small and
  // deterministic. Set on the shared `conf` before SharedSparkContext starts the SparkContext.
  conf.set("spark.testing.memory", "67108864")
  conf.set("spark.testing.reservedMemory", "0")

  /** A fresh Java serializer; the writer calls `dependency.serializer.newInstance()`. */
  private val serializer = new JavaSerializer(conf)

  /** Hash partitioner mirroring the production routing (`Utils.nonNegativeMod`). */
  private def newPartitioner(numParts: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  /**
   * Builds a mocked `ShuffleDependency[Int, V, V]` stubbed with exactly the members the writer
   * reads: the serializer and partitioner (read eagerly in the constructor) plus an empty
   * aggregator and key ordering.
   */
  private def newDependency[V](numParts: Int): ShuffleDependency[Int, V, V] = {
    val dependency = mock(classOf[ShuffleDependency[Int, V, V]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.partitioner).thenReturn(newPartitioner(numParts))
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    dependency
  }

  /** Builds a [[StreamingShuffleHandle]] carrying the three per-shuffle tuning values. */
  private def newHandle[V](
      numParts: Int,
      bufferSizePercent: Int = 20,
      spillThreshold: Int = 80,
      maxBandwidthMBps: Int = 0): StreamingShuffleHandle[Int, V, V] = {
    new StreamingShuffleHandle[Int, V, V](
      shuffleId = 0,
      dependency = newDependency[V](numParts),
      bufferSizePercent = bufferSizePercent,
      spillThreshold = spillThreshold,
      maxBandwidthMBps = maxBandwidthMBps)
  }

  /** Constructs the writer under test for the supplied handle and task context. */
  private def newWriter[V](
      handle: StreamingShuffleHandle[Int, V, V],
      context: TaskContext): StreamingShuffleWriter[Int, V, V] = {
    new StreamingShuffleWriter[Int, V, V](
      handle,
      mapId = 0L,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      new StreamingShuffleConfig(conf))
  }

  test("BLOCK_SIZE is the 2 MiB pipeline block boundary") {
    assert(StreamingShuffleWriter.BLOCK_SIZE === 2 * 1024 * 1024)
  }

  test("write then stop(true) returns a MapStatus describing the map output") {
    val numParts = 4
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter[Int](newHandle[Int](numParts), context)

    writer.write((0 until 1000).iterator.map(i => (i, i)))
    val status = writer.stop(success = true)

    assert(status.isDefined)
    // The map output is advertised at the local block manager.
    assert(status.get.location === sc.env.blockManager.shuffleServerId)
    assert(status.get.mapId === 0L)

    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numParts)
    assert(lengths.sum > 0L)

    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.recordsWritten === 1000)
    assert(writeMetrics.bytesWritten > 0L)
  }

  test("getPartitionLengths is consistent with the MapStatus block sizes") {
    val numParts = 8
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter[Int](newHandle[Int](numParts), context)

    writer.write((0 until 1000).iterator.map(i => (i, i)))
    val status = writer.stop(success = true).get

    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numParts)
    // `CompressedMapStatus` stores sizes lossily (log base 1.1), so the decompressed size is not
    // byte-exact; however the empty/non-empty correspondence is exact and every non-empty
    // partition decompresses to a strictly positive size.
    var p = 0
    while (p < numParts) {
      assert((lengths(p) == 0L) === (status.getSizeForBlock(p) == 0L))
      if (lengths(p) > 0L) {
        assert(status.getSizeForBlock(p) > 0L)
      }
      p += 1
    }
  }

  test("records are routed to partitions via the dependency partitioner") {
    val numParts = 4
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter[Int](newHandle[Int](numParts), context)

    // An Int's hashCode is the Int itself, so nonNegativeMod(key, 4) is deterministic:
    // 0, 4, 8 -> partition 0 ; 1, 5 -> partition 1 ; partitions 2 and 3 receive no records.
    val keys = Seq(0, 4, 8, 1, 5)
    writer.write(keys.iterator.map(k => (k, k)))
    writer.stop(success = true)

    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numParts)
    assert(lengths(0) > 0L)
    assert(lengths(1) > 0L)
    assert(lengths(2) === 0L)
    assert(lengths(3) === 0L)
  }

  test("per-partition budget derives from bufferSizePercent and numPartitions") {
    val numParts = 4
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val executorMemory = sc.env.memoryManager.maxOnHeapStorageMemory
    val writer = newWriter[Int](newHandle[Int](numParts, bufferSizePercent = 20), context)

    val expectedBudget = (executorMemory * 20) / 100
    assert(writer.streamingMemoryBudget === expectedBudget)
    assert(writer.perPartitionBudget === expectedBudget / numParts)
  }

  test("writing beyond the buffer budget triggers a spill to disk") {
    val numParts = 2
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    // A 1% buffer budget and 50% spill threshold over the suite's small executor memory make
    // the spill trigger only a few hundred KiB. Distinct 2 KiB values (a shared array would be
    // de-duplicated by Java serialization) over two spill-check intervals cross the trigger.
    val handle = newHandle[Array[Byte]](numParts, bufferSizePercent = 1, spillThreshold = 50)
    val writer = newWriter[Array[Byte]](handle, context)

    val numRecords = 2 * StreamingShuffleWriter.SPILL_CHECK_RECORD_INTERVAL
    val records = (0 until numRecords).iterator.map(i => (i, new Array[Byte](2 * 1024)))
    writer.write(records)
    val status = writer.stop(success = true)

    assert(status.isDefined)
    assert(writer.numSpills > 0)
    assert(writer.spilledBytes > 0L)
  }

  test("stop is idempotent: a second stop is a safe no-op") {
    val numParts = 4
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = newWriter[Int](newHandle[Int](numParts), context)

    writer.write((0 until 500).iterator.map(i => (i, i)))
    val first = writer.stop(success = true)
    assert(first.isDefined)

    // The double-stop guard makes the second call return None without throwing.
    val second = writer.stop(success = true)
    assert(second.isEmpty)
  }

  test("stop(false) releases all buffers and execution memory and returns None") {
    val numParts = 2
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    // Force the writer to acquire execution memory (and spill) so that its release on the failure
    // path is observable through the task memory manager.
    val handle = newHandle[Array[Byte]](numParts, bufferSizePercent = 1, spillThreshold = 50)
    val writer = newWriter[Array[Byte]](handle, context)

    val numRecords = 2 * StreamingShuffleWriter.SPILL_CHECK_RECORD_INTERVAL
    writer.write((0 until numRecords).iterator.map(i => (i, new Array[Byte](2 * 1024))))
    val result = writer.stop(success = false)

    assert(result.isEmpty)
    // All granted execution memory has been returned: the task holds nothing afterward.
    assert(context.taskMemoryManager().getMemoryConsumptionForThisTask() === 0L)
  }
}
