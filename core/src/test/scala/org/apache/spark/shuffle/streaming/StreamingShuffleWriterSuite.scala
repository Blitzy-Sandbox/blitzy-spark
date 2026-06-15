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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{
  HashPartitioner, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext,
  SparkFunSuite, TaskContext}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport

/**
 * Unit tests for [[StreamingShuffleWriter]] covering the CP2 writer findings: the sealed-block
 * pipelining counter, the defensive `getPartitionLengths` copy, mid-write exception cleanup, and
 * propagation of an asynchronous send failure. The send/memory tests run under a real local
 * [[SparkContext]] because the writer builds its [[org.apache.spark.scheduler.MapStatus]] and
 * resolves the send target from the live [[org.apache.spark.SparkEnv]].
 */
class StreamingShuffleWriterSuite extends SparkFunSuite with Matchers {

  // A payload larger than one 2 MB block so a single record forces an in-loop drain: the
  // drain acquires execution memory and streams the sealed block (not just the final drain).
  private def largePayload(): Array[Byte] =
    new Array[Byte](StreamingShuffleConfig.BLOCK_SIZE_BYTES + 64 * 1024)

  private def newLocalSparkContext(): SparkContext =
    new SparkContext(
      new SparkConf().setMaster("local[1]").setAppName("StreamingShuffleWriterSuite"))

  // A v1-style transport whose send always completes successfully and synchronously.
  private def okTransport(): StreamingShuffleTransport = {
    val transport = mock(classOf[StreamingShuffleTransport])
    when(transport.sendBlock(any(), any())).thenReturn(Future.unit)
    transport
  }

  // An iterator that yields `throwAfter` full-block records and then fails, modelling a record
  // source that breaks mid-write after the writer has already buffered and accounted memory.
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

  // The exception messages along a throwable's cause chain, root first, skipping null messages.
  private def messageChain(t: Throwable): List[String] = {
    def loop(e: Throwable, acc: List[String]): List[String] =
      if (e == null) acc.reverse
      else loop(e.getCause, Option(e.getMessage).map(_ :: acc).getOrElse(acc))
    loop(t, Nil)
  }

  // Builds a single-partition streaming writer over mocked collaborators and a real
  // TaskMemoryManager (over TestMemoryManager) so execution-memory accounting is observable.
  private def buildWriter(transport: StreamingShuffleTransport):
      (StreamingShuffleWriter[Int, Array[Byte]], TaskMemoryManager) = {
    val conf = new SparkConf(false)
    val dep = mock(classOf[ShuffleDependency[Int, Array[Byte], Array[Byte]]])
    when(dep.partitioner).thenReturn(new HashPartitioner(1))
    when(dep.serializer).thenReturn(new JavaSerializer(conf))
    val handle = new StreamingShuffleHandle[Int, Array[Byte], Array[Byte]](
      0, dep, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)
    val tmm = new TaskMemoryManager(new TestMemoryManager(conf), 0L)
    val context = mock(classOf[TaskContext])
    when(context.taskMemoryManager()).thenReturn(tmm)
    val writer = new StreamingShuffleWriter[Int, Array[Byte]](
      handle,
      mapId = 0L,
      context,
      mock(classOf[ShuffleWriteMetricsReporter]),
      new StreamingShuffleConfig(conf),
      new StreamingShuffleMetrics,
      mock(classOf[BackpressureProtocol]),
      mock(classOf[MemorySpillManager]),
      transport,
      mock(classOf[StreamingShuffleBlockResolver]))
    (writer, tmm)
  }

  test("numSealedBlocks excludes the pending tail that numBlocks includes") {
    val blockSize = StreamingShuffleConfig.BLOCK_SIZE_BYTES
    val buffer = new StreamingBuffer(0, 0L, 0, 8L * blockSize)
    // One full 2 MB block is sealed; the trailing 100 bytes remain a pending (unsealed) block.
    buffer.append(new Array[Byte](blockSize + 100))
    // The writer streams numSealedBlocks eagerly (non-final) and the pending tail only when
    // finalizing via numBlocks, so the two counts must differ by exactly the pending block.
    buffer.numSealedBlocks mustBe 1
    buffer.numBlocks mustBe 2
    buffer.clear()
    buffer.numSealedBlocks mustBe 0
    buffer.numBlocks mustBe 0
  }

  test("getPartitionLengths returns a defensive copy that cannot mutate writer state") {
    LocalSparkContext.withSpark(newLocalSparkContext()) { _ =>
      val (writer, _) = buildWriter(okTransport())
      writer.write(Iterator((0, largePayload()), (0, largePayload())))
      val first = writer.getPartitionLengths()
      val second = writer.getPartitionLengths()
      assert(first ne second)
      assert(first(0) > 0L)
      val original = first(0)
      first(0) = original + 987654321L
      writer.getPartitionLengths()(0) mustBe original
    }
  }

  test("a mid-write failure releases all execution memory without masking the original error") {
    LocalSparkContext.withSpark(newLocalSparkContext()) { _ =>
      val (writer, tmm) = buildWriter(okTransport())
      val ex = intercept[RuntimeException](writer.write(boomIterator(throwAfter = 3)))
      ex.getMessage mustBe "blitzy-boom"
      tmm.getMemoryConsumptionForThisTask mustBe 0L
    }
  }

  test("a failed asynchronous block send propagates to the map task and leaks no memory") {
    LocalSparkContext.withSpark(newLocalSparkContext()) { _ =>
      val failing = mock(classOf[StreamingShuffleTransport])
      when(failing.sendBlock(any(), any()))
        .thenReturn(Future.failed(new RuntimeException("blitzy-send-boom")))
      val (writer, tmm) = buildWriter(failing)
      val ex = intercept[Exception](
        writer.write(Iterator((0, largePayload()), (0, largePayload()))))
      assert(messageChain(ex).exists(_.contains("blitzy-send-boom")))
      tmm.getMemoryConsumptionForThisTask mustBe 0L
    }
  }
}
