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

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util.Properties

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 *
 * We need to define this class ourselves instead of using a spy because the NioManagedBuffer class
 * is final (final classes cannot be spied on). This mirrors the helper used by the sort-based
 * `BlockStoreShuffleReaderSuite`, which the streaming reader's read path intentionally reuses.
 */
class RecordingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {
  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()
  override def convertToNettyForSsl(): AnyRef = underlyingBuffer.convertToNettyForSsl()

  override def retain(): ManagedBuffer = {
    callsToRetain += 1
    underlyingBuffer.retain()
  }
  override def release(): ManagedBuffer = {
    callsToRelease += 1
    underlyingBuffer.release()
  }
}

/**
 * Unit tests for [[StreamingShuffleReader]].
 *
 * The streaming reader intentionally MIRRORS the sort-based
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] read path, so this suite closely mirrors
 * `BlockStoreShuffleReaderSuite`: it serves serialized `(i, 2*i)` records from a mocked
 * [[org.apache.spark.storage.BlockManager]] as local blocks, resolves them through a mocked
 * [[org.apache.spark.MapOutputTracker]], disables shuffle compression on the
 * [[org.apache.spark.serializer.SerializerManager]], and asserts that every
 * [[RecordingManagedBuffer]] is retained and released exactly once (no leak).
 *
 * Beyond the happy-path read it verifies the streaming-specific guarantees: honoring the
 * dependency's `aggregator` / `keyOrdering` / `mapSideCombine`, CRC32C acceptance of well-formed
 * blocks on the consumer-stream channel, and - the signature requirement - that a producer
 * failure increments `partialReadInvalidations` and IMMEDIATELY throws a
 * [[org.apache.spark.shuffle.FetchFailedException]] (SPARK-19276: never construct-and-ignore a
 * fetch failure). The failure path is driven deterministically through a corrupt-CRC block (the
 * same `invalidatePartialReads` entry point the 5 s connection timeout uses) so the test needs no
 * real network wait.
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  /** Fixed shuffle/reduce identifiers shared by every fixture in this suite. */
  private val shuffleId = 22
  private val reduceId = 15

  /**
   * The combineValuesByKey path builds an `ExternalAppendOnlyMap` that defaults its `TaskContext`
   * to the thread-local one, so the fixture installs a real context. Clear it after every test to
   * avoid leaking the binding into sibling suites that run on the same thread.
   */
  override def afterEach(): Unit = {
    try {
      TaskContext.unset()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Serialize `pairs` key-value records `(i, 2*i)` into a self-contained byte array using the
   * supplied serializer. The stream is closed so the frame is complete and every record reads
   * back deterministically (the reduce-side reader drains it to EOF).
   */
  private def serializedRecords(serializer: JavaSerializer, pairs: Int): Array[Byte] = {
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until pairs).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2 * i)
    }
    serializationStream.close()
    byteOutputStream.toByteArray
  }

  /**
   * Build a fully wired [[StreamingShuffleReader]] over `numMaps` local map blocks of
   * `pairsPerMap` records each, returning the reader, the recording buffers (for leak assertions),
   * and the streaming metrics holder (for invalidation assertions).
   *
   * When `consumerEnvelope` is `None` a real (v1 logging-only) transport is used, so the
   * consumer-stream drain is a no-op and the read flows through the mirrored fetch path. When it
   * is defined, the transport is mocked to yield that single envelope on the consumer stream,
   * which lets the tests exercise CRC acceptance (valid block) and partial-read invalidation
   * (corrupt block) deterministically.
   */
  private def buildFixture(
      aggregator: Option[Aggregator[Int, Int, Int]],
      keyOrdering: Option[Ordering[Int]],
      mapSideCombine: Boolean,
      numMaps: Int,
      pairsPerMap: Int,
      consumerEnvelope: Option[StreamingBlockEnvelope])
    : (StreamingShuffleReader[Int, Int], Seq[RecordingManagedBuffer], StreamingShuffleMetrics) = {

    val conf = new SparkConf(false)
    // A SparkContext is the convenient way to populate SparkEnv, which the reader's mirrored
    // fetch path and the aggregator/sorter memory accounting both consult via SparkEnv.get.
    sc = new SparkContext("local", "test", conf)

    val serializer = new JavaSerializer(conf)
    val recordBytes = serializedRecords(serializer, pairsPerMap)

    // A mocked BlockManager returns RecordingManagedBuffers so we can assert retain()/release().
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
    // All blocks are executor-local, so the host-local path is never taken; stub the accessor to
    // None defensively because a bare mock would otherwise return a null Option.
    when(blockManager.hostLocalDirManager).thenReturn(None)

    val buffers = (0 until numMaps).map { mapId =>
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(recordBytes))
      val managedBuffer = new RecordingManagedBuffer(nioBuffer)
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
      when(blockManager.getLocalBlockData(meq(shuffleBlockId))).thenReturn(managedBuffer)
      managedBuffer
    }

    // The reader resolves block locations itself through the unchanged MapOutputTracker. Serve a
    // single local address (or no address at all when there is no map output).
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)).thenReturn {
      if (numMaps == 0) {
        Iterator.empty
      } else {
        val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
          (ShuffleBlockId(shuffleId, mapId, reduceId), recordBytes.length.toLong, mapId)
        }
        Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).iterator
      }
    }

    // The dependency drives serialization, aggregation, and ordering exactly as the sort path.
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(aggregator)
    when(dependency.keyOrdering).thenReturn(keyOrdering)
    when(dependency.mapSideCombine).thenReturn(mapSideCombine)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)

    val streamingConfig = new StreamingShuffleConfig(conf)
    val streamingMetrics = new StreamingShuffleMetrics

    val transport = consumerEnvelope match {
      case Some(envelope) =>
        val mockedTransport = mock(classOf[StreamingShuffleTransport])
        when(mockedTransport.openConsumerStream(shuffleId, 0, numMaps, reduceId, reduceId + 1))
          .thenReturn(Iterator(envelope))
        mockedTransport
      case None =>
        new StreamingShuffleTransport(streamingConfig, None)
    }

    // The aggregator/sorter require a real TaskMemoryManager, and combineValuesByKey reads the
    // thread-local TaskContext, so install a real context rather than TaskContext.empty().
    val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.memoryManager, 0)
    val taskContext = new TaskContextImpl(
      0, 0, 0, 0L, 0, 1, taskMemoryManager, new Properties, null)
    TaskContext.setTaskContext(taskContext)
    val readMetrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val reader = new StreamingShuffleReader[Int, Int](
      handle, 0, numMaps, reduceId, reduceId + 1, taskContext, readMetrics,
      streamingConfig, streamingMetrics, transport, serializerManager, blockManager,
      mapOutputTracker)

    (reader, buffers.toSeq, streamingMetrics)
  }

  test("read() returns deserialized records and releases all buffers") {
    val numMaps = 6
    val pairsPerMap = 10
    val (reader, buffers, metrics) =
      buildFixture(None, None, mapSideCombine = false, numMaps, pairsPerMap, None)

    val records = reader.read().toList

    assert(records.length === pairsPerMap * numMaps)
    // Every record is the original (i, 2*i) pair, and each key occurs once per map.
    records.foreach(record => assert(record._2 === 2 * record._1))
    val byKey = records.groupBy(_._1)
    assert(byKey.keySet === (0 until pairsPerMap).toSet)
    byKey.foreach { case (_, values) => assert(values.size === numMaps) }

    // A well-formed read must not invalidate, and exhausting the iterator must have retained and
    // released each buffer exactly once (no leak), matching BlockStoreShuffleReaderSuite.
    assert(metrics.partialReadInvalidations === 0L)
    buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 1)
      assert(buffer.callsToRelease === 1)
    }
  }

  test("read() honors aggregator when mapSideCombine is false (combineValuesByKey)") {
    val numMaps = 6
    val pairsPerMap = 10
    // A minimal sum aggregator: createCombiner, mergeValue, mergeCombiners over Int values.
    val sumAggregator = new Aggregator[Int, Int, Int](
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2)
    val (reader, _, _) =
      buildFixture(Some(sumAggregator), None, mapSideCombine = false, numMaps, pairsPerMap, None)

    val combined = reader.read().toList

    // Each key i appears numMaps times with value 2*i, so the combined value is 2*i*numMaps.
    assert(combined.length === pairsPerMap)
    val byKey = combined.map(record => record._1 -> record._2).toMap
    (0 until pairsPerMap).foreach { i =>
      assert(byKey(i) === 2 * i * numMaps)
    }
  }

  test("read() honors keyOrdering by sorting output keys ascending") {
    val numMaps = 6
    val pairsPerMap = 10
    val (reader, _, _) =
      buildFixture(None, Some(implicitly[Ordering[Int]]), mapSideCombine = false, numMaps,
        pairsPerMap, None)

    val sorted = reader.read().toList

    assert(sorted.length === pairsPerMap * numMaps)
    val keys = sorted.map(_._1)
    // An ExternalSorter with a key ordering must emit keys in non-decreasing order.
    assert(keys === keys.sorted)
  }

  test("CRC32C validation accepts well-formed streaming blocks") {
    val numMaps = 2
    val pairsPerMap = 5
    val validEnvelope = StreamingBlockEnvelope.create(
      shuffleId, mapId = 0L, reduceId, sequenceNumber = 0L, payload = Array[Byte](1, 2, 3, 4))
    assert(validEnvelope.verifyChecksum)
    val (reader, _, metrics) =
      buildFixture(None, None, mapSideCombine = false, numMaps, pairsPerMap, Some(validEnvelope))

    // A well-formed block passes CRC validation, so the consumer-stream drain does not invalidate
    // and the read proceeds through the mirrored fetch path to return all records.
    val records = reader.read().toList

    assert(records.length === pairsPerMap * numMaps)
    assert(metrics.partialReadInvalidations === 0L)
  }

  test("corrupt block invalidates partial reads and throws FetchFailedException (SPARK-19276)") {
    val numMaps = 2
    val pairsPerMap = 5
    val payload = Array[Byte](10, 20, 30, 40)
    // A deliberately wrong CRC makes verifyChecksum fail, exercising the same
    // invalidatePartialReads entry point as the 5 s connection-timeout path WITHOUT any real wait.
    val corruptEnvelope = new StreamingBlockEnvelope(
      shuffleId, 0L, reduceId, 0L, StreamingBlockEnvelope.computeCrc32c(payload) + 1, payload)
    assert(!corruptEnvelope.verifyChecksum)
    val (reader, _, metrics) =
      buildFixture(None, None, mapSideCombine = false, numMaps, pairsPerMap, Some(corruptEnvelope))

    val before = metrics.partialReadInvalidations
    // SPARK-19276: the FetchFailedException must be thrown immediately, never swallowed, and the
    // invalidation must be counted. The reader drains the consumer stream eagerly inside read().
    intercept[FetchFailedException] {
      reader.read()
    }
    assert(metrics.partialReadInvalidations === before + 1)
  }

  test("empty map output yields an empty iterator") {
    val (reader, buffers, metrics) =
      buildFixture(None, None, mapSideCombine = false, numMaps = 0, pairsPerMap = 0, None)

    val records = reader.read().toList

    assert(records.isEmpty)
    assert(buffers.isEmpty)
    assert(metrics.partialReadInvalidations === 0L)
  }
}
