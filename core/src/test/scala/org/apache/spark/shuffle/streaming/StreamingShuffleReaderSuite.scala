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
import java.net.SocketTimeoutException
import java.nio.ByteBuffer

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{Aggregator, LocalSparkContext, MapOutputTracker, ShuffleDependency, SparkConf, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Unit tests for [[StreamingShuffleReader]].
 *
 * The streaming reader is the reduce-side counterpart of
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] and this suite mirrors
 * `BlockStoreShuffleReaderSuite` so the two readers are held to the same behavioral contract:
 * `read()` returns every producer record, honors `dep.aggregator` (map-side or reduce-side
 * combine), applies `dep.keyOrdering`, composes its iterators lazily, and releases every fetched
 * [[org.apache.spark.network.buffer.ManagedBuffer]] exactly once on completion. Preserving these
 * semantics is what lets the streaming backend coexist with the sort path without regression.
 *
 * On top of the mirrored pipeline the suite validates the streaming-specific failure contract: when
 * a producer connection times out (classified through the reader's 5-second
 * [[StreamingShuffleReader.PRODUCER_CONNECTION_TIMEOUT_MS]] window), the reader increments
 * `StreamingShuffleMetrics.incPartialReadInvalidations` exactly once and fails the task through the
 * standard [[FetchFailedException]] path (SPARK-19276: the exception is constructed and thrown in a
 * single statement because its constructor sets the [[TaskContext]] fetch-failed flag). The timeout
 * is injected deterministically (no real five-second sleep) by stubbing the local block fetch to
 * fail with a socket timeout.
 *
 * A [[SparkContext]] is created per test so that `SparkEnv.get` is populated: the reader's
 * `serializerManager`, `blockManager`, and `mapOutputTracker` parameters default from `SparkEnv`,
 * and the combine/ordering paths acquire execution memory from the active memory manager.
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  private val shuffleId = 22
  private val reduceId = 15
  private val numMaps = 6
  private val keyValuePairsPerMap = 10

  /**
   * Wrapper for a managed buffer that counts how many times `retain` and `release` are called.
   *
   * This mirrors the helper in `BlockStoreShuffleReaderSuite`; it is defined here (rather than
   * spying on [[NioManagedBuffer]]) because that class is final and cannot be spied on. It is
   * nested inside the suite so it never collides with an identically named helper defined by a
   * sibling suite in the same package.
   */
  private class RecordingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {
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
   * Bundle of the mocked collaborators shared by the read-path tests: the serializer used for both
   * the dependency and the [[SerializerManager]], the mocked [[BlockManager]] and
   * [[MapOutputTracker]] wired for a fully-local shuffle, the [[SerializerManager]] itself, and the
   * per-map recording buffers (empty for the failure fixture, which never returns a buffer).
   */
  private class ReaderFixture(
      val serializer: JavaSerializer,
      val blockManager: BlockManager,
      val mapOutputTracker: MapOutputTracker,
      val serializerManager: SerializerManager,
      val buffers: Seq[RecordingManagedBuffer])

  /**
   * Serializes `keyValuePairsPerMap` sample records `(i, 2 * i)` into a fresh byte stream, reusing
   * the exact serialize idiom from `BlockStoreShuffleReaderSuite` so the produced bytes decode
   * identically through the reader's deserialization pipeline.
   */
  private def serializeSampleData(serializer: JavaSerializer): ByteArrayOutputStream = {
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2 * i)
    }
    byteOutputStream
  }

  /** Builds a [[SerializerManager]] with shuffle compression disabled (mirrors the template). */
  private def newSerializerManager(serializer: JavaSerializer): SerializerManager = {
    new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))
  }

  /**
   * Builds a fixture describing a fully-local shuffle whose `numMaps` producer blocks are all
   * available: the mocked [[BlockManager]] returns a [[RecordingManagedBuffer]] for each block and
   * the mocked [[MapOutputTracker]] reports every block as local to a single block manager.
   */
  private def newSuccessFixture(): ReaderFixture = {
    val serializer = new JavaSerializer(new SparkConf(false))
    val byteOutputStream = serializeSampleData(serializer)

    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val buffers = (0 until numMaps).map { mapId =>
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))
      val managedBuffer = new RecordingManagedBuffer(nioBuffer)
      when(blockManager.getLocalBlockData(meq(ShuffleBlockId(shuffleId, mapId, reduceId))))
        .thenReturn(managedBuffer)
      managedBuffer
    }

    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0, numMaps, reduceId, reduceId + 1))
      .thenReturn {
        val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
          (ShuffleBlockId(shuffleId, mapId, reduceId), byteOutputStream.size().toLong, mapId)
        }
        Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).iterator
      }

    new ReaderFixture(
      serializer, blockManager, mapOutputTracker, newSerializerManager(serializer), buffers)
  }

  /**
   * Builds a fixture that simulates an unreachable producer: the mocked [[BlockManager]] fails
   * every local block fetch with a [[SocketTimeoutException]] wrapped in an unchecked exception.
   *
   * The wrapping is required because Mockito rejects checked exceptions on Scala methods that
   * declare no `throws` clause; the reader's connection-failure classifier walks the exception's
   * cause chain, so wrapping the socket timeout still triggers partial-read invalidation exactly as
   * a bare socket timeout would.
   */
  private def newTimeoutFixture(): ReaderFixture = {
    val serializer = new JavaSerializer(new SparkConf(false))

    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
    (0 until numMaps).foreach { mapId =>
      when(blockManager.getLocalBlockData(meq(ShuffleBlockId(shuffleId, mapId, reduceId))))
        .thenThrow(new RuntimeException(
          "simulated producer failure",
          new SocketTimeoutException("producer connection timed out")))
    }

    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0, numMaps, reduceId, reduceId + 1))
      .thenReturn {
        val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
          (ShuffleBlockId(shuffleId, mapId, reduceId), 128L, mapId)
        }
        Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).iterator
      }

    new ReaderFixture(
      serializer, blockManager, mapOutputTracker, newSerializerManager(serializer), Seq.empty)
  }

  /**
   * Builds a [[StreamingShuffleHandle]] backed by a mocked [[ShuffleDependency]] stubbed with the
   * given serializer, aggregator, key ordering, and map-side-combine flag. The streaming resource
   * envelope uses representative in-range values; the reader does not read them on the paths under
   * test, but they keep the handle self-consistent.
   */
  private def newHandle(
      serializer: JavaSerializer,
      aggregator: Option[Aggregator[Int, Int, Int]],
      keyOrdering: Option[Ordering[Int]],
      mapSideCombine: Boolean = false): StreamingShuffleHandle[Int, Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(aggregator)
    when(dependency.keyOrdering).thenReturn(keyOrdering)
    when(dependency.mapSideCombine).thenReturn(mapSideCombine)
    new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      dependency = dependency,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = 0)
  }

  test("read() returns all key-value pairs from the producer blocks") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val fixture = newSuccessFixture()
    val handle = newHandle(fixture.serializer, aggregator = None, keyOrdering = None)

    val taskContext = TaskContext.empty()
    val readMetrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val streamingMetrics = new StreamingShuffleMetrics()
    val streamingConf = new StreamingShuffleConfig(sc.getConf)
    val blocksByAddress = fixture.mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)

    val reader = new StreamingShuffleReader[Int, Int](
      handle,
      blocksByAddress,
      taskContext,
      readMetrics,
      streamingMetrics,
      streamingConf,
      fixture.serializerManager,
      fixture.blockManager,
      fixture.mapOutputTracker)

    // With no aggregator and no ordering the reader returns every record from every producer.
    assert(reader.read().length === keyValuePairsPerMap * numMaps)
  }

  test("read() honors dep.aggregator by combining values per key") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val fixture = newSuccessFixture()
    // Sum the values for each key. Every producer emitted (i, 2 * i), so combining across the
    // numMaps producers yields (i, 2 * i * numMaps).
    val aggregator = new Aggregator[Int, Int, Int](
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2)
    val handle = newHandle(
      fixture.serializer, Some(aggregator), keyOrdering = None, mapSideCombine = false)
    val streamingMetrics = new StreamingShuffleMetrics()
    val streamingConf = new StreamingShuffleConfig(sc.getConf)

    // combineValuesByKey builds an ExternalAppendOnlyMap bound to TaskContext.get(), so a task
    // context with a real memory manager must be installed for the duration of the read.
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    TaskContext.setTaskContext(taskContext)
    try {
      val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
      val blocksByAddress = fixture.mapOutputTracker.getMapSizesByExecutorId(
        shuffleId, 0, numMaps, reduceId, reduceId + 1)

      val reader = new StreamingShuffleReader[Int, Int](
        handle,
        blocksByAddress,
        taskContext,
        readMetrics,
        streamingMetrics,
        streamingConf,
        fixture.serializerManager,
        fixture.blockManager,
        fixture.mapOutputTracker)

      val combined = reader.read().map(record => (record._1, record._2)).toArray
      assert(combined.length === keyValuePairsPerMap)
      val combinedByKey = combined.toMap
      (0 until keyValuePairsPerMap).foreach { i =>
        assert(combinedByKey(i) === 2 * i * numMaps)
      }
    } finally {
      TaskContext.unset()
    }
  }

  test("read() honors dep.keyOrdering by returning records sorted by key") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val fixture = newSuccessFixture()
    val handle = newHandle(fixture.serializer, aggregator = None, keyOrdering = Some(Ordering.Int))
    val streamingMetrics = new StreamingShuffleMetrics()
    val streamingConf = new StreamingShuffleConfig(sc.getConf)

    // The reader routes ordered reads through an ExternalSorter that needs a real memory manager.
    val taskContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    TaskContext.setTaskContext(taskContext)
    try {
      val readMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
      val blocksByAddress = fixture.mapOutputTracker.getMapSizesByExecutorId(
        shuffleId, 0, numMaps, reduceId, reduceId + 1)

      val reader = new StreamingShuffleReader[Int, Int](
        handle,
        blocksByAddress,
        taskContext,
        readMetrics,
        streamingMetrics,
        streamingConf,
        fixture.serializerManager,
        fixture.blockManager,
        fixture.mapOutputTracker)

      val ordered = reader.read().map(record => (record._1, record._2)).toArray
      // No aggregation: every record is preserved, only reordered by key.
      assert(ordered.length === keyValuePairsPerMap * numMaps)
      val keys = ordered.map(_._1).toSeq
      assert(keys === keys.sorted)
      ordered.foreach { case (k, v) => assert(v === 2 * k) }
    } finally {
      TaskContext.unset()
    }
  }

  test("read() composes iterators lazily and does not materialize records eagerly") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val fixture = newSuccessFixture()
    val handle = newHandle(fixture.serializer, aggregator = None, keyOrdering = None)

    val taskContext = TaskContext.empty()
    val readMetrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val streamingMetrics = new StreamingShuffleMetrics()
    val streamingConf = new StreamingShuffleConfig(sc.getConf)
    val blocksByAddress = fixture.mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)

    val reader = new StreamingShuffleReader[Int, Int](
      handle,
      blocksByAddress,
      taskContext,
      readMetrics,
      streamingMetrics,
      streamingConf,
      fixture.serializerManager,
      fixture.blockManager,
      fixture.mapOutputTracker)

    val records = reader.read()
    // Merely obtaining the iterator must not deserialize records or release any buffer: the record
    // pipeline is composed lazily, so buffers are released only as the iterator is drained.
    fixture.buffers.foreach { buffer => assert(buffer.callsToRelease === 0) }

    assert(records.length === keyValuePairsPerMap * numMaps)
    // Draining the iterator deserializes every block and releases each buffer exactly once.
    fixture.buffers.foreach { buffer => assert(buffer.callsToRelease === 1) }
  }

  test("read() releases resources on completion") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val fixture = newSuccessFixture()
    val handle = newHandle(fixture.serializer, aggregator = None, keyOrdering = None)

    val taskContext = TaskContext.empty()
    val readMetrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val streamingMetrics = new StreamingShuffleMetrics()
    val streamingConf = new StreamingShuffleConfig(sc.getConf)
    val blocksByAddress = fixture.mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)

    val reader = new StreamingShuffleReader[Int, Int](
      handle,
      blocksByAddress,
      taskContext,
      readMetrics,
      streamingMetrics,
      streamingConf,
      fixture.serializerManager,
      fixture.blockManager,
      fixture.mapOutputTracker)

    assert(reader.read().length === keyValuePairsPerMap * numMaps)

    // Exhausting the iterator must retain and release each managed buffer exactly once.
    fixture.buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 1)
      assert(buffer.callsToRelease === 1)
    }
  }

  test("producer connection timeout invalidates the partial read and throws " +
    "FetchFailedException (SPARK-19276)") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val fixture = newTimeoutFixture()
    val handle = newHandle(fixture.serializer, aggregator = None, keyOrdering = None)

    val taskContext = TaskContext.empty()
    val readMetrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val streamingMetrics = new StreamingShuffleMetrics()
    val streamingConf = new StreamingShuffleConfig(sc.getConf)
    val blocksByAddress = fixture.mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)

    // SPARK-19276: the FetchFailedException's own constructor sets the TaskContext fetch-failed
    // flag, so the reader constructs-and-throws it in a single statement. The reader defers the
    // block fetch to read(), so the simulated producer timeout surfaces as the reduce iterator is
    // driven. Bracketing construction and the first read keeps the assertion correct regardless of
    // exactly where along that path the standard fault signal is raised.
    intercept[FetchFailedException] {
      val reader = new StreamingShuffleReader[Int, Int](
        handle,
        blocksByAddress,
        taskContext,
        readMetrics,
        streamingMetrics,
        streamingConf,
        fixture.serializerManager,
        fixture.blockManager,
        fixture.mapOutputTracker)
      reader.read().length
    }

    // The reader increments partialReadInvalidations exactly once on the producer timeout.
    assert(streamingMetrics.partialReadInvalidationsCounter.getCount === 1L)
  }

  test("PRODUCER_CONNECTION_TIMEOUT_MS is fixed at 5 seconds") {
    assert(StreamingShuffleReader.PRODUCER_CONNECTION_TIMEOUT_MS === 5000L)
  }

}
