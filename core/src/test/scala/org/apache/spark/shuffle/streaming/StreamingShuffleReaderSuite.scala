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

import org.mockito.ArgumentMatchers.{any, anyInt, anyString, eq => meq}
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.network.BlockTransferService
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
 * is final (final classes cannot be spied on). It mirrors the helper of the same name in
 * `BlockStoreShuffleReaderSuite`, which is the authoritative model for this suite: the streaming
 * reader deliberately reuses that read path. The counters let each test assert that the streaming
 * reader releases every fetched buffer (so no buffer is leaked).
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
 * Unit tests for [[StreamingShuffleReader]], the reduce-side reader of the opt-in streaming
 * shuffle backend.
 *
 * The streaming reader mirrors [[org.apache.spark.shuffle.BlockStoreShuffleReader]] for the
 * deserialize / aggregate / sort stages, so this suite mirrors `BlockStoreShuffleReaderSuite`
 * (the [[RecordingManagedBuffer]] helper, a mocked block manager / map-output tracker, and a
 * [[SerializerManager]] with compression disabled). The behavioral difference is the fetch stage:
 * the streaming reader pulls one CRC32C-validated [[StreamingBlockEnvelope]] at a time through the
 * existing [[BlockTransferService.fetchBlockSync]] path rather than materializing the whole reduce
 * partition first, so the mocks here stub `fetchBlockSync` to return framed envelopes.
 *
 * The suite covers:
 *   - the happy path: records deserialize correctly and every fetched buffer is released;
 *   - the aggregator path (`mapSideCombine = false` -> `combineValuesByKey`);
 *   - the key-ordering path (output sorted ascending);
 *   - CRC32C validation accepting well-formed blocks and rejecting a corrupted block;
 *   - the 5 s connection-timeout failure path, which (per SPARK-19276) must increment
 *     `partialReadInvalidations` and throw [[FetchFailedException]] immediately; and
 *   - the empty map-output case yielding an empty iterator.
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  // Fixed shuffle / reduce coordinates shared by all tests. Reading a single reduce partition
  // ([startPartition, endPartition)) across a configurable number of map outputs mirrors the model.
  private val shuffleId = 22
  private val reduceId = 15
  private val startPartition = reduceId
  private val endPartition = reduceId + 1

  /**
   * Bundle of the constructed reader together with the collaborators a test asserts against: the
   * recording buffers (for the no-leak assertion), the streaming metrics holder (for the
   * partial-read-invalidation counter), and the task context (installed as the thread-local
   * [[TaskContext]] while the reader runs).
   */
  private class Fixture(
      val reader: StreamingShuffleReader[Int, Int],
      val buffers: Seq[RecordingManagedBuffer],
      val metrics: StreamingShuffleMetrics,
      val context: TaskContext)

  /**
   * Serializes `recordsPerMap` `(i, 2*i)` pairs into a single map block's payload, exactly as
   * `BlockStoreShuffleReaderSuite` builds its shuffle data. The stream is closed so that every
   * record is flushed before the bytes are captured.
   */
  private def serializedPayload(serializer: JavaSerializer, recordsPerMap: Int): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteStream)
    (0 until recordsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2 * i)
    }
    serializationStream.close()
    byteStream.toByteArray
  }

  /**
   * Frames a map block's payload into the canonical [[StreamingBlockEnvelope]] wire encoding and
   * wraps it in a [[RecordingManagedBuffer]] so a test can assert the reader releases it. When
   * `corrupt` is true the stored CRC32C is flipped, so [[StreamingBlockEnvelope.verifyChecksum]]
   * fails and the reader must treat the block as a fetch failure.
   */
  private def frameBuffer(
      payload: Array[Byte],
      mapId: Int,
      corrupt: Boolean): RecordingManagedBuffer = {
    val frame =
      if (corrupt) {
        val badCrc = StreamingBlockEnvelope.computeCrc32c(payload) ^ 0x1
        new StreamingBlockEnvelope(
          shuffleId, mapId.toLong, reduceId, 0L, badCrc, payload).toByteArray
      } else {
        StreamingBlockEnvelope.create(
          shuffleId, mapId.toLong, reduceId, 0L, payload).toByteArray
      }
    new RecordingManagedBuffer(new NioManagedBuffer(ByteBuffer.wrap(frame)))
  }

  /**
   * Builds a fully-wired [[StreamingShuffleReader]] over mocked collaborators.
   *
   * `numMaps` map outputs each contribute one block of `recordsPerMap` `(i, 2*i)` pairs for the
   * single reduce partition under test. The dependency's `aggregator`, `keyOrdering`, and
   * `mapSideCombine` are stubbed from the parameters so the read path's aggregate / sort branches
   * can be exercised. The `failFetch`, `corruptCrc`, and `emptyOutput` flags drive the failure and
   * boundary cases:
   *   - `failFetch`   stubs `fetchBlockSync` to throw, simulating a connection timeout WITHOUT a
   *                   real wait (the future fails fast, so `ThreadUtils.awaitResult` returns at
   *                   once);
   *   - `corruptCrc`  flips each block's stored CRC32C so validation fails; and
   *   - `emptyOutput` makes the map-output tracker return no blocks.
   *
   * The transport is built with `None`, so the reader sources its transfer service from the
   * mocked block manager (`blockManager.blockTransferService`) exactly as it does in local mode.
   */
  private def buildReader(
      numMaps: Int,
      recordsPerMap: Int,
      aggregator: Option[Aggregator[Int, Int, Int]] = None,
      keyOrdering: Option[Ordering[Int]] = None,
      mapSideCombine: Boolean = false,
      failFetch: Boolean = false,
      corruptCrc: Boolean = false,
      emptyOutput: Boolean = false): Fixture = {
    val conf = new SparkConf(false)
    // A SparkContext sets the active SparkEnv that the aggregator / sorter collaborators read for
    // their serializer, block manager, and memory manager. LocalSparkContext resets it per test.
    if (sc == null) {
      sc = new SparkContext("local", "test", conf)
    }
    val serializer = new JavaSerializer(conf)

    // Control the dependency knobs the reader consults; isShuffleMergeFinalizedMarked is stubbed
    // false so the reader takes the plain (non-push-based) getMapSizesByExecutorId path.
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(aggregator)
    when(dependency.keyOrdering).thenReturn(keyOrdering)
    when(dependency.mapSideCombine).thenReturn(mapSideCombine)
    when(dependency.isShuffleMergeFinalizedMarked).thenReturn(false)
    val handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency, 20, 80, -1)

    // Compression disabled so SerializerManager.wrapStream is a no-op and the raw serialized
    // payload round-trips, mirroring BlockStoreShuffleReaderSuite.
    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val payload = serializedPayload(serializer, recordsPerMap)
    val buffers = (0 until numMaps).map(mapId => frameBuffer(payload, mapId, corruptCrc))

    // The reader fetches each block through BlockTransferService.fetchBlockSync, so stub it to
    // return the framed envelope for each map block (or to fail for the timeout scenario).
    val transferService = mock(classOf[BlockTransferService])
    if (failFetch) {
      when(transferService.fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(), any()))
        .thenThrow(new RuntimeException("simulated 5s connection timeout"))
    } else {
      buffers.zipWithIndex.foreach { case (buffer, mapId) =>
        val blockName = ShuffleBlockId(shuffleId, mapId, reduceId).name
        when(transferService.fetchBlockSync(
          anyString(), anyInt(), anyString(), meq(blockName), any()))
          .thenReturn(buffer)
      }
    }

    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockTransferService).thenReturn(transferService)

    // Resolve block locations through a mocked MapOutputTracker, all reported as living on one
    // (mocked) executor, mirroring the model's local-only scenario.
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    val blocksByAddress =
      if (emptyOutput) {
        Iterator.empty
      } else {
        val address = BlockManagerId("test-client", "test-client", 1)
        val blocks = (0 until numMaps).map { mapId =>
          (ShuffleBlockId(shuffleId, mapId, reduceId), (payload.length + 32).toLong, mapId)
        }
        Seq((address, blocks)).iterator
      }
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, startPartition, endPartition)).thenReturn(blocksByAddress)

    val streamingConfig = new StreamingShuffleConfig(new SparkConf(false))
    val transport = new StreamingShuffleTransport(streamingConfig, None)
    val streamingMetrics = new StreamingShuffleMetrics()
    // Backpressure is an incidental collaborator for the reader path under test (its own state
    // machine is covered by BackpressureProtocolSuite). The reader only records producer liveness
    // on it; a mock keeps isProducerTimedOut false so reads exercise the fetch/deserialize path,
    // while the dedicated timeout test drives failure through the stubbed fetchBlockSync instead.
    val backpressure = mock(classOf[BackpressureProtocol])

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle, 0, numMaps, startPartition, endPartition, context, readMetrics,
      streamingConfig, streamingMetrics, transport, backpressure, serializerManager, blockManager,
      mapOutputTracker)

    new Fixture(reader, buffers, streamingMetrics, context)
  }

  /**
   * Drives the reader to completion with the fixture's task context installed as the thread-local
   * [[TaskContext]]. This is required because `Aggregator.combineValuesByKey` resolves its
   * `ExternalAppendOnlyMap`'s memory manager from `TaskContext.get()`; the context is always
   * cleared afterward, even if `read()` throws.
   */
  private def drain(fixture: Fixture): List[Product2[Int, Int]] = {
    TaskContext.setTaskContext(fixture.context)
    try {
      fixture.reader.read().toList
    } finally {
      TaskContext.unset()
    }
  }

  test("read() returns deserialized records and releases all buffers") {
    val numMaps = 6
    val recordsPerMap = 10
    val fixture = buildReader(numMaps, recordsPerMap)

    val records = drain(fixture)

    assert(records.length === recordsPerMap * numMaps)
    val byKey = records.groupBy(_._1)
    assert(byKey.keySet === (0 until recordsPerMap).toSet)
    byKey.foreach { case (key, pairs) =>
      assert(pairs.length === numMaps)
      assert(pairs.forall(_._2 == 2 * key))
    }

    // The streaming reader releases each fetched buffer exactly once and never retains it (unlike
    // BlockStoreShuffleReader, which both retains and releases), so no buffer is leaked.
    fixture.buffers.foreach { buffer =>
      assert(buffer.callsToRelease === 1)
      assert(buffer.callsToRetain === 0)
    }
  }

  test("read() honors aggregator (mapSideCombine = false -> combineValuesByKey)") {
    val numMaps = 4
    val recordsPerMap = 5
    val sumAggregator = new Aggregator[Int, Int, Int](
      (value: Int) => value,
      (combined: Int, value: Int) => combined + value,
      (left: Int, right: Int) => left + right)
    val fixture = buildReader(
      numMaps, recordsPerMap, aggregator = Some(sumAggregator), mapSideCombine = false)

    val combined = drain(fixture).map(pair => pair._1 -> pair._2).toMap

    // Each of the numMaps producers emits (i, 2*i), so combineValuesByKey sums to numMaps * 2 * i.
    assert(combined.size === recordsPerMap)
    (0 until recordsPerMap).foreach { key =>
      assert(combined(key) === numMaps * 2 * key)
    }
  }

  test("read() honors keyOrdering (sorts when ordering present)") {
    val numMaps = 3
    val recordsPerMap = 8
    val fixture = buildReader(
      numMaps, recordsPerMap, keyOrdering = Some(implicitly[Ordering[Int]]))

    val records = drain(fixture)
    val keys = records.map(_._1)

    assert(records.length === recordsPerMap * numMaps)
    assert(keys === keys.sorted)
    assert(keys.toSet === (0 until recordsPerMap).toSet)
    // Sorting preserves the (i, 2*i) value mapping produced by every map.
    assert(records.forall(pair => pair._2 == 2 * pair._1))
  }

  test("CRC32C validation accepts well-formed blocks") {
    val numMaps = 2
    val recordsPerMap = 4
    val fixture = buildReader(numMaps, recordsPerMap)

    // Every block carries the CRC32C computed by StreamingBlockEnvelope.create, so verifyChecksum
    // passes and the read completes without raising a fetch failure.
    val records = drain(fixture)

    assert(records.length === recordsPerMap * numMaps)
    assert(records.forall(pair => pair._2 == 2 * pair._1))
    assert(fixture.metrics.partialReadInvalidations === 0L)
  }

  test("CRC32C mismatch invalidates the read and raises FetchFailedException") {
    val fixture = buildReader(numMaps = 2, recordsPerMap = 4, corruptCrc = true)
    val before = fixture.metrics.partialReadInvalidations

    // A flipped checksum must be treated as a fetch failure (recompute -> zero data loss).
    intercept[FetchFailedException] {
      drain(fixture)
    }

    assert(fixture.metrics.partialReadInvalidations === before + 1)
  }

  test("5s connection timeout invalidates partial read and throws FetchFailedException") {
    // SPARK-19276: the reader must throw the FetchFailedException immediately (never construct and
    // ignore it). The timeout is driven by a stubbed fetch failure, so the test is deterministic
    // and does not wait the real 5 s connection-timeout window.
    val fixture = buildReader(numMaps = 2, recordsPerMap = 4, failFetch = true)
    val before = fixture.metrics.partialReadInvalidations

    intercept[FetchFailedException] {
      drain(fixture)
    }

    assert(fixture.metrics.partialReadInvalidations === before + 1)
  }

  test("empty map output yields an empty iterator") {
    val fixture = buildReader(numMaps = 0, recordsPerMap = 0, emptyOutput = true)

    val records = drain(fixture)

    assert(records.isEmpty)
    assert(fixture.metrics.partialReadInvalidations === 0L)
  }
}
