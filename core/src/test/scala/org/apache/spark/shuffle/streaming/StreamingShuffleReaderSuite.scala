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

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
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
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] aggregation/ordering path, so the
 * end-to-end read tests closely mirror `BlockStoreShuffleReaderSuite`: they serve serialized
 * `(i, 2*i)` records -- each map block framed as a canonical [[StreamingBlockEnvelope]] -- from a
 * mocked [[org.apache.spark.storage.BlockManager]]'s
 * [[org.apache.spark.network.BlockTransferService#fetchBlockSync]] (the CP2 reader data plane),
 * resolve them through a mocked [[org.apache.spark.MapOutputTracker]], and assert that every
 * [[RecordingManagedBuffer]] is released exactly once (the CP2 reader copies validated payloads
 * out and never retains, so this is the no-leak invariant under the release-only lifecycle).
 *
 * Beyond the happy-path read they verify the streaming-specific guarantees: honoring the
 * dependency's `aggregator` / `keyOrdering` / `mapSideCombine`, CRC32C acceptance of well-formed
 * blocks on the consumer-stream channel, and - the signature requirement - that a producer
 * failure increments `partialReadInvalidations` and IMMEDIATELY throws a
 * [[org.apache.spark.shuffle.FetchFailedException]] (SPARK-19276: never construct-and-ignore a
 * fetch failure). The failure path is driven deterministically through a corrupt-CRC block (the
 * same `invalidatePartialReads` entry point the 5 s connection timeout uses) so the test needs no
 * real network wait. A second group of unit tests exercises the package-private
 * `extractValidatedPayloads` envelope de-framing / CRC32C integrity core directly with hand-built
 * frames (multi-frame concatenation, empty block, CRC mismatch, truncation, partial trailing
 * header).
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

    // A mocked BlockManager whose BlockTransferService serves each map block. The CP2 reader
    // data plane fetches every block through BlockTransferService.fetchBlockSync (NOT
    // getLocalBlockData) and expects the fetched bytes to be framed StreamingBlockEnvelopes, so
    // each RecordingManagedBuffer wraps the map's records in one canonical 32-byte-header frame.
    // The recorder still lets us assert that the reader releases every fetched buffer exactly once.
    val blockManager = mock(classOf[BlockManager])
    val blockTransferService = mock(classOf[BlockTransferService])
    when(blockManager.blockTransferService).thenReturn(blockTransferService)
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val buffers = (0 until numMaps).map { mapId =>
      val framed = StreamingBlockEnvelope
        .create(shuffleId, mapId.toLong, reduceId, sequenceNumber = 0L, payload = recordBytes)
        .toByteArray
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(framed))
      val managedBuffer = new RecordingManagedBuffer(nioBuffer)
      val blockId = ShuffleBlockId(shuffleId, mapId, reduceId).toString
      when(blockTransferService.fetchBlockSync(
        meq(localBlockManagerId.host),
        meq(localBlockManagerId.port),
        meq(localBlockManagerId.executorId),
        meq(blockId),
        any())).thenReturn(managedBuffer)
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

    // The CP2 reader deserializes the de-enveloped payload bytes RAW with the dependency
    // serializer (the writer does not compression-wrap the stream), so no SerializerManager is
    // threaded through the reader; the records were serialized without compression above.
    val reader = new StreamingShuffleReader[Int, Int](
      handle, 0, numMaps, reduceId, reduceId + 1, taskContext, readMetrics,
      streamingConfig, streamingMetrics, transport, blockManager,
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
    // The CP2 reader copies each validated payload into an independent array and releases the
    // fetched buffer once in a finally; it never retains. Asserting release == 1 (retain == 0)
    // preserves the original no-leak intent against the current release-only buffer lifecycle.
    buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 0)
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

  // ---------------------------------------------------------------------------------------------
  // Envelope de-framing / CRC32C integrity unit tests (retained from the CP2 reader review).
  //
  // These target the data-integrity core directly: the real fetched bytes are framed
  // StreamingBlockEnvelopes, so the reader must parse every frame, verify its CRC32C, and
  // concatenate the payload-only bytes before deserialization -- never feed the 32-byte headers
  // to the serializer, and never accept a truncated/oversized/corrupt frame. The package-private
  // extractValidatedPayloads is exercised directly with hand-built frames over fully mocked
  // collaborators (no SparkContext is needed for this path).
  // ---------------------------------------------------------------------------------------------

  private val integrityBmId = BlockManagerId("exec-1", "host-1", 7337)

  /**
   * Builds a reader with fully mocked collaborators. No SparkContext is required: the envelope
   * extraction path only uses the real [[StreamingShuffleMetrics]] (passed in so a test can assert
   * the invalidation counter) and the real [[StreamingShuffleConfig]]. The block manager and map
   * output tracker are mocked and never invoked by the envelope extraction under test.
   */
  private def buildIntegrityReader(
      metrics: StreamingShuffleMetrics): StreamingShuffleReader[Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      0, dep, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)
    new StreamingShuffleReader[Int, Int](
      handle,
      startMapIndex = 0,
      endMapIndex = 1,
      startPartition = 0,
      endPartition = 1,
      context = mock(classOf[TaskContext]),
      readMetrics = mock(classOf[ShuffleReadMetricsReporter]),
      config = new StreamingShuffleConfig(new SparkConf(false)),
      streamingMetrics = metrics,
      transport = mock(classOf[StreamingShuffleTransport]),
      blockManager = mock(classOf[BlockManager]),
      mapOutputTracker = mock(classOf[MapOutputTracker]))
  }

  /** Frames a payload into the canonical 32-byte-header envelope bytes (CRC computed by create). */
  private def frame(seq: Long, payload: Array[Byte]): Array[Byte] =
    StreamingBlockEnvelope.create(0, 0L, 0, seq, payload).toByteArray

  /** Concatenates frame byte arrays into a single fetched-block buffer. */
  private def concat(parts: Array[Byte]*): Array[Byte] = parts.flatten.toArray

  /** Deterministic payload of length `n`; content is irrelevant since the CRC covers it. */
  private def payloadOf(n: Int): Array[Byte] = Array.tabulate(n)(i => (i % 127).toByte)

  test("extractValidatedPayloads de-frames and concatenates multiple payloads, headers stripped") {
    val reader = buildIntegrityReader(new StreamingShuffleMetrics)
    val p0 = Array.emptyByteArray // an empty-payload frame must contribute zero bytes, not 32
    val p1 = payloadOf(5000)
    val p2 = payloadOf(37)
    val raw = concat(frame(0L, p0), frame(1L, p1), frame(2L, p2))

    val out = reader.extractValidatedPayloads(ByteBuffer.wrap(raw), integrityBmId, 0L, 0, 0)

    // Only the payload bytes, in frame order, with every 32-byte header stripped.
    assert(out.sameElements(p0 ++ p1 ++ p2))
  }

  test("extractValidatedPayloads returns an empty array for an empty fetched block") {
    val reader = buildIntegrityReader(new StreamingShuffleMetrics)
    val out = reader.extractValidatedPayloads(
      ByteBuffer.wrap(Array.emptyByteArray), integrityBmId, 0L, 0, 0)
    assert(out.isEmpty)
  }

  test("extractValidatedPayloads fails a CRC32C-mismatched frame and counts the invalidation") {
    val metrics = new StreamingShuffleMetrics
    val reader = buildIntegrityReader(metrics)
    val corrupt = frame(0L, payloadOf(64))
    // Flip the first payload byte (just past the 32-byte header) so the recomputed CRC32C differs.
    val idx = StreamingBlockEnvelope.HEADER_BYTES
    corrupt(idx) = (corrupt(idx) ^ 0xFF).toByte
    val before = metrics.partialReadInvalidations

    intercept[FetchFailedException] {
      reader.extractValidatedPayloads(ByteBuffer.wrap(corrupt), integrityBmId, 0L, 0, 0)
    }
    assert(metrics.partialReadInvalidations === before + 1L)
  }

  test("extractValidatedPayloads fails a truncated frame") {
    val reader = buildIntegrityReader(new StreamingShuffleMetrics)
    // A full frame minus the last 10 payload bytes: parse must reject it as truncated.
    val truncated = frame(0L, payloadOf(128)).dropRight(10)

    intercept[FetchFailedException] {
      reader.extractValidatedPayloads(ByteBuffer.wrap(truncated), integrityBmId, 0L, 0, 0)
    }
  }

  test("extractValidatedPayloads fails on trailing partial-header bytes after a valid frame") {
    val reader = buildIntegrityReader(new StreamingShuffleMetrics)
    // A valid frame followed by 10 stray bytes -- fewer than the 32-byte header -- must fail rather
    // than be silently ignored: a partial header signals a corrupt/truncated producer write.
    val raw = concat(frame(0L, payloadOf(32)), new Array[Byte](10))

    intercept[FetchFailedException] {
      reader.extractValidatedPayloads(ByteBuffer.wrap(raw), integrityBmId, 0L, 0, 0)
    }
  }
}
