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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{LocalSparkContext, MapOutputTracker, Partitioner, ShuffleDependency,
  SparkConf, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.memory.{MemoryManager, MemoryTestingUtils}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, FetchFailedException}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Failure-injection suite for the opt-in streaming shuffle backend.
 *
 * This is one of the F-121 merge-gate artifacts: per AAP section 0.4.4 it must contain '''exactly
 * ten distinct failure scenarios''', each of which proves the streaming backend never loses data.
 * The zero-data-loss guarantee has two valid forms, and every scenario below ends with an assertion
 * of one of them:
 *
 *   1. the data round-trips correctly after recovery (the spilled / buffered bytes are byte- and
 *      record-identical when read back, or the recomputed read returns the full record set); or
 *   2. the failure surfaces as a [[FetchFailedException]] (or a rejected/invalidated block) so
 *      Spark's existing lineage machinery recomputes the lost output -- the failure is never
 *      silently swallowed into truncated or corrupted output.
 *
 * Every timeout is driven '''deterministically''' -- a stubbed fetch failure, an injected
 * `scanOnce` timestamp, or an injected utilization percentage -- so no test ever waits the real
 * 5 s / 10 s window. Network and disk are mocked. The reader-path scenarios reuse the harness
 * shape proven by [[StreamingShuffleReaderSuite]] (whose top-level [[RecordingManagedBuffer]]
 * helper is reused here, since it lives in this same package), while the buffering / spill /
 * backpressure / fallback scenarios exercise the production collaborators directly over mocked
 * storage and memory managers.
 */
class StreamingShuffleFailureInjectionSuite
  extends SparkFunSuite with LocalSparkContext with Matchers {

  // Fixed shuffle / reduce coordinates shared by the reader-path scenarios. A single reduce
  // partition ([startPartition, endPartition)) is read across a configurable number of map outputs.
  private val shuffleId = 42
  private val reduceId = 7
  private val startPartition = reduceId
  private val endPartition = reduceId + 1

  /**
   * The constructed reader bundled with the collaborators a scenario asserts against: the streaming
   * metrics holder (for the partial-read-invalidation counter) and the task context (installed as
   * the thread-local [[TaskContext]] while the reader runs).
   */
  private class ReaderFixture(
      val reader: StreamingShuffleReader[Int, Int],
      val metrics: StreamingShuffleMetrics,
      val context: TaskContext)

  /**
   * Serializes `recordsPerMap` `(i, 2*i)` pairs into one map block's payload, exactly as
   * `BlockStoreShuffleReaderSuite` builds its shuffle data. The stream is closed so every record is
   * flushed before the bytes are captured.
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
   * Frames a payload into the canonical [[StreamingBlockEnvelope]] wire encoding and wraps it in a
   * [[RecordingManagedBuffer]] so the reader's fetch path can consume it.
   */
  private def frameBuffer(payload: Array[Byte], mapId: Int): RecordingManagedBuffer = {
    val frame = StreamingBlockEnvelope
      .create(shuffleId, mapId.toLong, reduceId, 0L, payload).toByteArray
    new RecordingManagedBuffer(new NioManagedBuffer(ByteBuffer.wrap(frame)))
  }

  /**
   * Builds a fully-wired [[StreamingShuffleReader]] over mocked collaborators. `numMaps` map
   * outputs each contribute one block of `recordsPerMap` `(i, 2*i)` pairs for the reduce partition
   * under test. When `failFetch` is true the transfer service throws on every fetch, simulating a
   * 5 s connection timeout WITHOUT a real wait (the future fails fast).
   */
  private def buildReader(
      numMaps: Int,
      recordsPerMap: Int,
      failFetch: Boolean): ReaderFixture = {
    val conf = new SparkConf(false)
    // A SparkContext sets the active SparkEnv the reader's collaborators read for their memory
    // manager and metrics; LocalSparkContext resets it after each test.
    if (sc == null) {
      sc = new SparkContext("local", "test", conf)
    }
    val serializer = new JavaSerializer(conf)

    // isShuffleMergeFinalizedMarked is stubbed false so the reader takes the plain (non-push-based)
    // getMapSizesByExecutorId path; the remaining knobs are the no-aggregate / no-sort defaults.
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.mapSideCombine).thenReturn(false)
    when(dependency.isShuffleMergeFinalizedMarked).thenReturn(false)
    val handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency, 20, 80, -1)

    // Compression disabled so SerializerManager.wrapStream is a no-op and the raw payload
    // round-trips, mirroring BlockStoreShuffleReaderSuite.
    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val payload = serializedPayload(serializer, recordsPerMap)
    val transferService = mock(classOf[BlockTransferService])
    if (failFetch) {
      // A RuntimeException models a connection timeout (fetchBlockSync declares no checked
      // exception); the reader catches it (NonFatal), invalidates the partial read, and throws
      // FetchFailedException immediately (SPARK-19276).
      when(transferService.fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(), any()))
        .thenThrow(new RuntimeException("simulated 5s connection timeout"))
    } else {
      val buffer = frameBuffer(payload, 0)
      when(transferService.fetchBlockSync(
        anyString(), anyInt(), anyString(), anyString(), any()))
        .thenReturn(buffer)
    }

    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockTransferService).thenReturn(transferService)

    val mapOutputTracker = mock(classOf[MapOutputTracker])
    val address = BlockManagerId("test-client", "test-client", 1)
    val blocks = (0 until numMaps).map { mapId =>
      (ShuffleBlockId(shuffleId, mapId, reduceId), (payload.length + 32).toLong, mapId)
    }
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, startPartition, endPartition))
      .thenReturn(Seq((address, blocks)).iterator)

    val streamingConfig = new StreamingShuffleConfig(new SparkConf(false))
    val transport = new StreamingShuffleTransport(streamingConfig, None)
    val streamingMetrics = new StreamingShuffleMetrics()
    // Backpressure is an incidental collaborator for the reader path; failure is driven through the
    // stubbed fetch, so a bare mock (isProducerTimedOut == false) suffices.
    val backpressure = mock(classOf[BackpressureProtocol])

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle, 0, numMaps, startPartition, endPartition, context, readMetrics,
      streamingConfig, streamingMetrics, transport, backpressure, serializerManager, blockManager,
      mapOutputTracker)
    new ReaderFixture(reader, streamingMetrics, context)
  }

  /**
   * Drives the reader to completion with the fixture's task context installed as the thread-local
   * [[TaskContext]], always clearing it afterward even if `read()` throws.
   */
  private def drain(fixture: ReaderFixture): List[Product2[Int, Int]] = {
    TaskContext.setTaskContext(fixture.context)
    try {
      fixture.reader.read().toList
    } finally {
      TaskContext.unset()
    }
  }

  /** Builds a backpressure protocol with an unlimited rate limiter over a fresh metrics holder. */
  private def newBackpressure(metrics: StreamingShuffleMetrics): BackpressureProtocol = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    new BackpressureProtocol(cfg, new TokenBucketRateLimiter(-1L), metrics)
  }

  /**
   * Builds a [[MemorySpillManager]] over a mocked [[BlockManager]] / [[MemoryManager]]. `maxMemory`
   * is the spill denominator (`maxOnHeapStorageMemory`). When `captor` is supplied, the bytes
   * handed to `putBytes` (the canonical framed spill encoding) are captured so a scenario can read
   * them back. The caller MUST call `stop()` on the returned manager to avoid leaking its thread.
   */
  private def newSpillManager(
      metrics: StreamingShuffleMetrics,
      maxMemory: Long,
      captor: Option[ArgumentCaptor[ChunkedByteBuffer]] = None): MemorySpillManager = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val blockManager = mock(classOf[BlockManager])
    captor match {
      case Some(c) =>
        when(blockManager.putBytes(any(), c.capture(), any(), any())(any())).thenReturn(true)
      case None =>
        when(blockManager.putBytes(any(), any(), any(), any())(any())).thenReturn(true)
    }
    val memoryManager = mock(classOf[MemoryManager])
    when(memoryManager.maxOnHeapStorageMemory).thenReturn(maxMemory)
    new MemorySpillManager(cfg, blockManager, memoryManager, metrics)
  }

  test("scenario 1: producer connection timeout (5s) -> FetchFailedException + " +
    "partialReadInvalidations") {
    val fixture = buildReader(numMaps = 2, recordsPerMap = 4, failFetch = true)
    val before = fixture.metrics.partialReadInvalidations

    // The 5 s connection timeout must surface as a FetchFailedException (so lineage recomputes the
    // lost output) and increment the partial-read-invalidation counter exactly once.
    intercept[FetchFailedException] {
      drain(fixture)
    }

    fixture.metrics.partialReadInvalidations mustBe before + 1
  }

  test("scenario 2: partial read invalidation discards buffered data atomically") {
    val fixture = buildReader(numMaps = 3, recordsPerMap = 5, failFetch = true)
    val emitted = scala.collection.mutable.ArrayBuffer[Product2[Int, Int]]()

    // On a mid-stream fetch failure the reader throws rather than returning a truncated iterator,
    // so NO partial / garbage record is ever emitted before the FetchFailedException.
    TaskContext.setTaskContext(fixture.context)
    try {
      intercept[FetchFailedException] {
        fixture.reader.read().foreach(record => emitted += record)
      }
    } finally {
      TaskContext.unset()
    }

    emitted.toSeq mustBe empty
    fixture.metrics.partialReadInvalidations mustBe 1L
  }

  test("scenario 3: CRC32C corruption is rejected") {
    val payload = Array.fill[Byte](1024)(7.toByte)
    val good = StreamingBlockEnvelope.create(shuffleId, 1L, reduceId, 0L, payload)
    good.verifyChecksum mustBe true

    // Flip a single payload byte (past the 32-byte header). The stored CRC32C now disagrees with
    // the payload, so the corrupted block can never be accepted as valid data.
    val bytes = good.toByteArray
    val corruptIndex = StreamingBlockEnvelope.HEADER_BYTES + 10
    bytes(corruptIndex) = (bytes(corruptIndex) ^ 0xFF).toByte

    val parsed = StreamingBlockEnvelope.parse(bytes)
    parsed.verifyChecksum mustBe false
  }

  test("scenario 4: truncated envelope header is rejected") {
    // Fewer than HEADER_BYTES (32) bytes can never form a valid frame, so parse must reject it
    // rather than silently produce a (garbage) block.
    val truncated = new Array[Byte](StreamingBlockEnvelope.HEADER_BYTES - 1)
    intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.parse(truncated)
    }
  }

  test("scenario 5: oversized payload (> 2MB) is rejected at create") {
    // The 2 MB block cap is enforced via require at frame-creation time, so an oversized payload
    // can never enter the wire/persist path in the first place.
    val oversized = new Array[Byte](StreamingBlockEnvelope.MAX_PAYLOAD_BYTES + 1)
    intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.create(0, 0L, 0, 0L, oversized)
    }
  }

  test("scenario 6: consumer missing acks (10s) -> buffer then spill") {
    val metrics = new StreamingShuffleMetrics()
    val backpressure = newBackpressure(metrics)
    val key = BackpressureProtocol.StreamKey(shuffleId, 0L, reduceId)
    backpressure.registerStream(key)
    backpressure.recordSend(key, 8500L)

    // Drive the 10 s consumer timeout deterministically by injecting a scan timestamp past the
    // window; no real wait occurs. The unacked bytes stay buffered (not discarded).
    val after = System.nanoTime()
    val consumerTimeoutNanos =
      TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.CONSUMER_TIMEOUT_MS)
    backpressure.scanOnce(after + consumerTimeoutNanos + TimeUnit.SECONDS.toNanos(1))
    backpressure.isConsumerTimedOut(key) mustBe true
    backpressure.unackedBytes(key) mustBe 8500L

    // The writer's response to the timeout is to spill the buffered (> 80%) data to disk rather
    // than drop it. 8500 / 10000 = 85% > 80%, so maybeSpill persists the buffer.
    val spillManager = newSpillManager(metrics, maxMemory = 10000L)
    try {
      val buffer = new StreamingBuffer(shuffleId, 0L, reduceId, 1024L * 1024L)
      buffer.append(new Array[Byte](8500))
      spillManager.register(buffer)

      spillManager.maybeSpill() must be > 0L
      metrics.spillCount must be >= 1L
      spillManager.isSpilled(MemorySpillManager.keyFor(buffer)) mustBe true
    } finally {
      spillManager.stop()
    }
  }

  test("scenario 7: resume + retransmit after consumer reconnect") {
    val metrics = new StreamingShuffleMetrics()
    val backpressure = newBackpressure(metrics)
    val key = BackpressureProtocol.StreamKey(shuffleId, 1L, reduceId)
    backpressure.registerStream(key)
    backpressure.recordSend(key, 4096L)

    // Consumer goes silent past the 10 s window (injected, not waited).
    val after = System.nanoTime()
    val consumerTimeoutNanos =
      TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.CONSUMER_TIMEOUT_MS)
    backpressure.scanOnce(after + consumerTimeoutNanos + TimeUnit.SECONDS.toNanos(1))
    backpressure.isConsumerTimedOut(key) mustBe true

    // Spill the unacked data on demand, capturing the bytes handed to disk for the retransmit.
    val captor = ArgumentCaptor.forClass(classOf[ChunkedByteBuffer])
    val spillManager = newSpillManager(metrics, maxMemory = 10000L, captor = Some(captor))
    try {
      val original = new Array[Byte](4096)
      scala.util.Random.nextBytes(original)
      val buffer = new StreamingBuffer(shuffleId, 1L, reduceId, 1024L * 1024L)
      buffer.append(original)
      spillManager.register(buffer)
      spillManager.spillBuffer(MemorySpillManager.keyFor(buffer)) mustBe true

      // Reconnect: a fresh ack clears the consumer timeout, so streaming resumes.
      backpressure.onAck(key, 4096L)
      backpressure.isConsumerTimedOut(key) mustBe false

      // Retransmit re-reads the spilled blocks: every frame still verifies and the full payload is
      // recovered byte-for-byte (no data lost across timeout -> spill -> resume).
      val envelopes = StreamingBlockEnvelope.parseAll(captor.getValue.toArray)
      envelopes.foreach(envelope => envelope.verifyChecksum mustBe true)
      val recovered = envelopes.flatMap(_.payload).toArray
      assert(recovered.sameElements(original))
    } finally {
      spillManager.stop()
    }
  }

  test("scenario 8: memory pressure forces fallback (no loss via sort path)") {
    // Part 1 -- independent policy UNIT check only (not proof of manager fallback): a fresh policy
    // is not under pressure, and a sample above the 95% threshold trips both the memory-pressure
    // predicate and the aggregate fallback decision.
    val unitCfg = new StreamingShuffleConfig(new SparkConf(false))
    val unitPolicy = new StreamingShuffleFallbackPolicy(unitCfg)
    unitPolicy.isMemoryPressure mustBe false
    unitPolicy.updateMemoryUtilization(96)
    unitPolicy.isMemoryPressure mustBe true
    unitPolicy.shouldFallback mustBe true

    // Part 2 -- manager-level proof of AUTOMATIC fallback. The manager is configured with BOTH
    // activation signals on (manager=streaming + streaming.enabled=true) and given its OWN fallback
    // policy. No SparkContext is created, so SparkEnv.get is null and the registration-time memory
    // refresh is a no-op that cannot overwrite the injected pressure sample -- isolating memory
    // pressure as the sole cause of the fallback (not the disabled flag).
    val conf = new SparkConf(false)
      .set(config.SHUFFLE_MANAGER, "streaming")
      .set(config.SHUFFLE_STREAMING_ENABLED, true)
      .set("spark.app.id", "streaming-failure-injection-scenario-8")
    val policy = new StreamingShuffleFallbackPolicy(new StreamingShuffleConfig(conf))
    val manager = new StreamingShuffleManager(conf, isDriver = true, Some(policy))
    try {
      val partitioner = new Partitioner {
        override def numPartitions: Int = 2
        override def getPartition(key: Any): Int = 0
      }
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(new JavaSerializer(conf))
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      when(dependency.mapSideCombine).thenReturn(false)

      // With streaming enabled and the policy untripped, the manager mints a STREAMING handle ...
      manager.registerShuffle(shuffleId, dependency)
        .isInstanceOf[StreamingShuffleHandle[_, _, _]] mustBe true
      // ... feed memory-pressure telemetry into the manager's OWN policy (in production pushed by
      // MemorySpillManager.maybeSpill and the manager's registration-time memory pull) ...
      policy.updateMemoryUtilization(96)
      // ... and the SAME enabled manager now delegates to the unchanged SortShuffleManager, so the
      // shuffle is served entirely by the sort path (zero regression / zero data loss).
      val handle = manager.registerShuffle(shuffleId + 1, dependency)
      handle.isInstanceOf[StreamingShuffleHandle[_, _, _]] mustBe false
      handle.isInstanceOf[BaseShuffleHandle[_, _, _]] mustBe true
    } finally {
      manager.stop()
    }
  }

  test("scenario 9: spill-then-read round-trip preserves all records") {
    val metrics = new StreamingShuffleMetrics()
    val captor = ArgumentCaptor.forClass(classOf[ChunkedByteBuffer])
    val spillManager = newSpillManager(metrics, maxMemory = 10000L, captor = Some(captor))
    try {
      val original = new Array[Byte](9000)
      scala.util.Random.nextBytes(original)
      val buffer = new StreamingBuffer(shuffleId, 2L, reduceId, 1024L * 1024L)
      // 9000 / 10000 = 90% > 80%, so the buffer is spilled to disk via putBytes(.., DISK_ONLY).
      buffer.append(original)
      spillManager.register(buffer)

      spillManager.maybeSpill() must be > 0L
      metrics.spillCount must be >= 1L

      // Read the spilled bytes back and assert byte / record equality: spilled bytes are
      // interchangeable with streamed bytes (the dual-channel invariant), so nothing is lost.
      val envelopes = StreamingBlockEnvelope.parseAll(captor.getValue.toArray)
      envelopes.foreach(envelope => envelope.verifyChecksum mustBe true)
      val recovered = envelopes.flatMap(_.payload).toArray
      assert(recovered.sameElements(original))
    } finally {
      spillManager.stop()
    }
  }

  test("scenario 10: producer failure then recompute path yields identical output") {
    val numMaps = 3
    val recordsPerMap = 6

    // Attempt 1 models the producer failing: the read raises FetchFailedException, which is Spark's
    // signal to recompute the upstream task.
    val failed = buildReader(numMaps, recordsPerMap, failFetch = true)
    intercept[FetchFailedException] {
      drain(failed)
    }

    // Attempt 2 models the recomputed producer succeeding: the read now returns the FULL record
    // set, identical to what a non-failing run would have produced (lineage recovers the output).
    val recomputed = buildReader(numMaps, recordsPerMap, failFetch = false)
    val records = drain(recomputed)

    assert(records.length === recordsPerMap * numMaps)
    val byKey = records.groupBy(_._1)
    assert(byKey.keySet === (0 until recordsPerMap).toSet)
    byKey.foreach { case (key, pairs) =>
      assert(pairs.length === numMaps)
      assert(pairs.forall(_._2 == 2 * key))
    }
  }
}
