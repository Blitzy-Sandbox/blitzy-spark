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

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, eq => meq}
import org.mockito.Mockito.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.PrivateMethodTester

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryManager
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Failure-injection suite for the streaming shuffle data path (test #12 of 14, feature F-121).
 *
 * This suite deliberately injects ten distinct failure scenarios into the streaming shuffle
 * data path -- primarily through [[StreamingShuffleReader]] and its flow-control / spill
 * collaborators -- and asserts the central '''zero data loss''' invariant under every one.
 * Concretely, every failure must resolve in exactly one of three loss-free ways:
 *
 *  1. '''Invalidate-and-recompute.''' An in-flight read that cannot complete correctly (a
 *     producer timeout, an abrupt mid-read stream termination, or any structurally corrupt frame)
 *     is invalidated rather than returning truncated or corrupt data. A CRC32C checksum mismatch
 *     is first re-fetched up to a bounded number of retransmission attempts within the producer
 *     deadline; only if the corruption persists is the read invalidated. On invalidation the
 *     reader increments `partialReadInvalidations` and constructs-and-throws a
 *     [[org.apache.spark.shuffle.FetchFailedException]] immediately (SPARK-19276), which the
 *     existing DAG scheduler already handles by recomputing the upstream stage -- with no
 *     scheduler modification whatsoever.
 *  2. '''Fall back to sort.''' A degradation condition that is not an in-flight corruption (a
 *     producer/consumer version mismatch, network saturation, or memory pressure that would
 *     risk an OutOfMemoryError) drives [[StreamingShuffleFallbackPolicy]] to a concrete
 *     `FallbackReason`, so the engine reverts to sort-based shuffle instead of losing data.
 *  3. '''Lossless internal mechanics.''' The acknowledgment watermark never regresses under ack
 *     loss / duplication / reordering, and spilling buffered bytes to disk is byte-for-byte
 *     lossless.
 *
 * '''Determinism / no flakiness.''' Producer deadlines are injected through the reader's
 * constructor parameters (a small producer timeout, a bounded retransmission attempt count, a
 * 1 ms backoff) rather than through real wall-clock sleeps, so the "5 s producer timeout"
 * scenario fails deterministically once its tiny injected deadline expires. That scenario models
 * a '''silent / unresponsive producer''' (a transport that accepts the fetch but never invokes
 * the listener), so the reader's bounded await genuinely expires at the injected deadline -- the
 * real timeout mechanism -- rather than completing through an immediate transport error. Spill
 * decisions are driven through the spill manager's synchronous private `pollOnce()` via
 * [[org.scalatest.PrivateMethodTester]] (mirroring `MemorySpillManagerSuite`) rather than its
 * scheduled poll thread. The single end-to-end recomputation scenario runs a real, deterministic
 * Spark job whose one-time fetch failure resolves on the retried stage attempt. The suite
 * therefore runs well within the `SparkFunSuite` time budget with negligible reliance on timing.
 *
 * '''Harness.''' Reader-level scenarios reuse the mock template established by
 * `BlockStoreShuffleReaderSuite`: a mocked [[org.apache.spark.MapOutputTracker]] resolves a
 * single producer block, a mocked [[org.apache.spark.storage.BlockManager]] exposes a mocked
 * [[org.apache.spark.network.BlockTransferService]] whose `fetchBlocks` is told how to fail
 * (or succeed), and a real [[org.apache.spark.serializer.SerializerManager]] with compression
 * disabled wraps the validated payload exactly as production does. The
 * [[StreamingShuffleMetrics]] and [[BackpressureProtocol]] are real, so invalidation and
 * acknowledgment accounting are asserted end to end rather than through stubs.
 */
class StreamingShuffleFailureInjectionSuite
  extends SparkFunSuite
  with LocalSparkContext
  with PrivateMethodTester {

  // -------------------------------------------------------------------------------------------
  // Fixed identifiers shared by the reader-level scenarios. A single producer block
  // (shuffle 0, map 0, reduce 0) at one block-manager address keeps fault injection focused on
  // the read/validate path rather than on map-output bookkeeping.
  // -------------------------------------------------------------------------------------------

  /** The shuffle id under test; matches the value the reader forwards to the map tracker. */
  private val testShuffleId = 0

  /** The producing map task id encoded into the single shuffle block. */
  private val testMapId = 0L

  /** The map index of the producing task within the [start, end) producer range. */
  private val testMapIndex = 0

  /** The reduce partition this consumer reads (within the [start, end) partition range). */
  private val testReduceId = 0

  /** The producer block-manager address fetches are issued against. */
  private val producerAddress = BlockManagerId("exec-producer", "host-producer", 7337)

  /** The single in-progress streaming block the consumer fetches. */
  private val streamingBlockId = ShuffleBlockId(testShuffleId, testMapId, testReduceId)

  /** A fast producer-connection timeout so the timeout scenarios never really sleep. */
  private val FastProducerTimeoutMs = 50L

  /** A single retransmission attempt so the first transport failure invalidates immediately. */
  private val SingleAttempt = 1

  /** A 1 ms initial backoff; bounded by the timeout above so no meaningful sleep ever occurs. */
  private val TinyBackoffMs = 1L

  // -------------------------------------------------------------------------------------------
  // Harness helpers
  // -------------------------------------------------------------------------------------------

  /** A Java serializer over an empty conf; the dependency and read side share its instances. */
  private def newSerializer(): JavaSerializer = new JavaSerializer(new SparkConf(false))

  /**
   * A real [[SerializerManager]] with shuffle and spill compression disabled, so `wrapStream` is
   * a pass-through and the bytes serialized by [[serializeRecords]] round-trip unchanged through
   * the reader -- exactly the configuration `BlockStoreShuffleReaderSuite` uses.
   */
  private def newSerializerManager(serializer: JavaSerializer): SerializerManager = {
    new SerializerManager(
      serializer,
      new SparkConf(false)
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))
  }

  /**
   * A mocked [[org.apache.spark.ShuffleDependency]] stubbed as the sort read path expects: a
   * concrete serializer, and neither an aggregator nor a key ordering, so the reader yields a
   * lazy, unaggregated, unsorted iterator (the configuration under which a failed drain throws
   * rather than eagerly materializing).
   */
  private def newDependency(serializer: JavaSerializer): ShuffleDependency[Int, Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dep.serializer).thenReturn(serializer)
    when(dep.aggregator).thenReturn(None)
    when(dep.keyOrdering).thenReturn(None)
    dep
  }

  /** A streaming handle carrying production-default tuning values over the mocked dependency. */
  private def newHandle(
      dep: ShuffleDependency[Int, Int, Int]): StreamingShuffleHandle[Int, Int, Int] = {
    new StreamingShuffleHandle[Int, Int, Int](
      testShuffleId, dep, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = 0)
  }

  /** The single-block map-output result the mocked tracker returns for the reader's range. */
  private def oneBlockSizes(
      payloadSize: Long): Iterator[(BlockManagerId, Seq[(ShuffleBlockId, Long, Int)])] = {
    Seq((producerAddress, Seq((streamingBlockId, payloadSize, testMapIndex)))).iterator
  }

  /**
   * Serialize `records` to raw bytes with no stream wrapping. Because the read-side
   * [[SerializerManager]] has compression disabled, these raw bytes are exactly what the reader
   * deserializes after validation, so a successful read reproduces `records` verbatim.
   */
  private def serializeRecords(
      serializer: JavaSerializer,
      records: Seq[(Int, Int)]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = serializer.newInstance().serializeStream(baos)
    records.foreach { case (k, v) =>
      out.writeKey(k)
      out.writeValue(v)
    }
    out.close()
    baos.toByteArray
  }

  /** Encode `payload` into a single self-describing envelope frame and return its raw bytes. */
  private def encodeFrameBytes(payload: Array[Byte]): Array[Byte] = {
    val buf = StreamingBlockEnvelope.encode(testShuffleId, testMapId, testReduceId, payload)
    val arr = new Array[Byte](buf.remaining())
    buf.get(arr)
    arr
  }

  /**
   * Flip the first payload byte of an encoded frame (leaving the header's stored checksum
   * intact) so the recomputed CRC32C no longer matches and `verifyChecksum` reports corruption.
   */
  private def corruptPayloadByte(frame: Array[Byte]): Array[Byte] = {
    val corrupted = frame.clone()
    val payloadStart = StreamingBlockEnvelope.HEADER_SIZE
    corrupted(payloadStart) = (corrupted(payloadStart) ^ 0xFF).toByte
    corrupted
  }

  /** Wrap raw block bytes in an [[NioManagedBuffer]] the mocked transfer service can return. */
  private def managedBufferOf(bytes: Array[Byte]): ManagedBuffer =
    new NioManagedBuffer(ByteBuffer.wrap(bytes))

  /**
   * A mocked [[BlockTransferService]] whose `fetchBlocks` evaluates `result` on every call and
   * delivers the outcome to the supplied [[BlockFetchingListener]] (the asynchronous fetch API
   * the streaming reader actually drives). A `result` expression that throws models a transport
   * failure and is delivered via `onBlockFetchFailure`; a buffer models a (possibly corrupt)
   * fetched block and is delivered via `onBlockFetchSuccess`. `result` is by-name so a throwing
   * expression is raised inside the fetch rather than at construction time.
   */
  private def transferReturning(result: => ManagedBuffer): BlockTransferService = {
    val transfer = mock(classOf[BlockTransferService])
    doAnswer { (inv: InvocationOnMock) =>
      val blockIds = inv.getArgument[Array[String]](3)
      val listener = inv.getArgument[BlockFetchingListener](4)
      val outcome: Either[Throwable, ManagedBuffer] =
        try Right(result) catch { case NonFatal(e) => Left(e) }
      blockIds.foreach { bid =>
        outcome match {
          case Right(buf) => listener.onBlockFetchSuccess(bid, buf)
          case Left(e) => listener.onBlockFetchFailure(bid, e)
        }
      }
      null
    }.when(transfer).fetchBlocks(any(), anyInt(), any(), any(), any(), any())
    transfer
  }

  /**
   * A mocked [[BlockTransferService]] whose `fetchBlocks` accepts the request but '''never'''
   * invokes the [[BlockFetchingListener]] -- modeling a silent / unresponsive producer. The
   * reader's bounded await therefore expires at the injected producer deadline (a genuine
   * timeout) rather than completing through either the success or the failure callback.
   */
  private def transferSilent(): BlockTransferService = {
    val transfer = mock(classOf[BlockTransferService])
    doAnswer((_: InvocationOnMock) => null)
      .when(transfer).fetchBlocks(any(), anyInt(), any(), any(), any(), any())
    transfer
  }

  /**
   * Build a [[StreamingShuffleReader]] over the supplied transfer service and metrics. The
   * producer timeout, retransmission cap, and backoff are tiny so any transport failure
   * invalidates almost instantly; the credit window is unlimited (a non-positive link capacity)
   * so acknowledgments on the success path never block.
   */
  private def newReader(
      transfer: BlockTransferService,
      metrics: StreamingShuffleMetrics,
      serializer: JavaSerializer,
      payloadSize: Long,
      producerTimeoutMs: Long = FastProducerTimeoutMs): StreamingShuffleReader[Int, Int] = {
    val dep = newDependency(serializer)
    val handle = newHandle(dep)
    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockTransferService).thenReturn(transfer)
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(testShuffleId, 0, 1, 0, 1))
      .thenReturn(oneBlockSizes(payloadSize))
    val context = TaskContext.empty()
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val backpressure = new BackpressureProtocol(metrics, linkCapacityBytes = 0L)
    new StreamingShuffleReader[Int, Int](
      handle,
      startMapIndex = 0,
      endMapIndex = 1,
      startPartition = 0,
      endPartition = 1,
      context,
      readMetrics,
      backpressure,
      metrics,
      mapOutputTracker,
      blockManager,
      newSerializerManager(serializer),
      producerTimeoutMs = producerTimeoutMs,
      maxRetransmitAttempts = SingleAttempt,
      initialRetryBackoffMs = TinyBackoffMs)
  }

  // ===========================================================================================
  // The ten failure-injection scenarios. Each asserts zero data loss: an invalidation that
  // throws FetchFailedException and advances partialReadInvalidations, a concrete fallback
  // reason, or a lossless round-trip / recompute -- never a silent or truncated result.
  // ===========================================================================================

  // (1) Producer-connection timeout: a SILENT producer accepts the fetch but never delivers the
  // block (no listener callback at all), so the reader's bounded await expires at the injected
  // producer deadline and the read is invalidated -- exercising the timeout MECHANISM itself, not
  // an immediate transport error. A non-trivial deadline (200 ms) lets the test assert that the
  // read genuinely blocked until the deadline rather than failing instantly.
  test("producer connection timeout (>5s) invalidates and throws FetchFailedException") {
    val metrics = new StreamingShuffleMetrics
    val serializer = newSerializer()
    val transfer = transferSilent()
    val timeoutMs = 200L
    val reader = newReader(
      transfer, metrics, serializer, payloadSize = 64L, producerTimeoutMs = timeoutMs)

    assert(metrics.getPartialReadInvalidations === 0L)
    val startNs = System.nanoTime()
    intercept[FetchFailedException] {
      reader.read().toList
    }
    val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)
    assert(metrics.getPartialReadInvalidations === 1L,
      "a producer timeout must record exactly one partial-read invalidation")
    // The invalidation was driven by the deadline-bounded await expiring against a silent
    // producer, not by an immediate transport failure: at least half the injected deadline must
    // have elapsed (an immediate error would return in single-digit milliseconds).
    assert(elapsedMs >= timeoutMs / 2,
      s"expected the read to block until the ~${timeoutMs}ms producer deadline, but returned " +
        s"after only ${elapsedMs}ms (did the producer fail immediately instead of timing out?)")
  }

  // (2) CRC32C mismatch: a structurally valid frame whose payload was altered after the checksum
  // was computed fails verification. The reader re-fetches it up to the bounded retransmission
  // cap (here a single attempt) within the producer deadline; because this transport returns the
  // same corrupt frame on every fetch, the corruption persists and the read is invalidated. (The
  // transient case -- corrupt once, then a clean frame on retransmission succeeds -- is covered
  // by StreamingShuffleReaderSuite.)
  test("CRC32C checksum mismatch invalidates and throws FetchFailedException") {
    val metrics = new StreamingShuffleMetrics
    val serializer = newSerializer()
    val frame = encodeFrameBytes(serializeRecords(serializer, Seq((1, 100))))
    val corrupted = corruptPayloadByte(frame)
    val transfer = transferReturning(managedBufferOf(corrupted))
    val reader = newReader(transfer, metrics, serializer, payloadSize = corrupted.length.toLong)

    intercept[FetchFailedException] {
      reader.read().toList
    }
    assert(metrics.getPartialReadInvalidations === 1L,
      "a persistently corrupt block must record exactly one partial-read invalidation")
  }

  // (3) Consumer crash mid-read: the fetch succeeds (non-empty buffer) but reading its bytes
  // fails partway, as if the producer's stream were severed; this surfaces as a FetchFailed.
  test("consumer crash mid-read surfaces as FetchFailedException (no silent partial result)") {
    val metrics = new StreamingShuffleMetrics
    val serializer = newSerializer()
    val severed = mock(classOf[ManagedBuffer])
    when(severed.size()).thenReturn(128L)
    when(severed.nioByteBuffer())
      .thenAnswer((_: InvocationOnMock) => throw new IOException("stream severed mid-read"))
    val transfer = transferReturning(severed)
    val reader = newReader(transfer, metrics, serializer, payloadSize = 128L)

    intercept[FetchFailedException] {
      reader.read().toList
    }
    assert(metrics.getPartialReadInvalidations === 1L,
      "an abrupt mid-read termination must record exactly one partial-read invalidation")
  }

  // (4) All-or-nothing: a block with two concatenated frames (first valid, second corrupt) is
  // assembled in full before returning, so the corrupt frame discards the valid one and the
  // read yields zero records -- proving a failed read never leaks a truncated prefix.
  test("partial-read invalidation does not return any records") {
    val metrics = new StreamingShuffleMetrics
    val serializer = newSerializer()
    val validFrame = encodeFrameBytes(serializeRecords(serializer, Seq((1, 10), (2, 20))))
    val corruptPayload = serializeRecords(serializer, Seq((3, 30)))
    val corruptFrame = corruptPayloadByte(encodeFrameBytes(corruptPayload))
    val combined = validFrame ++ corruptFrame
    val transfer = transferReturning(managedBufferOf(combined))
    val reader = newReader(transfer, metrics, serializer, payloadSize = combined.length.toLong)

    // read() is lazy: building the iterator must not throw or fetch anything yet.
    val iterator = reader.read()
    val drained = ArrayBuffer.empty[(Int, Int)]
    intercept[FetchFailedException] {
      iterator.foreach(record => drained += (record._1 -> record._2))
    }
    assert(drained.isEmpty,
      "a failed streaming read must yield no records (all-or-nothing), never a truncated set")
    assert(metrics.getPartialReadInvalidations === 1L)
  }

  // (5) Lossy/duplicating/reordering ack channel: dropped, duplicated, and out-of-order acks
  // must never regress the monotonic acknowledgment watermark, so no buffer is reclaimed early.
  test("RPC ack/heartbeat loss does not corrupt the ack watermark") {
    val protocol = new BackpressureProtocol(new StreamingShuffleMetrics, linkCapacityBytes = 0L)
    val key = StreamKey(shuffleId = 0, partitionId = 0, attemptId = 0L, executorId = "exec-c")
    assert(protocol.ackWatermark(key) === 0L)

    // Acks 2, 4 and 6 are dropped; ack 3 arrives twice; acks 1 and 5 arrive out of order.
    val deliveredAcks = Seq(3L, 3L, 1L, 7L, 5L, 7L)
    var previous = protocol.ackWatermark(key)
    deliveredAcks.foreach { seqNo =>
      protocol.mergeAck(key, seqNo)
      assert(protocol.ackWatermark(key) >= previous,
        s"watermark regressed after merging ack $seqNo")
      previous = protocol.ackWatermark(key)
    }
    assert(protocol.ackWatermark(key) === 7L,
      "the watermark must equal the maximum delivered ack despite loss/duplication/reordering")
  }

  // (6) Spill under pressure is lossless: the bytes the spill manager writes to the DISK_ONLY
  // store are captured and must equal the buffered input byte-for-byte (spilled => still
  // readable). pollOnce() is driven synchronously so the assertion does not depend on timing.
  test("spill-under-pressure preserves data (spilled blocks still readable)") {
    val metrics = new StreamingShuffleMetrics
    val blockManager = mock(classOf[BlockManager])
    val memoryManager = mock(classOf[MemoryManager])
    // 80% of a 1000-byte denominator is 800 bytes, so an 850-byte buffer crosses the threshold.
    when(memoryManager.maxOnHeapStorageMemory).thenReturn(1000L)

    val spilledBytes = ArgumentCaptor.forClass(classOf[ChunkedByteBuffer])
    when(
      blockManager.putBytes[Any](
        any(), spilledBytes.capture(), meq(StorageLevel.DISK_ONLY), anyBoolean())(any()))
      .thenReturn(true)

    val manager =
      new MemorySpillManager(blockManager, memoryManager, metrics, spillThresholdPercent = 80)
    try {
      val input = Array.tabulate(850)(i => (i % 127).toByte)
      val buffer = new StreamingBuffer(testReduceId)
      buffer.append(input)
      val key = MemorySpillManager.BufferKey(testShuffleId, testMapId, testReduceId)
      manager.registerBuffer(key, buffer)

      val pollOnce = PrivateMethod[Unit](Symbol("pollOnce"))
      manager.invokePrivate(pollOnce())

      assert(buffer.isSpilled, "the over-threshold buffer should have been spilled to disk")
      assert(metrics.getSpillCount === 1L, "the spill must be counted exactly once")
      assert(spilledBytes.getValue.toArray.sameElements(input),
        "spilling must be byte-for-byte lossless: the on-disk bytes equal the buffered input")
    } finally {
      manager.stop()
    }
  }

  // (7) Version mismatch falls back to sort rather than streaming an incompatible peer.
  test("producer/consumer version mismatch triggers fallback (FallbackPolicy.VersionMismatch)") {
    val policy = new StreamingShuffleFallbackPolicy(
      StreamingShuffleConfig(new SparkConf(false)), new StreamingShuffleMetrics)
    val reason = policy.evaluate(
      producerRate = 1.0,
      consumerRate = 1.0,
      sustainedMs = 0L,
      canAllocate = true,
      networkUtilizationFraction = 0.0,
      producerVersion = "4.2.0",
      consumerVersion = "4.1.0")
    assert(reason.contains(StreamingShuffleFallbackPolicy.VersionMismatch),
      "a version mismatch must fall back to sort rather than risk losing data")
  }

  // (8) Network saturation above 90% falls back to sort.
  test("network saturation > 90% triggers fallback (NetworkSaturation)") {
    val policy = new StreamingShuffleFallbackPolicy(
      StreamingShuffleConfig(new SparkConf(false)), new StreamingShuffleMetrics)
    val reason = policy.evaluate(
      producerRate = 1.0,
      consumerRate = 1.0,
      sustainedMs = 0L,
      canAllocate = true,
      networkUtilizationFraction = 0.91,
      producerVersion = "v1",
      consumerVersion = "v1")
    assert(reason.contains(StreamingShuffleFallbackPolicy.NetworkSaturation),
      "link saturation above 90% must fall back to sort")
  }

  // (9) Memory pressure (a buffer cannot be allocated) falls back to sort, avoiding an OOM
  // rather than losing data.
  test("memory pressure (cannot allocate buffer) triggers fallback (MemoryPressure)") {
    val policy = new StreamingShuffleFallbackPolicy(
      StreamingShuffleConfig(new SparkConf(false)), new StreamingShuffleMetrics)
    val reason = policy.evaluate(
      producerRate = 1.0,
      consumerRate = 1.0,
      sustainedMs = 0L,
      canAllocate = false,
      networkUtilizationFraction = 0.0,
      producerVersion = "v1",
      consumerVersion = "v1")
    assert(reason.contains(StreamingShuffleFallbackPolicy.MemoryPressure),
      "memory pressure must fall back to sort to avoid an OutOfMemoryError, not lose data")
  }

  // (10) Recoverable via recomputation (end-to-end, active streaming). Two complementary proofs:
  // (a) at the reader level, the streaming reader's invalidation surfaces as the very
  // scheduler-recognized FetchFailed reason; (b) end to end, a REAL reduceByKey shuffle running
  // with the streaming data path active throws that same FetchFailedException exactly once (on
  // the first reduce-stage attempt), and the DAG scheduler recomputes the upstream streaming map
  // stage and retries to completion with the full, correct output -- a genuine scheduler-driven
  // recovery with zero data loss, requiring no scheduler modification.
  test("end-to-end: an injected fetch failure is recoverable via recomputation") {
    val serializer = newSerializer()

    // (a) Reader level: a streaming-reader failure is the scheduler's FetchFailed reason.
    val failingMetrics = new StreamingShuffleMetrics
    val failingTransfer = transferReturning(
      throw new IOException("injected one-time fetch failure"))
    val failingReader = newReader(failingTransfer, failingMetrics, serializer, payloadSize = 64L)
    val failure = intercept[FetchFailedException] {
      failingReader.read().toList
    }
    assert(failure.toTaskFailedReason.isInstanceOf[FetchFailed],
      "the streaming failure must be the scheduler-recognized FetchFailed reason")
    assert(failingMetrics.getPartialReadInvalidations === 1L)

    // (b) End to end: a real active-streaming reduceByKey recovers from a one-time fetch failure.
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("streaming-shuffle-recompute")
      .set("spark.ui.enabled", "false")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
    sc = new SparkContext(conf)
    assert(sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager].isStreamingActive,
      "both flags must activate the streaming data path for the end-to-end recovery test")

    val numPartitions = 16
    val records = (1 to 1000).map(i => (i % numPartitions, i))
    val expected = records.groupBy(_._1).map { case (k, kvs) => (k, kvs.map(_._2).sum) }
      .toSeq.sortBy(_._1)

    // On the FIRST reduce-stage attempt (stageAttemptNumber == 0) every reduce task throws the
    // scheduler-recognized FetchFailedException, forcing the DAG scheduler to recompute the
    // upstream streaming map stage and retry the reduce stage; the retried attempt passes the
    // records through unchanged. This is the canonical Spark fetch-failure-recovery injection
    // (see TaskContextSuite). The local val keeps the closure free of the non-serializable suite.
    val parts = numPartitions
    val recovered = sc.parallelize(records, 8)
      .reduceByKey(_ + _, parts)
      .mapPartitions { iter =>
        if (TaskContext.get().stageAttemptNumber() == 0) {
          throw new FetchFailedException(null, 0, 0L, 0, 0,
            "injected one-time streaming fetch failure")
        }
        iter
      }
      .collect()
      .sortBy(_._1)
      .toSeq

    assert(recovered === expected,
      "after scheduler recomputation the active streaming shuffle must return the complete, " +
        "correct output (zero data loss)")
  }
}
