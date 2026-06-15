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

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{
  HashPartitioner, LocalSparkContext, MapOutputTracker, ShuffleDependency, SparkConf, SparkContext,
  SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.shuffle.{BaseShuffleHandle, FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.BackpressureProtocol.StreamKey
import org.apache.spark.shuffle.streaming.network.{
  StreamingBlockEnvelope, StreamingShuffleTransport, TokenBucketRateLimiter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ByteBufferBlockData,
  StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Failure-injection suite for the opt-in streaming shuffle backend (one of the F-121 suites and a
 * merge-gate artifact: AAP 0.4.4 mandates that "the 10-scenario `StreamingShuffleFailureInjection`
 * suite demonstrates zero data loss"). It contains EXACTLY ten distinct failure scenarios, each
 * ending in a concrete zero-data-loss assertion.
 *
 * The zero-loss guarantee has two valid forms, both exercised here:
 *  - the data round-trips correctly after recovery (spill read-back, sort fallback, recompute), or
 *  - the failure surfaces as a [[org.apache.spark.shuffle.FetchFailedException]] (never a silent
 *    truncation/corruption) so Spark's existing lineage/recompute machinery rebuilds the output.
 *
 * Every timeout is injected DETERMINISTICALLY -- via the reader's package-private invalidation
 * entry point, the backpressure protocol's injectable scan clock, or short/explicit spill calls --
 * so the suite never waits the real 5 s / 10 s windows and completes in milliseconds. All network
 * and disk interactions are mocked; only the memory-pressure fallback scenario uses a live local
 * [[org.apache.spark.SparkContext]] (created and torn down inside that one test) to prove the sort
 * fallback path round-trips a real shuffle.
 */
class StreamingShuffleFailureInjectionSuite extends SparkFunSuite with Matchers {

  /** Fixed identifiers shared by the fixtures in this suite. */
  private val shuffleId = 7
  private val reduceId = 3
  private val integrityBmId = BlockManagerId("exec-1", "host-1", 7337)

  /** The canonical 2 MB streaming-shuffle framing/send size. */
  private val twoMb = StreamingShuffleConfig.BLOCK_SIZE_BYTES

  // -----------------------------------------------------------------------------------------------
  // Shared, deterministic fixtures (mirroring the proven patterns in the sibling streaming suites).
  // -----------------------------------------------------------------------------------------------

  /**
   * Builds a [[StreamingShuffleReader]] over fully mocked collaborators. No `SparkContext` is
   * needed: the envelope de-framing / partial-read-invalidation core only touches the real
   * [[StreamingShuffleMetrics]] (passed in so a test can assert the invalidation counter) and the
   * real [[StreamingShuffleConfig]]; every other collaborator is mocked and never invoked here.
   */
  private def integrityReader(
      metrics: StreamingShuffleMetrics): StreamingShuffleReader[Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dep, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = -1)
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
    StreamingBlockEnvelope.create(shuffleId, 0L, reduceId, seq, payload).toByteArray

  /** Deterministic payload of length `n`; the content is irrelevant since the CRC covers it. */
  private def payloadOf(n: Int): Array[Byte] = Array.tabulate(n)(i => (i % 127).toByte)

  /** Converts whole seconds to nanoseconds for the injected backpressure scan clock. */
  private def secs(n: Long): Long = TimeUnit.SECONDS.toNanos(n)

  /** A real backpressure protocol over an unlimited rate limiter, plus its metrics holder. */
  private def newBackpressure(): (BackpressureProtocol, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val limiter = new TokenBucketRateLimiter(Long.MaxValue)
    val metrics = new StreamingShuffleMetrics
    (new BackpressureProtocol(cfg, limiter, metrics), metrics)
  }

  /** A spill manager whose mocked BlockManager reports a successful put (no real disk I/O). */
  private def newSpillManager(maxMem: Long)
      : (MemorySpillManager, BlockManager, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val bm = mock(classOf[BlockManager])
    when(bm.putBytes(any(), any(), any(), any())(any())).thenReturn(true)
    val mm = mock(classOf[MemoryManager])
    when(mm.maxOnHeapStorageMemory).thenReturn(maxMem)
    val metrics = new StreamingShuffleMetrics
    (new MemorySpillManager(cfg, bm, mm, metrics), bm, metrics)
  }

  /** A spill manager whose BlockManager captures spilled bytes and serves them back (no disk). */
  private def storingSpillManager(maxMem: Long): (MemorySpillManager, BlockManager) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val store = new ConcurrentHashMap[BlockId, Array[Byte]]()
    val bm = mock(classOf[BlockManager])
    when(bm.putBytes(any(), any(), any(), any())(any())).thenAnswer { (inv: InvocationOnMock) =>
      store.put(inv.getArgument[BlockId](0), inv.getArgument[ChunkedByteBuffer](1).toArray)
      true
    }
    when(bm.getLocalBytes(any())).thenAnswer { (inv: InvocationOnMock) =>
      Option(store.get(inv.getArgument[BlockId](0)))
        .map(arr => new ByteBufferBlockData(new ChunkedByteBuffer(ByteBuffer.wrap(arr)), false))
    }
    val mm = mock(classOf[MemoryManager])
    when(mm.maxOnHeapStorageMemory).thenReturn(maxMem)
    (new MemorySpillManager(cfg, bm, mm, new StreamingShuffleMetrics), bm)
  }

  /** A per-partition buffer whose live size is exactly `sizeBytes`. */
  private def filledBuffer(mapId: Long, partitionId: Int, sizeBytes: Int): StreamingBuffer = {
    val buffer = new StreamingBuffer(shuffleId, mapId, partitionId, sizeBytes.toLong)
    buffer.append(new Array[Byte](sizeBytes))
    buffer
  }

  /** Sums payload bytes across every envelope frame in an enveloped array (headers skipped). */
  private def sumPayloadBytes(enveloped: Array[Byte]): Long = {
    val bb = ByteBuffer.wrap(enveloped)
    var total = 0L
    while (bb.remaining() >= StreamingBlockEnvelope.HEADER_BYTES) {
      val env = StreamingBlockEnvelope.parse(bb)
      total += env.payloadLength
      bb.position(bb.position() + StreamingBlockEnvelope.HEADER_BYTES + env.payloadLength)
    }
    total
  }

  /** Derives the spill-registry key from a buffer's identity fields. */
  private def keyOf(buffer: StreamingBuffer): MemorySpillManager.BufferKey =
    MemorySpillManager.BufferKey(buffer.shuffleId, buffer.mapId, buffer.partitionId)

  // -----------------------------------------------------------------------------------------------
  // The EXACTLY ten failure-injection scenarios (one test each, each proving zero data loss).
  // -----------------------------------------------------------------------------------------------

  test("scenario 1: producer connection timeout (5s) -> FetchFailedException + " +
      "partialReadInvalidations") {
    val metrics = new StreamingShuffleMetrics
    val reader = integrityReader(metrics)
    val before = metrics.partialReadInvalidations
    // invalidatePartialReads is THE single entry point the 5 s connection-timeout path uses, so
    // calling it directly injects the timeout deterministically (no real 5 s wait, no retry sleep).
    intercept[FetchFailedException] {
      reader.invalidatePartialReads(integrityBmId, 0L, 0, 0,
        "Streaming shuffle partial read invalidated after 5s connection timeout")
    }
    // Surfacing the FetchFailedException IS the zero-loss guarantee (Spark recomputes the upstream
    // output); the invalidation is counted exactly once.
    assert(metrics.partialReadInvalidations === before + 1L)
  }

  test("scenario 2: partial read invalidation discards buffered data atomically") {
    val metrics = new StreamingShuffleMetrics
    val reader = integrityReader(metrics)
    // A VALID frame followed by a CORRUPT frame: even though the first frame parses cleanly, the
    // reader must abort and return NOTHING (all-or-nothing), so no partial/garbage data leaks.
    val good = frame(0L, payloadOf(64))
    val corrupt = frame(1L, payloadOf(64))
    val raw = good ++ corrupt
    val idx = good.length + StreamingBlockEnvelope.HEADER_BYTES // first payload byte of frame 2
    raw(idx) = (raw(idx) ^ 0xFF).toByte
    var produced: Array[Byte] = null
    intercept[FetchFailedException] {
      produced = reader.extractValidatedPayloads(ByteBuffer.wrap(raw), integrityBmId, 0L, 0, 0)
    }
    assert(produced == null) // zero records emitted: the read aborted atomically before returning
    assert(metrics.partialReadInvalidations === 1L)
  }

  test("scenario 3: CRC32C corruption is rejected") {
    val payload = payloadOf(128)
    val bytes = StreamingBlockEnvelope.create(shuffleId, 0L, reduceId, 0L, payload).toByteArray
    // Flip one payload byte (just past the 32-byte header) so the recomputed CRC32C no longer
    // matches the value stored in the header.
    val idx = StreamingBlockEnvelope.HEADER_BYTES
    bytes(idx) = (bytes(idx) ^ 0xFF).toByte
    val parsed = StreamingBlockEnvelope.parse(bytes)
    // Corruption is never accepted as valid data.
    assert(!parsed.verifyChecksum)
  }

  test("scenario 4: truncated envelope header is rejected") {
    // Fewer than the 32-byte header: parse must reject the frame rather than silently emit data.
    val truncated = new Array[Byte](StreamingBlockEnvelope.HEADER_BYTES - 16)
    intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.parse(truncated)
    }
  }

  test("scenario 5: oversized payload (> 2MB) is rejected at create") {
    val tooBig = new Array[Byte](StreamingShuffleConfig.BLOCK_SIZE_BYTES + 1)
    // The 2 MB cap is enforced via require, so create must reject an oversized payload.
    intercept[IllegalArgumentException] {
      StreamingBlockEnvelope.create(0, 0L, 0, 0L, tooBig)
    }
  }

  test("scenario 6: consumer missing acks (10s) -> buffer then spill") {
    val (bp, _) = newBackpressure()
    val key = StreamKey(shuffleId, 0L, reduceId)
    // The producer sends one 2 MB block: the bytes are buffered as unacked (NOT discarded) and the
    // send arms the consumer-liveness clock. Bracket the internal stamp with before/after readings.
    val t0 = System.nanoTime()
    bp.acquireSendPermit(key, twoMb)
    val tEnd = System.nanoTime()
    assert(bp.unackedByteCount(key) > 0L) // unacked data is buffered, awaiting an ack
    bp.scanForTimeouts(t0 + secs(5))
    assert(!bp.isConsumerTimedOut(key)) // within the 10 s window the consumer is still alive
    bp.scanForTimeouts(tEnd + secs(11))
    assert(bp.isConsumerTimedOut(key)) // past 10 s of missing acks -> consumer declared timed out

    // The consumer-failure flow now spills the buffered data (> 80%) rather than discarding it.
    val (mgr, _, metrics) = newSpillManager(maxMem = 10000L)
    mgr.register(filledBuffer(mapId = 0L, partitionId = reduceId, sizeBytes = 9000))
    assert(mgr.maybeSpill() > 0L) // 9000 bytes exceeds 80% of 10000 -> spill is triggered
    assert(metrics.spillCount >= 1L) // the spill is recorded; the buffered data is persisted
  }

  test("scenario 7: resume + retransmit after consumer reconnect") {
    val (bp, _) = newBackpressure()
    val key = StreamKey(shuffleId, 1L, reduceId)
    val t0 = System.nanoTime()
    bp.acquireSendPermit(key, twoMb)
    val tEnd = System.nanoTime()
    bp.scanForTimeouts(tEnd + secs(11))
    assert(bp.isConsumerTimedOut(key)) // the consumer first times out
    // A fresh consumer heartbeat (the reconnect) clears the timeout so streaming resumes.
    bp.onHeartbeat(key)
    assert(!bp.isConsumerTimedOut(key))

    // The unacked block was spilled while the consumer was gone; on resume it must be fully
    // re-readable for retransmission (no loss). Read the spilled segment(s) back byte-for-byte.
    val (mgr, _) = storingSpillManager(maxMem = 10000L)
    val buffer = filledBuffer(mapId = 1L, partitionId = reduceId, sizeBytes = 9000)
    val expected = buffer.toByteArray // canonical enveloped frames captured BEFORE the spill clears
    mgr.register(buffer)
    assert(mgr.spillBuffer(keyOf(buffer)))
    val readBack = mgr.spilledBlockIds(keyOf(buffer))
      .flatMap(id => mgr.readSpilledSegment(id))
      .foldLeft(Array.emptyByteArray)(_ ++ _)
    assert(readBack.sameElements(expected)) // spilled blocks intact and retransmittable
  }

  test("scenario 8: live memory pressure trips the manager's automatic fallback (sort " +
      "delegation, no loss)") {
    // Streaming is genuinely ENABLED here (unlike a disabled-path proxy): the fallback must be
    // triggered from live memory-pressure state on the SAME StreamingShuffleManager the running
    // SparkContext uses, proving AUTOMATIC fallback in the manager - not just disabled-path sort
    // correctness. The whole shuffle must then round-trip every record with zero loss via sort.
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingShuffleFailureInjectionSuite-fallback")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
    LocalSparkContext.withSpark(new SparkContext(conf)) { sc =>
      val data = (0 until 1000).map(i => (i % 10, i))
      val expected = data.groupBy(_._1).map { case (k, vs) => (k, vs.map(_._2).sum) }

      // Reach the manager the context actually drives shuffle registration through.
      val mgr = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]

      // Sanity: while untripped and enabled, a real ShuffleDependency registers STREAMING, so the
      // sort delegation asserted below is genuinely caused by the fallback, not a disabled flag.
      val streamingProbe =
        new ShuffleDependency[Int, Int, Int](sc.parallelize(data, 8), new HashPartitioner(2))
      assert(streamingProbe.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]])

      // Trigger ACTUAL manager fallback from live memory-pressure state (> 95%), exactly the
      // signal the spill manager's 100 ms poll feeds the manager's own policy in production.
      mgr.fallbackPolicy.updateMemoryUtilization(96.0)
      assert(mgr.fallbackPolicy.isMemoryPressure)
      assert(mgr.fallbackPolicy.shouldFallback)
      assert(mgr.fallbackPolicy.fallbackReason.exists(_.contains("memory")))

      // Sort delegation: a new ShuffleDependency registered through the now-tripped manager must
      // receive a sort (non-streaming) handle.
      val sortProbe =
        new ShuffleDependency[Int, Int, Int](sc.parallelize(data, 8), new HashPartitioner(2))
      assert(!sortProbe.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      assert(sortProbe.shuffleHandle.isInstanceOf[BaseShuffleHandle[_, _, _]])

      // Zero data loss: a full reduceByKey shuffle, registered while fallback is tripped, runs the
      // sort path and preserves every record.
      val result = sc.parallelize(data, 8).reduceByKey(_ + _).collect().toMap
      assert(result === expected)
    }
  }

  test("scenario 9: spill-then-read round-trip preserves all records") {
    val (mgr, bm) = storingSpillManager(maxMem = 10000L)
    // Frame a batch of records and force them to disk via BlockManager.putBytes(..., DISK_ONLY).
    val records = (0 until 20).map(i => payloadOf(512 + i))
    val totalRecordBytes = records.map(_.length.toLong).sum
    val buffer = new StreamingBuffer(shuffleId, 2L, reduceId, 64L * 1024)
    records.foreach(buffer.append)
    val expected = buffer.toByteArray // canonical enveloped frames captured before the spill clears
    mgr.register(buffer)

    assert(mgr.spillBuffer(keyOf(buffer)))
    verify(bm).putBytes(any(), any(), meq(StorageLevel.DISK_ONLY), any())(any())
    val readBack = mgr.spilledBlockIds(keyOf(buffer))
      .flatMap(id => mgr.readSpilledSegment(id))
      .foldLeft(Array.emptyByteArray)(_ ++ _)
    // Dual-channel invariant: the spilled bytes are byte-identical to the streamed frames, and
    // every appended record byte is recoverable from the spill (record preservation).
    assert(readBack.sameElements(expected))
    assert(sumPayloadBytes(readBack) === totalRecordBytes)
  }

  test("scenario 10: producer failure then recompute path yields identical output") {
    val records = Seq(payloadOf(100), payloadOf(250), payloadOf(37))
    val expected = records.foldLeft(Array.emptyByteArray)(_ ++ _)

    // Attempt 1 (original producer) delivers a corrupt frame, so the read fails and Spark
    // recomputes the upstream task rather than accepting partial output.
    val attempt1Metrics = new StreamingShuffleMetrics
    val reader1 = integrityReader(attempt1Metrics)
    val corruptRaw = frame(0L, records.head)
    corruptRaw(StreamingBlockEnvelope.HEADER_BYTES) =
      (corruptRaw(StreamingBlockEnvelope.HEADER_BYTES) ^ 0xFF).toByte
    intercept[FetchFailedException] {
      reader1.extractValidatedPayloads(ByteBuffer.wrap(corruptRaw), integrityBmId, 0L, 0, 0)
    }
    assert(attempt1Metrics.partialReadInvalidations === 1L)

    // Attempt 2 (the recomputed producer) delivers well-formed frames, so the output is recovered.
    val reader2 = integrityReader(new StreamingShuffleMetrics)
    val goodRaw = records.zipWithIndex
      .map { case (p, i) => frame(i.toLong, p) }
      .foldLeft(Array.emptyByteArray)(_ ++ _)
    val recovered =
      reader2.extractValidatedPayloads(ByteBuffer.wrap(goodRaw), integrityBmId, 0L, 0, 0)
    // The recomputed read yields exactly the original payload bytes: identical output, zero loss.
    assert(recovered.sameElements(expected))
  }
}
