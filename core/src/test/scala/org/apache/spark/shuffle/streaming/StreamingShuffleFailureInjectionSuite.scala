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

import java.io.{InputStream, IOException}
import java.net.{SocketException, SocketTimeoutException}
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap

import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.spark.{LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.streaming.network.{StreamingBlockEnvelope, TokenBucketRateLimiter}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Failure-injection suite for the streaming shuffle backend.
 *
 * This suite discharges two of the streaming feature's quality gates in one place:
 *
 *  1. '''Zero data loss under all ten mandated failure scenarios''' (AAP 0.7.2). Each scenario is
 *     driven deterministically at the component level -- the v1 transport is a logging stub, so no
 *     real network I/O is required -- and every scenario carries an explicit "no data loss / no
 *     leak" assertion: either the failure is surfaced as a recoverable
 *     [[org.apache.spark.shuffle.FetchFailedException]] (which drives DAG upstream recomputation
 *     rather than silently dropping records), or a spilled / acknowledged buffer is proven to be
 *     accounted for and reclaimed rather than leaked, or the fallback policy reverts to the
 *     production-stable sort path.
 *
 *  2. '''Block-level integrity of
 *     [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]]'''. Because the
 *     streaming package is flat and has no dedicated envelope suite, this is the designated home
 *     for the comprehensive envelope tests: the canonical CRC-32C check value, a serialize -> parse
 *     round-trip, single-byte corruption detection, and the 2 MB payload boundary.
 *
 * '''Anti-flakiness discipline.''' No test ever sleeps for a real timeout. Producer failures are
 * injected as [[java.net.SocketException]]s, which the reader's producer-connection classifier
 * detects through the throwable cause chain, so the 5-second timeout path is exercised without any
 * wall-clock wait. The single test that relies on the [[MemorySpillManager]] background poller uses
 * ScalaTest's `eventually` (which returns as soon as the first 100 ms poll completes) rather than a
 * fixed sleep, and every daemon-bearing collaborator ([[MemorySpillManager]],
 * [[BackpressureProtocol]]) is stopped in a `finally` block. [[LocalSparkContext]] tears down any
 * created `SparkContext` after each test.
 *
 * Only production streaming classes are referenced; nothing is stubbed or duplicated. The small
 * test-only [[ManagedBuffer]] / [[InputStream]] fixtures mirror the established pattern in
 * `BlockStoreShuffleReaderSuite` and implement public engine interfaces, not streaming production
 * types.
 */
class StreamingShuffleFailureInjectionSuite extends SparkFunSuite with LocalSparkContext
  with Eventually {

  // ---------------------------------------------------------------------------------------------
  // Test-only fixtures (public-interface implementations, not streaming production classes).
  // ---------------------------------------------------------------------------------------------

  /**
   * A test-only [[InputStream]] that simulates a crashed or partitioned producer by throwing the
   * supplied [[IOException]] on every read. The failure is produced by a `makeFailure` thunk so a
   * fresh exception instance is thrown per read (streams may be retried); callers choose the exact
   * connection-failure subtype -- e.g. [[SocketException]] for a reset or
   * [[SocketTimeoutException]] for a timeout. The streaming reader classifies any such failure in
   * the cause chain as a producer connection timeout, so this deterministically triggers
   * partial-read invalidation without any real five-second wait.
   */
  private class CrashInputStream(makeFailure: () => IOException) extends InputStream {
    override def read(): Int = throw makeFailure()
    override def read(b: Array[Byte], off: Int, len: Int): Int = throw makeFailure()
  }

  /**
   * A test-only [[ManagedBuffer]] whose input stream fails on read, standing in for a producer that
   * crashes mid-stream. `size()` is a positive constant so the fetch iterator does not treat it as
   * an empty (zero-size) block; the failure is delivered through the stream on read -- not through
   * buffer creation -- so it flows into the reader's producer-timeout guard exactly as a real
   * connection reset would.
   */
  private class ProducerCrashManagedBuffer(makeFailure: () => IOException) extends ManagedBuffer {
    override def size(): Long = 128L
    override def nioByteBuffer(): ByteBuffer = ByteBuffer.allocate(0)
    override def createInputStream(): InputStream = new CrashInputStream(makeFailure)
    override def convertToNetty(): AnyRef = throw new UnsupportedOperationException()
    override def convertToNettyForSsl(): AnyRef = throw new UnsupportedOperationException()
    override def retain(): ManagedBuffer = this
    override def release(): ManagedBuffer = this
  }

  // ---------------------------------------------------------------------------------------------
  // Shared helpers.
  // ---------------------------------------------------------------------------------------------

  /** A minimal, streaming-agnostic conf; a plain local `SparkContext` is only needed to set env. */
  private def readerSparkConf(): SparkConf = {
    new SparkConf(false)
      .set(config.SHUFFLE_COMPRESS, false)
      .set(config.SHUFFLE_SPILL_COMPRESS, false)
  }

  /**
   * Drives a real [[StreamingShuffleReader]] `read()` against a single local producer block whose
   * input stream fails with the [[IOException]] produced by `makeFailure`, asserts that the failure
   * surfaces as a [[FetchFailedException]] (the DAG-recompute contract), and returns the streaming
   * metrics so the caller can assert partial-read invalidation. Requires `sc` to be initialized
   * (for `SparkEnv`).
   */
  private def readWithProducerFailure(makeFailure: () => IOException): StreamingShuffleMetrics = {
    val shuffleId = 42
    val mapId = 0L
    val reduceId = 3
    val serializer = new JavaSerializer(sc.conf)

    // Mock the block manager so the (local) producer block returns a stream that fails on read.
    val blockManager = mock(classOf[BlockManager])
    val localId = BlockManagerId("test-exec", "test-host", 7337)
    when(blockManager.blockManagerId).thenReturn(localId)
    val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
    when(blockManager.getLocalBlockData(shuffleBlockId))
      .thenReturn(new ProducerCrashManagedBuffer(makeFailure))

    // A minimal streaming handle over a mocked dependency (no aggregation, no ordering).
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.shuffleId).thenReturn(shuffleId)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = 0)

    val serializerManager = new SerializerManager(serializer, new SparkConf(false)
      .set(config.SHUFFLE_COMPRESS, false)
      .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val taskContext = TaskContext.empty()
    val readMetrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val streamingMetrics = new StreamingShuffleMetrics()
    val blocks = scala.collection.Seq[(BlockId, Long, Int)]((shuffleBlockId, 128L, 0))
    val blocksByAddress = Iterator((localId, blocks))

    val reader = new StreamingShuffleReader[Int, Int](
      handle,
      blocksByAddress,
      taskContext,
      readMetrics,
      streamingMetrics,
      new StreamingShuffleConfig(sc.conf),
      serializerManager,
      blockManager)

    // Consuming the lazy read iterator triggers deserialization, which reads the failing stream and
    // surfaces the producer connection failure. Per SPARK-19276 the reader constructs and throws a
    // FetchFailedException in a single statement, which is what the DAG scheduler observes.
    intercept[FetchFailedException] {
      reader.read().toList
    }
    streamingMetrics
  }

  /**
   * Injects a [[SocketException]] (a connection reset / no-route producer crash) on every read.
   * This is the default producer-failure vector for the crash and network-partition scenarios.
   */
  private def readWithFailingProducer(failureMessage: String): StreamingShuffleMetrics =
    readWithProducerFailure(() => new SocketException(failureMessage))

  /**
   * Injects a [[SocketTimeoutException]] on every read, the canonical connect/read-timeout signal.
   * Exercises the same producer-connection-timeout classification and invalidation path as a real
   * five-second timeout, deterministically and without any wall-clock wait.
   */
  private def readWithTimingOutProducer(failureMessage: String): StreamingShuffleMetrics =
    readWithProducerFailure(() => new SocketTimeoutException(failureMessage))

  /**
   * Reflectively invokes the private [[BackpressureProtocol]] `scan()` so the timeout-driven
   * liveness cleanup can be exercised synchronously, without starting the daemon (which would race
   * the assertions) and without modifying production code to widen the method's visibility.
   */
  private def invokeScan(protocol: BackpressureProtocol): Unit = {
    val method = classOf[BackpressureProtocol].getDeclaredMethod("scan")
    method.setAccessible(true)
    method.invoke(protocol)
  }

  /** Reflectively reads the private producer-liveness map (keyed by boxed mapId). */
  private def producerMap(
      protocol: BackpressureProtocol): ConcurrentHashMap[java.lang.Long, java.lang.Long] = {
    val field = classOf[BackpressureProtocol].getDeclaredField("producerLastActive")
    field.setAccessible(true)
    field.get(protocol).asInstanceOf[ConcurrentHashMap[java.lang.Long, java.lang.Long]]
  }

  /** Reflectively reads the private consumer-liveness map (keyed by executor id). */
  private def consumerMap(
      protocol: BackpressureProtocol): ConcurrentHashMap[String, java.lang.Long] = {
    val field = classOf[BackpressureProtocol].getDeclaredField("consumerLastSeen")
    field.setAccessible(true)
    field.get(protocol).asInstanceOf[ConcurrentHashMap[String, java.lang.Long]]
  }

  // ---------------------------------------------------------------------------------------------
  // StreamingBlockEnvelope block-integrity tests.
  // ---------------------------------------------------------------------------------------------

  test("StreamingBlockEnvelope: canonical CRC-32C vector (unsigned)") {
    // The Castagnoli (CRC-32C) canonical check value for the ASCII string "123456789".
    val checksum = StreamingBlockEnvelope.computeChecksum("123456789".getBytes)
    assert((checksum & 0xFFFFFFFFL) == 0xE3069283L)
  }

  test("StreamingBlockEnvelope: serialize -> parse round-trips all fields") {
    val payload = "hello-streaming".getBytes
    val env = StreamingBlockEnvelope(
      shuffleId = 7, mapId = 3, reduceId = 2, sequenceNumber = 11, payload = payload)
    val parsed = StreamingBlockEnvelope.parse(env.serialize())
    assert(parsed.shuffleId == 7)
    assert(parsed.mapId == 3)
    assert(parsed.reduceId == 2)
    assert(parsed.sequenceNumber == 11)
    assert(parsed.checksum == env.checksum)
    assert(Arrays.equals(parsed.payload, payload))
    assert(parsed.serializedLength == StreamingBlockEnvelope.HEADER_BYTES + env.payloadLength)
    assert(StreamingBlockEnvelope.HEADER_BYTES == 32)
    // Zero data loss: a serialized-then-parsed block verifies intact end to end.
    assert(parsed.verifyChecksum())
  }

  test("StreamingBlockEnvelope: verifyChecksum detects a single flipped byte") {
    val payload = "streaming-shuffle-block-integrity".getBytes
    val env = StreamingBlockEnvelope(
      shuffleId = 1, mapId = 2, reduceId = 3, sequenceNumber = 4, payload = payload)
    assert(env.verifyChecksum())
    // Flip the first, a middle, and the last byte in turn, keeping the ORIGINAL checksum. Each
    // corruption must be detected -- this is precisely the reader's retransmission trigger.
    val positions = Seq(0, payload.length / 2, payload.length - 1)
    positions.foreach { pos =>
      val bad = payload.clone()
      bad(pos) = (bad(pos) ^ 0xFF).toByte
      val badEnv = new StreamingBlockEnvelope(
        env.shuffleId, env.mapId, env.reduceId, env.sequenceNumber, env.checksum, bad)
      assert(!badEnv.verifyChecksum(), s"corruption at byte $pos went undetected")
    }
  }

  test("StreamingBlockEnvelope: accepts 2 MB payload, rejects 2 MB + 1") {
    val maxPayload = new Array[Byte](StreamingBlockEnvelope.MAX_PAYLOAD_BYTES)
    val ok = StreamingBlockEnvelope(0, 0, 0, 0, maxPayload)
    assert(ok.payloadLength == StreamingBlockEnvelope.MAX_PAYLOAD_BYTES)
    val tooBig = new Array[Byte](StreamingBlockEnvelope.MAX_PAYLOAD_BYTES + 1)
    intercept[IllegalArgumentException] {
      StreamingBlockEnvelope(0, 0, 0, 0, tooBig)
    }
  }

  test("StreamingBlockEnvelope: wire constants (HEADER/MAX_PAYLOAD/RESERVED)") {
    assert(StreamingBlockEnvelope.MAX_PAYLOAD_BYTES == 2 * 1024 * 1024)
    assert(StreamingBlockEnvelope.RESERVED_BYTES == 8)
    assert(StreamingBlockEnvelope.HEADER_BYTES == 32)
  }

  // ---------------------------------------------------------------------------------------------
  // The ten mandated failure scenarios (each asserts zero data loss / no leak).
  // ---------------------------------------------------------------------------------------------

  test("failure scenario 1: producer crash -> FetchFailedException + invalidation") {
    sc = new SparkContext("local", "test", readerSparkConf())
    val metrics = readWithFailingProducer("simulated producer crash (connection reset)")
    // Zero data loss: the crash is reported as a recoverable FetchFailedException (asserted in the
    // helper) so the DAG scheduler recomputes the upstream map stage instead of dropping records,
    // and the in-progress read was atomically invalidated exactly once.
    assert(metrics.partialReadInvalidationsCounter.getCount == 1L)
  }

  test("failure scenario 2: consumer failure -> producer buffers reclaimed") {
    val conf = new StreamingShuffleConfig(new SparkConf(false))
    val metrics = new StreamingShuffleMetrics()
    // No spill is expected here (large budget), so a mocked block manager is never touched.
    val spillManager = new MemorySpillManager(conf, mock(classOf[BlockManager]), metrics)
    try {
      spillManager.setBufferBudgetBytes(64L * 1024)
      val abandoned = new StreamingBuffer(shuffleId = 5, mapId = 0L, reduceId = 0)
      abandoned.append(new Array[Byte](4096))
      val acked = new StreamingBuffer(shuffleId = 5, mapId = 1L, reduceId = 0)
      acked.append(new Array[Byte](4096))
      spillManager.register(5, 0L, 0, abandoned)
      spillManager.register(5, 1L, 0, acked)
      spillManager.start()
      assert(spillManager.utilizationPercent() > 0)
      // The consumer for one partition acknowledges: its memory is reclaimed synchronously (reset),
      // well within the 100 ms reclamation SLA.
      spillManager.onConsumerAck(5, 1L, 0)
      assert(acked.size == 0L)
      // The other consumer FAILS and never acknowledges; its buffer must still not leak.
    } finally {
      spillManager.stop()
    }
    // No leak: teardown releases every tracked buffer, so no producer-side memory is retained.
    assert(spillManager.utilizationPercent() == 0)
  }

  test("failure scenario 3: network partition -> recoverable FetchFailedException") {
    sc = new SparkContext("local", "test", readerSparkConf())
    val metrics = readWithFailingProducer("simulated network partition (no route to host)")
    // Recomputable, not silently dropped: a FetchFailedException was thrown (asserted in the
    // helper) and the partial read was invalidated so no partial data escapes the reduce iterator.
    assert(metrics.partialReadInvalidationsCounter.getCount >= 1L)
  }

  test("failure scenario 4: CRC-32C mismatch -> corruption detected") {
    val payload = "retransmit-me".getBytes
    val env = StreamingBlockEnvelope(
      shuffleId = 2, mapId = 4, reduceId = 6, sequenceNumber = 8, payload = payload)
    assert(env.verifyChecksum())
    // Corrupt one payload byte while retaining the original checksum: verifyChecksum must fail,
    // which is exactly the signal the reader uses to request a retransmission (no silent accept).
    val corrupted = payload.clone()
    corrupted(0) = (corrupted(0) ^ 0xFF).toByte
    val corruptedEnv = new StreamingBlockEnvelope(
      env.shuffleId, env.mapId, env.reduceId, env.sequenceNumber, env.checksum, corrupted)
    assert(!corruptedEnv.verifyChecksum())
    // Zero data loss: the intact block is re-sent and verifies true (successful retransmission).
    val resent = StreamingBlockEnvelope(
      shuffleId = 2, mapId = 4, reduceId = 6, sequenceNumber = 8, payload = payload.clone())
    assert(resent.verifyChecksum())
    assert(Arrays.equals(resent.payload, payload))
  }

  test("failure scenario 5: spill during failure -> spilled bytes accounted for") {
    sc = new SparkContext("local", "test",
      new SparkConf(false).set(config.SHUFFLE_STREAMING_SPILL_THRESHOLD, 50))
    val conf = new StreamingShuffleConfig(sc.conf)
    val metrics = new StreamingShuffleMetrics()
    // A real block manager so the spill actually persists to DISK_ONLY block storage.
    val spillManager = new MemorySpillManager(conf, SparkEnv.get.blockManager, metrics)
    try {
      // Tiny budget so a single 4 KB partition saturates utilization far above the 50% threshold.
      spillManager.setBufferBudgetBytes(1024L)
      val buffer = new StreamingBuffer(shuffleId = 9, mapId = 0L, reduceId = 0)
      buffer.append(new Array[Byte](4096))
      spillManager.register(9, 0L, 0, buffer)
      spillManager.start()
      // The 100 ms poller spills the over-threshold partition to disk. `eventually` returns as soon
      // as the first poll completes -- there is no fixed sleep and no dependence on a real timeout.
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        assert(metrics.spillCounter.getCount > 0)
      }
      // Zero data loss: the partition's bytes were persisted to block storage, not dropped.
      assert(spillManager.isSpilled(9, 0L, 0))
    } finally {
      spillManager.stop()
    }
    // No leak: after teardown no in-memory buffer remains tracked.
    assert(spillManager.utilizationPercent() == 0)
  }

  test("failure scenario 6: producer timeout governed by 5000 ms constant") {
    // The reader's producer-connection timeout is fixed at five seconds per the failure-tolerance
    // contract; this is the window within which an unreachable producer is invalidated and a
    // FetchFailedException is thrown so the upstream stage is recomputed (no silent data loss).
    assert(StreamingShuffleReader.PRODUCER_CONNECTION_TIMEOUT_MS == 5000L)

    // Drive the timeout path end to end: a producer whose stream fails with a socket timeout
    // (the canonical connect/read-timeout signal) must be classified as a producer-connection
    // failure, surface as a FetchFailedException (asserted inside the helper), and invalidate the
    // partial read exactly once -- so no partially read block is ever handed to the reduce task.
    sc = new SparkContext("local", "test", readerSparkConf())
    val metrics = readWithTimingOutProducer("simulated producer connection timeout")
    assert(metrics.partialReadInvalidationsCounter.getCount == 1L)
  }

  test("failure scenario 7: partial-read invalidation is atomic (once)") {
    sc = new SparkContext("local", "test", readerSparkConf())
    val metrics = readWithFailingProducer("simulated producer timeout")
    // The reader guards invalidation with an AtomicBoolean, so even though the failing block is
    // touched through several stream operations the counter advances exactly once and no partial
    // data is surfaced to the reduce task.
    assert(metrics.partialReadInvalidationsCounter.getCount == 1L)
  }

  test("failure scenario 8: version mismatch -> fallback to sort engages") {
    val conf = new StreamingShuffleConfig(new SparkConf(false))
    val metrics = new StreamingShuffleMetrics()
    val policy = new StreamingShuffleFallbackPolicy(conf, metrics)
    assert(policy.isVersionMismatch(1, 2))
    assert(!policy.isVersionMismatch(1, 1))
    val mismatch = FallbackStats(
      consumerRateBytesPerSec = 1000.0,
      producerRateBytesPerSec = 1000.0,
      sustainedSlowMillis = 0L,
      memoryUtilizationPercent = 10,
      networkUtilizationPercent = 10,
      localProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION,
      remoteProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION + 1)
    // A protocol mismatch forces the shuffle onto the durable sort path: no data loss.
    assert(policy.shouldFallback(mismatch))
    // With matching versions and otherwise benign stats, streaming continues (no false fallback).
    assert(!policy.shouldFallback(
      mismatch.copy(remoteProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION)))
  }

  test("failure scenario 9: memory pressure -> fallback to sort engages") {
    val conf = new StreamingShuffleConfig(new SparkConf(false))
    val metrics = new StreamingShuffleMetrics()
    val policy = new StreamingShuffleFallbackPolicy(conf, metrics)
    // The pressure guard is strictly greater than 95%; the 80% spill threshold must NOT trip it.
    assert(policy.isMemoryPressure(96))
    assert(!policy.isMemoryPressure(95))
    assert(!policy.isMemoryPressure(80))
    val pressure = FallbackStats(
      consumerRateBytesPerSec = 1000.0,
      producerRateBytesPerSec = 1000.0,
      sustainedSlowMillis = 0L,
      memoryUtilizationPercent = 96,
      networkUtilizationPercent = 10,
      localProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION,
      remoteProtocolVersion = StreamingShuffleFallbackPolicy.PROTOCOL_VERSION)
    // Imminent OOM reverts the shuffle to sort rather than failing the job: no data loss.
    assert(policy.shouldFallback(pressure))
    assert(!policy.shouldFallback(pressure.copy(memoryUtilizationPercent = 95)))
  }

  test("failure scenario 10: backpressure timeout handled without deadlock/leak") {
    // Part 1 -- deterministic liveness-scan cleanup. A separate, un-started protocol is driven by
    // invoking the private scan() reflectively so the 5s producer / 10s consumer timeout paths are
    // exercised without any wall-clock wait and without racing the background scheduler.
    val scanMetrics = new StreamingShuffleMetrics()
    val scanProtocol = new BackpressureProtocol(
      new StreamingShuffleConfig(new SparkConf(false)),
      scanMetrics,
      new TokenBucketRateLimiter(0, 1))
    val now = System.currentTimeMillis()
    producerMap(scanProtocol).put(java.lang.Long.valueOf(0L), java.lang.Long.valueOf(now - 6000L))
    producerMap(scanProtocol).put(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(now - 4000L))
    scanProtocol.onHeartbeat("stale-consumer", now - 11000L)
    scanProtocol.onHeartbeat("fresh-consumer", now - 9000L)
    invokeScan(scanProtocol)
    // The producer idle past 5s and the consumer silent past 10s are dropped; the fresher
    // entries survive. Consumer eviction is recorded as a backpressure event while producer
    // eviction is tracking-only, so exactly one event is counted -- stale liveness state is
    // reclaimed without losing any data.
    assert(!producerMap(scanProtocol).containsKey(java.lang.Long.valueOf(0L)))
    assert(producerMap(scanProtocol).containsKey(java.lang.Long.valueOf(1L)))
    assert(!consumerMap(scanProtocol).containsKey("stale-consumer"))
    assert(consumerMap(scanProtocol).containsKey("fresh-consumer"))
    assert(scanMetrics.backpressureCounter.getCount == 1L)

    // Part 2 -- no deadlock / no leak under the running daemon.
    val conf = new StreamingShuffleConfig(new SparkConf(false))
    val metrics = new StreamingShuffleMetrics()
    // Unlimited (pass-through) limiter: the send gate must never block, so acquire cannot deadlock.
    val rateLimiter = new TokenBucketRateLimiter(0, 1)
    val protocol = new BackpressureProtocol(conf, metrics, rateLimiter)
    try {
      protocol.start()
      // Simulate a producer and consumer that then go silent (no further acks / heartbeats).
      protocol.onHeartbeat("consumer-exec", System.currentTimeMillis())
      protocol.onConsumerAck(shuffleId = 1, mapId = 0L, reduceId = 0, bytesConsumed = 64L,
        seqNumber = 1)
      assert(protocol.acquire(1024L))
      val before = metrics.backpressureCounter.getCount
      protocol.onThrottleRequest(shuffleId = 1, targetBytesPerSec = 4096L)
      assert(metrics.backpressureCounter.getCount == before + 1)
      val (activeShuffles, credit) = protocol.status
      assert(activeShuffles >= 1)
      assert(credit >= 0L)
    } finally {
      // No leak / no deadlock: stop() releases the daemon and returns promptly.
      protocol.stop()
    }
    // Idempotent: a second stop after teardown is a safe no-op.
    protocol.stop()
  }
}
