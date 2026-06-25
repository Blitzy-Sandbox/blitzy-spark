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
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.ArgumentMatchers.{any, anyInt, eq => meq}
import org.mockito.Mockito.{doAnswer, mock, never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.PrivateMethodTester

import org.apache.spark.{Aggregator, MapOutputTracker, SharedSparkContext, ShuffleDependency,
  SparkFunSuite, TaskContext}
import org.apache.spark.memory.{MemoryManager, MemoryTestingUtils}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Unit tests for [[StreamingShuffleReader]] (streaming shuffle feature F-104), the consumer-side
 * reader that resolves producer (map output) locations through [[MapOutputTracker]], fetches each
 * in-progress streaming block through the executor's [[BlockTransferService]], validates the
 * per-frame CRC32C, and acknowledges every validated block so the producer can reclaim its
 * buffer.
 *
 * '''What these tests guard (the CP2 review findings).'''
 *   - '''C3 - bounded producer timeout.''' The reader must enforce the 5 s producer connection
 *     timeout: a hung fetch must invalidate the partial read and throw [[FetchFailedException]]
 *     rather than block forever. The "enforces the producer timeout" test stubs a transport that
 *     never answers the listener and asserts the read fails quickly (well under the wall-clock
 *     bound) while incrementing `partialReadInvalidations`. Before the fix the reader awaited
 *     `Duration.Inf`, so this test would hang indefinitely.
 *   - '''M7 - bounded decode.''' The reader must reject an oversized fetched buffer before
 *     touching it and reject an out-of-range declared frame payload length before allocating the
 *     payload array. Two tests cover this: a direct (`PrivateMethodTester`) invocation of
 *     `decodeFramesFromBuffer` with a mock buffer reporting a huge `size()` (asserting the buffer
 *     is released and never read), and an end-to-end fetch of a frame whose header advertises an
 *     out-of-range payload length.
 *   - '''Zero data loss.''' A frame whose CRC32C does not match its payload must invalidate the
 *     read instead of returning corrupt records.
 *   - '''M2 - reader-ack reclaim.''' Acknowledging a validated block must reclaim the producer's
 *     co-located per-partition buffer through the injected [[MemorySpillManager]].
 *   - '''Happy path / compression symmetry.''' A block framed exactly as the writer publishes it
 *     (records serialized through `serializerManager.wrapStream` for the block id, then
 *     enveloped) must round-trip back to the original records through the real reader path,
 *     proving the reader unwraps compression symmetrically with the writer.
 *   - '''Reduce-side parity with the sort reader (F-104).''' The reduce-side tail of `read()` is
 *     byte-identical to [[org.apache.spark.shuffle.BlockStoreShuffleReader]], so three tests
 *     drive a mocked [[ShuffleDependency]] with a real [[Aggregator]] (no map-side combine, the
 *     `combineValuesByKey` path), with map-side combine set (the `combineCombinersByKey` path),
 *     and with a key [[Ordering]] (the `ExternalSorter` sort path), asserting the exact
 *     aggregated/sorted output the sort reader would produce. A tagging aggregator makes the two
 *     combine branches produce distinct results, proving dispatch on `mapSideCombine`.
 *   - '''Checksum retransmission (zero data loss without recomputation).''' A transient CRC32C
 *     mismatch that clears on a re-fetch must self-heal via bounded retransmission, recovering
 *     every record without invalidating the read.
 *
 * '''Why this suite extends [[SharedSparkContext]].''' The reader deserializes through the live
 * `SparkEnv.serializerManager` (so compression/encryption wrapping matches the writer) and runs
 * its completion/interruptible-iterator plumbing against a real [[TaskContext]] built with
 * `MemoryTestingUtils.fakeTaskContext`. [[MapOutputTracker]], [[BlockManager]] and
 * [[BlockTransferService]] are Mockito mocks so the test drives the exact bytes and timing the
 * reader observes without standing up a cluster; the [[ShuffleDependency]] is mocked with only
 * the members the reader reads (shuffle id, serializer, and -- per test -- aggregator, key
 * ordering, and map-side-combine flag).
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with SharedSparkContext
  with PrivateMethodTester {

  /** A fresh Java serializer; the reader calls `dependency.serializer.newInstance()`. */
  private val serializer = new JavaSerializer(conf)

  /** Hands out a distinct shuffle id per reader so mocked map-output lookups never collide. */
  private val nextShuffleId = new AtomicInteger(0)

  /** The single producing executor every test fetches from. */
  private val producer = BlockManagerId("exec-1", "localhost", 7337)

  /** Synchronous handle into the private frame-decode path for the isolated budget-guard test. */
  private val decodeFrames =
    PrivateMethod[Array[Byte]](Symbol("decodeFramesFromBuffer"))

  /**
   * A sum aggregator whose `createCombiner` tags the first value (x1000). Because
   * `combineValuesByKey` calls `createCombiner` on the first value of each key while
   * `combineCombinersByKey` calls only `mergeCombiners`, the two reduce-side branches yield
   * provably different results for the same input. The aggregation and map-side-combine tests use
   * this to prove the reader dispatches on `dependency.mapSideCombine` like the sort reader.
   */
  private val taggingSumAggregator: Aggregator[Int, Int, Int] =
    new Aggregator[Int, Int, Int](
      createCombiner = (v: Int) => v * 1000,
      mergeValue = (c: Int, v: Int) => c + v,
      mergeCombiners = (c1: Int, c2: Int) => c1 + c2)

  /**
   * Bundles the reduce-side dependency shape the mocked [[ShuffleDependency]] reports so the
   * reader exercises the aggregation, map-side-combine, and key-ordering parity paths. Holding
   * these three knobs in one value keeps `newReader` within the Scalastyle parameter limit. The
   * defaults reproduce unaggregated, unsorted stream-order output (the base reader behavior).
   *
   * @param aggregator     the reduce-side aggregator (default none, i.e. stream-order output);
   *                       set to drive the combine paths
   * @param keyOrdering    the reduce-side key ordering (default none); set to drive the sort path
   * @param mapSideCombine whether the dependency reports map-side combine (default false); when
   *                       true the reader must merge already-combined values
   */
  private case class DepShape(
      aggregator: Option[Aggregator[Int, Int, Int]] = None,
      keyOrdering: Option[Ordering[Int]] = None,
      mapSideCombine: Boolean = false)

  /**
   * Builds a [[StreamingShuffleReader]] for a single map (index 0) and a single reduce partition
   * (0), wired to the supplied mocked transport and map-output tracker. The dependency is mocked
   * with exactly the members the reader reads; the block manager is a mock whose
   * `blockTransferService` returns `transferService`. The reader deserializes through the live
   * `SparkEnv.serializerManager`, matching the writer's wrapping.
   *
   * @param shuffleId             the shuffle id the tracker is stubbed for
   * @param transferService       the mocked transport that delivers (or withholds) fetched blocks
   * @param tracker               the mocked map-output tracker
   * @param context              the real task context the read runs within
   * @param metrics               the metrics holder asserted for invalidation counts
   * @param spillManagerOpt       optional co-located spill manager reclaimed on acknowledgment
   * @param producerTimeoutMs     the bounded producer connection timeout (small in timeout tests)
   * @param maxRetransmitAttempts the retransmit cap
   * @param initialRetryBackoffMs the first retry backoff (small to keep tests fast)
   * @param depShape              the reduce-side dependency shape (aggregator / key ordering /
   *                              map-side combine) driving the sort-reader parity paths
   */
  private def newReader(
      shuffleId: Int,
      transferService: BlockTransferService,
      tracker: MapOutputTracker,
      context: TaskContext,
      metrics: StreamingShuffleMetrics,
      spillManagerOpt: Option[MemorySpillManager] = None,
      producerTimeoutMs: Long = 5000L,
      maxRetransmitAttempts: Int = 2,
      initialRetryBackoffMs: Long = 20L,
      depShape: DepShape = DepShape()): StreamingShuffleReader[Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.shuffleId).thenReturn(shuffleId)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(depShape.aggregator)
    when(dependency.keyOrdering).thenReturn(depShape.keyOrdering)
    when(dependency.mapSideCombine).thenReturn(depShape.mapSideCombine)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      dependency = dependency,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = 0)
    val blockManager = mock(classOf[BlockManager])
    when(blockManager.blockTransferService).thenReturn(transferService)
    val backpressure = new BackpressureProtocol(metrics, linkCapacityBytes = 1L << 20)
    new StreamingShuffleReader[Int, Int](
      handle, 0, 1, 0, 1, context,
      context.taskMetrics().createTempShuffleReadMetrics(), backpressure, metrics,
      mapOutputTracker = tracker,
      blockManager = blockManager,
      serializerManager = sc.env.serializerManager,
      producerTimeoutMs = producerTimeoutMs,
      maxRetransmitAttempts = maxRetransmitAttempts,
      initialRetryBackoffMs = initialRetryBackoffMs,
      spillManagerOpt = spillManagerOpt)
  }

  /**
   * Stub `tracker.getMapSizesByExecutorId` to return a single block of `size` bytes located
   * on the producing executor, exactly as the reader resolves its partition range.
   */
  private def stubTracker(
      tracker: MapOutputTracker,
      shuffleId: Int,
      blockId: BlockId,
      size: Long): Unit = {
    val blocks: scala.collection.Seq[(BlockId, Long, Int)] = Seq((blockId, size, 0))
    when(tracker.getMapSizesByExecutorId(
      meq(shuffleId), anyInt(), anyInt(), anyInt(), anyInt()))
      .thenReturn(Iterator((producer, blocks)))
  }

  /**
   * Stub `transferService.fetchBlocks` to synchronously deliver `frameFor(blockId)` to the
   * fetch listener as an in-memory [[NioManagedBuffer]]. Mirrors the executor transport answering
   * a producer's in-progress block.
   */
  private def deliver(
      transferService: BlockTransferService,
      frameFor: String => Array[Byte]): Unit = {
    doAnswer { (inv: InvocationOnMock) =>
      val blockIds = inv.getArgument[Array[String]](3)
      val listener = inv.getArgument[BlockFetchingListener](4)
      blockIds.foreach { bid =>
        listener.onBlockFetchSuccess(bid, new NioManagedBuffer(ByteBuffer.wrap(frameFor(bid))))
      }
      null
    }.when(transferService).fetchBlocks(any(), anyInt(), any(), any(), any(), any())
  }

  /**
   * Frame `records` for `(shuffleId, mapId, reduceId)` byte-identically to the producer writer:
   * serialize through `serializerManager.wrapStream` (so the payload is compressed under the
   * default `spark.shuffle.compress=true`), then wrap the bytes in one [[StreamingBlockEnvelope]]
   * with a valid CRC32C. The reader must unwrap with the matching keyed `wrapStream` to recover
   * the records, which is precisely the compression-symmetry contract under test.
   */
  private def frameBlock(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      records: Seq[(Int, Int)]): Array[Byte] = {
    val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
    val baos = new ByteArrayOutputStream()
    val wrapped = sc.env.serializerManager.wrapStream(blockId, baos)
    val out = serializer.newInstance().serializeStream(wrapped)
    records.foreach { case (k, v) => out.writeKey(k); out.writeValue(v) }
    out.close()
    val buf = StreamingBlockEnvelope.encode(shuffleId, mapId, reduceId, baos.toByteArray)
    val frame = new Array[Byte](buf.remaining())
    buf.get(frame)
    frame
  }

  test("reader round-trips a published streaming block through the real fetch + decode path") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val records = Seq((1, 100), (2, 200), (3, 300))
    val frame = frameBlock(shuffleId, mapId = 0L, reduceId = 0, records)
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => frame)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, frame.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics)

    val read = reader.read().map { case (k, v) => (k, v) }.toList
    assert(read === records, "the reader must recover every published record in stream order")
    assert(metrics.getPartialReadInvalidations === 0L, "a clean read must not invalidate")
  }

  test("reader enforces the producer connection timeout with a bounded fetch (C3)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)

    // A transport that accepts the fetch but never answers the listener, modelling a hung/lost
    // producer. With the unbounded `Duration.Inf` await this read would block forever.
    val transfer = mock(classOf[BlockTransferService])
    doAnswer((_: InvocationOnMock) => null)
      .when(transfer).fetchBlocks(any(), anyInt(), any(), any(), any(), any())
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, size = 1024L)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics,
      producerTimeoutMs = 400L, maxRetransmitAttempts = 3, initialRetryBackoffMs = 20L)

    val startNs = System.nanoTime()
    intercept[FetchFailedException] {
      reader.read().toList
    }
    val elapsedMs = (System.nanoTime() - startNs) / 1000000L
    assert(elapsedMs < 4000L,
      s"a hung fetch must be bounded by the producer timeout, but took $elapsedMs ms")
    assert(metrics.getPartialReadInvalidations === 1L,
      "the timed-out partial read must be invalidated exactly once")
  }

  test("reader rejects an over-budget fetched buffer before reading it (M7)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val transfer = mock(classOf[BlockTransferService])
    val tracker = mock(classOf[MapOutputTracker])
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics)

    // A buffer that reports a size larger than any legal block. The decoder must reject it on the
    // upfront budget guard, releasing the buffer WITHOUT ever opening a stream over it (so a
    // corrupt/non-streaming block can never force an unbounded allocation).
    val oversized = mock(classOf[ManagedBuffer])
    when(oversized.size()).thenReturn(Long.MaxValue)

    val ex = intercept[FetchFailedException] {
      reader.invokePrivate(decodeFrames(oversized, producer, 0L, 0, 0, 0L))
    }
    assert(ex.getMessage.contains("fetch budget"),
      "rejection must cite the fetch budget guard")
    verify(oversized, never()).createInputStream()
    verify(oversized).release()
    assert(metrics.getPartialReadInvalidations === 1L)
  }

  test("reader rejects an out-of-range declared frame payload length before allocating (M7)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)

    // A 32-byte header whose payload-length field (offset 20) advertises Int.MaxValue, far beyond
    // MAX_PAYLOAD_SIZE. The reader must reject this on the header check, before allocating a
    // payload array of that size. The buffer is small so the upfront size budget passes first.
    val bad = new Array[Byte](StreamingBlockEnvelope.HEADER_SIZE + 4)
    val bb = ByteBuffer.wrap(bad).order(ByteOrder.BIG_ENDIAN)
    bb.putShort(0x5353.toShort) // MAGIC
    bb.putShort(1.toShort)      // VERSION
    bb.putInt(shuffleId)
    bb.putLong(0L)              // mapId
    bb.putInt(0)                // reduceId
    bb.putInt(Int.MaxValue)     // payloadLength @ offset 20

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => bad)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, size = bad.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics)

    val ex = intercept[FetchFailedException] {
      reader.read().toList
    }
    assert(ex.getMessage.contains("out-of-range payload length"),
      "rejection must cite the out-of-range declared payload length")
    assert(metrics.getPartialReadInvalidations === 1L)
  }

  test("reader invalidates a block whose CRC32C checksum does not match (zero data loss)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)
    val frame = frameBlock(shuffleId, 0L, 0, Seq((7, 70), (8, 80)))
    // Corrupt the first payload byte (just past the 32-byte header), invalidating the CRC32C the
    // producer stamped over the payload without disturbing the structural header fields.
    frame(StreamingBlockEnvelope.HEADER_SIZE) =
      (frame(StreamingBlockEnvelope.HEADER_SIZE) ^ 0xFF).toByte

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => frame)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, frame.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics)

    val ex = intercept[FetchFailedException] {
      reader.read().toList
    }
    assert(ex.getMessage.contains("CRC32C checksum mismatch"),
      "a corrupt payload must invalidate on the checksum check")
    assert(metrics.getPartialReadInvalidations === 1L)
  }

  test("acknowledging a validated block reclaims the producer's spill buffer (M2)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)
    val frame = frameBlock(shuffleId, 0L, 0, Seq((5, 50)))

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => frame)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, frame.length.toLong)

    // A real spill manager with a huge memory denominator (so the background poller never spills
    // our small buffer) wired to mock storage. The reader's acknowledgment must reclaim the
    // producer's per-partition buffer, resetting its heap.
    val memoryManager = mock(classOf[MemoryManager])
    when(memoryManager.maxOnHeapStorageMemory).thenReturn(1L << 30)
    val spillBlockManager = mock(classOf[BlockManager])
    val metrics = new StreamingShuffleMetrics
    val spillManager = new MemorySpillManager(spillBlockManager, memoryManager, metrics, 95)
    try {
      val buffer = new StreamingBuffer(0)
      buffer.append(new Array[Byte](500))
      spillManager.registerBuffer(MemorySpillManager.BufferKey(shuffleId, 0L, 0), buffer)
      assert(buffer.size === 500L, "the producer buffer must hold its bytes before reclaim")

      val context = MemoryTestingUtils.fakeTaskContext(sc.env)
      val reader = newReader(shuffleId, transfer, tracker, context, metrics,
        spillManagerOpt = Some(spillManager))
      assert(reader.read().toList === Seq((5, 50)))

      assert(buffer.size === 0L,
        "acknowledging the block must reclaim (reset) the producer's per-partition buffer")
    } finally {
      spillManager.stop()
    }
  }

  test("reader aggregates values by key when an aggregator is defined (sort parity)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)
    // Raw (key, value) records; with an aggregator and no map-side combine the reader must take
    // the `combineValuesByKey` branch -- byte-identical to BlockStoreShuffleReader -- so the
    // tagging aggregator runs createCombiner on the first value of each key, then mergeValue.
    val records = Seq((1, 10), (1, 20), (2, 7), (1, 1), (2, 3))
    val frame = frameBlock(shuffleId, 0L, 0, records)

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => frame)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, frame.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics,
      depShape = DepShape(aggregator = Some(taggingSumAggregator), mapSideCombine = false))

    // The aggregator's ExternalAppendOnlyMap reads TaskContext.get(); set it as a real task
    // thread would, so the reduce-side aggregation runs exactly as in production.
    TaskContext.setTaskContext(context)
    try {
      // combineValuesByKey: key 1 -> createCombiner(10)=10000, +20, +1 = 10021;
      //                     key 2 -> createCombiner(7)=7000, +3 = 7003.
      assert(reader.read().map { case (k, v) => (k, v) }.toMap === Map(1 -> 10021, 2 -> 7003),
        "the reader must combine values by key exactly as the sort reader does")
      assert(metrics.getPartialReadInvalidations === 0L)
    } finally {
      TaskContext.unset()
    }
  }

  test("reader merges already-combined values when map-side combine is set (sort parity)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)
    // With map-side combine the producer already created combiners, so the reader must take the
    // `combineCombinersByKey` branch -- merging via mergeCombiners only, never createCombiner.
    // The same tagging aggregator therefore yields a different (un-tagged) result than the
    // combineValuesByKey branch above, proving the reader dispatches on mapSideCombine.
    val records = Seq((1, 10), (1, 20), (2, 7), (1, 1), (2, 3))
    val frame = frameBlock(shuffleId, 0L, 0, records)

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => frame)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, frame.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics,
      depShape = DepShape(aggregator = Some(taggingSumAggregator), mapSideCombine = true))

    // The aggregator's ExternalAppendOnlyMap reads TaskContext.get(); set it as a real task
    // thread would, so the reduce-side aggregation runs exactly as in production.
    TaskContext.setTaskContext(context)
    try {
      // combineCombinersByKey: mergeCombiners only -> sums. key 1 = 10+20+1 = 31; key 2 = 10.
      assert(reader.read().map { case (k, v) => (k, v) }.toMap === Map(1 -> 31, 2 -> 10),
        "with map-side combine the reader must merge combiners by key (no createCombiner)")
      assert(metrics.getPartialReadInvalidations === 0L)
    } finally {
      TaskContext.unset()
    }
  }

  test("reader sorts output by key when the dependency defines a key ordering (sort parity)") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)
    // Records arrive in stream (unsorted) order; with a key ordering and no aggregator the reader
    // must take the ExternalSorter branch and emit them in ascending key order, exactly as the
    // sort reader does.
    val records = Seq((5, 50), (1, 10), (3, 30), (2, 20), (4, 40))
    val frame = frameBlock(shuffleId, 0L, 0, records)

    val transfer = mock(classOf[BlockTransferService])
    deliver(transfer, _ => frame)
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, frame.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    val reader = newReader(shuffleId, transfer, tracker, context, metrics,
      depShape = DepShape(keyOrdering = Some(Ordering.Int)))

    val read = reader.read().map { case (k, v) => (k, v) }.toList
    assert(read === records.sortBy(_._1),
      "the reader must emit records sorted by key when a key ordering is defined")
    assert(read.map(_._1) === Seq(1, 2, 3, 4, 5), "keys must be in ascending order")
    assert(metrics.getPartialReadInvalidations === 0L)
  }

  test("reader recovers via retransmission when a transient CRC32C mismatch self-heals") {
    val shuffleId = nextShuffleId.getAndIncrement()
    val blockId = ShuffleBlockId(shuffleId, 0L, 0)
    val records = Seq((9, 90), (10, 100))
    val good = frameBlock(shuffleId, 0L, 0, records)
    // A corrupt copy of the same frame: flip the first payload byte so its CRC32C fails. The
    // transport delivers the corrupt copy on the first fetch and the good copy thereafter,
    // modelling a transient on-wire corruption that a re-fetch clears.
    val corrupt = good.clone()
    corrupt(StreamingBlockEnvelope.HEADER_SIZE) =
      (corrupt(StreamingBlockEnvelope.HEADER_SIZE) ^ 0xFF).toByte

    val fetchCount = new AtomicInteger(0)
    val transfer = mock(classOf[BlockTransferService])
    doAnswer { (inv: InvocationOnMock) =>
      val blockIds = inv.getArgument[Array[String]](3)
      val listener = inv.getArgument[BlockFetchingListener](4)
      val frame = if (fetchCount.getAndIncrement() == 0) corrupt else good
      blockIds.foreach { bid =>
        listener.onBlockFetchSuccess(bid, new NioManagedBuffer(ByteBuffer.wrap(frame)))
      }
      null
    }.when(transfer).fetchBlocks(any(), anyInt(), any(), any(), any(), any())
    val tracker = mock(classOf[MapOutputTracker])
    stubTracker(tracker, shuffleId, blockId, good.length.toLong)

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val metrics = new StreamingShuffleMetrics
    // Allow up to 2 attempts so the corrupt first fetch is retransmitted into the good copy.
    val reader = newReader(shuffleId, transfer, tracker, context, metrics,
      maxRetransmitAttempts = 2, initialRetryBackoffMs = 1L)

    val read = reader.read().map { case (k, v) => (k, v) }.toList
    assert(read === records, "a transient checksum mismatch must self-heal via retransmission")
    assert(fetchCount.get() === 2, "the reader must have re-fetched the block exactly once")
    assert(metrics.getPartialReadInvalidations === 0L,
      "a recovered read must not be invalidated (zero data loss without recomputation)")
  }
}
