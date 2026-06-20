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

import java.nio.file.Files

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockManager, ShuffleBlockId,
  ShuffleDataBlockId}

/**
 * Unit tests for [[StreamingShuffleBlockResolver]]'s three-step lookup, focusing on the two CP2
 * data-plane guarantees the review flagged:
 *
 *   - a still-in-memory partition is served as the WHOLE multi-frame buffer (every 2 MB frame, not
 *     just the first), so a multi-block partition loses no data on the in-memory fast path; and
 *   - once a partition has been spilled (the spill manager calls [[trackSpill]]), the resolver
 *     drops the in-memory entry and serves the partition from its on-disk file as a
 *     [[FileSegmentManagedBuffer]] -- it does NOT delegate to the inner index resolver (which has
 *     no `.index` for a streaming spill) and never serves the cleared in-memory buffer.
 *
 * The inner [[IndexShuffleBlockResolver]] and the [[BlockManager]] (and its [[DiskBlockManager]])
 * are mocked so the disk-serve path reads a real temp file the test controls, while delegation for
 * untracked blocks is verified against the mock.
 */
class StreamingShuffleBlockResolverSuite extends SparkFunSuite with Matchers {

  private val shuffleId = 11
  private val mapId = 3L
  private val reduceId = 2

  /** Reads all bytes of a [[ManagedBuffer]] into a fresh array via its nio view. */
  private def bytesOf(buf: ManagedBuffer): Array[Byte] = {
    val bb = buf.nioByteBuffer()
    val out = new Array[Byte](bb.remaining())
    bb.get(out)
    out
  }

  /** A two-block buffer: two separate appends frame into two distinct 2 MB-capped envelopes. */
  private def twoFrameBuffer(payloadA: Array[Byte], payloadB: Array[Byte]): StreamingBuffer = {
    val buffer = new StreamingBuffer(shuffleId, mapId, reduceId, 1024L * 1024L)
    buffer.append(payloadA)
    buffer.append(payloadB)
    assert(buffer.numBlocks === 2, "two appends must produce two frames")
    buffer
  }

  test("in-memory lookup serves the whole multi-frame buffer, not just the first frame") {
    val indexResolver = mock(classOf[IndexShuffleBlockResolver])
    val blockManager = mock(classOf[BlockManager])
    val resolver = new StreamingShuffleBlockResolver(
      new SparkConf(false), indexResolver, blockManager)

    val payloadA = Array.tabulate(100)(i => (i & 0xFF).toByte)
    val payloadB = Array.tabulate(250)(i => ((i + 7) & 0xFF).toByte)
    resolver.trackBuffer(twoFrameBuffer(payloadA, payloadB))

    val served = resolver.getBlockData(ShuffleBlockId(shuffleId, mapId, reduceId), None)
    val frames = StreamingBlockEnvelope.parseAll(bytesOf(served))
    assert(frames.size === 2, "both frames must be served from the in-memory buffer")
    assert(frames.head.payload.sameElements(payloadA))
    assert(frames(1).payload.sameElements(payloadB))
    assert(frames.forall(_.verifyChecksum))
    // The in-memory fast path must not touch the inner sort-based resolver.
    verify(indexResolver, never()).getBlockData(any(), any())
  }

  test("after trackSpill the resolver serves the on-disk file, not the inner index resolver") {
    val indexResolver = mock(classOf[IndexShuffleBlockResolver])
    val blockManager = mock(classOf[BlockManager])
    val diskBlockManager = mock(classOf[DiskBlockManager])
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)

    val payloadA = Array.tabulate(64)(i => (i & 0xFF).toByte)
    val payloadB = Array.tabulate(128)(i => ((i + 3) & 0xFF).toByte)
    val buffer = twoFrameBuffer(payloadA, payloadB)
    // Materialize the canonical framed bytes to a temp file exactly as a DISK_ONLY spill would.
    val spilledBytes = buffer.toByteArray
    val tempFile = Files.createTempFile("blitzy_adhoc_test_streaming_spill", ".data").toFile
    try {
      Files.write(tempFile.toPath, spilledBytes)
      val diskBlockId = ShuffleDataBlockId(shuffleId, mapId, reduceId)
      when(diskBlockManager.getFile(any[BlockId]())).thenReturn(tempFile)

      val resolver = new StreamingShuffleBlockResolver(
        new SparkConf(false), indexResolver, blockManager)
      resolver.trackBuffer(buffer)
      resolver.trackedBufferCount mustBe 1

      // Simulate the spill manager's handoff: record the spill and drop the in-memory entry.
      resolver.trackSpill(shuffleId, mapId, reduceId, diskBlockId)
      resolver.trackedBufferCount mustBe 0
      resolver.spilledBlockCount mustBe 1

      val served = resolver.getBlockData(ShuffleBlockId(shuffleId, mapId, reduceId), None)
      assert(served.isInstanceOf[FileSegmentManagedBuffer],
        "a spilled block must be served as a FileSegmentManagedBuffer over its disk file")
      val frames = StreamingBlockEnvelope.parseAll(bytesOf(served))
      assert(frames.size === 2, "every frame must survive the spill -> disk -> read round-trip")
      assert(frames.head.payload.sameElements(payloadA))
      assert(frames(1).payload.sameElements(payloadB))
      assert(frames.forall(_.verifyChecksum))
      // The disk serve path must NOT delegate to the inner index resolver.
      verify(indexResolver, never()).getBlockData(any(), any())
    } finally {
      tempFile.delete()
    }
  }

  test("an untracked shuffle block is delegated to the inner index resolver") {
    val indexResolver = mock(classOf[IndexShuffleBlockResolver])
    val blockManager = mock(classOf[BlockManager])
    val sentinel = mock(classOf[ManagedBuffer])
    val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
    when(indexResolver.getBlockData(meq(blockId), meq(None))).thenReturn(sentinel)

    val resolver = new StreamingShuffleBlockResolver(
      new SparkConf(false), indexResolver, blockManager)
    // Nothing tracked: neither in memory nor spilled, so Step 3 delegates unconditionally.
    assert(resolver.getBlockData(blockId, None) eq sentinel)
    verify(indexResolver).getBlockData(meq(blockId), meq(None))
  }
}
