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

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, verify, when}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleBlockInfo,
  ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, BlockManager, RDDBlockId, ShuffleBlockId,
  ShuffleMergedBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

/**
 * Behavioral unit tests for [[StreamingShuffleBlockResolver]].
 *
 * The streaming block resolver is the component that (1) tracks streaming-shuffle output in an
 * in-memory / spilled three-level block map, (2) serves a tracked block from its resident
 * [[StreamingBuffer]] or its spilled on-disk file, and (3) preserves Spark's decommission block
 * migration by delegating every [[MigratableResolver]] operation -- and every untracked block --
 * to the inner sort-path [[IndexShuffleBlockResolver]]. Before this suite the resolver had only
 * type/wiring coverage (a single `isInstanceOf[MigratableResolver]` assertion in
 * `StreamingShuffleManagerSuite`); its substantive block-addressing logic and its entire
 * decommission-migration delegation surface were never executed by any test, so a regression in
 * either would have gone undetected. This suite closes that gap by exercising all ~13 methods
 * behaviorally.
 *
 * ==Delegation proof==
 * Delegation to the inner sort resolver is proven directly: a Mockito mock
 * [[IndexShuffleBlockResolver]] is injected into the resolver's private `index` field via
 * reflection (the same reflective-internal-access idiom used elsewhere in the streaming test
 * corpus, e.g. `MemorySpillManagerSuite`), then each delegating call is verified to forward to the
 * mock and to pass its return value straight through. This asserts the exact "delegate to the
 * sort-path resolver" contract that keeps block migration and unknown-block reads behaving
 * identically to the sort path, without depending on the inner resolver's on-disk file layout.
 *
 * ==Memory / disk serving==
 * The memory- and disk-serving paths are exercised against real collaborators (a real
 * [[StreamingBuffer]] and a real temp file) so the tests assert actual served bytes, not merely
 * that a call did not throw.
 *
 * No production class is stubbed except the inner sort resolver (whose delegation is the property
 * under test) and the [[BlockManager]] (consumed only by the inner resolver's lazy construction).
 */
class StreamingShuffleBlockResolverSuite extends SparkFunSuite {

  /**
   * Build a [[StreamingShuffleBlockResolver]] over a mocked [[BlockManager]] and an empty
   * shuffle-id-to-task-ids map. Construction builds a real inner [[IndexShuffleBlockResolver]];
   * that inner resolver resolves its own `blockManager` lazily and needs only the [[SparkConf]] to
   * build its transport conf, so a bare mock block manager is sufficient and no `SparkEnv` is
   * required.
   */
  private def newResolver(): StreamingShuffleBlockResolver = {
    val conf = new SparkConf(false)
    val blockManager = mock(classOf[BlockManager])
    val taskIdMapsForShuffle: ConcurrentMap[Int, OpenHashSet[Long]] =
      new ConcurrentHashMap[Int, OpenHashSet[Long]]()
    new StreamingShuffleBlockResolver(conf, blockManager, taskIdMapsForShuffle)
  }

  /**
   * Replace the resolver's private, final inner [[IndexShuffleBlockResolver]] with a Mockito mock
   * so delegation can be verified precisely. Reflection on a final instance field is permitted on
   * the enforced JDK 17 baseline once the field is made accessible.
   *
   * @return the injected mock, for stubbing and `verify`
   */
  private def injectMockIndex(
      resolver: StreamingShuffleBlockResolver): IndexShuffleBlockResolver = {
    val mockIndex = mock(classOf[IndexShuffleBlockResolver])
    val field = classOf[StreamingShuffleBlockResolver].getDeclaredField("index")
    field.setAccessible(true)
    field.set(resolver, mockIndex)
    mockIndex
  }

  /** Read all bytes out of a [[ManagedBuffer]] via its NIO view for byte-exact assertions. */
  private def readAll(buffer: ManagedBuffer): Array[Byte] = {
    val bb = buffer.nioByteBuffer()
    val out = new Array[Byte](bb.remaining())
    bb.get(out)
    out
  }

  // -------------------------------------------------------------------------------------------
  // (a) Three-level block map: putBlock / getBlock / removeShuffle.
  // -------------------------------------------------------------------------------------------

  test("putBlock then getBlock round-trips a reference; absent coordinates return None") {
    val resolver = newResolver()
    try {
      val buffer = new StreamingBuffer(shuffleId = 1, mapId = 2L, reduceId = 3)
      resolver.putBlock(1, 2L, 3, buffer)
      // Exact reference is returned for the tracked triple.
      assert(resolver.getBlock(1, 2L, 3).contains(buffer))
      // Every axis of the triple is honored independently: a miss on any level yields None.
      assert(resolver.getBlock(1, 2L, 99).isEmpty)
      assert(resolver.getBlock(1, 99L, 3).isEmpty)
      assert(resolver.getBlock(99, 2L, 3).isEmpty)
    } finally {
      resolver.stop()
    }
  }

  test("putBlock replaces a tracked reference (buffer -> spilled file)") {
    val resolver = newResolver()
    try {
      val buffer = new StreamingBuffer(shuffleId = 1, mapId = 0L, reduceId = 0)
      resolver.putBlock(1, 0L, 0, buffer)
      assert(resolver.getBlock(1, 0L, 0).contains(buffer))
      // The spill manager rewrites the leaf from a StreamingBuffer to the spilled File; the map
      // must observe the replacement so later reads are served from disk, not stale memory.
      val spilledFile = new File("/tmp/streaming-spilled-marker")
      resolver.putBlock(1, 0L, 0, spilledFile)
      assert(resolver.getBlock(1, 0L, 0).contains(spilledFile))
    } finally {
      resolver.stop()
    }
  }

  test("removeShuffle drops all tracked blocks for one shuffle only") {
    val resolver = newResolver()
    try {
      resolver.putBlock(5, 0L, 0, new StreamingBuffer(5, 0L, 0))
      resolver.putBlock(5, 1L, 0, new StreamingBuffer(5, 1L, 0))
      resolver.putBlock(6, 0L, 0, new StreamingBuffer(6, 0L, 0))
      resolver.removeShuffle(5)
      assert(resolver.getBlock(5, 0L, 0).isEmpty)
      assert(resolver.getBlock(5, 1L, 0).isEmpty)
      // A different shuffle's tracking is untouched.
      assert(resolver.getBlock(6, 0L, 0).isDefined)
      // Removing a shuffle that was never tracked is a safe no-op.
      resolver.removeShuffle(404)
    } finally {
      resolver.stop()
    }
  }

  // -------------------------------------------------------------------------------------------
  // (b) getBlockData serves tracked blocks from memory and from disk (real collaborators).
  // -------------------------------------------------------------------------------------------

  test("getBlockData serves a memory-resident StreamingBuffer from memory (byte-exact)") {
    val resolver = newResolver()
    try {
      val payload = Array.tabulate(256)(i => (i % 251).toByte)
      val buffer = new StreamingBuffer(shuffleId = 1, mapId = 2L, reduceId = 3)
      buffer.append(payload)
      resolver.putBlock(1, 2L, 3, buffer)
      val data = resolver.getBlockData(ShuffleBlockId(1, 2L, 3))
      // Memory-resident blocks are served over an on-heap NIO buffer, not a file segment.
      assert(data.isInstanceOf[NioManagedBuffer])
      assert(data.size() == payload.length.toLong)
      // Zero data corruption: the served bytes equal the buffered snapshot exactly.
      assert(readAll(data).sameElements(payload))
    } finally {
      resolver.stop()
    }
  }

  test("getBlockData serves a spilled block from its on-disk file (byte-exact)") {
    val resolver = newResolver()
    val tempDir = Utils.createTempDir()
    try {
      val content = Array.tabulate(1024)(i => (i % 97).toByte)
      val file = new File(tempDir, "shuffle_2_0_1.data")
      val out = new FileOutputStream(file)
      try {
        out.write(content)
      } finally {
        out.close()
      }
      resolver.putBlock(2, 0L, 1, file)
      val data = resolver.getBlockData(ShuffleBlockId(2, 0L, 1))
      // Spilled blocks are served zero-copy from disk via a file segment, not materialized on heap.
      assert(data.isInstanceOf[FileSegmentManagedBuffer])
      assert(data.size() == content.length.toLong)
      // Zero data loss: the on-disk bytes are served intact end to end.
      assert(readAll(data).sameElements(content))
    } finally {
      Utils.deleteRecursively(tempDir)
      resolver.stop()
    }
  }

  // -------------------------------------------------------------------------------------------
  // (c) Untracked blocks delegate to the inner sort-path resolver.
  // -------------------------------------------------------------------------------------------

  test("getBlockData delegates an untracked ShuffleBlockId to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val blockId = ShuffleBlockId(9, 9L, 9)
      val sentinel = new NioManagedBuffer(ByteBuffer.wrap(Array[Byte](42)))
      when(mockIndex.getBlockData(meq(blockId), any())).thenReturn(sentinel)
      // Not tracked by streaming -> the sort resolver serves it, unchanged.
      assert(resolver.getBlockData(blockId) eq sentinel)
      verify(mockIndex).getBlockData(meq(blockId), any())
    } finally {
      resolver.stop()
    }
  }

  test("getBlockData delegates a non-ShuffleBlockId to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val blockId = RDDBlockId(0, 0)
      val sentinel = new NioManagedBuffer(ByteBuffer.wrap(Array[Byte](7)))
      when(mockIndex.getBlockData(meq(blockId), any())).thenReturn(sentinel)
      // A non-ShuffleBlockId (e.g. a batch-fetched range or RDD block) is parsed and served by the
      // sort resolver, exactly as on the sort path.
      assert(resolver.getBlockData(blockId) eq sentinel)
      verify(mockIndex).getBlockData(meq(blockId), any())
    } finally {
      resolver.stop()
    }
  }

  test("getBlocksForShuffle delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val expected = Seq[BlockId](ShuffleBlockId(1, 2L, 0), ShuffleBlockId(1, 2L, 1))
      when(mockIndex.getBlocksForShuffle(meq(1), meq(2L))).thenReturn(expected)
      assert(resolver.getBlocksForShuffle(1, 2L) == expected)
      verify(mockIndex).getBlocksForShuffle(meq(1), meq(2L))
    } finally {
      resolver.stop()
    }
  }

  test("getMergedBlockData delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val mergedId = ShuffleMergedBlockId(1, 0, 0)
      val chunks = Seq[ManagedBuffer](new NioManagedBuffer(ByteBuffer.wrap(Array[Byte](1, 2))))
      when(mockIndex.getMergedBlockData(meq(mergedId), any())).thenReturn(chunks)
      // Push-based (merged) reads are out of scope for streaming v1 and handed to the sort path.
      assert(resolver.getMergedBlockData(mergedId, None) == chunks)
      verify(mockIndex).getMergedBlockData(meq(mergedId), any())
    } finally {
      resolver.stop()
    }
  }

  test("getMergedBlockMeta delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val mergedId = ShuffleMergedBlockId(1, 0, 0)
      val meta = mock(classOf[MergedBlockMeta])
      when(mockIndex.getMergedBlockMeta(meq(mergedId), any())).thenReturn(meta)
      assert(resolver.getMergedBlockMeta(mergedId, None) eq meta)
      verify(mockIndex).getMergedBlockMeta(meq(mergedId), any())
    } finally {
      resolver.stop()
    }
  }

  // -------------------------------------------------------------------------------------------
  // (d) MigratableResolver decommission-migration surface delegates to the inner sort resolver.
  //     This is the AAP "decommission migration preservation" requirement (0.1.1 / 0.4.1).
  // -------------------------------------------------------------------------------------------

  test("getStoredShuffles delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val stored = Seq(ShuffleBlockInfo(1, 2L), ShuffleBlockInfo(3, 4L))
      when(mockIndex.getStoredShuffles()).thenReturn(stored)
      assert(resolver.getStoredShuffles() == stored)
      verify(mockIndex).getStoredShuffles()
    } finally {
      resolver.stop()
    }
  }

  test("addShuffleToSkip delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      resolver.addShuffleToSkip(7)
      verify(mockIndex).addShuffleToSkip(7)
    } finally {
      resolver.stop()
    }
  }

  test("putShuffleBlockAsStream delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val blockId = ShuffleBlockId(1, 0L, 0)
      val serializerManager = mock(classOf[SerializerManager])
      val callback = mock(classOf[StreamCallbackWithID])
      when(mockIndex.putShuffleBlockAsStream(meq(blockId), meq(serializerManager)))
        .thenReturn(callback)
      assert(resolver.putShuffleBlockAsStream(blockId, serializerManager) eq callback)
      verify(mockIndex).putShuffleBlockAsStream(meq(blockId), meq(serializerManager))
    } finally {
      resolver.stop()
    }
  }

  test("getMigrationBlocks delegates to the inner sort resolver") {
    val resolver = newResolver()
    try {
      val mockIndex = injectMockIndex(resolver)
      val info = ShuffleBlockInfo(1, 2L)
      val blocks = List[(BlockId, ManagedBuffer)](
        ShuffleBlockId(1, 2L, 0) -> new NioManagedBuffer(ByteBuffer.wrap(Array[Byte](1))))
      when(mockIndex.getMigrationBlocks(meq(info))).thenReturn(blocks)
      assert(resolver.getMigrationBlocks(info) == blocks)
      verify(mockIndex).getMigrationBlocks(meq(info))
    } finally {
      resolver.stop()
    }
  }

  // -------------------------------------------------------------------------------------------
  // (e) Lifecycle + type/wiring.
  // -------------------------------------------------------------------------------------------

  test("stop clears the streaming block map and stops the inner sort resolver") {
    val resolver = newResolver()
    resolver.putBlock(1, 0L, 0, new StreamingBuffer(1, 0L, 0))
    val mockIndex = injectMockIndex(resolver)
    resolver.stop()
    // The inner resolver's own lifecycle is honored...
    verify(mockIndex).stop()
    // ...and streaming's tracking map is released so resident buffers become GC-eligible.
    assert(resolver.getBlock(1, 0L, 0).isEmpty)
  }

  test("the resolver is a migration-capable ShuffleBlockResolver") {
    val resolver = newResolver()
    try {
      assert(resolver.isInstanceOf[ShuffleBlockResolver])
      // Decommission block migration is preserved by delegating to the sort-path resolver, so the
      // streaming resolver must itself be a MigratableResolver.
      assert(resolver.isInstanceOf[MigratableResolver])
    } finally {
      resolver.stop()
    }
  }
}
