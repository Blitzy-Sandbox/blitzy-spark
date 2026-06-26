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
import java.util.concurrent.{Executors, TimeUnit}

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, verify, when}

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleMergedBlockId}

/**
 * Unit tests for [[StreamingShuffleBlockResolver]] (streaming shuffle feature F-105).
 *
 * The resolver has two responsibilities and the suite is organised to exercise both:
 *
 *  1. '''In-memory streaming block index.''' A thread-safe, three-level index keyed
 *     `shuffleId -> mapId -> partitionId`. The register / lookup / contains / mark-spilled /
 *     remove / count operations are asserted directly, including on-demand creation of the
 *     intermediate levels, the pruning of now-empty levels, and the ascending-partition ordering
 *     contract of `getStreamingBlocksForMap`.
 *
 *  2. '''Data resolution and migration by delegation.''' Every [[ShuffleBlockResolver]] data
 *     method and every [[org.apache.spark.shuffle.MigratableResolver]] method is forwarded
 *     verbatim to the composed [[IndexShuffleBlockResolver]]. Those forwards are verified against
 *     a Mockito mock so the suite proves the delegation happens, and that `getBlockData` serves a
 *     resident in-memory streaming block directly '''without''' delegating.
 *
 * '''Why the index resolver is mocked.''' A real [[IndexShuffleBlockResolver]] needs a live
 * `SparkConf`, `BlockManager` and on-disk index/data files, none of which is relevant to the
 * resolver's own logic (the in-memory index plus straight delegation). Mocking the collaborator
 * keeps every test a fast, deterministic pure-unit test (no `SparkContext`, no I/O) while still
 * letting the delegation contract be verified exactly.
 */
class StreamingShuffleBlockResolverSuite extends SparkFunSuite {

  import StreamingShuffleBlockResolver._

  /** The `dirs` argument is always `None` here; a typed val keeps the matchers unambiguous. */
  private val noDirs: Option[Array[String]] = None

  /** Build a resolver over a fresh mocked index resolver, returning both for assertions. */
  private def newResolver(): (StreamingShuffleBlockResolver, IndexShuffleBlockResolver) = {
    val indexResolver = mock(classOf[IndexShuffleBlockResolver])
    (new StreamingShuffleBlockResolver(indexResolver), indexResolver)
  }

  /** A tiny in-memory [[ManagedBuffer]] over the given byte values; keeps the tests terse. */
  private def buf(bytes: Int*): NioManagedBuffer =
    new NioManagedBuffer(ByteBuffer.wrap(bytes.map(_.toByte).toArray))

  /**
   * Build an in-memory streaming block metadata whose payload is `payload`. The CRC32C/offset
   * fields are arbitrary because this suite asserts the index/dispatch behaviour, not the
   * checksum maths (which is covered by the buffer and writer suites).
   */
  private def inMemoryBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      payload: NioManagedBuffer = buf(1, 2, 3)): StreamingBlockMetadata =
    StreamingBlockMetadata(shuffleId, mapId, partitionId, offset = 0L,
      length = payload.size, checksum = 0L, location = InMemory, data = Some(payload))

  // --------------------------------------------------------------------------
  // Streaming block index: register / lookup / contains.
  // --------------------------------------------------------------------------

  test("registerStreamingBlock then getStreamingBlock returns the registered metadata") {
    val (resolver, _) = newResolver()
    val md = inMemoryBlock(1, 2L, 3)
    resolver.registerStreamingBlock(md)
    assert(resolver.getStreamingBlock(1, 2L, 3).contains(md))
    assert(resolver.numStreamingBlocks === 1)
  }

  test("getStreamingBlock returns None for an unknown shuffle, map or partition") {
    val (resolver, _) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3))
    assert(resolver.getStreamingBlock(9, 2L, 3).isEmpty, "unknown shuffle")
    assert(resolver.getStreamingBlock(1, 9L, 3).isEmpty, "unknown map")
    assert(resolver.getStreamingBlock(1, 2L, 9).isEmpty, "unknown partition")
  }

  test("registerStreamingBlock replaces the metadata for the same coordinates") {
    val (resolver, _) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3, buf(0)))
    val replacement = inMemoryBlock(1, 2L, 3, buf(7, 8)).copy(length = 99L)
    resolver.registerStreamingBlock(replacement)
    assert(resolver.getStreamingBlock(1, 2L, 3).contains(replacement))
    assert(resolver.numStreamingBlocks === 1, "replacing must not grow the index")
  }

  test("containsStreamingBlock reflects registration and removal") {
    val (resolver, _) = newResolver()
    assert(!resolver.containsStreamingBlock(1, 2L, 3))
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3))
    assert(resolver.containsStreamingBlock(1, 2L, 3))
    resolver.removeStreamingBlock(1, 2L, 3)
    assert(!resolver.containsStreamingBlock(1, 2L, 3))
  }

  // --------------------------------------------------------------------------
  // getStreamingBlocksForMap.
  // --------------------------------------------------------------------------

  test("getStreamingBlocksForMap returns every partition in ascending partition order") {
    val (resolver, _) = newResolver()
    // Register out of order to prove the resolver sorts the result by partitionId.
    resolver.registerStreamingBlock(inMemoryBlock(1, 5L, 2))
    resolver.registerStreamingBlock(inMemoryBlock(1, 5L, 0))
    resolver.registerStreamingBlock(inMemoryBlock(1, 5L, 1))
    assert(resolver.getStreamingBlocksForMap(1, 5L).map(_.partitionId) === Seq(0, 1, 2))
  }

  test("getStreamingBlocksForMap returns an empty sequence for an unknown map") {
    val (resolver, _) = newResolver()
    assert(resolver.getStreamingBlocksForMap(1, 404L).isEmpty)
  }

  // --------------------------------------------------------------------------
  // markBlockSpilled.
  // --------------------------------------------------------------------------

  test("markBlockSpilled flips the location to Spilled and drops the in-memory payload") {
    val (resolver, _) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3))
    assert(resolver.markBlockSpilled(1, 2L, 3))
    val md = resolver.getStreamingBlock(1, 2L, 3)
    assert(md.exists(_.location == Spilled), "location must become Spilled")
    assert(md.exists(_.data.isEmpty), "the in-memory payload reference must be dropped")
  }

  test("markBlockSpilled returns false for an unknown block") {
    val (resolver, _) = newResolver()
    assert(!resolver.markBlockSpilled(1, 2L, 3))
  }

  // --------------------------------------------------------------------------
  // remove operations and empty-level pruning.
  // --------------------------------------------------------------------------

  test("removeStreamingBlock returns the removed metadata, or None when absent") {
    val (resolver, _) = newResolver()
    val md = inMemoryBlock(1, 2L, 3)
    resolver.registerStreamingBlock(md)
    assert(resolver.removeStreamingBlock(1, 2L, 3).contains(md))
    assert(resolver.removeStreamingBlock(1, 2L, 3).isEmpty, "a second remove finds nothing")
    assert(resolver.numStreamingBlocks === 0)
  }

  test("removeStreamingBlock prunes the now-empty map and shuffle levels") {
    val (resolver, _) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3))
    resolver.removeStreamingBlock(1, 2L, 3)
    assert(resolver.numStreamingBlocks === 0)
    // Re-registering after a full prune must rebuild the intermediate levels cleanly.
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3))
    assert(resolver.containsStreamingBlock(1, 2L, 3))
    assert(resolver.numStreamingBlocks === 1)
  }

  test("removeStreamingMap removes only the targeted map's blocks") {
    val (resolver, _) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 0))
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 1))
    resolver.registerStreamingBlock(inMemoryBlock(1, 9L, 0))
    resolver.removeStreamingMap(1, 2L)
    assert(resolver.getStreamingBlocksForMap(1, 2L).isEmpty)
    assert(resolver.containsStreamingBlock(1, 9L, 0), "the other map must be untouched")
    assert(resolver.numStreamingBlocks === 1)
  }

  test("removeStreamingShuffle removes only the targeted shuffle's blocks") {
    val (resolver, _) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 0))
    resolver.registerStreamingBlock(inMemoryBlock(2, 2L, 0))
    resolver.removeStreamingShuffle(1)
    assert(!resolver.containsStreamingBlock(1, 2L, 0))
    assert(resolver.containsStreamingBlock(2, 2L, 0), "the other shuffle must be untouched")
    assert(resolver.numStreamingBlocks === 1)
  }

  // --------------------------------------------------------------------------
  // numStreamingBlocks.
  // --------------------------------------------------------------------------

  test("numStreamingBlocks counts blocks across multiple shuffles and maps") {
    val (resolver, _) = newResolver()
    assert(resolver.numStreamingBlocks === 0)
    resolver.registerStreamingBlock(inMemoryBlock(1, 0L, 0))
    resolver.registerStreamingBlock(inMemoryBlock(1, 0L, 1))
    resolver.registerStreamingBlock(inMemoryBlock(1, 1L, 0))
    resolver.registerStreamingBlock(inMemoryBlock(2, 0L, 0))
    assert(resolver.numStreamingBlocks === 4)
  }

  // --------------------------------------------------------------------------
  // getBlockData dispatch (the streaming-aware ShuffleBlockResolver override).
  // --------------------------------------------------------------------------

  test("getBlockData serves a resident in-memory streaming block directly without delegating") {
    val (resolver, indexResolver) = newResolver()
    val md = inMemoryBlock(1, 2L, 3)
    resolver.registerStreamingBlock(md)
    val result = resolver.getBlockData(ShuffleBlockId(1, 2L, 3), noDirs)
    assert(result eq md.data.get, "must return the exact buffered payload")
    verify(indexResolver, never()).getBlockData(any(), any())
  }

  test("getBlockData delegates a spilled block to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3))
    resolver.markBlockSpilled(1, 2L, 3)
    val sentinel = buf(9)
    val blockId = ShuffleBlockId(1, 2L, 3)
    when(indexResolver.getBlockData(meq(blockId), meq(noDirs))).thenReturn(sentinel)
    assert(resolver.getBlockData(blockId, noDirs) eq sentinel)
    verify(indexResolver).getBlockData(meq(blockId), meq(noDirs))
  }

  test("getBlockData delegates a registered block that has no in-memory data") {
    val (resolver, indexResolver) = newResolver()
    // location == InMemory but data == None: the fast path requires both, so this must delegate.
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 3).copy(data = None))
    val sentinel = buf(9)
    val blockId = ShuffleBlockId(1, 2L, 3)
    when(indexResolver.getBlockData(meq(blockId), meq(noDirs))).thenReturn(sentinel)
    assert(resolver.getBlockData(blockId, noDirs) eq sentinel)
    verify(indexResolver).getBlockData(meq(blockId), meq(noDirs))
  }

  test("getBlockData delegates an unknown ShuffleBlockId to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val sentinel = buf(9)
    val blockId = ShuffleBlockId(7, 8L, 9)
    when(indexResolver.getBlockData(meq(blockId), meq(noDirs))).thenReturn(sentinel)
    assert(resolver.getBlockData(blockId, noDirs) eq sentinel)
    verify(indexResolver).getBlockData(meq(blockId), meq(noDirs))
  }

  test("getBlockData delegates a non per-partition block id to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val sentinel = buf(9)
    val mergedId = ShuffleMergedBlockId(1, 0, 2)
    when(indexResolver.getBlockData(meq(mergedId), meq(noDirs))).thenReturn(sentinel)
    assert(resolver.getBlockData(mergedId, noDirs) eq sentinel)
    verify(indexResolver).getBlockData(meq(mergedId), meq(noDirs))
  }

  // --------------------------------------------------------------------------
  // ShuffleBlockResolver delegations.
  // --------------------------------------------------------------------------

  test("getMergedBlockData delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val mergedId = ShuffleMergedBlockId(1, 0, 2)
    val chunks: Seq[ManagedBuffer] = Seq(buf(1))
    when(indexResolver.getMergedBlockData(meq(mergedId), meq(noDirs))).thenReturn(chunks)
    assert(resolver.getMergedBlockData(mergedId, noDirs) === chunks)
    verify(indexResolver).getMergedBlockData(meq(mergedId), meq(noDirs))
  }

  test("getMergedBlockMeta delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val mergedId = ShuffleMergedBlockId(1, 0, 2)
    val meta = mock(classOf[MergedBlockMeta])
    when(indexResolver.getMergedBlockMeta(meq(mergedId), meq(noDirs))).thenReturn(meta)
    assert(resolver.getMergedBlockMeta(mergedId, noDirs) eq meta)
    verify(indexResolver).getMergedBlockMeta(meq(mergedId), meq(noDirs))
  }

  test("getBlocksForShuffle delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val blocks: Seq[BlockId] = Seq(ShuffleBlockId(1, 2L, 0), ShuffleBlockId(1, 2L, 1))
    when(indexResolver.getBlocksForShuffle(1, 2L)).thenReturn(blocks)
    assert(resolver.getBlocksForShuffle(1, 2L) === blocks)
    verify(indexResolver).getBlocksForShuffle(1, 2L)
  }

  // --------------------------------------------------------------------------
  // MigratableResolver delegations (preserve block migration / decommission unchanged).
  // --------------------------------------------------------------------------

  test("getStoredShuffles delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val stored = Seq(ShuffleBlockInfo(1, 2L), ShuffleBlockInfo(3, 4L))
    when(indexResolver.getStoredShuffles()).thenReturn(stored)
    assert(resolver.getStoredShuffles() === stored)
    verify(indexResolver).getStoredShuffles()
  }

  test("addShuffleToSkip delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    resolver.addShuffleToSkip(42)
    verify(indexResolver).addShuffleToSkip(42)
  }

  test("putShuffleBlockAsStream delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val blockId = ShuffleBlockId(1, 2L, 3)
    val serializerManager = mock(classOf[SerializerManager])
    val callback = mock(classOf[StreamCallbackWithID])
    when(indexResolver.putShuffleBlockAsStream(meq(blockId), meq(serializerManager)))
      .thenReturn(callback)
    assert(resolver.putShuffleBlockAsStream(blockId, serializerManager) eq callback)
    verify(indexResolver).putShuffleBlockAsStream(meq(blockId), meq(serializerManager))
  }

  test("getMigrationBlocks delegates to the index resolver") {
    val (resolver, indexResolver) = newResolver()
    val info = ShuffleBlockInfo(1, 2L)
    val migrated: List[(BlockId, ManagedBuffer)] = List(ShuffleBlockId(1, 2L, 0) -> buf(1))
    when(indexResolver.getMigrationBlocks(meq(info))).thenReturn(migrated)
    assert(resolver.getMigrationBlocks(info) === migrated)
    verify(indexResolver).getMigrationBlocks(meq(info))
  }

  // --------------------------------------------------------------------------
  // Lifecycle, accessor and metadata helper.
  // --------------------------------------------------------------------------

  test("indexResolver exposes the injected shared resolver") {
    val (resolver, indexResolver) = newResolver()
    assert(resolver.indexResolver eq indexResolver)
  }

  test("stop clears the streaming index but does not stop the shared index resolver") {
    val (resolver, indexResolver) = newResolver()
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 0))
    resolver.registerStreamingBlock(inMemoryBlock(1, 2L, 1))
    assert(resolver.numStreamingBlocks === 2)
    resolver.stop()
    assert(resolver.numStreamingBlocks === 0, "stop must clear the in-memory index")
    // The shared index resolver is owned by the inner SortShuffleManager, so the streaming
    // resolver must NOT stop it here.
    verify(indexResolver, never()).stop()
  }

  test("StreamingBlockMetadata.blockId yields the addressed ShuffleBlockId") {
    val md = inMemoryBlock(11, 22L, 33)
    assert(md.blockId === ShuffleBlockId(11, 22L, 33))
  }

  // --------------------------------------------------------------------------
  // Concurrency: the index is a lock-free, three-level ConcurrentHashMap structure.
  // --------------------------------------------------------------------------

  test("the streaming index is thread-safe under concurrent register and remove") {
    val (resolver, _) = newResolver()
    val numThreads = 8
    val blocksPerThread = 50
    val registerPool = Executors.newFixedThreadPool(numThreads)
    try {
      // Each thread owns a distinct mapId so the final count is deterministic (no key contention)
      // while still driving concurrent mutation of the shared shuffle-level map.
      (0 until numThreads).foreach { t =>
        registerPool.submit(new Runnable {
          override def run(): Unit =
            (0 until blocksPerThread).foreach(p => resolver.registerStreamingBlock(
              inMemoryBlock(1, t.toLong, p)))
        })
      }
      registerPool.shutdown()
      assert(registerPool.awaitTermination(60, TimeUnit.SECONDS), "register tasks must finish")
      assert(resolver.numStreamingBlocks === numThreads * blocksPerThread)
    } finally {
      registerPool.shutdownNow()
    }

    val removePool = Executors.newFixedThreadPool(numThreads)
    try {
      // Concurrently drop every map and assert the index fully empties (and prunes its levels).
      (0 until numThreads).foreach { t =>
        removePool.submit(new Runnable {
          override def run(): Unit = resolver.removeStreamingMap(1, t.toLong)
        })
      }
      removePool.shutdown()
      assert(removePool.awaitTermination(60, TimeUnit.SECONDS), "remove tasks must finish")
    } finally {
      removePool.shutdownNow()
    }
    assert(resolver.numStreamingBlocks === 0)
  }
}
