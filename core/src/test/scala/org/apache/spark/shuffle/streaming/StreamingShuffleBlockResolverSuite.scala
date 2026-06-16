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

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, eq => meq}
import org.mockito.Mockito.{mock, never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.{BlockId, BlockManager, ByteBufferBlockData, ShuffleBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * End-to-end spill-retrieval tests for [[StreamingShuffleBlockResolver]], the CP2 review's
 * highest-risk integration gap (Area-of-Concern #5: missing spill-retrieval and end-to-end
 * spilled-read coverage).
 *
 * The resolver and a real [[MemorySpillManager]] share one instance (wired via `setSpillManager`)
 * so spill registration, the persisted (non-shuffle `TempLocalBlockId`) format, and the read path
 * are one atomic design. A map-backed [[BlockManager]] captures `putBytes` bytes and serves them
 * back through `getLocalBytes`, exercising a spill-then-read round trip without touching real disk.
 *
 * The tests prove: (1) a spilled partition is served through the resolver and the index resolver is
 * never consulted; (2) a partition that spilled and then re-buffered serves the spilled segments in
 * order followed by the live in-memory frames, byte-for-byte; (3) a block with no streaming buffer
 * is delegated to the index resolver (the sort-based `.data`/`.index` owner).
 */
class StreamingShuffleBlockResolverSuite extends SparkFunSuite with Matchers {

  /**
   * Builds a resolver wired to a real spill manager over a map-backed BlockManager. The Memory
   * manager is mocked only to supply the spill denominator; `spillBuffer` is invoked directly so
   * no poll thread is started.
   *
   * @return the resolver, its shared spill manager, the backing block store, and the mocked index
   *         resolver (so a test can assert it is or is not consulted)
   */
  private def newResolverWithSpill(): (
      StreamingShuffleBlockResolver,
      MemorySpillManager,
      ConcurrentHashMap[BlockId, Array[Byte]],
      IndexShuffleBlockResolver) = {
    val conf = new SparkConf(false)
    val cfg = new StreamingShuffleConfig(conf)
    val store = new ConcurrentHashMap[BlockId, Array[Byte]]()
    val bm = mock(classOf[BlockManager])
    when(bm.putBytes(any(), any(), any(), any())(any())).thenAnswer { (inv: InvocationOnMock) =>
      store.put(inv.getArgument[BlockId](0), inv.getArgument[ChunkedByteBuffer](1).toArray)
      true
    }
    when(bm.getLocalBytes(any())).thenAnswer { (inv: InvocationOnMock) =>
      Option(store.get(inv.getArgument[BlockId](0)))
        .map(a => new ByteBufferBlockData(new ChunkedByteBuffer(ByteBuffer.wrap(a)), false))
    }
    val mm = mock(classOf[MemoryManager])
    when(mm.maxOnHeapStorageMemory).thenReturn(100L * 1024 * 1024)
    val spillManager = new MemorySpillManager(cfg, bm, mm, new StreamingShuffleMetrics)
    val indexResolver = mock(classOf[IndexShuffleBlockResolver])
    val resolver = new StreamingShuffleBlockResolver(conf, indexResolver)
    resolver.setSpillManager(spillManager)
    (resolver, spillManager, store, indexResolver)
  }

  /** Reads a managed buffer fully into a byte array. */
  private def toArray(buf: ManagedBuffer): Array[Byte] = {
    val bb = buf.nioByteBuffer()
    val out = new Array[Byte](bb.remaining())
    bb.get(out)
    out
  }

  /** Deterministic payload of the given length. */
  private def payload(n: Int): Array[Byte] = Array.tabulate(n)(i => (i % 127).toByte)

  test("a spilled partition is served through the resolver, never via the index resolver") {
    val (resolver, spillManager, _, indexResolver) = newResolverWithSpill()
    val buffer = new StreamingBuffer(0, 0L, 0, 1L * 1024 * 1024)
    buffer.append(payload(1000))
    val expected = buffer.toByteArray // canonical enveloped frames, captured before the spill
    spillManager.register(buffer)
    resolver.trackBuffer(buffer)

    // Spill clears the in-memory buffer; the bytes must remain retrievable through the resolver.
    assert(spillManager.spillBuffer(MemorySpillManager.BufferKey(0, 0L, 0)))
    val served = toArray(resolver.getBlockData(ShuffleBlockId(0, 0L, 0), None))

    assert(served.sameElements(expected))
    // Raw spill blocks have no .index/.data files, so the index resolver must NOT be consulted.
    verify(indexResolver, never()).getBlockData(any(), any())
  }

  test("a re-buffered partition serves spilled bytes before its in-memory bytes") {
    val (resolver, spillManager, _, indexResolver) = newResolverWithSpill()
    val buffer = new StreamingBuffer(0, 0L, 0, 4L * 1024 * 1024)
    buffer.append(payload(1500))
    val spilledFrames = buffer.toByteArray // these frames go to disk
    spillManager.register(buffer)
    resolver.trackBuffer(buffer)
    assert(spillManager.spillBuffer(MemorySpillManager.BufferKey(0, 0L, 0)))

    // After the spill clears the buffer, new records re-buffer in memory for the same partition.
    buffer.append(payload(900))
    val inMemoryFrames = buffer.toByteArray

    val served = toArray(resolver.getBlockData(ShuffleBlockId(0, 0L, 0), None))

    // Deterministic order and byte-identity: spilled segments first, then the live in-memory tail.
    assert(served.sameElements(spilledFrames ++ inMemoryFrames))
    verify(indexResolver, never()).getBlockData(any(), any())
  }

  test("a non-tracked block delegates to the index resolver") {
    val (resolver, _, _, indexResolver) = newResolverWithSpill()
    val sentinel: ManagedBuffer = new NioManagedBuffer(ByteBuffer.wrap(payload(16)))
    when(indexResolver.getBlockData(any(), any())).thenReturn(sentinel)

    // No streaming buffer was tracked for this id, so resolution falls through to the index
    // resolver that owns the sort-based .data/.index format.
    val result = resolver.getBlockData(ShuffleBlockId(99, 0L, 0), None)

    assert(result eq sentinel)
    verify(indexResolver).getBlockData(ShuffleBlockId(99, 0L, 0), None)
  }

  test("commitDurableMapOutput writes the canonical enveloped bytes and returns enveloped " +
      "lengths matching the in-memory serve path") {
    val (resolver, spillManager, _, indexResolver) = newResolverWithSpill()
    val shuffleId = 7
    val mapId = 3L
    // Partition 0 spills then re-buffers (so durable assembly must concatenate spilled + live);
    // partition 1 stays purely in memory; partition 2 is never written (a zero-length hole).
    val buf0 = new StreamingBuffer(shuffleId, mapId, 0, 4L * 1024 * 1024)
    buf0.append(payload(1500))
    val buf0Spilled = buf0.toByteArray
    spillManager.register(buf0)
    resolver.trackBuffer(buf0)
    assert(spillManager.spillBuffer(MemorySpillManager.BufferKey(shuffleId, mapId, 0)))
    buf0.append(payload(700))
    val buf0InMemory = buf0.toByteArray
    val expected0 = buf0Spilled ++ buf0InMemory

    val buf1 = new StreamingBuffer(shuffleId, mapId, 1, 4L * 1024 * 1024)
    buf1.append(payload(900))
    val expected1 = buf1.toByteArray
    spillManager.register(buf1)
    resolver.trackBuffer(buf1)

    // The canonical durable bytes a fetch would read while the producer is alive (the in-memory
    // serve path) -- the durable file must be byte-identical to these.
    val live0 = toArray(resolver.getBlockData(ShuffleBlockId(shuffleId, mapId, 0), None))
    val live1 = toArray(resolver.getBlockData(ShuffleBlockId(shuffleId, mapId, 1), None))
    assert(live0.sameElements(expected0))
    assert(live1.sameElements(expected1))

    // Route the durable commit to real temp files; the mocked writeMetadataFileAndCommit is a
    // no-op so the data temp survives for inspection (the real delegate renames it into place).
    val tmpDir = Utils.createTempDir()
    val dataFile = new File(tmpDir, s"shuffle_${shuffleId}_${mapId}_0.data")
    val dataTmp = new File(tmpDir, s"shuffle_${shuffleId}_${mapId}_0.data.tmp")
    when(indexResolver.getDataFile(shuffleId, mapId)).thenReturn(dataFile)
    when(indexResolver.createTempFile(dataFile)).thenReturn(dataTmp)

    val lengths = resolver.commitDurableMapOutput(shuffleId, mapId, 3)

    // Returned lengths are the enveloped frame sizes -- exactly the bytes the live path served --
    // and the never-written partition is a zero-length hole.
    assert(lengths.length === 3)
    assert(lengths(0) === expected0.length.toLong)
    assert(lengths(1) === expected1.length.toLong)
    assert(lengths(2) === 0L)
    // The durable .data temp holds the partitions concatenated in reduce-id order, byte-for-byte
    // identical to the in-memory serve path: durable and live fetches return the same bytes.
    val written = Files.readAllBytes(dataTmp.toPath)
    assert(written.sameElements(expected0 ++ expected1))
    // The commit is delegated to the inner index resolver with the enveloped lengths, no separate
    // checksum file (CRC32C travels inside each envelope), and this map's temp data file.
    verify(indexResolver).writeMetadataFileAndCommit(
      meq(shuffleId), meq(mapId), meq(lengths), meq(Array.empty[Long]), meq(dataTmp))
    verify(indexResolver, never()).getBlockData(any(), any())
  }

  test("commitDurableMapOutput on a map with no tracked buffers writes all-zero lengths") {
    val (resolver, _, _, indexResolver) = newResolverWithSpill()
    val tmpDir = Utils.createTempDir()
    val dataFile = new File(tmpDir, "shuffle_1_0_0.data")
    val dataTmp = new File(tmpDir, "shuffle_1_0_0.data.tmp")
    when(indexResolver.getDataFile(anyInt(), anyLong())).thenReturn(dataFile)
    when(indexResolver.createTempFile(dataFile)).thenReturn(dataTmp)

    val lengths = resolver.commitDurableMapOutput(1, 0L, 4)

    assert(lengths.toSeq === Seq(0L, 0L, 0L, 0L))
    assert(Files.readAllBytes(dataTmp.toPath).isEmpty)
    verify(indexResolver).writeMetadataFileAndCommit(
      meq(1), meq(0L), meq(lengths), meq(Array.empty[Long]), meq(dataTmp))
  }
}
