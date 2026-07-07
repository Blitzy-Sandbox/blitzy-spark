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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId, ShuffleMergedBlockId}
import org.apache.spark.util.collection.OpenHashSet

/**
 * A [[ShuffleBlockResolver]] for the streaming shuffle that maps a logical shuffle block to either
 * an in-memory [[StreamingBuffer]] or a spilled on-disk `File`, while preserving Spark's existing
 * block-migration behavior by composition.
 *
 * ==Three-level block map==
 * Streaming-shuffle output is addressed by the same triple the sort path uses -- shuffle id, map
 * (producer) id, and reduce (consumer) partition id -- so this resolver tracks live blocks in a
 * three-level map:
 * {{{
 *   shuffleId (Int) -> mapId (Long) -> reduceId (Int) -> ref (StreamingBuffer | java.io.File)
 * }}}
 * A [[StreamingBuffer]] value means the block is still resident in executor memory; a
 * `java.io.File` value means the [[MemorySpillManager]] already spilled the block to local disk to
 * relieve memory pressure. Every level is a `ConcurrentHashMap`, so the write path (the streaming
 * writer registering produced blocks) and the read path ([[getBlockData]] serving consumers) stay
 * lock-free and can run concurrently with the spill manager rewriting a leaf value from a buffer to
 * a file.
 *
 * ==Coexistence strategy (zero regression)==
 * This resolver holds an inner [[IndexShuffleBlockResolver]] -- the exact resolver the sort path
 * uses -- constructed with the same `(conf, blockManager, taskIdMapsForShuffle)` triple that
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] passes to its own resolver. All
 * decommission/migration operations delegate to the sort-path `IndexShuffleBlockResolver` to
 * preserve existing behavior; the streaming resolver never reimplements block migration. Sharing
 * the identical `taskIdMapsForShuffle` instance keeps migration bookkeeping consistent across the
 * two managers, exactly mirroring how `SortShuffleManager` shares that map with its resolver.
 *
 * Concretely, delegation to the inner resolver covers:
 *  - the four [[MigratableResolver]] operations used during executor decommissioning, so shuffle
 *    blocks continue to migrate off a decommissioning executor with no change to that subsystem;
 *  - any block not tracked in the streaming map (for example a batch-fetched range, or a block
 *    produced through the sort fallback), which is served by the sort path unchanged;
 *  - merged/push-based block reads ([[getMergedBlockData]] / [[getMergedBlockMeta]]), which are out
 *    of scope for streaming v1 and therefore handled entirely by the sort resolver.
 *
 * ==Thread-safety==
 * The block map is fully concurrent. [[transportConf]] is built lazily so construction stays cheap
 * and safe in local/test mode where a full transport stack may be unnecessary; it is only needed to
 * serve spilled, file-backed blocks.
 *
 * @param conf                 the active [[SparkConf]]
 * @param _blockManager        the executor [[BlockManager]] (may be `null` in local/test mode, in
 *                             which case the inner resolver resolves it lazily from `SparkEnv`)
 * @param taskIdMapsForShuffle shuffle-id to producer task-ids map, shared with the owning
 *                             `StreamingShuffleManager` and forwarded verbatim to the inner
 *                             sort-path resolver so migration bookkeeping stays consistent
 */
@Since("4.2.0")
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager,
    taskIdMapsForShuffle: ConcurrentMap[Int, OpenHashSet[Long]])
  extends ShuffleBlockResolver
  with Logging
  with MigratableResolver {

  // COEXISTENCE STRATEGY: the inner sort-path resolver is the delegation target for BOTH the four
  // MigratableResolver operations AND for any block that streaming does not track in its in-memory
  // / spilled map (including merged/push-based blocks and non-ShuffleBlockId ids). It is built with
  // the SAME (conf, blockManager, taskIdMapsForShuffle) triple the SortShuffleManager passes to its
  // own resolver, so decommission/migration behavior is preserved byte-for-byte.
  private val index =
    new IndexShuffleBlockResolver(conf, _blockManager, taskIdMapsForShuffle)

  // Three-level in-memory / spilled block map:
  //   shuffleId (Int) -> mapId (Long) -> reduceId (Int) -> ref (StreamingBuffer | java.io.File)
  // ConcurrentHashMap at every level keeps put/get/remove lock-free for the write and read paths.
  private val blockMap =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Long, ConcurrentHashMap[Int, AnyRef]]]()

  // Lazily built so construction stays cheap and safe in local/test mode. Used only to serve
  // spilled, file-backed blocks through a zero-copy FileSegmentManagedBuffer, mirroring how the
  // sort path serves its shuffle data files.
  private lazy val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  logInfo("StreamingShuffleBlockResolver initialized; decommission/migration operations delegate " +
    "to the sort-path IndexShuffleBlockResolver to preserve existing behavior.")

  // ==========================================================================================
  // Streaming block-map helpers. Package-private so the streaming writer (which registers produced
  // blocks), the MemorySpillManager (which rewrites a buffer reference to a spilled file), and the
  // manager (which drops a shuffle's blocks) can update the map. These are the only mutators of the
  // in-memory tracking state; migration and unknown-block reads never touch it.
  // ==========================================================================================

  /**
   * Register (or replace) the reference tracked for a single streaming shuffle block. `ref` is a
   * [[StreamingBuffer]] while the block is memory-resident, or a `java.io.File` once the block has
   * been spilled to local disk.
   *
   * @param shuffleId the shuffle the block belongs to
   * @param mapId     the producing map task id
   * @param reduceId  the consuming reduce partition id
   * @param ref       the in-memory buffer or spilled file backing the block
   */
  private[streaming] def putBlock(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      ref: AnyRef): Unit = {
    blockMap
      .computeIfAbsent(shuffleId,
        _ => new ConcurrentHashMap[Long, ConcurrentHashMap[Int, AnyRef]]())
      .computeIfAbsent(mapId, _ => new ConcurrentHashMap[Int, AnyRef]())
      .put(reduceId, ref)
  }

  /**
   * Look up the reference tracked for a single streaming shuffle block, if any.
   *
   * @return `Some(ref)` where `ref` is a [[StreamingBuffer]] or a `java.io.File`, or `None` if the
   *         block is not tracked by the streaming path (and should therefore be served by the inner
   *         sort resolver).
   */
  private[streaming] def getBlock(shuffleId: Int, mapId: Long, reduceId: Int): Option[AnyRef] = {
    Option(blockMap.get(shuffleId))
      .flatMap(mapLevel => Option(mapLevel.get(mapId)))
      .flatMap(reduceLevel => Option(reduceLevel.get(reduceId)))
  }

  /**
   * Drop all streaming block references tracked for a shuffle. Called when the shuffle is
   * unregistered so the in-memory tracking state does not leak. Note this only clears streaming's
   * own map; the sort resolver owns the lifecycle of any spilled on-disk files.
   *
   * @param shuffleId the shuffle whose tracked blocks should be removed
   */
  private[streaming] def removeShuffle(shuffleId: Int): Unit = {
    Option(blockMap.remove(shuffleId)).foreach { removed =>
      logDebug(s"Removed streaming block tracking for shuffle $shuffleId " +
        s"(${removed.size()} map outputs).")
    }
  }

  // ==========================================================================================
  // ShuffleBlockResolver
  // ==========================================================================================

  /**
   * Retrieve the data for the specified block.
   *
   * If the block is tracked by the streaming path, it is served directly from memory (over a
   * defensive [[StreamingBuffer.snapshot]]) or from its spilled file. Any block not tracked by the
   * streaming path -- including batch-fetched ranges and blocks produced through the sort fallback
   * -- is delegated to the inner sort-path resolver unchanged.
   *
   * The `dirs` default (`None`) is inherited from [[ShuffleBlockResolver]] rather than redeclared
   * here; Scala forbids an override from redefining a default argument value.
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        getBlock(shuffleId, mapId, reduceId) match {
          case Some(buffer: StreamingBuffer) =>
            // Memory-resident block: wrap a defensive copy of the buffered bytes. snapshot() also
            // refreshes the buffer's LRU access time so the spill manager treats a served block as
            // recently used.
            new NioManagedBuffer(ByteBuffer.wrap(buffer.snapshot()))
          case Some(file: File) =>
            // Spilled block: serve directly from disk without materializing the whole file on heap.
            new FileSegmentManagedBuffer(transportConf, file, 0L, file.length())
          case _ =>
            // Not tracked by streaming (missing/unknown marker): let the sort path serve it.
            // This keeps the sort fallback fully functional.
            index.getBlockData(blockId, dirs)
        }
      case _ =>
        // Non-ShuffleBlockId (e.g. a batch-fetched range id): the sort resolver knows how to serve
        // it and applies its own id parsing and validation.
        index.getBlockData(blockId, dirs)
    }
  }

  /**
   * Retrieve the list of [[BlockId]]s for a given shuffle map. Delegated to the sort-path resolver
   * so external-shuffle-service cleanup after executor removal continues to behave identically to
   * the sort path.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    index.getBlocksForShuffle(shuffleId, mapId)
  }

  /**
   * Retrieve the data for the specified merged shuffle block as multiple chunks. Push-based
   * (merged) shuffle is out of scope for streaming v1, so this delegates to the sort-path resolver.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    index.getMergedBlockData(blockId, dirs)
  }

  /**
   * Retrieve the meta data for the specified merged shuffle block. Push-based (merged) shuffle is
   * out of scope for streaming v1, so this delegates to the sort-path resolver.
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    index.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Shut down this resolver. Clears streaming's in-memory tracking map first (releasing references
   * to any still-resident buffers so they become eligible for GC), then stops the inner sort-path
   * resolver so its lifecycle is honored.
   */
  override def stop(): Unit = {
    blockMap.clear()
    index.stop()
  }

  // ==========================================================================================
  // MigratableResolver -- delegation only.
  //
  // The delegation below is precisely what preserves decommission migration: block migration is NOT
  // reimplemented for the streaming path; it is forwarded to the same IndexShuffleBlockResolver the
  // sort path uses, which migrates shuffle blocks off a decommissioning executor exactly as before.
  // ==========================================================================================

  /** Get the shuffle ids stored locally. Delegated to the sort-path resolver. */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = index.getStoredShuffles()

  /** Mark a shuffle that should not be migrated. Delegated to the sort-path resolver. */
  override def addShuffleToSkip(shuffleId: Int): Unit = index.addShuffleToSkip(shuffleId)

  /** Write a provided shuffle block as a stream. Delegated to the sort-path resolver. */
  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    index.putShuffleBlockAsStream(blockId, serializerManager)
  }

  /** Get the blocks for migration for a shuffle and map. Delegated to the sort-path resolver. */
  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    index.getMigrationBlocks(shuffleBlockInfo)
  }
}
