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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleBlockInfo,
  ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleMergedBlockId}

/**
 * A [[ShuffleBlockResolver]] for the opt-in streaming shuffle backend that serves a reduce-side
 * fetch from the freshest available copy of a partition's bytes.
 *
 * ==Lookup order==
 *
 * For a per-partition [[org.apache.spark.storage.ShuffleBlockId]] the resolver consults, in order:
 *
 *  1. the in-memory streaming buffer, if the producing map task's bytes are still buffered and
 *     have not yet been spilled -- served with zero disk I/O, which is precisely what lets the
 *     reduce side read map output before it is ever materialized to disk;
 *  1. a disk spill, if the memory spill manager has drained the buffer to the
 *     [[org.apache.spark.storage.BlockManager]] under the sort-compatible on-disk format;
 *  1. the sort-based on-disk files, served by the inner [[IndexShuffleBlockResolver]] (the case
 *     when a fallback to sort-based shuffle, or a previous run, produced the block).
 *
 * Any non per-partition block id (batch ids, merged/push-based ids) has no in-memory streaming
 * representation and is resolved straight from the inner [[IndexShuffleBlockResolver]].
 *
 * ==Delegation and coexistence==
 *
 * This resolver owns no on-disk format of its own: `.data`/`.index` resolution, merged-shuffle
 * reads, and all block-migration concerns are delegated to an inner [[IndexShuffleBlockResolver]].
 * Delegation is the least-modification path -- it preserves the existing storage interface
 * contracts and the decommission/migration behavior for free, and keeps the streaming backend a
 * transparent peer of the sort-based path rather than a replacement for it.
 *
 * ==Tracking maps==
 *
 * Two concurrent maps, keyed by `(shuffleId, mapId, reduceId)`, record which partitions the
 * streaming backend can serve from memory or from a spill. They are populated by the streaming
 * writer ([[trackBuffer]]) and the memory spill manager ([[trackSpill]]), and purged per shuffle
 * by the manager's `unregisterShuffle` ([[untrackShuffle]]). They are what let [[getBlockData]]
 * serve still-in-memory streamed blocks before they ever hit disk.
 *
 * ==Thread-safety==
 *
 * Block-serving threads call [[getBlockData]] concurrently with the producing writer thread
 * ([[trackBuffer]]) and the spill manager's poll loop ([[trackSpill]] / [[untrackShuffle]]). All
 * shared state lives in [[java.util.concurrent.ConcurrentHashMap]]s, so every tracking and lookup
 * operation is individually thread-safe and lock-free for readers.
 *
 * @param conf          the [[SparkConf]] for this executor; read once to derive the streaming
 *                      configuration (used here only to gate verbose debug logging)
 * @param indexResolver the inner sort-based resolver every non-streaming concern is delegated to
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    indexResolver: IndexShuffleBlockResolver)
  extends ShuffleBlockResolver with MigratableResolver with Logging {

  import StreamingShuffleBlockResolver.BlockKey

  /**
   * Convenience constructor that owns a fresh inner [[IndexShuffleBlockResolver]]. Used when no
   * resolver is injected by `StreamingShuffleManager`; the inner resolver defers its
   * `BlockManager` lookup until first use, so this is safe to construct in local mode.
   *
   * @param conf the [[SparkConf]] for this executor
   */
  def this(conf: SparkConf) = this(conf, new IndexShuffleBlockResolver(conf))

  /** Typed view of the streaming config; used here only to gate verbose debug logging. */
  private val streamingConf = new StreamingShuffleConfig(conf)

  /**
   * In-memory, not-yet-spilled partition buffers, keyed by `(shuffleId, mapId, reduceId)`. An
   * entry is present only while the producing map task's bytes are buffered in memory; the spill
   * manager removes it (via [[trackSpill]]) once those bytes are drained to disk.
   */
  private val buffers = new ConcurrentHashMap[BlockKey, StreamingBuffer]()

  /**
   * Partitions whose bytes have been spilled to disk, keyed by `(shuffleId, mapId, reduceId)` and
   * mapped to the [[org.apache.spark.storage.BlockManager]] [[BlockId]] under which they were
   * stored. Populated by the memory spill manager through [[trackSpill]].
   */
  private val spilledBlocks = new ConcurrentHashMap[BlockKey, BlockId]()

  // == Tracking API (called by the streaming writer, the spill manager, and the manager) ==

  /**
   * Tracks an in-memory per-partition buffer so [[getBlockData]] can serve it directly, before it
   * is ever spilled. Called by the streaming writer as it buffers map output. Re-tracking the same
   * key replaces the previous buffer reference.
   *
   * @param buffer the per-partition buffer; its `(shuffleId, mapId, partitionId)` forms the key
   */
  def trackBuffer(buffer: StreamingBuffer): Unit = {
    buffers.put(BlockKey(buffer.shuffleId, buffer.mapId, buffer.partitionId), buffer)
  }

  /**
   * Records that a partition's bytes were spilled to disk under `diskBlockId`, and drops the now
   * redundant in-memory tracking entry so the [[getBlockData]] lookup falls through to the disk
   * path. Called by the memory spill manager after it drains a buffer through the `BlockManager`.
   *
   * @param shuffleId   the shuffle the spilled partition belongs to
   * @param mapId       the map task that produced the spilled bytes
   * @param reduceId    the reduce partition the spilled bytes are destined for
   * @param diskBlockId the [[BlockId]] under which the bytes were stored in the `BlockManager`
   */
  def trackSpill(shuffleId: Int, mapId: Long, reduceId: Int, diskBlockId: BlockId): Unit = {
    val key = BlockKey(shuffleId, mapId, reduceId)
    buffers.remove(key)
    spilledBlocks.put(key, diskBlockId)
  }

  /**
   * Removes every in-memory and spilled tracking entry for `shuffleId`. Called by the manager's
   * `unregisterShuffle` so the maps do not retain entries for shuffles Spark has discarded.
   *
   * @param shuffleId the shuffle whose tracking entries should be purged
   */
  def untrackShuffle(shuffleId: Int): Unit = {
    removeShuffleKeys(buffers.keySet().iterator(), shuffleId)
    removeShuffleKeys(spilledBlocks.keySet().iterator(), shuffleId)
  }

  /** Removes, in place, every key of `it` whose shuffleId matches, via the view iterator. */
  private def removeShuffleKeys(it: java.util.Iterator[BlockKey], shuffleId: Int): Unit = {
    while (it.hasNext) {
      if (it.next().shuffleId == shuffleId) {
        it.remove()
      }
    }
  }

  /** The number of partitions currently tracked as in-memory buffers. */
  private[streaming] def trackedBufferCount: Int = buffers.size()

  /** The number of partitions currently tracked as spilled to disk. */
  private[streaming] def spilledBlockCount: Int = spilledBlocks.size()

  // == ShuffleBlockResolver ==

  /**
   * Resolves the data for `blockId` following the documented lookup order. A per-partition
   * [[ShuffleBlockId]] is resolved against the streaming tracking maps first and falls back to the
   * inner resolver; every other block id is delegated unchanged.
   *
   * @param blockId the logical shuffle block to resolve
   * @param dirs    optional override of the local directories to read from; honored by the
   *                inner resolver on the disk paths
   * @return a [[ManagedBuffer]] over the requested block's bytes
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = blockId match {
    case ShuffleBlockId(shuffleId, mapId, reduceId) =>
      resolveTrackedBlock(BlockKey(shuffleId, mapId, reduceId), blockId, dirs)
    case _ =>
      // Batch ids, merged ids, and any other non per-partition block id have no in-memory
      // streaming representation, so resolve them straight from the inner resolver.
      indexResolver.getBlockData(blockId, dirs)
  }

  /** Applies the three-step in-memory -> spilled -> delegated lookup order for a tracked key. */
  private def resolveTrackedBlock(
      key: BlockKey,
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val buffer = buffers.get(key)
    if (buffer != null) {
      // Step 1 -- still in memory: serve the buffered bytes with no disk I/O. This streaming fast
      // path lets a reduce-side fetch read map output before it is ever materialized to disk.
      if (streamingConf.debug) {
        logDebug(s"Serving in-memory streaming block $blockId (${buffer.size} bytes)")
      }
      new NioManagedBuffer(ByteBuffer.wrap(buffer.toByteArray))
    } else if (spilledBlocks.containsKey(key)) {
      // Step 2 -- spilled to disk: the bytes were written through the BlockManager in the
      // sort-compatible on-disk format, so the inner resolver reads them as a sort-based block.
      if (streamingConf.debug) {
        logDebug(s"Serving spilled streaming block $blockId via IndexShuffleBlockResolver")
      }
      indexResolver.getBlockData(blockId, dirs)
    } else {
      // Step 3 -- untracked: a sort-based fallback (or a previous run) produced this block, so
      // delegate unconditionally. Coexistence with the sort-based path is transparent.
      indexResolver.getBlockData(blockId, dirs)
    }
  }

  /**
   * Returns the [[BlockId]]s of `shuffleId`/`mapId` for external shuffle-service cleanup.
   * Delegated to the inner resolver, which owns the on-disk files (streaming buffers are
   * in-memory and need no external cleanup).
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    indexResolver.getBlocksForShuffle(shuffleId, mapId)
  }

  /**
   * Resolves a merged (push-based) shuffle block as chunks. Push-based shuffle is not a streaming
   * concern, so this preserves existing behavior by delegating to the inner resolver.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    indexResolver.getMergedBlockData(blockId, dirs)
  }

  /**
   * Resolves the metadata for a merged (push-based) shuffle block. Delegated to the inner resolver
   * for the same reason as [[getMergedBlockData]].
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    indexResolver.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Clears the in-memory and spilled tracking maps, then stops the inner resolver. The buffers map
   * holds only references; the actual on-disk and `BlockManager` cleanup is the inner resolver's
   * responsibility.
   */
  override def stop(): Unit = {
    buffers.clear()
    spilledBlocks.clear()
    indexResolver.stop()
  }

  // == MigratableResolver ==
  //
  // Block migration / decommission is delegated wholesale to the inner IndexShuffleBlockResolver.
  // Pure delegation is the simplest correct v1: only the sort-compatible on-disk spill is
  // migratable, and that data already lives under the inner resolver, so migrating it for free
  // preserves decommission behavior without duplicating any of it here.

  /** Delegates: returns the shuffle ids stored locally on disk by the inner resolver. */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = indexResolver.getStoredShuffles()

  /** Delegates: marks a shuffle as one the inner resolver should not migrate. */
  override def addShuffleToSkip(shuffleId: Int): Unit = indexResolver.addShuffleToSkip(shuffleId)

  /** Delegates: accepts an incoming migrated shuffle block as a stream via the inner resolver. */
  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    indexResolver.putShuffleBlockAsStream(blockId, serializerManager)
  }

  /** Delegates: returns the index/data blocks the inner resolver will migrate for a shuffle map. */
  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    indexResolver.getMigrationBlocks(shuffleBlockInfo)
  }
}

private[spark] object StreamingShuffleBlockResolver {

  /**
   * Composite key identifying one streamed partition's bytes by the producing `(shuffleId, mapId)`
   * and the destination reduce `reduceId`. Used to key the in-memory and spilled tracking maps.
   *
   * @param shuffleId the shuffle the bytes belong to
   * @param mapId     the map task that produced the bytes
   * @param reduceId  the reduce partition the bytes are destined for
   */
  private[streaming] final case class BlockKey(shuffleId: Int, mapId: Long, reduceId: Int)
}
