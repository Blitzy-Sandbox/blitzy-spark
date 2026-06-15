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
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleMergedBlockId}

/**
 * Resolves shuffle block data for the opt-in streaming shuffle backend, serving
 * still-in-memory streamed partitions directly while delegating every on-disk concern to an
 * inner [[org.apache.spark.shuffle.IndexShuffleBlockResolver]].
 *
 * ==Why delegation==
 * Delegating `.data`/`.index` resolution, merged (push-based) shuffle, and all block-migration
 * concerns to an inner [[IndexShuffleBlockResolver]] is the least-modification path (AAP 0.4.2):
 * it reuses the battle-tested on-disk shuffle format and the existing decommission/migration
 * machinery unchanged, so the streaming backend inherits correct behavior for those paths for
 * free and never touches the block-manager storage interface contracts. The streaming resolver
 * adds exactly one capability on top: it can serve a reduce partition's bytes straight from the
 * producer's in-memory [[StreamingBuffer]] before those bytes are ever written to disk.
 *
 * ==Tracking maps==
 * Two concurrent maps, both keyed by the logical `(shuffleId, mapId, reduceId)` triple, record
 * where a streamed partition currently lives:
 *  - [[buffers]] holds partitions that are still resident in a producer-side
 *    [[StreamingBuffer]]; the map-side writer registers them via [[trackBuffer]].
 *  - [[spilledBlocks]] records partitions that the `MemorySpillManager` has evicted to disk
 *    via [[trackSpill]], mapping the logical key to the on-disk [[BlockId]] the index resolver
 *    knows how to read.
 * In v1 a partition is spilled wholesale (the spill manager spills the largest buffers and then
 * clears them), so the in-memory and spilled states are mutually exclusive for a given key;
 * [[trackSpill]] enforces this by dropping the buffer entry as it records the disk location.
 *
 * ==getBlockData lookup order==
 * For a [[ShuffleBlockId]] the resolver checks, in order: (1) the in-memory [[buffers]] map and,
 * if present, wraps the buffer's bytes in a [[NioManagedBuffer]] without ever touching disk;
 * (2) the [[spilledBlocks]] map and, if present, delegates to the index resolver using the
 * recorded on-disk block id; (3) otherwise delegates the original block id to the index
 * resolver (this covers output produced by the sort-based fallback path). Any non-shuffle or
 * batched block id shape is delegated to the index resolver unchanged.
 *
 * ==Concurrency==
 * Reduce-side fetches call [[getBlockData]] concurrently with the map-side writer and the spill
 * manager mutating the tracking maps. Both maps are [[ConcurrentHashMap]]s, so reads observe a
 * consistent per-key snapshot without external locking, and [[untrackShuffle]] uses each map's
 * weakly-consistent iterator to purge a shuffle without blocking concurrent lookups.
 *
 * This type coexists with the sort-based shuffle path and is constructed only when the streaming
 * backend is active; when streaming is disabled the manager never routes resolution here.
 *
 * @param conf the [[org.apache.spark.SparkConf]] used by the auxiliary constructor to build the
 *             default inner [[IndexShuffleBlockResolver]]; retained on the signature for API
 *             symmetry with the delegate so a manager can construct either resolver uniformly
 * @param indexResolver the inner resolver that owns the on-disk `.data`/`.index` representation
 *                      and all migration behavior
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    indexResolver: IndexShuffleBlockResolver)
  extends ShuffleBlockResolver
  with MigratableResolver
  with Logging {

  import StreamingShuffleBlockResolver.BlockKey

  /**
   * Convenience constructor that builds a private inner [[IndexShuffleBlockResolver]] from the
   * given configuration. Prefer the primary constructor when the `StreamingShuffleManager`
   * already holds a shared index resolver, so the streaming and fallback paths resolve `.data`/
   * `.index` blocks through the very same delegate.
   */
  def this(conf: SparkConf) = this(conf, new IndexShuffleBlockResolver(conf))

  // Partitions still resident in a producer-side StreamingBuffer, keyed by the logical
  // (shuffleId, mapId, reduceId) triple. Registered by the writer through `trackBuffer`.
  private val buffers = new ConcurrentHashMap[BlockKey, StreamingBuffer]()

  // Partitions the MemorySpillManager has evicted to disk, mapping the logical key to the
  // on-disk BlockId the index resolver can read. Populated through `trackSpill`.
  private val spilledBlocks = new ConcurrentHashMap[BlockKey, BlockId]()

  // ---------------------------------------------------------------------------------------
  // Tracking API consumed by the writer, the spill manager, and the manager's
  // `unregisterShuffle`. These are the hooks that let `getBlockData` serve still-in-memory
  // streamed blocks before (and after) they reach disk.
  // ---------------------------------------------------------------------------------------

  /**
   * Registers a producer-side [[StreamingBuffer]] so its bytes can be served in-memory by
   * [[getBlockData]]. The logical key is derived from the buffer's own `shuffleId`, `mapId`,
   * and `partitionId`, so the writer never has to restate it. Idempotent: re-tracking the same
   * key replaces the previous buffer reference.
   *
   * @param buffer the per-partition buffer holding the map-side output for one reduce partition
   */
  def trackBuffer(buffer: StreamingBuffer): Unit = {
    val key = BlockKey(buffer.shuffleId, buffer.mapId, buffer.partitionId)
    buffers.put(key, buffer)
    logTrace(s"Tracking in-memory streaming buffer for $key (${buffer.size} bytes)")
  }

  /**
   * Records that a partition has been spilled to disk under the given [[BlockId]] and removes
   * its now-stale in-memory buffer entry, so subsequent [[getBlockData]] calls resolve the
   * partition through the inner [[IndexShuffleBlockResolver]] rather than from a cleared buffer.
   *
   * @param shuffleId the shuffle id of the spilled partition
   * @param mapId the map id that produced the spilled partition
   * @param reduceId the reduce partition id that was spilled
   * @param diskBlockId the on-disk block id under which the bytes were persisted
   */
  def trackSpill(shuffleId: Int, mapId: Long, reduceId: Int, diskBlockId: BlockId): Unit = {
    val key = BlockKey(shuffleId, mapId, reduceId)
    spilledBlocks.put(key, diskBlockId)
    buffers.remove(key)
    logDebug(s"Tracked spill for $key -> $diskBlockId")
  }

  /**
   * Drops every tracking entry for the given shuffle from both maps. Invoked by
   * `StreamingShuffleManager.unregisterShuffle` so a completed or cleaned-up shuffle no longer
   * retains buffer references or spilled-block records. Uses each map's weakly-consistent
   * iterator, so concurrent [[getBlockData]] lookups are never blocked.
   *
   * @param shuffleId the shuffle whose tracked blocks should be removed
   */
  def untrackShuffle(shuffleId: Int): Unit = {
    removeShuffleEntries(buffers, shuffleId)
    removeShuffleEntries(spilledBlocks, shuffleId)
    logDebug(s"Untracked all streaming blocks for shuffle $shuffleId")
  }

  // ---------------------------------------------------------------------------------------
  // ShuffleBlockResolver
  // ---------------------------------------------------------------------------------------

  /**
   * Retrieves the data for the requested block, preferring still-in-memory streamed bytes and
   * otherwise delegating to the inner [[IndexShuffleBlockResolver]].
   *
   * The lookup order for a [[ShuffleBlockId]] is in-memory buffer, then spilled-to-disk record,
   * then plain delegation (see the class-level documentation). Wrapping the in-memory bytes in a
   * [[NioManagedBuffer]] is sound because [[StreamingBuffer]] guarantees a dual-channel
   * wire/persist invariant: the materialized array is byte-for-byte identical to the data the
   * spill and re-stream paths would produce.
   *
   * Note: the `dirs` parameter intentionally carries no default here. The default
   * (`None`) is declared once on the [[ShuffleBlockResolver]] trait; Scala forbids an
   * overriding method from restating it.
   *
   * @param blockId the logical shuffle block to resolve
   * @param dirs optional explicit local directories to read from, forwarded to the delegate
   * @return a managed buffer over the block's bytes
   */
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        val key = BlockKey(shuffleId, mapId, reduceId)
        val buffer = buffers.get(key)
        if (buffer != null) {
          // (1) Still in memory: serve directly, honoring StreamingBuffer's dual-channel
          // wire/persist invariant so the bytes match the spilled/re-streamed representation.
          logTrace(s"Serving in-memory streaming block $blockId (${buffer.size} bytes)")
          new NioManagedBuffer(ByteBuffer.wrap(buffer.toByteArray))
        } else {
          val spilled = spilledBlocks.get(key)
          if (spilled != null) {
            // (2) Spilled to disk: delegate to the index resolver, which owns the on-disk
            // .data/.index representation, using the recorded on-disk block id.
            logTrace(s"Serving spilled streaming block $blockId via $spilled")
            indexResolver.getBlockData(spilled, dirs)
          } else {
            // (3) Not streaming-tracked (e.g. produced by the sort-based fallback path):
            // delegate the original block id to the index resolver.
            indexResolver.getBlockData(blockId, dirs)
          }
        }
      case _ =>
        // Batched or other block-id shapes are resolved entirely by the index resolver.
        indexResolver.getBlockData(blockId, dirs)
    }
  }

  /**
   * Retrieves the [[BlockId]]s for a given shuffle map. Delegated to the inner resolver so any
   * on-disk (spilled or fallback) shuffle files are reported for external-shuffle-service
   * cleanup; purely in-memory streamed partitions have no files and are a no-op for cleanup.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    indexResolver.getBlocksForShuffle(shuffleId, mapId)
  }

  /**
   * Retrieves a merged (push-based) shuffle block as multiple chunks. Merged shuffle is not a
   * streaming concern, so this delegates unchanged to the inner resolver to preserve behavior.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    indexResolver.getMergedBlockData(blockId, dirs)
  }

  /**
   * Retrieves the metadata for a merged (push-based) shuffle block. Delegated unchanged to the
   * inner resolver for the same reason as [[getMergedBlockData]].
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    indexResolver.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Releases all tracking state and stops the delegate. Clears both tracking maps first so no
   * buffer references or spilled-block records survive teardown, then stops the inner
   * [[IndexShuffleBlockResolver]].
   */
  override def stop(): Unit = {
    buffers.clear()
    spilledBlocks.clear()
    indexResolver.stop()
  }

  // ---------------------------------------------------------------------------------------
  // MigratableResolver -- pure delegation to the inner IndexShuffleBlockResolver so the
  // streaming backend inherits Spark's existing block-migration / decommission behavior
  // unchanged (AAP 0.4.2: "delegating migration concerns to IndexShuffleBlockResolver").
  // ---------------------------------------------------------------------------------------

  /**
   * Returns the shuffle ids stored locally for migration. Pure delegation: the inner resolver
   * enumerates on-disk shuffle index files, which is the authoritative source for migratable
   * blocks. (Purely in-memory streamed partitions are transient and not migration candidates.)
   */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    indexResolver.getStoredShuffles()
  }

  /**
   * Marks a shuffle that should not be migrated. Delegated to the inner resolver so skip state
   * is honored by the same machinery that enumerates and serves migration blocks.
   */
  override def addShuffleToSkip(shuffleId: Int): Unit = {
    indexResolver.addShuffleToSkip(shuffleId)
  }

  /**
   * Accepts a shuffle block written as a stream during migration. Delegated to the inner
   * resolver, which persists the incoming bytes into the standard `.data`/`.index` files.
   */
  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    indexResolver.putShuffleBlockAsStream(blockId, serializerManager)
  }

  /**
   * Returns the index and data blocks for migrating a particular shuffle map. Delegated to the
   * inner resolver, which reads them from the on-disk consolidated files.
   */
  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    indexResolver.getMigrationBlocks(shuffleBlockInfo)
  }

  // ---------------------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------------------

  // Removes every entry whose key belongs to the given shuffle from the supplied concurrent
  // map. Generic over the value type so it serves both the buffer and spilled-block maps. The
  // ConcurrentHashMap entry iterator is weakly consistent and supports in-place removal, so
  // this never blocks concurrent `getBlockData` lookups.
  private def removeShuffleEntries[V](
      map: ConcurrentHashMap[BlockKey, V],
      shuffleId: Int): Unit = {
    val it = map.entrySet().iterator()
    while (it.hasNext) {
      if (it.next().getKey.shuffleId == shuffleId) {
        it.remove()
      }
    }
  }
}

/**
 * Companion holding the compound tracking key used by [[StreamingShuffleBlockResolver]].
 */
private[spark] object StreamingShuffleBlockResolver {

  /**
   * Logical identity of a single streamed reduce partition: the `(shuffleId, mapId, reduceId)`
   * triple. A case class gives correct structural `equals`/`hashCode` for use as a
   * [[java.util.concurrent.ConcurrentHashMap]] key.
   *
   * @param shuffleId the shuffle id
   * @param mapId the map (producer) task id
   * @param reduceId the reduce partition id
   */
  private final case class BlockKey(shuffleId: Int, mapId: Long, reduceId: Int)
}
