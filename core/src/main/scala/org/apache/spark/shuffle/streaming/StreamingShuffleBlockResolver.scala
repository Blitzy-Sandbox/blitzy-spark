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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleMergedBlockId}

/**
 * A [[ShuffleBlockResolver]] for the streaming shuffle path (feature F-105).
 *
 * The resolver has two clearly separated responsibilities:
 *
 *  1. '''Streaming block index.''' It maintains an in-memory, three-level index of the streaming
 *     shuffle blocks currently being produced/consumed on this executor. The index is keyed
 *     `shuffleId -> mapId -> partitionId` and yields a `StreamingBlockMetadata` value describing
 *     the block's byte range, CRC32C checksum and current location (in memory or spilled to
 *     disk). The streaming writer (F-103) registers blocks as their per-partition buffers become
 *     available, the streaming reader (F-104) looks them up while streaming, and the memory spill
 *     manager (F-109) flips a block to the spilled state when it reclaims heap. All three
 *     collaborators mutate the index exclusively through the register / lookup / mark-spilled /
 *     remove operations exposed below, all of which are thread-safe.
 *
 *  2. '''Migration by delegation.''' It PRESERVES the existing block-migration and decommission
 *     behaviour by composing an [[IndexShuffleBlockResolver]] and forwarding every
 *     [[MigratableResolver]] method to it verbatim. The streaming subsystem intentionally
 *     introduces no new migration semantics, so decommission / migration continues to behave
 *     exactly as it does for the sort-based shuffle. This delegation is the explicit
 *     zero-regression guarantee of the streaming feature and must not be reimplemented.
 *
 * Resolution order for [[getBlockData]] is: serve an in-memory streaming block directly when one
 * is registered for the requested [[ShuffleBlockId]]; otherwise (unknown block, spilled block, or
 * any non per-partition block id) delegate to the composed [[IndexShuffleBlockResolver]], which
 * reads the materialized sort-based index/data files.
 *
 * The class is `private[spark]` and therefore introduces no new public binary-compatible API.
 *
 * @param conf the active [[SparkConf]]; forwarded to the composed [[IndexShuffleBlockResolver]].
 */
private[spark] class StreamingShuffleBlockResolver(conf: SparkConf)
  extends ShuffleBlockResolver with Logging with MigratableResolver {

  import StreamingShuffleBlockResolver._

  // Internal type aliases for the nested concurrent index, keeping signatures readable.
  private type PartitionIndex = ConcurrentHashMap[Int, StreamingBlockMetadata]
  private type MapIndex = ConcurrentHashMap[Long, PartitionIndex]

  /**
   * Delegate used for block migration / decommission and for resolving any materialized
   * (sort-based) shuffle block. Constructed eagerly; its `BlockManager` dependency is resolved
   * lazily on first use, so building it here is safe even before `SparkEnv` is fully initialized.
   */
  private val indexResolver = new IndexShuffleBlockResolver(conf)

  /**
   * Three-level streaming block index: `shuffleId -> (mapId -> (partitionId -> metadata))`.
   *
   * Every level is a [[ConcurrentHashMap]] so concurrent map (writer) and reduce (reader) tasks
   * in the same executor can register, look up and evict blocks without external locking. Inner
   * maps are created on demand and pruned once they become empty to bound memory usage.
   */
  private val blockIndex = new ConcurrentHashMap[Int, MapIndex]()

  // ---------------------------------------------------------------------------------------------
  // Streaming block index operations (used by the streaming writer / reader / spill manager).
  // ---------------------------------------------------------------------------------------------

  /**
   * Register (or replace) the metadata for a single streaming shuffle block. Invoked by the
   * writer when a per-partition buffer becomes available for streaming and by the spill manager
   * when a block's location changes. Intermediate index levels are created on demand.
   */
  def registerStreamingBlock(metadata: StreamingBlockMetadata): Unit = {
    val mapLevel = blockIndex.computeIfAbsent(metadata.shuffleId, _ => new MapIndex())
    val partitionLevel = mapLevel.computeIfAbsent(metadata.mapId, _ => new PartitionIndex())
    partitionLevel.put(metadata.partitionId, metadata)
    logDebug(s"Registered streaming block ${metadata.blockId} " +
      s"(${metadata.length} bytes, location=${metadata.location})")
  }

  /**
   * Look up the metadata for a single streaming block, returning `None` when the block is unknown
   * to this resolver (for example, because it was produced by the sort-based path or has already
   * been reclaimed).
   */
  def getStreamingBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int): Option[StreamingBlockMetadata] = {
    for {
      mapLevel <- Option(blockIndex.get(shuffleId))
      partitionLevel <- Option(mapLevel.get(mapId))
      metadata <- Option(partitionLevel.get(partitionId))
    } yield metadata
  }

  /**
   * Return the metadata for every registered partition of a given map output, in ascending
   * partition order. Used by the reader to enumerate the in-progress blocks of a producer.
   */
  def getStreamingBlocksForMap(shuffleId: Int, mapId: Long): Seq[StreamingBlockMetadata] = {
    val blocks = for {
      mapLevel <- Option(blockIndex.get(shuffleId))
      partitionLevel <- Option(mapLevel.get(mapId))
    } yield partitionLevel.values().asScala.toSeq.sortBy(_.partitionId)
    blocks.getOrElse(Seq.empty)
  }

  /** Returns `true` if a streaming block is currently registered for the given coordinates. */
  def containsStreamingBlock(shuffleId: Int, mapId: Long, partitionId: Int): Boolean = {
    getStreamingBlock(shuffleId, mapId, partitionId).isDefined
  }

  /**
   * Atomically transition a registered block to the spilled state and drop its in-memory payload
   * reference, allowing the spill manager to reclaim the heap within its SLA. Returns `true` when
   * a matching block was found and updated, `false` otherwise.
   */
  def markBlockSpilled(shuffleId: Int, mapId: Long, partitionId: Int): Boolean = {
    var updated = false
    val partitionLevelOpt = for {
      mapLevel <- Option(blockIndex.get(shuffleId))
      partitionLevel <- Option(mapLevel.get(mapId))
    } yield partitionLevel
    partitionLevelOpt.foreach { partitionLevel =>
      partitionLevel.computeIfPresent(partitionId, (_, metadata: StreamingBlockMetadata) => {
        updated = true
        metadata.copy(location = Spilled, data = None)
      })
    }
    if (updated) {
      logDebug(s"Marked streaming block shuffle=$shuffleId map=$mapId part=$partitionId spilled")
    }
    updated
  }

  /**
   * Remove a single streaming block from the index, pruning now-empty intermediate levels
   * atomically. Returns the removed metadata, or `None` when no such block was registered.
   */
  def removeStreamingBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int): Option[StreamingBlockMetadata] = {
    var removed: Option[StreamingBlockMetadata] = None
    blockIndex.computeIfPresent(shuffleId, (_, mapLevel: MapIndex) => {
      mapLevel.computeIfPresent(mapId, (_, partitionLevel: PartitionIndex) => {
        removed = Option(partitionLevel.remove(partitionId))
        if (partitionLevel.isEmpty) null else partitionLevel
      })
      if (mapLevel.isEmpty) null else mapLevel
    })
    if (removed.isDefined) {
      logDebug(s"Removed streaming block shuffle=$shuffleId map=$mapId part=$partitionId")
    }
    removed
  }

  /**
   * Remove all streaming blocks produced by a single map task, pruning the shuffle entry when it
   * becomes empty.
   */
  def removeStreamingMap(shuffleId: Int, mapId: Long): Unit = {
    var pruned = false
    blockIndex.computeIfPresent(shuffleId, (_, mapLevel: MapIndex) => {
      if (mapLevel.remove(mapId) != null) {
        pruned = true
      }
      if (mapLevel.isEmpty) null else mapLevel
    })
    if (pruned) {
      logDebug(s"Removed streaming map output shuffle=$shuffleId map=$mapId")
    }
  }

  /** Remove every streaming block belonging to a shuffle. Called when a shuffle is dropped. */
  def removeStreamingShuffle(shuffleId: Int): Unit = {
    if (blockIndex.remove(shuffleId) != null) {
      logDebug(s"Removed all streaming blocks for shuffle=$shuffleId")
    }
  }

  /** Total number of streaming blocks tracked across all shuffles. For metrics and tests. */
  def numStreamingBlocks: Int = {
    blockIndex.values().asScala.iterator.map { mapLevel =>
      mapLevel.values().asScala.iterator.map(_.size()).sum
    }.sum
  }

  // ---------------------------------------------------------------------------------------------
  // ShuffleBlockResolver contract.
  // ---------------------------------------------------------------------------------------------

  /**
   * Resolve the data for a block. A per-partition [[ShuffleBlockId]] that is registered in the
   * streaming index and still resident in memory is served directly from its buffered payload.
   * Every other block id - unknown, already spilled/reclaimed, or a non per-partition id such
   * as a batch id - is delegated to the composed [[IndexShuffleBlockResolver]].
   */
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        getStreamingBlock(shuffleId, mapId, reduceId) match {
          case Some(metadata) if metadata.location == InMemory && metadata.data.isDefined =>
            logTrace(s"Resolved streaming block $blockId from the in-memory index")
            metadata.data.get
          case _ =>
            indexResolver.getBlockData(blockId, dirs)
        }
      case _ =>
        indexResolver.getBlockData(blockId, dirs)
    }
  }

  /**
   * Streaming shuffle does not introduce merged-shuffle (push-based) semantics in v1, so merged
   * block data reads are delegated to the composed [[IndexShuffleBlockResolver]].
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    indexResolver.getMergedBlockData(blockId, dirs)
  }

  /**
   * Streaming shuffle does not introduce merged-shuffle (push-based) semantics in v1, so merged
   * block meta reads are delegated to the composed [[IndexShuffleBlockResolver]].
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    indexResolver.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Delegate to the composed [[IndexShuffleBlockResolver]] so external-shuffle-service cleanup of
   * materialized blocks keeps working unchanged.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    indexResolver.getBlocksForShuffle(shuffleId, mapId)
  }

  // ---------------------------------------------------------------------------------------------
  // MigratableResolver contract - delegated verbatim to preserve migration/decommission.
  // ---------------------------------------------------------------------------------------------

  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = indexResolver.getStoredShuffles()

  override def addShuffleToSkip(shuffleId: Int): Unit = indexResolver.addShuffleToSkip(shuffleId)

  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    indexResolver.putShuffleBlockAsStream(blockId, serializerManager)
  }

  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    indexResolver.getMigrationBlocks(shuffleBlockInfo)
  }

  // ---------------------------------------------------------------------------------------------
  // Lifecycle.
  // ---------------------------------------------------------------------------------------------

  /** Stop the composed delegate and release all streaming-index state. */
  override def stop(): Unit = {
    val cleared = numStreamingBlocks
    blockIndex.clear()
    indexResolver.stop()
    logInfo(s"Stopped StreamingShuffleBlockResolver; cleared $cleared streaming block(s)")
  }
}

private[spark] object StreamingShuffleBlockResolver {

  /** Physical location of a streaming shuffle block's payload. */
  sealed trait BlockLocation

  /** The block's bytes are held in an on-heap streaming buffer and can be served directly. */
  case object InMemory extends BlockLocation

  /** The block has been spilled to disk by the memory spill manager (via the BlockManager). */
  case object Spilled extends BlockLocation

  /**
   * Metadata describing a single streaming shuffle block tracked by the resolver index.
   *
   * @param shuffleId   owning shuffle id
   * @param mapId       producing map task id
   * @param partitionId reduce partition id this block belongs to
   * @param offset      byte offset of this block within the map's logical output
   * @param length      length of the block payload in bytes
   * @param checksum    CRC32C checksum of the payload (0 when checksums are disabled)
   * @param location    whether the payload is currently in memory or spilled to disk
   * @param data        the in-memory payload, present only while `location == InMemory`
   */
  case class StreamingBlockMetadata(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      offset: Long,
      length: Long,
      checksum: Long,
      location: BlockLocation,
      data: Option[ManagedBuffer] = None) {

    /** The logical [[ShuffleBlockId]] addressed by this metadata. */
    def blockId: ShuffleBlockId = ShuffleBlockId(shuffleId, mapId, partitionId)
  }
}
