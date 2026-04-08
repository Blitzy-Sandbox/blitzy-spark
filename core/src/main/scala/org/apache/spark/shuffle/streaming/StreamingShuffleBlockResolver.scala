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
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage._

/**
 * Data for an in-progress streaming shuffle block.
 *
 * @param buffer    the byte buffer containing block data
 * @param checksum  CRC32C checksum of the data
 * @param size      size of the data in bytes
 * @param timestamp creation timestamp
 */
private[streaming] case class InProgressBlockData(
    buffer: ByteBuffer,
    checksum: Long,
    size: Long,
    timestamp: Long = System.currentTimeMillis())

/**
 * ShuffleBlockResolver implementation for streaming shuffle mode.
 *
 * Provides block resolution for streaming shuffle blocks, including both in-progress
 * blocks (held in memory) and spilled blocks (persisted to disk).
 *
 * == Block Types ==
 *
 * - In-progress blocks: Currently being streamed, held in memory
 * - Spilled blocks: Evicted from memory due to pressure, stored on disk
 *
 * == Coexistence with Sort-Based Shuffle ==
 *
 * This resolver handles StreamingShuffleBlockId blocks specifically. For other block
 * types (e.g., ShuffleBlockId, ShuffleDataBlockId from sort-based shuffle), it
 * delegates to the sort-based [[IndexShuffleBlockResolver]] for proper resolution.
 *
 * IMPORTANT: We cannot use blockManager.getLocalBlockData() for shuffle blocks because
 * that would create an infinite loop - the block manager calls back to the shuffle
 * manager's resolver, which is this class.
 *
 * == Thread Safety ==
 *
 * All operations are thread-safe via concurrent data structures.
 *
 * @param conf                 SparkConf containing configuration
 * @param _blockManager        optional explicit BlockManager (for testing)
 * @param _sortBlockResolver   optional explicit sort-based resolver (for testing)
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    var _blockManager: BlockManager = null,
    var _sortBlockResolver: ShuffleBlockResolver = null)
  extends ShuffleBlockResolver with Logging {

  // Lazily access block manager (not available at construction in some contexts)
  private lazy val blockManager: BlockManager =
    Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  // Lazily access the sort-based block resolver for fallback delegated resolution.
  // This MUST be accessed through the sort shuffle manager, NOT through the block manager
  // or current shuffle manager to avoid infinite delegation loops.
  private lazy val sortBlockResolver: ShuffleBlockResolver = {
    Option(_sortBlockResolver).getOrElse {
      // Create a dedicated IndexShuffleBlockResolver for sort-based shuffle blocks.
      // We cannot use SparkEnv.get.shuffleManager.shuffleBlockResolver because that
      // returns this StreamingShuffleBlockResolver, causing an infinite loop.
      import org.apache.spark.shuffle.sort.SortShuffleManager
      val sortManager = new SortShuffleManager(conf)
      sortManager.shuffleBlockResolver
    }
  }

  // In-progress blocks held in memory
  private val inProgressBlocks = new ConcurrentHashMap[StreamingShuffleBlockId, InProgressBlockData]()

  // Blocks that have been spilled to disk
  private val spilledBlocks = new ConcurrentHashMap[StreamingShuffleBlockId, File]()

  /**
   * Get block data for the specified block ID.
   *
   * This method handles multiple block ID types to support both streaming shuffle
   * and fallback to sort-based shuffle:
   *
   * - StreamingShuffleBlockId: Native streaming shuffle blocks (in-memory or spilled)
   * - ShuffleBlockId: Regular shuffle blocks (used by sort-based shuffle after fallback)
   * - ShuffleDataBlockId: Sort-based shuffle data blocks
   * - ShuffleIndexBlockId: Sort-based shuffle index blocks
   *
   * IMPORTANT: For sort-based shuffle blocks, we MUST delegate to the sort block resolver
   * directly, NOT through blockManager.getLocalBlockData(). The block manager calls back to
   * the shuffle manager's resolver (which is us), creating an infinite loop.
   *
   * @param blockId the block identifier
   * @param dirs    optional directories to search (unused for streaming blocks)
   * @return ManagedBuffer containing the block data
   * @throws IllegalArgumentException if block type is not supported
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]] = None): ManagedBuffer = {
    blockId match {
      case streamingId: StreamingShuffleBlockId =>
        getStreamingBlockData(streamingId)

      case shuffleId: ShuffleBlockId =>
        // First check if we have streaming data for this shuffle block.
        // Streaming shuffle stores data with StreamingShuffleBlockId which includes chunkIndex,
        // but the reader requests ShuffleBlockId. We need to look up all chunks for this
        // (shuffleId, mapId, reduceId) and combine them.
        val streamingData = getStreamingDataForShuffleBlock(shuffleId)
        if (streamingData.isDefined) {
          logDebug(s"Found streaming data for ShuffleBlockId $shuffleId")
          streamingData.get
        } else {
          // Delegate to sort-based resolver for regular shuffle blocks (used after fallback to sort)
          // This enables coexistence with sort-based shuffle when streaming falls back.
          // CRITICAL: Do NOT use blockManager.getLocalBlockData() - it creates an infinite loop!
          logDebug(s"Delegating ShuffleBlockId $shuffleId to sort block resolver (sort-based fallback)")
          sortBlockResolver.getBlockData(shuffleId, dirs)
        }

      case shuffleDataId: ShuffleDataBlockId =>
        // Delegate to sort-based resolver for shuffle data blocks
        logDebug(s"Delegating ShuffleDataBlockId $shuffleDataId to sort block resolver")
        sortBlockResolver.getBlockData(shuffleDataId, dirs)

      case shuffleIndexId: ShuffleIndexBlockId =>
        // Delegate to sort-based resolver for index blocks
        logDebug(s"Delegating ShuffleIndexBlockId $shuffleIndexId to sort block resolver")
        sortBlockResolver.getBlockData(shuffleIndexId, dirs)

      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected block ID type for streaming shuffle: $blockId")
    }
  }
  
  /**
   * Look up streaming data for a ShuffleBlockId by finding all matching StreamingShuffleBlockId chunks.
   * 
   * When the reader requests data via ShuffleBlockId (shuffleId, mapId, reduceId), we need to
   * find all streaming chunks that match and combine them into a single buffer.
   *
   * @param blockId the ShuffleBlockId to look up
   * @return Some(ManagedBuffer) if streaming data exists, None otherwise
   */
  private def getStreamingDataForShuffleBlock(blockId: ShuffleBlockId): Option[ManagedBuffer] = {
    // Find all streaming blocks that match this (shuffleId, mapId, reduceId)
    val matchingBlocks = inProgressBlocks.keySet().asScala.filter { streamingId =>
      streamingId.shuffleId == blockId.shuffleId &&
        streamingId.mapId == blockId.mapId &&
        streamingId.reduceId == blockId.reduceId
    }.toSeq.sortBy(_.chunkIndex)
    
    if (matchingBlocks.isEmpty) {
      // Also check spilled blocks
      val spilledMatches = spilledBlocks.keySet().asScala.filter { streamingId =>
        streamingId.shuffleId == blockId.shuffleId &&
          streamingId.mapId == blockId.mapId &&
          streamingId.reduceId == blockId.reduceId
      }.toSeq.sortBy(_.chunkIndex)
      
      if (spilledMatches.isEmpty) {
        None
      } else {
        // Combine spilled chunks
        val combined = spilledMatches.flatMap { streamingId =>
          Option(spilledBlocks.get(streamingId)).filter(_.exists()).map { file =>
            java.nio.file.Files.readAllBytes(file.toPath)
          }
        }
        if (combined.isEmpty) None
        else Some(new NioManagedBuffer(ByteBuffer.wrap(combined.flatten.toArray)))
      }
    } else {
      // Combine in-progress chunks
      val combined = matchingBlocks.flatMap { streamingId =>
        Option(inProgressBlocks.get(streamingId)).map { data =>
          val buf = data.buffer.duplicate()
          val arr = new Array[Byte](buf.remaining())
          buf.get(arr)
          arr
        }
      }
      if (combined.isEmpty) None
      else Some(new NioManagedBuffer(ByteBuffer.wrap(combined.flatten.toArray)))
    }
  }

  /**
   * Get merged block data for push-based shuffle compatibility.
   *
   * Streaming shuffle blocks are not merged in the same way as push-based shuffle,
   * so this returns an empty sequence.
   *
   * @param blockId the merged block identifier
   * @param dirs    optional directories
   * @return empty sequence (streaming blocks not merged)
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    // Streaming blocks are not merged like push-based shuffle
    logDebug(s"getMergedBlockData called for $blockId - streaming shuffle does not use merging")
    Seq.empty
  }

  /**
   * Get metadata for merged blocks.
   *
   * Streaming shuffle does not use merged blocks, so this returns empty metadata.
   *
   * @param blockId the merged block identifier
   * @param dirs    optional directories
   * @return MergedBlockMeta with 0 chunks
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    // Return empty metadata for streaming shuffle - use empty NioManagedBuffer
    val emptyBuffer = new NioManagedBuffer(ByteBuffer.allocate(0))
    new MergedBlockMeta(0, emptyBuffer)
  }

  /**
   * Register an in-progress block for tracking.
   *
   * Called by StreamingShuffleWriter when a buffer is ready for streaming.
   *
   * @param blockId  the streaming block identifier
   * @param data     the block data as byte array
   * @param checksum CRC32C checksum of the data
   */
  def registerInProgressBlock(
      blockId: StreamingShuffleBlockId,
      data: Array[Byte],
      checksum: Long): Unit = {
    val buffer = ByteBuffer.wrap(data)
    val blockData = InProgressBlockData(buffer, checksum, data.length)
    inProgressBlocks.put(blockId, blockData)
    logDebug(s"Registered in-progress block $blockId (${data.length} bytes)")
  }

  /**
   * Register an in-progress block with a ByteBuffer.
   *
   * @param blockId  the streaming block identifier
   * @param buffer   the block data as ByteBuffer
   * @param checksum CRC32C checksum of the data
   */
  def registerInProgressBlock(
      blockId: StreamingShuffleBlockId,
      buffer: ByteBuffer,
      checksum: Long): Unit = {
    val size = buffer.remaining()
    val blockData = InProgressBlockData(buffer.duplicate(), checksum, size)
    inProgressBlocks.put(blockId, blockData)
    logDebug(s"Registered in-progress block $blockId ($size bytes)")
  }

  /**
   * Mark a block as spilled to disk.
   *
   * Called when a block is evicted from memory due to memory pressure.
   *
   * @param blockId the streaming block identifier
   * @param file    the spill file location
   */
  def markBlockSpilled(blockId: StreamingShuffleBlockId, file: File): Unit = {
    inProgressBlocks.remove(blockId)
    spilledBlocks.put(blockId, file)
    logDebug(s"Marked block $blockId as spilled to ${file.getAbsolutePath}")
  }

  /**
   * Acknowledge receipt of a block by a consumer.
   *
   * Removes the block from tracking (memory can be reclaimed).
   * If spilled, optionally deletes the spill file.
   *
   * @param blockId    the streaming block identifier
   * @param deleteFile whether to delete spill file if exists
   */
  def acknowledgeBlock(blockId: StreamingShuffleBlockId, deleteFile: Boolean = true): Unit = {
    inProgressBlocks.remove(blockId)
    Option(spilledBlocks.remove(blockId)).foreach { file =>
      if (deleteFile && file.exists()) {
        file.delete()
        logDebug(s"Deleted spill file for acknowledged block $blockId")
      }
    }
    logDebug(s"Acknowledged block $blockId")
  }

  /**
   * Check if a block is available (in-memory or spilled).
   *
   * @param blockId the streaming block identifier
   * @return true if block is available
   */
  def isBlockAvailable(blockId: StreamingShuffleBlockId): Boolean = {
    inProgressBlocks.containsKey(blockId) ||
      (spilledBlocks.containsKey(blockId) && spilledBlocks.get(blockId).exists())
  }

  /**
   * Get the checksum for a block.
   *
   * @param blockId the streaming block identifier
   * @return checksum if block is in memory, None otherwise
   */
  def getBlockChecksum(blockId: StreamingShuffleBlockId): Option[Long] = {
    Option(inProgressBlocks.get(blockId)).map(_.checksum)
  }

  /**
   * Get all block IDs for a specific shuffle and map task.
   *
   * @param shuffleId the shuffle identifier
   * @param mapId     the map task identifier
   * @return sequence of block IDs
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val inProgress = inProgressBlocks.keySet().asScala
      .filter(b => b.shuffleId == shuffleId && b.mapId == mapId)
    val spilled = spilledBlocks.keySet().asScala
      .filter(b => b.shuffleId == shuffleId && b.mapId == mapId)
    (inProgress ++ spilled).toSeq
  }

  /**
   * Remove all data for a specific shuffle and map task.
   *
   * @param shuffleId the shuffle identifier
   * @param mapId     the map task identifier
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    // Remove in-progress blocks
    val inProgressToRemove = inProgressBlocks.keySet().asScala
      .filter(b => b.shuffleId == shuffleId && b.mapId == mapId)
    inProgressToRemove.foreach(inProgressBlocks.remove)

    // Remove and delete spilled blocks
    val spilledToRemove = spilledBlocks.keySet().asScala
      .filter(b => b.shuffleId == shuffleId && b.mapId == mapId)
    spilledToRemove.foreach { blockId =>
      Option(spilledBlocks.remove(blockId)).foreach { file =>
        if (file.exists()) file.delete()
      }
    }

    logDebug(s"Removed ${inProgressToRemove.size} in-progress and ${spilledToRemove.size} " +
      s"spilled blocks for shuffle $shuffleId, map $mapId")
  }

  /**
   * Get statistics about tracked blocks.
   *
   * @return tuple of (in-progress count, spilled count, total bytes in memory)
   */
  def getStats: (Int, Int, Long) = {
    val inProgressCount = inProgressBlocks.size()
    val spilledCount = spilledBlocks.size()
    val totalBytes = inProgressBlocks.values().asScala.map(_.size).sum
    (inProgressCount, spilledCount, totalBytes)
  }

  /**
   * Stop the block resolver and clean up resources.
   */
  override def stop(): Unit = {
    // Clear in-progress blocks
    inProgressBlocks.clear()

    // Delete all spill files
    spilledBlocks.values().asScala.foreach { file =>
      if (file.exists()) file.delete()
    }
    spilledBlocks.clear()

    logInfo("StreamingShuffleBlockResolver stopped")
  }

  /**
   * Get data for a streaming shuffle block.
   */
  private def getStreamingBlockData(blockId: StreamingShuffleBlockId): ManagedBuffer = {
    // Check in-memory first
    Option(inProgressBlocks.get(blockId)).map { data =>
      new NioManagedBuffer(data.buffer.duplicate())
    }.orElse {
      // Check spilled to disk - read file directly
      Option(spilledBlocks.get(blockId)).filter(_.exists()).map { file =>
        val bytes = java.nio.file.Files.readAllBytes(file.toPath)
        new NioManagedBuffer(ByteBuffer.wrap(bytes))
      }
    }.getOrElse {
      throw new IllegalStateException(s"Block $blockId not found in streaming resolver")
    }
  }
}

/**
 * Companion object for StreamingShuffleBlockResolver.
 */
private[spark] object StreamingShuffleBlockResolver {

  /**
   * Create a StreamingShuffleBlockResolver with default configuration.
   *
   * @param conf SparkConf
   * @return new resolver instance
   */
  def apply(conf: SparkConf): StreamingShuffleBlockResolver = {
    new StreamingShuffleBlockResolver(conf)
  }
}
