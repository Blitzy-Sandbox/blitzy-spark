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
 * types (e.g., ShuffleDataBlockId from sort-based shuffle), it provides minimal
 * compatibility by delegating to the standard block manager.
 *
 * == Thread Safety ==
 *
 * All operations are thread-safe via concurrent data structures.
 *
 * @param conf          SparkConf containing configuration
 * @param _blockManager optional explicit BlockManager (for testing)
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    var _blockManager: BlockManager = null)
  extends ShuffleBlockResolver with Logging {

  // Lazily access block manager (not available at construction in some contexts)
  private lazy val blockManager: BlockManager =
    Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  // In-progress blocks held in memory
  private val inProgressBlocks = new ConcurrentHashMap[StreamingShuffleBlockId, InProgressBlockData]()

  // Blocks that have been spilled to disk
  private val spilledBlocks = new ConcurrentHashMap[StreamingShuffleBlockId, File]()

  /**
   * Get block data for the specified block ID.
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

      case shuffleDataId: ShuffleDataBlockId =>
        // Fallback to standard shuffle block resolution for sort-based shuffle blocks
        getSpilledShuffleBlockData(shuffleDataId, dirs)

      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected block ID type for streaming shuffle: $blockId")
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

  /**
   * Get data for a sort-based shuffle block (fallback path).
   */
  private def getSpilledShuffleBlockData(
      blockId: ShuffleDataBlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    // Delegate to block manager for sort-based shuffle blocks
    blockManager.getLocalBlockData(blockId)
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
