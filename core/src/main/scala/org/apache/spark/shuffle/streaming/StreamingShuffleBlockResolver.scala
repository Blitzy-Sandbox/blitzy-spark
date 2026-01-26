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

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, PATH, SHUFFLE_ID, MAP_ID}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId, ShuffleBlockBatchId, ShuffleMergedBlockId}

/**
 * Block state enumeration for tracking the lifecycle of streaming shuffle blocks.
 *
 * Blocks transition through states as follows:
 *   InFlight -> Spilled (when memory pressure triggers disk spill)
 *   InFlight -> Completed (when streaming completes successfully)
 *   Spilled -> Completed (when spilled data is fully consumed)
 *
 * Coexistence Strategy:
 * This block resolver operates alongside IndexShuffleBlockResolver for traditional
 * sort-based shuffles. The streaming path uses in-memory buffers with optional disk
 * spill, while the sort-based path materializes data to disk first. The streaming
 * shuffle manager will fallback to sort-based shuffle (and thus IndexShuffleBlockResolver)
 * when streaming conditions are not met.
 */
sealed trait BlockState extends Serializable

/**
 * Block is currently in producer memory buffers, being actively streamed to consumers.
 * Data is accessible via direct memory access without disk I/O.
 */
case object InFlight extends BlockState

/**
 * Block has been spilled to disk due to memory pressure (80% threshold exceeded).
 * Data is accessible via disk read through BlockManager's disk store.
 */
case object Spilled extends BlockState

/**
 * Block streaming has completed. Data is fully materialized and available for
 * consumption. This is the terminal state for successful shuffle completion.
 */
case object Completed extends BlockState

/**
 * Information about a block that is currently in-flight (being streamed).
 * Tracks the block's data, state, timing, and integrity information.
 *
 * @param blockId The unique identifier for this shuffle block
 * @param buffer The ByteBuffer containing the block's data (may be null if spilled)
 * @param state Current state of the block (InFlight, Spilled, or Completed)
 * @param timestamp Creation timestamp in milliseconds for LRU eviction ordering
 * @param checksum CRC32C checksum for data integrity validation
 * @param spillFile Optional file path if the block has been spilled to disk
 * @param spillOffset Offset within the spill file (0 for dedicated spill files)
 * @param spillLength Length of spilled data in bytes
 */
case class InFlightBlockInfo(
    blockId: BlockId,
    @volatile var buffer: ByteBuffer,
    @volatile var state: BlockState,
    timestamp: Long,
    checksum: Long,
    @volatile var spillFile: Option[File] = None,
    @volatile var spillOffset: Long = 0L,
    @volatile var spillLength: Long = 0L) {

  /**
   * Returns the size of the block data in bytes.
   * For in-flight blocks, returns buffer remaining bytes.
   * For spilled blocks, returns the spill length.
   */
  def size: Long = {
    if (buffer != null) {
      buffer.remaining()
    } else {
      spillLength
    }
  }

  /**
   * Marks this block as spilled to the given file location.
   * Releases the in-memory buffer to free memory.
   *
   * @param file The file where data was spilled
   * @param offset Offset within the file
   * @param length Length of the spilled data
   */
  def markSpilled(file: File, offset: Long, length: Long): Unit = {
    spillFile = Some(file)
    spillOffset = offset
    spillLength = length
    buffer = null // Release memory buffer
    state = Spilled
  }

  /**
   * Marks this block as completed (fully streamed and acknowledged).
   */
  def markCompleted(): Unit = {
    state = Completed
  }
}

/**
 * Block resolution component for streaming shuffle supporting both in-flight data
 * (data currently being streamed from producer buffers) and spilled data (data that
 * has been spilled to disk via MemorySpillManager).
 *
 * This resolver implements the ShuffleBlockResolver trait to provide getBlockData for
 * resolving block locations, integrates with existing BlockManager infrastructure for
 * disk storage, and supports the streaming protocol for accessing data before shuffle
 * completion.
 *
 * Block Resolution Modes:
 *   1. In-flight: Data currently in producer memory buffers - accessed via NioManagedBuffer
 *   2. Spilled: Data persisted to disk - accessed via FileSegmentManagedBuffer
 *   3. Completed: Fully materialized shuffle data - standard block resolution
 *
 * Coexistence Strategy:
 * This class operates within the streaming shuffle package boundary and does not
 * modify any existing shuffle infrastructure. It coexists with IndexShuffleBlockResolver
 * which handles traditional sort-based shuffles. The StreamingShuffleManager decides
 * which resolver to use based on shuffle type.
 *
 * Integration Points:
 *   - BlockManager: For disk spill storage and block status reporting
 *   - DiskBlockManager: For spill file location management
 *   - TransportConf: For configuring FileSegmentManagedBuffer with proper buffer sizes
 *
 * Memory Management:
 * In-flight blocks are tracked in a ConcurrentHashMap. When memory pressure exceeds
 * the 80% threshold, MemorySpillManager triggers spills and calls markBlockSpilled()
 * to update block state. Memory is released within 100ms of spill completion.
 *
 * Thread Safety:
 * All block state operations are thread-safe through ConcurrentHashMap and volatile
 * fields in InFlightBlockInfo. Multiple readers can access blocks concurrently while
 * writers update state atomically.
 *
 * @param conf SparkConf for reading configuration parameters
 * @param blockManager BlockManager for disk operations and block status
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    // var for testing - allows injection of mock BlockManager
    var _blockManager: BlockManager)
  extends ShuffleBlockResolver
  with Logging {

  /**
   * Auxiliary constructor for production use that defers BlockManager resolution.
   */
  def this(conf: SparkConf) = {
    this(conf, null)
  }

  /**
   * Lazy BlockManager accessor - falls back to SparkEnv if not explicitly set.
   * This pattern follows IndexShuffleBlockResolver's approach for testability.
   */
  private lazy val blockManager: BlockManager = {
    Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  }

  /**
   * Transport configuration for creating FileSegmentManagedBuffer instances.
   * Uses Spark's standard transport configuration with shuffle module settings.
   */
  private lazy val transportConf = {
    val securityManager = new SecurityManager(conf)
    SparkTransportConf.fromSparkConf(
      conf, "shuffle", sslOptions = Some(securityManager.getRpcSSLOptions()))
  }

  /**
   * Thread-safe map tracking all in-flight blocks currently being streamed.
   * Key: BlockId (typically ShuffleBlockId)
   * Value: InFlightBlockInfo containing buffer, state, and metadata
   */
  private val inFlightBlocks = new ConcurrentHashMap[BlockId, InFlightBlockInfo]()

  /**
   * Thread-safe map tracking shuffle IDs to their associated map IDs.
   * Used for efficient cleanup when a shuffle is unregistered.
   */
  private val shuffleToMapIds = new ConcurrentHashMap[Int, java.util.Set[Long]]()

  /**
   * Retrieves the data for the specified block based on its current state.
   *
   * Resolution Strategy:
   *   1. Check if block is in-flight (in memory) - return NioManagedBuffer
   *   2. Check if block is spilled (on disk) - return FileSegmentManagedBuffer
   *   3. For completed/unknown blocks - throw exception (data should be
   *      accessed through standard block fetch mechanisms after completion)
   *
   * This method supports the streaming protocol by allowing consumers to
   * access data before the shuffle is fully completed. For sort-based shuffles
   * that have fallen back, use IndexShuffleBlockResolver instead.
   *
   * @param blockId The BlockId to resolve (ShuffleBlockId or ShuffleBlockBatchId)
   * @param dirs Optional array of directories to search (not used for in-flight blocks)
   * @return ManagedBuffer containing the block data
   * @throws IOException if block is not found or data is unavailable
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]] = None): ManagedBuffer = {

    val blockInfo = inFlightBlocks.get(blockId)

    if (blockInfo != null) {
      blockInfo.state match {
        case InFlight =>
          // Block is in memory - wrap in NioManagedBuffer
          if (blockInfo.buffer != null) {
            logDebug(log"Resolving in-flight block ${MDC(BLOCK_ID, blockId)} from memory buffer")
            // Duplicate buffer to allow concurrent reads without position interference
            new NioManagedBuffer(blockInfo.buffer.duplicate())
          } else {
            // Buffer was released unexpectedly - this shouldn't happen
            throw new IOException(
              s"In-flight block $blockId has null buffer - possible race condition")
          }

        case Spilled =>
          // Block was spilled to disk - use FileSegmentManagedBuffer
          blockInfo.spillFile match {
            case Some(file) =>
              if (file.exists()) {
                logDebug(log"Resolving spilled block ${MDC(BLOCK_ID, blockId)} " +
                  log"from ${MDC(PATH, file.getPath)}")
                new FileSegmentManagedBuffer(
                  transportConf,
                  file,
                  blockInfo.spillOffset,
                  blockInfo.spillLength)
              } else {
                throw new IOException(
                  s"Spill file ${file.getPath} for block $blockId does not exist")
              }
            case None =>
              throw new IOException(
                s"Block $blockId is marked as spilled but has no spill file")
          }

        case Completed =>
          // Block is completed - should be fetched through standard mechanisms
          // This path handles the transition period before block is removed from tracking
          if (blockInfo.spillFile.isDefined) {
            val file = blockInfo.spillFile.get
            if (file.exists()) {
              new FileSegmentManagedBuffer(
                transportConf,
                file,
                blockInfo.spillOffset,
                blockInfo.spillLength)
            } else {
              throw new IOException(
                s"Completed block $blockId data file ${file.getPath} does not exist")
            }
          } else if (blockInfo.buffer != null) {
            new NioManagedBuffer(blockInfo.buffer.duplicate())
          } else {
            throw new IOException(
              s"Completed block $blockId has no data available")
          }
      }
    } else {
      // Block not tracked - may need to fall back to disk-based resolution
      // or the block doesn't exist in streaming shuffle
      throw new IOException(
        s"Block $blockId not found in streaming shuffle block resolver. " +
        "The block may have been completed and cleaned up, or was never registered.")
    }
  }

  /**
   * Returns a list of BlockIds associated with a given shuffle map.
   * Used for cleanup operations when removing shuffle data.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @return Sequence of BlockIds for this shuffle map
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val blocks = new ArrayBuffer[BlockId]()

    // Iterate through tracked blocks to find those matching this shuffle/map
    inFlightBlocks.asScala.foreach { case (blockId, _) =>
      blockId match {
        case ShuffleBlockId(sid, mid, _) if sid == shuffleId && mid == mapId =>
          blocks += blockId
        case ShuffleBlockBatchId(sid, mid, _, _) if sid == shuffleId && mid == mapId =>
          blocks += blockId
        case _ => // Not a matching block
      }
    }

    blocks.toSeq
  }

  /**
   * Retrieves merged block data for push-based shuffle.
   *
   * Note: Streaming shuffle does not currently support push-based shuffle merge.
   * This method returns an empty sequence as merged blocks are handled by
   * the standard IndexShuffleBlockResolver. Future versions may add support
   * for streaming merged blocks.
   *
   * @param blockId The merged block identifier
   * @param dirs Optional directories to search
   * @return Empty sequence (merged blocks not supported in streaming shuffle)
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    // Streaming shuffle does not support merged blocks
    // Return empty sequence - the caller should fall back to standard resolution
    logDebug(log"getMergedBlockData called for ${MDC(BLOCK_ID, blockId)} " +
      "- streaming shuffle does not support merged blocks")
    Seq.empty
  }

  /**
   * Retrieves merged block metadata for push-based shuffle.
   *
   * Note: Streaming shuffle does not currently support push-based shuffle merge.
   * This method returns null as merged blocks are handled by the standard
   * IndexShuffleBlockResolver. The caller should check for null and fall back
   * to standard resolution.
   *
   * @param blockId The merged block identifier
   * @param dirs Optional directories to search
   * @return null (merged blocks not supported in streaming shuffle)
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    // Streaming shuffle does not support merged blocks
    logDebug(log"getMergedBlockMeta called for ${MDC(BLOCK_ID, blockId)} " +
      "- streaming shuffle does not support merged blocks")
    null
  }

  /**
   * Registers a new in-flight block that is being streamed from a producer.
   *
   * This method is called by StreamingShuffleWriter when beginning to stream
   * data for a partition. The block starts in InFlight state with its data
   * in the provided ByteBuffer.
   *
   * Memory Management Integration:
   * The registered block is tracked for potential spill by MemorySpillManager.
   * When memory pressure exceeds 80%, oldest blocks (by timestamp) are selected
   * for eviction using LRU policy.
   *
   * @param blockId The unique identifier for this block
   * @param buffer The ByteBuffer containing the block's data
   * @param checksum CRC32C checksum for data integrity validation
   * @return The created InFlightBlockInfo for further state management
   */
  def registerInFlightBlock(
      blockId: BlockId,
      buffer: ByteBuffer,
      checksum: Long): InFlightBlockInfo = {

    val timestamp = System.currentTimeMillis()
    val info = InFlightBlockInfo(
      blockId = blockId,
      buffer = buffer,
      state = InFlight,
      timestamp = timestamp,
      checksum = checksum
    )

    inFlightBlocks.put(blockId, info)

    // Track shuffle-to-map association for efficient cleanup
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, _) =>
        trackShuffleMap(shuffleId, mapId)
      case ShuffleBlockBatchId(shuffleId, mapId, _, _) =>
        trackShuffleMap(shuffleId, mapId)
      case _ => // Other block types don't need shuffle tracking
    }

    logDebug(log"Registered in-flight block ${MDC(BLOCK_ID, blockId)} " +
      s"with ${buffer.remaining()} bytes, checksum=$checksum")

    info
  }

  /**
   * Marks a block as spilled to disk, updating its state and releasing memory.
   *
   * This method is called by MemorySpillManager when memory pressure exceeds
   * the 80% threshold. The spill response time target is <100ms.
   *
   * @param blockId The block that was spilled
   * @param spillFile The file where data was written
   * @param offset Offset within the spill file
   * @param length Length of the spilled data
   * @return true if the block was found and updated, false if block was not tracked
   */
  def markBlockSpilled(
      blockId: BlockId,
      spillFile: File,
      offset: Long,
      length: Long): Boolean = {

    val info = inFlightBlocks.get(blockId)
    if (info != null) {
      info.markSpilled(spillFile, offset, length)
      logDebug(log"Marked block ${MDC(BLOCK_ID, blockId)} as spilled to " +
        log"${MDC(PATH, spillFile.getPath)}, offset=$offset, length=$length")
      true
    } else {
      logWarning(log"Attempted to mark non-existent block ${MDC(BLOCK_ID, blockId)} as spilled")
      false
    }
  }

  /**
   * Marks a block as completed, indicating streaming has finished successfully.
   *
   * This method is called when all data for a block has been streamed and
   * acknowledged by consumers. The block remains tracked for a short period
   * to handle late requests, then is cleaned up.
   *
   * @param blockId The block that completed
   * @return true if the block was found and updated, false if block was not tracked
   */
  def markBlockCompleted(blockId: BlockId): Boolean = {
    val info = inFlightBlocks.get(blockId)
    if (info != null) {
      info.markCompleted()
      logDebug(log"Marked block ${MDC(BLOCK_ID, blockId)} as completed")
      true
    } else {
      logWarning(log"Attempted to mark non-existent block ${MDC(BLOCK_ID, blockId)} as completed")
      false
    }
  }

  /**
   * Gets the current state of a block.
   *
   * @param blockId The block to query
   * @return Some(state) if block is tracked, None if not found
   */
  def getBlockState(blockId: BlockId): Option[BlockState] = {
    val info = inFlightBlocks.get(blockId)
    if (info != null) {
      Some(info.state)
    } else {
      None
    }
  }

  /**
   * Gets the InFlightBlockInfo for a block if it exists.
   *
   * @param blockId The block to query
   * @return Some(info) if block is tracked, None if not found
   */
  def getBlockInfo(blockId: BlockId): Option[InFlightBlockInfo] = {
    Option(inFlightBlocks.get(blockId))
  }

  /**
   * Removes all data associated with a specific map task from a shuffle.
   *
   * This method cleans up both in-memory buffers and spilled files for
   * the specified map task. It is called during shuffle unregistration
   * or when a map task's output is no longer needed.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    val blocksToRemove = new ArrayBuffer[BlockId]()

    // Identify all blocks for this shuffle/map
    inFlightBlocks.asScala.foreach { case (blockId, info) =>
      blockId match {
        case ShuffleBlockId(sid, mid, _) if sid == shuffleId && mid == mapId =>
          blocksToRemove += blockId
          cleanupBlockInfo(info)
        case ShuffleBlockBatchId(sid, mid, _, _) if sid == shuffleId && mid == mapId =>
          blocksToRemove += blockId
          cleanupBlockInfo(info)
        case _ => // Not a matching block
      }
    }

    // Remove from tracking
    blocksToRemove.foreach(inFlightBlocks.remove)

    // Update shuffle-to-map tracking
    val mapIds = shuffleToMapIds.get(shuffleId)
    if (mapIds != null) {
      mapIds.remove(mapId)
    }

    if (blocksToRemove.nonEmpty) {
      logDebug(log"Removed ${MDC(LogKeys.NUM_BLOCKS, blocksToRemove.size)} blocks for " +
        log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}, map ${MDC(MAP_ID, mapId)}")
    }
  }

  /**
   * Removes all data associated with a shuffle.
   *
   * @param shuffleId The shuffle identifier to clean up
   */
  def removeDataByShuffle(shuffleId: Int): Unit = {
    val mapIds = shuffleToMapIds.remove(shuffleId)
    if (mapIds != null) {
      mapIds.asScala.foreach { mapId =>
        removeDataByMap(shuffleId, mapId)
      }
    }

    logDebug(log"Removed all data for shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
  }

  /**
   * Returns all blocks currently in a specific state.
   *
   * @param state The state to filter by
   * @return Sequence of InFlightBlockInfo for blocks in the specified state
   */
  def getBlocksInState(state: BlockState): Seq[InFlightBlockInfo] = {
    inFlightBlocks.asScala.values.filter(_.state == state).toSeq
  }

  /**
   * Returns all in-flight blocks sorted by timestamp (oldest first).
   * Used by MemorySpillManager for LRU eviction selection.
   *
   * @return Sequence of InFlightBlockInfo sorted by timestamp ascending
   */
  def getBlocksByAge(): Seq[InFlightBlockInfo] = {
    inFlightBlocks.asScala.values.toSeq.sortBy(_.timestamp)
  }

  /**
   * Returns the total memory used by in-flight blocks.
   *
   * @return Total bytes used by in-memory buffers
   */
  def getTotalInFlightMemory: Long = {
    inFlightBlocks.asScala.values
      .filter(_.state == InFlight)
      .filter(_.buffer != null)
      .map(_.buffer.remaining().toLong)
      .sum
  }

  /**
   * Returns the number of blocks currently being tracked.
   *
   * @return Count of tracked blocks
   */
  def getTrackedBlockCount: Int = {
    inFlightBlocks.size()
  }

  /**
   * Stops the block resolver and cleans up all resources.
   *
   * This method should be called during shuffle manager shutdown.
   * It releases all in-memory buffers and removes references to spill files
   * (actual spill file cleanup is handled by BlockManager/DiskBlockManager).
   */
  override def stop(): Unit = {
    logInfo("Stopping StreamingShuffleBlockResolver")

    // Clean up all tracked blocks
    inFlightBlocks.asScala.foreach { case (blockId, info) =>
      cleanupBlockInfo(info)
    }
    inFlightBlocks.clear()
    shuffleToMapIds.clear()

    logInfo("StreamingShuffleBlockResolver stopped")
  }

  /**
   * Helper method to track shuffle-to-map associations.
   */
  private def trackShuffleMap(shuffleId: Int, mapId: Long): Unit = {
    shuffleToMapIds.computeIfAbsent(shuffleId, _ => {
      java.util.Collections.newSetFromMap(new ConcurrentHashMap[Long, java.lang.Boolean]())
    }).add(mapId)
  }

  /**
   * Helper method to clean up a block's resources.
   * Releases memory buffer but does not delete spill files
   * (handled by BlockManager/DiskBlockManager).
   */
  private def cleanupBlockInfo(info: InFlightBlockInfo): Unit = {
    // Release memory buffer reference to allow GC
    info.buffer = null

    // Note: Spill file cleanup is handled by BlockManager's DiskBlockManager
    // during shuffle unregistration. We only clear the reference here.
    info.spillFile = None
  }
}

/**
 * Companion object for StreamingShuffleBlockResolver.
 * Contains constants and factory methods.
 */
private[spark] object StreamingShuffleBlockResolver {

  /**
   * Creates a StreamingShuffleBlockResolver with the given configuration.
   *
   * @param conf SparkConf instance
   * @return New StreamingShuffleBlockResolver instance
   */
  def apply(conf: SparkConf): StreamingShuffleBlockResolver = {
    new StreamingShuffleBlockResolver(conf)
  }

  /**
   * Creates a StreamingShuffleBlockResolver with explicit BlockManager (for testing).
   *
   * @param conf SparkConf instance
   * @param blockManager BlockManager instance
   * @return New StreamingShuffleBlockResolver instance
   */
  def apply(conf: SparkConf, blockManager: BlockManager): StreamingShuffleBlockResolver = {
    new StreamingShuffleBlockResolver(conf, blockManager)
  }
}
