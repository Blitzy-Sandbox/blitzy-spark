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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage._
import org.apache.spark.util.collection.OpenHashSet

/**
 * Block resolver implementation for streaming shuffle that implements the ShuffleBlockResolver
 * trait. This resolver handles both in-memory streaming buffers and spilled files, providing
 * seamless block data retrieval for streaming shuffle operations.
 *
 * Key responsibilities:
 * - Maintains mapping of shuffleId -> mapId -> partitionId -> buffer/spill locations
 * - Provides block data retrieval from in-memory buffers or spilled files
 * - Supports external shuffle service compatibility through standard block ID patterns
 * - Coordinates with MemorySpillManager for spilled block retrieval
 * - Manages block cleanup during shuffle unregistration
 *
 * The resolver uses a three-level nested ConcurrentHashMap structure:
 *   shuffleId -> mapId -> partitionId -> (StreamingBuffer | File)
 *
 * This allows efficient lookup and cleanup of streaming shuffle data at various
 * granularities (per-shuffle, per-map, per-partition).
 *
 * @param conf SparkConf for reading configuration settings
 * @param taskIdMapsForShuffle Map tracking map task IDs for each shuffle, used for cleanup
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    val taskIdMapsForShuffle: ConcurrentHashMap[Int, OpenHashSet[Long]] = 
      new ConcurrentHashMap[Int, OpenHashSet[Long]]())
  extends ShuffleBlockResolver
  with Logging {

  // Secondary constructor for simpler instantiation
  def this(conf: SparkConf) = {
    this(conf, new ConcurrentHashMap[Int, OpenHashSet[Long]]())
  }

  // Lazy initialization of BlockManager to avoid circular dependency during SparkEnv creation
  private lazy val blockManager = SparkEnv.get.blockManager

  // Transport configuration for creating FileSegmentManagedBuffer instances
  private val transportConf = {
    val securityManager = new SecurityManager(conf)
    SparkTransportConf.fromSparkConf(
      conf, "shuffle", sslOptions = Some(securityManager.getRpcSSLOptions()))
  }

  /**
   * Thread-safe storage for streaming shuffle buffers.
   * Structure: shuffleId -> mapId -> partitionId -> StreamingBuffer
   */
  private val streamingBuffers = new ConcurrentHashMap[Int, 
    ConcurrentHashMap[Long, ConcurrentHashMap[Int, StreamingBuffer]]]()

  /**
   * Thread-safe storage for spilled shuffle data files.
   * Structure: shuffleId -> mapId -> partitionId -> File
   */
  private val spilledFiles = new ConcurrentHashMap[Int,
    ConcurrentHashMap[Long, ConcurrentHashMap[Int, File]]]()

  /**
   * Retrieves the data for the specified block.
   *
   * This method supports multiple block ID types:
   * - ShuffleBlockId: Standard shuffle block with shuffleId, mapId, reduceId (partition)
   * - ShuffleDataBlockId: Data-specific block ID variant
   *
   * The method first checks for in-memory buffer data, then falls back to spilled
   * file data. If neither is found, an exception is thrown.
   *
   * @param blockId The block identifier to retrieve
   * @param dirs Optional array of directories to search (for external shuffle service)
   * @return ManagedBuffer containing the block data
   * @throws SparkException if the block data is not found
   */
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, partitionId) = blockId match {
      case ShuffleBlockId(sid, mid, rid) =>
        (sid, mid, rid)
      case ShuffleDataBlockId(sid, mid, rid) =>
        (sid, mid, rid)
      case _ =>
        throw SparkException.internalError(
          s"Unexpected block ID type for streaming shuffle: $blockId", 
          category = "SHUFFLE")
    }

    logDebug(s"Retrieving block data for shuffleId=$shuffleId, mapId=$mapId, " +
      s"partitionId=$partitionId")

    // First, try to get data from in-memory streaming buffer
    val bufferData = getStreamingBufferData(shuffleId, mapId, partitionId)
    if (bufferData.isDefined) {
      logDebug(s"Found streaming buffer data for block $blockId, " +
        s"size=${bufferData.get.remaining()}")
      return new NioManagedBuffer(bufferData.get)
    }

    // Second, try to get data from spilled file
    val spillFile = getSpilledFile(shuffleId, mapId, partitionId)
    if (spillFile.isDefined && spillFile.get.exists()) {
      val file = spillFile.get
      logDebug(s"Found spilled file data for block $blockId, " +
        s"path=${file.getPath()}, size=${file.length()}")
      return new FileSegmentManagedBuffer(transportConf, file, 0, file.length())
    }

    // Block data not found - throw exception
    throw SparkException.internalError(
      s"Block data not found for streaming shuffle block: $blockId " +
        s"(shuffleId=$shuffleId, mapId=$mapId, partitionId=$partitionId)",
      category = "SHUFFLE")
  }

  /**
   * Retrieves a list of BlockIds for a given shuffle map.
   *
   * This method is used for cleanup operations, returning all block IDs
   * associated with the specified shuffle and map task. The returned
   * blocks include both in-memory buffered blocks and spilled blocks.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @return Sequence of BlockIds for the specified map output
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val blocks = new ArrayBuffer[BlockId]()

    // Collect block IDs from streaming buffers
    val shuffleBuffers = streamingBuffers.get(shuffleId)
    if (shuffleBuffers != null) {
      val mapBuffers = shuffleBuffers.get(mapId)
      if (mapBuffers != null) {
        mapBuffers.keys().asScala.foreach { partitionId =>
          blocks += ShuffleBlockId(shuffleId, mapId, partitionId)
        }
      }
    }

    // Collect block IDs from spilled files
    val shuffleSpills = spilledFiles.get(shuffleId)
    if (shuffleSpills != null) {
      val mapSpills = shuffleSpills.get(mapId)
      if (mapSpills != null) {
        mapSpills.keys().asScala.foreach { partitionId =>
          // Only add if not already in buffers (avoid duplicates)
          val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
          if (!blocks.contains(blockId)) {
            blocks += blockId
          }
        }
      }
    }

    logDebug(s"Found ${blocks.size} blocks for shuffleId=$shuffleId, mapId=$mapId")
    blocks.toSeq
  }

  /**
   * Retrieves data for a merged shuffle block.
   *
   * Merged shuffle blocks are not supported by streaming shuffle, as the streaming
   * model operates on individual partition buffers rather than merged/consolidated
   * blocks. This method returns an empty sequence.
   *
   * @param blockId The merged block identifier
   * @param dirs Optional directories (unused)
   * @return Empty sequence as merged blocks are not supported
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    logDebug(s"getMergedBlockData called for $blockId - " +
      "merged blocks not supported for streaming shuffle")
    Seq.empty[ManagedBuffer]
  }

  /**
   * Retrieves metadata for a merged shuffle block.
   *
   * Merged shuffle blocks are not supported by streaming shuffle. This method
   * returns a placeholder MergedBlockMeta with zero chunks.
   *
   * @param blockId The merged block identifier
   * @param dirs Optional directories (unused)
   * @return MergedBlockMeta with zero chunks indicating no merged data
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    logDebug(s"getMergedBlockMeta called for $blockId - " +
      "merged blocks not supported for streaming shuffle")
    // Return empty metadata with zero chunks
    new MergedBlockMeta(0, new NioManagedBuffer(ByteBuffer.allocate(0)))
  }

  /**
   * Registers a streaming buffer for a specific partition.
   *
   * This method is called by StreamingShuffleWriter to register in-memory
   * buffer data that is available for streaming to consumers.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @param partitionId The partition (reduce task) identifier
   * @param buffer The StreamingBuffer containing partition data
   */
  def registerStreamingBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      buffer: StreamingBuffer): Unit = {
    
    val shuffleMap = streamingBuffers.computeIfAbsent(shuffleId,
      _ => new ConcurrentHashMap[Long, ConcurrentHashMap[Int, StreamingBuffer]]())
    val mapMap = shuffleMap.computeIfAbsent(mapId,
      _ => new ConcurrentHashMap[Int, StreamingBuffer]())
    
    val previousBuffer = mapMap.put(partitionId, buffer)
    if (previousBuffer != null) {
      logWarning(log"Replaced existing streaming buffer for " +
        log"shuffleId=${MDC(SHUFFLE_ID, shuffleId)}, mapId=${MDC(BLOCK_ID, mapId.toString)}, " +
        log"partitionId=${MDC(PARTITION_ID, partitionId)}")
      // Release the previous buffer to free memory
      previousBuffer.release()
    }

    // Track the map task ID for this shuffle
    val taskIds = taskIdMapsForShuffle.computeIfAbsent(shuffleId,
      _ => new OpenHashSet[Long]())
    taskIds.add(mapId)

    logDebug(s"Registered streaming buffer for shuffleId=$shuffleId, " +
      s"mapId=$mapId, partitionId=$partitionId, size=${buffer.size()}")
  }

  /**
   * Registers a spilled file for a specific partition.
   *
   * This method is called by MemorySpillManager when buffer data has been
   * spilled to disk due to memory pressure.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @param partitionId The partition (reduce task) identifier
   * @param spillFile The file containing spilled partition data
   */
  def registerSpilledBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      spillFile: File): Unit = {
    
    val shuffleMap = spilledFiles.computeIfAbsent(shuffleId,
      _ => new ConcurrentHashMap[Long, ConcurrentHashMap[Int, File]]())
    val mapMap = shuffleMap.computeIfAbsent(mapId,
      _ => new ConcurrentHashMap[Int, File]())
    
    val previousFile = mapMap.put(partitionId, spillFile)
    if (previousFile != null && previousFile.exists()) {
      logWarning(log"Replaced existing spill file for " +
        log"shuffleId=${MDC(SHUFFLE_ID, shuffleId)}, mapId=${MDC(BLOCK_ID, mapId.toString)}, " +
        log"partitionId=${MDC(PARTITION_ID, partitionId)}")
      // Delete the previous spill file
      if (!previousFile.delete()) {
        logWarning(log"Failed to delete previous spill file: " +
          log"${MDC(PATH, previousFile.getPath())}")
      }
    }

    // Track the map task ID for this shuffle
    val taskIds = taskIdMapsForShuffle.computeIfAbsent(shuffleId,
      _ => new OpenHashSet[Long]())
    taskIds.add(mapId)

    logDebug(s"Registered spilled block for shuffleId=$shuffleId, " +
      s"mapId=$mapId, partitionId=$partitionId, file=${spillFile.getPath()}")
  }

  /**
   * Removes all data for a specific map task.
   *
   * This method cleans up both in-memory buffers and spilled files
   * associated with the specified map task. It is called during shuffle
   * cleanup or when a map task needs to be re-executed.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    // Clean up streaming buffers for this map
    val shuffleBuffers = streamingBuffers.get(shuffleId)
    if (shuffleBuffers != null) {
      val mapBuffers = shuffleBuffers.remove(mapId)
      if (mapBuffers != null) {
        mapBuffers.forEach { (partitionId, buffer) =>
          try {
            buffer.release()
            logDebug(s"Released streaming buffer for shuffleId=$shuffleId, " +
              s"mapId=$mapId, partitionId=$partitionId")
          } catch {
            case e: Exception =>
              logWarning(log"Error releasing streaming buffer for " +
                log"shuffleId=${MDC(SHUFFLE_ID, shuffleId)}, " +
                log"mapId=${MDC(BLOCK_ID, mapId.toString)}, " +
                log"partitionId=${MDC(PARTITION_ID, partitionId)}: ${MDC(ERROR, e.getMessage)}")
          }
        }
      }
    }

    // Clean up spilled files for this map
    val shuffleSpills = spilledFiles.get(shuffleId)
    if (shuffleSpills != null) {
      val mapSpills = shuffleSpills.remove(mapId)
      if (mapSpills != null) {
        mapSpills.forEach { (partitionId, file) =>
          try {
            if (file.exists() && !file.delete()) {
              logWarning(log"Error deleting spill file: ${MDC(PATH, file.getPath())}")
            } else {
              logDebug(s"Deleted spill file for shuffleId=$shuffleId, " +
                s"mapId=$mapId, partitionId=$partitionId")
            }
          } catch {
            case e: Exception =>
              logWarning(log"Error deleting spill file ${MDC(PATH, file.getPath())}: " +
                log"${MDC(ERROR, e.getMessage)}")
          }
        }
      }
    }

    logDebug(s"Removed all data for shuffleId=$shuffleId, mapId=$mapId")
  }

  /**
   * Removes all data for a specific shuffle.
   *
   * This method is called during shuffle unregistration to clean up all
   * resources associated with the shuffle.
   *
   * @param shuffleId The shuffle identifier to clean up
   */
  private[streaming] def removeDataByShuffle(shuffleId: Int): Unit = {
    // Clean up streaming buffers for this shuffle
    val shuffleBuffers = streamingBuffers.remove(shuffleId)
    if (shuffleBuffers != null) {
      shuffleBuffers.forEach { (mapId, mapBuffers) =>
        mapBuffers.forEach { (partitionId, buffer) =>
          try {
            buffer.release()
          } catch {
            case e: Exception =>
              logWarning(log"Error releasing streaming buffer during shuffle cleanup: " +
                log"${MDC(ERROR, e.getMessage)}")
          }
        }
      }
    }

    // Clean up spilled files for this shuffle
    val shuffleSpills = spilledFiles.remove(shuffleId)
    if (shuffleSpills != null) {
      shuffleSpills.forEach { (mapId, mapSpills) =>
        mapSpills.forEach { (partitionId, file) =>
          try {
            if (file.exists() && !file.delete()) {
              logWarning(log"Error deleting spill file: ${MDC(PATH, file.getPath())}")
            }
          } catch {
            case e: Exception =>
              logWarning(log"Error deleting spill file: ${MDC(ERROR, e.getMessage)}")
          }
        }
      }
    }

    // Remove from task ID tracking
    taskIdMapsForShuffle.remove(shuffleId)

    logInfo(log"Removed all data for shuffleId=${MDC(SHUFFLE_ID, shuffleId)}")
  }

  /**
   * Stops the block resolver and releases all resources.
   *
   * This method is called during shutdown to clean up all streaming
   * shuffle data. It releases all in-memory buffers and deletes all
   * spilled files.
   */
  override def stop(): Unit = {
    logInfo(log"Stopping StreamingShuffleBlockResolver")

    // Clean up all streaming buffers
    streamingBuffers.forEach { (shuffleId, shuffleMap) =>
      shuffleMap.forEach { (mapId, mapMap) =>
        mapMap.forEach { (partitionId, buffer) =>
          try {
            buffer.release()
          } catch {
            case e: Exception =>
              logWarning(log"Error releasing buffer during stop: ${MDC(ERROR, e.getMessage)}")
          }
        }
      }
    }
    streamingBuffers.clear()

    // Clean up all spilled files
    spilledFiles.forEach { (shuffleId, shuffleMap) =>
      shuffleMap.forEach { (mapId, mapMap) =>
        mapMap.forEach { (partitionId, file) =>
          try {
            if (file.exists() && !file.delete()) {
              logWarning(log"Error deleting spill file during stop: ${MDC(PATH, file.getPath())}")
            }
          } catch {
            case e: Exception =>
              logWarning(log"Error deleting spill file during stop: ${MDC(ERROR, e.getMessage)}")
          }
        }
      }
    }
    spilledFiles.clear()

    // Clear task ID tracking
    taskIdMapsForShuffle.clear()

    logInfo(log"StreamingShuffleBlockResolver stopped")
  }

  /**
   * Retrieves the streaming buffer data for a specific partition.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @param partitionId The partition identifier
   * @return Optional ByteBuffer containing the buffer data
   */
  private def getStreamingBufferData(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int): Option[ByteBuffer] = {
    val shuffleMap = streamingBuffers.get(shuffleId)
    if (shuffleMap == null) return None

    val mapMap = shuffleMap.get(mapId)
    if (mapMap == null) return None

    val buffer = mapMap.get(partitionId)
    if (buffer == null) return None

    // Get data from buffer and wrap in ByteBuffer
    val data = buffer.getData()
    if (data != null && data.length > 0) {
      Some(ByteBuffer.wrap(data))
    } else {
      None
    }
  }

  /**
   * Retrieves the spilled file for a specific partition.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @param partitionId The partition identifier
   * @return Optional File if a spill file exists
   */
  private def getSpilledFile(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int): Option[File] = {
    val shuffleMap = spilledFiles.get(shuffleId)
    if (shuffleMap == null) return None

    val mapMap = shuffleMap.get(mapId)
    if (mapMap == null) return None

    val file = mapMap.get(partitionId)
    if (file != null) Some(file) else None
  }

  /**
   * Checks if a streaming buffer exists for the specified partition.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @param partitionId The partition identifier
   * @return true if a buffer exists
   */
  private[streaming] def hasStreamingBuffer(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int): Boolean = {
    val shuffleMap = streamingBuffers.get(shuffleId)
    if (shuffleMap == null) return false

    val mapMap = shuffleMap.get(mapId)
    if (mapMap == null) return false

    mapMap.containsKey(partitionId)
  }

  /**
   * Checks if a spilled file exists for the specified partition.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @param partitionId The partition identifier
   * @return true if a spill file exists
   */
  private[streaming] def hasSpilledFile(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int): Boolean = {
    val shuffleMap = spilledFiles.get(shuffleId)
    if (shuffleMap == null) return false

    val mapMap = shuffleMap.get(mapId)
    if (mapMap == null) return false

    mapMap.containsKey(partitionId)
  }

  /**
   * Returns the total number of registered streaming buffers across all shuffles.
   * This is primarily used for testing and monitoring.
   *
   * @return The total count of registered buffers
   */
  private[streaming] def getBufferCount(): Int = {
    var count = 0
    streamingBuffers.forEach { (_, shuffleMap) =>
      shuffleMap.forEach { (_, mapMap) =>
        count += mapMap.size()
      }
    }
    count
  }

  /**
   * Returns the total number of spilled files across all shuffles.
   * This is primarily used for testing and monitoring.
   *
   * @return The total count of spilled files
   */
  private[streaming] def getSpillFileCount(): Int = {
    var count = 0
    spilledFiles.forEach { (_, shuffleMap) =>
      shuffleMap.forEach { (_, mapMap) =>
        count += mapMap.size()
      }
    }
    count
  }
}
