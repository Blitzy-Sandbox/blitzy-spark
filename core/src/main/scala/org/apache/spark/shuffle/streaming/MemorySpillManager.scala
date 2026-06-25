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
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService,
  TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel, TempLocalBlockId}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Memory spill manager for the streaming shuffle data path (feature F-109).
 *
 * The streaming shuffle keeps serialized, per-partition shuffle bytes in bounded in-memory
 * [[StreamingBuffer]]s while they are pipelined from producer (map) tasks to consumer (reduce)
 * tasks. Those buffers consume executor heap, so this manager continuously guards against memory
 * exhaustion: a single daemon thread polls aggregate buffer utilization every
 * [[POLL_INTERVAL_MS]] milliseconds and, when utilization reaches the configured spill threshold
 * (default 80 percent), spills the largest buffers to local disk to reclaim heap. Spilling never
 * loses data: the bytes are written through the existing [[BlockManager]] storage path before the
 * heap reference is released, so a spilled block can still be served from disk.
 *
 * '''Utilization.''' Utilization is computed as the sum of the live (not-yet-spilled) buffer
 * sizes divided by [[org.apache.spark.memory.MemoryManager#maxOnHeapStorageMemory]], the same
 * denominator the rest of Spark uses for on-heap storage accounting. The result feeds the
 * `shuffle.streaming.bufferUtilizationPercent` gauge on every poll. The division is guarded so a
 * zero (or negative) maximum never produces a divide-by-zero.
 *
 * '''Spill selection.''' When the threshold is crossed the manager spills the LARGEST partitions
 * first (they free the most heap per spill), breaking ties in least-recently-used order using
 * [[StreamingBuffer#lastAccess]]. It stops as soon as projected utilization drops back below the
 * threshold, so it never spills more than is necessary to relieve pressure.
 *
 * '''Spill sink and block identity.''' Each spilled buffer is written via the EXISTING
 * `BlockManager.putBytes(blockId, bytes, StorageLevel.DISK_ONLY, tellMaster = false)` API; the
 * `BlockManager` itself is never modified. Spilled blocks are identified with a
 * [[org.apache.spark.storage.TempLocalBlockId]]. A streaming spill is a transient, executor-local
 * overflow of in-flight bytes that is reclaimed once acknowledged, so a temporary local block id
 * models it precisely and is intentionally not advertised to the `BlockManagerMaster` (hence
 * `tellMaster = false`), whereas a `ShuffleBlockId` would imply a durable, master-tracked map
 * output that this short-lived overflow is not.
 *
 * '''Reclamation.''' When the consumer acknowledges receipt of a partition the owning component
 * calls [[reclaim]], which frees the buffer and drops it from the registry. The manager targets a
 * [[RECLAIM_DEADLINE_MS]] millisecond reclaim SLA and logs a warning if a reclaim is observed to
 * exceed it.
 *
 * '''Concurrency.''' The poll runs on a dedicated daemon thread obtained from
 * [[org.apache.spark.util.ThreadUtils]]. Buffer sizes and access timestamps are read through
 * [[StreamingBuffer]]'s lock-free atomics, and a consistent byte snapshot for spilling is taken
 * via the synchronized [[StreamingBuffer#toBytes]]. The buffer registry is a thread-safe Guava
 * cache, and [[start]] / [[stop]] are idempotent. The poll body never propagates exceptions, so a
 * transient failure can never silently cancel the periodic monitor.
 *
 * This class is `private[spark]` and therefore introduces no new public, binary-compatible API.
 *
 * @param blockManager          the executor `BlockManager` used as the DISK_ONLY spill sink
 * @param memoryManager         supplies `maxOnHeapStorageMemory`, the utilization denominator
 * @param metrics               streaming shuffle telemetry updated on every poll and spill event
 * @param spillThresholdPercent buffer-utilization percentage (1-100) at which spilling triggers;
 *                              sourced from `StreamingShuffleConfig.spillThreshold` (default 80)
 */
private[spark] class MemorySpillManager(
    blockManager: BlockManager,
    memoryManager: MemoryManager,
    metrics: StreamingShuffleMetrics,
    spillThresholdPercent: Int)
  extends Logging {

  import MemorySpillManager.{BufferKey, SpilledSegment}

  /**
   * Convenience constructor that resolves the `BlockManager` and `MemoryManager` collaborators
   * from the active [[org.apache.spark.SparkEnv]]. Intended for executor-side construction by
   * `StreamingShuffleManager`, where the environment is always initialized.
   *
   * @param metrics               streaming shuffle telemetry
   * @param spillThresholdPercent buffer-utilization percentage at which spilling triggers
   */
  def this(metrics: StreamingShuffleMetrics, spillThresholdPercent: Int) = {
    this(SparkEnv.get.blockManager, SparkEnv.get.memoryManager, metrics, spillThresholdPercent)
  }

  /** Poll the aggregate buffer utilization every 100 ms (AAP section 0.2.1). */
  private val POLL_INTERVAL_MS: Long = 100L

  /** Reclaim an acknowledged buffer within 100 ms of the acknowledgment (AAP section 0.2.1). */
  private val RECLAIM_DEADLINE_MS: Long = 100L

  /** Spill threshold expressed as a fraction in (0, 1], e.g. 80 percent becomes 0.8. */
  private val spillThresholdFraction: Double = spillThresholdPercent / 100.0

  /**
   * Registry of live per-partition buffers keyed by an opaque [[BufferKey]]. Backed by a Guava
   * cache for weakly-consistent `asMap()` iteration, so the poll thread can scan while producers
   * concurrently register and reclaim buffers. See decision log ADR-17.
   */
  private val bufferRegistry: Cache[BufferKey, StreamingBuffer] =
    CacheBuilder.newBuilder().build[BufferKey, StreamingBuffer]()

  /**
   * Per-key ordered ledger of the DISK_ONLY blocks a buffer has been spilled to. Each spill
   * drains the buffer's resident bytes to a fresh [[org.apache.spark.storage.TempLocalBlockId]]
   * and appends a [[SpilledSegment]] (block id, length, CRC32C) to this key's queue in
   * on-the-wire byte order; the producing writer reads the ordered segments back via
   * [[spilledSegmentsFor]] at commit time. A [[ConcurrentLinkedQueue]] allows lock-free append by
   * the poll thread while the writer drains. See decision log ADR-06 and ADR-17 for the
   * block-identity rationale.
   */
  private val spillLedger =
    new ConcurrentHashMap[BufferKey, ConcurrentLinkedQueue[SpilledSegment]]()

  /** Single daemon thread that runs the utilization poll. */
  private val scheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-spill-monitor")

  /** Guards [[start]] so the poll is scheduled at most once. */
  private val started = new AtomicBoolean(false)

  /** Guards [[stop]] so the scheduler is torn down at most once. */
  private val stopped = new AtomicBoolean(false)

  /** The periodic task; defers to [[pollOnce]] which never throws. */
  private val pollRunnable: Runnable = () => pollOnce()

  /**
   * Start the utilization monitor. Schedules [[pollOnce]] at a fixed [[POLL_INTERVAL_MS]] rate on
   * the daemon thread. Idempotent: repeated invocations after the first are no-ops.
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      scheduler.scheduleAtFixedRate(
        pollRunnable, POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)
      logInfo(log"Started streaming shuffle MemorySpillManager: polling every " +
        log"${MDC(INTERVAL, POLL_INTERVAL_MS)} ms, spill threshold " +
        log"${MDC(THRESHOLD, spillThresholdPercent)} percent")
    } else {
      logDebug("MemorySpillManager.start() ignored; monitor is already running")
    }
  }

  /**
   * Register a per-partition buffer so it is included in utilization accounting and is eligible
   * for spilling. Overwrites any existing mapping for the same key.
   *
   * @param key    opaque identifier for the buffer's (shuffle, map, partition) tuple
   * @param buffer the live per-partition buffer to track
   */
  def registerBuffer(key: BufferKey, buffer: StreamingBuffer): Unit = {
    bufferRegistry.put(key, buffer)
    logTrace(log"Registered streaming buffer shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
      log"map=${MDC(MAP_ID, key.mapId)} reduce=${MDC(REDUCE_ID, key.partitionId)} " +
      log"(partition ${MDC(PARTITION_ID, buffer.partitionId)})")
  }

  /**
   * Return the ordered spilled segments recorded for `key`, oldest spill first. The producing
   * writer calls this at commit time, after [[StreamingBuffer#finalizeForCommit]] has frozen the
   * ledger, to read the spilled bytes back and frame them ahead of the buffer's resident
   * snapshot. Returns an empty sequence for a key that has never been spilled.
   *
   * @param key the buffer key whose spilled segments to retrieve
   * @return the spilled segments in spill (on-the-wire) order
   */
  def spilledSegmentsFor(key: BufferKey): Seq[SpilledSegment] = {
    val queue = spillLedger.get(key)
    if (queue == null) Seq.empty else queue.asScala.toSeq
  }

  /**
   * Reclaim the buffer associated with `key`, typically after its output has been committed or in
   * response to a consumer acknowledgment. Any DISK_ONLY blocks the buffer was spilled to are
   * removed from the `BlockManager`, the buffer's heap is freed via [[StreamingBuffer#reset]],
   * the spill ledger entry is cleared, and the mapping is removed from the registry. The elapsed
   * time is measured against the [[RECLAIM_DEADLINE_MS]] SLA and a warning is logged if it is
   * exceeded.
   * Reclaiming an unknown key still drops any orphaned ledger entry and is otherwise a harmless
   * no-op.
   *
   * @param key the buffer key to reclaim
   */
  def reclaim(key: BufferKey): Unit = {
    val startNanos = System.nanoTime()
    removeSpilledBlocks(key)
    val buffer = bufferRegistry.getIfPresent(key)
    if (buffer != null) {
      buffer.reset()
      bufferRegistry.invalidate(key)
      val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
      if (elapsedMs > RECLAIM_DEADLINE_MS) {
        logWarning(log"Reclaim of streaming buffer shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"map=${MDC(MAP_ID, key.mapId)} reduce=${MDC(REDUCE_ID, key.partitionId)} took " +
          log"${MDC(DURATION, elapsedMs)} ms, exceeding the " +
          log"${MDC(TIMEOUT, RECLAIM_DEADLINE_MS)} ms deadline")
      } else {
        logTrace(log"Reclaimed streaming buffer shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"map=${MDC(MAP_ID, key.mapId)} reduce=${MDC(REDUCE_ID, key.partitionId)} in " +
          log"${MDC(DURATION, elapsedMs)} ms")
      }
    } else {
      logTrace(log"Reclaim requested for unknown streaming buffer " +
        log"shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} map=${MDC(MAP_ID, key.mapId)} " +
        log"reduce=${MDC(REDUCE_ID, key.partitionId)}; nothing to do")
    }
  }

  /**
   * Remove and free every DISK_ONLY block recorded in `key`'s spill ledger, then drop the ledger
   * entry. The transient temp-local blocks created by [[spillBuffer]] are executor-local and not
   * advertised to the master, so `removeBlock(tellMaster = false)` is sufficient. Failures to
   * remove an individual block are logged and swallowed so reclamation always completes.
   *
   * @param key the buffer key whose spilled blocks to remove
   */
  private def removeSpilledBlocks(key: BufferKey): Unit = {
    val queue = spillLedger.remove(key)
    if (queue != null) {
      queue.asScala.foreach { segment =>
        try {
          blockManager.removeBlock(segment.blockId, tellMaster = false)
        } catch {
          case t: Throwable =>
            logWarning(log"Failed to remove spilled streaming block " +
              log"${MDC(BLOCK_ID, segment.blockId)} for " +
              log"shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} map=${MDC(MAP_ID, key.mapId)} " +
              log"reduce=${MDC(REDUCE_ID, key.partitionId)} during reclaim; continuing", t)
        }
      }
    }
  }

  /**
   * Stop the utilization monitor and release all tracked buffers. Shuts the scheduler down
   * immediately, waits briefly for the in-flight poll (if any) to finish, then resets every
   * still-registered buffer to free its heap before clearing the registry, and removes any
   * DISK_ONLY blocks left in the spill ledger. Resetting the buffers before invalidating the
   * registry ensures the manager actually reclaims the memory it is designed to protect even if
   * a producer aborted without committing. Idempotent: repeated invocations after the first are
   * no-ops. Invoked by
   * `StreamingShuffleManager.stop()` in the order Backpressure -> Spill -> inner Sort.
   */
  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      scheduler.shutdownNow()
      try {
        if (!scheduler.awaitTermination(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
          logWarning(log"Streaming shuffle spill monitor did not terminate within " +
            log"${MDC(TIMEOUT, POLL_INTERVAL_MS)} ms of shutdown")
        }
      } catch {
        case _: InterruptedException =>
          // Preserve the interrupt status and stop waiting; shutdownNow has already been issued.
          Thread.currentThread().interrupt()
      }
      // Reset every live buffer to release its heap before dropping it from the registry, so a
      // producer that aborted without reclaiming does not leak memory past the manager's
      // lifetime.
      bufferRegistry.asMap().asScala.valuesIterator.foreach { buffer =>
        try {
          buffer.reset()
        } catch {
          case t: Throwable =>
            logWarning(log"Failed to reset streaming buffer for partition " +
              log"${MDC(PARTITION_ID, buffer.partitionId)} during " +
              log"MemorySpillManager.stop(); continuing", t)
        }
      }
      bufferRegistry.invalidateAll()
      // Remove any DISK_ONLY spill blocks that were never reclaimed by a producer.
      spillLedger.keySet().asScala.toSeq.foreach(removeSpilledBlocks)
      logInfo("Stopped streaming shuffle MemorySpillManager")
    } else {
      logDebug("MemorySpillManager.stop() ignored; monitor is already stopped")
    }
  }

  /**
   * A single poll cycle: refresh the utilization gauge and, if the threshold is crossed, spill.
   *
   * This method NEVER propagates an exception. A `scheduleAtFixedRate` task that throws is
   * silently removed from the scheduler, which would permanently disable memory protection;
   * catching every `Throwable` keeps the monitor alive across transient storage failures.
   */
  private def pollOnce(): Unit = {
    try {
      val maxMem = memoryManager.maxOnHeapStorageMemory
      if (maxMem <= 0L) {
        // No on-heap storage memory configured (or not yet initialized): nothing to protect.
        metrics.setBufferUtilizationPercent(0)
      } else {
        val totalBytes = currentBufferedBytes()
        val utilization = totalBytes.toDouble / maxMem.toDouble
        metrics.setBufferUtilizationPercent((utilization * 100.0).toInt)
        if (utilization >= spillThresholdFraction) {
          spillToThreshold(totalBytes, maxMem)
        }
      }
    } catch {
      case t: Throwable =>
        logWarning(log"Streaming shuffle spill monitor poll failed: " +
          log"${MDC(ERROR, t.getMessage)}", t)
    }
  }

  /**
   * Sum the sizes of all live (not-yet-spilled) buffers currently in the registry. Reads use
   * [[StreamingBuffer]]'s lock-free atomics, so this scan does not contend with the producer's
   * write path.
   *
   * @return the aggregate number of buffered bytes held on the heap
   */
  private def currentBufferedBytes(): Long = {
    bufferRegistry.asMap().asScala.valuesIterator
      .filterNot(_.isSpilled)
      .map(_.size)
      .sum
  }

  /**
   * Spill the largest buffers to disk until projected utilization drops back below the threshold.
   *
   * Candidates are ordered largest-first (to free the most heap per spill) and, among equal
   * sizes, least-recently-used first (oldest [[StreamingBuffer#lastAccess]]). After the loop the
   * utilization gauge is refreshed to reflect the heap that was reclaimed.
   *
   * @param totalBytes the aggregate buffered bytes observed by the current poll
   * @param maxMem      the on-heap storage memory denominator (guaranteed positive by the caller)
   */
  private def spillToThreshold(totalBytes: Long, maxMem: Long): Unit = {
    val candidates = bufferRegistry.asMap().asScala.toSeq
      .filterNot { case (_, buffer) => buffer.isSpilled }
      .sortBy { case (_, buffer) => (-buffer.size, buffer.lastAccess) }

    var remainingBytes = totalBytes
    val iterator = candidates.iterator
    while (iterator.hasNext && remainingBytes.toDouble / maxMem >= spillThresholdFraction) {
      val (key, buffer) = iterator.next()
      remainingBytes -= spillBuffer(key, buffer)
    }

    val projectedPercent = (remainingBytes.toDouble / maxMem * 100.0).toInt
    metrics.setBufferUtilizationPercent(projectedPercent)
  }

  /**
   * Spill a single buffer's resident bytes to a DISK_ONLY block, releasing its heap, and append
   * the resulting [[SpilledSegment]] to the key's ordered ledger.
   *
   * The drain, the DISK_ONLY write, and the buffer reset are performed atomically under the
   * buffer's monitor by [[StreamingBuffer#spillUnderLock]]: it snapshots the resident bytes
   * (consistent with the buffer's CRC32C), invokes the supplied store callback, and only on a
   * successful store clears the buffer's heap and returns the freed byte count. This is the
   * crucial difference from a naive spill: the heap that the manager exists to protect is
   * actually reclaimed. If the buffer is empty or has already been finalized for commit,
   * [[StreamingBuffer#spillUnderLock]] returns 0 and the store callback is never invoked, so no
   * ledger entry is created and no disk write occurs. If the `putBytes` write fails or throws,
   * the buffer is left intact in memory so no data is lost.
   *
   * @param key    the registry key of the buffer being spilled
   * @param buffer the buffer to spill
   * @return the number of bytes freed (the buffer's pre-spill resident size) on success,
   *         otherwise 0
   */
  def spillBuffer(key: BufferKey, buffer: StreamingBuffer): Long = {
    val freed = buffer.spillUnderLock { snapshot =>
      try {
        val blockId = TempLocalBlockId(UUID.randomUUID())
        val stored = blockManager.putBytes(
          blockId,
          new ChunkedByteBuffer(ByteBuffer.wrap(snapshot.bytes)),
          StorageLevel.DISK_ONLY,
          tellMaster = false)
        if (stored) {
          spillLedger
            .computeIfAbsent(key, _ => new ConcurrentLinkedQueue[SpilledSegment]())
            .add(SpilledSegment(blockId, snapshot.size, snapshot.checksum))
          logDebug(log"Spilled streaming buffer shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
            log"map=${MDC(MAP_ID, key.mapId)} reduce=${MDC(REDUCE_ID, key.partitionId)} " +
            log"(${MDC(NUM_BYTES, snapshot.size)} bytes) to disk block " +
            log"${MDC(BLOCK_ID, blockId)}")
          true
        } else {
          logWarning(log"Failed to spill streaming buffer " +
            log"shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} map=${MDC(MAP_ID, key.mapId)} " +
            log"reduce=${MDC(REDUCE_ID, key.partitionId)} to disk; retaining it in memory")
          false
        }
      } catch {
        case t: Throwable =>
          logWarning(log"Error spilling streaming buffer " +
            log"shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} map=${MDC(MAP_ID, key.mapId)} " +
            log"reduce=${MDC(REDUCE_ID, key.partitionId)} to disk; retaining it in memory", t)
          false
      }
    }
    if (freed > 0L) {
      metrics.incrementSpillCount()
    }
    freed
  }
}

private[spark] object MemorySpillManager {

  /**
   * Opaque registry key that uniquely identifies a single per-partition streaming buffer within
   * an executor. The triple `(shuffleId, mapId, partitionId)` is sufficient to distinguish every
   * in-flight buffer; the spill manager treats it purely as a value-equality key and never
   * interprets its components.
   *
   * @param shuffleId   the shuffle the buffer belongs to
   * @param mapId       the producing map task
   * @param partitionId the reduce partition the buffer accumulates bytes for
   */
  case class BufferKey(shuffleId: Int, mapId: Long, partitionId: Int)

  /**
   * A single overflow segment that a per-partition buffer was spilled to. Each segment records
   * the executor-local DISK_ONLY [[org.apache.spark.storage.BlockId]] the bytes were written to,
   * the number of bytes, and the CRC32C checksum of those bytes (carried over from the buffer
   * snapshot so the producing writer can re-checksum framed output without a second pass). The
   * producing writer reads segments back, in ledger (spill) order, ahead of the buffer's final
   * resident bytes to reconstruct the partition's complete byte stream at commit time.
   *
   * @param blockId  the executor-local DISK_ONLY block the spilled bytes were written to
   * @param length   the number of spilled bytes
   * @param checksum the CRC32C checksum of exactly the spilled bytes
   */
  case class SpilledSegment(blockId: BlockId, length: Long, checksum: Long)
}
