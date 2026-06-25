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
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockManager, StorageLevel, TempLocalBlockId}
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

  import MemorySpillManager.BufferKey

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
   * Registry of live per-partition buffers keyed by an opaque [[BufferKey]]. A Guava cache
   * is used (the same dependency already on the classpath for `IndexShuffleBlockResolver`) for
   * its thread-safe, weakly-consistent iteration semantics: the poll thread can scan `asMap()`
   * while producers concurrently register and reclaim buffers without risking a
   * `ConcurrentModificationException`.
   */
  private val bufferRegistry: Cache[BufferKey, StreamingBuffer] =
    CacheBuilder.newBuilder().build[BufferKey, StreamingBuffer]()

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
      logInfo(s"Started streaming shuffle MemorySpillManager: polling every $POLL_INTERVAL_MS " +
        s"ms, spill threshold $spillThresholdPercent percent")
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
    logTrace(s"Registered streaming buffer $key (partition ${buffer.partitionId})")
  }

  /**
   * Reclaim the buffer associated with `key`, typically in response to a consumer acknowledgment.
   * The buffer's heap is freed via [[StreamingBuffer#reset]] and the mapping is removed from the
   * registry. The elapsed time is measured against the [[RECLAIM_DEADLINE_MS]] SLA and a warning
   * is logged if it is exceeded. Reclaiming an unknown key is a harmless no-op.
   *
   * @param key the buffer key to reclaim
   */
  def reclaim(key: BufferKey): Unit = {
    val startNanos = System.nanoTime()
    val buffer = bufferRegistry.getIfPresent(key)
    if (buffer != null) {
      buffer.reset()
      bufferRegistry.invalidate(key)
      val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
      if (elapsedMs > RECLAIM_DEADLINE_MS) {
        logWarning(s"Reclaim of streaming buffer $key took $elapsedMs ms, exceeding the " +
          s"$RECLAIM_DEADLINE_MS ms deadline")
      } else {
        logTrace(s"Reclaimed streaming buffer $key in $elapsedMs ms")
      }
    } else {
      logTrace(s"Reclaim requested for unknown streaming buffer $key; nothing to do")
    }
  }

  /**
   * Stop the utilization monitor and release all tracked buffers. Shuts the scheduler down
   * immediately, waits briefly for the in-flight poll (if any) to finish, then clears the
   * registry. Idempotent: repeated invocations after the first are no-ops. Invoked by
   * `StreamingShuffleManager.stop()` in the order Backpressure -> Spill -> inner Sort.
   */
  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      scheduler.shutdownNow()
      try {
        if (!scheduler.awaitTermination(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
          logWarning(s"Streaming shuffle spill monitor did not terminate within " +
            s"$POLL_INTERVAL_MS ms of shutdown")
        }
      } catch {
        case _: InterruptedException =>
          // Preserve the interrupt status and stop waiting; shutdownNow has already been issued.
          Thread.currentThread().interrupt()
      }
      bufferRegistry.invalidateAll()
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
        logWarning(s"Streaming shuffle spill monitor poll failed: ${t.getMessage}", t)
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
      remainingBytes -= spillOne(key, buffer)
    }

    val projectedPercent = (remainingBytes.toDouble / maxMem * 100.0).toInt
    metrics.setBufferUtilizationPercent(projectedPercent)
  }

  /**
   * Spill a single buffer to a DISK_ONLY block and reclaim its registry slot.
   *
   * The bytes are snapshotted via the synchronized [[StreamingBuffer#toBytes]] so the on-disk
   * copy is internally consistent with the buffer's checksum. On a successful write the buffer is
   * marked spilled, removed from the registry, and the spill counter is incremented. If the write
   * fails (returns `false`) or throws, the buffer is left intact in memory so no data is lost.
   *
   * @param key    the registry key of the buffer being spilled
   * @param buffer the buffer to spill
   * @return the number of bytes freed (the buffer's pre-spill size) on success, otherwise 0
   */
  private def spillOne(key: BufferKey, buffer: StreamingBuffer): Long = {
    val sizeBefore = buffer.size
    if (sizeBefore <= 0L) {
      // Empty buffer contributes nothing; drop it from the registry without touching disk.
      bufferRegistry.invalidate(key)
      0L
    } else {
      try {
        val bytes = buffer.toBytes
        val blockId = TempLocalBlockId(UUID.randomUUID())
        val stored = blockManager.putBytes(
          blockId,
          new ChunkedByteBuffer(ByteBuffer.wrap(bytes)),
          StorageLevel.DISK_ONLY,
          tellMaster = false)
        if (stored) {
          buffer.markSpilled()
          bufferRegistry.invalidate(key)
          metrics.incrementSpillCount()
          logDebug(s"Spilled streaming buffer $key (${bytes.length} bytes) to disk block " +
            s"$blockId")
          sizeBefore
        } else {
          logWarning(s"Failed to spill streaming buffer $key to disk; retaining it in memory")
          0L
        }
      } catch {
        case t: Throwable =>
          logWarning(s"Error spilling streaming buffer $key to disk; retaining it in memory", t)
          0L
      }
    }
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
}
