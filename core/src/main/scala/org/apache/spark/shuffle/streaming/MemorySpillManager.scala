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

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleDataBlockId, StorageLevel}
import org.apache.spark.util.ThreadUtils

/**
 * The memory-pressure relief valve for the streaming shuffle backend.
 *
 * The streaming writer buffers per-partition map output in memory (one [[StreamingBuffer]] per
 * `(shuffleId, mapId, partitionId)` triple) so it can be pipelined directly to reduce-side
 * consumers without first materializing the whole shuffle to local disk. Those buffers are bounded,
 * but a slow or stalled consumer can let the executor's aggregate buffered footprint grow until it
 * threatens an OutOfMemoryError. This class is the bounded-footprint guarantor: it polls the
 * aggregate buffer utilization every [[StreamingShuffleConfig.SPILL_POLL_INTERVAL_MS]] (100 ms),
 * once utilization reaches the configured spill threshold (default 80%), spills the largest
 * partitions to disk to reclaim heap, all within the 100 ms reclamation SLA.
 *
 * ==Spill denominator==
 *
 * Aggregate utilization is measured against [[MemoryManager.maxOnHeapStorageMemory]] -- the on-heap
 * storage memory budget, the denominator mandated by the design. The numerator is the sum
 * of every registered buffer's [[StreamingBuffer.size]]. Utilization is therefore
 * `sum(buffer.size) / memoryManager.maxOnHeapStorageMemory`, surfaced as a percentage through
 * [[StreamingShuffleMetrics.setBufferUtilizationPercent]] on every poll tick so the
 * `shuffle.streaming.bufferUtilizationPercent` gauge always reflects the live footprint.
 *
 * ==Spill victim selection: largest first, LRU tie-break==
 *
 * When a spill is required, candidates are ordered by [[StreamingBuffer.size]] descending so the
 * largest partitions are evicted first (reclaiming the most heap per disk write), with the oldest
 * [[StreamingBuffer.lastAccess]] breaking ties so a least-recently-used buffer wins. Buffers
 * are spilled in that order only until aggregate utilization drops back under the threshold, never
 * more than necessary.
 *
 * ==On-disk block-id scheme==
 *
 * Each spilled buffer is written through the existing [[BlockManager.putBytes]] API at
 * [[StorageLevel.DISK_ONLY]] under a deterministic [[ShuffleDataBlockId]] of
 * `(shuffleId, mapId, partitionId)`. This scheme is chosen deliberately: the BlockManager routes
 * fetches of any shuffle block to `shuffleManager.shuffleBlockResolver.getBlockData`, so a buffer
 * spilled under its `ShuffleDataBlockId` is transparently served (and migrated) by
 * `StreamingShuffleBlockResolver` with no extra lookup, and the bytes land in the canonical
 * `shuffle_<shuffleId>_<mapId>_<partitionId>.data` file. The spilled block ids are also tracked in
 * [[spilledBlockId]] so the resolver can enumerate what has been spilled. Blocks are stored with
 * `tellMaster = false`: these are transient, locally-served spill blocks tracked by the resolver;
 * the [[org.apache.spark.storage.BlockManagerMaster]] is intentionally not notified.
 *
 * ==Coexistence with sort-based shuffle (least-modification)==
 *
 * This class adds no new storage machinery and never alters the storage interface contract: it
 * reuses [[BlockManager.putBytes]] and [[StorageLevel.DISK_ONLY]] exactly as the sort-based path
 * relies on them. Buffers and disk spills live entirely inside the streaming subsystem; when the
 * streaming backend falls back to `SortShuffleManager`, this manager simply holds no buffers and
 * never spills.
 *
 * ==Thread-safety==
 *
 * Buffer registration, the spill registry, and the utilization gauge are all lock-free
 * ([[ConcurrentHashMap]] plus the atomic [[StreamingShuffleMetrics]]), so the producing map task's
 * append path is never blocked by the poll loop. A single coarse monitor ([[spillLock]]) serializes
 * spill *episodes* only -- it guards which buffers are drained, not producer appends -- so two
 * concurrent triggers (the poll thread and an on-demand caller) can never double-spill the same
 * partition.
 *
 * @param conf          the typed streaming-shuffle configuration (threshold, poll cadence, debug)
 * @param blockManager  the executor [[BlockManager]] used to persist spilled buffers to disk
 * @param memoryManager [[MemoryManager]] whose `maxOnHeapStorageMemory` is the spill denominator
 * @param metrics       the shared telemetry holder updated with the gauge and spill counter
 * @param resolver       the streaming block resolver to notify on spill completion so a reduce-side
 *                       fetch is served from the on-disk copy (and never from the cleared in-memory
 *                       buffer); `None` only in unit tests that exercise the spill loop alone
 * @param fallbackPolicy the manager-owned zero-regression decision object. Every utilization
 *                       recomputation (each 100 ms poll tick and every on-demand `maybeSpill`)
 *                       pushes the fresh buffer-memory-utilization percentage into it, so its
 *                       memory-pressure revert condition observes real buffer growth. This is the
 *                       production signal wiring for the memory-pressure automatic fallback
 *                       condition. `None` only in isolation unit tests; the manager always supplies
 *                       its own policy instance so production fallback is wired end-to-end.
 */
private[spark] class MemorySpillManager(
    conf: StreamingShuffleConfig,
    blockManager: BlockManager,
    memoryManager: MemoryManager,
    metrics: StreamingShuffleMetrics,
    resolver: Option[StreamingShuffleBlockResolver] = None,
    fallbackPolicy: Option[StreamingShuffleFallbackPolicy] = None)
  extends Logging {

  import MemorySpillManager._

  /** Live, in-memory buffers eligible for spill, keyed by their shuffle coordinates. */
  private val liveBuffers = new ConcurrentHashMap[BufferKey, StreamingBuffer]()

  /** The on-disk block id recorded for each buffer already spilled, for the resolver to serve. */
  private val spilledBlocks = new ConcurrentHashMap[BufferKey, BlockId]()

  /** Daemon executor that runs the periodic 100 ms spill-poll loop. */
  private val poller = ThreadUtils.newDaemonSingleThreadScheduledExecutor(POLL_THREAD_NAME)

  /** Handle to the scheduled poll task so [[stop]] can cancel it; null until [[start]] runs. */
  @volatile private var pollFuture: ScheduledFuture[_] = _

  /** Whether the poll loop is currently scheduled; makes [[start]] / [[stop]] idempotent. */
  private val started = new AtomicBoolean(false)

  /** Serializes spill episodes; deliberately never held on the producer append path. */
  private val spillLock = new Object()

  /** The recurring poll action; one instance is reused across every tick. */
  private val pollingTask = new Runnable {
    override def run(): Unit = pollOnce()
  }

  /**
   * Registers a buffer so it participates in utilization accounting and is eligible for spill. The
   * buffer is keyed by its `(shuffleId, mapId, partitionId)` triple; re-registering the same key
   * replaces the previous mapping.
   *
   * @param buffer the per-partition buffer to track
   */
  def register(buffer: StreamingBuffer): Unit = {
    liveBuffers.put(keyFor(buffer), buffer)
  }

  /**
   * Stops tracking the buffer identified by `key`, removing it from accounting and spill
   * eligibility. The on-disk lifecycle of any already-spilled block is owned by the resolver and is
   * intentionally left untouched here.
   *
   * @param key the `(shuffleId, mapId, partitionId)` identity of the buffer to drop
   */
  def unregister(key: BufferKey): Unit = {
    liveBuffers.remove(key)
  }

  /** The number of buffers currently registered for spill accounting. */
  def registeredBufferCount: Int = liveBuffers.size()

  /** True once the periodic poll loop has been started and not yet stopped. */
  def isStarted: Boolean = started.get()

  /** True if the buffer identified by `key` has already been spilled to disk. */
  def isSpilled(key: BufferKey): Boolean = spilledBlocks.containsKey(key)

  /**
   * The on-disk [[BlockId]] under which `key` was spilled, if any. Lets the
   * `StreamingShuffleBlockResolver` resolve a spilled partition to the block it must serve.
   *
   * @param key the `(shuffleId, mapId, partitionId)` identity to look up
   * @return `Some(blockId)` when the partition has been spilled, otherwise `None`
   */
  def spilledBlockId(key: BufferKey): Option[BlockId] = Option(spilledBlocks.get(key))

  /**
   * A snapshot of aggregate buffer utilization as a percentage in `[0, 100]`, measured
   * against [[MemoryManager.maxOnHeapStorageMemory]]. Exposed for tests; the poll loop
   * publishes the same value to the metrics gauge on every tick.
   *
   * @return the current aggregate utilization percentage
   */
  def aggregateUtilizationPercent: Double = {
    utilizationPercentOf(totalBufferedBytes(), memoryManager.maxOnHeapStorageMemory)
  }

  /**
   * Recomputes aggregate utilization, refreshes the buffer-utilization gauge, and spills the
   * largest / least-recently-used buffers to disk if utilization has reached the configured spill
   * threshold. Safe to call on demand (e.g. from the consumer-failure resume path) in addition to
   * the periodic poll. The gauge is always refreshed, even when no spill is needed.
   *
   * @return the number of heap bytes reclaimed by spilling during this call (0 if none)
   */
  def maybeSpill(): Long = {
    val denominator = memoryManager.maxOnHeapStorageMemory
    val total = totalBufferedBytes()
    val utilizationPercent = utilizationPercentOf(total, denominator)
    metrics.setBufferUtilizationPercent(utilizationPercent)
    // Production fallback wiring: push the live buffer-memory utilization into the manager-owned
    // policy so its memory-pressure revert condition (an OOM risk for buffer allocation) observes
    // real buffer growth. A non-positive denominator yields 0%, so this never trips spuriously.
    fallbackPolicy.foreach(_.updateMemoryUtilization(math.round(utilizationPercent).toInt))
    if (denominator <= 0L) {
      // No meaningful budget to measure against; nothing to spill.
      return 0L
    }
    if (total.toDouble / denominator < conf.spillThresholdFraction) {
      // Below the spill threshold -- leave every buffer in memory for low-latency streaming.
      return 0L
    }
    spillLock.synchronized {
      doSpillEpisode(denominator)
    }
  }

  /**
   * Spills one registered buffer to disk on demand, regardless of the aggregate threshold. Used
   * by the consumer-failure resume path to proactively persist unacked data. A buffer that is
   * missing, empty, or already spilled is a no-op.
   *
   * @param key the `(shuffleId, mapId, partitionId)` identity of the buffer to spill
   * @return true if the buffer was spilled to disk by this call
   */
  def spillBuffer(key: BufferKey): Boolean = spillLock.synchronized {
    val buffer = liveBuffers.get(key)
    if (buffer == null || buffer.size == 0L || spilledBlocks.containsKey(key)) {
      false
    } else {
      spillSingle(key, buffer) > 0L
    }
  }

  /**
   * Starts the periodic spill-poll loop on a daemon thread, scheduled every
   * [[StreamingShuffleConfig.SPILL_POLL_INTERVAL_MS]] (100 ms). Idempotent: repeated calls while
   * already running are no-ops. A fixed delay (not rate) is used so a slow spill episode can never
   * cause overlapping poll runs to pile up.
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      val intervalMs = StreamingShuffleConfig.SPILL_POLL_INTERVAL_MS
      pollFuture = poller.scheduleWithFixedDelay(
        pollingTask, intervalMs, intervalMs, TimeUnit.MILLISECONDS)
      logInfo(log"Started streaming shuffle spill manager; polling every " +
        log"${MDC(DURATION, intervalMs)} ms")
    }
  }

  /**
   * Stops the poll loop and shuts the daemon executor down cleanly so no thread is leaked (verified
   * under `spark.unsafe.exceptionOnMemoryLeak=true`). Idempotent and safe to call even if [[start]]
   * was never invoked. The scheduled task is cancelled first so executor termination returns
   * promptly.
   */
  def stop(): Unit = {
    started.set(false)
    Option(pollFuture).foreach(_.cancel(false))
    pollFuture = null
    ThreadUtils.shutdown(poller)
  }

  /**
   * A poll iteration: refresh the gauge and spill if needed. Any failure is caught and logged
   * rather than propagated, because an exception escaping a scheduled task would cancel all
   * future polls.
   */
  private def pollOnce(): Unit = {
    try {
      maybeSpill()
    } catch {
      case NonFatal(e) =>
        logWarning("Streaming shuffle spill poll iteration failed; retrying on next tick", e)
    }
  }

  /**
   * Runs one spill episode under [[spillLock]]: spill the largest / least-recently-used buffers, in
   * order, only until aggregate utilization drops back below the spill threshold. Emits exactly one
   * summary log line per episode (not one per buffer) to respect the per-executor log budget, and
   * notes -- at debug level only -- any episode that overruns the 100 ms reclaim SLA.
   *
   * @param denominator the spill denominator ([[MemoryManager.maxOnHeapStorageMemory]]); positive
   * @return the number of heap bytes reclaimed during this episode
   */
  private def doSpillEpisode(denominator: Long): Long = {
    val startNanos = System.nanoTime()
    val totalBefore = totalBufferedBytes()
    // Resident bytes that keep utilization just under the threshold, and how much to reclaim.
    val targetResident = (conf.spillThresholdFraction * denominator).toLong
    val bytesToReclaim = math.max(0L, totalBefore - targetResident)
    if (bytesToReclaim == 0L) {
      return 0L
    }
    var reclaimed = 0L
    var spilledCount = 0
    val candidates = selectCandidates().iterator
    while (candidates.hasNext && reclaimed < bytesToReclaim) {
      val (key, buffer) = candidates.next()
      val bytes = spillSingle(key, buffer)
      if (bytes > 0L) {
        reclaimed += bytes
        spilledCount += 1
      }
    }
    // Refresh the gauge so it reflects the post-spill resident footprint immediately.
    metrics.setBufferUtilizationPercent(
      utilizationPercentOf(totalBufferedBytes(), denominator))
    val elapsedMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI
    if (spilledCount > 0) {
      logInfo(log"Streaming shuffle spilled ${MDC(COUNT, spilledCount)} buffer(s), " +
        log"reclaimed ${MDC(NUM_BYTES, reclaimed)} bytes in ${MDC(DURATION, elapsedMs)} ms")
    }
    if (elapsedMs > StreamingShuffleConfig.SPILL_RECLAIM_SLA_MS && conf.debug) {
      logDebug(log"Streaming shuffle spill episode took ${MDC(DURATION, elapsedMs)} ms, " +
        log"exceeding the 100 ms reclaim SLA")
    }
    reclaimed
  }

  /**
   * Snapshots the spill candidates -- buffers that hold bytes and are not already spilled
   * -- ordered largest first with the oldest [[StreamingBuffer.lastAccess]] breaking ties (LRU).
   *
   * @return the ordered candidate `(key, buffer)` pairs
   */
  private def selectCandidates(): Seq[(BufferKey, StreamingBuffer)] = {
    liveBuffers.entrySet().asScala.iterator
      .filter(e => e.getValue.size > 0L && !spilledBlocks.containsKey(e.getKey))
      .map(e => (e.getKey, e.getValue))
      .toVector
      .sortBy { case (_, buffer) => (-buffer.size, buffer.lastAccess) }
  }

  /**
   * Spills one buffer's bytes to disk via [[BlockManager.putBytes]] at [[StorageLevel.DISK_ONLY]]
   * under its deterministic [[ShuffleDataBlockId]], then reclaims the heap with
   * [[StreamingBuffer.clear]]. The snapshot is taken with [[StreamingBuffer.toChunkedByteBuffer]],
   * which holds its own references, so clearing the buffer afterwards cannot corrupt the in-flight
   * disk write. On a storage failure the buffer is left intact (no data loss) and nothing is
   * recorded as spilled.
   *
   * @param key    the buffer's `(shuffleId, mapId, partitionId)` identity
   * @param buffer the buffer to spill
   * @return the number of bytes persisted (and thus reclaimable), or 0 if nothing was spilled
   */
  private def spillSingle(key: BufferKey, buffer: StreamingBuffer): Long = {
    val blockId = ShuffleDataBlockId(key.shuffleId, key.mapId, key.partitionId)
    val chunked = buffer.toChunkedByteBuffer
    val bytes = chunked.size
    if (bytes == 0L) {
      return 0L
    }
    val stored =
      try {
        blockManager.putBytes[Array[Byte]](blockId, chunked, StorageLevel.DISK_ONLY,
          tellMaster = false)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to spill streaming shuffle buffer to disk for block " +
            log"${MDC(BLOCK_ID, blockId)}", e)
          false
      }
    if (!stored) {
      return 0L
    }
    // Bridge spill completion into the resolver's state BEFORE clearing the buffer so a concurrent
    // reduce-side fetch can never observe a cleared in-memory buffer: trackSpill records the disk
    // copy as authoritative and drops the resolver's in-memory entry, after which getBlockData
    // resolves this partition from the spilled file. Doing this before clear() preserves zero data
    // loss across the in-memory -> disk handoff.
    resolver.foreach(_.trackSpill(key.shuffleId, key.mapId, key.partitionId, blockId))
    spilledBlocks.put(key, blockId)
    metrics.incSpillCount()
    buffer.clear()
    if (conf.debug) {
      logDebug(log"Spilled streaming buffer shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
        log"map=${MDC(MAP_ID, key.mapId)} partition=${MDC(PARTITION_ID, key.partitionId)} " +
        log"(${MDC(NUM_BYTES, bytes)} bytes) to block ${MDC(BLOCK_ID, blockId)}")
    }
    bytes
  }

  /** Sum of buffered bytes across every registered buffer; lock-free, never blocks appends. */
  private def totalBufferedBytes(): Long = {
    var sum = 0L
    val it = liveBuffers.values().iterator()
    while (it.hasNext) {
      sum += it.next().size
    }
    sum
  }

  /** Aggregate utilization of `totalBytes` against `denominator`, as a percentage in `[0, 100]`. */
  private def utilizationPercentOf(totalBytes: Long, denominator: Long): Double = {
    if (denominator <= 0L) {
      0.0
    } else {
      math.min(100.0, totalBytes * 100.0 / denominator)
    }
  }
}

/**
 * Companion holding the spill manager's identity key, its construction helper, and the few
 * constants the manager references.
 */
private[spark] object MemorySpillManager {

  /** Name of the daemon thread that runs the spill-poll loop. */
  private val POLL_THREAD_NAME = "streaming-shuffle-spill"

  /** Nanoseconds per millisecond, used to render spill-episode durations for the SLA check. */
  private val NANOS_PER_MILLI = 1000000L

  /**
   * The identity of a registered buffer: the `(shuffleId, mapId, partitionId)` triple that uniquely
   * names a per-partition streaming buffer and, once spilled, its [[ShuffleDataBlockId]].
   *
   * @param shuffleId   the shuffle the buffer belongs to
   * @param mapId       the map task that produced the buffered output
   * @param partitionId the reduce partition the buffered bytes are destined for
   */
  case class BufferKey(shuffleId: Int, mapId: Long, partitionId: Int)

  /**
   * Derives the [[BufferKey]] identity for a buffer from its shuffle coordinates.
   *
   * @param buffer the buffer to key
   * @return the buffer's `(shuffleId, mapId, partitionId)` key
   */
  def keyFor(buffer: StreamingBuffer): BufferKey = {
    BufferKey(buffer.shuffleId, buffer.mapId, buffer.partitionId)
  }
}
