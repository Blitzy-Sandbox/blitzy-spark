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
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.annotation.Since
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Memory-pressure monitor and least-recently-used (LRU) disk-spill coordinator for the streaming
 * shuffle backend (`spark.shuffle.manager=streaming`).
 *
 * The streaming writer buffers each reduce partition's output in memory (a [[StreamingBuffer]])
 * so it can be pipelined directly to consumers. To bound that memory, this manager runs a single
 * daemon thread that, every [[POLL_INTERVAL_MS]] milliseconds, measures how full the per-executor
 * buffer budget is and -- when utilization exceeds the configured spill threshold
 * (`spark.shuffle.streaming.spillThreshold`, default 80%) -- evicts buffered partitions to disk
 * until the executor is back under the threshold.
 *
 * ==Spill mechanics==
 *
 * Eviction is largest-first with a least-recently-used tie-break, so the fewest spills reclaim the
 * most memory. Each spilled partition's bytes are snapshotted and persisted through the public
 * [[org.apache.spark.storage.BlockManager]] API at [[StorageLevel.DISK_ONLY]] under standard
 * [[org.apache.spark.storage.ShuffleBlockId]] addressing. No new on-disk format is introduced and
 * the executor memory model is not redesigned: the manager only *consumes* existing public APIs,
 * satisfying the streaming feature's "least modification" discipline.
 *
 * ==Two distinct thresholds==
 *
 * The spill threshold enforced here (default 80%, configurable 50-95) merely moves data to disk;
 * the streaming path stays active. It is intentionally distinct from the separate, higher memory
 * pressure threshold used by `StreamingShuffleFallbackPolicy` (95%) to abandon streaming entirely
 * and fall back to sort-based shuffle. Spilling is a graceful degradation *within* streaming;
 * fallback is a switch *away* from it.
 *
 * ==Reclamation SLA==
 *
 * Consumer acknowledgments reclaim memory synchronously through [[onConsumerAck]], and the poller
 * itself runs on the same 100 ms cadence, so acknowledged or spilled buffers are always released
 * within the 100 ms reclamation SLA mandated by the feature.
 *
 * ==Thread-safety==
 *
 * Registration and acknowledgment calls arrive from map-side task threads while the daemon poller
 * scans concurrently, so the tracked-buffer registry is a `ConcurrentHashMap` and the buffer-budget
 * denominator is a `@volatile` field. The poll body is wrapped so that a transient spill failure
 * can never terminate the scheduled thread and leave memory unmonitored.
 *
 * The [[BlockManager]] is injected (rather than resolved from `SparkEnv`) so this class stays
 * unit-testable and safe to construct in local mode; the caller (the streaming manager) obtains it
 * from `SparkEnv.get.blockManager` and passes it in.
 *
 * @param conf         typed streaming-shuffle configuration accessor (supplies `spillThreshold`)
 * @param blockManager the executor's block manager, used to persist spilled blocks to disk
 * @param metrics      streaming-shuffle telemetry updated on every poll and on every spill
 */
@Since("4.2.0")
private[spark] class MemorySpillManager(
    conf: StreamingShuffleConfig,
    blockManager: BlockManager,
    metrics: StreamingShuffleMetrics)
  extends Logging {

  /**
   * Utilization polling cadence, in milliseconds. This is deliberately identical to the memory
   * reclamation SLA: because the poller fires every 100 ms and consumer acknowledgments reclaim
   * buffers synchronously, spilled or acknowledged memory is always released within 100 ms.
   */
  private val POLL_INTERVAL_MS = 100L

  /**
   * Upper bound on the number of spilled-block presence markers retained. The registry only needs
   * to answer "has this block been spilled?" for the lifetime of a shuffle, so a bounded, self-
   * evicting Guava cache keeps it leak-free even for very long-lived executors.
   */
  private val SPILLED_BLOCKS_CACHE_MAX_SIZE = 100000L

  /**
   * Registry of in-memory buffers currently tracked for possible spill, keyed by the block they
   * hold. [[org.apache.spark.storage.ShuffleBlockId]] is reused as the key because it already
   * carries exactly `(shuffleId, mapId, reduceId)` with correct `hashCode`/`equals`. A
   * [[java.util.concurrent.ConcurrentHashMap]] is used because the writer registers and
   * unregisters buffers from task threads while the poller scans concurrently.
   */
  private val buffers = new ConcurrentHashMap[ShuffleBlockId, StreamingBuffer]()

  /**
   * Registry of blocks already spilled to disk. A Guava `Cache` is used exactly as in
   * `IndexShuffleBlockResolver` (the established precedent for a bounded in-memory shuffle
   * registry), so the set is self-evicting rather than unbounded. The value is an opaque presence
   * marker; only key membership matters.
   */
  private val spilledBlocks: Cache[ShuffleBlockId, java.lang.Boolean] =
    CacheBuilder.newBuilder()
      .maximumSize(SPILLED_BLOCKS_CACHE_MAX_SIZE)
      .build[ShuffleBlockId, java.lang.Boolean]()

  /**
   * Total per-executor streaming-buffer budget, in bytes, against which utilization is measured.
   * The writer sizes buffers as `(executorMemory * bufferSizePercent / 100)`; that same product is
   * the denominator here and is injected via [[setBufferBudgetBytes]] once the executor memory is
   * known. Declared `@volatile` so the poller observes updates without locking. The default of `0`
   * makes [[utilizationPercent]] report `0` so no spill is attempted until a real budget is set.
   */
  @volatile private var _bufferBudgetBytes: Long = 0L

  /** Guards [[start]]/[[stop]] so both are idempotent and the poller is created/destroyed once. */
  private val started = new AtomicBoolean(false)

  /** The daemon thread running [[maybeSpill]]; `null` until [[start]] creates it. */
  private var poller: ScheduledExecutorService = _

  /**
   * Set the total per-executor streaming-buffer budget, in bytes, used as the denominator of the
   * utilization computation. Called once by the writer/manager after the executor's execution
   * memory and `bufferSizePercent` are known. Negative inputs are clamped to `0`.
   *
   * @param bytes the buffer budget in bytes; values below zero are treated as zero
   */
  def setBufferBudgetBytes(bytes: Long): Unit = {
    _bufferBudgetBytes = if (bytes < 0L) 0L else bytes
  }

  /** The current per-executor streaming-buffer budget in bytes (`0` means "not yet configured"). */
  def bufferBudgetBytes: Long = _bufferBudgetBytes

  /**
   * Register a partition buffer so the manager tracks its in-memory size and access time. Called
   * by the streaming writer as it creates per-partition buffers. Re-registering the same block
   * replaces the tracked buffer.
   *
   * @param shuffleId the shuffle the buffer belongs to
   * @param mapId     the map (producer) task whose output the buffer holds
   * @param reduceId  the reduce (consumer) partition the buffer feeds
   * @param buffer    the in-memory buffer to track; must not be null
   */
  def register(shuffleId: Int, mapId: Long, reduceId: Int, buffer: StreamingBuffer): Unit = {
    require(buffer != null, "buffer must not be null")
    buffers.put(ShuffleBlockId(shuffleId, mapId, reduceId), buffer)
  }

  /**
   * Stop tracking a partition buffer without spilling or resetting it. Used when a buffer's data is
   * fully handed off through the normal (non-spill) path and no longer needs memory monitoring.
   */
  def unregister(shuffleId: Int, mapId: Long, reduceId: Int): Unit = {
    buffers.remove(ShuffleBlockId(shuffleId, mapId, reduceId))
  }

  /**
   * Reclaim the memory held by a buffer whose data the consumer has acknowledged. The buffer is
   * removed from tracking and [[StreamingBuffer.reset]] so its backing array becomes eligible for
   * garbage collection. Reclamation is synchronous and therefore well within the 100 ms SLA.
   */
  def onConsumerAck(shuffleId: Int, mapId: Long, reduceId: Int): Unit = {
    val buffer = buffers.remove(ShuffleBlockId(shuffleId, mapId, reduceId))
    if (buffer != null) {
      buffer.reset()
    }
  }

  /**
   * Compute current buffer utilization as an integer percentage in `[0, 100]` of the configured
   * per-executor budget, and publish it to the [[StreamingShuffleMetrics]] gauge so the exported
   * `bufferUtilizationPercent` metric stays fresh on every poll.
   *
   * If the budget has not been configured yet (`0`), utilization is undefined and reported as `0`
   * so that no spill is triggered prematurely. Long intermediates are used to avoid overflow for
   * multi-gigabyte budgets, and the result is clamped defensively into `[0, 100]`.
   *
   * @return the current utilization percentage in `[0, 100]`
   */
  def utilizationPercent(): Int = {
    val budget = _bufferBudgetBytes
    val percent =
      if (budget <= 0L) {
        0
      } else {
        var totalBytes = 0L
        val it = buffers.values().iterator()
        while (it.hasNext) {
          totalBytes += it.next().size
        }
        val raw = totalBytes * 100L / budget
        if (raw < 0L) 0 else if (raw > 100L) 100 else raw.toInt
      }
    metrics.updateBufferUtilization(percent)
    percent
  }

  /**
   * A single utilization poll. If utilization exceeds the configured spill threshold, spill
   * buffered partitions -- largest first, breaking ties by least-recently-used access time -- until
   * utilization falls back to or below the threshold.
   *
   * The entire body is wrapped in a non-fatal guard: a transient spill failure (for example a
   * disk error) must never terminate the scheduled poller, or memory pressure would go unmonitored
   * for the remainder of the executor's life.
   */
  private def maybeSpill(): Unit = {
    try {
      val threshold = conf.spillThreshold
      if (utilizationPercent() > threshold) {
        // Snapshot the tracked buffers, then order them largest-first with an oldest-access
        // tie-break so the least-recently-used partition is evicted first. Sorting a snapshot
        // (rather than the live map) keeps the scan stable while writers mutate the map.
        val candidates = new ArrayBuffer[(ShuffleBlockId, StreamingBuffer)]()
        val it = buffers.entrySet().iterator()
        while (it.hasNext) {
          val entry = it.next()
          candidates += ((entry.getKey, entry.getValue))
        }
        val ordered = candidates.sortBy {
          case (_, buffer) => (-buffer.size, buffer.lastAccessMillis)
        }
        // Spill in order, re-checking utilization after each spill so we stop as soon as the
        // executor is back under the threshold and never over-spill.
        val orderedIt = ordered.iterator
        while (orderedIt.hasNext && utilizationPercent() > threshold) {
          val (blockId, buffer) = orderedIt.next()
          spill(blockId, buffer)
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming-shuffle spill poll failed; will retry next interval", e)
    }
  }

  /**
   * Persist a single partition buffer to disk and reclaim its heap.
   *
   * The buffer's bytes are snapshotted (a defensive copy that also refreshes the LRU timestamp),
   * wrapped in a [[ChunkedByteBuffer]], and written via the public [[BlockManager.putBytes]] API at
   * [[StorageLevel.DISK_ONLY]]. `putBytes` is generic on `T: ClassTag`; because the payload is
   * opaque bytes, an explicit `ClassTag.Any` is supplied. The in-memory buffer is then reset (which
   * releases its backing array), removed from tracking, and recorded in the spilled-block registry
   * so the reader/resolver can locate it on disk. An empty buffer is simply dropped from tracking.
   */
  private def spill(blockId: ShuffleBlockId, buffer: StreamingBuffer): Unit = {
    val bytes = buffer.snapshot()
    if (bytes.length == 0) {
      // Nothing to persist; drop it from tracking so it no longer counts toward memory use.
      buffers.remove(blockId)
    } else {
      val chunked = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))
      blockManager.putBytes(blockId, chunked, StorageLevel.DISK_ONLY)(ClassTag.Any)
      buffer.reset()
      buffers.remove(blockId)
      spilledBlocks.put(blockId, java.lang.Boolean.TRUE)
      metrics.incSpillCount()
      logInfo(log"Spilled streaming shuffle partition to disk " +
        log"(shuffleId=${MDC(LogKeys.SHUFFLE_ID, blockId.shuffleId)}, " +
        log"mapId=${MDC(LogKeys.MAP_ID, blockId.mapId)}, " +
        log"reduceId=${MDC(LogKeys.REDUCE_ID, blockId.reduceId)}, " +
        log"bytes=${MDC(LogKeys.NUM_BYTES, bytes.length)})")
    }
  }

  /**
   * Start the background utilization poller. Idempotent: repeated calls have no effect until
   * [[stop]] is called. The poller is a single daemon thread (so it never blocks JVM shutdown)
   * named `streaming-spill-poller`, scheduled at the fixed [[POLL_INTERVAL_MS]] cadence.
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      val threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("streaming-spill-poller")
        .build()
      poller = Executors.newSingleThreadScheduledExecutor(threadFactory)
      poller.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = maybeSpill()
      }, POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)
      logInfo(log"Started streaming-shuffle memory spill manager")
    }
  }

  /**
   * Stop the background poller and release tracking state. Idempotent: safe to call multiple times
   * and safe to call even if [[start]] was never invoked. The poller is shut down gracefully and
   * then forcibly if it does not terminate promptly; the tracked-buffer and spilled-block
   * registries are cleared.
   */
  def stop(): Unit = {
    if (started.compareAndSet(true, false)) {
      val p = poller
      if (p != null) {
        p.shutdown()
        try {
          if (!p.awaitTermination(POLL_INTERVAL_MS * 10, TimeUnit.MILLISECONDS)) {
            p.shutdownNow()
          }
        } catch {
          case _: InterruptedException =>
            p.shutdownNow()
            Thread.currentThread().interrupt()
        }
        poller = null
      }
      buffers.clear()
      spilledBlocks.invalidateAll()
      logInfo(log"Stopped streaming-shuffle memory spill manager")
    }
  }

  /**
   * Whether the given partition's data has been spilled to disk. Consumed by the reader/resolver
   * to decide whether to read from the in-memory buffer or from block storage.
   */
  def isSpilled(shuffleId: Int, mapId: Long, reduceId: Int): Boolean = {
    spilledBlocks.getIfPresent(ShuffleBlockId(shuffleId, mapId, reduceId)) != null
  }
}
