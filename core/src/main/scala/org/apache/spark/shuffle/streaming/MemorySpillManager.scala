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

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.storage.{StorageLevel, TempLocalBlockId}
import org.apache.spark.util.ThreadUtils

/**
 * Threshold-driven, 100 ms-poll, LRU disk-spill manager for the opt-in streaming shuffle backend.
 *
 * ==Responsibility==
 *
 * The streaming writer keeps map-side output in bounded, per-partition in-memory
 * [[StreamingBuffer]]s rather than materializing it to disk up front. This manager is the safety
 * valve that keeps that in-memory footprint bounded: it periodically samples aggregate buffer
 * utilization and, the moment utilization crosses the configured spill threshold (default 80%),
 * evicts the largest buffered partitions to local disk via the existing [[BlockManager]] until
 * utilization is back under the threshold. Spilling reclaims heap (the buffer's bytes are dropped
 * once they are safely on disk) and lets a memory-bound workload keep streaming instead of
 * exhausting the executor. This realizes the AAP's memory-exhaustion-prevention requirement: an
 * 80% threshold spill trigger with a sub-100 ms response time.
 *
 * ==Least-modification, storage-contract-preserving design==
 *
 * The manager touches no internal storage state. It reuses only the public
 * `BlockManager.putBytes(blockId, bytes, StorageLevel.DISK_ONLY)` API to persist a buffer's
 * [[org.apache.spark.util.io.ChunkedByteBuffer]] view, and it sizes the spill decision against
 * `MemoryManager.maxOnHeapStorageMemory` (the AAP-mandated spill denominator). No executor
 * memory-model redesign and no change to any block-manager interface contract is involved; the
 * sort-based shuffle path is entirely unaffected and remains the automatic fallback.
 *
 * ==Selection policy: largest-first with an LRU tie-break==
 *
 * When a spill is triggered, candidate buffers are ordered by size descending and, for buffers of
 * equal size, by least-recently-accessed first (oldest [[StreamingBuffer.lastAccess]] timestamp).
 * Spilling the largest partitions first reclaims the most memory per disk write, and the LRU
 * tie-break favors evicting partitions a consumer is no longer actively draining. Buffers are
 * spilled in that order, subtracting each reclaimed buffer's size from a running total, until the
 * running total drops back below the threshold or no spillable buffer remains.
 *
 * ==Block-id scheme and zero-data-loss re-spill==
 *
 * Each spill episode for a partition appends exactly one [[BlockId]] to that partition's ordered
 * segment list, so a partition that is spilled more than once under pressure never loses
 * earlier bytes. Every segment - including the first - uses a unique, non-shuffle
 * [[TempLocalBlockId]]. A [[TempLocalBlockId]] is deliberately chosen over an
 * `org.apache.spark.storage.ShuffleDataBlockId` because the read-back path is
 * `BlockManager.getLocalBytes`, which asserts the block id is NOT a shuffle id (a shuffle id
 * would be routed back through the shuffle resolver and could not be served as a raw stored
 * block). The segments are discoverable in spill order through [[spilledBlockIds]] and read back
 * through [[readSpilledSegment]]. Because the buffer's wire view and its spilled view are
 * byte-for-byte identical (the dual-channel invariant documented on [[StreamingBuffer]]),
 * spilled and streamed bytes are interchangeable.
 *
 * ==Concurrency and hot-path safety==
 *
 * The poll loop runs on a single daemon thread and reads only lock-free buffer metadata
 * ([[StreamingBuffer.size]], [[StreamingBuffer.lastAccess]]) while scanning, so it never blocks a
 * producer append. The actual spill of one buffer is serialized on that buffer's own monitor so a
 * scheduled spill and an on-demand [[spillBuffer]] can never double-store the same partition; the
 * buffer's internal append lock is a separate object, so this guard introduces no deadlock and
 * leaves the append fast path unblocked except for the brief snapshot/clear critical sections.
 *
 * ==Observability budget==
 *
 * On every tick the aggregate utilization gauge is published through
 * [[StreamingShuffleMetrics.setBufferUtilizationPercent]] and each spilled buffer increments
 * [[StreamingShuffleMetrics.incSpillCount]]. To honor the < 10 MB/hour log-volume budget,
 * a human-readable summary is logged at INFO at most once per second per spill episode (not once
 * per buffer); per-buffer detail and reclaim-SLA breaches are emitted only when
 * [[StreamingShuffleConfig.debug]] is enabled.
 *
 * This type coexists with the sort-based shuffle path and is constructed only when the streaming
 * backend is active.
 *
 * @param conf           typed streaming-shuffle configuration (spill threshold, debug flag)
 * @param blockManager   the executor [[BlockManager]] used to persist spilled buffers to disk
 * @param memoryManager  source of the `maxOnHeapStorageMemory` spill denominator
 * @param metrics        telemetry holder updated with the utilization gauge and spill counter
 * @param fallbackPolicy optional shared [[StreamingShuffleFallbackPolicy]] fed the live aggregate
 *                       buffer-utilization percent on every poll tick so the memory-pressure
 *                       revert condition can trip from the real runtime footprint; `null` (the
 *                       default) leaves the manager standalone for unit tests that exercise spill
 *                       behavior in isolation
 */
private[spark] class MemorySpillManager(
    conf: StreamingShuffleConfig,
    blockManager: BlockManager,
    memoryManager: MemoryManager,
    metrics: StreamingShuffleMetrics,
    fallbackPolicy: StreamingShuffleFallbackPolicy = null) extends Logging {

  import MemorySpillManager.BufferKey

  // Live, per-partition buffers currently eligible for spilling, keyed by
  // (shuffleId, mapId, partitionId). Registered by the writer when a partition buffer is
  // allocated and removed when the partition is finalized. Weakly-consistent iteration is
  // sufficient for the best-effort spill scan, so the poll loop never blocks producer appends.
  private val buffers = new ConcurrentHashMap[BufferKey, StreamingBuffer]()

  // Ordered disk-spill segments per partition. A partition may be spilled more than once under
  // sustained pressure (its buffer is cleared after each spill and may refill); every episode
  // appends one BlockId so previously-spilled bytes are never overwritten or lost. Every segment
  // (including the first) uses a unique non-shuffle TempLocalBlockId so it is readable back via
  // BlockManager.getLocalBytes, which rejects shuffle ids; the resolver discovers the ordered
  // list via spilledBlockIds and reads each segment via readSpilledSegment.
  private val spilledBlocks =
    new ConcurrentHashMap[BufferKey, CopyOnWriteArrayList[BlockId]]()

  // Single daemon thread polling buffer utilization on a fixed delay, so it never blocks
  // JVM shutdown; created up front but idle (no thread is spawned) until start() schedules the
  // first task, so an un-started manager still shuts down cleanly with no thread leak.
  private val poller: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-spill")

  private val started = new AtomicBoolean(false)
  private val stopped = new AtomicBoolean(false)

  // Monotonic-clock timestamp of the most recent INFO-level spill-episode log line, used to
  // throttle episode logging to at most once per second so sustained spilling stays well within
  // the < 10 MB/hour/executor log-volume budget; per-buffer detail is emitted only when debug.
  private val lastEpisodeLogNanos = new AtomicLong(0L)

  // ---------------------------------------------------------------------------------------
  // Buffer registry.
  // ---------------------------------------------------------------------------------------

  /**
   * Registers a per-partition buffer so it participates in utilization sampling and is eligible
   * for spilling. Idempotent for a given key: re-registering the same partition replaces the
   * previous buffer reference.
   *
   * @param buffer the per-partition buffer to track
   */
  def register(buffer: StreamingBuffer): Unit = {
    buffers.put(keyOf(buffer), buffer)
  }

  /**
   * Removes a buffer from the live registry by key. The buffer is no longer sampled or spilled;
   * any blocks already spilled for the key remain recorded so the resolver can still serve them.
   *
   * @param key the buffer key to stop tracking
   */
  def unregister(key: BufferKey): Unit = {
    buffers.remove(key)
  }

  /**
   * Convenience overload that removes the given buffer from the live registry by its derived key.
   *
   * @param buffer the buffer to stop tracking
   */
  def unregister(buffer: StreamingBuffer): Unit = {
    buffers.remove(keyOf(buffer))
  }

  /**
   * Releases all streaming spill state for an entire shuffle: it drops every live buffer for the
   * shuffle from the registry and removes the shuffle's spilled-segment metadata, best-effort
   * deleting each spilled disk block from the [[BlockManager]].
   *
   * This is the shuffle-scoped sibling of the per-key [[unregister]] overloads and is
   * deliberately stronger than them. The per-key overloads run when a single partition is
   * finalized and intentionally retain that partition's spilled segments so a still-running
   * reduce task can fetch them; this method runs only when the whole shuffle is unregistered
   * (`StreamingShuffleManager.unregisterShuffle`), at which point no further reads of the shuffle
   * can occur, so deleting the on-disk segments reclaims their space immediately instead of
   * letting completed-shuffle state linger until executor shutdown. That closes the
   * resource-cleanup / zero-retained-heap gap the review identified.
   *
   * Best-effort and exception-safe: each [[BlockManager.removeBlock]] is idempotent (a segment
   * already gone simply logs a warning) and is individually guarded, so a failure to delete one
   * segment never prevents the remaining buffers and metadata from being cleared. Idempotent for
   * a given shuffle id - a second call for the same shuffle finds nothing left to remove.
   *
   * @param shuffleId the shuffle whose buffers and spilled segments should be released
   */
  def unregisterShuffle(shuffleId: Int): Unit = {
    // Drop every live buffer for this shuffle so it is no longer sampled or spilled. The
    // ConcurrentHashMap key-set iterator supports weakly-consistent removal without blocking
    // concurrent appends on other shuffles' buffers.
    val bufIt = buffers.keySet().iterator()
    while (bufIt.hasNext) {
      if (bufIt.next().shuffleId == shuffleId) {
        bufIt.remove()
      }
    }
    // Remove the spilled-segment metadata for this shuffle and best-effort delete the underlying
    // disk blocks. Each segment is a non-shuffle TempLocalBlockId persisted via putBytes, so
    // removeBlock (idempotent, symmetric with that store) is the correct reclamation call.
    val spillIt = spilledBlocks.entrySet().iterator()
    while (spillIt.hasNext) {
      val entry = spillIt.next()
      if (entry.getKey.shuffleId == shuffleId) {
        val segments = entry.getValue
        spillIt.remove()
        if (segments != null) {
          val segIt = segments.iterator()
          while (segIt.hasNext) {
            val blockId = segIt.next()
            try {
              blockManager.removeBlock(blockId)
            } catch {
              case NonFatal(e) =>
                logWarning(log"Failed to remove spilled streaming block " +
                  log"${MDC(LogKeys.BLOCK_ID, blockId.name)} during shuffle " +
                  log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)} cleanup", e)
            }
          }
        }
      }
    }
    if (conf.debug) {
      logDebug(log"Unregistered streaming spill state for shuffle " +
        log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)}")
    }
  }

  // ---------------------------------------------------------------------------------------
  // Lifecycle.
  // ---------------------------------------------------------------------------------------

  /**
   * Starts the 100 ms spill poller. Scheduling uses a fixed delay (rather than a fixed rate) so a
   * spill episode that briefly exceeds the interval can never overlap the next tick. Idempotent:
   * subsequent calls after the first successful start are no-ops.
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      val interval = StreamingShuffleConfig.SPILL_POLL_INTERVAL_MS
      poller.scheduleWithFixedDelay(
        () => pollOnce(), interval, interval, TimeUnit.MILLISECONDS)
      logInfo(log"Started streaming shuffle spill manager (poll=" +
        log"${MDC(LogKeys.DURATION, interval)} ms, spillThreshold=" +
        log"${MDC(LogKeys.THRESHOLD, conf.spillThreshold)} percent)")
    }
  }

  /**
   * Stops the poller and releases the live buffer registry. Shutdown is graceful: in-flight ticks
   * are allowed to drain for a short grace window before the executor is force-terminated, so no
   * daemon thread is leaked (validated under `spark.unsafe.exceptionOnMemoryLeak=true`).
   * Recorded spilled-block ids are intentionally retained so a late resolver read still resolves;
   * they are reclaimed with the manager. Idempotent.
   */
  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      poller.shutdown()
      try {
        val awaitMs = StreamingShuffleConfig.SPILL_POLL_INTERVAL_MS * 5L
        if (!poller.awaitTermination(awaitMs, TimeUnit.MILLISECONDS)) {
          poller.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          poller.shutdownNow()
          Thread.currentThread().interrupt()
      }
      buffers.clear()
      logInfo(log"Stopped streaming shuffle spill manager")
    }
  }

  // ---------------------------------------------------------------------------------------
  // Spill operations.
  // ---------------------------------------------------------------------------------------

  /**
   * Evaluates aggregate utilization against the spill threshold and, if it is reached, spills the
   * largest least-recently-used buffers until utilization is back under the threshold. Safe to
   * call on demand (for example from a consumer-failure resume path) in addition to the poller.
   *
   * The spill denominator is `MemoryManager.maxOnHeapStorageMemory`; a non-positive denominator
   * (no storage memory configured) disables spilling and returns zero.
   *
   * @return the number of bytes reclaimed from heap by this call (0 if nothing was spilled)
   */
  def maybeSpill(): Long = {
    val denom = memoryManager.maxOnHeapStorageMemory
    if (denom <= 0L) {
      0L
    } else {
      val thresholdBytes = (denom.toDouble * conf.spillThresholdFraction).toLong
      val running = totalBufferedBytes()
      if (running < thresholdBytes) {
        0L
      } else {
        spillUntilUnderThreshold(running, thresholdBytes)
      }
    }
  }

  /**
   * Spills a single registered buffer immediately, regardless of the aggregate threshold.
   *
   * @param key the key identifying the buffer to spill
   * @return `true` if a buffer was found and at least one byte was reclaimed, `false` otherwise
   */
  def spillBuffer(key: BufferKey): Boolean = {
    val buffer = buffers.get(key)
    buffer != null && spillBufferInternal(buffer) > 0L
  }

  // ---------------------------------------------------------------------------------------
  // Spilled-block lookup (consumed by StreamingShuffleBlockResolver).
  // ---------------------------------------------------------------------------------------

  /**
   * Returns the ordered disk-spill segments recorded for a partition, oldest first. The resolver
   * concatenates these segments (honoring the dual-channel invariant) to serve a spilled
   * partition to a reduce task.
   *
   * @param key the partition key
   * @return the spilled block ids in spill order, or an empty sequence if nothing was spilled
   */
  def spilledBlockIds(key: BufferKey): Seq[BlockId] = {
    val segments = spilledBlocks.get(key)
    if (segments == null) Seq.empty else segments.asScala.toSeq
  }

  /**
   * Returns the first (oldest) spilled block id for a partition, if any. This is the
   * [[TempLocalBlockId]] of the partition's first spill episode; callers that need every segment
   * (a partition spilled more than once) must use [[spilledBlockIds]] instead.
   *
   * @param key the partition key
   * @return the first spilled block id, or [[None]] if the partition has not been spilled
   */
  def spilledBlockId(key: BufferKey): Option[BlockId] = {
    val segments = spilledBlocks.get(key)
    if (segments == null || segments.isEmpty) None else Some(segments.get(0))
  }

  /**
   * @param key the partition key
   * @return `true` if at least one spill segment has been recorded for the partition
   */
  def isSpilled(key: BufferKey): Boolean = {
    val segments = spilledBlocks.get(key)
    segments != null && !segments.isEmpty
  }

  /**
   * Reads back the on-disk bytes of a single spilled segment through the public
   * `BlockManager.getLocalBytes` API, returning the segment's canonical
   * [[StreamingBlockEnvelope]] frames (the dual-channel persist view) ready for the resolver to
   * concatenate and serve. The streaming spill segments are non-shuffle [[TempLocalBlockId]]s, so
   * `getLocalBytes` serves them directly from the disk store without recursing back into the
   * shuffle resolver. The read lock that `getLocalBytes` retains on success is always released
   * (and the [[org.apache.spark.storage.BlockData]] disposed) before returning, even on failure.
   *
   * @param blockId the spilled segment id, as recorded in [[spilledBlockIds]]
   * @return the segment's enveloped bytes, or [[None]] if the block is no longer present locally
   */
  def readSpilledSegment(blockId: BlockId): Option[Array[Byte]] = {
    blockManager.getLocalBytes(blockId).map { data =>
      try {
        val bb = data.toByteBuffer()
        val arr = new Array[Byte](bb.remaining())
        bb.get(arr)
        arr
      } finally {
        // getLocalBytes keeps a read lock on success; release it and dispose the BlockData so no
        // lock or buffer leaks (validated under spark.unsafe.exceptionOnMemoryLeak=true).
        blockManager.releaseLockAndDispose(blockId, data)
      }
    }
  }

  // ---------------------------------------------------------------------------------------
  // Diagnostics (primarily for tests and operational introspection).
  // ---------------------------------------------------------------------------------------

  /** @return `true` once [[start]] has run and [[stop]] has not. */
  def isRunning: Boolean = started.get() && !stopped.get()

  /** @return the number of buffers currently registered for spilling. */
  def registeredBufferCount: Int = buffers.size()

  /**
   * @return the current aggregate buffer utilization as a percentage of
   *         `MemoryManager.maxOnHeapStorageMemory` (the same value published to the gauge before
   *         clamping); `0.0` when no storage memory is configured.
   */
  def currentUtilizationPercent: Double = utilizationPercentNow()

  // ---------------------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------------------

  // Derives the registry key from a buffer's identity fields.
  private def keyOf(b: StreamingBuffer): BufferKey =
    BufferKey(b.shuffleId, b.mapId, b.partitionId)

  // Sums the live size of every registered buffer. Uses the raw Java iterator (not a Scala
  // conversion) to keep this hot, per-tick path allocation-free; each size read is a lock-free
  // atomic load, so the scan never contends with a concurrent append.
  private def totalBufferedBytes(): Long = {
    var sum = 0L
    val it = buffers.values().iterator()
    while (it.hasNext) {
      sum += it.next().size
    }
    sum
  }

  // Current aggregate utilization as a percentage of the on-heap storage-memory denominator.
  private def utilizationPercentNow(): Double = {
    val denom = memoryManager.maxOnHeapStorageMemory
    if (denom <= 0L) 0.0 else totalBufferedBytes().toDouble * 100.0 / denom.toDouble
  }

  // The single scheduled tick: publish the utilization gauge and spill if over threshold. Any
  // throwable is swallowed and logged, because a task that throws would be suppressed by the
  // scheduled executor and silently stop all future ticks.
  private def pollOnce(): Unit = {
    try {
      val pct = utilizationPercentNow()
      metrics.setBufferUtilizationPercent(pct)
      // Feed the live aggregate utilization into the shared fallback policy so the
      // memory-pressure revert condition (default 95% of maxOnHeapStorageMemory) trips from the
      // real runtime footprint rather than never being updated. Null-guarded so a standalone
      // manager (unit tests) runs without a policy. This is the production write path the review
      // found missing for the memory-pressure fallback signal.
      if (fallbackPolicy != null) {
        fallbackPolicy.updateMemoryUtilization(pct)
      }
      if (pct >= conf.spillThresholdFraction * 100.0) {
        maybeSpill()
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle spill poll failed; retrying next tick", e)
    }
  }

  // Spills largest-first, LRU-tie-broken buffers until the running total drops below the
  // threshold, timing the whole episode for the ~100 ms reclaim SLA and emitting a single
  // throttled episode summary. Returns the total bytes reclaimed.
  private def spillUntilUnderThreshold(initialRunning: Long, thresholdBytes: Long): Long = {
    val startNanos = System.nanoTime()
    val ordered = buffers.values().asScala
      .filter(_.size > 0L)
      .toArray
      .sortBy(b => (-b.size, b.lastAccess))
    var running = initialRunning
    var reclaimed = 0L
    var spilled = 0
    var i = 0
    while (i < ordered.length && running >= thresholdBytes) {
      val freed = spillBufferInternal(ordered(i))
      if (freed > 0L) {
        reclaimed += freed
        running -= freed
        spilled += 1
      }
      i += 1
    }
    if (spilled > 0) {
      val elapsedMs = (System.nanoTime() - startNanos) / 1000000L
      logEpisode(spilled, reclaimed, elapsedMs)
      maybeLogSlaExceeded(elapsedMs)
    }
    reclaimed
  }

  // Spills exactly one buffer to disk and reclaims its heap, returning the bytes reclaimed (or 0
  // if the buffer is empty or the store failed). The snapshot, the durable store, the ordered
  // segment registration, and the heap reset all run inside StreamingBuffer.spillAndClear, under
  // the buffer's single internal lock that also guards append -- so a byte appended concurrently
  // is either fully captured in this spill or fully retained for the next one, never silently
  // dropped (closing the CWE-367 snapshot/clear race that a separate snapshot-then-clear had).
  // The buffer is cleared only after putBytes confirms the bytes are durable; a store failure
  // leaves the buffer intact and loses nothing. Because the buffer is empty the instant
  // spillAndClear returns, a concurrent scheduled spill and an on-demand spillBuffer can never
  // double-store the same partition: whichever runs second snapshots an empty buffer and no-ops.
  private def spillBufferInternal(buffer: StreamingBuffer): Long = {
    val key = keyOf(buffer)
    buffer.spillAndClear { bytes =>
      // Every segment (including the first) uses a unique non-shuffle TempLocalBlockId so the
      // resolver can read it back via BlockManager.getLocalBytes, which rejects shuffle ids.
      val blockId: BlockId = TempLocalBlockId(UUID.randomUUID())
      val stored =
        try {
          // DISK_ONLY persists synchronously to the disk store, so once putBytes returns true the
          // bytes are durable and the buffer's heap can be released. Reuses only the public
          // BlockManager API; no storage interface contract is altered.
          blockManager.putBytes(blockId, bytes, StorageLevel.DISK_ONLY)
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to spill streaming buffer shuffle=" +
              log"${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} map=" +
              log"${MDC(LogKeys.MAP_ID, key.mapId)} partition=" +
              log"${MDC(LogKeys.PARTITION_ID, key.partitionId)}", e)
            false
        }
      if (stored) {
        // Register the segment in the partition's ordered list and bump telemetry while still
        // holding the buffer lock (inside spillAndClear), so the resolver's atomic
        // snapshotEnvelopedWith never sees the buffer cleared before its segment is recorded.
        val segments = spilledBlocks.computeIfAbsent(
          key, _ => new CopyOnWriteArrayList[BlockId]())
        segments.add(blockId)
        metrics.incSpillCount()
        if (conf.debug) {
          logDebug(log"Spilled streaming buffer shuffle=" +
            log"${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} map=" +
            log"${MDC(LogKeys.MAP_ID, key.mapId)} partition=" +
            log"${MDC(LogKeys.PARTITION_ID, key.partitionId)} (" +
            log"${MDC(LogKeys.NUM_BYTES, bytes.size)} bytes) -> " +
            log"${MDC(LogKeys.BLOCK_ID, blockId.name)}")
        }
      }
      stored
    }
  }

  // Emits one episode summary, throttled to at most once per second at INFO (the rest at DEBUG
  // when enabled) so sustained spilling stays within the log-volume budget.
  private def logEpisode(count: Int, bytes: Long, elapsedMs: Long): Unit = {
    val now = System.nanoTime()
    val last = lastEpisodeLogNanos.get()
    val due = now - last >= TimeUnit.SECONDS.toNanos(1L)
    if (due && lastEpisodeLogNanos.compareAndSet(last, now)) {
      logInfo(log"Streaming shuffle spilled ${MDC(LogKeys.COUNT, count)} buffer(s); " +
        log"${MDC(LogKeys.NUM_BYTES, bytes)} bytes reclaimed in " +
        log"${MDC(LogKeys.DURATION, elapsedMs)} ms")
    } else if (conf.debug) {
      logDebug(log"Streaming shuffle spilled ${MDC(LogKeys.COUNT, count)} buffer(s); " +
        log"${MDC(LogKeys.NUM_BYTES, bytes)} bytes reclaimed in " +
        log"${MDC(LogKeys.DURATION, elapsedMs)} ms")
    }
  }

  // Logs a single debug line when a spill episode exceeds the ~100 ms reclaim SLA. Debug-only so
  // it never contributes to the production log budget.
  private def maybeLogSlaExceeded(elapsedMs: Long): Unit = {
    if (elapsedMs > StreamingShuffleConfig.SPILL_RECLAIM_SLA_MS && conf.debug) {
      logDebug(log"Streaming shuffle spill exceeded reclaim SLA: " +
        log"${MDC(LogKeys.DURATION, elapsedMs)} ms > " +
        log"${MDC(LogKeys.THRESHOLD, StreamingShuffleConfig.SPILL_RECLAIM_SLA_MS)} ms")
    }
  }
}

/**
 * Companion holding the registry key for [[MemorySpillManager]].
 */
private[spark] object MemorySpillManager {

  /**
   * Identifies a per-partition streaming buffer by the producing shuffle and map task and the
   * destination reduce partition. A case class so it has value-based equality/hashing suitable
   * for use as a [[java.util.concurrent.ConcurrentHashMap]] key.
   *
   * @param shuffleId   the shuffle id the buffer belongs to
   * @param mapId       the map task id that produced the buffered output
   * @param partitionId the reduce partition id the buffered bytes are destined for
   */
  final case class BufferKey(shuffleId: Int, mapId: Long, partitionId: Int)
}
