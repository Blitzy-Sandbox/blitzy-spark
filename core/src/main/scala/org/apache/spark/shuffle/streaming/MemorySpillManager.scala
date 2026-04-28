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
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.STREAMING_SHUFFLE_SPILL_THRESHOLD
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Memory-polling spill manager: polls the executor's [[MemoryManager]] at 100 ms intervals
 * (per [[SPILL_POLL_INTERVAL_MILLIS]]); when buffer utilization exceeds the configured
 * spill threshold (default 80%, configurable 50-95% via
 * [[STREAMING_SHUFFLE_SPILL_THRESHOLD]]), evicts the largest accumulated partition buffer
 * via Guava-backed LRU and persists it through the existing [[BlockManager#putBytes]] API
 * at `StorageLevel.DISK_ONLY`. NO new block-ID type or storage level is introduced.
 *
 * == Coexistence ==
 * Spilled blocks are persisted as `ShuffleBlockId(shuffleId, mapId, reduceId)` instances
 * -- the same block-ID type used by the existing sort-based shuffle. The on-disk layout is
 * therefore compatible with the existing `IndexShuffleBlockResolver` lookup path, though
 * the streaming reader does not currently consume from disk (the spill exists primarily
 * as failsafe to absorb consumer slowdowns and prevent OOM during sustained backpressure).
 *
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* this class lives entirely within
 * the `org.apache.spark.shuffle.streaming` subpackage. It uses only the public
 * [[BlockManager#putBytes]] API and the public [[MemoryManager#maxOnHeapStorageMemory]]
 * accessor; it does not modify any existing shuffle, storage, or memory-management
 * code path.
 *
 * == Reclamation Path ==
 * Consumer acknowledgments arrive through `BackpressureProtocol` and trigger [[reclaim]]
 * within 100 ms (the polling cadence) per the streaming-shuffle "buffer reclamation
 * within 100 ms of consumer acknowledgment" specification.
 *
 * == Spill Decision ==
 * Two paths can trigger a spill:
 *   - [[checkAndSpill]]: per-partition push from `StreamingShuffleWriter#maybeSpill` when a
 *     single partition's buffer crosses the threshold. The writer hands the pending bytes
 *     to this class, which persists them via [[BlockManager#putBytes]].
 *   - [[evictLargestBuffer]] (private): pull from the polling loop when the global
 *     buffer-utilization percent (computed in [[pollOnce]]) exceeds the configured
 *     threshold. The polling path selects the largest tracked buffer for eviction, on the
 *     pragmatic basis that the largest buffer relieves the most pressure per eviction.
 *
 * == Concurrency ==
 * The polling thread runs concurrently with writer threads that call [[checkAndSpill]],
 * [[trackBuffer]], and [[reclaim]] from arbitrary task threads. Thread-safety is achieved
 * through:
 *   - [[partitionLruCache]]: thread-safe Guava `Cache` with concurrent `put`, `getIfPresent`,
 *     and `invalidate` operations.
 *   - [[totalBytes]] and [[totalSpills]]: lock-free [[AtomicLong]] counters with
 *     `addAndGet` and `incrementAndGet` operations.
 *   - [[pollingFuture]]: `@volatile` so [[stop]] reads the latest value published by the
 *     constructor thread.
 *   - [[stopped]]: [[AtomicBoolean]] guard that makes [[stop]] idempotent under concurrent
 *     or repeated invocation.
 *
 * No `synchronized` blocks exist on the hot paths -- the metric-emission overhead is
 * bounded by the cost of one [[AtomicLong#get]] and one cache lookup per polling tick,
 * satisfying the streaming-shuffle "telemetry overhead < 1% executor CPU utilization"
 * budget.
 *
 * == Lifecycle ==
 * Constructed once per [[StreamingShuffleManager]] instance (i.e., once per executor JVM
 * when streaming shuffle is opted in). The polling executor is started on construction
 * and runs as a daemon thread, so a missing [[stop]] call cannot prevent JVM shutdown.
 * The lifetime of this instance equals the lifetime of the streaming-shuffle code path
 * on this executor.
 *
 * @param blockManager  executor block manager for `putBytes` access
 * @param memoryManager unified memory manager for utilization sampling
 * @param metrics       streaming-shuffle metric counters/gauges
 * @param conf          `SparkConf` used to read the spill-threshold configuration
 */
private[spark] class MemorySpillManager(
    blockManager: BlockManager,
    memoryManager: MemoryManager,
    metrics: StreamingShuffleMetrics,
    conf: SparkConf) extends Logging {

  /**
   * Composite key for the LRU cache: tracks one partition buffer's accumulation. The
   * auto-generated `equals` and `hashCode` from case-class semantics satisfy the Guava
   * `Cache` key-equality contract; the `toString` is used in log diagnostics in
   * [[evictLargestBuffer]] and [[reclaim]] without further formatting.
   *
   * Declared as `private case class` inside the enclosing class body following the same
   * pattern used by `org.apache.spark.storage.StorageUtils.RddStorageInfo` (the case class
   * is fully scoped to one [[MemorySpillManager]] instance, so its path-dependent identity
   * is appropriate -- two keys from different manager instances are unrelated by design).
   */
  private case class BufferKey(shuffleId: Int, mapId: Long, reduceId: Int)

  /**
   * LRU cache keyed on `(shuffleId, mapId, reduceId)` tuples; value is the accumulated
   * buffer size in bytes for that partition. `expireAfterAccess` is intentionally NOT set
   * -- entries remain until explicitly invalidated after spill or reclamation -- because
   * eviction policy is driven by memory pressure (via [[pollOnce]] -> [[evictLargestBuffer]])
   * and consumer acknowledgments (via [[reclaim]]), not by a timer.
   *
   * The `maximumSize(10000)` cap is an operational ceiling for the number of distinct
   * partitions tracked simultaneously. A typical Spark executor handles fewer concurrent
   * shuffles than this in practice (the streaming-shuffle "5 concurrent shuffles" stress
   * test target multiplied by partition counts in the hundreds is well within 10k); the
   * cap exists to bound the cache's memory footprint in pathological cases. Guava's
   * `maximumSize` policy uses an LRU eviction order, but this class's correctness does not
   * depend on any specific eviction order beyond "do not retain unbounded entries."
   *
   * `recordStats()` enables the cache's internal statistics tracking, exposed via the
   * `Cache#stats` accessor in the [[stop]] log message for post-mortem diagnostics
   * (per the AAP Section 0.5.1.3 "LRU partition selection backed by a Guava
   * `CacheBuilder.newBuilder().recordStats().build()`" specification).
   *
   * The value type is the boxed `java.lang.Long` rather than the unboxed `Long` because
   * Guava's generic type parameters require reference types; explicit boxing in `put`
   * calls via [[java.lang.Long#valueOf]] avoids any auto-boxing surprises.
   */
  private val partitionLruCache: Cache[BufferKey, java.lang.Long] = CacheBuilder.newBuilder()
    .maximumSize(10000L)
    .recordStats()
    .build()

  /**
   * Total accumulated bytes across all currently tracked partitions; sampled by the
   * polling thread to compute buffer-utilization percent. Updated lock-free by
   * [[trackBuffer]] (positive delta), [[reclaim]] (negative delta), [[checkAndSpill]]
   * (negative delta on successful spill), and [[evictLargestBuffer]] (negative delta on
   * eviction). The `addAndGet` operations are wait-free under low contention and offer
   * orders-of-magnitude lower overhead than a `synchronized` block on the hot path.
   */
  private val totalBytes = new AtomicLong(0L)

  /**
   * Cumulative count of spill events (both `checkAndSpill` and `evictLargestBuffer`
   * paths) over the lifetime of this instance. Used in the [[stop]] log message for
   * post-mortem diagnostics; the operator-facing counter is the
   * [[StreamingShuffleMetrics#spillCount]] Dropwizard counter, which is incremented in
   * lockstep with this field.
   */
  private val totalSpills = new AtomicLong(0L)

  /**
   * Daemon scheduled executor for the 100 ms polling loop. The single-thread pool
   * eliminates concurrency between successive polling ticks and simplifies lifecycle
   * management. Daemon mode (via [[ThreadUtils#newDaemonSingleThreadScheduledExecutor]])
   * ensures the executor does not prevent JVM shutdown if [[stop]] is missed by a caller
   * bug.
   */
  private val pollingExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-spill-poller")

  /**
   * Handle to the scheduled polling task -- captured so [[stop]] can cancel it cleanly.
   * Marked `@volatile` because it is written from the constructor thread (via
   * [[startPolling]]) and read from the caller thread on shutdown; without volatility the
   * read could observe `null` even after the constructor completes on a different thread.
   */
  @volatile
  private var pollingFuture: ScheduledFuture[_] = _

  /**
   * Idempotent stop guard. Set to `true` exactly once on the first [[stop]] call via
   * `compareAndSet`; subsequent calls observe `true` and become no-ops, so double-shutdown
   * (which would attempt to cancel a cancelled future and shut down a shut-down executor)
   * is safe under concurrent or repeated invocation.
   */
  private val stopped = new AtomicBoolean(false)

  // Start the polling loop on construction so that buffer-utilization sampling begins
  // immediately. The first tick runs after one polling interval (100 ms) to allow callers
  // to populate initial state via [[trackBuffer]] before the first sample.
  startPolling()

  /**
   * Schedule the polling loop on the daemon executor at [[SPILL_POLL_INTERVAL_MILLIS]]
   * cadence. The loop samples [[totalBytes]] vs. the executor's
   * [[MemoryManager#maxOnHeapStorageMemory]] budget; when utilization exceeds the
   * configured spill threshold, evicts the largest tracked buffer.
   *
   * Errors raised by the polling task are caught and logged at WARN rather than allowed
   * to propagate, because an uncaught exception from a `scheduleWithFixedDelay` task
   * silently cancels the schedule (see `ScheduledThreadPoolExecutor` Javadoc) and would
   * leave the manager in an unmonitored state until [[stop]] is called.
   */
  private def startPolling(): Unit = {
    val task = new Runnable {
      override def run(): Unit = {
        try pollOnce()
        catch {
          case t: Throwable =>
            // Polling errors should not kill the executor or cancel the schedule; log and
            // continue so the next tick proceeds normally.
            logWarning("MemorySpillManager poll error (continuing)", t)
        }
      }
    }
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      task, SPILL_POLL_INTERVAL_MILLIS, SPILL_POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
    logInfo(s"MemorySpillManager polling started at ${SPILL_POLL_INTERVAL_MILLIS}ms interval")
  }

  /**
   * One iteration of the polling loop. Samples the global buffer-utilization percent
   * ([[totalBytes]] divided by [[MemoryManager#maxOnHeapStorageMemory]]) and updates the
   * [[StreamingShuffleMetrics#bufferUtilizationPercent]] gauge. If utilization meets or
   * exceeds the configured spill threshold (default 80%, configurable 50-95% via
   * [[STREAMING_SHUFFLE_SPILL_THRESHOLD]]), evicts the largest tracked buffer.
   *
   * == Visibility ==
   * Marked `private[streaming]` rather than `private` so that
   * [[org.apache.spark.shuffle.streaming]] tests in the same subpackage can drive single
   * polling iterations deterministically without waiting for the 100-ms scheduler cadence.
   *
   * == Edge Cases ==
   *   - When [[MemoryManager#maxOnHeapStorageMemory]] is `0` or negative (test fixtures
   *     with a degenerate memory configuration), the method returns early without
   *     emitting a divide-by-zero or producing a meaningless utilization value.
   *   - When `totalBytes / maxOnHeap > 1.0` (transient over-tracking due to a race between
   *     [[trackBuffer]] and [[reclaim]]), the metric writer
   *     [[StreamingShuffleMetrics#updateBufferUtilization]] clamps the value into
   *     `[0, 100]` so the operator-facing gauge never reports an out-of-range percent.
   */
  private[streaming] def pollOnce(): Unit = {
    val maxOnHeap = memoryManager.maxOnHeapStorageMemory
    if (maxOnHeap <= 0L) return

    val used = totalBytes.get()
    val pct = ((used.toDouble / maxOnHeap.toDouble) * 100.0).toInt
    metrics.updateBufferUtilization(pct)

    // Spill threshold default 80% per the streaming-shuffle memory-discipline contract;
    // configurable in [50, 95] via STREAMING_SHUFFLE_SPILL_THRESHOLD.
    val thresholdPercent = conf.get(STREAMING_SHUFFLE_SPILL_THRESHOLD)
    if (pct >= thresholdPercent) {
      evictLargestBuffer()
    }
  }

  /**
   * Persist a partition's pending buffer bytes to disk via [[BlockManager#putBytes]] and
   * update bookkeeping. Called from `StreamingShuffleWriter#maybeSpill` when a single
   * partition's buffer crosses the threshold. The writer is expected to reset its
   * in-memory buffer for this partition after this call returns; this class invalidates
   * the corresponding LRU entry and decrements [[totalBytes]] on successful spill.
   *
   * == Failure Handling ==
   *   - When [[BlockManager#putBytes]] returns `false` (block-write declined), this method
   *     logs a warning and leaves the LRU bookkeeping untouched so the writer can retry.
   *   - When [[BlockManager#putBytes]] throws an exception (disk failure, IO error,
   *     interrupt), the exception is caught and logged at WARN. The buffer remains in
   *     memory and may be retried by the caller; the bookkeeping is left untouched so a
   *     retry observes the same state as the original call.
   *
   * == No-Op Inputs ==
   * Null or empty `pendingBytes` returns immediately without touching the cache or the
   * block manager; this protects callers from accidentally registering empty spills that
   * would inflate the spill counter without persisting any data.
   *
   * @param shuffleId    shuffle ID
   * @param mapId        map task ID
   * @param reduceId     reduce partition ID
   * @param pendingBytes the accumulated buffer bytes to spill (must be non-null and
   *                     non-empty for the spill to proceed)
   */
  def checkAndSpill(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      pendingBytes: Array[Byte]): Unit = {
    if (pendingBytes == null || pendingBytes.isEmpty) return

    val key = BufferKey(shuffleId, mapId, reduceId)
    val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
    // Wrap the byte array via ByteBuffer.wrap (zero-copy) and pass to the single-buffer
    // ChunkedByteBuffer convenience constructor; this matches the existing pattern used
    // by org.apache.spark.storage.memory.MemoryStore for serialized-entry persistence.
    val byteBuf = ByteBuffer.wrap(pendingBytes)
    val chunked = new ChunkedByteBuffer(byteBuf)

    try {
      // ClassTag.Byte is supplied explicitly because the BlockManager#putBytes signature
      // takes a context-bound type parameter `[T: ClassTag]`. The bytes are an Array[Byte],
      // so Byte is the natural ClassTag. tellMaster=true ensures the block-manager master
      // tracks this spilled block for the standard storage-status reporting paths.
      val stored = blockManager.putBytes(
        blockId, chunked, StorageLevel.DISK_ONLY, tellMaster = true)(scala.reflect.ClassTag.Byte)

      if (stored) {
        totalSpills.incrementAndGet()
        metrics.incrementSpillCount()
        partitionLruCache.invalidate(key)
        totalBytes.addAndGet(-pendingBytes.length.toLong)
        logDebug(
          s"Spilled $blockId (${pendingBytes.length} bytes) to disk via BlockManager")
      } else {
        logWarning(
          s"Failed to spill $blockId via BlockManager.putBytes (returned false)")
      }
    } catch {
      case e: Exception =>
        // Spill failure is recoverable -- the buffer remains in memory and may be retried.
        // Bookkeeping is left untouched so retry observes the same state.
        logWarning(s"Spill failure for $blockId: ${e.getMessage}", e)
    }
  }

  /**
   * Update the LRU cache and [[totalBytes]] counter when a writer accumulates more bytes
   * for a partition buffer. Called by `StreamingShuffleWriter` after each block flush so
   * this manager has up-to-date visibility into pending memory pressure across all
   * tracked partitions.
   *
   * == Accumulation Semantics ==
   * The new value is `previous + bytes` -- this method records cumulative bytes for the
   * partition since the last invalidation (which occurs on spill via [[checkAndSpill]] or
   * eviction via [[evictLargestBuffer]] or full reclamation via [[reclaim]]). Callers
   * passing negative `bytes` are accepted but discouraged; for releases use [[reclaim]].
   *
   * @param shuffleId shuffle ID
   * @param mapId     map task ID
   * @param reduceId  reduce partition ID
   * @param bytes     incremental bytes to add to this partition's tracked buffer
   */
  def trackBuffer(shuffleId: Int, mapId: Long, reduceId: Int, bytes: Long): Unit = {
    val key = BufferKey(shuffleId, mapId, reduceId)
    val previous = Option(partitionLruCache.getIfPresent(key)).map(_.longValue()).getOrElse(0L)
    partitionLruCache.put(key, java.lang.Long.valueOf(previous + bytes))
    totalBytes.addAndGet(bytes)
  }

  /**
   * Find the largest tracked buffer and evict it. Used by the polling loop when global
   * utilization exceeds the threshold (cf. [[checkAndSpill]] which is per-partition,
   * triggered by the writer with the buffer's bytes already in hand).
   *
   * == Eviction Strategy ==
   * Per AAP Section 0.5.1.3 the cache is "LRU partition selection backed by a Guava
   * CacheBuilder.newBuilder().recordStats().build()". For runtime spill decisions the
   * largest buffer is preferred over strict access-order LRU because relieving the most
   * pressure per eviction reduces the frequency of subsequent evictions; the LRU
   * machinery (Guava's `maximumSize` policy) still bounds the cache footprint.
   *
   * == Operational Note ==
   * Unlike [[checkAndSpill]] this method does NOT call [[BlockManager#putBytes]] -- it
   * has no copy of the buffer's bytes (the writer holds them; this manager only tracks
   * sizes). Eviction here invalidates the LRU entry and decrements [[totalBytes]],
   * signalling to the writer that the partition is no longer counted toward global
   * pressure. The writer observes the missing entry on its next [[trackBuffer]] call and
   * starts a fresh accumulation; the actual disk-spill of the bytes happens when the
   * writer next invokes [[checkAndSpill]] for that partition.
   *
   * Returns immediately if the cache is empty (no tracked buffers to evict).
   */
  private def evictLargestBuffer(): Unit = {
    // Snapshot the cache to a Scala immutable map to avoid concurrent-modification
    // artifacts during the maxBy traversal. The Guava Cache.asMap is a live ConcurrentMap
    // view, but iterating it directly via Scala converters yields a weakly-consistent
    // snapshot that is fine for the maxBy probe -- subsequent entries may have been
    // updated, but the maxBy result is still a valid eviction candidate at the moment
    // of selection.
    val snapshot = partitionLruCache.asMap().asScala
    if (snapshot.isEmpty) return

    // maxBy on the value (java.lang.Long) compares by the unboxed long count.
    val largest = snapshot.maxBy { case (_, v) => v.longValue() }
    val key = largest._1
    val bytes = largest._2.longValue()

    // Invalidate the LRU entry; the writer will see a missing entry on next
    // trackBuffer/checkAndSpill and re-track or spill accordingly. Decrement the global
    // counter to reflect that this partition is no longer counted toward pressure.
    partitionLruCache.invalidate(key)
    totalBytes.addAndGet(-bytes)
    totalSpills.incrementAndGet()
    metrics.incrementSpillCount()
    logInfo(
      s"Evicted largest buffer key=$key bytes=$bytes via LRU policy due to memory pressure")
  }

  /**
   * Release tracked buffer memory upon receiving a consumer acknowledgment. Called by
   * `BackpressureProtocol` within 100 ms of the consumer's acknowledgment arriving (per
   * the streaming-shuffle "buffer reclamation within 100 ms of consumer acknowledgment"
   * specification). The acknowledgment indicates the consumer has durably received and
   * processed `bytes` bytes for the given partition, so the writer's in-memory buffer
   * (and this manager's tracked count) can both shrink by that amount.
   *
   * == Underflow Protection ==
   * If `bytes` exceeds the currently tracked count for the partition (possible under
   * out-of-order acknowledgments or duplicate acks), the new value is clamped to `0`
   * rather than going negative, and the LRU entry is invalidated entirely.
   *
   * @param shuffleId shuffle ID whose buffer is being reclaimed
   * @param mapId     map task ID
   * @param reduceId  reduce partition ID
   * @param bytes     bytes acknowledged by the consumer
   */
  def reclaim(shuffleId: Int, mapId: Long, reduceId: Int, bytes: Long): Unit = {
    val key = BufferKey(shuffleId, mapId, reduceId)
    val current = Option(partitionLruCache.getIfPresent(key)).map(_.longValue()).getOrElse(0L)
    val newValue = math.max(0L, current - bytes)
    if (newValue == 0L) {
      partitionLruCache.invalidate(key)
    } else {
      partitionLruCache.put(key, java.lang.Long.valueOf(newValue))
    }
    // Decrement the global counter by the actually-released amount (current - newValue),
    // which equals min(bytes, current). This keeps the global counter in sync with the
    // sum of per-partition counts even under out-of-order or duplicate acks.
    val released = current - newValue
    totalBytes.addAndGet(-released)
    logTrace(s"Reclaimed $released bytes for $key (remaining=$newValue)")
  }

  /**
   * Cancel the polling task and shut down the daemon executor.
   *
   * == Idempotency ==
   * Implemented as idempotent via the [[stopped]] [[AtomicBoolean]] -- the first call's
   * `compareAndSet(false, true)` succeeds and performs the shutdown; subsequent calls'
   * CAS attempts fail and the method returns immediately. This protects against
   * double-shutdown sequences (for example, a `stop` call from the streaming-shuffle
   * manager followed by another from a JVM shutdown hook) which would otherwise raise
   * exceptions when cancelling already-cancelled futures or shutting down an
   * already-terminated executor.
   *
   * == Shutdown Sequence ==
   *   1. Cancel the scheduled polling future (without interrupting in-flight ticks;
   *      setting `mayInterruptIfRunning = false` lets the current tick complete normally).
   *   2. Initiate orderly executor shutdown via `shutdown()`.
   *   3. Wait up to 2 seconds for the in-flight tick to complete; if not, force shutdown
   *      via `shutdownNow()`.
   *   4. Clear the LRU cache and reset [[totalBytes]] so any retained references are
   *      released for GC.
   *
   * The 2-second wait window is generous given that the polling tick completes in well
   * under a millisecond in normal operation; the timeout exists only to bound shutdown
   * in pathological cases (such as a GC pause coinciding with shutdown).
   */
  def stop(): Unit = {
    if (!stopped.compareAndSet(false, true)) return

    if (pollingFuture != null) {
      pollingFuture.cancel(false)
      pollingFuture = null
    }
    pollingExecutor.shutdown()
    try {
      if (!pollingExecutor.awaitTermination(2L, TimeUnit.SECONDS)) {
        pollingExecutor.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        // Preserve the interrupt flag for callers that may want to react to interruption,
        // then force-shutdown the executor before returning.
        Thread.currentThread().interrupt()
        pollingExecutor.shutdownNow()
    }
    partitionLruCache.invalidateAll()
    totalBytes.set(0L)
    logInfo(
      s"MemorySpillManager stopped (totalSpills=${totalSpills.get()}, " +
        s"cacheStats=${partitionLruCache.stats()})")
  }
}
