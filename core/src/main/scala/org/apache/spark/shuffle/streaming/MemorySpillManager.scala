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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.util.ThreadUtils

/**
 * Executor-side memory pressure monitor and spill coordinator for the streaming shuffle
 * feature (F-001). Instances are constructed by
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]] on executor initialization
 * (only when `spark.shuffle.manager=streaming`) and stopped on manager shutdown.
 *
 * == Responsibilities ==
 *
 *   - Poll aggregate streaming-buffer utilization once every 100 ms via a dedicated
 *     single-threaded daemon [[java.util.concurrent.ScheduledExecutorService]].
 *   - Publish the observed utilization percent on every poll to
 *     [[StreamingShuffleMetrics#setBufferUtilizationPercent]] so that the Dropwizard gauge
 *     `shuffle.streaming.bufferUtilizationPercent` reflects the most recent sample.
 *   - When utilization reaches or exceeds the configured `spark.shuffle.streaming.spillThreshold`
 *     (percent, range 50-95, default 80), select one buffered partition for eviction using an
 *     LRU tie-breaking policy (primary order = largest size first, secondary order = oldest
 *     last-access-time first) and invoke the caller-supplied [[java.lang.Runnable]] spill
 *     callback registered for that buffer key.
 *   - Increment [[StreamingShuffleMetrics#incrementSpillCount]] once per successful spill
 *     callback invocation so the `shuffle.streaming.spillCount` counter tracks spill frequency.
 *
 * == Coexistence strategy ==
 *
 * This class is a v1 COORDINATOR that tracks WHEN to spill via self-tracked byte counters
 * and a callback dispatch mechanism. It does NOT perform the actual byte transfer to disk;
 * that responsibility belongs to the [[StreamingShuffleWriter]], which holds the
 * [[org.apache.spark.storage.BlockManager]] reference and owns the HOW of spill.
 *
 * The decision to track bytes with [[java.util.concurrent.atomic.AtomicLong]] counters
 * (rather than integrating directly with [[org.apache.spark.memory.MemoryManager]]) is
 * deliberate. Spark's `MemoryManager.acquireExecutionMemory` / `releaseExecutionMemory`
 * APIs are declared `private[memory]`, so calling them from
 * `org.apache.spark.shuffle.streaming` is not permitted at compile time.
 * `TaskMemoryManager.acquireExecutionMemory(long, MemoryConsumer)` is public but requires
 * implementing [[org.apache.spark.memory.MemoryConsumer]] (including the abstract
 * `spill(long, MemoryConsumer)` method and its cooperative-spill contract) which is
 * heavyweight for v1. Self-tracked counters give us observability without coupling to
 * private APIs. A post-v1 enhancement can promote the integration once the streaming path
 * is battle-tested; see `blitzy-docs/streaming-shuffle-decision-log.md` for the decision
 * record.
 *
 * This approach directly preserves the User Directive from AAP section 0.7.1:
 * "When implementation choices exist, select approach requiring least modification to
 * executor memory model and network transport layer."
 *
 * This class is ONLY loaded on executors that have opted into streaming shuffle via
 * `spark.shuffle.manager=streaming`; a sort-path executor never instantiates it and
 * therefore incurs zero overhead. This satisfies the Implementation Discipline directive
 * "Isolate streaming logic in dedicated classes with zero cross-contamination into
 * existing shuffle code paths" (AAP section 0.7.1).
 *
 * == Thread-safety ==
 *
 * All mutation paths are lock-free:
 *   - `totalBudgetBytes` / `currentUsageBytes` are [[java.util.concurrent.atomic.AtomicLong]].
 *   - `perBufferUsage` / `lastAccessMillis` / `spillCallbacks` are
 *     [[java.util.concurrent.ConcurrentHashMap]] instances.
 *   - The poll loop runs on a single dedicated daemon thread and reads snapshots from the
 *     concurrent maps via their iterator APIs.
 *
 * Instances may safely be shared across map-side tasks running on the same executor.
 *
 * == Observability ==
 *
 * Every poll iteration publishes the current utilization as a Dropwizard gauge. Every spill
 * triggered by the poll increments a counter. Exceptions raised by a caller's spill callback
 * are caught and logged at WARN level with a structured `BLOCK_ID` MDC so that downstream
 * alerting systems can attribute spill failures to specific buffer keys. Unrecoverable
 * exceptions inside the poll body (e.g. an OOM during map iteration) are caught at the
 * outermost layer and logged at ERROR level; the scheduler is not cancelled so that the
 * next poll can attempt recovery.
 *
 * == Binary compatibility ==
 *
 * This class is `private[spark]` and lives in a brand-new sub-package, so it introduces no
 * public SPI signature and requires no entry in `project/MimaExcludes.scala` (F-017).
 *
 * @param conf the active [[org.apache.spark.SparkConf]]; read-only access to
 *             `spark.shuffle.streaming.spillThreshold` at construction time.
 * @param metrics the [[StreamingShuffleMetrics]] source registered with the executor's
 *                [[org.apache.spark.metrics.MetricsSystem]]. May be `null` in non-executor
 *                contexts (for example, unit tests that verify coordinator logic without
 *                wiring a `MetricsSystem`); every invocation is null-checked before
 *                dispatch.
 */
private[spark] class MemorySpillManager(
    conf: SparkConf,
    metrics: StreamingShuffleMetrics)
  extends Logging {

  // --------------------------------------------------------------------------
  // Configuration constants.
  // --------------------------------------------------------------------------

  /**
   * Poll interval in milliseconds. Fixed at 100 ms per the user specification
   * (AAP section 0.1.1: "100ms polling of memory manager") and the user-specified
   * fallback SLA (AAP section 0.1.1: "<100ms response time" for memory exhaustion
   * prevention).
   */
  private val pollIntervalMillis: Long = 100L

  /**
   * Spill-trigger threshold in percent (range 50-95, default 80) as configured via
   * `spark.shuffle.streaming.spillThreshold`. Typed as `Int` to match
   * [[org.apache.spark.internal.config.package#SHUFFLE_STREAMING_SPILL_THRESHOLD]]. Value is
   * validated by the `ConfigBuilder.checkValue` constraint at configuration parsing time,
   * so this cached field is guaranteed to be within [50, 95].
   */
  private val spillThresholdPct: Int =
    conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  // --------------------------------------------------------------------------
  // State tables. All fields are thread-safe and lock-free. Together they track:
  //   - the total budget (bytes) allocated across streaming buffers on this
  //     executor (set once by the writer at construction);
  //   - the per-buffer-key byte count most recently reported by the writer;
  //   - the per-buffer-key last-access wall-clock time (for LRU tie-breaking);
  //   - the per-buffer-key spill callback to invoke when eviction is decided.
  // --------------------------------------------------------------------------

  /**
   * Total streaming-buffer budget in bytes across all streaming shuffles on this executor.
   * Initialized to `0L` meaning "not yet configured"; set by the writer via
   * [[setBudget]] after resolving `(executorMemory * bufferSizePercent) / 100` at
   * construction time. Reads in [[currentUtilizationPercent]] and
   * [[pollAndMaybeSpill]] guard against the zero case to avoid division by zero.
   *
   * An [[java.util.concurrent.atomic.AtomicLong]] is used (instead of a `@volatile var`)
   * so that later iterations can atomically adjust the budget in response to an explicit
   * API (for example, if the writer learns that a task completed and frees a partition's
   * share of the budget).
   */
  private val totalBudgetBytes: AtomicLong = new AtomicLong(0L)

  /**
   * Direct-read cache of aggregate current usage in bytes, recomputed on every poll from
   * the [[perBufferUsage]] table. Maintained for parity with
   * [[org.apache.spark.memory.MemoryManager]] telemetry conventions (which also expose a
   * scalar "used bytes" read-path) and to preserve an `AtomicLong` read surface if a future
   * iteration needs to sample usage outside the poll thread. Value is updated by the poll
   * thread only; external callers read via [[currentUtilizationPercent]].
   */
  private val currentUsageBytes: AtomicLong = new AtomicLong(0L)

  /**
   * Per-buffer-key byte count most recently reported by the writer. Key convention:
   * `"shuffleId-partitionId"` (the writer owns the key format; the coordinator treats it
   * as an opaque string identifier). Values are [[java.lang.Long]] to satisfy
   * [[java.util.concurrent.ConcurrentHashMap]]'s requirement that values be reference
   * types.
   */
  private val perBufferUsage: ConcurrentHashMap[String, java.lang.Long] =
    new ConcurrentHashMap[String, java.lang.Long]()

  /**
   * Per-buffer-key last-access wall-clock time in milliseconds (from
   * [[java.lang.System#currentTimeMillis]]). Updated on every [[reportUsage]] call so that
   * the LRU tie-breaker in [[pollAndMaybeSpill]] can select the oldest-accessed buffer
   * when several buffers share the largest size.
   */
  private val lastAccessMillis: ConcurrentHashMap[String, java.lang.Long] =
    new ConcurrentHashMap[String, java.lang.Long]()

  /**
   * Per-buffer-key spill callback registry. Registered by the writer via
   * [[registerSpillCallback]]; invoked by the poll loop when a buffer is selected as the
   * eviction victim. The callback is expected to be idempotent-safe (may be invoked once
   * per spill decision) and to handle its own errors internally; uncaught throwables
   * propagated out of the callback are caught by the poll loop and logged at WARN.
   *
   * The callback is removed from this map BEFORE it is invoked so that a slow callback
   * does not cause a second poll iteration to also select the same victim.
   */
  private val spillCallbacks: ConcurrentHashMap[String, Runnable] =
    new ConcurrentHashMap[String, Runnable]()

  // --------------------------------------------------------------------------
  // Rate-limit state for the poll-thread's info logging. We throttle the spill
  // INFO log to at most one emission per second to honor the user-specified
  // per-executor log-volume cap of "<10 MB/hour per executor" (AAP section 0.1.2).
  // --------------------------------------------------------------------------

  /**
   * Wall-clock timestamp of the most recent "threshold exceeded" INFO log emission.
   * Used by [[pollAndMaybeSpill]] to guard log emission to at most once per second, so
   * that a pathologically-full executor does not flood the log with identical messages
   * while the threshold remains exceeded. Updated by the single poll thread only; no
   * external access.
   */
  private var lastThresholdLogMillis: Long = 0L

  // --------------------------------------------------------------------------
  // Scheduler. Single daemon thread named "streaming-shuffle-memory-poll" per
  // AAP section 0.5.1.1 so that it appears with a distinct prefix in thread-dump
  // diagnostics. The daemon flag ensures the thread does not prevent JVM exit on
  // shutdown; `stop()` still calls `shutdownNow()` as a defensive measure.
  // --------------------------------------------------------------------------

  /**
   * Single-threaded daemon scheduler that runs [[pollAndMaybeSpill]] at a fixed 100 ms
   * rate (poll interval == initial delay). Created eagerly at construction so that
   * utilization telemetry is live the moment the manager is usable.
   */
  private val scheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-memory-poll")

  // Start the poll loop immediately. Using `scheduleAtFixedRate` guarantees the 100 ms
  // cadence is preserved across drift; if a poll runs long (for example, during a heavy
  // spill), the next poll is scheduled immediately after the previous one returns, with
  // the subsequent poll 100 ms later. This preserves the user's <100 ms response time
  // objective under typical load.
  scheduler.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = pollAndMaybeSpill()
    },
    pollIntervalMillis,
    pollIntervalMillis,
    TimeUnit.MILLISECONDS)

  // --------------------------------------------------------------------------
  // Public API. Called by StreamingShuffleWriter (and indirectly by
  // StreamingShuffleManager on construction and teardown).
  // --------------------------------------------------------------------------

  /**
   * Sets the total streaming-buffer budget in bytes across all streaming shuffles on this
   * executor. Typically called exactly once by the writer at task start with the resolved
   * `(executorMemory * bufferSizePercent) / 100` value. Values &lt;= 0 are coerced to 1
   * to prevent division by zero in [[currentUtilizationPercent]].
   *
   * Called infrequently (once per writer at task start) so no caching is performed;
   * the atomic `set` is sufficient.
   *
   * @param bytes the total buffer budget in bytes; clamped to at least `1L`.
   */
  def setBudget(bytes: Long): Unit = {
    totalBudgetBytes.set(math.max(1L, bytes))
  }

  /**
   * Reports the current byte usage of a buffered partition. Called by the writer every
   * time its per-partition buffer grows (for example, after each record appended to the
   * buffer) or shrinks (for example, after a flush). Also updates the last-access
   * timestamp for LRU tie-breaking; the timestamp advances on every report so that a
   * buffer that is actively being written to is considered "fresh" for LRU purposes.
   *
   * Callers must call [[releaseBuffer]] when the partition is no longer buffered (e.g.
   * after the spill has completed or after the writer has finalized output for that
   * partition) to avoid leaking entries into [[perBufferUsage]].
   *
   * @param bufferKey an opaque buffer identifier, typically `"shuffleId-partitionId"`
   * @param bytes the current buffer size in bytes (must be &gt;= 0)
   */
  def reportUsage(bufferKey: String, bytes: Long): Unit = {
    perBufferUsage.put(bufferKey, java.lang.Long.valueOf(bytes))
    lastAccessMillis.put(bufferKey, java.lang.Long.valueOf(System.currentTimeMillis()))
  }

  /**
   * Releases all coordinator state associated with `bufferKey`. Called by the writer when
   * a partition's buffer has been fully flushed (either to the network or to disk) and is
   * no longer eligible for further spill evaluation. Safe to call concurrently with the
   * poll thread; if the poll thread is mid-iteration over the backing
   * [[java.util.concurrent.ConcurrentHashMap]] it observes weakly-consistent snapshots
   * and will simply not select this key as the victim.
   *
   * Also removes any registered spill callback so that a late poll does not invoke a
   * callback that is no longer valid.
   *
   * @param bufferKey the buffer identifier previously passed to [[reportUsage]]
   */
  def releaseBuffer(bufferKey: String): Unit = {
    perBufferUsage.remove(bufferKey)
    lastAccessMillis.remove(bufferKey)
    spillCallbacks.remove(bufferKey)
  }

  /**
   * Registers a spill callback for a buffered partition. When the poll thread selects
   * this `bufferKey` as the eviction victim, it will remove the callback from the
   * registry and invoke [[java.lang.Runnable#run]] exactly once. The callback must
   * perform the actual spill (for example, by invoking
   * [[org.apache.spark.storage.BlockManager#putBytes]] on the writer's behalf).
   *
   * If a callback is already registered for `bufferKey`, it is replaced. The writer is
   * responsible for ensuring that the registered callback correctly targets the most
   * recent buffer state (typically the writer uses a new callback instance each time
   * the buffer grows).
   *
   * @param bufferKey the buffer identifier (must match the key used by [[reportUsage]])
   * @param callback the callback to invoke when this buffer is selected for spill
   */
  def registerSpillCallback(bufferKey: String, callback: Runnable): Unit = {
    spillCallbacks.put(bufferKey, callback)
  }

  /**
   * Returns the current aggregate streaming-buffer utilization as a percentage in
   * `[0.0, +Inf)`. Values exceeding 100.0 are not clamped; callers that need a clamped
   * value (for example, the Dropwizard gauge publication path) should clamp on their
   * side. If the budget has not yet been set (or has been set to 0 or negative via
   * misuse), this method returns `0.0` to avoid a division-by-zero.
   *
   * Thread-safe; reads a weakly-consistent snapshot of [[perBufferUsage]]. Safe to call
   * from any thread, including the poll thread and caller threads.
   *
   * @return the current utilization percent, or `0.0` if no budget has been set
   */
  def currentUtilizationPercent(): Double = {
    val budget = totalBudgetBytes.get()
    if (budget <= 0L) {
      0.0
    } else {
      // Sum all per-buffer bytes in the most recent snapshot.
      // ConcurrentHashMap.values() is a weakly-consistent view; summing it is safe but
      // the result reflects the state at some point during the iteration (not a single
      // atomic snapshot). This is acceptable for the 100 ms polling cadence because the
      // next poll (100 ms later) will re-read and converge on any transient inconsistency.
      var totalBytes = 0L
      val valuesIter = perBufferUsage.values().iterator()
      while (valuesIter.hasNext) {
        val v = valuesIter.next()
        if (v != null) {
          totalBytes += v.longValue()
        }
      }
      currentUsageBytes.set(totalBytes)
      (totalBytes.toDouble / budget.toDouble) * 100.0
    }
  }

  /**
   * Stops the poll thread. Idempotent: safe to call multiple times, and safe to call from
   * any thread (including the poll thread itself, though that would cause a best-effort
   * interruption).
   *
   * After `stop()` returns:
   *   - No further poll iterations will run (scheduler is shut down).
   *   - No further spill callbacks will be invoked from the poll loop of this manager.
   *   - Internal state tables (budget, per-buffer usage, last-access timestamps, and
   *     spill callbacks) are PRESERVED so that diagnostic queries such as
   *     [[currentUtilizationPercent]] continue to return meaningful values after
   *     shutdown. The scheduler thread is the only resource released; the in-memory
   *     state tables are small (bounded by the number of concurrent partitions on the
   *     executor) and are reclaimed by the garbage collector when the manager itself
   *     becomes unreachable.
   *
   * Called by [[StreamingShuffleManager#stop]] on manager shutdown (task, stage, or
   * application teardown).
   */
  def stop(): Unit = {
    // shutdownNow() requests immediate termination of the executor and interrupts the
    // poll thread if it is currently running. Returns any pending tasks, which we
    // discard because the next bound-to-fire task is just another poll iteration.
    // Subsequent calls to shutdownNow() on an already-terminated scheduler are safe
    // no-ops, preserving the idempotent contract of this method.
    scheduler.shutdownNow()
  }

  // --------------------------------------------------------------------------
  // Private poll loop.
  // --------------------------------------------------------------------------

  /**
   * Invoked on every 100 ms tick of the scheduler. Publishes the current utilization to
   * the metrics source, then (if the threshold is crossed) selects a victim buffer and
   * invokes its spill callback.
   *
   * Exception handling: the entire body is wrapped in a `try`/`catch` of `Throwable` so
   * that a pathological callback or an unexpected JVM error cannot cancel the poll
   * schedule. `ScheduledExecutorService.scheduleAtFixedRate` silently cancels the
   * recurring task if a single invocation throws; catching here preserves the cadence.
   */
  private def pollAndMaybeSpill(): Unit = {
    try {
      val util = currentUtilizationPercent()

      // Publish the gauge on every poll regardless of threshold. Even when the executor
      // is idle (utilization == 0.0) we want the gauge to reflect that truthfully so
      // that dashboards show a correct baseline.
      if (metrics != null) {
        metrics.setBufferUtilizationPercent(util)
      }

      if (util >= spillThresholdPct.toDouble) {
        maybeLogThresholdExceeded(util)
        selectVictimAndSpill()
      }
    } catch {
      case t: Throwable =>
        // ERROR because the poll loop is critical infrastructure; a consistent ERROR
        // stream signals a real problem that operators should investigate. The poll
        // schedule continues running so that transient errors do not permanently
        // disable the coordinator.
        logError(log"MemorySpillManager poll iteration failed", t)
    }
  }

  /**
   * Logs an INFO message at most once per second when utilization is at or above the
   * spill threshold. Rate-limited to preserve the user-specified log-volume cap of
   * &lt;10 MB/hour per executor (AAP section 0.1.2).
   *
   * @param util the observed utilization percent
   */
  private def maybeLogThresholdExceeded(util: Double): Unit = {
    val now = System.currentTimeMillis()
    if (now - lastThresholdLogMillis >= 1000L) {
      lastThresholdLogMillis = now
      logInfo(log"Streaming shuffle buffer utilization " +
        log"${MDC(LogKeys.BUFFER_UTILIZATION_PERCENT, util)} reached or exceeded " +
        log"spill threshold ${MDC(LogKeys.THRESHOLD, spillThresholdPct)} (percent); " +
        log"initiating LRU spill of the largest buffered partition.")
    }
  }

  /**
   * Selects the next eviction victim and invokes its spill callback. The selection
   * policy is:
   *
   *   1. Primary order: DESCENDING byte size. Larger buffers spill first because they
   *      free more memory per spill and are most likely responsible for the threshold
   *      crossing.
   *   2. Secondary order (tie-breaker): ASCENDING last-access time. Among buffers of
   *      equal size, the oldest-accessed buffer spills first (classic LRU). This
   *      protects actively-written buffers from being preempted by stale ones.
   *
   * The winning buffer's callback is REMOVED from the registry before being invoked so
   * that a slow callback cannot cause the same buffer to be re-selected by the next
   * poll (which runs on the same thread 100 ms later, but a pathological callback may
   * run longer than 100 ms). If no callbacks are registered this method is a no-op.
   *
   * Exceptions thrown by the callback are caught and logged at WARN level with
   * structured `BLOCK_ID` MDC for diagnostic correlation with the writer's log stream.
   */
  private def selectVictimAndSpill(): Unit = {
    // Build a snapshot Seq from the weakly-consistent ConcurrentHashMap view; this
    // allows us to sort by (size desc, age asc) without holding a lock for the sort
    // duration. The snapshot is O(n) in buffer count which is bounded by the number
    // of concurrent partitions per executor (typically tens to hundreds, never tens
    // of thousands). `.toSeq` materializes the entries so we can sort with a stable
    // comparator.
    val snapshot = perBufferUsage.asScala.toSeq

    if (snapshot.isEmpty) {
      // No buffers currently tracked (for example, between the manager starting and
      // the first `reportUsage` call). Nothing to spill.
      return
    }

    val victim = snapshot
      .sortBy { case (k, v) =>
        // Primary key: negative size => descending by size.
        // Secondary key: last-access millis ascending (so smaller number = older = LRU).
        // If a key has no recorded last-access time we default to
        // `java.lang.Long.MAX_VALUE`, treating it as "most-recently accessed" and
        // therefore LAST candidate among equal-size buffers; this is a conservative
        // choice (we never spill a buffer whose freshness is unknown unless it's the
        // only candidate). `headOption` returns the first element, which is the
        // largest-size, oldest-access victim.
        (
          -v.longValue(),
          lastAccessMillis.getOrDefault(k, java.lang.Long.MAX_VALUE).longValue()
        )
      }
      .headOption

    victim.foreach { case (key, bytes) =>
      // `remove` returns the callback if present and removes it atomically; returns
      // `null` if the callback was never registered or has already been consumed by
      // a prior poll. The `null`-check below handles both cases.
      val callback = spillCallbacks.remove(key)
      if (callback != null) {
        try {
          callback.run()
          if (metrics != null) {
            metrics.incrementSpillCount()
          }
          logInfo(log"Spilled streaming shuffle buffer " +
            log"${MDC(LogKeys.BLOCK_ID, key)} of size " +
            log"${MDC(LogKeys.NUM_BYTES, bytes.longValue())} bytes to disk.")
        } catch {
          case t: Throwable =>
            // Callback failure must not crash the poll thread. We log and move on;
            // the writer is responsible for handling its own failure recovery
            // (retry, fail the task, ...) inside the callback itself.
            logWarning(log"Failed to spill streaming shuffle buffer " +
              log"${MDC(LogKeys.BLOCK_ID, key)}", t)
        }
      }
    }
  }
}
