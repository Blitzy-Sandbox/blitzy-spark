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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS
import org.apache.spark.util.ThreadUtils

/**
 * Streaming-shuffle backpressure protocol: heartbeat-based flow control with token-bucket
 * rate limiting and priority arbitration across concurrent shuffles.
 *
 * == Heartbeat Exchange ==
 * Producer-failure detection uses the 5-second cutoff
 * [[org.apache.spark.shuffle.streaming.PRODUCER_TIMEOUT_MILLIS]]; consumer-failure
 * detection uses the 10-second cutoff
 * [[org.apache.spark.shuffle.streaming.CONSUMER_TIMEOUT_MILLIS]]. A 1-second daemon scan
 * detects timeouts within ~1 second of expiry, sufficient to honor the
 * producer-failure-detection (5 s) and consumer-liveness-heartbeat (10 s) windows specified
 * for the streaming-shuffle feature.
 *
 * == Token-Bucket Rate Limiter ==
 * Refill rate is `maxBandwidthMBps / numConcurrentShuffles` per AAP Section 0.7.2.3 -- where
 * `numConcurrentShuffles` is the count of *distinct currently-active shuffle IDs*, not the
 * count of transmitted blocks. Active shuffles are tracked through explicit
 * [[registerShuffle]] / [[unregisterShuffle]] life-cycle calls invoked by the
 * [[StreamingShuffleManager]] at `registerShuffle` and `unregisterShuffle` SPI boundaries
 * respectively. Bucket capacity defaults to one second of refill at peak rate.
 *
 * The token count is held in a single [[AtomicLong]] and updated via `compareAndSet` so the
 * hot-path acquire is lock-free and wait-free under low contention -- satisfying the
 * "telemetry overhead < 1% CPU utilization" budget specified in AAP Section 0.7.2.5.
 *
 * Each token represents one byte of allowed transmission. When `maxBandwidthMBps == -1`
 * (the default sentinel for "unlimited" per AAP Section 0.5.1.5), the rate limiter is
 * disabled: [[tryAcquire]] always returns `true` and the bucket is held at capacity by the
 * refill scheduler.
 *
 * == Priority Arbitration ==
 * Concurrent shuffles share the bucket, with the per-100-ms refill amount divided across
 * the active shuffle count -- so when N distinct shuffles run concurrently, each receives
 * roughly `maxBandwidthMBps / N` of the configured bandwidth budget. Shuffles with more
 * partitions and larger data volume naturally consume more tokens per unit time and
 * therefore drive proportionally larger transmission rates, satisfying the
 * "priority arbitration" directive specified in AAP Section 0.5.1.3.
 *
 * == Telemetry Emission ==
 * Every backpressure event -- a rate-limit failure in [[tryAcquire]] or a missed heartbeat
 * detected by the periodic scan -- increments the cumulative
 * [[StreamingShuffleMetrics.backpressureEvents]] counter via
 * [[StreamingShuffleMetrics.incrementBackpressureEvents]]. The counter is the operator's
 * primary signal that the streaming path is encountering flow-control pressure and is
 * exposed through the standard executor `MetricsSystem` (see
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleMetrics]]).
 *
 * == Scheduling Choice ==
 * Uses [[org.apache.spark.util.ThreadUtils.newDaemonSingleThreadScheduledExecutor]] rather
 * than Netty's `GlobalEventExecutor` for parity with `MemorySpillManager`'s daemon-thread
 * pattern and predictable test shutdown. Both options are daemon-thread-based and have
 * comparable overhead for this use case; the Spark-idiomatic choice was selected for
 * consistency, easier deterministic shutdown in unit tests, and avoidance of a
 * cross-package dependency from the streaming-shuffle subpackage on Netty internals.
 *
 * == Concurrency ==
 * All mutable state is held in `java.util.concurrent.atomic.*` primitives or
 * [[ConcurrentHashMap]]; there are no `synchronized` blocks on the hot paths
 * ([[tryAcquire]], [[recordTransmission]], [[recordConsumerAck]]). The scheduled refill
 * and heartbeat tasks run on a single dedicated daemon thread, so updates from those tasks
 * never contend with each other -- only with caller threads on `tokens` and the heartbeat
 * maps.
 *
 * == Lifecycle and Coexistence ==
 * Constructed once per [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]]
 * instance (i.e., once per executor JVM when streaming shuffle is opted in). The lifetime
 * of this instance equals the lifetime of the streaming-shuffle code path on this executor.
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."* This protocol class lives
 * entirely within the `org.apache.spark.shuffle.streaming` subpackage and does not modify
 * the existing sort-shuffle code paths or any external transport-layer source.
 *
 * @param metrics streaming-shuffle metric counters that record backpressure events
 * @param conf    `SparkConf` used to read the `maxBandwidthMBps` configuration entry
 */
private[spark] class BackpressureProtocol(
    metrics: StreamingShuffleMetrics,
    conf: SparkConf) extends Logging {

  import BackpressureProtocol._

  /**
   * Configured maximum outbound bandwidth in megabytes per second. The value `-1` is the
   * "unlimited" sentinel per AAP Section 0.5.1.5; any other negative value is rejected by
   * the [[STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS]] config validator at SparkConf-load time.
   */
  private val maxBandwidthMBps: Int = conf.get(STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS)

  /**
   * Set of currently-active shuffle IDs. The size of this set is the divisor used by the
   * per-100-ms refill computation in [[refillTokens]] -- per AAP Section 0.7.2.3, the refill
   * rate is `maxBandwidthMBps / numConcurrentShuffles` where `numConcurrentShuffles` is the
   * count of *distinct* currently-active shuffles, not the number of transmitted blocks.
   *
   * Mutated through [[registerShuffle]] / [[unregisterShuffle]] which are invoked by
   * [[StreamingShuffleManager]] at the corresponding SPI boundaries. Implemented as a
   * [[ConcurrentHashMap]] (used as a set with sentinel `Boolean.TRUE` values) so that
   * `register/unregister` is lock-free and concurrent register/unregister calls from
   * multiple driver-side threads remain race-free.
   */
  private val activeShuffleIds = new ConcurrentHashMap[java.lang.Integer, java.lang.Boolean]()

  /**
   * Token bucket -- current token count. Each token represents one byte of allowed
   * outbound transmission. Refilled by the scheduler at the 100 ms cadence based on the
   * `maxBandwidthMBps / activeShuffleIds.size` formula. Decremented by [[tryAcquire]]
   * using `compareAndSet` for lock-free, wait-free hot-path performance.
   */
  private val tokens = new AtomicLong(0L)

  /**
   * Maximum bucket capacity in bytes. Sized to one second of refill at peak rate so that
   * the bucket can absorb short bursts up to the configured bandwidth without rejecting
   * acquires that arrive in the same 100-ms refill window. When `maxBandwidthMBps <= 0`
   * (unlimited), the cap is `Long.MaxValue` so the bucket is effectively unbounded.
   */
  private val bucketCapacityBytes: Long =
    if (maxBandwidthMBps <= 0) Long.MaxValue
    else maxBandwidthMBps.toLong * 1024L * 1024L

  /**
   * Map from `(shuffleId, mapId)` [[ProducerKey]] to the timestamp of the last
   * block-transmit call for that producer. Scanned at 1-second cadence by
   * [[checkHeartbeats]]; entries older than
   * [[org.apache.spark.shuffle.streaming.PRODUCER_TIMEOUT_MILLIS]] are removed and a
   * backpressure event is recorded.
   *
   * Uses a typed [[ProducerKey]] case class as the map key so the full 64-bit `mapId` is
   * preserved -- this eliminates the bit-discarding collision risk that an encoded-Long
   * key would exhibit when `mapId` exceeds 32 bits in long-running applications.
   */
  private val producerLastSeen =
    new ConcurrentHashMap[ProducerKey, java.lang.Long]()

  /**
   * Map from `(shuffleId, reduceId)` [[ConsumerKey]] to the timestamp of the last consumer
   * acknowledgment. Scanned at 1-second cadence by [[checkHeartbeats]]; entries older than
   * [[org.apache.spark.shuffle.streaming.CONSUMER_TIMEOUT_MILLIS]] are removed and a
   * backpressure event is recorded.
   *
   * Uses a typed [[ConsumerKey]] case class as the map key for consistency with
   * [[producerLastSeen]] and to provide structured types for any future test harness or
   * observability tooling that introspects the heartbeat state.
   */
  private val consumerLastAck =
    new ConcurrentHashMap[ConsumerKey, java.lang.Long]()

  /**
   * Daemon scheduler for the periodic refill (100 ms cadence) and heartbeat scan (1 s
   * cadence). The scheduler uses a single thread so the two tasks never run concurrently,
   * eliminating contention between them and simplifying lifecycle management. Daemon
   * mode ensures the scheduler does not prevent JVM shutdown if [[stop]] is missed by a
   * caller bug.
   */
  private val scheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-backpressure")

  /**
   * Handle to the scheduled refill task -- captured so [[stop]] can cancel it cleanly.
   * Marked `@volatile` because it is written from the constructor thread and read from the
   * caller thread on shutdown; without volatility the read could observe `null` even after
   * the constructor completes on a different thread.
   */
  @volatile
  private var refillFuture: ScheduledFuture[_] = _

  /**
   * Handle to the scheduled heartbeat task -- captured so [[stop]] can cancel it cleanly.
   * Marked `@volatile` for the same publication reason as [[refillFuture]].
   */
  @volatile
  private var heartbeatFuture: ScheduledFuture[_] = _

  /**
   * Idempotent stop guard. Set to `true` exactly once on the first [[stop]] call via
   * `compareAndSet`; subsequent calls observe `true` and become no-ops, so double-shutdown
   * (which would attempt to cancel cancelled futures and shut down a shut-down executor)
   * is safe.
   */
  private val stopped = new AtomicBoolean(false)

  // ------------------------------------------------------------------------
  // Construction-time initialization: prime the bucket and start the scheduled tasks.
  // Initializing tokens to the bucket capacity allows the first acquires to succeed
  // immediately (in the unlimited case) or up to 1 second's worth of bandwidth (in the
  // capped case) without waiting for the first refill tick.
  // ------------------------------------------------------------------------
  tokens.set(bucketCapacityBytes)
  startScheduledTasks()

  /**
   * Start the periodic refill and heartbeat tasks on the daemon scheduler.
   *
   * The refill task runs at the 100 ms cadence -- the same cadence used by
   * [[org.apache.spark.shuffle.streaming.SPILL_POLL_INTERVAL_MILLIS]] in
   * `MemorySpillManager`, so backpressure and spill telemetry remain timing-correlated for
   * operator dashboards.
   *
   * The heartbeat scan runs at 1-second cadence, sufficient to catch the
   * 5-second producer and 10-second consumer timeouts within ~1 second of expiry without
   * burning CPU on more frequent scans.
   *
   * Both tasks wrap their bodies in a try/catch that logs at WARN level on failure, so a
   * transient exception in one task tick (for example, a transient GC-induced timing glitch)
   * never terminates the scheduled task -- it continues to fire on the next cadence.
   */
  private def startScheduledTasks(): Unit = {
    refillFuture = scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try refillTokens()
          catch {
            case t: Throwable => logWarning("Token-bucket refill error", t)
          }
        }
      }, 100L, 100L, TimeUnit.MILLISECONDS)

    heartbeatFuture = scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try checkHeartbeats()
          catch {
            case t: Throwable => logWarning("Heartbeat scan error", t)
          }
        }
      }, 1000L, 1000L, TimeUnit.MILLISECONDS)

    logInfo(log"BackpressureProtocol started " +
      log"(maxBandwidthMBps=${MDC(NUM_BYTES, maxBandwidthMBps.toLong)}, " +
      log"bucketCapacity=${MDC(NUM_BYTES, bucketCapacityBytes)} bytes)")
  }

  /**
   * Refill the token bucket. Computes the per-100-ms refill amount from the configured
   * `maxBandwidthMBps` divided by the current count of registered shuffles
   * (`activeShuffleIds.size`), with a `max(1, ...)` floor so the divisor is never zero,
   * then adds that amount to the bucket up to the configured `bucketCapacityBytes` cap.
   *
   * When `maxBandwidthMBps <= 0` (the unlimited sentinel), the bucket is held at capacity
   * unconditionally so [[tryAcquire]] always succeeds via the early-return shortcut.
   *
   * The refill uses a plain `set` rather than `compareAndSet` because this method runs on
   * the single-threaded scheduler -- there is no contention between concurrent refill
   * ticks. Caller threads in [[tryAcquire]] use `compareAndSet` to safely interleave
   * acquires with refills.
   */
  private def refillTokens(): Unit = {
    if (maxBandwidthMBps <= 0) {
      // Unlimited bandwidth: hold the bucket at capacity so tryAcquire always succeeds.
      tokens.set(bucketCapacityBytes)
      return
    }
    val active = math.max(1L, activeShuffleIds.size().toLong)
    val refillBytesPerSecond = (maxBandwidthMBps.toLong * 1024L * 1024L) / active
    val refillBytesPer100Ms = refillBytesPerSecond / 10L

    val current = tokens.get()
    val newTokens = math.min(bucketCapacityBytes, current + refillBytesPer100Ms)
    tokens.set(newTokens)
  }

  /**
   * Register a new shuffle as active. Increases the divisor in the token-bucket refill
   * computation, giving each concurrent shuffle a proportional share of the configured
   * bandwidth budget per AAP Section 0.7.2.3.
   *
   * Idempotent: calling this method multiple times for the same `shuffleId` is a no-op
   * (the shuffle is registered exactly once).
   *
   * Called by [[StreamingShuffleManager.registerShuffle]] when a new shuffle is added to
   * the streaming-shuffle path.
   *
   * @param shuffleId the shuffle identifier
   */
  def registerShuffle(shuffleId: Int): Unit = {
    activeShuffleIds.put(java.lang.Integer.valueOf(shuffleId), java.lang.Boolean.TRUE)
  }

  /**
   * Unregister a shuffle. Decreases the divisor in the token-bucket refill computation
   * (a no-op if the shuffle was never registered or has already been unregistered).
   *
   * Called by [[StreamingShuffleManager.unregisterShuffle]] when a shuffle is removed
   * from the streaming-shuffle path.
   *
   * @param shuffleId the shuffle identifier
   */
  def unregisterShuffle(shuffleId: Int): Unit = {
    activeShuffleIds.remove(java.lang.Integer.valueOf(shuffleId))
  }

  /**
   * @return a snapshot of the count of currently-active shuffles. Provided for tests and
   *         observability tooling. The returned value is the size at the time of the call;
   *         concurrent register/unregister calls may change the size before or after.
   */
  private[streaming] def numActiveShuffles: Int = activeShuffleIds.size()

  /**
   * Attempt to acquire `byteCount` tokens from the bucket non-blockingly.
   *
   * Returns `true` on success (tokens were sufficient and have been atomically deducted) or
   * `false` on failure (tokens were insufficient; the caller must back off and retry later,
   * typically after the next refill tick). On failure, [[StreamingShuffleMetrics]] is
   * incremented so operators can observe rate-limit hits in dashboards.
   *
   * == Lock-Free Hot Path ==
   * Implemented as a `@tailrec` recursion using `compareAndSet` for the token decrement,
   * which is wait-free under low contention. On CAS failure (another thread updated
   * `tokens` between the read and the CAS), the recursion retries from the top -- re-reading
   * the current value and re-checking the `current >= byteCount` precondition. This is the
   * standard optimistic-concurrency pattern for atomic counters; the `@tailrec` annotation
   * ensures the compiler emits a bytecode-level loop rather than a recursive call, so no
   * stack growth occurs even under sustained CAS contention.
   *
   * == Unlimited Bandwidth Shortcut ==
   * When `maxBandwidthMBps <= 0` (the unlimited sentinel), this method returns `true`
   * immediately without touching the token bucket. This shortcut keeps the rate-limiter
   * overhead at effectively zero CPU when bandwidth limiting is disabled, which is the
   * default configuration.
   *
   * @param byteCount number of tokens to attempt to acquire
   *                  (each token = 1 byte of bandwidth); MUST be `>= 0`
   * @return `true` if the tokens were successfully acquired, `false` otherwise
   * @throws IllegalArgumentException if `byteCount` is negative
   */
  def tryAcquire(byteCount: Long): Boolean = {
    require(byteCount >= 0L, s"byteCount must be non-negative, got $byteCount")
    if (maxBandwidthMBps <= 0) return true // unlimited bandwidth: no rate limiting
    if (byteCount == 0L) return true // zero-byte transmissions are trivially allowed
    tryAcquireRec(byteCount)
  }

  /**
   * Tail-recursive token acquisition with CAS retry. Implements the lock-free body of
   * [[tryAcquire]]. Termination is guaranteed: every recursion either returns `false`
   * (tokens insufficient) or attempts a `compareAndSet` which, on success, returns `true`;
   * on CAS failure the recursion retries with a fresh read of `tokens.get()`.
   *
   * @param byteCount tokens to acquire (already validated as `>= 0` by the caller)
   * @return `true` on successful CAS deduction, `false` on insufficient tokens
   */
  @tailrec
  private def tryAcquireRec(byteCount: Long): Boolean = {
    val current = tokens.get()
    if (current < byteCount) {
      // Insufficient tokens: record a backpressure event so operators can see this in
      // dashboards, then signal failure to the caller for back-off.
      metrics.incrementBackpressureEvents()
      false
    } else if (tokens.compareAndSet(current, current - byteCount)) {
      true
    } else {
      // CAS failed because another thread updated tokens between our read and our CAS.
      // Retry with a fresh `tokens.get()`. @tailrec ensures this is compiled to a loop.
      tryAcquireRec(byteCount)
    }
  }

  /**
   * Record a transmission attempt for the given block. This is a non-blocking, fail-fast
   * operation: a single [[tryAcquire]] is performed; if successful, the heartbeat is
   * updated and `true` is returned; if unsuccessful, the heartbeat is still updated
   * (the producer is alive and trying) and `false` is returned so the caller can decide
   * how to react (typically: back off until the next refill tick, then retry from a higher
   * level with exponential backoff per AAP Section 0.7.2.4).
   *
   * == Naming Note ==
   * This method does NOT perform the network send. The actual block transmission is the
   * responsibility of [[StreamingShuffleWriter]] -- this method only updates rate-limiter
   * and heartbeat state. The writer calls this method before performing the network call
   * so that rate limiting and heartbeat tracking are applied uniformly regardless of
   * which transport primitive the writer chooses. The name `recordTransmission` reflects
   * this accounting role and replaces the prior `transmitBlock` naming, which incorrectly
   * suggested this method performed I/O.
   *
   * == Non-Blocking Design ==
   * Per AAP Section 0.7.1 *"select approach requiring least modification to executor
   * memory model and network transport layer"*, this method is non-blocking and
   * never calls `Thread.sleep`. Bounded back-off retries (with sleep) violate the
   * "no `Thread.sleep` outside test code" engineering rule; instead, the writer-level
   * caller is responsible for retry/back-off using the existing Spark scheduling
   * primitives (such as `ScheduledExecutorService.schedule`).
   *
   * == Producer Heartbeat Update ==
   * The producer heartbeat is updated unconditionally on every call -- even when the rate
   * limiter rejects the transmission -- because the heartbeat reflects "this producer is
   * alive and trying to make progress", which is true regardless of the rate-limiter
   * outcome. Without the unconditional update, a producer that is rate-limited for more
   * than 5 seconds would be falsely declared as failed by [[checkHeartbeats]].
   *
   * @param shuffleId shuffle identifier from [[org.apache.spark.scheduler.DAGScheduler]]
   * @param mapId     map task identifier (typically `taskAttemptId`)
   * @param reduceId  reduce-partition identifier (logged for trace correlation; reserved
   *                  for future per-reducer backpressure tracking)
   * @param byteCount the block byte count for length-based rate limiting; MUST be `>= 0`
   * @param checksum  CRC32C checksum for the block (logged for debug correlation; not
   *                  validated here)
   * @return `true` if tokens were acquired and the network send is permitted; `false`
   *         if the rate limiter rejected the acquire and the caller must back off
   * @throws IllegalArgumentException if `byteCount` is negative
   */
  def recordTransmission(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      byteCount: Long,
      checksum: Long): Boolean = {
    require(byteCount >= 0L, s"byteCount must be non-negative, got $byteCount")
    val acquired = tryAcquire(byteCount)

    // Update producer heartbeat unconditionally, even if the rate limiter rejected --
    // see the "Producer Heartbeat Update" Scaladoc note above for the rationale.
    val producerKey = ProducerKey(shuffleId, mapId)
    producerLastSeen.put(producerKey, java.lang.Long.valueOf(System.currentTimeMillis()))

    logTrace(log"recordTransmission: shuffleId=${MDC(SHUFFLE_ID, shuffleId)} " +
      log"map=${MDC(MAP_ID, mapId)} " +
      log"reduce=${MDC(REDUCE_ID, reduceId)} " +
      log"len=${MDC(NUM_BYTES, byteCount)} " +
      log"crc32c=${MDC(CHECKSUM, checksum)} " +
      log"(acquired=${MDC(NUM_BYTES, if (acquired) 1L else 0L)})")
    acquired
  }

  /**
   * Record a consumer acknowledgment. Updates the consumer-heartbeat timestamp so the
   * heartbeat scanner does not erroneously declare the consumer as failed.
   *
   * Called by [[StreamingShuffleReader]] (or by the implicit-ack mechanism described in
   * AAP Section 0.4.3.2 where the next fetch request serves as proof of consumption
   * progress for prior offsets) on each consumer-side acknowledgment received by the
   * producer side.
   *
   * @param shuffleId shuffle identifier
   * @param reduceId  reduce-partition identifier
   */
  def recordConsumerAck(shuffleId: Int, reduceId: Int): Unit = {
    val key = ConsumerKey(shuffleId, reduceId)
    consumerLastAck.put(key, java.lang.Long.valueOf(System.currentTimeMillis()))
  }

  /**
   * Periodic heartbeat scan: iterate all tracked producer/consumer heartbeat timestamps
   * and remove (with a backpressure-event increment and INFO log) any whose timestamp is
   * older than the configured timeout.
   *
   * This method only detects and reports timeouts; the actual failure-handling response
   * (`FetchFailedException` for failed producers, retransmission for failed consumers) is
   * the responsibility of [[StreamingShuffleWriter]] and [[StreamingShuffleReader]] in
   * concert with the existing `DAGScheduler` upstream-recomputation path. This separation
   * keeps the backpressure protocol focused on flow control while preserving the user
   * directive *"Never modify DAG scheduler, task lifecycle, or user-facing APIs."*
   *
   * Uses [[java.util.Iterator.remove]] on the [[ConcurrentHashMap.entrySet]] iterator for
   * safe in-place removal during iteration. The iterator is weakly consistent per
   * [[ConcurrentHashMap]] semantics, which is acceptable here -- entries added during the
   * scan are picked up in the next scan tick at most 1 second later.
   */
  private def checkHeartbeats(): Unit = {
    val now = System.currentTimeMillis()
    val producerCutoff = now - PRODUCER_TIMEOUT_MILLIS
    val consumerCutoff = now - CONSUMER_TIMEOUT_MILLIS

    val producerIter = producerLastSeen.entrySet().iterator()
    while (producerIter.hasNext) {
      val entry = producerIter.next()
      if (entry.getValue.longValue() < producerCutoff) {
        metrics.incrementBackpressureEvents()
        val key = entry.getKey
        logInfo(log"Producer heartbeat missed for shuffleId=" +
          log"${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"mapId=${MDC(MAP_ID, key.mapId)} " +
          log"timeoutMs=${MDC(TIMEOUT, PRODUCER_TIMEOUT_MILLIS)}")
        producerIter.remove()
      }
    }

    val consumerIter = consumerLastAck.entrySet().iterator()
    while (consumerIter.hasNext) {
      val entry = consumerIter.next()
      if (entry.getValue.longValue() < consumerCutoff) {
        metrics.incrementBackpressureEvents()
        val key = entry.getKey
        logInfo(log"Consumer heartbeat missed for shuffleId=" +
          log"${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"reduceId=${MDC(REDUCE_ID, key.reduceId)} " +
          log"timeoutMs=${MDC(TIMEOUT, CONSUMER_TIMEOUT_MILLIS)}")
        consumerIter.remove()
      }
    }
  }

  /**
   * Stop the protocol. Cancels the scheduled refill and heartbeat tasks, shuts down the
   * daemon scheduler, and clears the heartbeat-tracking maps.
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
   *   1. Cancel the scheduled futures (without interrupting in-flight tasks; setting
   *      `mayInterruptIfRunning = false` lets the current tick complete normally).
   *   2. Initiate orderly executor shutdown via `shutdown()`.
   *   3. Wait up to 2 seconds for in-flight tasks to complete; if not, force shutdown
   *      via `shutdownNow()`.
   *   4. Clear the heartbeat-tracking maps and active-shuffle set so any retained
   *      references are released for GC.
   *
   * The 2-second wait window is generous given that both scheduled tasks complete in well
   * under a millisecond in normal operation; the timeout exists only to bound shutdown in
   * pathological cases (such as a GC pause coinciding with shutdown).
   */
  def stop(): Unit = {
    if (!stopped.compareAndSet(false, true)) return
    if (refillFuture != null) refillFuture.cancel(false)
    if (heartbeatFuture != null) heartbeatFuture.cancel(false)
    refillFuture = null
    heartbeatFuture = null
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(2L, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        // Preserve the interrupt flag for callers that may want to react to interruption,
        // then force-shutdown the executor before returning.
        Thread.currentThread().interrupt()
        scheduler.shutdownNow()
    }
    producerLastSeen.clear()
    consumerLastAck.clear()
    activeShuffleIds.clear()
    logInfo(log"BackpressureProtocol stopped")
  }
}

/**
 * Companion object holding the typed key case classes used by the heartbeat
 * [[ConcurrentHashMap]]s. Exposing these as `private[streaming]` rather than nested types
 * keeps them syntactically lightweight at every call site within the subpackage.
 */
private[streaming] object BackpressureProtocol {

  /**
   * Typed key for the producer-heartbeat map. Replaces the earlier
   * `(shuffleId.toLong << 32) | mapId.toLong` encoding so the full 64-bit `mapId` is
   * preserved -- eliminating the bit-discarding collision risk for long-running
   * applications where the cumulative `taskAttemptId` exceeds 32 bits.
   *
   * @param shuffleId the shuffle identifier
   * @param mapId     the map task identifier (typically `taskAttemptId`)
   */
  case class ProducerKey(shuffleId: Int, mapId: Long)

  /**
   * Typed key for the consumer-heartbeat map. Used for symmetry with [[ProducerKey]]; both
   * fields are 32-bit `Int` so collisions are not a structural concern, but the typed key
   * provides better debuggability in heap dumps and structured-log output than an encoded
   * `Long`.
   *
   * @param shuffleId the shuffle identifier
   * @param reduceId  the reduce-partition identifier
   */
  case class ConsumerKey(shuffleId: Int, reduceId: Int)
}
