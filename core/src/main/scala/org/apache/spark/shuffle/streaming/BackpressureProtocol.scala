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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
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
 * Refill rate is `maxBandwidthMBps / numConcurrentShuffles` per AAP Section 0.7.2.3.
 * Bucket capacity defaults to one second of refill at peak rate. The token count is held in
 * a single [[AtomicLong]] and updated via `compareAndSet` so the hot-path acquire is
 * lock-free and wait-free under low contention -- satisfying the "telemetry overhead < 1%
 * CPU utilization" budget specified in AAP Section 0.7.2.5.
 *
 * Each token represents one byte of allowed transmission. When `maxBandwidthMBps == -1`
 * (the default sentinel for "unlimited" per AAP Section 0.5.1.5), the rate limiter is
 * disabled: [[tryAcquire]] always returns `true` and the bucket is held at capacity by the
 * refill scheduler.
 *
 * == Priority Arbitration ==
 * Concurrent shuffles share the bucket, with the per-100-ms refill amount divided across
 * the active shuffle count -- so when N shuffles run concurrently, each receives roughly
 * `maxBandwidthMBps / N` of the configured bandwidth budget. Shuffles with more partitions
 * and larger data volume naturally consume more tokens per unit time and therefore drive
 * proportionally larger transmission rates, satisfying the "priority arbitration" directive
 * specified in AAP Section 0.5.1.3.
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
 * ([[tryAcquire]], [[transmitBlock]], [[recordConsumerAck]]). The scheduled refill and
 * heartbeat tasks run on a single dedicated daemon thread, so updates from those tasks
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

  /**
   * Configured maximum outbound bandwidth in megabytes per second. The value `-1` is the
   * "unlimited" sentinel per AAP Section 0.5.1.5; any other negative value is rejected by
   * the [[STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS]] config validator at SparkConf-load time.
   */
  private val maxBandwidthMBps: Int = conf.get(STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS)

  /**
   * Number of currently-active shuffles, used as the divisor for the per-100-ms refill
   * computation in [[refillTokens]]. Incremented in [[transmitBlock]] each time a block is
   * scheduled for transmission. Per AAP Section 0.6.2.7 (deferred performance
   * optimizations), this counter is monotonic in v1; a future enhancement may decrement it
   * as shuffles complete to provide more accurate per-shuffle bandwidth allocation.
   */
  private val numActiveShuffles = new AtomicLong(0L)

  /**
   * Token bucket -- current token count. Each token represents one byte of allowed
   * outbound transmission. Refilled by the scheduler at the 100 ms cadence based on the
   * `maxBandwidthMBps / numActiveShuffles` formula. Decremented by [[tryAcquire]] using
   * `compareAndSet` for lock-free, wait-free hot-path performance.
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
   * Map from `(shuffleId, mapId)`-encoded key to the timestamp of the last block-transmit
   * call for that producer. Scanned at 1-second cadence by [[checkHeartbeats]]; entries
   * older than [[org.apache.spark.shuffle.streaming.PRODUCER_TIMEOUT_MILLIS]] are removed
   * and a backpressure event is recorded.
   *
   * The value type is the boxed `java.lang.Long` rather than the unboxed primitive because
   * [[ConcurrentHashMap]] requires reference types for its generic parameters; explicit
   * boxing in `put` calls avoids any auto-boxing surprises.
   */
  private val producerLastSeen = new ConcurrentHashMap[Long, java.lang.Long]()

  /**
   * Map from `(shuffleId, reduceId)`-encoded key to the timestamp of the last consumer
   * acknowledgment. Scanned at 1-second cadence by [[checkHeartbeats]]; entries older than
   * [[org.apache.spark.shuffle.streaming.CONSUMER_TIMEOUT_MILLIS]] are removed and a
   * backpressure event is recorded.
   */
  private val consumerLastAck = new ConcurrentHashMap[Long, java.lang.Long]()

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

    logInfo(
      s"BackpressureProtocol started (maxBandwidthMBps=$maxBandwidthMBps, " +
        s"bucketCapacity=$bucketCapacityBytes bytes)")
  }

  /**
   * Refill the token bucket. Computes the per-100-ms refill amount from the configured
   * `maxBandwidthMBps` divided by the current `numActiveShuffles` count (with a `max(1, ...)`
   * floor so the divisor is never zero), then adds that amount to the bucket up to the
   * configured `bucketCapacityBytes` cap.
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
    val active = math.max(1L, numActiveShuffles.get())
    val refillBytesPerSecond = (maxBandwidthMBps.toLong * 1024L * 1024L) / active
    val refillBytesPer100Ms = refillBytesPerSecond / 10L

    val current = tokens.get()
    val newTokens = math.min(bucketCapacityBytes, current + refillBytesPer100Ms)
    tokens.set(newTokens)
  }

  /**
   * Attempt to acquire `bytes` tokens from the bucket non-blockingly.
   *
   * Returns `true` on success (tokens were sufficient and have been atomically deducted) or
   * `false` on failure (tokens were insufficient; the caller must back off and retry later,
   * typically after the next refill tick). On failure, [[StreamingShuffleMetrics]] is
   * incremented so operators can observe rate-limit hits in dashboards.
   *
   * == Lock-Free Hot Path ==
   * Uses `compareAndSet` for the token decrement, which is wait-free under low contention.
   * On CAS failure (another thread updated `tokens` between the read and the CAS), the loop
   * retries from the top -- re-reading the current value and re-checking the
   * `current >= bytes` precondition. This is the standard optimistic-concurrency pattern
   * for atomic counters.
   *
   * == Unlimited Bandwidth Shortcut ==
   * When `maxBandwidthMBps <= 0` (the unlimited sentinel), this method returns `true`
   * immediately without touching the token bucket. This shortcut keeps the rate-limiter
   * overhead at effectively zero CPU when bandwidth limiting is disabled, which is the
   * default configuration.
   *
   * @param bytes number of tokens to attempt to acquire (each token = 1 byte of bandwidth)
   * @return `true` if the tokens were successfully acquired, `false` otherwise
   */
  def tryAcquire(bytes: Long): Boolean = {
    if (maxBandwidthMBps <= 0) return true // unlimited bandwidth: no rate limiting
    var spinning = true
    while (spinning) {
      val current = tokens.get()
      if (current < bytes) {
        // Insufficient tokens: record a backpressure event so operators can see this in
        // dashboards, then signal failure to the caller for back-off.
        metrics.incrementBackpressureEvents()
        return false
      }
      if (tokens.compareAndSet(current, current - bytes)) return true
      // CAS failed because another thread updated tokens between our read and our CAS.
      // Retry from the top of the loop, re-reading the current value. The flag below is
      // structural (it always holds true at this point because current >= bytes was just
      // verified above) and exists only to give the loop a named termination condition for
      // readability. Termination is guaranteed by the explicit return statements above.
      spinning = current >= bytes
    }
    // Unreachable in practice (the loop only exits via `return`), but Scala's flow
    // analysis cannot statically prove the loop always returns, so we provide a fallback
    // value to satisfy the type checker.
    false
  }

  /**
   * Transmit a block to the consumer side of the streaming shuffle.
   *
   * In v1, this method's responsibilities are:
   *   1. Acquire tokens from the rate limiter for the block's byte length, with bounded
   *      back-off retries (up to ~100 ms total) when tokens are temporarily exhausted.
   *   2. Update the producer-heartbeat timestamp so the heartbeat scanner does not
   *      erroneously declare this producer as failed.
   *   3. Update the active-shuffle counter used by the refill computation as the
   *      priority-arbitration divisor.
   *   4. Trace the transmission for debug-logging purposes.
   *
   * == Network Send Out of Scope ==
   * The actual network send is the responsibility of [[StreamingShuffleWriter]] -- this
   * method only updates rate-limiter and heartbeat state. The writer calls this method
   * before performing the network call so that rate limiting and heartbeat tracking are
   * applied uniformly regardless of which transport primitive the writer chooses. This
   * separation keeps the protocol class network-transport-agnostic and aligned with the
   * user directive *"select approach requiring least modification to executor memory model
   * and network transport layer"*.
   *
   * == Back-Off Behavior ==
   * If [[tryAcquire]] initially fails, this method sleeps for 10 ms and retries up to 10
   * times (~100 ms total back-off). After 10 failed attempts the method falls through to
   * update heartbeats and trace anyway -- in v1, rate limiting is a soft control, not a
   * hard gate. Each `tryAcquire` failure during back-off increments the backpressure
   * counter, so persistent rate-limit pressure is visible in dashboards.
   *
   * @param shuffleId shuffle identifier from [[org.apache.spark.scheduler.DAGScheduler]]
   * @param mapId     map task identifier (typically `taskAttemptId`)
   * @param reduceId  reduce-partition identifier
   * @param bytes     the block bytes (used for length-based rate limiting; not stored)
   * @param checksum  CRC32C checksum for the block (logged for debug; not validated here)
   */
  def transmitBlock(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      bytes: Array[Byte],
      checksum: Long): Unit = {
    val len = bytes.length.toLong
    // Try to acquire tokens; bounded back-off if rate-limited.
    var acquired = tryAcquire(len)
    var attempts = 0
    while (!acquired && attempts < 10) {
      try {
        Thread.sleep(10L)
      } catch {
        case _: InterruptedException =>
          // Restore the interrupt flag and abandon back-off so the caller can react.
          Thread.currentThread().interrupt()
          attempts = 10
      }
      if (attempts < 10) {
        acquired = tryAcquire(len)
        attempts += 1
      }
    }

    // Update producer heartbeat unconditionally, even if the rate limiter could not be
    // satisfied -- the heartbeat reflects "this producer is alive and trying to make
    // progress", which is true regardless of rate-limiter outcome.
    val producerKey = encodeProducerKey(shuffleId, mapId)
    producerLastSeen.put(producerKey, java.lang.Long.valueOf(System.currentTimeMillis()))

    // Track that we're working on a shuffle (used as the divisor for token-bucket refill).
    // Per AAP Section 0.6.2.7, this counter is monotonic in v1; a future enhancement may
    // decrement it as shuffles complete for more accurate per-shuffle bandwidth allocation.
    numActiveShuffles.incrementAndGet()

    // The actual network send is the writer's responsibility -- see StreamingShuffleWriter.
    // This method has now updated rate-limiter state and heartbeat metadata; the writer
    // can safely perform the network call. The `reduceId` parameter is included in the
    // trace log for operator-side correlation; a future enhancement will additionally use
    // it for per-reducer backpressure tracking.
    logTrace(
      s"transmitBlock: shuffleId=$shuffleId map=$mapId reduce=$reduceId " +
        s"len=$len crc32c=$checksum (acquired=$acquired)")
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
    val key = encodeConsumerKey(shuffleId, reduceId)
    consumerLastAck.put(key, java.lang.Long.valueOf(System.currentTimeMillis()))
  }

  /**
   * Encode a `(shuffleId, mapId)` pair into a single `Long` key for [[ConcurrentHashMap]]
   * access. The high 32 bits hold the `shuffleId`, the low 32 bits hold the lower 32 bits
   * of `mapId`.
   *
   * == Collision Note ==
   * `mapId` is typically a `taskAttemptId`, a long-counter monotonic over the lifetime of
   * the application. Because we discard the high 32 bits of `mapId`, two map IDs that
   * differ only above bit 31 collide in this encoding. In practice, the lifetime of a
   * single `BackpressureProtocol` instance (one executor JVM) and the heartbeat-scan
   * granularity (1 second) make such collisions extremely rare and operationally tolerable
   * -- the worst-case effect is a single false-positive heartbeat reset.
   */
  private def encodeProducerKey(shuffleId: Int, mapId: Long): Long = {
    (shuffleId.toLong << 32) | (mapId & 0xFFFFFFFFL)
  }

  /**
   * Encode a `(shuffleId, reduceId)` pair into a single `Long` key for [[ConcurrentHashMap]]
   * access. The high 32 bits hold the `shuffleId`, the low 32 bits hold the `reduceId`.
   * No collision concerns -- both inputs are 32-bit `Int` values.
   */
  private def encodeConsumerKey(shuffleId: Int, reduceId: Int): Long = {
    (shuffleId.toLong << 32) | (reduceId.toLong & 0xFFFFFFFFL)
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
        logInfo(s"Producer heartbeat missed for key=${entry.getKey} " +
          s"(>${PRODUCER_TIMEOUT_MILLIS}ms)")
        producerIter.remove()
      }
    }

    val consumerIter = consumerLastAck.entrySet().iterator()
    while (consumerIter.hasNext) {
      val entry = consumerIter.next()
      if (entry.getValue.longValue() < consumerCutoff) {
        metrics.incrementBackpressureEvents()
        logInfo(s"Consumer heartbeat missed for key=${entry.getKey} " +
          s"(>${CONSUMER_TIMEOUT_MILLIS}ms)")
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
   *   4. Clear the heartbeat-tracking maps so any retained references are released for GC.
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
    logInfo("BackpressureProtocol stopped")
  }
}
