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

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.annotation.Since
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * The token-bucket and heartbeat flow-control engine that governs the producer -> consumer
 * transfer rate of the streaming shuffle backend (`spark.shuffle.manager=streaming`).
 *
 * This class is the executor-side "brain" of streaming backpressure. It is constructed by
 * `StreamingShuffleManager`, driven by messages that `BackpressureRpcEndpoint` decodes off the
 * wire, and it collaborates with the [[TokenBucketRateLimiter]] to keep a fast producer from
 * overrunning a slow consumer. It performs three jobs:
 *
 *  1. '''Credit accounting (token bucket).''' A single lock-free [[AtomicLong]] tracks the number
 *     of byte-credits the consumer has granted the producer. [[onConsumerAck]] replenishes credit
 *     as the consumer drains data; [[acquire]] debits credit before the producer sends. All token
 *     math is performed with atomic `addAndGet` / `compareAndSet` operations -- there is no lock on
 *     any hot path, which keeps the telemetry/flow-control overhead well under the 1% CPU budget
 *     mandated for the feature.
 *  2. '''Rate limiting.''' [[acquire]] first consults the injected [[TokenBucketRateLimiter]] (a
 *     Guava-backed, byte-granular limiter) so the executor stays within its configured bandwidth
 *     budget. When `spark.shuffle.streaming.maxBandwidthMBps` is `0` (unlimited, the default) the
 *     limiter is a pass-through and the credit gate is disabled, so a freshly started protocol can
 *     never deadlock -- flow is then governed solely by the (pass-through) limiter.
 *  3. '''Liveness detection.''' A daemon [[ScheduledExecutorService]] runs [[scan]] once per second
 *     to detect producers that have gone silent for longer than [[PRODUCER_TIMEOUT_MS]] and
 *     consumers that have missed heartbeats for longer than [[CONSUMER_TIMEOUT_MS]], logging and
 *     cleaning up their tracking entries.
 *
 * '''Producer failure is observability-only here.''' Detecting a producer timeout in [[scan]] only
 * logs a warning and drops the stale tracking entry; it never fails the stage. The authoritative
 * fault path is `StreamingShuffleReader`, which independently throws a
 * `org.apache.spark.shuffle.FetchFailedException` on the same 5-second producer timeout, and that
 * exception is what drives DAG upstream recomputation. The two timeouts are deliberately kept
 * identical ([[PRODUCER_TIMEOUT_MS]] == 5000 ms) so the observability signal and the fault signal
 * agree.
 *
 * '''Buffer reclamation.''' [[onConsumerAck]] releases send credit and records producer liveness;
 * it does not itself free buffer memory. The actual memory reclamation (within 100 ms of the
 * acknowledgment) is performed by `MemorySpillManager` / `StreamingShuffleWriter` on the write
 * side. Keeping this class free of buffer ownership preserves the streaming subsystem's isolation.
 *
 * '''Isolation.''' Like every other type in the `org.apache.spark.shuffle.streaming` package, this
 * class touches no existing shuffle code path. It is instantiated and wired entirely inside the
 * streaming manager and has no effect on the production-stable sort-based shuffle.
 *
 * '''Thread-safety.''' Every mutable field is a concurrent, lock-free structure. [[start]] and
 * [[stop]] are idempotent and safe to call from any thread; the callbacks are invoked from the
 * single-threaded RPC endpoint but may also be touched by producer threads, which the concurrent
 * structures tolerate.
 *
 * @param conf        the typed streaming-shuffle configuration accessor (bandwidth / debug flag)
 * @param metrics     the streaming-shuffle metrics holder; backpressure events are recorded here
 * @param rateLimiter the per-executor, byte-granular rate limiter consulted before each send
 */
@Since("4.2.0")
private[spark] class BackpressureProtocol(
    conf: StreamingShuffleConfig,
    metrics: StreamingShuffleMetrics,
    rateLimiter: TokenBucketRateLimiter)
  extends Logging {

  // Streaming-only MDC correlation-id key/formatter, defined inside the streaming package to keep
  // shared LogKeys untouched (see StreamingShuffleLogKeys for the coexistence rationale).
  import StreamingShuffleLogKeys.{REDUCE_PARTITION_RANGE, singlePartition}

  // Daemon scan cadence: run liveness detection once per second (AAP 0.5.2 + folder spec #8).
  private val SCAN_INTERVAL_MS = 1000L
  // Producer connection timeout: a producer silent longer than this is treated as failed. Kept
  // identical to the reader-side FetchFailedException timeout so the two signals agree.
  private val PRODUCER_TIMEOUT_MS = 5000L
  // Consumer heartbeat liveness timeout: a consumer that misses heartbeats for this long is gone.
  private val CONSUMER_TIMEOUT_MS = 10000L
  // Grace period awaited for the daemon scanner to terminate during stop().
  private val SHUTDOWN_TIMEOUT_MS = 1000L
  // Name of the single daemon scan thread.
  private val SCAN_THREAD_NAME = "streaming-backpressure-scan"

  // ---------------------------------------------------------------------------------------------
  // Security bounds for RPC-originated flow-control payloads.
  //
  // The backpressure callbacks below (onConsumerAck / onThrottleRequest / onHeartbeat) are invoked
  // with values decoded off the RPC wire by BackpressureRpcEndpoint and therefore originate from
  // *remote* executors. They must be treated as untrusted input: a malformed or malicious payload
  // must never be allowed to overflow the credit accumulator, inflate send credit without bound
  // (which would silently defeat backpressure), pollute the liveness/active-shuffle tracking maps
  // with negative identifiers, or corrupt the liveness math in scan(). These constants define the
  // single source of truth for the acceptable ranges; validation is performed both here (the
  // authoritative state owner) and, as defense-in-depth, at the RPC trust boundary.
  // ---------------------------------------------------------------------------------------------

  // Absolute ceiling on outstanding send credit, in bytes. onConsumerAck clamps the token bucket to
  // this value so the credit AtomicLong can never overflow and a fast (or hostile) consumer can
  // never inflate credit past the ceiling. 1 TiB is many orders of magnitude above any legitimate
  // per-executor shuffle credit yet ~2^23x below Long.MaxValue, leaving no realistic overflow risk.
  private val MAX_CREDIT_BYTES = 1L << 40

  // Upper bound on the bytes a single consumer acknowledgment may release. An ack above this is
  // rejected outright as malformed; legitimate acks are bounded by the (much smaller) per-partition
  // buffer capacity, so this is a defensive guard rather than a functional limit.
  private val MAX_ACK_BYTES = MAX_CREDIT_BYTES

  // Upper bound on a dynamically requested throttle rate, in bytes/second. A request above this is
  // treated as malformed and ignored; no physical executor link approaches 1 TiB/s.
  private val MAX_RATE_BYTES_PER_SEC = 1L << 40

  // Maximum tolerated forward clock skew, in millis, for a consumer heartbeat timestamp. A
  // heartbeat dated more than this far in the future is implausible (bad clock or malformed) and
  // is rejected so it cannot make a dead consumer look perpetually alive in scan()'s silence math.
  private val MAX_HEARTBEAT_SKEW_MS = 3600000L

  // A bandwidth cap is configured only when maxBandwidthMBps is positive; 0 means unlimited. When
  // unlimited, the credit gate in acquire() is disabled (see acquire() for the rationale).
  private val bandwidthLimited: Boolean = conf.maxBandwidthMBps > 0
  // Cached debug flag (spark.shuffle.streaming.debug); gates verbose per-message debug logging.
  private val debugEnabled: Boolean = conf.debug

  // Available send credit, in bytes. Seeded at zero and driven entirely by consumer acknowledgment
  // (onConsumerAck adds, acquire subtracts). Lock-free: mutated only via AtomicLong atomics.
  private val tokens = new AtomicLong(0L)

  // executorId -> last heartbeat timestamp (millis). Boxed java.lang.Long values so the map can
  // hold them directly; unboxed via Scala's implicit conversions at each arithmetic use site.
  private val consumerLastSeen = new ConcurrentHashMap[String, java.lang.Long]()

  // mapId -> last-activity timestamp (millis) for producer liveness detection.
  private val producerLastActive = new ConcurrentHashMap[Long, java.lang.Long]()

  // Set of shuffle ids currently being flow-controlled; the count is reported by status(). Entries
  // persist for the executor lifetime (v1 has no per-shuffle teardown hook), which is bounded and
  // consistent with the "no dynamic reconfiguration" constraint.
  private val activeShuffleIds = ConcurrentHashMap.newKeySet[Int]()

  // Guards start()/stop() idempotency and reflects whether the scan daemon is running.
  private val started = new AtomicBoolean(false)

  // The daemon scan executor; created by start(), torn down by stop(). Volatile so the assignment
  // in start() is visible to a concurrent stop() on another thread.
  @volatile private var scanner: ScheduledExecutorService = _

  /**
   * Start the daemon scan that detects producer/consumer timeouts. Idempotent: a second call while
   * already started is a no-op. The scan thread is a daemon so it never blocks JVM shutdown.
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      val threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(SCAN_THREAD_NAME)
        .build()
      val exec = Executors.newSingleThreadScheduledExecutor(threadFactory)
      // scan() is wrapped in try/catch(Throwable) so a throw can never cancel the periodic task.
      exec.scheduleAtFixedRate(() => scan(), SCAN_INTERVAL_MS, SCAN_INTERVAL_MS,
        TimeUnit.MILLISECONDS)
      scanner = exec
      logInfo(s"Started streaming shuffle backpressure protocol (scanIntervalMs=$SCAN_INTERVAL_MS" +
        s", producerTimeoutMs=$PRODUCER_TIMEOUT_MS, consumerTimeoutMs=$CONSUMER_TIMEOUT_MS" +
        s", maxBandwidthMBps=${conf.maxBandwidthMBps}, bandwidthLimited=$bandwidthLimited)")
    }
  }

  /**
   * Stop the daemon scan, shutting the scanner down gracefully. Idempotent: a call when not started
   * is a no-op. Waits up to [[SHUTDOWN_TIMEOUT_MS]] for the in-flight scan to finish before forcing
   * shutdown.
   */
  def stop(): Unit = {
    if (started.compareAndSet(true, false)) {
      val exec = scanner
      if (exec != null) {
        exec.shutdown()
        try {
          if (!exec.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            exec.shutdownNow()
          }
        } catch {
          case _: InterruptedException =>
            exec.shutdownNow()
            // Restore the interrupt status for callers up the stack.
            Thread.currentThread().interrupt()
        }
        scanner = null
      }
      logInfo("Stopped streaming shuffle backpressure protocol")
    }
  }

  /**
   * Record a consumer acknowledgment. This is the refill side of the token bucket: `bytesConsumed`
   * of send credit is released back to the producer (buffer memory itself is reclaimed within
   * 100 ms by `MemorySpillManager` / `StreamingShuffleWriter`, not here). The producer identified
   * by `mapId` is also marked live so [[scan]] does not treat it as timed out.
   *
   * '''Security.''' The arguments are decoded off the RPC wire and therefore originate from a
   * remote executor; they are treated as untrusted input. A message carrying a negative identifier
   * or sequence number is rejected wholesale (it would otherwise pollute the liveness and
   * active-shuffle tracking maps). Credit is released only for a strictly positive `bytesConsumed`
   * no greater than [[MAX_ACK_BYTES]], and the resulting credit is clamped to [[MAX_CREDIT_BYTES]]
   * via a saturating add, so a malformed or hostile acknowledgment can neither overflow the credit
   * [[AtomicLong]] nor inflate credit without bound (either of which would defeat backpressure).
   *
   * @param shuffleId     the shuffle this acknowledgment belongs to (must be >= 0)
   * @param mapId         the producer (map task) whose data was consumed (must be >= 0)
   * @param reduceId      the reduce partition that consumed the data (must be >= 0)
   * @param bytesConsumed number of bytes the consumer drained (credit released); ignored if <= 0,
   *                      rejected if greater than [[MAX_ACK_BYTES]]
   * @param seqNumber     the block sequence number acknowledged (for correlation/observability;
   *                      must be >= 0)
   */
  def onConsumerAck(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      bytesConsumed: Long,
      seqNumber: Int): Unit = {
    // SECURITY: validate the (remote-originated) payload before mutating any state. Shuffle, map
    // and reduce ids and the sequence number are always non-negative in Spark; a negative value
    // marks a malformed or malicious message that must not be allowed to create garbage entries.
    if (shuffleId < 0 || mapId < 0L || reduceId < 0 || seqNumber < 0) {
      logWarning(log"Rejecting malformed streaming shuffle ConsumerAck with a " +
        log"negative identifier: shuffleId=${MDC(LogKeys.SHUFFLE_ID, shuffleId)} " +
        log"mapId=${MDC(LogKeys.MAP_ID, mapId)} " +
        log"reducePartitionRange=${MDC(REDUCE_PARTITION_RANGE, singlePartition(reduceId))} " +
        log"seqNumber=${MDC(LogKeys.VALUE, seqNumber)}")
      return
    }
    // SECURITY: an ack larger than the maximum creditable per-ack byte count is malformed and must
    // be dropped; it would otherwise attempt to release an impossible amount of credit in one step.
    if (bytesConsumed > MAX_ACK_BYTES) {
      logWarning(log"Rejecting streaming shuffle ConsumerAck: bytesConsumed " +
        log"${MDC(LogKeys.NUM_BYTES, bytesConsumed)} exceeds the maximum creditable per-ack byte " +
        log"count for shuffle ${MDC(LogKeys.SHUFFLE_ID, shuffleId)} " +
        log"map ${MDC(LogKeys.MAP_ID, mapId)}")
      return
    }
    if (bytesConsumed > 0L) {
      // Saturating add: credit rises toward but never past MAX_CREDIT_BYTES, so the AtomicLong can
      // never overflow and a fast (or hostile) consumer can never inflate credit past the ceiling.
      addCreditSaturating(bytesConsumed)
    }
    producerLastActive.put(mapId, System.currentTimeMillis())
    activeShuffleIds.add(shuffleId)
    if (debugEnabled) {
      logDebug(s"Consumer ack: shuffleId=$shuffleId mapId=$mapId reduceId=$reduceId " +
        s"bytesConsumed=$bytesConsumed seq=$seqNumber creditNow=${tokens.get()}")
    }
  }

  /**
   * Apply a dynamic throttle requested by a consumer under backpressure. Adjusts the rate limiter
   * to `targetBytesPerSec` and records a backpressure event. A target outside the valid range
   * `(0, `[[MAX_RATE_BYTES_PER_SEC]]`]` is rejected with a warning (the limiter requires a positive
   * rate and no physical link approaches the ceiling); in unlimited pass-through mode the limiter
   * ignores the update, matching the v1 "no dynamic reconfiguration" constraint.
   *
   * '''Security.''' `shuffleId` is decoded off the RPC wire; a negative value is malformed and the
   * request is dropped before any state is recorded. `targetBytesPerSec` is bounded so a hostile
   * request can neither disable throttling with a non-positive rate nor set an absurd rate.
   *
   * @param shuffleId        the shuffle requesting the throttle (must be >= 0)
   * @param targetBytesPerSec the desired maximum send rate in bytes/second
   */
  def onThrottleRequest(shuffleId: Int, targetBytesPerSec: Long): Unit = {
    // SECURITY: reject a malformed shuffle identifier before recording any state.
    if (shuffleId < 0) {
      logWarning(log"Rejecting streaming shuffle ThrottleRequest with a negative shuffleId " +
        log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)}")
      return
    }
    // A positive, bounded target adjusts the limiter; a non-positive or implausibly large target is
    // rejected (the limiter requires a positive rate, and MAX_RATE_BYTES_PER_SEC is far above any
    // real link capacity).
    if (targetBytesPerSec > 0L && targetBytesPerSec <= MAX_RATE_BYTES_PER_SEC) {
      rateLimiter.setRate(targetBytesPerSec.toDouble)
    } else {
      logWarning(log"Ignoring streaming shuffle throttle request with out-of-range " +
        log"targetBytesPerSec=${MDC(LogKeys.NUM_BYTES, targetBytesPerSec)} for shuffle " +
        log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)}")
    }
    activeShuffleIds.add(shuffleId)
    // A throttle request is itself a backpressure event, recorded whether or not the rate changed.
    metrics.incBackpressureEvents()
  }

  /**
   * Record a consumer heartbeat, refreshing the consumer's liveness timestamp so [[scan]] does not
   * evict it.
   *
   * '''Security.''' Both arguments are decoded off the RPC wire. A null or empty executor id cannot
   * be correlated and is ignored so it cannot create a phantom liveness entry. The timestamp is
   * validated to be strictly positive and no more than [[MAX_HEARTBEAT_SKEW_MS]] in the future; an
   * implausible timestamp would otherwise corrupt the silence computation in [[scan]] (a future
   * timestamp yields a negative "silent" duration, making a dead consumer look perpetually alive).
   *
   * @param executorId     the consumer executor id (must be non-empty)
   * @param timestampMillis the heartbeat timestamp in epoch millis (must be > 0 and not far future)
   */
  def onHeartbeat(executorId: String, timestampMillis: Long): Unit = {
    // SECURITY: an empty/blank executor id cannot be correlated and must not create a fake entry.
    if (executorId == null || executorId.isEmpty) {
      logWarning("Ignoring streaming shuffle heartbeat with an empty executor id")
      return
    }
    // SECURITY: reject an implausible timestamp so it cannot corrupt scan()'s silence math.
    val now = System.currentTimeMillis()
    if (timestampMillis <= 0L || timestampMillis > now + MAX_HEARTBEAT_SKEW_MS) {
      logWarning(log"Ignoring streaming shuffle heartbeat with an implausible timestamp " +
        log"${MDC(LogKeys.TIMESTAMP, timestampMillis)} from executor " +
        log"${MDC(LogKeys.EXECUTOR_ID, executorId)}")
      return
    }
    consumerLastSeen.put(executorId, timestampMillis)
    if (debugEnabled) {
      logDebug(s"Consumer heartbeat: executorId=$executorId ts=$timestampMillis")
    }
  }

  /**
   * Producer-side send gate. Returns `true` when the producer may send `numBytes`, or `false` when
   * it must apply backpressure. The decision consults, in order:
   *
   *  1. the byte-granular rate limiter (denied => throttled), and
   *  2. the token-bucket credit, but only when a bandwidth cap is configured.
   *
   * When unlimited (the default), the credit gate is skipped so the producer is never blocked
   * waiting for credit that a stub v1 transport might not yet grant -- flow is then governed solely
   * by the pass-through limiter. Every throttled request records a backpressure event. A
   * non-positive request is trivially granted.
   *
   * @param numBytes the number of bytes the producer wishes to send
   * @return `true` if the send may proceed, `false` if the caller must back off
   */
  def acquire(numBytes: Long): Boolean = {
    if (numBytes <= 0L) {
      true
    } else {
      // Guava permits are ints; a streaming block is capped at 2 MB by the envelope layer, so this
      // saturation is defensive only. In unlimited mode tryAcquire is a pass-through (always true).
      val permits = if (numBytes > Int.MaxValue.toLong) Int.MaxValue else numBytes.toInt
      if (!rateLimiter.tryAcquire(permits)) {
        metrics.incBackpressureEvents()
        false
      } else if (!bandwidthLimited) {
        // No bandwidth cap: credit gate disabled, so a freshly started protocol cannot deadlock.
        true
      } else if (tryConsumeCredit(numBytes)) {
        true
      } else {
        // Producer has outrun the consumer: no credit available, so signal backpressure.
        metrics.incBackpressureEvents()
        false
      }
    }
  }

  /**
   * Current backpressure status for `GetBackpressureStatus` RPC replies.
   *
   * @return a tuple of (number of active shuffles, available send credit in bytes)
   */
  def status: (Int, Long) = (activeShuffleIds.size(), tokens.get())

  /**
   * Atomically add `delta` bytes of send credit, saturating at [[MAX_CREDIT_BYTES]]. Lock-free: a
   * bounded compare-and-set retry loop with no synchronization on the token math. Because the
   * result is clamped to the ceiling, the credit [[AtomicLong]] can never overflow no matter how
   * many or how large the acknowledgments are -- this is the core defense against a malformed or
   * hostile `ConsumerAck` inflating credit and thereby defeating backpressure.
   *
   * @param delta positive number of credit bytes to add (validated and bounded by the caller)
   */
  private def addCreditSaturating(delta: Long): Unit = {
    var updated = false
    while (!updated) {
      val current = tokens.get()
      // `current` is always within [0, MAX_CREDIT_BYTES], so (MAX_CREDIT_BYTES - current) >= 0 and
      // the subtraction cannot overflow; the branch clamps the sum to the ceiling.
      val room = MAX_CREDIT_BYTES - current
      val next = if (delta >= room) MAX_CREDIT_BYTES else current + delta
      if (tokens.compareAndSet(current, next)) {
        updated = true
      }
      // else: lost the race with a concurrent ack/acquire; re-read and retry.
    }
  }

  /**
   * Atomically debit `numBytes` of credit from the token bucket if enough is available. Lock-free:
   * a bounded compare-and-set retry loop with no synchronization on the token math.
   *
   * @param numBytes credit to consume (assumed positive by the sole caller, [[acquire]])
   * @return `true` if the credit was consumed, `false` if insufficient credit was available
   */
  private def tryConsumeCredit(numBytes: Long): Boolean = {
    var claimed = false
    var current = tokens.get()
    while (!claimed && current >= numBytes) {
      if (tokens.compareAndSet(current, current - numBytes)) {
        claimed = true
      } else {
        // Lost the race with a concurrent ack/acquire; re-read and retry.
        current = tokens.get()
      }
    }
    claimed
  }

  /**
   * Periodic liveness scan invoked by the daemon scheduler. Detects and cleans up timed-out
   * producers and consumers. The entire body is wrapped in a catch of [[Throwable]] so that a
   * transient failure can never cancel the scheduled task and silently disable backpressure; the
   * next tick simply runs again.
   */
  private def scan(): Unit = {
    try {
      val now = System.currentTimeMillis()

      // Producer liveness: OBSERVABILITY + tracking cleanup only. The authoritative fault path is
      // StreamingShuffleReader throwing FetchFailedException on the same 5s timeout, which triggers
      // DAG upstream recomputation; here we only warn and drop the stale tracking entry.
      val producerIt = producerLastActive.entrySet().iterator()
      while (producerIt.hasNext) {
        val entry = producerIt.next()
        val idleMs = now - entry.getValue
        if (idleMs > PRODUCER_TIMEOUT_MS) {
          logWarning(s"Streaming shuffle producer mapId=${entry.getKey} timed out after " +
            s"$idleMs ms (> $PRODUCER_TIMEOUT_MS ms); dropping backpressure tracking. " +
            s"Reader-side FetchFailedException drives recovery.")
          producerIt.remove()
        }
      }

      // Consumer liveness: drop consumers that have missed heartbeats and record the loss so it is
      // observable as a backpressure event.
      val consumerIt = consumerLastSeen.entrySet().iterator()
      while (consumerIt.hasNext) {
        val entry = consumerIt.next()
        val silentMs = now - entry.getValue
        if (silentMs > CONSUMER_TIMEOUT_MS) {
          logWarning(s"Streaming shuffle consumer executorId=${entry.getKey} missed heartbeat " +
            s"for $silentMs ms (> $CONSUMER_TIMEOUT_MS ms); removing from liveness set.")
          consumerIt.remove()
          metrics.incBackpressureEvents()
        }
      }
    } catch {
      case t: Throwable =>
        logWarning("Streaming shuffle backpressure scan failed; will retry on next tick", t)
    }
  }
}
