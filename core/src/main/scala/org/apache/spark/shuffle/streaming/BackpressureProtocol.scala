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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{MAP_ID, NUM_BYTES, NUM_RETRY, RATE_LIMIT, REDUCE_ID, SHUFFLE_ID}
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter
import org.apache.spark.util.ThreadUtils

/**
 * The lock-free flow-control "brain" of the streaming shuffle backend: a heartbeat plus
 * token-bucket state machine that throttles map-side producers so reduce-side consumers are never
 * overwhelmed, while detecting producer and consumer stalls and driving the failure-recovery
 * transitions mandated by the streaming design.
 *
 * ==Responsibilities==
 *
 *  - '''Producer send gating''' -- [[acquireSendPermit]] (and the non-blocking
 *    [[tryAcquireSendPermit]]) delegate to the composed [[TokenBucketRateLimiter]] (one permit per
 *    byte) so the executor's send rate stays within its configured bandwidth share. A contiguous
 *    period of throttling is counted as exactly one backpressure event via
 *    [[StreamingShuffleMetrics.incBackpressureEvents]], never once per byte.
 *  - '''Liveness tracking''' -- per-stream timestamps are refreshed by [[onHeartbeat]] (producer
 *    liveness, observed at the consumer) and [[onAck]] (consumer liveness, observed at the
 *    producer). The background scan turns a lapse into a timeout flag.
 *  - '''Timeout state machine''' -- a single daemon scheduler runs [[scanOnce]] every
 *    [[StreamingShuffleConfig.SCAN_INTERVAL_MS]] (1 s) and flips the producer-timeout
 *    ([[StreamingShuffleConfig.PRODUCER_TIMEOUT_MS]], 5 s) and consumer-timeout
 *    ([[StreamingShuffleConfig.CONSUMER_TIMEOUT_MS]], 10 s) flags, scheduling retransmits under
 *    exponential backoff.
 *  - '''Bandwidth arbitration''' -- [[onRateLimitRequest]] lets a consumer ask the producer to slow
 *    down; the effective ceiling is the lowest positive request across all concurrent shuffles on
 *    the executor, never above the construction-time cap.
 *
 * ==Liveness model (why two clocks)==
 *
 * Each executor runs a single instance of this class, which for any given stream plays either the
 * producer or the consumer role. The two timeout clocks are therefore deliberately independent:
 *
 *  - On the '''consumer''' side the reader refreshes the heartbeat clock via [[onHeartbeat]] each
 *    time it observes the producer alive (a heartbeat message or an inbound block). If that clock
 *    lapses for more than 5 s the producer is declared timed out and
 *    [[isProducerTimedOut]] tells [[StreamingShuffleReader]] to invalidate its partial reads
 *    and let lineage recompute the lost output.
 *  - On the '''producer''' side the writer refreshes the ack clock via [[onAck]] each time the
 *    consumer acknowledges bytes. If that clock lapses for more than 10 s the consumer is declared
 *    timed out and [[isConsumerTimedOut]] tells [[StreamingShuffleWriter]] to buffer/spill the
 *    unacked data and retransmit when the consumer reconnects.
 *
 * The 10 s heartbeat cadence ([[StreamingShuffleConfig.HEARTBEAT_INTERVAL_MS]]) is the idle
 * keep-alive interval; during active streaming the reader also refreshes the heartbeat clock on
 * every inbound block, so the shorter 5 s producer timeout catches genuine stalls rather than the
 * keep-alive gap.
 *
 * ==Scheduler ownership==
 *
 * This class owns the one and only timer for the protocol. `BackpressureRpcEndpoint` is a thin
 * executor-scoped mailbox that merely forwards heartbeat/ack/rate-limit messages to the `onXxx`
 * methods here; it never schedules its own timer. Keeping the scheduler ownership explicit and
 * singular prevents double-timers and makes the timeout semantics easy to reason about and test.
 *
 * ==Thread-safety==
 *
 * All state is lock-free: per-stream fields are JDK atomics held in a [[ConcurrentHashMap]], and
 * the single per-executor throttle flag is an [[AtomicBoolean]]. The producer hot path
 * ([[acquireSendPermit]]/[[recordSend]]) takes no coarse locks. The background scan iterates the
 * map with weakly consistent semantics and mutates only atomics, so it is safe to run
 * concurrently with the mailbox callbacks and the send path. The rare [[onRateLimitRequest]]
 * reconfiguration is the only multi-stream read, and it converges last-writer-wins.
 *
 * ==Coexistence with the sort-based shuffle==
 *
 * This type is constructed only on the streaming path; it does not touch, wrap, or alter the
 * sort-based shuffle in any way. When the streaming backend falls back, the writer/reader simply
 * stop calling into this protocol, and the daemon scan idles over an empty stream map.
 *
 * @param conf        the typed streaming-shuffle configuration (used for the debug-logging gate and
 *                    as the single source of the timing/retry constants)
 * @param rateLimiter the byte-budget token-bucket gate shared by all producers on this executor
 * @param metrics     the shared telemetry holder; backpressure episodes are counted here
 */
private[spark] class BackpressureProtocol(
    conf: StreamingShuffleConfig,
    rateLimiter: TokenBucketRateLimiter,
    metrics: StreamingShuffleMetrics) extends Logging {

  import BackpressureProtocol._

  // Per-stream liveness, accounting, and timeout state, keyed by the encoded stream identity.
  private val streams = new ConcurrentHashMap[StreamKey, StreamState]()

  // Per-executor throttle-episode flag. It flips false->true when the rate limiter first makes a
  // send wait, and is reset to false the moment a send is admitted without waiting. Counting the
  // backpressure metric on the false->true edge alone keeps it one-per-episode, not per byte.
  private val throttled = new AtomicBoolean(false)

  // Cumulative byte tallies feeding the windowed throughput accessors. Producer bytes accrue as
  // send permits are acquired; consumer bytes accrue as acks arrive.
  private val totalProducerBytes = new AtomicLong(0L)
  private val totalConsumerBytes = new AtomicLong(0L)

  // Most recent windowed throughput samples in bytes/second, recomputed once per scan over the
  // bytes observed since the previous scan. Read by StreamingShuffleFallbackPolicy via the
  // accessors. @volatile gives atomic, immediately-visible reads/writes without locking.
  @volatile private var producerThroughputBps: Long = 0L
  @volatile private var consumerThroughputBps: Long = 0L

  // Scan-window markers captured at the previous scan, used to derive the per-window byte deltas.
  @volatile private var lastScanNanos: Long = System.nanoTime()
  @volatile private var producerBytesAtLastScan: Long = 0L
  @volatile private var consumerBytesAtLastScan: Long = 0L

  // The construction-time ceiling of the shared limiter (the executor's already-80%-factored
  // per-shuffle bandwidth share, or Long.MaxValue when unlimited). Consumer rate-limit requests can
  // only LOWER the effective rate below this base, never raise it above the configured cap.
  private val baseRateBytesPerSec: Long = rateLimiter.currentBytesPerSecond

  // Pre-computed nanosecond thresholds derived once from the millisecond config constants, so the
  // hot scan compares nanoTime deltas without repeated unit conversions.
  private val producerTimeoutNanos: Long =
    TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.PRODUCER_TIMEOUT_MS)
  private val consumerTimeoutNanos: Long =
    TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.CONSUMER_TIMEOUT_MS)

  // The single daemon scheduler that drives the timeout state machine. Held here (not in the RPC
  // endpoint) so there is exactly one timer for the protocol. @volatile because start()/stop()
  // publish and clear it across threads.
  @volatile private var scanScheduler: ScheduledExecutorService = _

  // Guards start()/stop() so both are idempotent and the scheduler is created at most once.
  private val running = new AtomicBoolean(false)

  /**
   * Mutable, lock-free per-stream state. Every field is a JDK atomic so the producer hot path, the
   * RPC mailbox callbacks, and the background scan can all touch a stream concurrently without any
   * lock. A freshly created state starts its liveness clocks at the construction instant so a newly
   * registered stream gets a full timeout window of grace before it can be declared stalled.
   *
   * @param createdNanos the `System.nanoTime()` instant the stream was first seen
   */
  private final class StreamState(createdNanos: Long) {
    // Last instant producer liveness was observed (a heartbeat or inbound block on the consumer
    // side). Drives the 5 s producer timeout.
    val lastHeartbeatNanos = new AtomicLong(createdNanos)
    // Last instant a consumer ack was observed (the producer side). Drives the 10 s consumer
    // timeout.
    val lastAckNanos = new AtomicLong(createdNanos)
    // Bytes sent to the consumer but not yet acknowledged; decremented as acks arrive.
    val unackedBytes = new AtomicLong(0L)
    // Most recent per-stream consumer rate-limit request in bytes/second. Zero means "no request",
    // which keeps the stream out of the arbitration that lowers the shared limiter.
    val requestedRateBytesPerSec = new AtomicLong(0L)
    // Set when producer liveness lapses; read by the reader to invalidate partial reads.
    val producerTimedOut = new AtomicBoolean(false)
    // Set by the scan when consumer acks lapse; read by the writer to buffer/spill and retransmit.
    val consumerTimedOut = new AtomicBoolean(false)
    // Count of retransmit windows opened under exponential backoff after a consumer timeout.
    val retransmitAttempts = new AtomicInteger(0)
    // nanoTime instant before which the next retransmit must not be attempted (backoff deadline).
    val nextRetransmitDeadlineNanos = new AtomicLong(0L)
    // Set once when the retransmit budget is exhausted, so the give-up transition is logged once.
    val exhausted = new AtomicBoolean(false)
  }

  /**
   * Starts the background timeout scan on a single daemon thread. Idempotent: a second call while
   * already running is a no-op. The scan runs every [[StreamingShuffleConfig.SCAN_INTERVAL_MS]] and
   * never blocks the producer hot path. Each iteration is wrapped so a transient failure cannot
   * cancel the fixed-rate schedule.
   */
  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      val scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor(SCAN_THREAD_NAME)
      scanScheduler = scheduler
      val now = System.nanoTime()
      lastScanNanos = now
      producerBytesAtLastScan = totalProducerBytes.get()
      consumerBytesAtLastScan = totalConsumerBytes.get()
      scheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          try {
            scanOnce(System.nanoTime())
          } catch {
            case NonFatal(t) =>
              logWarning(log"Backpressure scan iteration failed; retrying next interval", t)
          }
        }
      }, StreamingShuffleConfig.SCAN_INTERVAL_MS, StreamingShuffleConfig.SCAN_INTERVAL_MS,
        TimeUnit.MILLISECONDS)
      logInfo("Streaming-shuffle backpressure protocol started; timeout scan scheduled.")
    }
  }

  /**
   * Stops the background scan, shuts the daemon scheduler down cleanly (no thread leak), and clears
   * all per-stream state. Idempotent: a second call after stopping is a no-op.
   */
  def stop(): Unit = {
    if (running.compareAndSet(true, false)) {
      val scheduler = scanScheduler
      scanScheduler = null
      if (scheduler != null) {
        ThreadUtils.shutdown(scheduler)
      }
      streams.clear()
      logInfo("Streaming-shuffle backpressure protocol stopped; scheduler shut down.")
    }
  }

  // ---- Stream registration -------------------------------------------------------------------

  /**
   * Registers a stream so the scan begins tracking its liveness. Safe to call repeatedly; the
   * existing state is reused. Call sites can also rely on lazy creation by
   * [[onHeartbeat]]/[[onAck]], but explicit registration starts the timeout clocks immediately.
   *
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   */
  def registerStream(streamKey: StreamKey): Unit = {
    stateOf(streamKey)
  }

  /**
   * Removes a stream and its state once it has completed or failed past recovery, and relaxes the
   * shared rate limit if the departing stream had requested a cap.
   *
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   */
  def unregisterStream(streamKey: StreamKey): Unit = {
    if (streams.remove(streamKey) != null) {
      recomputeRateLimit()
    }
  }

  // ---- Inbound control signals (forwarded by BackpressureRpcEndpoint) ------------------------

  /**
   * Records a producer-liveness signal for a stream (a heartbeat or an inbound block observed on
   * the consumer side), refreshing the producer-timeout clock. A heartbeat after a prior producer
   * timeout clears the flag, so a transient blip that recovers within the window does not force a
   * spurious partial-read invalidation.
   *
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   */
  def onHeartbeat(streamKey: StreamKey): Unit = {
    val state = stateOf(streamKey)
    state.lastHeartbeatNanos.set(System.nanoTime())
    if (state.producerTimedOut.compareAndSet(true, false)) {
      logInfo(log"Producer liveness restored on stream " +
        log"shuffle=${MDC(SHUFFLE_ID, streamKey.shuffleId)} " +
        log"map=${MDC(MAP_ID, streamKey.mapId)} " +
        log"reduce=${MDC(REDUCE_ID, streamKey.reduceId)}")
    }
  }

  /**
   * Records a consumer acknowledgement: refreshes the consumer-timeout clock, decrements the
   * stream's unacked-byte counter (clamped at zero), credits consumer throughput, and -- because an
   * ack proves the consumer is alive -- clears any consumer timeout and resets the retransmit
   * backoff so streaming resumes cleanly on reconnect.
   *
   * @param streamKey  the encoded (shuffleId, mapId, reduceId) stream identity
   * @param bytesAcked the number of bytes the consumer acknowledged; non-positive values only
   *                   refresh liveness
   */
  def onAck(streamKey: StreamKey, bytesAcked: Long): Unit = {
    val state = stateOf(streamKey)
    state.lastAckNanos.set(System.nanoTime())
    if (bytesAcked > 0L) {
      state.unackedBytes.updateAndGet(prev => math.max(0L, prev - bytesAcked))
      totalConsumerBytes.addAndGet(bytesAcked)
    }
    val wasTimedOut = state.consumerTimedOut.compareAndSet(true, false)
    state.retransmitAttempts.set(0)
    state.nextRetransmitDeadlineNanos.set(0L)
    state.exhausted.set(false)
    if (wasTimedOut) {
      logInfo(log"Consumer reconnected; resuming stream " +
        log"shuffle=${MDC(SHUFFLE_ID, streamKey.shuffleId)} " +
        log"map=${MDC(MAP_ID, streamKey.mapId)} " +
        log"reduce=${MDC(REDUCE_ID, streamKey.reduceId)}")
    }
  }

  /**
   * Applies a consumer's request to cap the producer send rate for a stream. The shared limiter is
   * set to the lowest positive request across all active streams (bandwidth arbitration), but never
   * above the construction-time base cap. A non-positive request withdraws this stream's cap.
   *
   * @param streamKey            the encoded (shuffleId, mapId, reduceId) stream identity
   * @param requestedBytesPerSec the requested ceiling in bytes/second; non-positive withdraws it
   */
  def onRateLimitRequest(streamKey: StreamKey, requestedBytesPerSec: Long): Unit = {
    stateOf(streamKey).requestedRateBytesPerSec.set(math.max(0L, requestedBytesPerSec))
    recomputeRateLimit()
  }

  // ---- Producer send gating (hot path) -------------------------------------------------------

  /**
   * Acquires permission to send `bytes` bytes, blocking the calling producer thread until the
   * token-bucket limiter admits them (one permit per byte). Entering a throttle episode increments
   * the backpressure metric exactly once on the false->true edge, never per byte. A non-positive
   * request is a no-op.
   *
   * @param bytes the number of bytes about to be sent
   */
  def acquireSendPermit(bytes: Int): Unit = {
    if (bytes <= 0) {
      return
    }
    totalProducerBytes.addAndGet(bytes.toLong)
    if (rateLimiter.tryAcquire(bytes)) {
      throttled.set(false)
    } else {
      if (throttled.compareAndSet(false, true)) {
        metrics.incBackpressureEvents()
      }
      rateLimiter.acquire(bytes)
    }
  }

  /**
   * Non-blocking variant of [[acquireSendPermit]]. Returns `true` if the permits were granted
   * immediately, `false` if sending would have to wait. A failed attempt still records the start of
   * a throttle episode (counted once per episode). A non-positive request always succeeds.
   *
   * @param bytes the number of bytes about to be sent
   * @return whether the send may proceed without waiting
   */
  def tryAcquireSendPermit(bytes: Int): Boolean = {
    if (bytes <= 0) {
      return true
    }
    if (rateLimiter.tryAcquire(bytes)) {
      totalProducerBytes.addAndGet(bytes.toLong)
      throttled.set(false)
      true
    } else {
      if (throttled.compareAndSet(false, true)) {
        metrics.incBackpressureEvents()
      }
      false
    }
  }

  /**
   * Records that `bytes` bytes were sent on a stream but are not yet acknowledged, so the
   * consumer-failure path knows how much to buffer/spill and retransmit. Lock-free; a non-positive
   * value is a no-op.
   *
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @param bytes     the number of bytes just sent on the stream
   */
  def recordSend(streamKey: StreamKey, bytes: Long): Unit = {
    if (bytes > 0L) {
      stateOf(streamKey).unackedBytes.addAndGet(bytes)
    }
  }

  /**
   * Records that the writer performed a retransmit for a timed-out stream, advancing the
   * exponential-backoff schedule. Each call roughly doubles the wait before the next retransmit
   * and, once the [[StreamingShuffleConfig.RETRY_MAX_ATTEMPTS]] budget is spent, marks the stream
   * exhausted so [[isRetransmitDue]] stops returning `true`.
   *
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @param nowNanos  the current `System.nanoTime()` instant (overridable for tests)
   */
  def recordRetransmit(streamKey: StreamKey, nowNanos: Long = System.nanoTime()): Unit = {
    val state = streams.get(streamKey)
    if (state != null) {
      val attempt = state.retransmitAttempts.incrementAndGet()
      if (attempt >= StreamingShuffleConfig.RETRY_MAX_ATTEMPTS) {
        if (state.exhausted.compareAndSet(false, true)) {
          logWarning(log"Retransmit budget exhausted after " +
            log"${MDC(NUM_RETRY, attempt)} attempts on stream " +
            log"shuffle=${MDC(SHUFFLE_ID, streamKey.shuffleId)} " +
            log"map=${MDC(MAP_ID, streamKey.mapId)} " +
            log"reduce=${MDC(REDUCE_ID, streamKey.reduceId)}")
        }
      } else {
        state.nextRetransmitDeadlineNanos.set(nowNanos + backoffNanos(attempt))
      }
    }
  }

  // ---- Query methods -------------------------------------------------------------------------

  /**
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @return whether the producer for this stream has been declared timed out by the scan; the
   *         reader uses this to invalidate partial reads and surface a fetch failure
   */
  def isProducerTimedOut(streamKey: StreamKey): Boolean = {
    val state = streams.get(streamKey)
    state != null && state.producerTimedOut.get()
  }

  /**
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @return whether the consumer for this stream has been declared timed out by the scan; the
   *         writer uses this to buffer/spill unacked data and retransmit on reconnect
   */
  def isConsumerTimedOut(streamKey: StreamKey): Boolean = {
    val state = streams.get(streamKey)
    state != null && state.consumerTimedOut.get()
  }

  /**
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @return the bytes sent on this stream but not yet acknowledged, or zero if unknown
   */
  def unackedBytes(streamKey: StreamKey): Long = {
    val state = streams.get(streamKey)
    if (state == null) 0L else state.unackedBytes.get()
  }

  /**
   * Reports whether a timed-out stream is due for a retransmit attempt under the
   * exponential-backoff schedule, with budget remaining. The writer polls this to decide when to
   * resend unacked data and calls [[recordRetransmit]] after each resend to advance the schedule.
   *
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @param nowNanos  the current `System.nanoTime()` instant (overridable for tests)
   * @return whether a retransmit may proceed now
   */
  def isRetransmitDue(streamKey: StreamKey, nowNanos: Long = System.nanoTime()): Boolean = {
    val state = streams.get(streamKey)
    state != null && state.consumerTimedOut.get() && !state.exhausted.get() &&
      state.retransmitAttempts.get() < StreamingShuffleConfig.RETRY_MAX_ATTEMPTS &&
      nowNanos >= state.nextRetransmitDeadlineNanos.get()
  }

  /**
   * @param streamKey the encoded (shuffleId, mapId, reduceId) stream identity
   * @return the number of retransmits performed for this stream so far
   */
  def retransmitAttempts(streamKey: StreamKey): Int = {
    val state = streams.get(streamKey)
    if (state == null) 0 else state.retransmitAttempts.get()
  }

  /** @return the most recent producer throughput sample, in bytes/second. */
  def producerThroughput: Long = producerThroughputBps

  /** @return the most recent consumer throughput sample, in bytes/second. */
  def consumerThroughput: Long = consumerThroughputBps

  /** @return the number of streams currently tracked by the protocol. */
  def activeStreamCount: Int = streams.size()

  // ---- Timeout scan state machine ------------------------------------------------------------

  /**
   * Performs one pass of the timeout state machine: detects producer and consumer stalls, opens the
   * first retransmit window for a newly stalled consumer, and recomputes the windowed throughput
   * samples. The scheduler invokes this every [[StreamingShuffleConfig.SCAN_INTERVAL_MS]]; it
   * is exposed to the streaming package so tests can drive the state machine deterministically by
   * passing a synthetic `nowNanos` instead of waiting in real time.
   *
   * @param nowNanos the logical "now" for this pass, in `System.nanoTime()` units
   */
  private[streaming] def scanOnce(nowNanos: Long): Unit = {
    streams.asScala.foreach { case (key, state) =>
      detectProducerTimeout(key, state, nowNanos)
      detectConsumerTimeout(key, state, nowNanos)
    }
    updateThroughputWindow(nowNanos)
  }

  // Lazily creates (or returns) the per-stream state, starting its liveness clocks at "now".
  private def stateOf(streamKey: StreamKey): StreamState = {
    streams.computeIfAbsent(streamKey, _ => new StreamState(System.nanoTime()))
  }

  // Flags a producer timeout on the false->true edge and logs the transition once.
  private def detectProducerTimeout(key: StreamKey, state: StreamState, nowNanos: Long): Unit = {
    if (nowNanos - state.lastHeartbeatNanos.get() > producerTimeoutNanos &&
        state.producerTimedOut.compareAndSet(false, true)) {
      logWarning(log"Producer timed out on stream " +
        log"shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
        log"map=${MDC(MAP_ID, key.mapId)} " +
        log"reduce=${MDC(REDUCE_ID, key.reduceId)}; invalidating partial reads")
    }
  }

  // Flags a consumer timeout on the false->true edge, opens the first retransmit window one initial
  // backoff out, and logs the transition once.
  private def detectConsumerTimeout(key: StreamKey, state: StreamState, nowNanos: Long): Unit = {
    if (nowNanos - state.lastAckNanos.get() > consumerTimeoutNanos &&
        state.consumerTimedOut.compareAndSet(false, true)) {
      state.nextRetransmitDeadlineNanos.set(nowNanos + backoffNanos(0))
      logWarning(log"Consumer timed out on stream " +
        log"shuffle=${MDC(SHUFFLE_ID, key.shuffleId)} " +
        log"map=${MDC(MAP_ID, key.mapId)} " +
        log"reduce=${MDC(REDUCE_ID, key.reduceId)}; " +
        log"unacked=${MDC(NUM_BYTES, state.unackedBytes.get())} bytes, buffering for retransmit")
    }
  }

  // Exponential backoff in nanoseconds: base * 2^attempt, with the shift bounded by the max attempt
  // count to avoid overflow. attempt is always within [0, RETRY_MAX_ATTEMPTS] at the call sites.
  private def backoffNanos(attempt: Int): Long = {
    val shift = math.max(0, math.min(attempt, StreamingShuffleConfig.RETRY_MAX_ATTEMPTS))
    val baseNanos = TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.RETRY_INITIAL_BACKOFF_MS)
    baseNanos * (1L << shift)
  }

  // Recomputes the windowed producer/consumer throughput over the bytes seen since the last scan.
  private def updateThroughputWindow(nowNanos: Long): Unit = {
    val elapsedNanos = nowNanos - lastScanNanos
    if (elapsedNanos > 0L) {
      val producedNow = totalProducerBytes.get()
      val consumedNow = totalConsumerBytes.get()
      producerThroughputBps = ratePerSecond(producedNow - producerBytesAtLastScan, elapsedNanos)
      consumerThroughputBps = ratePerSecond(consumedNow - consumerBytesAtLastScan, elapsedNanos)
      lastScanNanos = nowNanos
      producerBytesAtLastScan = producedNow
      consumerBytesAtLastScan = consumedNow
    }
  }

  // Converts a byte delta over an elapsed nanosecond window into a bytes/second rate, using BigInt
  // to avoid overflow on large windows. Non-positive inputs yield zero.
  private def ratePerSecond(bytes: Long, elapsedNanos: Long): Long = {
    if (bytes <= 0L || elapsedNanos <= 0L) {
      0L
    } else {
      (BigInt(bytes) * NANOS_PER_SECOND / elapsedNanos).toLong
    }
  }

  // Arbitrates the shared executor bandwidth across concurrent shuffles: the effective ceiling is
  // the lowest positive consumer request, never above the construction-time base cap. With no
  // active request the limiter returns to its base (configured) rate.
  private def recomputeRateLimit(): Unit = {
    var minRequest = Long.MaxValue
    streams.values().asScala.foreach { state =>
      val req = state.requestedRateBytesPerSec.get()
      if (req > 0L && req < minRequest) {
        minRequest = req
      }
    }
    val effective =
      if (minRequest == Long.MaxValue) baseRateBytesPerSec
      else math.min(baseRateBytesPerSec, minRequest)
    rateLimiter.setBytesPerSecond(effective)
    if (conf.debug) {
      logDebug(log"Recomputed streaming send rate to ${MDC(RATE_LIMIT, effective)} bytes/sec")
    }
  }
}

private[spark] object BackpressureProtocol {

  /** Thread name for the single daemon scheduler that drives the timeout state machine. */
  private val SCAN_THREAD_NAME = "streaming-shuffle-backpressure-scan"

  /** Nanoseconds in one second, used to convert byte counts into per-second throughput. */
  private val NANOS_PER_SECOND = 1000000000L

  /**
   * Encoded identity of a single streaming shuffle flow: the (shuffleId, mapId, reduceId) triple
   * that uniquely names a producer-to-consumer block stream. Used as the [[ConcurrentHashMap]] key
   * for per-stream backpressure state; case-class equality and hashing give the correct map
   * semantics for free.
   *
   * @param shuffleId the shuffle this stream belongs to
   * @param mapId     the map task (a Long task-attempt id) producing the stream
   * @param reduceId  the reduce partition consuming the stream
   */
  final case class StreamKey(shuffleId: Int, mapId: Long, reduceId: Int)
}
