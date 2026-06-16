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
import org.apache.spark.internal.LogKeys
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter
import org.apache.spark.util.ThreadUtils

/**
 * The lock-free flow-control "brain" of the streaming shuffle backend.
 *
 * `BackpressureProtocol` implements the heartbeat + token-bucket state machine that throttles
 * map-side producers so reduce-side consumers are never overwhelmed, enforces the per-executor
 * bandwidth cap, and drives the timeout-based state transitions behind the streaming-shuffle
 * failure-handling protocol. It composes the Guava-backed
 * [[org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter]] for rate limiting and
 * reports throttling activity to [[StreamingShuffleMetrics]]; every timing value comes from the
 * shared [[StreamingShuffleConfig]] constants so no magic numbers are duplicated here.
 *
 * Separation of concerns: this class is the brain, while `BackpressureRpcEndpoint` (a separate
 * executor-only RPC endpoint) is merely the mailbox that forwards heartbeat, ack, rate-limit, and
 * timeout messages from the network into the handler methods below. To prevent double timers, the
 * periodic timeout scan is owned HERE: [[start]] launches a single daemon scheduled thread that
 * invokes [[scanForTimeouts]] every `SCAN_INTERVAL_MS` (1 s), and the endpoint never schedules
 * its own timer.
 *
 * Lock-free design: per-stream state lives in a single [[java.util.concurrent.ConcurrentHashMap]]
 * of [[StreamKey]] to an internal state record whose fields are all `java.util.concurrent.atomic`
 * primitives. The hot send path ([[acquireSendPermit]]) and the scan never take a coarse lock,
 * keeping flow-control overhead well under the 1% executor-CPU budget.
 *
 * Role model: a single instance serves both producer and consumer roles for the streams on its
 * executor. Each stream lazily activates only the liveness track relevant to the role it plays,
 * which avoids spurious timeouts:
 *   - The '''consumer-liveness''' track activates on the first send ([[acquireSendPermit]] /
 *     [[tryAcquireSendPermit]]) and is refreshed by [[onAck]] / [[onHeartbeat]]; if no consumer
 *     activity is seen within `CONSUMER_TIMEOUT_MS` (10 s) the consumer is declared timed out
 *     (the producer-side view of a dead consumer).
 *   - The '''producer-liveness''' track activates on [[beginConsuming]] and is refreshed by
 *     [[onProducerActivity]]; if no producer activity is seen within `PRODUCER_TIMEOUT_MS` (5 s)
 *     the producer is declared timed out (the consumer-side view of a dead producer).
 *
 * Failure-handling protocol (state transitions only; the actual recovery lives in the reader,
 * writer, and spill manager):
 *   - '''Producer failure''': a 5 s producer timeout sets a flag surfaced by
 *     [[isProducerTimedOut]] so the streaming reader can invalidate partial reads and raise a
 *     `FetchFailedException`.
 *   - '''Consumer failure''': a 10 s consumer timeout sets a flag surfaced by
 *     [[isConsumerTimedOut]] so the streaming writer can buffer unacked data, spill if its buffer
 *     exceeds the spill threshold, and retransmit when the consumer reconnects;
 *     [[nextRetransmitBackoffMs]] supplies the 1 s-start, x2, max-5-attempts backoff schedule.
 *
 * Coexistence: this type is engaged only when the streaming shuffle path is active; it has no
 * effect on, and is never constructed by, the sort-based fallback path.
 *
 * Thread-safety: every public method is safe to call concurrently from producer, consumer, RPC,
 * and scan threads. Structured logs (via [[org.apache.spark.internal.Logging]] with the
 * `shuffle_id`, `map_id`, and `reduce_id` MDC keys) are emitted only on state transitions to
 * respect the < 10 MB/hour/executor streaming-shuffle log budget.
 *
 * @param conf
 *   the typed streaming-shuffle configuration accessor
 * @param rateLimiter
 *   the shared token-bucket limiter gating the producer send path
 * @param metrics
 *   the streaming-shuffle metrics holder receiving backpressure-event counts
 * @param fallbackPolicy
 *   optional shared [[StreamingShuffleFallbackPolicy]] fed live measurements on every scan tick:
 *   producer/consumer throughput (for the sustained slow-consumer condition), derived
 *   network-link utilization (for the saturation condition), and peer protocol versions (for the
 *   version-mismatch condition). `null` (the default) leaves the protocol standalone for unit
 *   tests that exercise flow control in isolation
 */
private[spark] class BackpressureProtocol(
    conf: StreamingShuffleConfig,
    rateLimiter: TokenBucketRateLimiter,
    metrics: StreamingShuffleMetrics,
    fallbackPolicy: StreamingShuffleFallbackPolicy = null)
    extends Logging {

  import BackpressureProtocol.StreamKey

  // Per-stream lock-free state, keyed by stream identity. Weakly-consistent iteration makes it
  // safe for the scan to traverse this map while producer/consumer threads mutate it.
  private val streams = new ConcurrentHashMap[StreamKey, StreamState]()

  // Executor-wide throughput accumulators feeding producerThroughput / consumerThroughput, which
  // StreamingShuffleFallbackPolicy uses to detect a sustained "consumer 2x slower" condition.
  private val totalBytesSent = new AtomicLong(0L)
  private val totalBytesAcked = new AtomicLong(0L)

  // Guards the daemon scan thread so start()/stop() are idempotent and never leak a thread.
  private val running = new AtomicBoolean(false)
  private val startNanos: Long = System.nanoTime()
  @volatile private var scanExecutor: ScheduledExecutorService = null
  @volatile private var scanFuture: java.util.concurrent.ScheduledFuture[_] = null

  // Timeout thresholds resolved once from the shared streaming-shuffle constants so this class
  // never hard-codes timing magic numbers (see StreamingShuffleConfig for the canonical values).
  private val producerTimeoutNanos: Long =
    TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.PRODUCER_TIMEOUT_MS)
  private val consumerTimeoutNanos: Long =
    TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.CONSUMER_TIMEOUT_MS)
  private val scanIntervalMs: Long = StreamingShuffleConfig.SCAN_INTERVAL_MS
  private val nanosPerSecond: Double = TimeUnit.SECONDS.toNanos(1L).toDouble
  private val scanThreadName: String = "streaming-shuffle-backpressure-scan"

  // ---------------------------------------------------------------------------------------------
  // Registration and liveness handlers (invoked by BackpressureRpcEndpoint and the reader).
  // ---------------------------------------------------------------------------------------------

  /**
   * Registers a stream so it is tracked by the timeout scan. Idempotent: re-registering an
   * existing stream is a no-op. Most call sites need not call this explicitly because every
   * handler lazily creates per-stream state on first use; it exists so a stream can be made known
   * before any heartbeat, ack, or send is observed.
   *
   * @param key
   *   the (shuffleId, mapId, reduceId) identity of the stream
   */
  def registerStream(key: StreamKey): Unit = {
    stateOf(key)
  }

  /**
   * Stops tracking a stream and releases its per-stream state. Safe to call for an unknown
   * stream. Typically invoked when a shuffle is unregistered or a stream completes.
   *
   * @param key
   *   the stream identity to forget
   */
  def unregisterStream(key: StreamKey): Unit = {
    streams.remove(key)
  }

  /**
   * Marks the local executor as a consumer of the given stream, starting the producer-liveness
   * clock. After this call the scan will declare the producer timed out if no producer activity
   * is observed within `PRODUCER_TIMEOUT_MS` (5 s). The streaming reader calls this when it
   * begins reading a partition so that a producer that never sends is still detected.
   *
   * @param key
   *   the stream identity the consumer is starting to read
   */
  def beginConsuming(key: StreamKey): Unit = {
    val state = stateOf(key)
    state.producerTracked.set(true)
    state.lastProducerActivityNanos.set(System.nanoTime())
  }

  /**
   * Records observed producer activity (for example, a block received by the consumer),
   * refreshing the producer-liveness clock. If the producer had previously been declared timed
   * out, this clears the flag (the producer reconnected) and resets the retransmit backoff.
   *
   * @param key
   *   the stream identity whose producer was just seen
   */
  def onProducerActivity(key: StreamKey): Unit = {
    val state = stateOf(key)
    state.producerTracked.set(true)
    state.lastProducerActivityNanos.set(System.nanoTime())
    if (state.producerTimedOut.compareAndSet(true, false)) {
      logProducerRecovered(key)
    }
    state.retransmitAttempts.set(0)
  }

  /**
   * Handles a consumer-to-producer heartbeat, refreshing the consumer-liveness clock. Clears any
   * prior consumer-timeout flag (the consumer reconnected) and resets the retransmit backoff.
   *
   * @param key
   *   the stream identity the heartbeat is for
   */
  def onHeartbeat(key: StreamKey): Unit = {
    refreshConsumerLiveness(stateOf(key), key)
  }

  /**
   * Handles a consumer acknowledgement of `bytesAcked` received bytes. Refreshes the
   * consumer-liveness clock, decrements the per-stream unacked counter (clamped at zero), credits
   * consumer throughput, clears any consumer-timeout flag, and resets the retransmit backoff.
   *
   * @param key
   *   the stream identity the ack is for
   * @param bytesAcked
   *   the number of bytes the consumer confirmed receiving; values `<= 0` only refresh liveness
   */
  def onAck(key: StreamKey, bytesAcked: Long): Unit = {
    val state = stateOf(key)
    if (bytesAcked > 0L) {
      state.unackedBytes.updateAndGet(v => math.max(0L, v - bytesAcked))
      totalBytesAcked.addAndGet(bytesAcked)
    }
    refreshConsumerLiveness(state, key)
  }

  /**
   * Applies a consumer-requested rate adjustment to the shared token-bucket limiter, after
   * validating and clamping it against the operator-configured bandwidth cap. In v1 a single
   * limiter governs the executor's streaming send path (per the AAP), so the most recent ACCEPTED
   * request wins.
   *
   * ==Security: an untrusted peer can only ever NARROW the rate, never widen it==
   *
   * The request arrives over RPC from another executor and is therefore untrusted. Applying it
   * verbatim (the pre-hardening behavior) would let a malformed or malicious `RateLimitRequest`
   * bypass the operator's `spark.shuffle.streaming.maxBandwidthMBps` cap - either by sending the
   * non-positive "unlimited" sentinel to disable the cap entirely, or a value above the cap to
   * raise it. To close that unbounded-resource vector (CWE-770), the requested rate is reconciled
   * with [[StreamingShuffleConfig.effectiveBandwidthBytesPerSec]] (the configured ceiling, already
   * 80%-factored; `Long.MaxValue` only when the operator configured the executor as unlimited):
   *
   *  - A positive request is clamped to `min(request, ceiling)`, so a peer can only throttle the
   *    executor DOWN, never above the configured cap (the clamp is a no-op when unlimited).
   *  - A non-positive ("unlimited") request is honored ONLY when the executor is itself configured
   *    unlimited ([[StreamingShuffleConfig.isBandwidthUnlimited]]); otherwise it is REJECTED and
   *    the limiter is left at its configured rate, since no untrusted peer may remove a cap the
   *    operator set.
   *
   * Because the request is consumer-originated control traffic, it always refreshes the
   * consumer-liveness track - even when the rate change itself is rejected - so that sustained
   * rate negotiation alone can never let the scan declare an otherwise-live consumer timed out.
   *
   * @param key
   *   the stream identity the request originated from (tracked and refreshed for liveness)
   * @param requestedBytesPerSec
   *   the new desired throttle in bytes per second (a value `<= 0` is the "unlimited" sentinel)
   */
  def onRateLimitRequest(key: StreamKey, requestedBytesPerSec: Long): Unit = {
    // A rate-limit request is consumer activity: refresh liveness before (possibly) retuning the
    // limiter so active control traffic keeps the stream alive even when the rate change is
    // rejected. Retuning the limiter does not, by itself, touch the liveness clock.
    refreshConsumerLiveness(stateOf(key), key)

    // Reconcile the untrusted request with the operator-configured ceiling so a peer can only
    // narrow the rate, never widen it past spark.shuffle.streaming.maxBandwidthMBps.
    val ceiling = conf.effectiveBandwidthBytesPerSec // Long.MaxValue only when configured unlimited
    val accepted: Option[Long] =
      if (requestedBytesPerSec > 0L) {
        // Positive throttle: clamp DOWN to the configured ceiling (a no-op when unlimited).
        Some(math.min(requestedBytesPerSec, ceiling))
      } else if (conf.isBandwidthUnlimited) {
        // "Unlimited" request honored only because the operator configured no cap to bypass.
        Some(requestedBytesPerSec)
      } else {
        // "Unlimited" request against a finite configured cap: reject (must not disable the cap).
        None
      }

    accepted match {
      case Some(rate) =>
        rateLimiter.setBytesPerSecond(rate)
        if (conf.debug) {
          logDebug(
            log"Adjusted streaming shuffle rate limit to " +
              log"${MDC(LogKeys.NUM_BYTES, rate)} bytes/sec for " +
              log"shuffle ${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)}")
        }
      case None =>
        // A cap-bypass attempt is auditable but infrequent, so a single warn stays within budget.
        logWarning(
          log"Rejected remote streaming shuffle rate-limit request " +
            log"(${MDC(LogKeys.NUM_BYTES, requestedBytesPerSec)} bytes/sec) for shuffle " +
            log"${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)}: a peer may not lift the configured " +
            log"maxBandwidthMBps cap")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Producer send gating (the hot path).
  // ---------------------------------------------------------------------------------------------

  /**
   * Acquires permission to send `bytes` bytes for the given stream, blocking until the
   * token-bucket limiter grants the permits (1 permit = 1 byte). When the limiter cannot grant
   * immediately, a throttle episode begins (recording exactly one backpressure event) and ends as
   * soon as the blocking acquire returns, so every send that actually had to wait is counted as
   * exactly one backpressure episode. Ending the episode after the blocking acquire -- rather than
   * leaving it open until some later non-blocking send happens to succeed -- is what makes
   * independent blocked sends each count once instead of collapsing into a single under-counted
   * episode. A non-positive `bytes` is a no-op.
   *
   * @param key
   *   the stream identity sending data
   * @param bytes
   *   the number of bytes about to be sent
   */
  def acquireSendPermit(key: StreamKey, bytes: Int): Unit = {
    if (bytes > 0) {
      val state = stateOf(key)
      if (rateLimiter.tryAcquire(bytes)) {
        endThrottleEpisode(state)
      } else {
        beginThrottleEpisode(state, key)
        rateLimiter.acquire(bytes)
        // End the episode now that the blocking acquire has returned, so the next send that has
        // to wait begins and counts a fresh, separately-recorded backpressure episode.
        endThrottleEpisode(state)
      }
      recordSend(state, bytes)
    }
  }

  /**
   * Non-blocking variant of [[acquireSendPermit]]. Returns `true` and records the send when the
   * permits are granted immediately; returns `false` (beginning a throttle episode) when they are
   * not, leaving the caller to retry. A non-positive `bytes` always succeeds.
   *
   * @param key
   *   the stream identity sending data
   * @param bytes
   *   the number of bytes about to be sent
   * @return
   *   `true` if the send may proceed now, `false` if it must be retried later
   */
  def tryAcquireSendPermit(key: StreamKey, bytes: Int): Boolean = {
    if (bytes <= 0) {
      true
    } else {
      val state = stateOf(key)
      if (rateLimiter.tryAcquire(bytes)) {
        endThrottleEpisode(state)
        recordSend(state, bytes)
        true
      } else {
        beginThrottleEpisode(state, key)
        false
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Query methods (consumed by the reader, writer, and StreamingShuffleFallbackPolicy).
  // ---------------------------------------------------------------------------------------------

  /**
   * @param key
   *   the stream identity to query
   * @return
   *   `true` if the producer for this stream has been declared timed out by the scan
   */
  def isProducerTimedOut(key: StreamKey): Boolean = {
    val state = streams.get(key)
    state != null && state.producerTimedOut.get()
  }

  /**
   * @param key
   *   the stream identity to query
   * @return
   *   `true` if the consumer for this stream has been declared timed out by the scan
   */
  def isConsumerTimedOut(key: StreamKey): Boolean = {
    val state = streams.get(key)
    state != null && state.consumerTimedOut.get()
  }

  /**
   * @param key
   *   the stream identity to query
   * @return
   *   the number of bytes sent but not yet acknowledged for this stream, or `0` if the stream is
   *   unknown
   */
  def unackedByteCount(key: StreamKey): Long = {
    val state = streams.get(key)
    if (state == null) 0L else state.unackedBytes.get()
  }

  /**
   * @return
   *   the aggregate producer throughput in bytes per second since this protocol was constructed
   *   (total bytes sent divided by elapsed seconds)
   */
  def producerThroughput: Double = bytesPerSecond(totalBytesSent.get())

  /**
   * @return
   *   the aggregate consumer throughput in bytes per second since this protocol was constructed
   *   (total bytes acknowledged divided by elapsed seconds)
   */
  def consumerThroughput: Double = bytesPerSecond(totalBytesAcked.get())

  /**
   * Records a streaming-protocol version advertised by a peer executor (delivered by the
   * executor-only `BackpressureRpcEndpoint` on a `PeerVersion` control message). When the peer's
   * version differs from this build's [[StreamingShuffleConfig.STREAMING_PROTOCOL_VERSION]], the
   * version-mismatch revert condition is tripped on the shared fallback policy so a mixed-version
   * cluster (for example, mid-rolling-upgrade) automatically reverts new shuffles to the
   * sort-based path rather than risking an incompatible exchange. Null-safe when no policy is
   * attached (standalone unit tests); a matching version is a no-op.
   *
   * @param peerVersion
   *   the streaming-protocol version advertised by the remote executor
   */
  def recordPeerProtocolVersion(peerVersion: Int): Unit = {
    val mismatch = peerVersion != StreamingShuffleConfig.STREAMING_PROTOCOL_VERSION
    if (mismatch && fallbackPolicy != null) {
      fallbackPolicy.markVersionMismatch()
    }
  }

  /**
   * @return
   *   the configured consumer-to-producer heartbeat interval in milliseconds, exposed so the RPC
   *   endpoint can schedule heartbeats at the same cadence the timeouts assume
   */
  def heartbeatIntervalMs: Long = StreamingShuffleConfig.HEARTBEAT_INTERVAL_MS

  /**
   * @return
   *   the number of streams currently tracked (primarily for tests and diagnostics)
   */
  def registeredStreamCount: Int = streams.size()

  /**
   * Computes the next retransmit backoff for a stream using exponential backoff that starts at
   * `RETRY_INITIAL_BACKOFF_MS` (1 s) and doubles each attempt, capped at `RETRY_MAX_ATTEMPTS`
   * (5). Each call advances the attempt counter; the counter is reset to zero whenever positive
   * liveness is observed ([[onAck]] / [[onHeartbeat]] / [[onProducerActivity]]).
   *
   * @param key
   *   the stream identity scheduling a retransmit
   * @return
   *   the backoff delay in milliseconds, or `-1` once the maximum attempts are exhausted
   */
  def nextRetransmitBackoffMs(key: StreamKey): Long = {
    val initial = StreamingShuffleConfig.RETRY_INITIAL_BACKOFF_MS
    val maxAttempts = StreamingShuffleConfig.RETRY_MAX_ATTEMPTS
    val attempt = stateOf(key).retransmitAttempts.incrementAndGet()
    if (attempt > maxAttempts) {
      -1L
    } else {
      initial * (1L << (attempt - 1))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Timeout scan state machine and lifecycle.
  // ---------------------------------------------------------------------------------------------

  /**
   * Explicitly marks both liveness tracks of the addressed stream as timed out, immediately and
   * regardless of how much idle time has elapsed. This backs the rare, out-of-band `Timeout`
   * control message: an explicit signal that a peer is unreachable must take effect at once rather
   * than waiting for the next periodic [[scanForTimeouts]] or for the idle threshold to be crossed
   * (the periodic scan alone could ignore a freshly-signalled timeout whose idle time has not yet
   * elapsed, or whose per-stream state was only just created). Marking both the producer and
   * consumer tracks makes the signal role-agnostic: whichever role this executor plays for the
   * stream, the timeout becomes visible through [[isProducerTimedOut]] / [[isConsumerTimedOut]] so
   * the reader can invalidate partial reads and the writer can begin buffering and retransmit. The
   * transition is logged once on each false -> true edge, mirroring the scan, to respect the log
   * budget; re-marking an already-timed-out stream logs nothing.
   *
   * @param key
   *   the stream identity to mark timed out
   */
  def markTimedOut(key: StreamKey): Unit = {
    val state = stateOf(key)
    if (state.producerTimedOut.compareAndSet(false, true)) {
      logProducerTimedOut(key)
    }
    if (state.consumerTimedOut.compareAndSet(false, true)) {
      logConsumerTimedOut(key)
    }
  }

  /**
   * Scans every tracked stream once and applies the producer- and consumer-timeout transitions
   * relative to `nowNanos`. This is the single point of timeout detection; the daemon scheduler
   * started by [[start]] invokes it with `System.nanoTime()` every `SCAN_INTERVAL_MS`, and tests
   * drive it directly with a controlled clock value. It is lock-free and only mutates atomic
   * flags, so it is safe to run concurrently with the hot path.
   *
   * @param nowNanos
   *   the current time in nanoseconds against which idle durations are measured
   */
  private[streaming] def scanForTimeouts(nowNanos: Long): Unit = {
    streams.asScala.foreach { case (key, state) =>
      checkProducerTimeout(key, state, nowNanos)
      checkConsumerTimeout(key, state, nowNanos)
    }
  }

  /**
   * Starts the daemon scan thread. Idempotent: a second call while already running is ignored.
   * The single-threaded scheduled executor is a daemon, so it never blocks JVM shutdown, and it
   * is fully torn down by [[stop]].
   */
  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(scanThreadName)
      val task: Runnable = () => runScanSafely()
      scanFuture =
        executor.scheduleAtFixedRate(task, scanIntervalMs, scanIntervalMs, TimeUnit.MILLISECONDS)
      scanExecutor = executor
      logInfo(log"Streaming shuffle backpressure protocol started")
    }
  }

  /**
   * Stops the daemon scan thread and releases it. Idempotent: a call when not running is a no-op.
   * Cancels the scheduled scan and shuts the executor down so no thread is leaked (verified under
   * `spark.unsafe.exceptionOnMemoryLeak=true` in the stress suite).
   */
  def stop(): Unit = {
    if (running.compareAndSet(true, false)) {
      val future = scanFuture
      if (future != null) {
        future.cancel(false)
        scanFuture = null
      }
      val executor = scanExecutor
      if (executor != null) {
        ThreadUtils.shutdown(executor)
        scanExecutor = null
      }
      logInfo(log"Streaming shuffle backpressure protocol stopped")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------------------------

  /** Returns the per-stream state, creating it on first use so callers need not pre-register. */
  private def stateOf(key: StreamKey): StreamState = {
    streams.computeIfAbsent(key, _ => new StreamState())
  }

  /**
   * Refreshes the consumer-liveness track on any valid consumer-originated control signal
   * (heartbeat, ack, or rate-limit request): marks the consumer tracked, stamps the activity
   * clock, clears any prior consumer-timeout flag (logging the reconnect once on the true -> false
   * edge), and resets the retransmit backoff. Centralizing this guarantees every consumer signal
   * keeps the stream alive identically, so control traffic such as rate-limit negotiation can
   * never be mistaken by the scan for a dead consumer.
   */
  private def refreshConsumerLiveness(state: StreamState, key: StreamKey): Unit = {
    state.consumerTracked.set(true)
    state.lastConsumerActivityNanos.set(System.nanoTime())
    if (state.consumerTimedOut.compareAndSet(true, false)) {
      logConsumerRecovered(key)
    }
    state.retransmitAttempts.set(0)
  }

  /** Records a granted send: tracks consumer liveness, unacked bytes, and producer throughput. */
  private def recordSend(state: StreamState, bytes: Int): Unit = {
    if (state.consumerTracked.compareAndSet(false, true)) {
      state.lastConsumerActivityNanos.set(System.nanoTime())
    }
    state.unackedBytes.addAndGet(bytes.toLong)
    totalBytesSent.addAndGet(bytes.toLong)
  }

  /** Begins a throttle episode, counting exactly one backpressure event per episode. */
  private def beginThrottleEpisode(state: StreamState, key: StreamKey): Unit = {
    if (state.throttled.compareAndSet(false, true)) {
      metrics.incBackpressureEvents()
      if (conf.debug) {
        logDebug(
          log"Backpressure throttling engaged for " +
            log"shuffle ${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} " +
            log"map ${MDC(LogKeys.MAP_ID, key.mapId)} " +
            log"reduce ${MDC(LogKeys.REDUCE_ID, key.reduceId)}")
      }
    }
  }

  /** Ends a throttle episode so the next blocked send starts a fresh, separately counted one. */
  private def endThrottleEpisode(state: StreamState): Unit = {
    state.throttled.set(false)
  }

  /** Producer-timeout transition: flag and warn once on the false -> true edge. */
  private def checkProducerTimeout(key: StreamKey, state: StreamState, now: Long): Unit = {
    if (state.producerTracked.get() && !state.producerTimedOut.get()) {
      val idle = now - state.lastProducerActivityNanos.get()
      if (idle > producerTimeoutNanos && state.producerTimedOut.compareAndSet(false, true)) {
        logProducerTimedOut(key)
      }
    }
  }

  /** Consumer-timeout transition: flag and warn once on the false -> true edge. */
  private def checkConsumerTimeout(key: StreamKey, state: StreamState, now: Long): Unit = {
    if (state.consumerTracked.get() && !state.consumerTimedOut.get()) {
      val idle = now - state.lastConsumerActivityNanos.get()
      if (idle > consumerTimeoutNanos && state.consumerTimedOut.compareAndSet(false, true)) {
        logConsumerTimedOut(key)
      }
    }
  }

  /** Runs one scan, swallowing non-fatal errors so the daemon scheduler keeps running. */
  private def runScanSafely(): Unit = {
    try {
      scanForTimeouts(System.nanoTime())
      feedFallbackSignals()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle backpressure scan failed", e)
    }
  }

  // Feeds the shared fallback policy the live throughput and network-utilization measurements on
  // every 1 s scan tick, so the sustained-slow-consumer and network-saturation revert conditions
  // trip from real runtime state rather than never being updated. Null-guarded so a standalone
  // protocol (unit tests) runs without a policy. This is the production write path the review
  // found missing for the slow-consumer and network-saturation fallback signals.
  private def feedFallbackSignals(): Unit = {
    if (fallbackPolicy != null) {
      fallbackPolicy.recordThroughput(producerThroughput.toLong, consumerThroughput.toLong)
      fallbackPolicy.updateNetworkUtilization(currentNetworkUtilizationPercent())
    }
  }

  // Derives the per-executor network-link-utilization percentage from producer throughput relative
  // to the configured bandwidth cap. An unlimited (uncapped) limiter has no link the streaming
  // path can saturate, so it reports 0%; otherwise utilization is producer bytes/sec as a
  // percentage of the cap, clamped to [0, 100].
  private def currentNetworkUtilizationPercent(): Double = {
    if (rateLimiter.isUnlimited) {
      0.0
    } else {
      val cap = rateLimiter.currentBytesPerSecond
      if (cap <= 0L || cap == Long.MaxValue) {
        0.0
      } else {
        math.min(100.0, producerThroughput * 100.0 / cap.toDouble)
      }
    }
  }

  private def bytesPerSecond(bytes: Long): Double = {
    val elapsedSeconds = (System.nanoTime() - startNanos).toDouble / nanosPerSecond
    if (elapsedSeconds <= 0.0) 0.0 else bytes.toDouble / elapsedSeconds
  }

  private def logProducerTimedOut(key: StreamKey): Unit = {
    logWarning(
      log"Streaming shuffle producer timed out; signaling partial-read " +
        log"invalidation for shuffle ${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} " +
        log"map ${MDC(LogKeys.MAP_ID, key.mapId)} " +
        log"reduce ${MDC(LogKeys.REDUCE_ID, key.reduceId)}")
  }

  private def logConsumerTimedOut(key: StreamKey): Unit = {
    logWarning(
      log"Streaming shuffle consumer timed out; buffering unacked data and " +
        log"scheduling retransmit for shuffle ${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} " +
        log"map ${MDC(LogKeys.MAP_ID, key.mapId)} " +
        log"reduce ${MDC(LogKeys.REDUCE_ID, key.reduceId)}")
  }

  private def logProducerRecovered(key: StreamKey): Unit = {
    logInfo(
      log"Streaming shuffle producer recovered for " +
        log"shuffle ${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} " +
        log"map ${MDC(LogKeys.MAP_ID, key.mapId)} " +
        log"reduce ${MDC(LogKeys.REDUCE_ID, key.reduceId)}")
  }

  private def logConsumerRecovered(key: StreamKey): Unit = {
    logInfo(
      log"Streaming shuffle consumer reconnected; resuming for " +
        log"shuffle ${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} " +
        log"map ${MDC(LogKeys.MAP_ID, key.mapId)} " +
        log"reduce ${MDC(LogKeys.REDUCE_ID, key.reduceId)}")
  }

  /**
   * Per-stream lock-free state. Every field is a `java.util.concurrent.atomic` primitive so the
   * hot send, ack, and scan paths mutate it without ever taking a coarse lock.
   */
  private final class StreamState {
    val lastProducerActivityNanos = new AtomicLong(0L)
    val producerTracked = new AtomicBoolean(false)
    val lastConsumerActivityNanos = new AtomicLong(0L)
    val consumerTracked = new AtomicBoolean(false)
    val unackedBytes = new AtomicLong(0L)
    val producerTimedOut = new AtomicBoolean(false)
    val consumerTimedOut = new AtomicBoolean(false)
    val throttled = new AtomicBoolean(false)
    val retransmitAttempts = new AtomicInteger(0)
  }
}

/**
 * Companion holding the stream-identity key for the backpressure protocol.
 */
private[spark] object BackpressureProtocol {

  /**
   * Identity of a single streaming-shuffle stream: the producing map task and the consuming
   * reduce partition within a shuffle. Used as the key for all per-stream backpressure state and
   * as the correlation identity in structured logs (`shuffle_id`, `map_id`, `reduce_id`).
   *
   * @param shuffleId
   *   the shuffle this stream belongs to
   * @param mapId
   *   the producing map task's id
   * @param reduceId
   *   the consuming reduce partition's id
   */
  case class StreamKey(shuffleId: Int, mapId: Long, reduceId: Int)
}
