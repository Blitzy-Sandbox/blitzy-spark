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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * The consumer-to-producer flow-control protocol for the streaming shuffle data path (F-107).
 *
 * This class is the protocol/state-machine: it owns the in-memory, lock-free flow-control state
 * and exposes the decisions a producer (`StreamingShuffleWriter`) and consumer
 * (`StreamingShuffleReader`) act on. It does NOT perform any network I/O itself; the
 * cross-executor transport of the [[BackpressureMessage]] envelopes it defines is carried by
 * the `BackpressureRpcEndpoint` (F-108), which depends on this class.
 *
 * '''Two complementary rate mechanisms, both honoring the 80% factor.''' The protocol
 * distinguishes two facets of "rate" that operate on different time scales and are
 * intentionally kept separate:
 *
 *  1. A lock-free '''credit window''' (an instantaneous in-flight byte budget) backed by a single
 *     [[java.util.concurrent.atomic.AtomicLong]] where ''one token == one byte''. Its capacity
 *     is capped at 80% of the supplied link capacity (the
 *     [[org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter.BANDWIDTH_CAP_FACTOR]]
 *     factor). A producer calls [[tryAcquire]] before transmitting a block and the consumer
 *     calls [[refill]] as buffer space is reclaimed. This window bounds how many unacknowledged
 *     bytes may be in flight at any instant.
 *  2. A '''sustained byte-rate cap''' delegated to a composed
 *     [[org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter]] (F-110), constructed
 *     via `fromMaxBandwidthMBps`, which already applies the same 80% link-capacity factor and the
 *     MB/s-to-bytes/s conversion. This shapes throughput over time (bytes per second) rather than
 *     the instantaneous in-flight budget.
 *
 * Composing the existing limiter for the sustained cap avoids duplicating Guava-based,
 * time-windowed rate-limiting logic, while the `AtomicLong` credit window provides the cheap,
 * lock-free per-block admission decision the writer makes on its hot path.
 *
 * '''Heartbeat.''' A 5 s ([[BackpressureProtocol.HEARTBEAT_INTERVAL_MS]]) liveness signal is
 * the flow-control heartbeat (AAP timing semantics). [[recordHeartbeat]] stamps the most recent
 * heartbeat and [[isHeartbeatExpired]] reports whether the window has elapsed so a stalled peer
 * can be detected.
 *
 * '''Buffer-utilization monitoring.''' [[updateUtilization]] is fed the current buffer fill
 * level and signals backpressure when utilization crosses the configured threshold (default
 * 80%). The activation is edge-triggered: a single backpressure event is counted (via
 * `metrics.incrementBackpressureEvents()`) on each low-to-high transition rather than on every
 * poll while utilization remains high, and the latch is released when utilization falls back
 * below the threshold.
 *
 * '''Monotonic acknowledgment merge.''' Acknowledgments are sequence-numbered. [[mergeAck]]
 * advances a monotonically non-decreasing high-water mark with a compare-and-set loop so that
 * out-of-order or duplicate acknowledgments can never regress [[ackWatermark]].
 *
 * '''Priority arbitration.''' [[arbitrate]] selects the highest-priority competing stream using
 * a straightforward v1 policy: the most-starved stream (the smallest remaining capacity) wins,
 * with ties broken in favor of the oldest stream (the greatest age). The policy is
 * intentionally simple and stateless.
 *
 * '''Concurrency model.''' Every piece of shared mutable state is backed by a JDK atomic and all
 * mutators use lock-free compare-and-set loops; no coarse locks are taken. All public methods are
 * therefore safe to invoke concurrently from multiple executor threads without external
 * synchronization.
 *
 * @param metrics                   the shared streaming-shuffle metrics holder; backpressure
 *                                   activations are tallied through it
 * @param linkCapacityBytes         the raw per-link capacity, in bytes, used to derive the
 *                                   80%-capped credit-window capacity; a non-positive value means
 *                                   the credit window is unlimited (a no-op pass-through)
 * @param maxBandwidthMBps          the per-executor bandwidth ceiling, in MB/s, for the composed
 *                                   sustained-rate limiter; `<= 0` means unlimited
 * @param utilizationThresholdPercent the buffer-utilization percentage at which backpressure
 *                                   activates; clamped into the inclusive range [1, 100]
 */
private[spark] class BackpressureProtocol(
    metrics: StreamingShuffleMetrics,
    linkCapacityBytes: Long,
    maxBandwidthMBps: Int = 0,
    utilizationThresholdPercent: Int =
      BackpressureProtocol.DEFAULT_UTILIZATION_THRESHOLD_PERCENT)
  extends Logging {

  /**
   * Whether the credit window is unlimited. A non-positive [[linkCapacityBytes]] is interpreted
   * as "no cap" (mirroring the unlimited convention of `TokenBucketRateLimiter`), in which case
   * [[tryAcquire]] always succeeds and [[refill]] is a no-op.
   */
  private val unlimitedCredits: Boolean = linkCapacityBytes <= 0L

  /**
   * The maximum number of in-flight credit bytes the window will admit, i.e. 80% of the supplied
   * link capacity (at least 1 byte for any positive input). Reported as `Long.MaxValue` when the
   * window is unlimited.
   */
  val capacityBytes: Long =
    if (unlimitedCredits) {
      Long.MaxValue
    } else {
      val capped =
        (linkCapacityBytes.toDouble * TokenBucketRateLimiter.BANDWIDTH_CAP_FACTOR).toLong
      math.max(1L, capped)
    }

  /** Lock-free count of currently available credit bytes; initialized to the full capacity. */
  private val availableTokens = new AtomicLong(capacityBytes)

  /**
   * Per-stream, monotonically non-decreasing acknowledgment high-water marks, keyed by
   * [[StreamKey]] (shuffle, reduce partition, consumer attempt, consumer executor). Each
   * [[AtomicLong]] is advanced under its own CAS loop. See decision log ADR-11.
   */
  private val ackWatermarks = new ConcurrentHashMap[StreamKey, AtomicLong]()

  /**
   * Per-stream `System.nanoTime` of the most recent consumer signal (a keyed acknowledgment or an
   * explicit liveness stamp). Consulted by [[isConsumerTimedOut]] to detect a consumer that has
   * gone silent for longer than [[BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS]] (10 s).
   * This is the consumer-failure detector, distinct from the single 5 s flow-control heartbeat.
   */
  private val lastConsumerSignalNanos = new ConcurrentHashMap[StreamKey, java.lang.Long]()

  /** `System.nanoTime` of the most recently recorded heartbeat. */
  private val lastHeartbeatNanos = new AtomicLong(System.nanoTime())

  /** Edge-trigger latch: `true` while backpressure is currently active. */
  private val backpressureActive = new AtomicBoolean(false)

  /** Most recently observed buffer-utilization percentage, clamped into [0, 100]. */
  private val currentUtilization = new AtomicInteger(0)

  /** The effective backpressure threshold, clamped into the inclusive range [1, 100]. */
  private val effectiveThresholdPercent: Int =
    math.max(1, math.min(100, utilizationThresholdPercent))

  /**
   * The composed sustained-rate limiter (F-110). The 80% link-capacity cap and the
   * MB/s-to-bytes/s conversion are applied by the limiter's factory, so this class never
   * re-derives them.
   */
  private val rateLimiter: TokenBucketRateLimiter =
    TokenBucketRateLimiter.fromMaxBandwidthMBps(maxBandwidthMBps)

  logDebug(s"BackpressureProtocol created: capacityBytes=$capacityBytes " +
    s"(unlimited=$unlimitedCredits), thresholdPercent=$effectiveThresholdPercent, " +
    s"rateLimited=${rateLimiter.isLimited}")

  // ---------------------------------------------------------------------------------------------
  // Credit window (lock-free token bucket; one token == one byte)
  // ---------------------------------------------------------------------------------------------

  /**
   * Attempt to reserve `bytes` credits from the in-flight window without blocking.
   *
   * Always succeeds for a non-positive request or when the window is unlimited. Otherwise a
   * compare-and-set loop atomically decrements the available credits only if at least `bytes`
   * remain, so concurrent callers can never over-commit the window.
   *
   * @param bytes the number of credit bytes to reserve
   * @return `true` if the credits were reserved; `false` if insufficient credits are available
   */
  def tryAcquire(bytes: Long): Boolean = {
    if (bytes <= 0L || unlimitedCredits) {
      true
    } else {
      var result = false
      var continue = true
      while (continue) {
        val current = availableTokens.get()
        if (current < bytes) {
          // Insufficient credits: do not block, report failure so the caller can apply
          // backpressure or retry after a subsequent refill.
          continue = false
        } else if (availableTokens.compareAndSet(current, current - bytes)) {
          result = true
          continue = false
        }
        // Otherwise the CAS lost a race; loop and retry against the fresh value.
      }
      result
    }
  }

  /**
   * Return `tokens` credits to the in-flight window, saturating at [[capacityBytes]] so the
   * window never exceeds its configured capacity. A non-positive amount, or any call while the
   * window is unlimited, is a no-op. The addition is computed against the remaining headroom to
   * avoid overflow.
   *
   * @param tokens the number of credit bytes to return to the window
   */
  def refill(tokens: Long): Unit = {
    if (tokens > 0L && !unlimitedCredits) {
      var continue = true
      while (continue) {
        val current = availableTokens.get()
        val room = capacityBytes - current
        if (room <= 0L) {
          // Already at capacity; nothing to add.
          continue = false
        } else {
          val add = math.min(room, tokens)
          if (availableTokens.compareAndSet(current, current + add)) {
            continue = false
          }
          // Otherwise the CAS lost a race; loop and retry against the fresh value.
        }
      }
    }
  }

  /** The number of credit bytes currently available in the in-flight window. */
  def availableCredits: Long = availableTokens.get()

  // ---------------------------------------------------------------------------------------------
  // Sustained byte-rate cap (delegated to the composed TokenBucketRateLimiter)
  // ---------------------------------------------------------------------------------------------

  /** Whether a real sustained-rate cap is in effect (`false` means an unlimited pass-through). */
  def isRateLimited: Boolean = rateLimiter.isLimited

  /**
   * Non-blocking sustained-rate admission check for `bytes` (one permit per byte). Always `true`
   * when unlimited or when `bytes <= 0`.
   */
  def tryAcquireBandwidth(bytes: Int): Boolean = rateLimiter.tryAcquire(bytes)

  /**
   * Blocking sustained-rate acquisition for `bytes` (one permit per byte).
   *
   * @return the time spent sleeping, in seconds (`0.0` when unlimited or `bytes <= 0`)
   */
  def acquireBandwidth(bytes: Int): Double = rateLimiter.acquire(bytes)

  // ---------------------------------------------------------------------------------------------
  // Heartbeat (5 s flow-control liveness signal)
  // ---------------------------------------------------------------------------------------------

  /** Stamp the current time as the most recent heartbeat. */
  def recordHeartbeat(): Unit = {
    lastHeartbeatNanos.set(System.nanoTime())
  }

  /** Milliseconds elapsed since the most recently recorded heartbeat. */
  def millisSinceLastHeartbeat: Long =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastHeartbeatNanos.get())

  /**
   * Whether the heartbeat window has elapsed since the last recorded heartbeat, i.e. more than
   * [[BackpressureProtocol.HEARTBEAT_INTERVAL_MS]] milliseconds have passed.
   */
  def isHeartbeatExpired: Boolean =
    millisSinceLastHeartbeat > BackpressureProtocol.HEARTBEAT_INTERVAL_MS

  // ---------------------------------------------------------------------------------------------
  // Buffer-utilization threshold monitoring
  // ---------------------------------------------------------------------------------------------

  /**
   * Record the current buffer-utilization percentage and decide whether backpressure is active.
   *
   * The supplied value is clamped into [0, 100]. Backpressure activation is edge-triggered: a
   * single backpressure event is tallied through `metrics.incrementBackpressureEvents()` on
   * each transition from below the threshold to at/above it; while utilization stays high no
   * further events are counted. When utilization falls back below the threshold the latch is
   * released so a subsequent crossing is counted again.
   *
   * @param utilizationPercent the raw buffer-utilization percentage (clamped into [0, 100])
   * @return `true` if backpressure is currently active (utilization at/above the threshold)
   */
  def updateUtilization(utilizationPercent: Int): Boolean = {
    val clamped = math.max(0, math.min(100, utilizationPercent))
    currentUtilization.set(clamped)
    if (clamped >= effectiveThresholdPercent) {
      if (backpressureActive.compareAndSet(false, true)) {
        metrics.incrementBackpressureEvents()
        logDebug(log"Backpressure ACTIVATED at utilization=${MDC(PERCENT, clamped)}% " +
          log"(threshold=${MDC(THRESHOLD, effectiveThresholdPercent)}%)")
      }
      true
    } else {
      if (backpressureActive.compareAndSet(true, false)) {
        logDebug(log"Backpressure released at utilization=${MDC(PERCENT, clamped)}% " +
          log"(threshold=${MDC(THRESHOLD, effectiveThresholdPercent)}%)")
      }
      false
    }
  }

  /** The most recently observed buffer-utilization percentage, in [0, 100]. */
  def currentUtilizationPercent: Int = currentUtilization.get()

  /** Whether backpressure is currently active. */
  def isBackpressureActive: Boolean = backpressureActive.get()

  // ---------------------------------------------------------------------------------------------
  // Per-stream monotonic acknowledgment merge
  // ---------------------------------------------------------------------------------------------

  /**
   * Merge an acknowledgment for sequence number `seqNo` into the high-water mark of the stream
   * identified by `key`, and stamp that stream's consumer-liveness signal.
   *
   * Uses a compare-and-set loop to advance the stream's watermark only when `seqNo` is strictly
   * greater than its current value, so each per-stream watermark is monotonically non-decreasing
   * even under concurrent, out-of-order, or duplicate acknowledgments. Per-stream keying isolates
   * acknowledgment state (see decision log ADR-11).
   *
   * @param key   the identity of the stream being acknowledged
   * @param seqNo the acknowledged sequence number
   * @return the stream's acknowledgment high-water mark after the merge
   */
  def mergeAck(key: StreamKey, seqNo: Long): Long = {
    val watermark = ackWatermarks.computeIfAbsent(key, _ => new AtomicLong(0L))
    var continue = true
    while (continue) {
      val current = watermark.get()
      if (seqNo <= current) {
        // Out-of-order or duplicate ack: never regress this stream's watermark.
        continue = false
      } else if (watermark.compareAndSet(current, seqNo)) {
        continue = false
      }
      // Otherwise the CAS lost a race; loop and retry against the fresh value.
    }
    recordConsumerSignal(key)
    watermark.get()
  }

  /**
   * The current monotonically non-decreasing acknowledgment high-water mark for `key`, or `0` if
   * the stream has never been acknowledged.
   */
  def ackWatermark(key: StreamKey): Long = {
    val watermark = ackWatermarks.get(key)
    if (watermark == null) 0L else watermark.get()
  }

  /** The number of distinct streams for which acknowledgment state is currently tracked. */
  def trackedStreamCount: Int = ackWatermarks.size()

  // ---------------------------------------------------------------------------------------------
  // Consumer liveness (10 s missing-ack / consumer-failure detector, per stream)
  // ---------------------------------------------------------------------------------------------

  /**
   * Stamp the current time as the most recent consumer signal for `key`. A signal is any evidence
   * the consumer is alive: a keyed acknowledgment (see [[mergeAck]], which calls this) or an
   * explicit liveness update. This is distinct from [[recordHeartbeat]], which maintains the
   * single 5 s flow-control heartbeat rather than per-stream consumer liveness.
   */
  def recordConsumerSignal(key: StreamKey): Unit = {
    lastConsumerSignalNanos.put(key, java.lang.Long.valueOf(System.nanoTime()))
  }

  /**
   * Test/diagnostic seam: stamp `key`'s most recent consumer signal at an explicit `nanos`
   * timestamp (a `System.nanoTime`-domain value). Used to exercise the 10 s liveness boundary
   * deterministically and available to a future scheduler-driven liveness sweep.
   */
  private[streaming] def markConsumerSignalAt(key: StreamKey, nanos: Long): Unit = {
    lastConsumerSignalNanos.put(key, java.lang.Long.valueOf(nanos))
  }

  /**
   * Whether the consumer behind `key` has gone silent for longer than the 10 s consumer-liveness
   * window (see [[BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS]]). A stream that has never
   * signaled is treated as not-yet-started (returns `false`) so a consumer that has not begun
   * cannot trigger a spurious fallback or invalidation. The producer/fallback path consults this
   * to decide whether to invalidate a stalled stream or degrade to the sort path.
   */
  def isConsumerTimedOut(key: StreamKey): Boolean = {
    val last = lastConsumerSignalNanos.get(key)
    if (last == null) {
      false
    } else {
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - last.longValue()) >
        BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS
    }
  }

  /**
   * The streams whose consumer has exceeded the 10 s liveness window, for runtime fallback
   * evaluation. Computed against a single `now` snapshot so the result is internally consistent.
   */
  def timedOutStreams: Seq[StreamKey] = {
    val now = System.nanoTime()
    lastConsumerSignalNanos.entrySet().asScala.iterator.collect {
      case e if TimeUnit.NANOSECONDS.toMillis(now - e.getValue.longValue()) >
        BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS => e.getKey
    }.toSeq
  }

  // ---------------------------------------------------------------------------------------------
  // Priority arbitration
  // ---------------------------------------------------------------------------------------------

  /**
   * Select the highest-priority stream among `candidates`.
   *
   * v1 policy: the most-starved stream wins (the smallest
   * [[StreamPriority.remainingCapacityBytes]] is highest priority); ties are broken in favor of
   * the oldest stream (the greatest [[StreamPriority.ageNanos]]). The scan is a single
   * left-fold over the candidates and performs no allocation beyond the returned `Option`.
   *
   * @param candidates the competing streams to arbitrate among
   * @return the highest-priority candidate, or `None` if `candidates` is empty
   */
  def arbitrate(candidates: Seq[StreamPriority]): Option[StreamPriority] = {
    if (candidates.isEmpty) {
      None
    } else {
      Some(candidates.reduceLeft { (best, candidate) =>
        if (candidate.remainingCapacityBytes < best.remainingCapacityBytes) {
          candidate
        } else if (candidate.remainingCapacityBytes > best.remainingCapacityBytes) {
          best
        } else if (candidate.ageNanos > best.ageNanos) {
          candidate
        } else {
          best
        }
      })
    }
  }

  /**
   * A compact, human-readable snapshot of the protocol's current flow-control state, intended for
   * debug logging. Reads only public atomics and therefore never blocks.
   */
  def debugState: String =
    s"BackpressureProtocol[credits=$availableCredits/$capacityBytes, " +
      s"utilization=$currentUtilizationPercent%, backpressure=$isBackpressureActive, " +
      s"trackedStreams=$trackedStreamCount, msSinceHeartbeat=$millisSinceLastHeartbeat]"
}

/**
 * Constants and shared definitions for [[BackpressureProtocol]].
 */
private[spark] object BackpressureProtocol {

  /**
   * The flow-control heartbeat liveness interval: 5 seconds. A producer that has not observed a
   * consumer heartbeat within this window is treated as having a stalled peer (see
   * [[BackpressureProtocol.isHeartbeatExpired]]). This is an exact protocol constant defined by
   * the streaming-shuffle timing semantics.
   */
  val HEARTBEAT_INTERVAL_MS: Long = 5000L

  /**
   * The consumer-liveness window: 10 seconds. A consumer that has not signaled (acknowledged)
   * within this window is treated as failed by [[BackpressureProtocol.isConsumerTimedOut]], which
   * the runtime fallback/invalidation path consults. This is distinct from, and longer than, the
   * 5 s flow-control [[HEARTBEAT_INTERVAL_MS]], matching the streaming-shuffle timing semantics
   * (producer-connection 5 s, heartbeat 5 s, consumer liveness 10 s).
   */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /**
   * The default buffer-utilization percentage at which backpressure activates when no explicit
   * threshold is supplied to the constructor.
   */
  val DEFAULT_UTILIZATION_THRESHOLD_PERCENT: Int = 80
}

/**
 * A point-in-time snapshot of a competing stream's flow-control state, used as the input to
 * [[BackpressureProtocol.arbitrate]] for priority arbitration.
 *
 * @param partitionId            the reduce partition the stream feeds
 * @param remainingCapacityBytes the credit bytes still available to the stream; a smaller value
 *                               denotes a more-starved (higher-priority) stream
 * @param ageNanos               the age of the stream's oldest outstanding work, in nanoseconds;
 *                               used to break ties in favor of older streams
 */
private[spark] case class StreamPriority(
    partitionId: Int,
    remainingCapacityBytes: Long,
    ageNanos: Long)

/**
 * The identity of a single consumer flow-control stream, used to key per-stream acknowledgment
 * and consumer-liveness state in [[BackpressureProtocol]]. Two acknowledgments belong to the same
 * stream iff their [[StreamKey]]s are equal, so an ack for one stream can never affect another.
 * On the consumer side the tuple mirrors the producer's per-partition buffer identity, augmented
 * with the consumer attempt and executor so that distinct consumer attempts of the same partition
 * are tracked independently.
 *
 * @param shuffleId   the shuffle the stream belongs to
 * @param partitionId the reduce partition the consumer is reading
 * @param attemptId   the consumer task attempt id (uniquely identifies this consumer attempt)
 * @param executorId  the executor id of the consumer
 */
private[spark] case class StreamKey(
    shuffleId: Int,
    partitionId: Int,
    attemptId: Long,
    executorId: String)

/**
 * The protocol-level message algebraic data type exchanged between consumers and producers.
 *
 * These are pure, immutable, [[Serializable]] value objects: this file owns their definition so
 * the cross-executor `BackpressureRpcEndpoint` (F-108) can route them across executor
 * boundaries. They carry no behavior beyond their payload; all flow-control logic lives in
 * [[BackpressureProtocol]].
 */
private[spark] sealed trait BackpressureMessage extends Serializable

/**
 * A liveness heartbeat emitted on the 5 s flow-control interval. Carries the consumer's
 * cross-executor correlation identity (executor, shuffle, attempt, and the reduce-partition range
 * it consumes) so a producer receiving it can attribute the heartbeat to the correct stream(s).
 *
 * @param executorId           the identifier of the executor emitting the heartbeat
 * @param shuffleId            the shuffle the heartbeat pertains to
 * @param attemptId            the consumer task attempt id (cross-executor correlation identity)
 * @param reducePartitionRange the reduce-partition range the consumer reads, rendered as
 *                             `"[start,end)"` (the `reduce_partition_range` correlation field)
 * @param timestampMs          the wall-clock time the heartbeat was produced, in epoch millis
 */
private[spark] case class Heartbeat(
    executorId: String,
    shuffleId: Int,
    attemptId: Long,
    reducePartitionRange: String,
    timestampMs: Long)
  extends BackpressureMessage

/**
 * A sequence-numbered acknowledgment that lets the producer reclaim buffer space and advance its
 * acknowledgment watermark (see [[BackpressureProtocol.mergeAck]]). The
 * `(shuffleId, partitionId, attemptId, executorId)` tuple is the [[StreamKey]] the producer uses
 * to advance exactly the acknowledged stream's watermark, never an unrelated stream's.
 *
 * @param shuffleId      the shuffle being acknowledged
 * @param partitionId    the reduce partition being acknowledged
 * @param attemptId      the consumer task attempt id (part of the stream identity)
 * @param executorId     the consumer executor id (part of the stream identity)
 * @param seqNo          the acknowledged sequence number (monotonic high-water mark input)
 * @param reclaimedBytes the number of buffer bytes the consumer has consumed and the producer may
 *                       therefore return to its credit window
 */
private[spark] case class Ack(
    shuffleId: Int,
    partitionId: Int,
    attemptId: Long,
    executorId: String,
    seqNo: Long,
    reclaimedBytes: Long)
  extends BackpressureMessage

/**
 * A directive advising the producer of a new effective sustained byte-rate cap.
 *
 * @param shuffleId      the shuffle the update applies to
 * @param partitionId    the reduce partition the update applies to
 * @param attemptId      the consumer task attempt id (cross-executor correlation identity)
 * @param maxBytesPerSec the new effective rate cap, in bytes per second
 */
private[spark] case class RateUpdate(
    shuffleId: Int,
    partitionId: Int,
    attemptId: Long,
    maxBytesPerSec: Long)
  extends BackpressureMessage

/**
 * A timeout signal indicating a peer became unresponsive; the consumer uses it to invalidate a
 * partial read and the protocol surfaces it for fallback consideration. The free-text `reason` is
 * untrusted and is bounded and sanitized by the receiving endpoint before it is ever logged.
 *
 * @param shuffleId   the shuffle the timeout pertains to
 * @param partitionId the reduce partition the timeout pertains to
 * @param attemptId   the consumer task attempt id (cross-executor correlation identity)
 * @param reason      a short, human-readable description of what timed out (untrusted free text)
 */
private[spark] case class Timeout(
    shuffleId: Int,
    partitionId: Int,
    attemptId: Long,
    reason: String)
  extends BackpressureMessage
