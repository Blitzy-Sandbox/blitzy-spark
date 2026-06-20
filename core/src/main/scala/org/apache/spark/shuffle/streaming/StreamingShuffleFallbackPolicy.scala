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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import org.apache.spark.internal.Logging

/**
 * Decides whether the streaming shuffle backend must revert ("fall back") to the sort-based
 * shuffle on a given executor. This object embodies the feature's zero-regression guarantee: the
 * streaming path is engaged only while none of the four revert conditions holds; the moment any
 * condition trips, `StreamingShuffleManager` delegates to its lazily-instantiated inner
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]]. Decision and delegation are deliberately
 * separated -- this type only DECIDES; it never performs the delegation. That keeps it
 * side-effect-light and lets each condition be driven independently from tests.
 *
 * ==The four revert conditions==
 *
 * [[shouldFallback]] signals a fallback when ANY of the following holds. Every threshold is sourced
 * from the [[StreamingShuffleConfig]] companion constants and is never hard-coded here:
 *
 *  1. Slow consumer -- the producer has sustained more than
 *     [[StreamingShuffleConfig.SLOW_CONSUMER_RATIO]]x the consumer's throughput for longer than
 *     [[StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS]] seconds. A transient spike does
 *     not trip the policy; the imbalance must persist.
 *  2. Memory pressure -- buffer-allocation memory utilization exceeds
 *     [[StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT]]% (an OOM risk).
 *  3. Network saturation -- link utilization exceeds
 *     [[StreamingShuffleConfig.NETWORK_SATURATION_PERCENT]]%.
 *  4. Version mismatch -- a producer/consumer streaming-protocol version mismatch was reported.
 *
 * ==Concurrency==
 *
 * Every field is a lock-free `java.util.concurrent.atomic` primitive, so the update hooks (called
 * by the writer, the backpressure protocol, and the spill manager) and the predicates (evaluated on
 * the hot path) are all O(1) and contention-free. [[shouldFallback]] additionally detects the
 * false-to-true and true-to-false edges with a single compare-and-swap, so the fallback log line
 * is emitted at most once per state transition, never once per evaluation -- this honors the
 * < 10 MB/hour/executor log budget.
 *
 * @param conf      the typed streaming configuration; consulted for debug-log gating
 * @param nanoClock the monotonic clock source (in `System.nanoTime()` units) used to stamp and
 *                  measure the slow-consumer window. It defaults to `System.nanoTime` so production
 *                  callers need not supply it; it is a constructor seam purely so the manager-owned
 *                  policy can be driven to the sustained-slowness boundary deterministically in
 *                  tests (via [[recordThroughput]] and [[shouldFallback]]) without real waiting.
 */
private[spark] class StreamingShuffleFallbackPolicy(
    conf: StreamingShuffleConfig,
    nanoClock: () => Long = () => System.nanoTime()) extends Logging {

  import StreamingShuffleFallbackPolicy._

  // Most recent producer/consumer throughput samples, in bytes/sec, retained for diagnostics.
  private val lastProducerBytesPerSec = new AtomicLong(0L)
  private val lastConsumerBytesPerSec = new AtomicLong(0L)

  // Monotonic-clock timestamp (System.nanoTime) at which the slow-consumer imbalance most recently
  // became true, or NOT_SLOW while the consumer is keeping up. The sustained-slowness window is
  // measured as (now - slowSinceNanos); resetting to NOT_SLOW restarts the timer.
  private val slowSinceNanos = new AtomicLong(NOT_SLOW)

  // Latest sampled executor memory and network-link utilization, as integer percentages [0, 100].
  private val memoryUtilizationPercent = new AtomicInteger(0)
  private val networkUtilizationPercent = new AtomicInteger(0)

  // Set once a producer/consumer streaming-protocol version mismatch is detected; never cleared
  // for the policy's lifetime, since a mismatch cannot self-heal without an executor restart.
  private val versionMismatchFlag = new AtomicBoolean(false)

  // Tracks whether the policy is currently signalling fallback, enabling once-per-transition
  // counting and logging via compare-and-swap.
  private val fallbackActive = new AtomicBoolean(false)

  // -- Update hooks -------------------------------------------------------------------------------

  /**
   * Records the latest producer and consumer throughput samples and maintains the slow-consumer
   * window. When the producer sustains more than the configured slow-consumer ratio of the
   * consumer's throughput the window is opened (its start timestamp is preserved across subsequent
   * slow samples); when the consumer recovers the window is closed, so the sustained-slowness
   * timer restarts on the next imbalance.
   *
   * @param producerBytesPerSec producer-side throughput sample, in bytes per second
   * @param consumerBytesPerSec consumer-side throughput sample, in bytes per second
   */
  def recordThroughput(producerBytesPerSec: Long, consumerBytesPerSec: Long): Unit = {
    lastProducerBytesPerSec.set(producerBytesPerSec)
    lastConsumerBytesPerSec.set(consumerBytesPerSec)
    if (isThroughputTooSlow(producerBytesPerSec, consumerBytesPerSec)) {
      // Open the window on the first slow sample; a failed CAS means it is already open, which is
      // exactly what we want -- the earliest timestamp must win so we measure continuous slowness.
      slowSinceNanos.compareAndSet(NOT_SLOW, nanoClock())
    } else {
      // Consumer caught up: close the window so a future imbalance starts its timer afresh.
      slowSinceNanos.set(NOT_SLOW)
    }
  }

  /**
   * Updates the latest executor memory-utilization sample.
   *
   * @param percent memory utilization as an integer percentage in `[0, 100]`
   */
  def updateMemoryUtilization(percent: Int): Unit = memoryUtilizationPercent.set(percent)

  /**
   * Updates the latest network-link-utilization sample.
   *
   * @param percent link utilization as an integer percentage in `[0, 100]`
   */
  def updateNetworkUtilization(percent: Int): Unit = networkUtilizationPercent.set(percent)

  /**
   * Records that a producer/consumer streaming-protocol version mismatch was detected. Once set the
   * flag stays set: the streaming path remains disabled until the executor is restarted.
   */
  def markVersionMismatch(): Unit = versionMismatchFlag.set(true)

  // -- Predicates (pure reads of the atomic state) ------------------------------------------------

  /**
   * True only when the slow-consumer imbalance has held continuously for longer than
   * [[StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS]] seconds. A momentary imbalance never
   * trips this predicate because the window must have been opened by [[recordThroughput]] and have
   * remained open for the full threshold.
   *
   * @param nowNanos the current monotonic-clock reading; defaults to the policy's `nanoClock` (i.e.
   *                 `System.nanoTime()` in production) and is also an explicit parameter so tests
   *                 can drive the sustained-slowness boundary precisely. Because [[shouldFallback]]
   *                 calls this with the default, supplying a test `nanoClock` at construction time
   *                 makes the manager-owned policy's slow-consumer decision deterministic too.
   * @return `true` if the consumer has been sustained-slow past the threshold, otherwise `false`
   */
  def isSlowConsumer(nowNanos: Long = nanoClock()): Boolean = {
    val since = slowSinceNanos.get()
    since != NOT_SLOW && (nowNanos - since) > SLOW_CONSUMER_THRESHOLD_NANOS
  }

  /**
   * True when the latest memory-utilization sample exceeds
   * [[StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT]]%, signalling an OOM risk for buffer
   * allocation.
   */
  def isMemoryPressure: Boolean =
    memoryUtilizationPercent.get() > StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT

  /**
   * True when the latest network-utilization sample exceeds
   * [[StreamingShuffleConfig.NETWORK_SATURATION_PERCENT]]%.
   */
  def isNetworkSaturated: Boolean =
    networkUtilizationPercent.get() > StreamingShuffleConfig.NETWORK_SATURATION_PERCENT

  /** True once a producer/consumer streaming-protocol version mismatch has been reported. */
  def isVersionMismatch: Boolean = versionMismatchFlag.get()

  // -- Aggregate decision -------------------------------------------------------------------------

  /**
   * The fallback decision: `true` if ANY of the four revert conditions currently holds. The four
   * predicates are combined with short-circuit OR so the cheapest checks run first and a tripped
   * condition skips the rest.
   *
   * As a side effect bounded to state transitions, the first evaluation that observes a
   * false-to-true edge logs the reason once; the first evaluation that observes the true-to-false
   * edge logs recovery (only when streaming debug logging is enabled, to conserve the log budget).
   * The edge detection uses a single compare-and-swap, so concurrent callers never log twice.
   *
   * @return `true` when the streaming path must fall back to sort-based shuffle
   */
  def shouldFallback: Boolean = {
    val fallback =
      isSlowConsumer() || isMemoryPressure || isNetworkSaturated || isVersionMismatch
    if (fallback) {
      if (fallbackActive.compareAndSet(false, true)) {
        logInfo("Streaming shuffle is falling back to sort-based shuffle; reason: " +
          fallbackReason.getOrElse("unknown"))
      }
    } else if (fallbackActive.compareAndSet(true, false)) {
      // Recovery is informational only; gate it on debug to respect the per-executor log budget.
      if (conf.debug) {
        logInfo("Streaming shuffle fallback conditions cleared; streaming path re-enabled")
      }
    }
    fallback
  }

  /**
   * A human-readable explanation of the current fallback decision, suitable for the decision log
   * and structured logging. Conditions are checked in the same order as [[shouldFallback]] and the
   * first match is returned.
   *
   * @return `Some(reason)` when a revert condition holds, or `None` when streaming may proceed
   */
  def fallbackReason: Option[String] = {
    if (isSlowConsumer()) {
      Some("slow consumer sustained > " +
        s"${StreamingShuffleConfig.SLOW_CONSUMER_RATIO}x slower than producer for > " +
        s"${StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS}s")
    } else if (isMemoryPressure) {
      Some(s"memory pressure: utilization ${memoryUtilizationPercent.get()}% > " +
        s"${StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT}% (OOM risk)")
    } else if (isNetworkSaturated) {
      Some(s"network saturation: utilization ${networkUtilizationPercent.get()}% > " +
        s"${StreamingShuffleConfig.NETWORK_SATURATION_PERCENT}%")
    } else if (isVersionMismatch) {
      Some("producer/consumer streaming-protocol version mismatch")
    } else {
      None
    }
  }

  override def toString: String =
    s"StreamingShuffleFallbackPolicy(producerBytesPerSec=${lastProducerBytesPerSec.get()}, " +
      s"consumerBytesPerSec=${lastConsumerBytesPerSec.get()}, " +
      s"memoryUtil=${memoryUtilizationPercent.get()}%, " +
      s"networkUtil=${networkUtilizationPercent.get()}%, " +
      s"versionMismatch=${versionMismatchFlag.get()}, fallbackActive=${fallbackActive.get()})"

  /**
   * Resets all mutable state to its initial values. Intended solely for test isolation so a single
   * policy instance can exercise each condition independently; it is not used on production paths.
   */
  private[streaming] def reset(): Unit = {
    lastProducerBytesPerSec.set(0L)
    lastConsumerBytesPerSec.set(0L)
    slowSinceNanos.set(NOT_SLOW)
    memoryUtilizationPercent.set(0)
    networkUtilizationPercent.set(0)
    versionMismatchFlag.set(false)
    fallbackActive.set(false)
  }

  // -- Private helpers ----------------------------------------------------------------------------

  /**
   * Returns `true` when the producer is sustaining more than
   * [[StreamingShuffleConfig.SLOW_CONSUMER_RATIO]]x the consumer's throughput. The `producer > 0`
   * guard means an idle shuffle (nothing produced) is never deemed slow, while a fully stalled
   * consumer (`consumer == 0`, `producer > 0`) is correctly flagged.
   */
  private def isThroughputTooSlow(producer: Long, consumer: Long): Boolean = {
    producer > 0L &&
      producer.toDouble > StreamingShuffleConfig.SLOW_CONSUMER_RATIO * consumer.toDouble
  }
}

/**
 * Holds the policy's derived constants so the package contains no duplicated literals: the
 * sentinel marking an inactive slow-consumer window, and the sustained-slowness threshold
 * expressed in nanoseconds (derived from the [[StreamingShuffleConfig]] constant).
 */
private[spark] object StreamingShuffleFallbackPolicy {

  // Sentinel stored in `slowSinceNanos` when the slow-consumer window is closed. Long.MinValue
  // cannot collide with a real System.nanoTime() reading, and the predicate guards against it
  // before computing any elapsed-time difference, so no overflow is possible.
  private val NOT_SLOW: Long = Long.MinValue

  // The sustained-slowness window (SLOW_CONSUMER_THRESHOLD_SECONDS) expressed in nanoseconds to
  // match System.nanoTime() arithmetic; computed once from the canonical config constant.
  private val SLOW_CONSUMER_THRESHOLD_NANOS: Long =
    TimeUnit.SECONDS.toNanos(StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS)
}
