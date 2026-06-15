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
import org.apache.spark.internal.LogKeys

/**
 * Lock-free decision object that evaluates the four "revert to sort-based shuffle" conditions
 * for the opt-in streaming shuffle backend.
 *
 * ==Role: decide, never act==
 *
 * This policy ONLY decides whether the streaming path should be abandoned in favor of the
 * sort-based path; it performs no delegation itself. `StreamingShuffleManager` consults
 * [[shouldFallback]] (and short-circuits when streaming is disabled) and, when a fallback is
 * indicated, routes the shuffle to its lazily-instantiated inner
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]]. Concentrating the decision here keeps
 * the manager's wiring trivial and makes every condition independently testable without a live
 * shuffle. This separation is the linchpin of the feature's zero-regression guarantee:
 * memory-bound or otherwise unsuitable workloads silently revert to the unchanged sort path.
 *
 * ==The four revert conditions==
 *
 * A fallback is indicated when ANY of the following holds (thresholds are sourced from
 * [[StreamingShuffleConfig]] so there is a single source of truth and no duplicated magic
 * numbers):
 *
 *  1. Slow consumer: the consumer has been sustained more than
 *     [[StreamingShuffleConfig.SLOW_CONSUMER_RATIO]]x (2x) slower than the producer
 *     continuously for more than [[StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS]]
 *     (60 s). The sustained-duration requirement avoids flapping on transient dips: the window
 *     timer starts when the condition first becomes true and resets the moment the consumer
 *     recovers.
 *  2. Memory pressure / OOM risk: executor memory utilization exceeds
 *     [[StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT]] (95%), at which point buffer
 *     allocation can no longer be guaranteed.
 *  3. Network saturation: link utilization exceeds
 *     [[StreamingShuffleConfig.NETWORK_SATURATION_PERCENT]] (90%) of capacity.
 *  4. Version mismatch: the producer and consumer negotiate incompatible streaming-protocol
 *     versions.
 *
 * ==Lock-free and hot-path safe==
 *
 * All mutable state is held in `java.util.concurrent.atomic` primitives, so update hooks and
 * predicates are lock-free, allocation-free, and cheap enough to call on the shuffle hot path.
 * Update hooks ([[recordThroughput]], [[updateMemoryUtilization]], [[updateNetworkUtilization]],
 * [[markVersionMismatch]]) are invoked by the writer, backpressure protocol, and spill manager;
 * the predicates are pure reads. Every method is safe to call concurrently from any number of
 * producer, consumer, backpressure, and spill threads.
 *
 * ==Observability==
 *
 * To respect the telemetry budget (at most a few KB of logs per executor per hour), state is
 * counted and logged at most once per fallback state transition rather than on every
 * evaluation: the first evaluation that flips the policy from "streaming" to "fallback"
 * increments the supplied [[StreamingShuffleMetrics]] indicator once and emits a single
 * structured warning carrying the human-readable [[fallbackReason]]; the inverse transition is
 * logged at debug level only. Because [[StreamingShuffleMetrics]] exposes no dedicated fallback
 * counter, the backpressure-event counter is reused as the flow-control fallback indicator.
 *
 * @param conf    the typed streaming-shuffle configuration accessor (its `debug` flag gates
 *                verbose recovery logging; the numeric thresholds come from the
 *                [[StreamingShuffleConfig]] companion constants)
 * @param metrics optional metrics holder; when non-null, a fallback transition increments the
 *                backpressure/flow-control indicator exactly once
 */
private[spark] class StreamingShuffleFallbackPolicy(
    conf: StreamingShuffleConfig,
    metrics: StreamingShuffleMetrics = null) extends Logging {

  import StreamingShuffleFallbackPolicy._

  // Most recent producer/consumer throughput samples in bytes per second. Retained so that
  // diagnostics and tests can observe the values that drove the slow-consumer decision.
  private val producerBytesPerSecond = new AtomicLong(0L)
  private val consumerBytesPerSecond = new AtomicLong(0L)

  // Monotonic-clock timestamp (nanoTime) marking when the slow-consumer condition first became
  // true, or NOT_SLOW when the consumer is keeping up. The sustained-duration check in
  // isSlowConsumer compares the elapsed time against the configured window.
  private val slowSince = new AtomicLong(NOT_SLOW)

  // Sampled executor memory and network-link utilization percentages, each clamped to [0, 100].
  private val memoryUtilizationPercent = new AtomicInteger(0)
  private val networkUtilizationPercent = new AtomicInteger(0)

  // Whether a producer/consumer streaming-protocol version mismatch has been observed. A
  // mismatch is sticky for the lifetime of the policy (cleared only by reset()), since an
  // incompatible peer does not become compatible mid-shuffle.
  private val versionMismatch = new AtomicBoolean(false)

  // Tracks the current fallback state so the metric increment and the structured log fire
  // exactly once per false -> true transition (and the recovery log once per true -> false).
  private val fallbackActive = new AtomicBoolean(false)

  // The sustained slow-consumer window, precomputed once in nanoseconds to keep isSlowConsumer
  // free of per-call unit conversions on the hot path.
  private val slowConsumerWindowNanos =
    TimeUnit.SECONDS.toNanos(StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS)

  // ---------------------------------------------------------------------------------------
  // Update hooks (called by the writer, backpressure protocol, and spill manager).
  // ---------------------------------------------------------------------------------------

  /**
   * Records the latest producer/consumer throughput sample and maintains the slow-consumer
   * window timer. Negative inputs are normalized to zero. When the consumer is more than the
   * configured ratio slower than the producer, the window start timestamp is set the first time
   * the condition is observed; when the consumer recovers, the timer is cleared so the
   * sustained-duration requirement restarts from scratch on the next dip.
   *
   * @param producerBytesPerSec observed producer throughput in bytes per second
   * @param consumerBytesPerSec observed consumer throughput in bytes per second
   */
  def recordThroughput(producerBytesPerSec: Long, consumerBytesPerSec: Long): Unit = {
    val producer = math.max(0L, producerBytesPerSec)
    val consumer = math.max(0L, consumerBytesPerSec)
    producerBytesPerSecond.set(producer)
    consumerBytesPerSecond.set(consumer)
    if (isCurrentlySlow(producer, consumer)) {
      // Mark the start of the slow window only on the first observation, so the >60s timer
      // measures continuous slowness rather than restarting on every sample.
      if (slowSince.get() == NOT_SLOW) {
        val now = System.nanoTime()
        // Avoid colliding with the NOT_SLOW sentinel on the astronomically rare nanoTime() == 0.
        val stamp = if (now == NOT_SLOW) 1L else now
        slowSince.compareAndSet(NOT_SLOW, stamp)
      }
    } else {
      // Consumer recovered: clear the window so a future dip must again persist for >60s.
      slowSince.set(NOT_SLOW)
    }
  }

  /**
   * Publishes the latest executor memory-utilization percentage, defensively clamped to
   * [0, 100]. A value above [[StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT]] trips the
   * memory-pressure fallback condition.
   *
   * @param percent sampled memory utilization in percent
   */
  def updateMemoryUtilization(percent: Int): Unit = {
    memoryUtilizationPercent.set(clampPercent(percent))
  }

  /**
   * Publishes the latest network-link utilization percentage, defensively clamped to [0, 100].
   * A value above [[StreamingShuffleConfig.NETWORK_SATURATION_PERCENT]] trips the
   * network-saturation fallback condition.
   *
   * @param percent sampled network-link utilization in percent
   */
  def updateNetworkUtilization(percent: Int): Unit = {
    networkUtilizationPercent.set(clampPercent(percent))
  }

  /**
   * Marks that a producer/consumer streaming-protocol version mismatch has been detected. The
   * flag is sticky until [[reset]] is called, immediately tripping the version-mismatch
   * fallback condition.
   */
  def markVersionMismatch(): Unit = {
    versionMismatch.set(true)
  }

  // ---------------------------------------------------------------------------------------
  // Predicates (pure reads of the atomic state).
  // ---------------------------------------------------------------------------------------

  /**
   * @param nowNanos the current monotonic-clock reading; defaults to `System.nanoTime()`. It is
   *   injectable so tests can exercise the sustained-duration logic deterministically.
   * @return `true` only when the consumer has been more than the configured ratio slower than
   *         the producer continuously for longer than the configured window
   */
  def isSlowConsumer(nowNanos: Long = System.nanoTime()): Boolean = {
    val since = slowSince.get()
    since != NOT_SLOW && (nowNanos - since) > slowConsumerWindowNanos
  }

  /** @return `true` when memory utilization exceeds the configured OOM-risk threshold. */
  def isMemoryPressure: Boolean =
    memoryUtilizationPercent.get() > StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT

  /** @return `true` when network-link utilization exceeds the configured saturation threshold. */
  def isNetworkSaturated: Boolean =
    networkUtilizationPercent.get() > StreamingShuffleConfig.NETWORK_SATURATION_PERCENT

  /** @return `true` when a producer/consumer protocol version mismatch has been marked. */
  def isVersionMismatch: Boolean = versionMismatch.get()

  // ---------------------------------------------------------------------------------------
  // Composite decision.
  // ---------------------------------------------------------------------------------------

  /**
   * Evaluates all four revert conditions against the current clock. This is the method
   * `StreamingShuffleManager` calls to decide whether to route a shuffle to the sort-based
   * fallback.
   *
   * The first evaluation that transitions the policy from "streaming" to "fallback" increments
   * the backpressure/flow-control indicator on the supplied metrics holder exactly once and
   * emits a single structured warning; subsequent evaluations while the condition persists are
   * side-effect free, honoring the per-executor log budget.
   *
   * @return `true` if any revert condition is currently satisfied
   */
  def shouldFallback: Boolean = shouldFallbackAt(System.nanoTime())

  /**
   * Time-injectable variant of [[shouldFallback]] used to drive the slow-consumer condition
   * deterministically from tests. Production code uses the no-argument [[shouldFallback]], which
   * supplies `System.nanoTime()`.
   *
   * @param nowNanos the monotonic-clock reading used for the sustained slow-consumer check
   * @return `true` if any revert condition is currently satisfied
   */
  def shouldFallbackAt(nowNanos: Long): Boolean = {
    val fallback =
      isSlowConsumer(nowNanos) || isMemoryPressure || isNetworkSaturated || isVersionMismatch
    if (fallback) {
      // Count and log the transition exactly once (false -> true). compareAndSet guarantees a
      // single winner even under concurrent evaluation, preventing double counting.
      if (fallbackActive.compareAndSet(false, true)) {
        if (metrics != null) {
          metrics.incBackpressureEvents()
        }
        logFallbackTransition(nowNanos)
      }
    } else if (fallbackActive.compareAndSet(true, false)) {
      logFallbackRecovery()
    }
    fallback
  }

  /**
   * @return a human-readable description of the highest-priority active revert condition for the
   *         decision log and structured logging, or [[scala.None]] when no condition is
   *         currently satisfied. Conditions are reported in the same order they are evaluated.
   */
  def fallbackReason: Option[String] = reasonAt(System.nanoTime())

  /**
   * Resets all tracked state back to its initial values. Intended for test isolation and for
   * reuse of a single policy instance across stress iterations; not used on any production code
   * path. Each field is cleared with an independent lock-free write.
   */
  def reset(): Unit = {
    producerBytesPerSecond.set(0L)
    consumerBytesPerSecond.set(0L)
    slowSince.set(NOT_SLOW)
    memoryUtilizationPercent.set(0)
    networkUtilizationPercent.set(0)
    versionMismatch.set(false)
    fallbackActive.set(false)
  }

  // ---------------------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------------------

  /**
   * Determines whether the current throughput sample qualifies as "consumer too slow": no
   * production means nothing can outpace the consumer; production with a stalled consumer is
   * treated as infinitely slow; otherwise the producer must exceed the consumer by more than the
   * configured ratio.
   */
  private def isCurrentlySlow(producer: Long, consumer: Long): Boolean = {
    if (producer <= 0L) {
      false
    } else if (consumer <= 0L) {
      true
    } else {
      producer.toDouble > consumer.toDouble * StreamingShuffleConfig.SLOW_CONSUMER_RATIO
    }
  }

  /** Builds the reason for the highest-priority active condition at the given clock reading. */
  private def reasonAt(nowNanos: Long): Option[String] = {
    if (isSlowConsumer(nowNanos)) {
      Some(slowConsumerReason)
    } else if (isMemoryPressure) {
      Some(memoryPressureReason)
    } else if (isNetworkSaturated) {
      Some(networkSaturationReason)
    } else if (isVersionMismatch) {
      Some(VERSION_MISMATCH_REASON)
    } else {
      None
    }
  }

  private def slowConsumerReason: String =
    s"slow consumer sustained >${StreamingShuffleConfig.SLOW_CONSUMER_RATIO}x slower than " +
      s"producer for >${StreamingShuffleConfig.SLOW_CONSUMER_THRESHOLD_SECONDS}s"

  private def memoryPressureReason: String =
    s"memory utilization ${memoryUtilizationPercent.get()}% exceeds " +
      s"${StreamingShuffleConfig.MEMORY_PRESSURE_PERCENT}% OOM-risk threshold"

  private def networkSaturationReason: String =
    s"network utilization ${networkUtilizationPercent.get()}% exceeds " +
      s"${StreamingShuffleConfig.NETWORK_SATURATION_PERCENT}% saturation threshold"

  /** Emits a single structured warning on the streaming -> fallback transition. */
  private def logFallbackTransition(nowNanos: Long): Unit = {
    val reason = reasonAt(nowNanos).getOrElse(UNKNOWN_REASON)
    logWarning(log"Streaming shuffle reverting to sort-based fallback: " +
      log"${MDC(LogKeys.REASON, reason)}")
  }

  /** Emits a debug-level note on the fallback -> streaming recovery transition. */
  private def logFallbackRecovery(): Unit = {
    if (conf.debug) {
      logInfo(log"Streaming shuffle fallback cleared; streaming path eligible again")
    }
  }
}

/**
 * Constants and pure helpers backing [[StreamingShuffleFallbackPolicy]]. The numeric fallback
 * thresholds intentionally live in [[StreamingShuffleConfig]] (the single source of truth);
 * only values local to the decision logic are defined here.
 */
private[spark] object StreamingShuffleFallbackPolicy {

  /** Sentinel stored in `slowSince` when the consumer is keeping up (no active slow window). */
  private val NOT_SLOW: Long = 0L

  /** Fixed reason text for the version-mismatch condition (carries no sampled values). */
  private val VERSION_MISMATCH_REASON: String =
    "producer/consumer streaming-protocol version mismatch"

  /** Defensive fallback reason used only if a transition is observed with no active condition. */
  private val UNKNOWN_REASON: String = "unspecified fallback condition"

  /** Clamps a utilization percentage into the inclusive range [0, 100]. */
  private def clampPercent(percent: Int): Int = math.max(0, math.min(100, percent))
}
