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

import org.apache.spark.internal.Logging

/**
 * Encapsulates the four degradation conditions that trigger automatic reversion ("fallback")
 * from the streaming shuffle data path back to the built-in sort-based shuffle path.
 *
 * This policy is the safety envelope that guarantees zero regression: whenever any of the four
 * conditions holds, the streaming shuffle manager delegates the affected shuffle to its inner
 * `org.apache.spark.shuffle.sort.SortShuffleManager` instead of streaming it. The four conditions
 * are, in priority order:
 *
 *  1. The consumer (reduce) side has been sustained at least
 *     [[StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_FACTOR]]x slower than the producer (map)
 *     side for longer than [[StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_DURATION_MS]] ms.
 *  2. Memory pressure prevents a new buffer allocation (allocating would risk an
 *     `OutOfMemoryError`).
 *  3. Network utilization exceeds
 *     [[StreamingShuffleFallbackPolicy.NETWORK_SATURATION_THRESHOLD]] of link capacity.
 *  4. The producer and consumer report mismatched versions.
 *
 * '''Decision-only contract.''' This class strictly computes and reports decisions; it never
 * switches managers, mutates shuffle state, or performs I/O. Acting on a returned decision (for
 * example, routing the shuffle through the sort-based manager) is the responsibility of the
 * streaming shuffle manager. The individual `shouldFallbackFor*` predicates are pure functions of
 * their arguments and therefore deterministic and trivially unit-testable; the only observable
 * side effect in this class is a single WARN log emitted by [[evaluate]] / [[shouldFallback]]
 * when a fallback is decided.
 *
 * '''Input sourcing.''' In v1 the runtime signals required to evaluate each condition
 * (instantaneous producer/consumer throughput, sustained-slowness duration, buffer-allocation
 * feasibility, network utilization, and peer versions) are supplied as method arguments by the
 * writer, reader, and backpressure layers; wiring those signals is the manager's job. The
 * collaborating [[StreamingShuffleConfig]] (tuning thresholds such as the effective bandwidth)
 * and [[StreamingShuffleMetrics]] (live telemetry) are held for that purpose and for diagnostic
 * logging.
 *
 * '''Thread-safety.''' Instances are stateless apart from the immutable references to the
 * supplied configuration and metrics, so a single instance may be shared and queried
 * concurrently from multiple executor threads without external synchronization.
 *
 * @param config  the streaming shuffle configuration supplying tuning thresholds
 * @param metrics the streaming shuffle metrics, observed for diagnostic context when a fallback
 *                is logged
 */
private[spark] class StreamingShuffleFallbackPolicy(
    config: StreamingShuffleConfig,
    metrics: StreamingShuffleMetrics)
  extends Logging {

  import StreamingShuffleFallbackPolicy._

  logDebug(s"StreamingShuffleFallbackPolicy initialized with config=$config")

  /**
   * Condition 1: the consumer is sustained too slow relative to the producer.
   *
   * Returns `true` when the producer is actively making progress (`producerRate` &gt; 0) and the
   * consumer's throughput is at least
   * [[StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_FACTOR]]x slower
   * (`consumerRate * CONSUMER_SLOWNESS_FACTOR <= producerRate`), and this condition has held for
   * longer than [[StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_DURATION_MS]] milliseconds.
   * A non-positive `producerRate` means there is no production to outpace, so the condition is
   * not met regardless of the consumer rate.
   *
   * @param producerRate the producer-side throughput (for example, bytes/second); must be
   *                     positive for the condition to apply
   * @param consumerRate the consumer-side throughput in the same unit as `producerRate`
   * @param sustainedMs  how long, in milliseconds, the slowness has been continuously observed
   * @return `true` if the streaming path should fall back due to a sustained slow consumer
   */
  def shouldFallbackForConsumerLag(
      producerRate: Double,
      consumerRate: Double,
      sustainedMs: Long): Boolean = {
    val sustainedLongEnough = sustainedMs > CONSUMER_SLOWNESS_DURATION_MS
    val consumerTooSlow =
      producerRate > 0.0 && consumerRate * CONSUMER_SLOWNESS_FACTOR <= producerRate
    sustainedLongEnough && consumerTooSlow
  }

  /**
   * Condition 2: memory pressure prevents buffer allocation.
   *
   * Returns `true` when a required streaming buffer cannot be allocated without risking an
   * `OutOfMemoryError`, i.e. when the caller reports that allocation is not currently feasible.
   *
   * @param canAllocate whether the next buffer allocation can proceed safely
   * @return `true` if the streaming path should fall back due to memory pressure
   */
  def shouldFallbackForMemoryPressure(canAllocate: Boolean): Boolean = !canAllocate

  /**
   * Condition 3: the network link is saturated.
   *
   * Returns `true` when the observed link utilization strictly exceeds
   * [[StreamingShuffleFallbackPolicy.NETWORK_SATURATION_THRESHOLD]] (90% of capacity). At or
   * below the threshold the condition is not met.
   *
   * @param utilizationFraction the observed link utilization as a fraction in `[0.0, 1.0]`
   * @return `true` if the streaming path should fall back due to network saturation
   */
  def shouldFallbackForNetworkSaturation(utilizationFraction: Double): Boolean =
    utilizationFraction > NETWORK_SATURATION_THRESHOLD

  /**
   * Condition 4: the producer and consumer report mismatched versions.
   *
   * Returns `true` when the two version identifiers are not equal. The comparison is null-safe:
   * two `null` versions are treated as equal (no mismatch), while a `null` paired with a
   * non-`null` value is treated as a mismatch.
   *
   * @param producerVersion the version reported by the producing executor
   * @param consumerVersion the version reported by the consuming executor
   * @return `true` if the streaming path should fall back due to a version mismatch
   */
  def shouldFallbackForVersionMismatch(
      producerVersion: String,
      consumerVersion: String): Boolean = producerVersion != consumerVersion

  /**
   * Evaluates all four fallback conditions in priority order and returns the first one that is
   * triggered, or `None` when streaming may continue.
   *
   * The conditions are checked in the order they are enumerated by this policy (consumer lag,
   * then memory pressure, then network saturation, then version mismatch) so that the returned
   * reason is deterministic when more than one condition holds simultaneously. When a reason is
   * returned, a single WARN log is emitted recording the reason and the current buffer
   * utilization for operator diagnostics.
   *
   * @param producerRate producer-side throughput, as in [[shouldFallbackForConsumerLag]]
   * @param consumerRate consumer-side throughput, as in [[shouldFallbackForConsumerLag]]
   * @param sustainedMs duration the consumer slowness has been observed, in milliseconds
   * @param canAllocate whether the next buffer allocation can proceed safely
   * @param networkUtilizationFraction observed link utilization as a fraction in `[0.0, 1.0]`
   * @param producerVersion the version reported by the producing executor
   * @param consumerVersion the version reported by the consuming executor
   * @return `Some(reason)` for the highest-priority triggered condition, or `None`
   */
  def evaluate(
      producerRate: Double,
      consumerRate: Double,
      sustainedMs: Long,
      canAllocate: Boolean,
      networkUtilizationFraction: Double,
      producerVersion: String,
      consumerVersion: String): Option[FallbackReason] = {
    val reason: Option[FallbackReason] =
      if (shouldFallbackForConsumerLag(producerRate, consumerRate, sustainedMs)) {
        Some(ConsumerTooSlow)
      } else if (shouldFallbackForMemoryPressure(canAllocate)) {
        Some(MemoryPressure)
      } else if (shouldFallbackForNetworkSaturation(networkUtilizationFraction)) {
        Some(NetworkSaturation)
      } else if (shouldFallbackForVersionMismatch(producerVersion, consumerVersion)) {
        Some(VersionMismatch)
      } else {
        None
      }
    reason.foreach { r =>
      logWarning(s"Streaming shuffle reverting to sort-based shuffle (reason=${r.message}, " +
        s"bufferUtilizationPercent=${metrics.getBufferUtilizationPercent})")
    }
    reason
  }

  /**
   * Convenience boolean form of [[evaluate]]: returns `true` when any fallback condition is
   * triggered. Like [[evaluate]], this emits a single WARN log on a triggered fallback.
   *
   * @return `true` if the streaming path should fall back to sort-based shuffle
   */
  def shouldFallback(
      producerRate: Double,
      consumerRate: Double,
      sustainedMs: Long,
      canAllocate: Boolean,
      networkUtilizationFraction: Double,
      producerVersion: String,
      consumerVersion: String): Boolean = {
    evaluate(
      producerRate,
      consumerRate,
      sustainedMs,
      canAllocate,
      networkUtilizationFraction,
      producerVersion,
      consumerVersion).isDefined
  }
}

/**
 * Companion object holding the fixed fallback thresholds and the closed set of fallback reasons.
 *
 * The numeric thresholds mirror the streaming shuffle specification exactly and must not be
 * altered: a sustained 2x slowness over 60 seconds, and a 90% network-saturation ceiling.
 */
private[spark] object StreamingShuffleFallbackPolicy {

  /**
   * Minimum producer-to-consumer throughput ratio that qualifies the consumer as "too slow":
   * the producer must be at least this many times faster than the consumer.
   */
  val CONSUMER_SLOWNESS_FACTOR: Double = 2.0

  /**
   * Minimum continuous duration, in milliseconds, the consumer slowness must persist before it
   * triggers a fallback (greater than 60 seconds).
   */
  val CONSUMER_SLOWNESS_DURATION_MS: Long = 60000L

  /**
   * Link-utilization fraction above which the network is considered saturated (90% of capacity).
   */
  val NETWORK_SATURATION_THRESHOLD: Double = 0.90

  /**
   * The closed set of reasons that cause the streaming shuffle path to fall back to the
   * sort-based path. Sealed so that consumers can match exhaustively and so that the set of
   * reasons is fixed at compile time.
   */
  sealed trait FallbackReason {

    /** A short, factual description of the condition, suitable for logs and traceability. */
    def message: String
  }

  /**
   * The consumer has been sustained at least [[CONSUMER_SLOWNESS_FACTOR]]x slower than the
   * producer for longer than the sustained-slowness duration.
   */
  case object ConsumerTooSlow extends FallbackReason {
    override def message: String =
      "consumer sustained at least 2x slower than producer for over 60s"
  }

  /** A required streaming buffer could not be allocated without risking an OutOfMemoryError. */
  case object MemoryPressure extends FallbackReason {
    override def message: String = "memory pressure prevents buffer allocation (OOM risk)"
  }

  /** Observed network utilization exceeded [[NETWORK_SATURATION_THRESHOLD]] of link capacity. */
  case object NetworkSaturation extends FallbackReason {
    override def message: String = "network saturation exceeds 90% of link capacity"
  }

  /** The producer and consumer reported mismatched versions. */
  case object VersionMismatch extends FallbackReason {
    override def message: String = "producer/consumer version mismatch"
  }
}
