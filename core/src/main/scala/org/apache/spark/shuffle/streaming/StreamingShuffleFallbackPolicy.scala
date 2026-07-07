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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging

/**
 * Immutable snapshot of the runtime signals evaluated by [[StreamingShuffleFallbackPolicy]] for a
 * single fallback decision. Each field maps to exactly one of the four fallback guards so that the
 * decision engine and its unit tests can reason about every threshold independently.
 *
 * All fields are cheap primitives sampled by the streaming subsystem immediately before a
 * decision: the producer/consumer throughput samples and the sustained-slow duration come from the
 * backpressure protocol, the memory-utilization percentage comes from the spill monitor, the
 * network-utilization percentage comes from the transport layer, and the two protocol versions
 * come from the streaming handshake. The type is a pure data carrier and holds no references to
 * executor state, so it is safe to construct and pass around on any thread.
 *
 * @param consumerRateBytesPerSec   most recent reduce-side consumption throughput, in bytes/second
 * @param producerRateBytesPerSec   most recent map-side production throughput, in bytes/second
 * @param sustainedSlowMillis       how long the consumer has been sustained slow, in milliseconds
 * @param memoryUtilizationPercent  current streaming buffer memory utilization, an integer percent
 * @param networkUtilizationPercent current network link utilization, an integer percent
 * @param localProtocolVersion      the streaming protocol version running on this executor
 * @param remoteProtocolVersion     the streaming protocol version reported by the peer executor
 */
@Since("4.2.0")
private[spark] case class FallbackStats(
    consumerRateBytesPerSec: Double,
    producerRateBytesPerSec: Double,
    sustainedSlowMillis: Long,
    memoryUtilizationPercent: Int,
    networkUtilizationPercent: Int,
    localProtocolVersion: Int,
    remoteProtocolVersion: Int)

/**
 * The four-condition fallback decision engine for the streaming shuffle backend.
 *
 * Streaming shuffle is an opt-in optimization that trades some of the durability of the sort-based
 * shuffle for lower end-to-end latency. To honor the feature's "zero regression / automatic
 * fallback" guarantee, the streaming path continuously self-monitors and, whenever conditions make
 * streaming unsafe or counterproductive, reverts to the production-stable sort path. This policy
 * encapsulates that decision: when [[shouldFallback]] returns `true`, `StreamingShuffleManager`
 * selects its inner `SortShuffleManager` (composition delegation) for the affected shuffle, so
 * correctness and performance never regress below the sort baseline.
 *
 * The engine evaluates four independent, defense-in-depth guards; a `true` from any one triggers
 * fallback:
 *
 *  - '''Slow consumer''' - the reduce side is sustained at least 2x slower than the map side for
 *    strictly longer than 60 seconds. Streaming provides no latency benefit once the consumer
 *    cannot keep up, and continuing to buffer would only grow memory pressure, so the sort path
 *    (which fully materializes map output) becomes preferable.
 *  - '''Memory pressure''' - streaming buffer memory utilization exceeds 95%, signalling an
 *    imminent out-of-memory risk. This threshold is deliberately distinct from the 80% spill
 *    threshold: at 80% the subsystem spills the largest / least-recently-used buffers to disk yet
 *    keeps streaming, whereas above 95% the safe course is to abandon streaming for this shuffle
 *    entirely and fall back to sort.
 *  - '''Network saturation''' - network link utilization exceeds 90% of capacity, at which point
 *    pipelining competes with, rather than complements, the existing shuffle transfer traffic.
 *  - '''Version mismatch''' - the local and remote streaming protocol versions differ, so the
 *    peers cannot safely interpret each other's wire framing and must use the sort path.
 *
 * Together these guards ensure zero regression for memory-bound and CPU-bound workloads: such
 * workloads either never engage streaming or fall back before streaming can degrade them.
 *
 * The individual predicates ([[isSlowConsumer]], [[isMemoryPressure]], [[isNetworkSaturated]] and
 * [[isVersionMismatch]]) are pure, side-effect-free functions so that each threshold can be unit
 * tested in isolation. Only the aggregating [[shouldFallback]] method performs observability side
 * effects (an INFO log naming the guard that fired, and a metric update for the flow-control case).
 *
 * @param conf    the resolved streaming configuration for this application; logged once at DEBUG
 *                when the policy is created to record the configuration context. The v1 fallback
 *                thresholds are the fixed constants on the companion object, per the feature
 *                specification, so no threshold is read from this configuration.
 * @param metrics the streaming telemetry sink; used to record a backpressure event when a fallback
 *                is caused by a sustained slow consumer, so the fallback is observable through
 *                Spark's `MetricsSystem` alongside the other streaming metrics.
 */
@Since("4.2.0")
private[spark] class StreamingShuffleFallbackPolicy(
    conf: StreamingShuffleConfig,
    metrics: StreamingShuffleMetrics)
  extends Logging {

  import StreamingShuffleFallbackPolicy.{MEMORY_PRESSURE_THRESHOLD_PERCENT,
    NETWORK_SATURATION_THRESHOLD_PERCENT, SLOW_CONSUMER_MIN_SUSTAINED_MILLIS,
    SLOW_CONSUMER_RATE_MULTIPLIER}

  // Record the resolved streaming configuration once, when the policy is created. This is gated at
  // DEBUG (and therefore by spark.shuffle.streaming.debug), so it adds no overhead in normal
  // operation while giving operators a record of the configuration context in which this policy's
  // fallback decisions are made. The v1 fallback thresholds themselves are the fixed constants on
  // the companion object, per the feature specification, so none is read from the configuration.
  logDebug(s"Initialized streaming shuffle fallback policy with config: $conf")

  /**
   * Returns `true` when the reduce-side consumer is sustained at least twice as slow as the
   * map-side producer for strictly longer than the sustained-slow window.
   *
   * The rate comparison is expressed as `consumerRate * 2 <= producerRate` rather than a division
   * so that a momentarily idle producer (rate 0) cannot cause a divide-by-zero and so the test is
   * exact. The duration guard is strict (`> 60000 ms`), so a consumer that has only just reached
   * the 60-second boundary does not yet trigger fallback.
   *
   * This predicate is pure: it performs no logging and mutates no state.
   *
   * @param consumerRateBytesPerSec reduce-side consumption throughput, in bytes/second
   * @param producerRateBytesPerSec map-side production throughput, in bytes/second
   * @param sustainedMillis         how long the slow condition has held, in milliseconds
   * @return `true` iff the consumer is at least 2x slower AND the condition has held strictly for
   *         more than 60 seconds
   */
  def isSlowConsumer(
      consumerRateBytesPerSec: Double,
      producerRateBytesPerSec: Double,
      sustainedMillis: Long): Boolean = {
    consumerRateBytesPerSec * SLOW_CONSUMER_RATE_MULTIPLIER <= producerRateBytesPerSec &&
      sustainedMillis > SLOW_CONSUMER_MIN_SUSTAINED_MILLIS
  }

  /**
   * Returns `true` when streaming buffer memory utilization is above the memory-pressure threshold
   * (strictly greater than 95%), indicating an imminent out-of-memory risk that must revert the
   * shuffle to the sort path. Note this is distinct from the 80% spill threshold, which triggers
   * spilling to disk while keeping the shuffle on the streaming path.
   *
   * This predicate is pure: it performs no logging and mutates no state.
   *
   * @param utilizationPercent current streaming buffer memory utilization, an integer percent
   * @return `true` iff `utilizationPercent` is greater than 95
   */
  def isMemoryPressure(utilizationPercent: Int): Boolean = {
    utilizationPercent > MEMORY_PRESSURE_THRESHOLD_PERCENT
  }

  /**
   * Returns `true` when network link utilization is above the saturation threshold (strictly
   * greater than 90% of link capacity), at which point pipelining streamed blocks would contend
   * with the existing transfer traffic instead of accelerating the shuffle.
   *
   * This predicate is pure: it performs no logging and mutates no state.
   *
   * @param linkUtilizationPercent current network link utilization, an integer percent
   * @return `true` iff `linkUtilizationPercent` is greater than 90
   */
  def isNetworkSaturated(linkUtilizationPercent: Int): Boolean = {
    linkUtilizationPercent > NETWORK_SATURATION_THRESHOLD_PERCENT
  }

  /**
   * Returns `true` when the local and remote streaming protocol versions differ. A mismatch means
   * the two executors cannot safely interpret each other's wire framing, so the shuffle must use
   * the sort path. Callers typically pass [[StreamingShuffleFallbackPolicy.PROTOCOL_VERSION]] as
   * the local version and the peer-reported version as the remote version.
   *
   * This predicate is pure: it performs no logging and mutates no state.
   *
   * @param localVersion  the streaming protocol version implemented by this executor
   * @param remoteVersion the streaming protocol version reported by the peer executor
   * @return `true` iff the two versions are not equal
   */
  def isVersionMismatch(localVersion: Int, remoteVersion: Int): Boolean = {
    localVersion != remoteVersion
  }

  /**
   * Evaluates all four fallback guards against a single [[FallbackStats]] snapshot and returns
   * `true` if any guard fires, meaning `StreamingShuffleManager` must delegate this shuffle to its
   * inner `SortShuffleManager`.
   *
   * The guards are checked in priority order - slow consumer, memory pressure, network saturation,
   * then version mismatch - and the first that fires is logged at INFO so operators can see
   * precisely why streaming was disabled for the shuffle. Evaluation short-circuits on the first
   * firing guard because the fallback decision is already made. When the slow-consumer guard is
   * the cause, the streaming backpressure counter is also incremented, since a sustained slow
   * consumer is the terminal outcome of the flow-control subsystem.
   *
   * @param stats the runtime signals captured immediately before the decision
   * @return `true` if the streaming path must fall back to sort; `false` to remain on streaming
   */
  def shouldFallback(stats: FallbackStats): Boolean = {
    if (isSlowConsumer(stats.consumerRateBytesPerSec, stats.producerRateBytesPerSec,
        stats.sustainedSlowMillis)) {
      // A sustained slow consumer is the terminal outcome of backpressure; record it on the
      // streaming backpressure metric so the fallback is observable alongside other flow-control
      // events, then revert to the durable sort path.
      metrics.incBackpressureEvents()
      logInfo(s"Streaming shuffle falling back to sort: consumer sustained at least " +
        s"${SLOW_CONSUMER_RATE_MULTIPLIER}x slower than producer for " +
        s"${stats.sustainedSlowMillis} ms (consumerRate=${stats.consumerRateBytesPerSec} B/s, " +
        s"producerRate=${stats.producerRateBytesPerSec} B/s).")
      true
    } else if (isMemoryPressure(stats.memoryUtilizationPercent)) {
      logInfo(s"Streaming shuffle falling back to sort: buffer memory utilization " +
        s"${stats.memoryUtilizationPercent}% exceeds " +
        s"${MEMORY_PRESSURE_THRESHOLD_PERCENT}% (out-of-memory risk).")
      true
    } else if (isNetworkSaturated(stats.networkUtilizationPercent)) {
      logInfo(s"Streaming shuffle falling back to sort: network utilization " +
        s"${stats.networkUtilizationPercent}% exceeds " +
        s"${NETWORK_SATURATION_THRESHOLD_PERCENT}% of link capacity.")
      true
    } else if (isVersionMismatch(stats.localProtocolVersion, stats.remoteProtocolVersion)) {
      logInfo(s"Streaming shuffle falling back to sort: protocol version mismatch " +
        s"(local=${stats.localProtocolVersion}, remote=${stats.remoteProtocolVersion}).")
      true
    } else {
      false
    }
  }
}

/**
 * Companion object holding the streaming protocol version and the fixed fallback thresholds shared
 * by every [[StreamingShuffleFallbackPolicy]] instance. These constants encode the exact thresholds
 * mandated by the streaming shuffle specification and are exposed so that other streaming
 * components and tests can reference the same authoritative values.
 */
@Since("4.2.0")
private[spark] object StreamingShuffleFallbackPolicy {

  /**
   * The streaming shuffle wire-protocol version implemented by this build. Producer and consumer
   * executors exchange this value during the streaming handshake; a difference indicates an
   * incompatible peer and forces a fallback through the [[isVersionMismatch]] guard. Bump this
   * whenever the on-wire framing changes in an incompatible way.
   */
  val PROTOCOL_VERSION: Int = 1

  /**
   * Multiplier defining "at least 2x slower": the consumer is considered slow when its throughput,
   * doubled, still does not reach the producer's throughput.
   */
  val SLOW_CONSUMER_RATE_MULTIPLIER: Int = 2

  /**
   * Minimum sustained duration, in milliseconds, before a slow consumer forces fallback. The guard
   * is strict (`> 60000L`), so transient slowness within the first 60 seconds is tolerated.
   */
  val SLOW_CONSUMER_MIN_SUSTAINED_MILLIS: Long = 60000L

  /**
   * Buffer-memory utilization percentage above which fallback is forced to avoid an OOM. Distinct
   * from the 80% spill threshold: spilling keeps the shuffle streaming, whereas exceeding this
   * bound abandons streaming in favor of the sort path.
   */
  val MEMORY_PRESSURE_THRESHOLD_PERCENT: Int = 95

  /**
   * Network link utilization percentage above which fallback is forced, because pipelining would
   * contend with saturated links rather than accelerate the shuffle.
   */
  val NETWORK_SATURATION_THRESHOLD_PERCENT: Int = 90
}
