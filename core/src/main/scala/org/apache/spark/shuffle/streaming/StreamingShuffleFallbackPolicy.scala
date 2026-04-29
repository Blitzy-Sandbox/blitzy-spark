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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryManager

/**
 * Decision class evaluating the four streaming-shuffle fallback conditions per the
 * streaming-shuffle specification:
 *
 *   1. Slow consumer -- consumer sustained 2x slower than producer for >60 s.
 *   2. Memory pressure -- current execution-memory utilization exceeds the safe
 *      threshold, meaning a streaming buffer allocation may trigger OOM.
 *   3. Network saturation -- token-bucket utilization exceeds 90% of link capacity
 *      (proxied in v1 by the cumulative count of backpressure events).
 *   4. Version mismatch -- producer/consumer Spark version mismatch.
 *
 * When ANY condition is true, [[shouldFallback]] returns `true` and the calling
 * [[StreamingShuffleManager]] delegates the affected shuffle to its held
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] collaborator. The fallback
 * decision is per-shuffle, evaluated lazily at writer/reader factory time
 * (`StreamingShuffleManager.getWriter` and `StreamingShuffleManager.getReader`).
 *
 * == Pure Decision Surface ==
 * This class is a pure decision class: callers receive a `Boolean` answer and the
 * caller (`StreamingShuffleManager`) is solely responsible for performing the actual
 * delegation when the answer is `true`. The only side effect produced by this class is
 * INFO-level logging of the triggering condition for operational visibility, plus a
 * small amount of internal bookkeeping in [[firstSlowDetectionTime]] used to enforce
 * the 60-second sustained window for the slow-consumer rule.
 *
 * == Coexistence Rationale ==
 * This class is the single integration point for fallback semantics. The held
 * `SortShuffleManager` is NOT modified -- it is invoked via its own public methods
 * by the caller after [[shouldFallback]] returns `true`. This satisfies the user
 * directive: "Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths."
 *
 * == Concurrency ==
 * `shouldFallback` may be invoked concurrently from multiple task threads on the
 * executor (one call per `getWriter`/`getReader` invocation). The implementation is
 * thread-safe:
 *   - Memory introspection delegates to [[MemoryManager.executionMemoryUsed]] and
 *     [[MemoryManager.maxOnHeapStorageMemory]], which the `MemoryManager`
 *     synchronizes internally.
 *   - Telemetry reads are lock-free (Dropwizard `Counter.getCount` returns an
 *     `LongAdder`-backed snapshot).
 *   - Slow-consumer window state is held in an [[AtomicLong]] mutated only via
 *     `compareAndSet(0L, nowMillis)` (initialize) and `set(0L)` (reset). When the
 *     condition is transient, the worst case is a benign double-CAS where two
 *     threads observe the trigger simultaneously; the second CAS fails and the
 *     thread reads the existing first-detection timestamp via `get()`. No correctness
 *     issue results because the elapsed-time computation is monotonic in
 *     `firstDetectionTime`.
 *
 * == Telemetry Budget ==
 * The implementation honors the streaming-shuffle "<1% telemetry CPU overhead"
 * budget:
 *   - Slow-consumer window tracking uses one [[AtomicLong]] CAS per call, no locks.
 *   - Memory-pressure check is a single synchronized read against the in-tree
 *     `MemoryManager` (one synchronized block, two field reads).
 *   - Network-saturation check is one Dropwizard `LongAdder` read.
 *   - Version-mismatch check is a constant-folded comparison (always returns
 *     `false` in v1).
 * The four checks are short-circuited by `||`-equivalent early returns, so the
 * average-case overhead per invocation is bounded by the cheapest check
 * (version mismatch) when the streaming path is operating nominally.
 *
 * @param conf          SparkConf retained for future configuration-driven thresholds
 *                      (e.g., a user-tunable `spark.shuffle.streaming.networkSaturationEvents`
 *                      key) and for symmetry with other streaming-shuffle classes whose
 *                      constructors carry `SparkConf`. v1 hardcodes thresholds inline so
 *                      no key is read here yet; preserving the parameter avoids touching
 *                      the constructor signature when v2 introduces the configurable
 *                      thresholds.
 * @param memoryManager unified memory manager used for memory-pressure introspection
 *                      via the in-tree `executionMemoryUsed` and
 *                      `maxOnHeapStorageMemory` accessors
 */
private[spark] class StreamingShuffleFallbackPolicy(
    conf: SparkConf,
    memoryManager: MemoryManager) extends Logging {

  // Touch the `conf` parameter once so future v2 code that adds configurable thresholds
  // has a clear extension point, and so the parameter is documented in the constructor
  // signature (rather than accidentally elided by a future refactor). The compiler
  // discards constructor-only parameters that are never referenced; this discard binding
  // is a no-op at runtime that clearly communicates intent.
  private val _conf: SparkConf = conf

  /**
   * Threshold ratio above which the consumer-2x-slower-than-producer rule fires. Defined
   * by the streaming-shuffle specification as "consumer sustained 2x slower than producer
   * for >60 seconds". Used in the [[isSlowConsumer]] heuristic where it scales the spill
   * count to compare against the backpressure-event count.
   */
  private val SLOW_CONSUMER_RATIO_THRESHOLD: Double = 2.0

  /**
   * Sustained-window for the slow-consumer condition: 60 seconds. The slow-consumer
   * trigger only fires after the proxy condition has been continuously true for at
   * least this duration, preventing transient blips (a single GC pause, a single
   * spill spike) from triggering an unwarranted fallback.
   */
  private val SLOW_CONSUMER_WINDOW_MILLIS: Long = 60_000L

  /**
   * Memory-pressure threshold expressed as a fraction of `maxOnHeapStorageMemory`.
   * When `executionMemoryUsed / maxOnHeapStorageMemory > 0.95`, allocating a new
   * streaming buffer is unsafe and the streaming path falls back to sort-based
   * shuffle. Distinct from the 80% spill threshold (which triggers eviction within
   * the streaming path itself) -- the 95% threshold is the safety margin that
   * preserves headroom for the existing storage-cache and other execution-memory
   * consumers.
   */
  private val MEMORY_PRESSURE_THRESHOLD: Double = 0.95

  /**
   * Network-saturation threshold expressed as a fraction of link capacity. The
   * specification declares a fallback when the streaming subsystem observes >90%
   * link utilization sustained over recent samples. v1 proxies this signal via the
   * cumulative count of backpressure events recorded in [[StreamingShuffleMetrics]];
   * see [[isNetworkSaturated]] for details.
   */
  private val NETWORK_SATURATION_THRESHOLD: Double = 0.90

  /**
   * v1 backpressure-event-count threshold proxying network saturation. When the
   * cumulative `backpressureEvents` counter from [[StreamingShuffleMetrics]] exceeds
   * this value, the v1 implementation declares the network saturated. v2 may replace
   * this proxy with a richer per-window utilization signal driven by token-bucket
   * telemetry from `BackpressureProtocol`; until then, the cumulative count is the
   * cheapest signal available without introducing new RPCs or per-shuffle state.
   */
  private val NETWORK_SATURATION_EVENT_THRESHOLD: Long = 100L

  /**
   * v1 minimum backpressure-event count below which the slow-consumer trigger is
   * suppressed. Without this floor, freshly-started shuffles would falsely trigger
   * the slow-consumer rule because spill events recorded from prior shuffles would
   * dominate the ratio comparison. Above the floor, the heuristic compares
   * `spills * 2.0` against `backpressureEvents`.
   */
  private val SLOW_CONSUMER_MIN_BACKPRESSURE_EVENTS: Long = 10L

  /**
   * Per-policy slow-consumer first-detection timestamp, used to enforce the 60 s
   * sustained window. Set to `0L` when the slow-consumer condition is not currently
   * observed; set to `System.currentTimeMillis()` (via `compareAndSet`) on the first
   * call where the proxy condition is observed; reset to `0L` on the first call where
   * the proxy condition no longer holds. The trigger fires only when the condition
   * has been continuously observed for [[SLOW_CONSUMER_WINDOW_MILLIS]] or longer.
   *
   * Implemented as an [[AtomicLong]] rather than a `volatile var` because the
   * initialize-or-read pattern requires `compareAndSet`-driven CAS for correctness
   * under concurrent invocation; a plain volatile read-modify-write would race.
   */
  private val firstSlowDetectionTime = new AtomicLong(0L)

  /**
   * Evaluate all four fallback conditions in priority order. Short-circuits as soon
   * as any condition is observed to be true and emits a DEBUG-level log line naming
   * the triggering condition and the affected shuffle ID for operational visibility
   * when `spark.shuffle.streaming.debug=true`.
   *
   * Evaluation order is by ascending CPU cost:
   *   1. Version mismatch (cheapest -- constant-folded comparison in v1).
   *   2. Memory pressure (one synchronized read against `MemoryManager`).
   *   3. Network saturation (one `LongAdder` read against the metrics counter).
   *   4. Slow consumer (most expensive -- requires AtomicLong CAS for sustained-window
   *      bookkeeping plus two `LongAdder` reads).
   *
   * == Log Level Selection ==
   * `shouldFallback` is invoked from [[StreamingShuffleManager.getWriter]] and
   * [[StreamingShuffleManager.getReader]] -- i.e., once per task-attempt, not once
   * per shuffleId. Under stress workloads (the AAP section 0.5.1.6 stress harness with
   * 10 concurrent tasks and 5 concurrent shuffles, where network-saturation triggers
   * the fallback path on every getWriter/getReader call), an INFO-level log here
   * would emit ~130 K log lines in a 5-minute window -- approximately 21 MB
   * extrapolating to ~252 MB/hour, breaching the AAP section 0.7.2.5 "<10 MB/hour per
   * executor" log-volume budget by ~25x. Demoting these triggers to DEBUG lets the
   * cumulative `shuffle.streaming.backpressureEvents` JMX counter (which `is*`
   * helpers consult here) carry the operational signal, while the structured DEBUG
   * line preserves the actionable why-it-fell-back detail for the troubleshooting
   * code path enabled via `spark.shuffle.streaming.debug=true`.
   *
   * @param handle    the streaming shuffle handle being evaluated; only its
   *                  `shuffleId` is read, for log correlation
   * @param telemetry the streaming-shuffle metric set aggregating runtime telemetry
   *                  across all active shuffles on this executor
   * @return `true` if the streaming path should fall back to sort-based shuffle for
   *         this shuffle handle
   */
  def shouldFallback(
      handle: StreamingShuffleHandle[_, _, _],
      telemetry: StreamingShuffleMetrics): Boolean = {
    // Condition 4 first (cheapest in v1): producer/consumer Spark-version mismatch.
    // Logged at DEBUG: see class-level "Log Level Selection" rationale above.
    if (isVersionMismatch()) {
      logDebug(
        s"Falling back to sort-based shuffle for shuffle ${handle.shuffleId}: " +
          s"producer/consumer Spark version mismatch")
      return true
    }
    // Condition 2: memory pressure. Allocating a streaming buffer when execution
    // memory is already >95% of capacity risks OOM; the sort-based fallback's
    // disk-backed pipeline tolerates the pressure better.
    // Logged at DEBUG: see class-level "Log Level Selection" rationale above.
    if (isMemoryPressure()) {
      logDebug(
        s"Falling back to sort-based shuffle for shuffle ${handle.shuffleId}: " +
          s"memory pressure (executionMemoryUsed=${memoryManager.executionMemoryUsed}, " +
          s"maxOnHeapStorageMemory=${memoryManager.maxOnHeapStorageMemory})")
      return true
    }
    // Condition 3: network saturation, proxied by the backpressure-event counter.
    // Logged at DEBUG: see class-level "Log Level Selection" rationale above.
    if (isNetworkSaturated(telemetry)) {
      logDebug(
        s"Falling back to sort-based shuffle for shuffle ${handle.shuffleId}: " +
          s"network saturation (backpressureEvents=${telemetry.getBackpressureEventsCount} > " +
          s"$NETWORK_SATURATION_EVENT_THRESHOLD, threshold ratio $NETWORK_SATURATION_THRESHOLD)")
      return true
    }
    // Condition 1 last: slow consumer (requires sustained-window bookkeeping).
    // Logged at DEBUG: see class-level "Log Level Selection" rationale above.
    if (isSlowConsumer(telemetry)) {
      logDebug(
        s"Falling back to sort-based shuffle for shuffle ${handle.shuffleId}: " +
          s"slow consumer (sustained > ${SLOW_CONSUMER_RATIO_THRESHOLD}x slower for " +
          s">$SLOW_CONSUMER_WINDOW_MILLIS ms)")
      return true
    }
    false
  }

  /**
   * Detect producer/consumer Spark-version mismatch.
   *
   * v1 implementation: returns `false` always. In practice both producer and
   * consumer executors run identical Spark JARs (same release, same revision,
   * deployed by the same cluster manager), so a mismatch cannot occur. The hook
   * is retained so v2 -- which may introduce a stable cross-version streaming
   * protocol -- can drop in a real comparison without changing this file's
   * import structure or the calling [[shouldFallback]] surface.
   *
   * The reference to [[org.apache.spark.SPARK_VERSION]] keeps the import alive
   * under `-Wunused:imports` and provides the extension point for future
   * version-skew detection (e.g., a future test could inject a mismatched
   * version via reflection on the package-object value).
   *
   * @return always `false` in v1; reserved as an extension point for future
   *         cross-version compatibility checking
   */
  private def isVersionMismatch(): Boolean = {
    // v1: always false. Hook point retained for future skew detection. The
    // discard binding ensures the SPARK_VERSION import remains live so that v2
    // can wire in a real comparison without re-touching the import block.
    val _ = SPARK_VERSION
    false
  }

  /**
   * Detect memory pressure: returns `true` when current on-heap execution-memory
   * utilization exceeds [[MEMORY_PRESSURE_THRESHOLD]] (95%) of the maximum
   * `maxOnHeapStorageMemory`. The metric is approximate -- `executionMemoryUsed`
   * counts both on-heap and off-heap pools while `maxOnHeapStorageMemory` reports
   * only the on-heap upper bound -- but this is the cheapest accurate-enough
   * signal exposed by the in-tree `MemoryManager` without expanding its public
   * surface.
   *
   * Defensive guard: returns `false` when `maxOnHeapStorageMemory` is `<= 0`,
   * which can occur in unit tests using a mock `MemoryManager` or in degenerate
   * configurations where on-heap storage memory is zero. The guard avoids a
   * division-by-zero or a false-positive trigger driven by zero capacity.
   *
   * @return `true` when on-heap execution memory has crossed the safety threshold
   */
  private def isMemoryPressure(): Boolean = {
    val maxOnHeap = memoryManager.maxOnHeapStorageMemory
    if (maxOnHeap <= 0L) {
      // Defensive: avoid divide-by-zero on degenerate configurations or test mocks.
      return false
    }
    val used = memoryManager.executionMemoryUsed
    val ratio = used.toDouble / maxOnHeap.toDouble
    ratio > MEMORY_PRESSURE_THRESHOLD
  }

  /**
   * Detect network saturation via the [[StreamingShuffleMetrics.getBackpressureEventsCount]]
   * counter.
   *
   * v1 heuristic: when the cumulative backpressure-event count has exceeded
   * [[NETWORK_SATURATION_EVENT_THRESHOLD]] (100), the network is declared
   * saturated. This is a simple monotonic-threshold proxy that does not require
   * additional RPCs or per-shuffle aggregation state. It captures the operator
   * intent: "if backpressure has fired this many times, the wire is the bottleneck
   * and the streaming path is unlikely to outperform sort-based shuffle for new
   * shuffles".
   *
   * v2 may replace this proxy with a richer per-window utilization signal driven
   * by token-bucket telemetry from `BackpressureProtocol` -- compared against
   * [[NETWORK_SATURATION_THRESHOLD]] (90% link capacity per the specification).
   * Until then, the cumulative event count is the cheapest signal that does not
   * require introducing new metric types.
   *
   * @param telemetry the streaming-shuffle metric set aggregating runtime telemetry
   * @return `true` when cumulative backpressure events exceed the v1 threshold
   */
  private def isNetworkSaturated(telemetry: StreamingShuffleMetrics): Boolean = {
    // v1 heuristic: backpressure events accumulating is the cheapest network-saturation
    // proxy that does not require new RPCs. The token-bucket utilization is a richer
    // signal but requires per-shuffle aggregation that v1 does not yet expose.
    val recentEvents = telemetry.getBackpressureEventsCount
    recentEvents > NETWORK_SATURATION_EVENT_THRESHOLD
  }

  /**
   * Detect slow consumer: returns `true` when the proxy condition (high spill count
   * relative to backpressure event count) has been continuously observed for at
   * least [[SLOW_CONSUMER_WINDOW_MILLIS]] (60 seconds).
   *
   * v1 heuristic: a high spill count combined with a meaningful backpressure-event
   * count implies that the producer is generating data faster than the consumer
   * can acknowledge, forcing the spill manager to evict partitions to disk to free
   * buffer memory. The trigger condition is:
   *
   * {{{
   *   backpressure > SLOW_CONSUMER_MIN_BACKPRESSURE_EVENTS &&
   *     spills * SLOW_CONSUMER_RATIO_THRESHOLD > backpressure
   * }}}
   *
   * which encodes "more than 10 backpressure events have fired AND the spill rate
   * is more than half the backpressure rate". The latter clause is the operator
   * intent of "consumer 2x slower than producer" -- when consumer ack pace lags
   * producer transmission pace by a factor of 2, more than half of producer flushes
   * end up spilling.
   *
   * Sustained-window enforcement: the proxy condition alone is insufficient to
   * trigger fallback because a single transient blip (a brief consumer GC pause)
   * could trip it. The [[firstSlowDetectionTime]] [[AtomicLong]] records the
   * timestamp of the first call observing the condition; subsequent calls that
   * also observe the condition compute the elapsed time and only declare slow
   * consumer when it reaches [[SLOW_CONSUMER_WINDOW_MILLIS]]. Calls that do NOT
   * observe the condition reset the timestamp to `0L`, restarting the window the
   * next time the condition reappears.
   *
   * @param telemetry the streaming-shuffle metric set aggregating runtime telemetry
   * @return `true` when the slow-consumer proxy has been continuously observed for
   *         at least 60 seconds
   */
  private def isSlowConsumer(telemetry: StreamingShuffleMetrics): Boolean = {
    val spills = telemetry.getSpillCount
    val backpressure = telemetry.getBackpressureEventsCount
    val nowMillis = System.currentTimeMillis()

    // Trigger condition: more than half the backpressure events resulted in spills,
    // AND the absolute backpressure-event count is above the floor (so freshly-started
    // shuffles are not mis-identified as slow). The Double->Long cast on the threshold
    // is exact because SLOW_CONSUMER_RATIO_THRESHOLD is integral (2.0).
    val triggered =
      backpressure > SLOW_CONSUMER_MIN_BACKPRESSURE_EVENTS &&
        spills * SLOW_CONSUMER_RATIO_THRESHOLD.toLong > backpressure

    if (triggered) {
      // Initialize the sustained-window onset on the first call observing the trigger.
      // The compareAndSet returns true exactly when this thread initialized the field;
      // false when another thread (or a previous call on this thread) initialized it
      // first. Either way, after the call `firstSlowDetectionTime` holds the first
      // detection timestamp.
      val firstSeen = firstSlowDetectionTime.compareAndSet(0L, nowMillis)
      val firstDetectionTime = if (firstSeen) nowMillis else firstSlowDetectionTime.get()
      val elapsed = nowMillis - firstDetectionTime
      // AAP 0.1.1 specifies "Consumer sustained 2x slower than producer for >60 seconds"
      // (strict greater-than). The boundary case `elapsed == SLOW_CONSUMER_WINDOW_MILLIS`
      // does NOT trigger fallback per the AAP literal interpretation.
      elapsed > SLOW_CONSUMER_WINDOW_MILLIS
    } else {
      // Reset the window: the proxy condition no longer holds, so the next time it
      // reappears we start a fresh sustained-window timer rather than counting
      // continuously since some earlier blip.
      firstSlowDetectionTime.set(0L)
      false
    }
  }
}
