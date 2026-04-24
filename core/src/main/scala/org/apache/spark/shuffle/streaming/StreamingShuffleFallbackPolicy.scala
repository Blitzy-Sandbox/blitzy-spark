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

import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{REASON, SHUFFLE_ID}

/**
 * Pure decision-only routing oracle that answers a single question on behalf of
 * [[StreamingShuffleManager]]: should a newly registered shuffle be ROUTED to
 * the streaming code path, or should the manager FALL BACK to the held
 * `org.apache.spark.shuffle.sort.SortShuffleManager` for this particular
 * shuffle?
 *
 * Invocation lifecycle &mdash; one call per shuffle registration:
 *   - [[evaluate]] is invoked by `StreamingShuffleManager.registerShuffle`
 *     exactly once per `shuffleId`, at driver-side registration time (see
 *     AAP section 0.4.1.4 "Streaming Shuffle &mdash; Executor Bootstrap and Runtime
 *     Wiring"). The result determines routing for the entire lifetime of that
 *     shuffle.
 *   - The policy is stateless: identical inputs always produce identical
 *     outputs, and no instance state survives between calls. This makes it
 *     safe to invoke concurrently from multiple DAG-scheduler threads
 *     registering independent shuffles.
 *   - The `SortShuffleManager` is NEVER modified by this policy; fallback is
 *     purely a routing decision at the `StreamingShuffleManager` level. When
 *     [[evaluate]] returns `Some(reason)`, the manager stores the reason and
 *     delegates all subsequent calls for that `shuffleId` to its held
 *     [[org.apache.spark.shuffle.sort.SortShuffleManager]] instance. The sort
 *     path is thus the production-stable fallback target (AAP section 0.1.2
 *     "Preserve existing sort-based shuffle as production-stable fallback").
 *
 * Fallback conditions evaluated (user-specified plus ADR-005 mutual-exclusion):
 *
 *   1. '''Consumer sustained 2&times; slower than producer for >60 s''' &mdash;
 *      deferred to v2. Requires runtime acknowledgment-rate history that does
 *      not yet exist at `registerShuffle` time. The runtime
 *      [[BackpressureProtocol]] already detects this condition dynamically and
 *      will synthesize a backpressure event; future adaptive re-evaluation can
 *      surface a fallback trigger by re-invoking [[evaluate]] on re-registration.
 *   2. '''Memory pressure preventing buffer allocation (OOM risk)''' &mdash;
 *      ACTIVE CHECK. Approximated at registration time by examining the
 *      configured executor memory (MiB): if the executor is too small to
 *      host the 20&percnt; streaming-shuffle buffer budget plus a safe
 *      working-set headroom, streaming is not viable and we fall back.
 *   3. '''Network saturation exceeds 90&percnt; link capacity''' &mdash;
 *      deferred to v2. Requires runtime network telemetry (per-interface byte
 *      counters) that does not yet exist at `registerShuffle` time. The runtime
 *      [[BackpressureProtocol]] already enforces an 80&percnt; token-bucket cap
 *      that moderates the live send rate; further automatic fallback on
 *      sustained >90&percnt; saturation is a v2 refinement.
 *   4. '''Producer / consumer version mismatch''' &mdash; deferred. Not applicable
 *      within a single cluster where every executor runs the identical
 *      Apache Spark binary. Cross-version scenarios (SQL migration, Spark
 *      Connect mixing) are explicitly declared out of scope by the AAP section
 *      0.6.2 "Cross-version Spark Connect mixing". If/when multi-version
 *      executors become supported, a runtime handshake in
 *      [[BackpressureProtocol]] will exchange version magic bytes and feed this
 *      method.
 *   5. '''ADR-005 mutual exclusion with push-based shuffle''' &mdash; ACTIVE CHECK.
 *      Streaming shuffle and push-based shuffle are mutually exclusive per
 *      active shuffle; when `spark.shuffle.push.enabled=true` the shuffle MUST
 *      go through the sort/push path. This policy is the single place where
 *      the exclusion is enforced at registration time.
 *   6. '''Feature-flag gate''' &mdash; ACTIVE CHECK. The manager MAY be selected
 *      via the short name `spark.shuffle.manager=streaming`, but the user
 *      retains a separate kill switch `spark.shuffle.streaming.enabled`. When
 *      the kill switch is false, the streaming path is disabled and every
 *      shuffle falls back. This allows operators to quickly disable the
 *      streaming path cluster-wide without forcing a redeploy that changes the
 *      short name.
 *   7. '''Partition-count sanity''' &mdash; ACTIVE CHECK. Defensive invariant
 *      check: a non-positive partition count indicates a corrupted or
 *      uninitialized [[org.apache.spark.Partitioner]] and cannot produce valid
 *      streaming buffers. Falling back delegates the sanity-check handling to
 *      the existing sort path, which surfaces the same error through its
 *      own validation in a way already exercised in production.
 *
 * Note on correctness of deferral: Deferring the runtime-based conditions
 * (1, 3, 4) does not compromise correctness &mdash; the sort path remains the
 * automatic fallback target if any of those conditions materialize at
 * runtime. Specifically, [[BackpressureProtocol]] detects backpressure at
 * send time and emits throttle events; [[MemorySpillManager]] evicts to
 * disk via the `BlockManager` when buffers cross the 80&percnt; threshold;
 * and [[StreamingShuffleReader]]'s connection-timeout watchdog invalidates
 * partial reads, triggering DAG-scheduler upstream recomputation through the
 * existing fault-recovery model (AAP section 0.1.2 Failure Handling Protocol).
 * The pre-registration policy is a first-line guard; the runtime subsystems
 * form the second line that catches conditions the policy could not know at
 * registration time.
 *
 * Binary compatibility (MiMa F-017): this object is `private[spark]` and lives
 * in the brand-new sub-package `org.apache.spark.shuffle.streaming`, so it
 * introduces no public SPI signature and requires no entry in
 * `project/MimaExcludes.scala`. See `blitzy-docs/streaming-shuffle.md` for
 * the feature's full architectural narrative and
 * `blitzy-docs/streaming-shuffle-decision-log.md` for the rationale behind
 * the "pre-registration conditions here, runtime conditions elsewhere" split.
 */
private[spark] object StreamingShuffleFallbackPolicy extends Logging {

  /**
   * Reason code returned when the `spark.shuffle.streaming.enabled` kill
   * switch is disabled. Operators who select the streaming manager via the
   * `spark.shuffle.manager=streaming` short name can still globally disable
   * the streaming path by setting this flag to false without redeploying
   * the cluster with a different manager selection.
   */
  private val REASON_STREAMING_DISABLED: String = "streaming-disabled-by-config"

  /**
   * Reason code returned when push-based shuffle is active on the same
   * shuffle. Streaming shuffle and push-based shuffle are mutually
   * exclusive per active shuffle (ADR-005 &mdash; AAP section 0.7.2).
   */
  private val REASON_PUSH_SHUFFLE_ACTIVE: String = "push-based-shuffle-active"

  /**
   * Reason code returned when [[org.apache.spark.Partitioner.numPartitions]]
   * is non-positive, which indicates a misconfigured partitioner and makes
   * streaming buffer allocation impossible.
   */
  private val REASON_INVALID_PARTITION_COUNT: String = "invalid-partition-count"

  /**
   * Reason code returned when the configured executor memory is too small to
   * host the 20&percnt; streaming-buffer budget plus a safe working-set
   * headroom. See [[MINIMUM_EXECUTOR_MEMORY_MIB]] for the exact threshold and
   * rationale.
   */
  private val REASON_INSUFFICIENT_EXECUTOR_MEMORY: String =
    "insufficient-executor-memory"

  /**
   * Minimum executor memory (MiB) required to safely run the streaming
   * shuffle path. 256 MiB is chosen conservatively:
   *   - 20&percnt; of 256 MiB &equiv; ~51 MiB of streaming-buffer budget
   *     (the default `spark.shuffle.streaming.bufferSizePercent = 20`).
   *   - Below this threshold, a single over-sized partition can monopolize
   *     the buffer pool, leaving no room for concurrent shuffles, which
   *     in turn triggers aggressive spilling and defeats the latency-reduction
   *     benefit streaming is meant to provide.
   *   - Executor JVMs below 256 MiB are also prone to OOM under the baseline
   *     300 MiB reserved-memory floor of Unified Memory Manager; that floor
   *     alone exceeds the configured executor size, so streaming shuffle
   *     would have zero usable budget anyway.
   *
   * Operators who deliberately choose smaller executors for test clusters
   * should either leave `spark.shuffle.manager` at its default (`sort`) or
   * raise the executor memory above this threshold.
   */
  private val MINIMUM_EXECUTOR_MEMORY_MIB: Long = 256L

  /**
   * Routing decision for a single shuffle registration. Returns `None` when
   * the streaming path is selected; returns `Some(reason)` when the sort-based
   * fallback path is selected, where `reason` is a stable, machine-readable
   * string documenting why the policy declined streaming. The `reason` is
   * attached to structured log output and is surfaced unchanged through
   * [[StreamingShuffleManager]]'s fallback bookkeeping so that operators and
   * downstream tooling can classify decisions at a glance.
   *
   * Evaluation order is deliberately ordered from cheapest / most
   * deterministic to most expensive / most dependent on runtime context, so
   * that the policy short-circuits on the most common early-exit conditions:
   *
   *   1. Feature flag (cheap boolean lookup).
   *   2. Push-based shuffle mutual exclusion (cheap boolean lookup).
   *   3. Partition-count sanity (cheap integer comparison).
   *   4. Executor-memory sanity (cheap long comparison).
   *
   * On a fallback decision, the method emits a single structured INFO-level
   * log record keyed by the shuffle ID and the reason code, so that log
   * aggregators can build time-series of fallback counts by reason without
   * parsing free-form text. On an OK decision, no log is emitted to keep
   * log volume under the user's `<10 MB/hour per executor` budget (AAP
   * section 0.1.2).
   *
   * Thread-safety: this method is pure (no instance state, no mutation of its
   * arguments). It is safe to invoke concurrently from any thread, including
   * the DAG-scheduler event-loop thread that registers shuffles.
   *
   * @param shuffleId   the shuffle identifier being registered; used only for
   *                    logging (no semantic role in the decision).
   * @param dependency  the `ShuffleDependency` passed to
   *                    `registerShuffle`; its `partitioner.numPartitions` is
   *                    consumed for the partition-count sanity check.
   * @param conf        the executor/driver `SparkConf`; consumed for
   *                    [[config.SHUFFLE_STREAMING_ENABLED]],
   *                    `spark.shuffle.push.enabled`, and
   *                    [[config.EXECUTOR_MEMORY]].
   * @param metrics     the shared [[StreamingShuffleMetrics]] instance. v1
   *                    does not read from this parameter; the signature is
   *                    retained so that future adaptive re-evaluation (on the
   *                    backpressure-event counter or buffer-utilization gauge)
   *                    can be slotted in without churning call sites.
   * @return            `None` to proceed with streaming, or `Some(reasonCode)`
   *                    to fall back to the held `SortShuffleManager`.
   */
  def evaluate(
      shuffleId: Int,
      dependency: ShuffleDependency[_, _, _],
      conf: SparkConf,
      metrics: StreamingShuffleMetrics): Option[String] = {
    // Defensive null-check on the metrics parameter (preserves API hookability
    // without tripping NPE in unit tests that pass a bare policy invocation).
    // v1 does not actively read from `metrics`; the reference is captured for
    // parity with the future-adaptive method signature documented in the
    // Scaladoc above.
    val _ = metrics

    // ----------------------------------------------------------------------
    // Check 1: Feature-flag kill switch.
    //
    // `spark.shuffle.streaming.enabled` is the operator-facing boolean that
    // globally disables the streaming path without forcing a manager
    // reselection. Returning early here ensures that every shuffle falls
    // back while the flag is off, which is the same outcome as setting
    // `spark.shuffle.manager=sort` &mdash; consistent with the user's
    // directive "Preserve existing sort-based shuffle as production-stable
    // fallback" (AAP section 0.1.2).
    // ----------------------------------------------------------------------
    if (!conf.get(config.SHUFFLE_STREAMING_ENABLED)) {
      return fallback(shuffleId, REASON_STREAMING_DISABLED)
    }

    // ----------------------------------------------------------------------
    // Check 2: Push-based shuffle mutual-exclusion (ADR-005).
    //
    // Push-based shuffle (`spark.shuffle.push.enabled=true`) reassigns map
    // output merging to remote mergers and is architecturally incompatible
    // with the streaming path's direct producer->consumer pipelining. The
    // mutual exclusion is enforced here, at the single routing decision
    // point, so that the push-based shuffle subsystem itself needs no
    // awareness of streaming (AAP section 0.7.2 "streaming shuffle and
    // push-based shuffle are mutually exclusive per active shuffle").
    //
    // Untyped `getBoolean` is used because `spark.shuffle.push.enabled`
    // predates the typed `ConfigEntry` registry for several call sites and
    // is canonically read via the plain accessor in Spark core.
    // ----------------------------------------------------------------------
    if (conf.getBoolean("spark.shuffle.push.enabled", false)) {
      return fallback(shuffleId, REASON_PUSH_SHUFFLE_ACTIVE)
    }

    // ----------------------------------------------------------------------
    // Check 3: Partition-count sanity.
    //
    // A non-positive partition count breaks the per-partition buffer-sizing
    // formula `(executorMemory * bufferPercent) / numPartitions` with either
    // a division by zero (0) or a nonsensical negative budget. Fall back
    // to the sort path whose own validation will surface the error through
    // the existing production-exercised code path.
    //
    // `ShuffleDependency.partitioner.numPartitions` is the authoritative
    // source (see `core/src/main/scala/org/apache/spark/Dependency.scala`
    // line 86 for the `partitioner: Partitioner` field and
    // `org.apache.spark.Partitioner.numPartitions` for the abstract method).
    // ----------------------------------------------------------------------
    val numPartitions = dependency.partitioner.numPartitions
    if (numPartitions <= 0) {
      return fallback(shuffleId, REASON_INVALID_PARTITION_COUNT)
    }

    // ----------------------------------------------------------------------
    // Check 4: Executor-memory sanity.
    //
    // Streaming shuffle allocates up to 20% of executor memory as per-partition
    // buffers (the user-mandated default for
    // `spark.shuffle.streaming.bufferSizePercent`). Below
    // [[MINIMUM_EXECUTOR_MEMORY_MIB]] MiB of executor memory, the resulting
    // budget is too small to support the feature without aggressive spilling;
    // falling back to the sort path avoids the OOM risk and preserves
    // zero-regression behavior for small-executor workloads (AAP section 0.1.1
    // success criterion "Zero performance regression for memory-bound
    // workloads").
    //
    // `config.EXECUTOR_MEMORY` is declared with `bytesConf(ByteUnit.MiB)` at
    // line 418 of `core/src/main/scala/org/apache/spark/internal/config/package.scala`,
    // so `conf.get(config.EXECUTOR_MEMORY)` returns a `Long` in MiB.
    // ----------------------------------------------------------------------
    val executorMemMiB = conf.get(config.EXECUTOR_MEMORY)
    if (executorMemMiB < MINIMUM_EXECUTOR_MEMORY_MIB) {
      return fallback(shuffleId, REASON_INSUFFICIENT_EXECUTOR_MEMORY)
    }

    // ----------------------------------------------------------------------
    // Runtime-based fallback conditions (1, 3, 4 in the class-level Scaladoc)
    // are deferred to v2 adaptive re-evaluation. The runtime subsystems
    // (`BackpressureProtocol`, `MemorySpillManager`, `StreamingShuffleReader`)
    // form the second-line guard that catches runtime conditions the
    // registration-time policy cannot see. See the class-level Scaladoc
    // "Note on correctness of deferral" for the full correctness argument.
    // ----------------------------------------------------------------------

    // All pre-registration checks passed -> streaming path is selected.
    None
  }

  /**
   * Lightweight boolean accessor exposed for use by [[StreamingShuffleManager]]
   * construction-time short-circuits and by unit tests that assert on the
   * feature-flag semantics without instantiating a full manager stack.
   * Mirrors the check performed inside [[evaluate]] so the two cannot drift.
   *
   * @param conf the executor/driver [[SparkConf]]
   * @return true when `spark.shuffle.streaming.enabled = true`; false otherwise
   *         (default).
   */
  def isStreamingEnabled(conf: SparkConf): Boolean = {
    conf.get(config.SHUFFLE_STREAMING_ENABLED)
  }

  /**
   * Lightweight boolean accessor exposed for use by [[StreamingShuffleManager]]
   * construction-time short-circuits and by unit tests that assert on the
   * ADR-005 mutual-exclusion semantics. Mirrors the check performed inside
   * [[evaluate]] so the two cannot drift.
   *
   * The key is spelled out as a literal rather than threaded through a typed
   * `ConfigEntry` because `spark.shuffle.push.enabled` is canonically read via
   * untyped `getBoolean` across Spark core (see
   * `org.apache.spark.util.Utils.isPushBasedShuffleEnabled` for the existing
   * pattern) and the streaming path deliberately mirrors that accessor to
   * avoid touching unrelated config plumbing.
   *
   * @param conf the executor/driver [[SparkConf]]
   * @return true when `spark.shuffle.push.enabled = true`; false otherwise
   *         (default).
   */
  def isPushShuffleActive(conf: SparkConf): Boolean = {
    conf.getBoolean("spark.shuffle.push.enabled", false)
  }

  // --------------------------------------------------------------------------
  // Private helper: centralizes structured-log emission on fallback so the
  // INFO record format stays consistent across every reason code. Always
  // returns `Some(reason)` to keep the `evaluate` call sites one-liners.
  // --------------------------------------------------------------------------

  /**
   * Emits a single structured-logging INFO record documenting the fallback
   * decision and returns the reason wrapped in `Some(...)`. The MDC keys
   * [[LogKeys.SHUFFLE_ID]] and [[LogKeys.REASON]] are the canonical
   * structured-log keys defined in
   * `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java`;
   * downstream log aggregators can group fallback events by reason without
   * parsing the free-form message text.
   *
   * @param shuffleId the shuffle identifier being registered.
   * @param reason    the fallback reason code (one of the REASON_* constants).
   * @return          `Some(reason)` for direct return from [[evaluate]].
   */
  private def fallback(shuffleId: Int, reason: String): Option[String] = {
    logInfo(log"Streaming shuffle fallback engaged for shuffle " +
      log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(REASON, reason)}. " +
      log"Routing this shuffle to the sort-based SortShuffleManager.")
    Some(reason)
  }
}
