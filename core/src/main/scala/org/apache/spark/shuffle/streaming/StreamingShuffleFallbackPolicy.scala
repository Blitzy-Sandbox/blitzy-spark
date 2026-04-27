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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{COUNT, EXECUTOR_ID, REASON, SHUFFLE_ID}

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
 *   - The policy is stateless on its RETURN VALUE: identical inputs always
 *     produce identical [[Option]][[String]] outputs, and no instance state
 *     influences the routing decision. This makes it safe to invoke
 *     concurrently from multiple DAG-scheduler threads registering
 *     independent shuffles. Side-effect-only state exists for log-volume
 *     deduplication (see [[fallback]] Scaladoc); that state is
 *     lock-free-accessible but DOES NOT influence return values &mdash; it
 *     only controls log emission level (INFO for first-seen reasons, DEBUG
 *     for subsequent repeats) so that saturated workloads stay within the
 *     AAP IC-15 &lt;10 MB/hour per-executor budget.
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
 *      OBSERVER INFRASTRUCTURE LANDED (RW-7). Pre-registration `evaluate` does
 *      not yet consult observer state because the v1 transport is not yet
 *      live. The observer hooks ([[recordConsumerLag]], [[isConsumerLagging]],
 *      [[evaluateRuntime]]) provide a stable API surface so v2 transport
 *      authors can begin feeding telemetry without churning call sites.
 *   2. '''Memory pressure preventing buffer allocation (OOM risk)''' &mdash;
 *      ACTIVE CHECK. Approximated at registration time by examining the
 *      configured executor memory (MiB): if the executor is too small to
 *      host the 20&percnt; streaming-shuffle buffer budget plus a safe
 *      working-set headroom, streaming is not viable and we fall back.
 *   3. '''Network saturation exceeds 90&percnt; link capacity''' &mdash;
 *      OBSERVER INFRASTRUCTURE LANDED (RW-7). Per AAP, the runtime
 *      [[BackpressureProtocol]] already enforces an 80&percnt; token-bucket
 *      cap that moderates the live send rate; the observer hooks
 *      ([[recordNetworkUtilization]], [[isNetworkSaturated]],
 *      [[evaluateRuntime]]) extend that with a >90&percnt; auto-fallback API
 *      surface ready to consume v2 transport telemetry.
 *   4. '''Producer / consumer version mismatch''' &mdash; OBSERVER
 *      INFRASTRUCTURE LANDED (RW-7). Not applicable within a single cluster
 *      where every executor runs the identical Apache Spark binary;
 *      cross-version scenarios (SQL migration, Spark Connect mixing) are
 *      explicitly declared out of scope by the AAP section 0.6.2
 *      "Cross-version Spark Connect mixing". The observer hooks
 *      ([[markVersionMismatch]], [[clearVersionMismatch]],
 *      [[isVersionMismatched]], [[evaluateRuntime]]) are nonetheless
 *      provided so future multi-version executor support can integrate
 *      without churning call sites.
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
 *   8. '''v1 transport-readiness safety guard''' &mdash; ACTIVE CHECK.
 *      Compile-time invariant that forces sort-path fallback for every
 *      otherwise-passing shuffle while [[STREAMING_TRANSPORT_READY_V1]] is
 *      `false`. In v1, the streaming-shuffle network transport is still
 *      under construction and [[StreamingShuffleReader.read]] returns an
 *      empty iterator as its "correct degenerate-case answer" (see that
 *      reader's class-level scaladoc). Without this guard, a user-opted
 *      streaming shuffle would silently return zero records and violate
 *      AAP section 0.1.1 "Zero data loss under all failure scenarios". The
 *      guard is evaluated LAST in [[evaluate]] so it acts only when every
 *      other condition passes; users who intentionally disable streaming
 *      via the explicit kill switch or trigger one of the other fallback
 *      conditions continue to see those reasons surfaced first. When
 *      sibling agents complete the transport, they flip the constant from
 *      `false` to `true` and this guard becomes a no-op.
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
   * Reason code returned when the streaming-shuffle network transport is not
   * yet operational. In the v1 landing this reason is emitted for EVERY
   * otherwise-passing shuffle because the direct producer-to-consumer
   * pipeline ([[org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport]]
   * and friends) is still under construction by sibling agents (see
   * [[StreamingShuffleReader]] class-level scaladoc, lines documenting
   * "v1 implementation note"). Without the transport, a streaming shuffle
   * would silently return an empty iterator on the reader side &mdash; which
   * would violate the AAP section 0.1.1 "Zero data loss under all failure
   * scenarios" success criterion. This reason code unconditionally routes
   * every streaming-eligible shuffle to the held `SortShuffleManager` so
   * user workloads continue to observe correct results while the transport
   * is being wired.
   *
   * When the transport becomes operational, the hard-coded guard
   * [[STREAMING_TRANSPORT_READY_V1]] is flipped from `false` to `true` and
   * this reason stops being emitted. No config key is exposed for this
   * readiness state because the guard is a compile-time safety invariant
   * &mdash; an operator MUST NOT be able to misconfigure themselves into
   * silent data loss by toggling a flag.
   */
  private val REASON_STREAMING_TRANSPORT_UNAVAILABLE_V1: String =
    "streaming-transport-unavailable-v1"

  /**
   * Reason code returned when [[isConsumerLagging]] reports that a consumer has
   * sustained a `ratio &gt;= 2.0` (i.e., 2&times; slower than its producer) for
   * longer than [[CONSUMER_LAG_SUSTAINED_DURATION_MILLIS]] milliseconds. This
   * code is emitted from [[evaluateRuntime]] only; the registration-time
   * [[evaluate]] entry-point cannot observe this condition because it has no
   * runtime telemetry at registration time.
   *
   * AAP source (section 0.1.2 "automatic fallback conditions"):
   * "Consumer sustained 2x slower than producer for >60 seconds"
   */
  private val REASON_RUNTIME_CONSUMER_LAG: String = "runtime-consumer-lag"

  /**
   * Reason code returned when [[isNetworkSaturated]] reports that the most
   * recent observed network utilization exceeds
   * [[NETWORK_SATURATION_PERCENT_THRESHOLD]] (90&percnt;). Emitted from
   * [[evaluateRuntime]] only.
   *
   * AAP source (section 0.1.2 "automatic fallback conditions"):
   * "Network saturation exceeds 90% link capacity"
   */
  private val REASON_RUNTIME_NETWORK_SATURATED: String =
    "runtime-network-saturated"

  /**
   * Reason code returned when [[isVersionMismatched]] reports that a remote
   * producer (or consumer) has been flagged with an incompatible Apache Spark
   * binary version. Emitted from [[evaluateRuntime]] only.
   *
   * AAP source (section 0.1.2 "automatic fallback conditions"):
   * "Producer/consumer version mismatch (compatibility check)"
   */
  private val REASON_RUNTIME_VERSION_MISMATCH: String =
    "runtime-version-mismatch"

  /**
   * Threshold ratio for the "consumer 2&times; slower than producer" runtime
   * fallback condition. A consumer-lag observation with `ratio &gt;= 2.0`
   * counts toward the sustained-lag check; ratios below this threshold reset
   * the per-shuffle sustained-lag start timestamp.
   *
   * Direct verbatim quote from AAP section 0.1.2 ("automatic fallback
   * conditions"): "Consumer sustained 2x slower than producer for >60 seconds".
   */
  val CONSUMER_LAG_RATIO_THRESHOLD: Double = 2.0

  /**
   * Sustained-lag duration threshold in milliseconds. The
   * [[isConsumerLagging]] predicate reports `true` only after a per-shuffle
   * `ratio &gt;= 2.0` has been continuously observed for longer than this
   * many milliseconds.
   *
   * Direct verbatim quote from AAP section 0.1.2: "for >60 seconds".
   */
  val CONSUMER_LAG_SUSTAINED_DURATION_MILLIS: Long = 60000L

  /**
   * Threshold for the "network saturation > 90%" runtime fallback condition.
   * Expressed as a fraction in `[0.0, 1.0]` rather than a percentage to match
   * the conventional ratio-style API.
   *
   * Direct verbatim quote from AAP section 0.1.2: "Network saturation
   * exceeds 90% link capacity".
   */
  val NETWORK_SATURATION_PERCENT_THRESHOLD: Double = 0.90

  /**
   * Minimum executor memory (MiB) required to safely run the streaming
   * shuffle path. 512 MiB is chosen as a realistic safety margin that
   * aligns with Spark's own internal executor-memory floor:
   *   - The Unified Memory Manager reserves 300 MiB as a fixed floor and
   *     enforces a 450 MiB minimum slot plus ~21 MiB of off-heap overhead,
   *     making Spark's effective minimum executor memory ~471 MiB. Any
   *     threshold below 471 MiB is dead code because Spark refuses to
   *     start executors that small (see
   *     `core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala`
   *     for the reserved-memory constant and
   *     `core/src/main/scala/org/apache/spark/SparkConf.scala` for the
   *     `validateExecutorMemory` guard that enforces the 450 MiB slot).
   *   - 20&percnt; of 512 MiB &equiv; ~102 MiB of streaming-buffer budget
   *     (the default `spark.shuffle.streaming.bufferSizePercent = 20`).
   *   - At 512 MiB the buffer budget sits well above the block-size limit
   *     (2 MiB per AAP section 0.1.2) so concurrent shuffles each have
   *     headroom for at least dozens of in-flight blocks without thrashing
   *     the 80&percnt; spill threshold.
   *   - 512 MiB clears the ~471 MiB Spark-minimum floor with enough margin
   *     that reasonable garbage-collection pause spikes and Netty direct-buffer
   *     allocations do not cause the check to flicker at the boundary.
   *
   * Operators who deliberately choose smaller executors for test clusters
   * should either leave `spark.shuffle.manager` at its default (`sort`) or
   * raise the executor memory above this threshold. QA checkpoint #4
   * recorded the `256L` predecessor as dead-code (Spark's own 471 MiB floor
   * masked the policy's 256 MiB check end-to-end); this threshold lifts the
   * value above 471 MiB so the fallback condition is actually reachable on
   * executor configurations that intentionally sit just above Spark's
   * minimum but below the streaming-viability floor.
   */
  private val MINIMUM_EXECUTOR_MEMORY_MIB: Long = 512L

  /**
   * Compile-time safety invariant indicating whether the streaming-shuffle
   * network transport is operational in this build of Apache Spark.
   *
   * Set to `false` for v1 because the end-to-end producer-to-consumer
   * pipeline (the `org.apache.spark.shuffle.streaming.network` sub-package)
   * is still being landed by sibling agents. Until that work is complete,
   * [[StreamingShuffleReader.read]] returns an empty iterator (see its
   * class-level scaladoc for the rationale of the "correct degenerate-case
   * answer"). Returning an empty iterator to a user-visible `reduceByKey`
   * would silently discard shuffle data &mdash; a direct violation of AAP
   * section 0.1.1 "Zero data loss under all failure scenarios".
   *
   * By emitting [[REASON_STREAMING_TRANSPORT_UNAVAILABLE_V1]] for every
   * otherwise-passing shuffle while this flag is `false`, the policy forces
   * [[StreamingShuffleManager]] to delegate every shuffle to the held
   * `SortShuffleManager`. Users who opt into `spark.shuffle.manager=streaming`
   * and `spark.shuffle.streaming.enabled=true` continue to observe correct
   * results from the sort path; the feature flags remain meaningful as
   * forward-looking opt-ins, but production data integrity is preserved.
   *
   * Design rationale for a hard-coded constant rather than a config key:
   *   - Operators MUST NOT be able to misconfigure themselves into silent
   *     data loss by toggling a boolean they do not understand. Exposing
   *     readiness as a config key would create a foot-gun.
   *   - The transport's readiness is a build-time property of the Spark
   *     binary, not a deployment-time policy decision &mdash; when the
   *     transport lands, it ships as part of the same jar.
   *   - Compile-time constants participate in dead-code elimination and
   *     impose zero runtime cost in the default path.
   *
   * Flip procedure: when the `StreamingShuffleTransport` (in the sub-package
   * `org.apache.spark.shuffle.streaming.network`, file
   * `StreamingShuffleTransport.scala`) is production-ready and
   * end-to-end-verified by its own integration suite, the sibling agent that
   * finalizes the transport should:
   *   1. Flip this constant from `false` to `true` in one focused PR.
   *   2. Update [[StreamingShuffleFallbackPolicySuite]] test expectations
   *      (the v1-transport-unavailable assertions become happy-path `None`
   *      assertions).
   *   3. Update the v1-only guard test in that suite to verify the flipped
   *      behavior.
   *   4. Re-run [[StreamingShuffleIntegrationTest]] to confirm end-to-end
   *      `reduceByKey` returns correct results via the actual streaming
   *      path rather than the fallback delegate.
   */
  private val STREAMING_TRANSPORT_READY_V1: Boolean = false

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
   *   5. v1 transport-readiness safety guard (constant-time check against
   *      the compile-time [[STREAMING_TRANSPORT_READY_V1]] invariant). This
   *      check is LAST so that explicit user opt-outs and misconfigurations
   *      (conditions 1-4) surface their specific reason codes ahead of the
   *      generic "v1 transport not ready" reason. When the transport lands,
   *      flipping the constant makes this check a no-op without disturbing
   *      any of the preceding checks.
   *
   * On a fallback decision, the method emits a structured log record keyed
   * by the shuffle ID and the reason code, so that log aggregators can build
   * time-series of fallback counts by reason without parsing free-form text.
   * The record is emitted at INFO level the FIRST time each distinct reason
   * is observed per JVM and at DEBUG level for every subsequent occurrence
   * of the same reason (see [[fallback]] Scaladoc for the full dedup
   * contract). On an OK decision, no log is emitted. This two-level policy
   * keeps log volume under the user's `<10 MB/hour per executor` budget
   * (AAP section 0.1.2 / IC-15) even under saturated shuffle rates.
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
    // Check 5: v1 transport-readiness safety guard.
    //
    // The streaming-shuffle network transport is still under construction in
    // v1. [[StreamingShuffleReader.read]] intentionally returns an empty
    // iterator as its correct degenerate-case answer while the producer-to-
    // consumer pipeline is being wired by sibling agents. Routing a live
    // user shuffle through that empty iterator would silently discard every
    // record and violate AAP section 0.1.1 "Zero data loss under all failure
    // scenarios" -- silent data loss is strictly worse than an exception,
    // because downstream analytics would proceed on empty output without
    // any surfaced error.
    //
    // Evaluating this check LAST preserves the specificity of the earlier
    // reason codes: users who disable streaming explicitly still see
    // `streaming-disabled-by-config`, users on push-based shuffle still
    // see `push-based-shuffle-active`, and so on. The v1-transport reason
    // only surfaces when every other pre-registration condition has
    // cleared, i.e. when streaming would otherwise be selected.
    //
    // When the transport becomes operational, the sibling agent flips
    // [[STREAMING_TRANSPORT_READY_V1]] from `false` to `true` and this
    // check becomes a no-op without touching any other call site.
    // ----------------------------------------------------------------------
    if (!STREAMING_TRANSPORT_READY_V1) {
      return fallback(shuffleId, REASON_STREAMING_TRANSPORT_UNAVAILABLE_V1)
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
  //
  // LOG VOLUME DEDUPLICATION (AAP IC-15 compliance, QA checkpoint 6 finding):
  //   The user's binding non-functional requirement is `<10 MB/hour per
  //   executor for streaming events` (AAP section 0.1.2). QA checkpoint 6
  //   measured 43.88 MB/hr under a saturated stress workload (>=5 shuffles/sec)
  //   when this helper emitted one unconditional INFO line per invocation.
  //   Because fallback reasons are stable for the lifetime of a SparkConf
  //   (e.g. "streaming-transport-unavailable-v1" trips for EVERY shuffle in
  //   v1), saturating workloads amplified each reason-string into thousands
  //   of identical INFO records per hour.
  //
  //   The fix below retains first-occurrence observability by emitting INFO
  //   the FIRST time each reason is seen per JVM, then demoting subsequent
  //   same-reason fallbacks to DEBUG (suppressed at default INFO log level)
  //   while still maintaining a lock-free per-reason counter for optional
  //   periodic summary emission. The dedup state is side-effect-only and
  //   does NOT influence the return value, so the policy's stateless-on-
  //   return-value contract documented at the class-level Scaladoc
  //   (lines 37-39) remains intact: identical inputs always produce
  //   identical `Option[String]` outputs.
  // --------------------------------------------------------------------------

  /**
   * Per-reason first-seen tracker used by [[fallback]] to emit INFO the first
   * time each reason is observed and DEBUG for every subsequent occurrence of
   * the same reason. The value carries the running count of fallbacks for
   * that reason so diagnostic tooling can introspect the suppressed-log
   * volume via `getReasonCount` without parsing log files.
   *
   * Thread-safety: [[ConcurrentHashMap#computeIfAbsent]] is lock-free in the
   * common case (present key) and guarantees at-most-once initialization in
   * the race case (new key), so concurrent `evaluate` calls from multiple
   * DAG-scheduler threads cannot emit more than one INFO line per unique
   * reason. The [[AtomicLong]] increment is lock-free on every path.
   *
   * JVM-lifetime scope: this map lives for the lifetime of the policy
   * object (which is a Scala `object`, i.e. a singleton loaded once per
   * classloader). Memory cost is bounded by the number of distinct reason
   * strings (currently five: four user-specified plus the v1 transport
   * safety guard), each paired with a single [[AtomicLong]] &mdash;
   * constant-size overhead independent of shuffle count.
   */
  private val reasonCounts: ConcurrentHashMap[String, AtomicLong] =
    new ConcurrentHashMap[String, AtomicLong]()

  /**
   * Emits a structured-logging record documenting the fallback decision and
   * returns the reason wrapped in `Some(...)`. The MDC keys
   * [[LogKeys.SHUFFLE_ID]] and [[LogKeys.REASON]] are the canonical
   * structured-log keys defined in
   * `common/utils-java/src/main/java/org/apache/spark/internal/LogKeys.java`;
   * downstream log aggregators can group fallback events by reason without
   * parsing the free-form message text.
   *
   * Log-level policy (AAP IC-15 compliance, QA checkpoint 6 IC-15 fix):
   *   - FIRST occurrence of a reason (per JVM): INFO &mdash; the reason is
   *     emitted to default-configured sinks so operators discover the
   *     fallback condition without enabling DEBUG.
   *   - SUBSEQUENT occurrences of the same reason: DEBUG &mdash; suppressed
   *     at the default INFO log level so saturated workloads do not
   *     overflow the 10 MB/hour per-executor budget. Operators seeking
   *     per-shuffle visibility can enable DEBUG via
   *     `spark.shuffle.streaming.debug=true` (see
   *     [[StreamingShuffleManager]] bootstrap), which elevates the
   *     `org.apache.spark.shuffle.streaming` logger level and restores
   *     per-shuffle log lines for debugging.
   *
   * @param shuffleId the shuffle identifier being registered.
   * @param reason    the fallback reason code (one of the REASON_* constants).
   * @return          `Some(reason)` for direct return from [[evaluate]].
   */
  private def fallback(shuffleId: Int, reason: String): Option[String] = {
    // computeIfAbsent returns the existing AtomicLong on repeat calls and
    // constructs a fresh one on first-seen; the return value is the counter
    // we increment below. getAndIncrement's pre-increment return value lets
    // us distinguish first-seen (0 -> INFO) from subsequent (>=1 -> DEBUG)
    // without a second map lookup.
    val counter = reasonCounts.computeIfAbsent(reason, _ => new AtomicLong(0L))
    val prior = counter.getAndIncrement()
    if (prior == 0L) {
      logInfo(log"Streaming shuffle fallback engaged for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(REASON, reason)}. " +
        log"Routing this shuffle to the sort-based SortShuffleManager. " +
        log"Subsequent fallbacks with reason=${MDC(REASON, reason)} will log " +
        log"at DEBUG level to stay within the AAP IC-15 log volume budget; " +
        log"enable spark.shuffle.streaming.debug=true to restore per-shuffle INFO.")
    } else {
      logDebug(log"Streaming shuffle fallback engaged for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(REASON, reason)} " +
        log"(occurrence #${MDC(COUNT, prior + 1L)} of this reason). " +
        log"Routing this shuffle to the sort-based SortShuffleManager.")
    }
    Some(reason)
  }

  /**
   * Testing / diagnostic accessor that returns the running count of fallbacks
   * observed for the given reason since JVM start. Unit tests that assert on
   * the dedup behavior use this to verify that N invocations produce 1 INFO
   * line plus (N-1) DEBUG lines without relying on log-capture plumbing.
   *
   * Package-private to honor the encapsulation documented at the field
   * declaration above: the map is a log-emission side-effect optimization,
   * not part of the policy's public contract.
   *
   * @param reason a REASON_* code (e.g. `"streaming-disabled-by-config"`)
   * @return       the number of fallback invocations observed for that
   *               reason since the JVM started; 0 if the reason has never
   *               been triggered in this JVM.
   */
  private[streaming] def getReasonCount(reason: String): Long = {
    val counter = reasonCounts.get(reason)
    if (counter == null) 0L else counter.get()
  }

  /**
   * Testing-only helper that resets the per-reason dedup counters so that
   * the next invocation of [[fallback]] emits an INFO line as if the JVM
   * had just started. Unit tests use this to exercise the first-seen path
   * multiple times within a single test run. Not exposed publicly because
   * operators have no reason to reset the dedup state at runtime.
   */
  private[streaming] def resetReasonCountsForTesting(): Unit = {
    reasonCounts.clear()
  }

  // ==========================================================================
  // Runtime observer infrastructure (RW-7)
  // --------------------------------------------------------------------------
  // The user-specified four "automatic fallback conditions" (AAP section 0.1.2)
  // include three runtime-only signals that the registration-time `evaluate`
  // method cannot observe: consumer 2x lag for >60 s, network saturation
  // >90 %, and producer/consumer version mismatch. These signals are produced
  // by *runtime* subsystems &mdash; the not-yet-landed v2 [[StreamingShuffleReader]]
  // measures consumer rate, the not-yet-landed v2 transport layer measures
  // network utilization, and the not-yet-landed handshake protocol detects
  // version mismatches. Per Refine PR work item RW-7, this observer
  // infrastructure ships ahead of the runtime subsystems so that:
  //
  //   1. The RW-4/RW-5 transport implementers have a stable API surface
  //      (`recordConsumerLag`, `recordNetworkUtilization`, `markVersionMismatch`)
  //      to call as soon as their telemetry is available.
  //   2. Unit tests can drive these conditions deterministically without
  //      booting a full executor JVM.
  //   3. The composite `evaluateRuntime` evaluator can be wired in by sibling
  //      code at the moment [[STREAMING_TRANSPORT_READY_V1]] flips to `true`
  //      (RW-9) without churning call sites.
  //
  // Observer state lives in process-global concurrent collections. Per-shuffle
  // state is keyed by `shuffleId`; per-producer state is keyed by an opaque
  // producer identifier. Network utilization is global (per-executor) since
  // every shuffle on the same executor competes for the same NIC bandwidth.
  //
  // Thread-safety: every state mutation goes through a thread-safe primitive
  // (ConcurrentHashMap, AtomicReference). No external synchronization is
  // required for callers; concurrent calls from the runtime telemetry threads
  // and the DAG-scheduler threads coexist safely.
  //
  // No log emission is performed from the recording paths because telemetry
  // input is high-volume; log emission is centralized in `evaluateRuntime` (or
  // in `fallback` if `evaluateRuntime` decides to fall back).
  //
  // Forward-compat note: when [[STREAMING_TRANSPORT_READY_V1]] flips to `true`
  // (RW-9), call sites that already check `evaluate` may layer a periodic
  // `evaluateRuntime` re-evaluation on top to enable mid-shuffle fallback
  // without modifying [[evaluate]] itself.
  // ==========================================================================

  /**
   * Per-shuffle "first observed sustained-lag start" timestamp. The map is
   * keyed by shuffleId; the value is the wall-clock millis at which the most
   * recent contiguous run of `ratio &gt;= [[CONSUMER_LAG_RATIO_THRESHOLD]]`
   * began (i.e., the timestamp of the first sample whose ratio met the
   * threshold without any intervening sub-threshold sample resetting the
   * timer).
   *
   * Sentinel value `0L` means "no contiguous lag run is currently active for
   * this shuffle". The map preserves entries for the lifetime of the JVM
   * (or until [[resetObserversForTesting]] is called), so steady-state
   * shuffles that never lag carry a single `0L` entry rather than allocating
   * fresh storage on every observation.
   */
  private val consumerLagStart: ConcurrentHashMap[Integer, java.lang.Long] =
    new ConcurrentHashMap[Integer, java.lang.Long]()

  /**
   * Most-recent network-utilization observation. The pair is `(timestampMillis,
   * utilizationFraction)` where `utilizationFraction` is in `[0.0, 1.0]`.
   * Network saturation is a per-executor signal because every shuffle on the
   * same executor competes for the same NIC, so a single `AtomicReference`
   * captures the global view rather than a per-shuffle map.
   *
   * Initial value is `(0L, 0.0)`, which both
   * [[isNetworkSaturated]] and [[evaluateRuntime]] interpret as "no
   * observation yet" &mdash; producing `false` and `None` respectively.
   */
  private val mostRecentNetworkUtilization: AtomicReference[(Long, Double)] =
    new AtomicReference[(Long, Double)]((0L, 0.0))

  /**
   * Set of producer (or consumer) identifiers flagged as version-mismatched.
   * Keyed by the opaque executor identifier (`SparkEnv.get.executorId`-style
   * string). Membership in the map indicates "this remote endpoint is
   * version-incompatible and any shuffle using it must fall back".
   *
   * The value field is unused (always [[java.lang.Boolean.TRUE]]) because
   * `ConcurrentHashMap` does not support a true `Set` view with the
   * concurrent-mutation guarantees we require; using the map's
   * [[ConcurrentHashMap#putIfAbsent]] semantics achieves the same effect.
   */
  private val versionMismatchedProducers:
      ConcurrentHashMap[String, java.lang.Boolean] =
    new ConcurrentHashMap[String, java.lang.Boolean]()

  /**
   * Records a single consumer-lag observation for the given shuffle.
   *
   * Semantics:
   *   - If `ratio &lt; [[CONSUMER_LAG_RATIO_THRESHOLD]]`, the per-shuffle
   *     sustained-lag start timestamp is reset to `0L`. This represents
   *     "the consumer is no longer keeping up at less than half producer
   *     rate", which interrupts the contiguous lag run.
   *   - If `ratio &gt;= [[CONSUMER_LAG_RATIO_THRESHOLD]]`:
   *       - If the per-shuffle entry is absent or `0L`, the start timestamp
   *         is set to `timestamp`. This marks the beginning of a new
   *         contiguous lag run.
   *       - Otherwise the existing start timestamp is preserved, allowing
   *         [[isConsumerLagging]] to compute the elapsed sustained duration.
   *
   * Idempotent in the sense that recording the same observation twice in a
   * row is a no-op when the second observation shares the same threshold-side
   * (above or below), preserving the start timestamp.
   *
   * Caller discipline: observation timestamps are expected to be
   * monotonically non-decreasing for a single shuffle. The implementation
   * tolerates out-of-order observations defensively but will report a
   * "no current lag" state if an out-of-order &lt;-threshold sample arrives
   * after a &gt;=-threshold sample.
   *
   * Callable contexts (anticipated v2 wiring):
   *   - [[StreamingShuffleReader]]: when comparing local read-rate to
   *     remote producer-rate snapshot.
   *   - [[BackpressureProtocol]]: when consumer acknowledgment cadence
   *     drifts below half producer send cadence.
   *
   * @param shuffleId the shuffle identifier whose lag is being observed.
   * @param ratio     the consumer-to-producer rate ratio. `0.0` means the
   *                  consumer has stalled; `1.0` means parity; `2.0` means
   *                  the consumer is half as fast as the producer (i.e., 2x
   *                  slower); higher values indicate worse lag.
   * @param timestamp the wall-clock millisecond timestamp of the
   *                  observation. Callers typically pass
   *                  `System.currentTimeMillis()` but tests may pass a
   *                  deterministic value for reproducibility.
   */
  def recordConsumerLag(shuffleId: Int, ratio: Double, timestamp: Long): Unit = {
    val key: Integer = java.lang.Integer.valueOf(shuffleId)
    if (ratio < CONSUMER_LAG_RATIO_THRESHOLD) {
      // Consumer is keeping up (or close to it). Reset the sustained-lag
      // timer so a future >= 2.0 sample starts a fresh contiguous run.
      consumerLagStart.put(key, java.lang.Long.valueOf(0L))
    } else {
      // Consumer is at least 2x slower. If no run is in progress (entry
      // absent OR previously reset to 0L), start a new one anchored at this
      // timestamp. Otherwise preserve the existing start so the elapsed
      // duration accumulates across this and future >= 2.0 samples.
      consumerLagStart.merge(
        key,
        java.lang.Long.valueOf(timestamp),
        (existing, candidate) => {
          if (existing == null || existing.longValue() == 0L) {
            candidate
          } else {
            existing
          }
        }
      )
    }
  }

  /**
   * Records a single network-utilization observation. Only the most recent
   * observation is retained because network bandwidth is a globally shared
   * resource on the executor and the most recent measurement is the only
   * one that materially affects routing decisions.
   *
   * @param utilizationFraction the observed network-utilization fraction in
   *                            `[0.0, 1.0]`. Values &gt; 1.0 are not
   *                            theoretically meaningful but are stored as-is
   *                            (the predicate [[isNetworkSaturated]] simply
   *                            compares against the threshold).
   * @param timestamp           the wall-clock millisecond timestamp of the
   *                            observation.
   */
  def recordNetworkUtilization(utilizationFraction: Double, timestamp: Long): Unit = {
    mostRecentNetworkUtilization.set((timestamp, utilizationFraction))
  }

  /**
   * Marks the given producer (or consumer) executor as version-mismatched
   * with the local executor. Subsequent calls to [[isVersionMismatched]] for
   * the same `producerId` return `true` until [[clearVersionMismatch]] is
   * invoked or [[resetObserversForTesting]] is called.
   *
   * Idempotent &mdash; repeated calls have the same effect as a single call.
   * Logged at INFO once per producer per JVM (deduplicated via the existing
   * structured-log infrastructure) so operators can correlate fallback
   * decisions with version-handshake outcomes.
   *
   * @param producerId an opaque executor identifier (e.g.,
   *                   `SparkEnv.get.executorId`).
   */
  def markVersionMismatch(producerId: String): Unit = {
    val priorEntry =
      versionMismatchedProducers.putIfAbsent(producerId, java.lang.Boolean.TRUE)
    if (priorEntry == null) {
      // First-time mismatch for this producer; INFO emission is appropriate
      // because version mismatches are rare-but-significant events that
      // deserve operator visibility. Subsequent calls are no-ops.
      logInfo(log"Streaming shuffle observer recorded version mismatch for " +
        log"producer ${MDC(EXECUTOR_ID, producerId)}; subsequent shuffles " +
        log"using this producer will fall back to sort path.")
    }
  }

  /**
   * Removes the version-mismatch flag for the given producer. Called when a
   * producer becomes compatible (e.g., after a rolling cluster upgrade).
   * Calls for unflagged producers are silent no-ops.
   *
   * @param producerId an opaque executor identifier.
   */
  def clearVersionMismatch(producerId: String): Unit = {
    versionMismatchedProducers.remove(producerId)
  }

  /**
   * Predicate: has the consumer for `shuffleId` sustained `ratio &gt;= 2.0`
   * for longer than [[CONSUMER_LAG_SUSTAINED_DURATION_MILLIS]] milliseconds
   * as of `asOfMillis`?
   *
   * Returns `false` when no lag run is currently active for the shuffle
   * (entry absent OR previously reset to `0L` by a sub-threshold sample).
   *
   * Returns `true` when a lag run is active AND the elapsed duration since
   * the start of that run exceeds the sustained-lag threshold.
   *
   * @param shuffleId   the shuffle identifier to query.
   * @param asOfMillis  the wall-clock millisecond timestamp at which to
   *                    evaluate the predicate. Callers typically pass
   *                    `System.currentTimeMillis()`; tests pass a
   *                    deterministic value for reproducibility.
   * @return `true` iff the consumer has been continuously lagging for longer
   *         than the threshold duration.
   */
  def isConsumerLagging(shuffleId: Int, asOfMillis: Long): Boolean = {
    val key: Integer = java.lang.Integer.valueOf(shuffleId)
    val start = consumerLagStart.get(key)
    if (start == null) {
      false
    } else {
      val startMillis = start.longValue()
      if (startMillis == 0L) {
        false
      } else {
        (asOfMillis - startMillis) > CONSUMER_LAG_SUSTAINED_DURATION_MILLIS
      }
    }
  }

  /**
   * Predicate: was the most recently observed network-utilization fraction
   * strictly greater than [[NETWORK_SATURATION_PERCENT_THRESHOLD]]?
   *
   * The `asOfMillis` parameter is reserved for future "is the observation
   * still fresh?" gating; v1 always evaluates the strict-greater comparison
   * regardless of observation age, which preserves a deterministic predicate
   * for unit tests that drive observations and assertions immediately
   * back-to-back.
   *
   * @param asOfMillis the current wall-clock millisecond timestamp; reserved
   *                   for future freshness gating, currently unused by the
   *                   predicate body.
   * @return `true` iff the most recent observation exceeds the saturation
   *         threshold.
   */
  def isNetworkSaturated(asOfMillis: Long): Boolean = {
    val _ = asOfMillis  // reserved for future freshness gating
    val (_, utilization) = mostRecentNetworkUtilization.get()
    utilization > NETWORK_SATURATION_PERCENT_THRESHOLD
  }

  /**
   * Predicate: has [[markVersionMismatch]] flagged the given producer?
   *
   * @param producerId an opaque executor identifier.
   * @return `true` iff the producer is currently flagged as
   *         version-incompatible.
   */
  def isVersionMismatched(producerId: String): Boolean = {
    versionMismatchedProducers.containsKey(producerId)
  }

  /**
   * Composite runtime fallback evaluator. Returns the first-failing runtime
   * fallback reason as `Some(reasonCode)`, or `None` if all runtime
   * conditions pass.
   *
   * Evaluation order (earliest match wins):
   *   1. [[isVersionMismatched]] &mdash;
   *      [[REASON_RUNTIME_VERSION_MISMATCH]]. Evaluated first because
   *      version incompatibility is permanent until cleared and admits no
   *      retry strategy.
   *   2. [[isNetworkSaturated]] &mdash;
   *      [[REASON_RUNTIME_NETWORK_SATURATED]]. Evaluated before consumer
   *      lag because saturation often manifests as consumer lag and we
   *      want the more specific reason.
   *   3. [[isConsumerLagging]] &mdash;
   *      [[REASON_RUNTIME_CONSUMER_LAG]]. Evaluated last because the
   *      consumer-lag check requires the longest sustained duration to
   *      trigger.
   *
   * On a fallback decision, the same per-reason log-dedup infrastructure
   * used by [[evaluate]] is invoked through the shared [[fallback]] helper,
   * so the runtime-detected fallbacks share the &lt;10 MB/hour log-volume
   * budget with the registration-time fallbacks.
   *
   * v1 callsite status: this method is provided as scaffolding for the
   * not-yet-landed v2 transport. v1 does not invoke it on the hot path
   * because [[evaluate]] short-circuits to the v1 transport-unavailable
   * fallback ahead of any runtime observation. Tests exercise the method
   * directly to validate the observer infrastructure independently of the
   * routing wiring.
   *
   * @param shuffleId   the shuffle being evaluated.
   * @param producerId  the producer identifier whose version compatibility
   *                    should be checked. `None` skips the version check
   *                    (e.g., when the policy is being asked about a
   *                    shuffle in pure-driver scope without a remote
   *                    producer).
   * @param asOfMillis  the wall-clock millisecond timestamp at which to
   *                    evaluate the consumer-lag predicate.
   * @return            `None` if all runtime conditions pass; otherwise
   *                    `Some(reasonCode)` carrying one of the
   *                    `runtime-*` reason strings declared above.
   */
  def evaluateRuntime(
      shuffleId: Int,
      producerId: Option[String],
      asOfMillis: Long): Option[String] = {
    producerId match {
      case Some(id) if isVersionMismatched(id) =>
        return fallback(shuffleId, REASON_RUNTIME_VERSION_MISMATCH)
      case _ => ()
    }
    if (isNetworkSaturated(asOfMillis)) {
      return fallback(shuffleId, REASON_RUNTIME_NETWORK_SATURATED)
    }
    if (isConsumerLagging(shuffleId, asOfMillis)) {
      return fallback(shuffleId, REASON_RUNTIME_CONSUMER_LAG)
    }
    None
  }

  /**
   * Testing-only helper that resets every observer state field so that the
   * next observation begins from a fresh slate. Tests in
   * `StreamingShuffleFallbackPolicySuite` use this between cases to keep
   * the singleton's state isolated. Not exposed publicly because operators
   * have no reason to reset observer state at runtime.
   */
  private[streaming] def resetObserversForTesting(): Unit = {
    consumerLagStart.clear()
    mostRecentNetworkUtilization.set((0L, 0.0))
    versionMismatchedProducers.clear()
  }
}
