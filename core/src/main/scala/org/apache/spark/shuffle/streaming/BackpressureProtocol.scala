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
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.util.ThreadUtils

/**
 * Stateful in-JVM flow-control coordinator for the streaming shuffle feature (F-001).
 * Instances are constructed by [[StreamingShuffleManager]] on executor-side initialization
 * (only when `spark.shuffle.manager=streaming`) and stopped on manager shutdown. A
 * BackpressureProtocol owns four responsibilities:
 *
 *   1. '''Per-producer heartbeat tracking''' &mdash; producers (map-side writers) invoke
 *      [[registerProducer]] on shuffle start and [[recordHeartbeat]] at the user-specified
 *      10 second cadence. A single-threaded daemon scheduler runs [[checkProducerTimeouts]]
 *      at the same 10 second cadence; any producer whose most-recent heartbeat is older
 *      than the user-specified 5 second timeout is removed from the registry, at which
 *      point the surrounding [[StreamingShuffleReader]] treats it as failed and triggers
 *      the partial-read-invalidation path that forces upstream recomputation via the
 *      existing DAG scheduler. Both the 5 s timeout and 10 s heartbeat cadence are
 *      verbatim-preserved user requirements (AAP section 0.1.2).
 *   2. '''Per-block acknowledgment table''' &mdash; consumers (reduce-side readers) call
 *      [[acknowledgeReceipt]] once per received envelope with the opaque `blockId` and the
 *      consumer's current position in the shuffle. The writer side uses the latest
 *      position to reclaim buffer memory within the user-mandated 100 ms window (AAP
 *      section 0.1.1).
 *   3. '''Token-bucket rate coordination''' &mdash; the initial rate is derived at
 *      construction from [[config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS]] using the
 *      "80 % of link capacity" cap defined by the user (AAP section 0.1.2). The
 *      [[updateRate]] entry-point permits sibling code (e.g.
 *      [[StreamingShuffleFallbackPolicy]], [[BackpressureRpcEndpoint]]) to downgrade the
 *      rate on throttle signals. '''Actual enforcement lives in the network layer
 *      wrapper `network/TokenBucketRateLimiter.scala`''' which composes with this
 *      coordinator; here we only hold the shared rate state so that all concurrent
 *      shuffles observe the same bound.
 *   4. '''Priority arbitration''' &mdash; [[setProducerPriority]] captures each producer's
 *      weight via the user-specified `partitionCount * dataVolumeBytes` formula (AAP
 *      section 0.1.2), which sibling code reads when scheduling memory to concurrent
 *      shuffles.
 *
 * == Coexistence strategy ==
 *
 * This class is a BRAND-NEW coordinator introduced by the streaming shuffle feature. No
 * pre-existing Spark code refers to it. Its only two callers are siblings in the
 * `org.apache.spark.shuffle.streaming` package &mdash; [[BackpressureRpcEndpoint]] (RPC
 * wrapper for cross-executor signaling) and [[StreamingShuffleWriter]] (direct
 * `acquirePermission` call before each block send). This matches the Implementation
 * Discipline directive "Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths" (AAP section 0.7.1).
 *
 * When `spark.shuffle.manager=sort` (the production-stable default), a
 * BackpressureProtocol is never instantiated and never classloaded, so sort-path
 * executors incur zero overhead and the `SortShuffleManager` behavior remains
 * bit-for-bit unchanged (F-017 MiMa binary compatibility gate).
 *
 * == Thread-safety ==
 *
 * All state is maintained in lock-free data structures so that the hot path (heartbeat
 * recording, acknowledgment processing, priority lookup) stays well below the user's
 * 1 % CPU telemetry budget (AAP section 0.7.4):
 *
 *   - [[producerHeartbeats]], [[ackTable]], [[producerPriorities]] are
 *     [[java.util.concurrent.ConcurrentHashMap]] keyed by `String` and valued by
 *     `java.lang.Long`. Reads are lock-free; writes use lock-striping internal to
 *     ConcurrentHashMap. Wrapper-type `java.lang.Long` avoids Scala/Java boxing drift at
 *     the map boundary.
 *   - [[currentRateBytesPerSec]] is an [[java.util.concurrent.atomic.AtomicReference]]
 *     of `java.lang.Double` so that the rate can be replaced without locks on every
 *     throttle event. Using `AtomicReference[java.lang.Double]` (rather than a bare
 *     `Double`) satisfies the requirement that `Double` is an `AnyVal` and cannot appear
 *     directly in an atomic reference.
 *   - Cumulative diagnostic counters [[heartbeatCount]] and [[acknowledgmentCount]] use
 *     [[java.util.concurrent.atomic.AtomicLong]]; these are internal bookkeeping for
 *     observability and tests, separate from the user-visible
 *     `shuffle.streaming.backpressureEvents` Dropwizard counter emitted through
 *     [[StreamingShuffleMetrics]].
 *
 * == Metrics emission ==
 *
 * The optional [[StreamingShuffleMetrics]] constructor parameter is NULLABLE; driver-side
 * instantiation (or unit tests) may pass `null`. Every site that increments
 * `backpressureEvents` performs a `null` check before calling
 * [[StreamingShuffleMetrics.incrementBackpressureEvents]]. Incrementation happens on two
 * throttle signals, verbatim from the user spec:
 *   - [[updateRate]] &mdash; every rate adjustment is a throttle decision.
 *   - [[checkProducerTimeouts]] &mdash; a producer exceeding the 5 s timeout is evicted
 *     and counted as a throttle event for consumer-facing telemetry.
 *
 * == Binary compatibility (MiMa F-017) ==
 *
 * This class is `private[spark]` and lives in a brand-new sub-package, so it introduces
 * no public SPI signature and requires no entry in `project/MimaExcludes.scala`. See
 * `blitzy-docs/streaming-shuffle-decision-log.md` for the full design rationale
 * (lock-free discipline, nullable-metrics pattern, 80 %-cap derivation).
 *
 * @param conf the [[org.apache.spark.SparkConf]] queried at construction for
 *             [[config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS]]; the configuration is
 *             immutable for the lifetime of this coordinator (reconfiguration requires
 *             executor restart, per AAP section 0.1.2).
 * @param metrics the executor-scoped Dropwizard source for `shuffle.streaming.*`
 *                instruments; MAY be `null` in driver-side or test contexts where
 *                metrics are not registered. All usage sites are guarded by a null
 *                check.
 */
private[spark] class BackpressureProtocol(
    conf: SparkConf,
    metrics: StreamingShuffleMetrics)
  extends Logging {

  // --------------------------------------------------------------------------
  // Constants. Both values are verbatim-preserved user requirements (AAP
  // section 0.1.2, "Implementation constraints") and must not be altered
  // without updating the accompanying documentation and decision log.
  // --------------------------------------------------------------------------

  /**
   * Producer liveness timeout in milliseconds. A producer whose most-recent heartbeat
   * is older than this threshold is treated as failed and removed from the registry
   * on the next scheduled [[checkProducerTimeouts]] invocation. Verbatim user spec:
   * "Connection timeout: 5 seconds for producer failure detection." (AAP section 0.1.2)
   */
  private val timeoutMillis: Long = 5000L

  /**
   * Cadence at which [[checkProducerTimeouts]] runs (and the cadence at which
   * producers are expected to call [[recordHeartbeat]]). Verbatim user spec:
   * "Heartbeat interval: 10 seconds for consumer liveness monitoring." (AAP section 0.1.2)
   * We intentionally schedule the timeout check at the same cadence as the heartbeat
   * so that a producer whose heartbeat drops is detected within at most
   * (heartbeatIntervalMillis + timeoutMillis) = 15 s of its actual failure.
   */
  private val heartbeatIntervalMillis: Long = 10000L

  // --------------------------------------------------------------------------
  // Lock-free state tables. All three maps are keyed by the opaque producer/block
  // identifier (String) and valued by java.lang.Long so that the map contract
  // parameterizes cleanly over JDK types without Scala/Java boxing friction.
  // --------------------------------------------------------------------------

  /**
   * Producer heartbeat registry. Key = opaque producer identifier (typically the
   * producer's executor ID or a task-attempt correlation ID); value = most-recent
   * heartbeat wall-clock timestamp in milliseconds since the Unix epoch.
   *
   * Mutated on the hot path by [[recordHeartbeat]] and [[registerProducer]]; snapshotted
   * once every [[heartbeatIntervalMillis]] by [[checkProducerTimeouts]].
   */
  private val producerHeartbeats: ConcurrentHashMap[String, java.lang.Long] =
    new ConcurrentHashMap[String, java.lang.Long]()

  /**
   * Per-block acknowledgment table. Key = opaque block identifier (typically
   * "shuffleId-mapId-reduceId-sequenceNumber"); value = consumer-reported position for
   * that block. Read by [[StreamingShuffleWriter]] to decide when a buffered block can
   * be reclaimed (per the user-mandated 100 ms reclamation window, AAP section 0.1.1).
   */
  private val ackTable: ConcurrentHashMap[String, java.lang.Long] =
    new ConcurrentHashMap[String, java.lang.Long]()

  /**
   * Per-producer priority weights computed via the user-specified
   * `partitionCount * dataVolumeBytes` formula (AAP section 0.1.2). Read by sibling
   * arbitration code (memory allocator / spill policy) when multiple concurrent
   * shuffles compete for the executor's streaming-buffer budget.
   */
  private val producerPriorities: ConcurrentHashMap[String, java.lang.Long] =
    new ConcurrentHashMap[String, java.lang.Long]()

  /**
   * Current token-bucket rate in bytes per second. Initialized from
   * [[config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS]] (times the 80 % link-capacity cap
   * per AAP section 0.1.2), and atomically replaced by [[updateRate]] on each throttle
   * event. Wrapped as [[java.lang.Double]] (not Scala primitive `Double`) because
   * [[java.util.concurrent.atomic.AtomicReference]]'s type parameter must be an `AnyRef`.
   *
   * A value of [[Double.MaxValue]] is the sentinel for "no cap" (user-facing
   * configuration `spark.shuffle.streaming.maxBandwidthMBps = 0`).
   */
  private val currentRateBytesPerSec: AtomicReference[java.lang.Double] =
    new AtomicReference[java.lang.Double](computeInitialRate())

  // --------------------------------------------------------------------------
  // Internal diagnostic counters. Separate from the user-visible Dropwizard
  // instruments on StreamingShuffleMetrics; these are unit-test hooks and
  // internal bookkeeping. Using AtomicLong (rather than a java.lang.Long inside
  // a ConcurrentHashMap) keeps the increment path at a single CAS, well within
  // the <1 % CPU telemetry budget (AAP section 0.7.4).
  // --------------------------------------------------------------------------

  /**
   * Monotonically-increasing count of heartbeats recorded via [[recordHeartbeat]] since
   * construction. Exposed for read-access via [[heartbeatCountValue]] so that unit
   * tests and integration tests can assert on flow-control activity without scraping
   * Dropwizard. Overflow is theoretically possible but practically impossible
   * (2^63 heartbeats at the 10 s cadence = ~3 * 10^12 years).
   */
  private val heartbeatCount: AtomicLong = new AtomicLong(0L)

  /**
   * Monotonically-increasing count of per-block acknowledgments recorded via
   * [[acknowledgeReceipt]] since construction. Exposed for read-access via
   * [[acknowledgmentCountValue]] so that unit tests can assert on acknowledgment
   * throughput.
   */
  private val acknowledgmentCount: AtomicLong = new AtomicLong(0L)

  // --------------------------------------------------------------------------
  // Scheduler. Single daemon thread named "streaming-shuffle-backpressure"
  // (distinct from the "streaming-shuffle-memory-poll" thread owned by
  // MemorySpillManager) so that thread-dump diagnostics cleanly distinguish
  // backpressure-timeout activity from memory-spill activity.
  // --------------------------------------------------------------------------

  /**
   * Single-threaded daemon scheduler that runs [[checkProducerTimeouts]] at a fixed
   * [[heartbeatIntervalMillis]] cadence. Created eagerly at construction so that the
   * timeout detection is live the moment this coordinator is usable.
   *
   * The thread is daemon so JVM exit is not blocked; [[stop]] additionally calls
   * `shutdownNow()` as a defensive measure.
   */
  private val scheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-backpressure")

  // Start the timeout-check loop immediately. Using scheduleAtFixedRate (rather than
  // scheduleWithFixedDelay) guarantees the 10 s cadence is preserved across drift; if
  // one iteration runs long (for example, during a large producer eviction), the next
  // iteration is scheduled immediately after the previous one returns.
  scheduler.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = checkProducerTimeouts()
    },
    heartbeatIntervalMillis,
    heartbeatIntervalMillis,
    TimeUnit.MILLISECONDS)

  // --------------------------------------------------------------------------
  // Private construction helper.
  // --------------------------------------------------------------------------

  /**
   * Computes the initial token-bucket rate in bytes per second from the user-facing
   * `spark.shuffle.streaming.maxBandwidthMBps` configuration key (wrapped as
   * [[config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS]]).
   *
   * User spec (AAP section 0.1.2):
   *   - Default value 0 means "unlimited"; we encode that as [[Double.MaxValue]] so that
   *     downstream enforcement treats the limit as non-binding.
   *   - Non-zero values are interpreted as MB/s and capped at 80 % of link capacity,
   *     matching the verbatim user statement: "per-executor bandwidth cap at 80% link
   *     capacity via token bucket algorithm" (AAP section 0.2.3.2, N5).
   *
   * @return the initial rate in bytes per second as [[java.lang.Double]]
   */
  private def computeInitialRate(): java.lang.Double = {
    val maxMbps = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
    if (maxMbps <= 0) {
      // 0 (the default) means unlimited. Return Double.MaxValue as the "no cap" sentinel
      // so that downstream comparisons (`if (blockSize <= currentRateBytesPerSec.get)`)
      // always succeed without a separate boolean flag.
      java.lang.Double.valueOf(Double.MaxValue)
    } else {
      // MB -> bytes: multiply by (1024 * 1024). Cap at 80 % per user spec.
      java.lang.Double.valueOf(maxMbps.toDouble * 1024.0 * 1024.0 * 0.80)
    }
  }

  // --------------------------------------------------------------------------
  // Public API. Called by sibling streaming-shuffle classes only; no pre-existing
  // Spark code references this class. See class-level "Coexistence strategy".
  // --------------------------------------------------------------------------

  /**
   * Reserves permission to send a block of the given size on the writer's hot path.
   *
   * '''v1 stub''' &mdash; token-bucket rate limiting enforcement lives in
   * `network/TokenBucketRateLimiter.scala`. Hot-path integration between this
   * coordinator and that rate limiter is deferred to the transport wiring phase
   * (completed by a peer agent). The method signature is preserved here so
   * [[StreamingShuffleWriter]] can call it unconditionally without guarding on
   * whether rate-limiting is wired yet; in v1, every call is a no-op that logs at
   * TRACE level. No throttle events are emitted from this path in v1.
   *
   * Once the transport wiring is complete, the intended body consults
   * [[currentRateBytesPerSec]] and delegates to the network-layer `RateLimiter.acquire`
   * to block until a token is available, then returns.
   *
   * Thread-safety: the v1 stub is trivially thread-safe (side-effect-free aside from
   * TRACE logging).
   *
   * @param blockSize the size of the block the caller intends to send, in bytes
   */
  def acquirePermission(blockSize: Long): Unit = {
    // Trace-level only; TRACE is disabled by default and therefore incurs no overhead
    // in production. Enable via `spark.shuffle.streaming.debug=true` for diagnostic
    // inspection (AAP section 0.1.2).
    if (log.isTraceEnabled) {
      log.trace("acquirePermission(blockSize={}) called; v1 stub returns immediately",
        java.lang.Long.valueOf(blockSize))
    }
  }

  /**
   * Records a consumer-sent acknowledgment for a specific block. The writer side uses
   * the most-recent entry to decide when a buffered block's memory can be reclaimed,
   * honoring the user-mandated 100 ms reclamation window (AAP section 0.1.1).
   *
   * The stored position is guaranteed to be the MAXIMUM of the current value (if any)
   * and the incoming `consumerPos`. Under out-of-order RPC delivery (e.g. a fast
   * network re-ordering a stale acknowledgment behind a newer one) this preserves the
   * monotonic non-decreasing watermark semantics the writer relies on, so buffer
   * reclamation is never rewound. This is the correct semantics because `consumerPos`
   * is monotonically non-decreasing for a given block and the most-recent watermark is
   * the highest one.
   *
   * See decision-log entry D19 in `blitzy-docs/streaming-shuffle-decision-log.md` for
   * the rationale behind choosing `merge` with `Math.max` over unconditional `put`.
   *
   * @param blockId the opaque block identifier matching the one sent by the writer
   * @param consumerPos the consumer's current position within that block (or within
   *                    the overall stream, depending on the caller's convention)
   */
  def acknowledgeReceipt(blockId: String, consumerPos: Long): Unit = {
    // Use `merge` with `Math.max` to guarantee monotonic non-decreasing consumerPos
    // under out-of-order RPC delivery. A BiFunction<Long, Long, Long> is supplied
    // inline; the `ConcurrentHashMap.merge` contract atomically inserts the new value
    // when the key is absent and invokes the merge function otherwise.
    ackTable.merge(
      blockId,
      java.lang.Long.valueOf(consumerPos),
      (existing: java.lang.Long, incoming: java.lang.Long) =>
        java.lang.Long.valueOf(Math.max(existing.longValue(), incoming.longValue())))
    acknowledgmentCount.getAndIncrement()
    if (log.isTraceEnabled) {
      log.trace("acknowledgeReceipt(blockId={}, consumerPos={})",
        blockId, java.lang.Long.valueOf(consumerPos))
    }
  }

  /**
   * Records the start of a new producer session. The current wall-clock time becomes
   * the first heartbeat so that the producer is not immediately evicted by a racing
   * [[checkProducerTimeouts]] iteration. This method must be called before any block
   * is expected from `producerId`; subsequent liveness is maintained by
   * [[recordHeartbeat]].
   *
   * Both the [[producerHeartbeats]] and [[producerPriorities]] registries are populated
   * atomically with respect to this call: the heartbeat receives the current wall-clock
   * time, and the priority is initialized to `0L` (neutral). This preserves the invariant
   * that every producer present in the heartbeat registry also has a valid priority
   * entry, so that priority-arbitration code (e.g. the memory allocator or
   * spill-arbitration policy consuming [[prioritySnapshot]]) never observes a registered
   * producer as "unknown". `unregisterProducer` performs the symmetric cleanup on both
   * tables. Priority is updated to a non-zero value via [[setProducerPriority]] once the
   * partition count and data volume for the producer are known.
   *
   * Idempotent: calling with an already-registered `producerId` refreshes the heartbeat
   * timestamp and re-initializes the priority to `0L`. Callers that have already
   * installed a non-zero priority via [[setProducerPriority]] should NOT re-invoke
   * [[registerProducer]] for the same producer without calling [[setProducerPriority]]
   * again afterward. In practice, registration happens exactly once per producer session
   * before the first [[setProducerPriority]] call, so this caveat does not arise on the
   * steady-state path.
   *
   * See decision-log entry D19 in `blitzy-docs/streaming-shuffle-decision-log.md` for
   * the rationale behind the two-table register/unregister symmetry.
   *
   * @param producerId the opaque producer identifier (typically the producer's
   *                   executor ID)
   */
  def registerProducer(producerId: String): Unit = {
    producerHeartbeats.put(producerId,
      java.lang.Long.valueOf(System.currentTimeMillis()))
    // Initialize the priority entry to the neutral value 0L so that the
    // `(producerHeartbeats, producerPriorities)` pair remains in lock-step with the
    // matching `unregisterProducer` cleanup. Priority-arbitration code can safely
    // assume that every registered producer has a valid priority entry.
    producerPriorities.put(producerId, java.lang.Long.valueOf(0L))
    logDebug(log"Registered streaming shuffle producer " +
      log"${MDC(LogKeys.EXECUTOR_ID, producerId)} for backpressure tracking.")
  }

  /**
   * Removes all coordinator state associated with `producerId`. Called by the writer
   * on shuffle completion (clean teardown) or by [[checkProducerTimeouts]] on timeout.
   * Both the heartbeat registry and the priority table are cleared so that a
   * subsequent [[registerProducer]] with the same ID starts from a clean slate.
   *
   * Note: we do NOT clear [[ackTable]] entries here because acknowledgments are keyed
   * by block ID (not producer ID), and a consumer may legitimately retain
   * acknowledgment records beyond the producer's departure (for example, for
   * diagnostic inspection). The ack table is eventually cleared by [[stop]].
   *
   * Idempotent: safe to call for an already-removed `producerId` (the underlying
   * `ConcurrentHashMap.remove` returns `null` in that case).
   *
   * @param producerId the opaque producer identifier
   */
  def unregisterProducer(producerId: String): Unit = {
    producerHeartbeats.remove(producerId)
    producerPriorities.remove(producerId)
    logInfo(log"Unregistered streaming shuffle producer " +
      log"${MDC(LogKeys.EXECUTOR_ID, producerId)}.")
  }

  /**
   * Refreshes the most-recent heartbeat timestamp for an already-registered producer.
   * Producers are expected to call this method at the user-specified 10 s cadence
   * (AAP section 0.1.2); failure to do so causes [[checkProducerTimeouts]] to evict
   * the producer once its last heartbeat is older than 5 s.
   *
   * If `producerId` has not been [[registerProducer]]-ed, this method still records
   * the heartbeat &mdash; we prefer the lenient "implicit registration" semantics so
   * that out-of-order wire messages (heartbeat arriving before register, possible
   * under network re-ordering) are not dropped.
   *
   * @param producerId the opaque producer identifier
   * @param timestamp the wall-clock timestamp of the heartbeat in milliseconds since
   *                  the Unix epoch; callers typically pass `System.currentTimeMillis()`
   *                  but may pass the sender's clock for cross-executor correlation
   */
  def recordHeartbeat(producerId: String, timestamp: Long): Unit = {
    producerHeartbeats.put(producerId, java.lang.Long.valueOf(timestamp))
    heartbeatCount.getAndIncrement()
    if (log.isTraceEnabled) {
      log.trace("recordHeartbeat(producerId={}, timestamp={})",
        producerId, java.lang.Long.valueOf(timestamp))
    }
  }

  /**
   * Replaces the current token-bucket rate with `newRateBytesPerSec`. Called by
   * sibling throttle logic (fallback policy, RPC endpoint) on every rate-downgrade
   * event. The metric `shuffle.streaming.backpressureEvents` is incremented on every
   * call because every rate update represents a flow-control decision that operators
   * should be able to observe (AAP section 0.1.1).
   *
   * This method is permissive about the argument: negative values are stored as-is
   * (the downstream consumer must clamp or reject) and zero represents a complete
   * stall. Callers are expected to compute the desired rate upstream using the
   * user-specified `maxBandwidthMBps / numConcurrentShuffles` formula (AAP section 0.1.2).
   *
   * @param newRateBytesPerSec the new rate in bytes per second; callers are
   *                           responsible for computing this from the shared
   *                           `maxBandwidthMBps` configuration and the current
   *                           concurrent-shuffle count
   */
  def updateRate(newRateBytesPerSec: Double): Unit = {
    currentRateBytesPerSec.set(java.lang.Double.valueOf(newRateBytesPerSec))
    logInfo(log"Updated streaming shuffle rate to " +
      log"${MDC(LogKeys.NUM_BYTES, newRateBytesPerSec.toLong)} bytes/sec.")
    if (metrics != null) {
      metrics.incrementBackpressureEvents()
    }
  }

  /**
   * Records a producer's arbitration priority using the user-specified priority
   * formula `partitionCount * dataVolumeBytes` (AAP section 0.1.2). Sibling code reads
   * the resulting weight when allocating the executor-wide streaming-buffer budget
   * across concurrent shuffles; higher weights receive larger buffer shares.
   *
   * Overflow protection: `partitionCount` is an `Int` and `dataVolumeBytes` is a `Long`,
   * so the multiplication is performed in `Long` arithmetic after widening
   * `partitionCount` via `.toLong`. For pathological inputs (e.g. partitionCount =
   * 16 777 216 and dataVolumeBytes = 4 GB), the result (~6.7 * 10^16) still fits in
   * `Long.MaxValue` (~9.2 * 10^18) with four orders of magnitude to spare.
   *
   * @param producerId the opaque producer identifier
   * @param partitionCount the number of reduce-side partitions this producer is
   *                       feeding
   * @param dataVolumeBytes the estimated total shuffle output this producer will
   *                        generate, in bytes
   */
  def setProducerPriority(
      producerId: String,
      partitionCount: Int,
      dataVolumeBytes: Long): Unit = {
    val priority = partitionCount.toLong * dataVolumeBytes
    producerPriorities.put(producerId, java.lang.Long.valueOf(priority))
    if (log.isDebugEnabled) {
      log.debug("setProducerPriority(producerId={}, partitionCount={}, " +
        "dataVolumeBytes={}) => priority={}",
        producerId,
        java.lang.Integer.valueOf(partitionCount),
        java.lang.Long.valueOf(dataVolumeBytes),
        java.lang.Long.valueOf(priority))
    }
  }

  /**
   * Stops the scheduled timeout-check thread and clears all coordinator state.
   * Idempotent: safe to call multiple times and safe to call from any thread.
   *
   * After `stop()` returns:
   *   - No further [[checkProducerTimeouts]] iterations will run.
   *   - All heartbeat, acknowledgment, and priority state is cleared, promptly
   *     releasing any Java object references held by the coordinator.
   *   - The token-bucket rate state is preserved (retrievable via
   *     [[currentRateBytesPerSecValue]]) so that post-stop introspection is
   *     possible.
   *
   * Called by [[StreamingShuffleManager.stop]] on manager shutdown (task, stage, or
   * application teardown).
   */
  def stop(): Unit = {
    // shutdownNow() interrupts the poll thread if it is currently running. Pending
    // tasks are discarded because the only pending task is the next iteration of
    // checkProducerTimeouts, which we have no reason to run after stop.
    scheduler.shutdownNow()
    producerHeartbeats.clear()
    ackTable.clear()
    producerPriorities.clear()
    logInfo(log"Stopped BackpressureProtocol; cleared all flow-control state.")
  }

  // --------------------------------------------------------------------------
  // Private timeout-check loop. Runs on the scheduler thread only.
  // --------------------------------------------------------------------------

  /**
   * Scans [[producerHeartbeats]] and evicts any producer whose last heartbeat is
   * older than [[timeoutMillis]] relative to `System.currentTimeMillis()`. Each
   * eviction:
   *   1. Logs a WARN with the evicted producer ID and the observed age (structured
   *      MDC so operators can filter by producer or by duration).
   *   2. Removes the producer from both [[producerHeartbeats]] and
   *      [[producerPriorities]] (mirroring [[unregisterProducer]] semantics).
   *   3. Increments `shuffle.streaming.backpressureEvents` via
   *      [[StreamingShuffleMetrics.incrementBackpressureEvents]] when metrics are
   *      registered (guarded for null).
   *
   * The entire body is wrapped in a `try`/`catch` of `Throwable` so that a pathological
   * iteration error does not silently cancel the recurring schedule. Per
   * [[java.util.concurrent.ScheduledExecutorService.scheduleAtFixedRate]]'s contract,
   * any exception from the task aborts subsequent executions; catching here preserves
   * the cadence even in the face of unexpected JVM errors.
   */
  private def checkProducerTimeouts(): Unit = {
    try {
      val now = System.currentTimeMillis()

      // Snapshot the entry set via .asScala so we can iterate and simultaneously
      // remove entries from the map without invalidating the iterator. The
      // ConcurrentHashMap.entrySet() view is weakly-consistent so direct iteration
      // would technically be safe, but using a materialized list keeps the loop body
      // simpler and more obviously correct.
      val entries = producerHeartbeats.entrySet().asScala.toList

      entries.foreach { entry =>
        val producerId = entry.getKey
        val lastHeartbeat = entry.getValue.longValue()
        val ageMs = now - lastHeartbeat

        if (ageMs > timeoutMillis) {
          // Use remove(key, expected) for safe concurrent eviction: if a late heartbeat
          // arrives between our snapshot and this call, the conditional-remove returns
          // false and we leave the (now-fresh) entry in place.
          val removed = producerHeartbeats.remove(producerId, entry.getValue)
          if (removed) {
            producerPriorities.remove(producerId)
            logWarning(log"Streaming shuffle producer " +
              log"${MDC(LogKeys.EXECUTOR_ID, producerId)} timed out after " +
              log"${MDC(LogKeys.DURATION, ageMs)} ms without a heartbeat; " +
              log"evicting from backpressure tracking.")
            if (metrics != null) {
              metrics.incrementBackpressureEvents()
            }
          }
        }
      }
    } catch {
      case t: Throwable =>
        // ERROR level because the timeout-check loop is critical infrastructure. A
        // consistent ERROR stream signals a real problem that operators should
        // investigate. The schedule continues running (this single iteration's error
        // is swallowed) so transient failures do not permanently disable the
        // coordinator.
        logError(log"BackpressureProtocol timeout-check iteration failed", t)
    }
  }

  // --------------------------------------------------------------------------
  // Package-private read-access helpers for unit tests and sibling-class
  // inspection. These do NOT appear in the members_exposed schema because they
  // are internal observability hooks, not coordination API.
  // --------------------------------------------------------------------------

  /**
   * Read-only view of the producer heartbeat registry. Used by unit tests to assert
   * on the producer set after register / unregister / timeout operations. Returns a
   * snapshot Scala `Map` (not the live ConcurrentHashMap view) so that callers cannot
   * mutate the coordinator state through this handle.
   */
  private[streaming] def heartbeatSnapshot: Map[String, Long] = {
    producerHeartbeats.asScala.iterator
      .map { case (k, v) => (k, v.longValue()) }
      .toMap
  }

  /**
   * Read-only view of the per-block acknowledgment table. Used by unit tests.
   */
  private[streaming] def ackSnapshot: Map[String, Long] = {
    ackTable.asScala.iterator
      .map { case (k, v) => (k, v.longValue()) }
      .toMap
  }

  /**
   * Read-only view of the per-producer priority weights. Used by unit tests.
   */
  private[streaming] def prioritySnapshot: Map[String, Long] = {
    producerPriorities.asScala.iterator
      .map { case (k, v) => (k, v.longValue()) }
      .toMap
  }

  /**
   * Current token-bucket rate in bytes per second, unboxed to a primitive `Double`.
   * [[Double.MaxValue]] indicates "no cap" (unlimited).
   */
  private[streaming] def currentRateBytesPerSecValue: Double =
    currentRateBytesPerSec.get().doubleValue()

  /**
   * Cumulative count of heartbeats recorded since construction. Exposed for test
   * assertions on flow-control activity.
   */
  private[streaming] def heartbeatCountValue: Long = heartbeatCount.get()

  /**
   * Cumulative count of acknowledgments recorded since construction. Exposed for test
   * assertions on acknowledgment throughput.
   */
  private[streaming] def acknowledgmentCountValue: Long = acknowledgmentCount.get()
}
