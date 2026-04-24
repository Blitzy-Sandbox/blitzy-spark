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

import org.apache.spark.SparkContext
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

/**
 * Executor-side Netty RPC endpoint implementing the consumer&rarr;producer backpressure
 * signaling protocol for the streaming shuffle feature (F-001). Registered against the
 * executor's [[org.apache.spark.rpc.RpcEnv]] under the constant endpoint name
 * [[BackpressureRpcEndpoint.ENDPOINT_NAME]] (`"streaming-shuffle-backpressure"`).
 *
 * == Responsibility ==
 *
 * This endpoint is a thin message-dispatch adapter: it receives the four wire-message
 * types introduced by the streaming shuffle feature and forwards each one to the
 * corresponding method on the sibling [[BackpressureProtocol]] coordinator that owns
 * the actual flow-control state (heartbeat registry, acknowledgment table, token-bucket
 * rate, priority arbitration).
 *
 *   - [[BackpressureRpcEndpoint.HeartbeatMessage]] &rarr;
 *     [[BackpressureProtocol.recordHeartbeat]]
 *   - [[BackpressureRpcEndpoint.AcknowledgmentMessage]] &rarr;
 *     [[BackpressureProtocol.acknowledgeReceipt]]
 *   - [[BackpressureRpcEndpoint.RateLimitMessage]] &rarr;
 *     [[BackpressureProtocol.updateRate]]
 *   - [[BackpressureRpcEndpoint.TimeoutMessage]] &rarr;
 *     [[BackpressureProtocol.unregisterProducer]]
 *
 * Keeping message dispatch decoupled from coordinator state lets the protocol be unit
 * tested in isolation (without an [[RpcEnv]]) and lets the endpoint be unit tested with
 * a mock protocol, matching the user's Implementation Discipline directive: "Isolate
 * streaming logic in dedicated classes with zero cross-contamination into existing
 * shuffle code paths" (AAP section 0.7.1).
 *
 * == Executor-only binding (AAP section 0.7.5) ==
 *
 * This endpoint MUST be registered only on executors, never on the driver. The factory
 * helper [[BackpressureRpcEndpoint.setupOnExecutor]] enforces this invariant by
 * consulting the caller-supplied `executorId`: if it matches
 * [[org.apache.spark.SparkContext.DRIVER_IDENTIFIER]] the factory returns `None` and
 * the endpoint is never constructed. Callers on the driver side are expected to pass
 * their own executor ID through (typically `SparkEnv.get.executorId` at
 * endpoint-wiring time); the factory cannot consult `SparkEnv` directly because
 * driver-side construction may occur before `SparkEnv.get` is available.
 *
 * == Thread-safety ==
 *
 * Extends [[ThreadSafeRpcEndpoint]]: the [[RpcEnv]] dispatches at most one message to
 * this endpoint at a time. Internal handler bodies therefore need no locks &mdash;
 * [[BackpressureProtocol]]'s own thread-safety (lock-free
 * [[java.util.concurrent.ConcurrentHashMap]] state) absorbs concurrent access from the
 * map-side writer, the reduce-side reader, and this endpoint's dispatch thread.
 *
 * == Coexistence strategy ==
 *
 * This class is a BRAND-NEW component introduced by the streaming shuffle feature
 * and lives in the `org.apache.spark.shuffle.streaming` sub-package. No pre-existing
 * Spark class references this endpoint; it is classloaded only when
 * `spark.shuffle.manager=streaming` is active and [[StreamingShuffleManager]]
 * invokes [[setupOnExecutor]]. When the default `spark.shuffle.manager=sort` is in
 * effect, this class is not loaded and the sort-path JVM footprint is unchanged
 * (F-017 MiMa binary compatibility gate).
 *
 * == Binary compatibility (MiMa F-017) ==
 *
 * The class and its companion are `private[spark]` and reside in a brand-new
 * sub-package, so they introduce no public SPI signature and require no entry in
 * `project/MimaExcludes.scala`.
 *
 * == Wire message inventory ==
 *
 * All four message types are declared as nested case classes in the companion
 * [[BackpressureRpcEndpoint]] object. Keeping them companion-nested (instead of
 * top-level) scopes them to this endpoint's namespace and avoids polluting the
 * `org.apache.spark.shuffle.streaming` root with wire-only types.
 *
 * @param rpcEnv the [[RpcEnv]] that hosts this endpoint; required because the
 *               [[org.apache.spark.rpc.RpcEndpoint]] trait exposes `rpcEnv` as an
 *               abstract `val`. Supplied by [[setupOnExecutor]] from
 *               `SparkEnv.get.rpcEnv` at registration time.
 * @param protocol the sibling [[BackpressureProtocol]] coordinator whose four
 *                 mutator methods this endpoint delegates to. Must be non-null;
 *                 null is not a supported operating mode because every received
 *                 message dispatches into it.
 */
private[spark] class BackpressureRpcEndpoint(
    override val rpcEnv: RpcEnv,
    protocol: BackpressureProtocol)
  extends ThreadSafeRpcEndpoint
  with Logging {

  // --------------------------------------------------------------------------
  // Lifecycle hooks.
  //
  // The Spark RPC framework invokes onStart exactly once per endpoint instance,
  // after the instance has been successfully registered with the RpcEnv and
  // before the first message is delivered. onStop is invoked exactly once when
  // the RpcEnv unregisters the endpoint (either explicitly via RpcEnv.stop or
  // implicitly when the RpcEnv shuts down).
  //
  // Both hooks emit a single INFO-level log line so that operators observing
  // executor log aggregation can trivially correlate endpoint lifecycle events
  // with shuffle start/stop boundaries. No side effects occur in these hooks
  // because BackpressureProtocol owns all flow-control state and its own
  // scheduler; this endpoint is a pure message-dispatch adapter.
  // --------------------------------------------------------------------------

  /**
   * Called exactly once by the [[RpcEnv]] after this endpoint is registered and
   * immediately before the first message is dispatched. Emits a single INFO log
   * line confirming executor-side endpoint readiness.
   */
  override def onStart(): Unit = {
    logInfo(log"BackpressureRpcEndpoint started on executor " +
      log"(endpoint=${MDC(LogKeys.ENDPOINT_NAME, BackpressureRpcEndpoint.ENDPOINT_NAME)}).")
  }

  /**
   * Called exactly once by the [[RpcEnv]] when this endpoint is being unregistered
   * (either from an explicit [[RpcEnv.stop]] or from [[RpcEnv.shutdown]]). Emits a
   * single INFO log line confirming endpoint teardown. The companion
   * [[BackpressureProtocol]]'s own `stop()` is NOT invoked from here because the
   * protocol's lifetime is owned by [[StreamingShuffleManager]]; this endpoint
   * merely stops handling messages. De-coupling the two lifecycles prevents the
   * executor's RPC shutdown from inadvertently tearing down flow-control state
   * that other sibling classes (e.g. [[StreamingShuffleWriter]] during final
   * drain) may still be inspecting.
   */
  override def onStop(): Unit = {
    logInfo(log"BackpressureRpcEndpoint stopped " +
      log"(endpoint=${MDC(LogKeys.ENDPOINT_NAME, BackpressureRpcEndpoint.ENDPOINT_NAME)}).")
  }

  // --------------------------------------------------------------------------
  // Fire-and-forget message dispatch.
  //
  // All four streaming-shuffle wire messages are delivered through this handler
  // when the sender uses RpcEndpointRef.send (as opposed to RpcEndpointRef.ask).
  // Fire-and-forget is the expected hot-path dispatch mode: heartbeats and
  // acknowledgments flow at high frequency and don't block the sender for a
  // reply. Rate-limit notifications and timeout notifications are also
  // fire-and-forget because the sender cannot meaningfully act on a reply.
  //
  // Because this endpoint extends ThreadSafeRpcEndpoint, the RpcEnv guarantees
  // at-most-one concurrent invocation of this partial function per endpoint
  // instance. The handler bodies therefore require no local synchronization;
  // the underlying ConcurrentHashMap state in BackpressureProtocol absorbs any
  // cross-thread visibility concerns.
  // --------------------------------------------------------------------------

  /**
   * Partial-function dispatcher for messages delivered via
   * [[org.apache.spark.rpc.RpcEndpointRef.send]]. Each of the four streaming-shuffle
   * wire messages is forwarded to its corresponding coordinator method:
   *
   *   - [[BackpressureRpcEndpoint.HeartbeatMessage]]: producer liveness update;
   *     delegates to [[BackpressureProtocol.recordHeartbeat]]. A DEBUG log line is
   *     emitted with the producer ID (MDC [[LogKeys.EXECUTOR_ID]]) and timestamp
   *     (MDC [[LogKeys.DURATION]]) so operators can trace heartbeat arrival at
   *     DEBUG verbosity; DEBUG is off by default and carries no measurable cost
   *     in production.
   *
   *   - [[BackpressureRpcEndpoint.AcknowledgmentMessage]]: per-block consumer
   *     acknowledgment; delegates to [[BackpressureProtocol.acknowledgeReceipt]].
   *     No log line at this verbosity: acknowledgments flow at the highest
   *     frequency of the four message types (potentially once per 2 MB block)
   *     and the combined log budget for the executor is capped at 10 MB / hour
   *     (AAP section 0.1.2). [[BackpressureProtocol]] itself emits a TRACE line
   *     when `spark.shuffle.streaming.debug=true` is set.
   *
   *   - [[BackpressureRpcEndpoint.RateLimitMessage]]: throttle adjustment;
   *     delegates to [[BackpressureProtocol.updateRate]]. No explicit log here:
   *     [[BackpressureProtocol.updateRate]] already emits an INFO line with the
   *     new rate, and duplicating that at the RPC boundary would inflate the
   *     log volume without adding observability value.
   *
   *   - [[BackpressureRpcEndpoint.TimeoutMessage]]: producer-timeout notification;
   *     delegates to [[BackpressureProtocol.unregisterProducer]]. A WARN log line
   *     is emitted with the producer ID (MDC [[LogKeys.EXECUTOR_ID]]) because
   *     timeouts signal probable upstream failure that an operator should be
   *     made aware of.
   *
   * @return a partial function over `Any` that handles the four wire messages;
   *         messages whose runtime type matches none of the four fall through
   *         to the [[org.apache.spark.rpc.RpcEndpoint]] default handler, which
   *         raises [[org.apache.spark.SparkException]] via `onError`.
   */
  override def receive: PartialFunction[Any, Unit] = {
    case BackpressureRpcEndpoint.HeartbeatMessage(producerId, ts) =>
      // DEBUG-only on the hot path. Structured MDC fields let an operator
      // filter post-hoc by producer or by timestamp when investigating a
      // suspected liveness issue.
      logDebug(log"Heartbeat received from producer=" +
        log"${MDC(LogKeys.EXECUTOR_ID, producerId)} at ts=" +
        log"${MDC(LogKeys.DURATION, ts)}")
      protocol.recordHeartbeat(producerId, ts)

    case BackpressureRpcEndpoint.AcknowledgmentMessage(blockId, consumerPos) =>
      // No log line; acknowledgments are the highest-frequency message type and
      // any log I/O at this site would violate the 10 MB/hour streaming log
      // budget. BackpressureProtocol.acknowledgeReceipt emits a TRACE line when
      // `spark.shuffle.streaming.debug=true`; that is the correct diagnostic
      // surface.
      protocol.acknowledgeReceipt(blockId, consumerPos)

    case BackpressureRpcEndpoint.RateLimitMessage(newRate) =>
      // BackpressureProtocol.updateRate emits its own INFO log line and bumps
      // the shuffle.streaming.backpressureEvents Dropwizard counter. We avoid
      // double-logging here.
      protocol.updateRate(newRate)

    case BackpressureRpcEndpoint.TimeoutMessage(producerId) =>
      // WARN because timeouts indicate probable upstream failure. The structured
      // MDC producer field lets operators correlate the timeout with the
      // failing producer's own executor logs.
      logWarning(log"Producer timeout: producer=" +
        log"${MDC(LogKeys.EXECUTOR_ID, producerId)}")
      protocol.unregisterProducer(producerId)
  }

  // --------------------------------------------------------------------------
  // Request-response message dispatch.
  //
  // In v1, only HeartbeatMessage supports the request-response (ask) pattern.
  // The reply is a constant boolean `true` acknowledging receipt; callers who
  // use RpcEndpointRef.ask can block on this reply to obtain end-to-end
  // delivery confirmation when desired (for example, during unit tests or
  // integration diagnostics). All other message types fall through to the
  // RpcEndpoint default receiveAndReply, which replies with a SparkException
  // indicating the endpoint won't reply; this matches the user-spec intent
  // that Ack/RateLimit/Timeout are strictly fire-and-forget.
  // --------------------------------------------------------------------------

  /**
   * Partial-function dispatcher for messages delivered via
   * [[org.apache.spark.rpc.RpcEndpointRef.ask]]. The only supported request-response
   * in v1 is [[BackpressureRpcEndpoint.HeartbeatMessage]]: the endpoint records the
   * heartbeat into [[BackpressureProtocol]] and then replies with a constant
   * `true` so that the caller obtains explicit delivery confirmation.
   *
   * This is useful for integration tests that need to synchronize on heartbeat
   * receipt and for debug scenarios where end-to-end delivery is itself the
   * subject under test. Production writer/reader code uses the fire-and-forget
   * [[receive]] handler instead.
   *
   * Messages whose runtime type does not match [[BackpressureRpcEndpoint.HeartbeatMessage]]
   * fall through to the [[org.apache.spark.rpc.RpcEndpoint]] default handler,
   * which replies with a [[org.apache.spark.SparkException]] indicating that the
   * endpoint won't reply. This is the correct behavior: Ack/RateLimit/Timeout
   * are fire-and-forget in v1 and any `ask` attempt on those types is a caller
   * bug the framework should surface.
   *
   * @param context the [[RpcCallContext]] through which the reply is sent.
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case BackpressureRpcEndpoint.HeartbeatMessage(producerId, ts) =>
      // Delegate to the coordinator FIRST, then reply, so that the reply is
      // only returned after the heartbeat is durably recorded in the registry.
      // Callers receiving `true` can therefore trust that a subsequent
      // checkProducerTimeouts iteration will see this heartbeat timestamp.
      protocol.recordHeartbeat(producerId, ts)
      context.reply(true)
  }
}

/**
 * Companion object for [[BackpressureRpcEndpoint]]. Holds the endpoint-name constant,
 * the four case-class wire-message types, and the executor-only setup helper.
 *
 * All members are `private[spark]` because streaming shuffle is an internal feature
 * with no public API surface (AAP section 0.7.1). This visibility ensures the feature
 * does not widen Spark's public SPI and therefore does not require entries in
 * `project/MimaExcludes.scala` (F-017 binary compatibility gate).
 */
private[spark] object BackpressureRpcEndpoint {

  /**
   * The canonical name under which this endpoint is registered with the executor's
   * [[RpcEnv]]. The string literal is verbatim from the user specification (AAP
   * section 0.2.3.2, N6) and MUST NOT be changed without updating the accompanying
   * documentation and any cross-executor client that resolves endpoints by name.
   *
   * Consumers looking up the endpoint from another executor/driver should call:
   * {{{
   *   rpcEnv.setupEndpointRef(executorAddress, BackpressureRpcEndpoint.ENDPOINT_NAME)
   * }}}
   */
  val ENDPOINT_NAME: String = "streaming-shuffle-backpressure"

  /**
   * Consumer&rarr;producer heartbeat message carrying the consumer's liveness
   * signal and its current wall-clock timestamp.
   *
   * Expected cadence (AAP section 0.1.2): once every 10 seconds per active
   * producer/consumer pair. The receiving [[BackpressureRpcEndpoint]] forwards
   * the heartbeat to [[BackpressureProtocol.recordHeartbeat]], which refreshes
   * the producer's entry in the liveness registry. Producers whose registry
   * entry goes stale beyond 5 seconds are evicted by
   * [[BackpressureProtocol.checkProducerTimeouts]] on its next scheduled
   * iteration and treated as failed.
   *
   * @param producerId opaque producer identifier (typically the producer's
   *                   executor ID or a task-attempt correlation ID).
   * @param timestamp wall-clock timestamp of the heartbeat in milliseconds
   *                  since the Unix epoch. Callers typically pass
   *                  `System.currentTimeMillis()`.
   */
  case class HeartbeatMessage(producerId: String, timestamp: Long)

  /**
   * Consumer&rarr;producer per-block acknowledgment carrying proof-of-receipt
   * for a specific streaming-shuffle block. The receiving endpoint forwards
   * this to [[BackpressureProtocol.acknowledgeReceipt]], which updates the
   * per-block acknowledgment table used by
   * [[org.apache.spark.shuffle.streaming.StreamingShuffleWriter]] to reclaim
   * the in-memory buffer associated with the acknowledged block within the
   * user-mandated 100 ms reclamation window (AAP section 0.1.1).
   *
   * @param blockId opaque block identifier matching the identifier the
   *                producer sent with the original envelope (typically
   *                `"shuffleId-mapId-reduceId-sequenceNumber"`).
   * @param consumerPos the consumer's current position within the block or
   *                    overall stream at the time of acknowledgment; monotonically
   *                    non-decreasing per block.
   */
  case class AcknowledgmentMessage(blockId: String, consumerPos: Long)

  /**
   * Rate-limit notification instructing the recipient's [[BackpressureProtocol]]
   * to adjust its token-bucket rate to `newRateBytesPerSec`. The receiving
   * endpoint forwards this to [[BackpressureProtocol.updateRate]], which
   * atomically replaces the current rate and increments the
   * `shuffle.streaming.backpressureEvents` Dropwizard counter.
   *
   * Sent by [[org.apache.spark.shuffle.streaming.StreamingShuffleFallbackPolicy]]
   * or by sibling monitoring code on each throttle decision.
   *
   * @param newRateBytesPerSec the new rate in bytes per second. A value of
   *                           [[Double.MaxValue]] is the "no cap" sentinel.
   *                           Callers are responsible for honoring the user-spec
   *                           formula `maxBandwidthMBps / numConcurrentShuffles`
   *                           (AAP section 0.1.2).
   */
  case class RateLimitMessage(newRateBytesPerSec: Double)

  /**
   * Producer-timeout notification instructing the recipient's
   * [[BackpressureProtocol]] to evict the named producer from the liveness
   * registry. The receiving endpoint forwards this to
   * [[BackpressureProtocol.unregisterProducer]], which removes the producer's
   * heartbeat and priority entries.
   *
   * Typically sent by the consumer side after observing a connection-timeout
   * error on the streaming transport, prior to the reader's partial-read
   * invalidation (AAP section 0.1.2, "Failure Handling Protocol").
   *
   * @param producerId opaque producer identifier matching the one used in
   *                   [[HeartbeatMessage.producerId]].
   */
  case class TimeoutMessage(producerId: String)

  /**
   * Idempotent executor-only setup helper. Checks whether the supplied
   * `executorId` identifies the driver; if so, returns [[None]] without
   * registering anything. Otherwise, constructs a new
   * [[BackpressureRpcEndpoint]] wrapping the supplied `protocol` and registers
   * it with the supplied `rpcEnv` under [[ENDPOINT_NAME]].
   *
   * Preserves the AAP section 0.7.5 invariant: "The BackpressureRpcEndpoint
   * MUST be registered only on executors, never on the driver". The driver
   * check is performed against [[org.apache.spark.SparkContext.DRIVER_IDENTIFIER]]
   * (the string literal `"driver"`). Callers supply their own executor ID
   * &mdash; the helper does NOT consult `SparkEnv.get` directly because
   * driver-side construction may legitimately occur before `SparkEnv.get` is
   * usable (for example, in unit tests that build an [[RpcEnv]] without a
   * full `SparkEnv`).
   *
   * Idempotency note: the [[RpcEnv.setupEndpoint]] contract disallows
   * registering two endpoints with the same name; repeated invocation with
   * the same [[ENDPOINT_NAME]] would raise an
   * [[IllegalArgumentException]]. Callers are expected to call this helper
   * exactly once per executor lifetime, typically from
   * [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]] during its
   * executor-side construction.
   *
   * @param rpcEnv the executor's [[RpcEnv]] against which to register the
   *               endpoint.
   * @param protocol the [[BackpressureProtocol]] coordinator the endpoint
   *                 should delegate to.
   * @param executorId the current SparkEnv executor identifier; checked
   *                   against [[SparkContext.DRIVER_IDENTIFIER]] to refuse
   *                   driver-side registration.
   * @return [[Some]] containing the [[RpcEndpointRef]] on successful executor-side
   *         registration, or [[None]] if the current side is the driver.
   */
  def setupOnExecutor(
      rpcEnv: RpcEnv,
      protocol: BackpressureProtocol,
      executorId: String): Option[RpcEndpointRef] = {
    if (executorId == SparkContext.DRIVER_IDENTIFIER) {
      // Driver-side: refuse to register. Returning None lets callers branch on
      // the result and treat absence-of-endpoint as an expected, non-error
      // state on the driver. We intentionally do NOT log here because
      // driver-side invocation is expected to happen once per SparkEnv
      // construction and routinely emitting a log line would inflate the
      // driver log volume without adding operator value.
      None
    } else {
      // Executor-side: register the endpoint and return its ref. RpcEnv
      // guarantees thread-safe registration; we do not need an additional
      // lock or AtomicReference here. The returned ref is immediately usable
      // by clients (cross-executor consumers) via
      // RpcEnv.setupEndpointRef(addr, ENDPOINT_NAME).
      Some(rpcEnv.setupEndpoint(ENDPOINT_NAME, new BackpressureRpcEndpoint(rpcEnv, protocol)))
    }
  }
}
