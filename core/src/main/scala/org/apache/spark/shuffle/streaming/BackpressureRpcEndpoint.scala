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
import org.apache.spark.internal.LogKeys.{MAP_ID, REASON, REDUCE_ID, SHUFFLE_ID}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.shuffle.streaming.BackpressureProtocol.StreamKey

/**
 * Executor-scoped RPC mailbox for the streaming-shuffle backpressure protocol.
 *
 * This endpoint is the only network surface the streaming backpressure channel adds. It receives
 * the consumer-to-producer (and producer-to-consumer) control messages -- heartbeats, acks,
 * rate-limit requests, and timeout notifications -- delivered over the executor [[RpcEnv]] and
 * forwards each one verbatim to the matching [[BackpressureProtocol]] callback. It is a thin
 * mailbox by design: it owns no flow-control state and makes no policy decisions; every decision
 * (throttling, liveness tracking, timeout detection, retransmit scheduling) lives in
 * [[BackpressureProtocol]].
 *
 * ==Scheduler ownership==
 *
 * [[BackpressureProtocol]] owns the single daemon timer that drives the timeout state machine.
 * This endpoint therefore starts no timer of its own; [[onStart]] and [[onStop]] only log, which
 * keeps the protocol's timeout semantics easy to reason about and avoids double-timers.
 *
 * ==Executor-only registration==
 *
 * The backpressure channel is strictly an executor-to-executor concern: the driver neither
 * produces nor consumes shuffle blocks, so it must never register this mailbox. Registration
 * therefore flows exclusively through [[BackpressureRpcEndpoint.registerIfExecutor]], which
 * returns [[scala.None]] on the driver and only calls `setupEndpoint` on executors. See AAP
 * sections 0.3.3 and 0.6.1: "the backpressure RPC endpoint is rejected on the driver and
 * registered on executors only."
 *
 * ==Thread-safety==
 *
 * As a [[ThreadSafeRpcEndpoint]], message processing is serialized by the [[RpcEnv]] -- one
 * message is fully handled before the next begins -- so this endpoint needs no synchronization of
 * its own. The [[BackpressureProtocol]] it forwards to is independently lock-free, so the two
 * compose safely.
 *
 * ==Coexistence with the sort-based shuffle==
 *
 * This endpoint is constructed only on the streaming path and never touches, wraps, or alters the
 * sort-based shuffle. When the streaming backend falls back to `SortShuffleManager`, no messages
 * are sent here and the mailbox simply idles until it is stopped with the streaming manager.
 *
 * @param rpcEnv   the executor RPC environment this endpoint is registered with; satisfies the
 *                 [[org.apache.spark.rpc.RpcEndpoint.rpcEnv]] contract
 * @param protocol the flow-control "brain" that every received message is forwarded to
 */
private[spark] class BackpressureRpcEndpoint(
    override val rpcEnv: RpcEnv,
    protocol: BackpressureProtocol)
  extends ThreadSafeRpcEndpoint with Logging {

  import BackpressureRpcEndpoint._

  /**
   * Logs that the executor-scoped mailbox is live. The endpoint deliberately starts no timer of
   * its own because [[BackpressureProtocol]] owns the single timeout-scan scheduler.
   */
  override def onStart(): Unit = {
    logInfo("Streaming-shuffle backpressure endpoint started on executor.")
  }

  /**
   * Logs shutdown. The shared [[BackpressureProtocol]] lifecycle is owned by
   * `StreamingShuffleManager`, so its scheduler and per-stream state are torn down there, not
   * here.
   */
  override def onStop(): Unit = {
    logInfo("Streaming-shuffle backpressure endpoint stopped on executor.")
  }

  /**
   * Handles fire-and-forget control messages delivered via `RpcEndpointRef.send`. Each
   * [[BackpressureMessage]] is validated and, when well-formed, forwarded to the matching
   * [[BackpressureProtocol]] callback (malformed messages are dropped by [[handle]] without
   * mutating state); this endpoint holds no state and performs no flow-control logic itself.
   */
  override def receive: PartialFunction[Any, Unit] = {
    case message: BackpressureMessage =>
      handle(message)
  }

  /**
   * Handles request/response messages delivered via `RpcEndpointRef.ask`. A [[Ping]] is answered
   * with [[Pong]] as a liveness probe; a [[BackpressureMessage]] is validated and forwarded exactly
   * as in [[receive]], and the reply carries `true` when it was accepted or `false` when validation
   * rejected it, so synchronous callers can confirm delivery (or learn of a drop).
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Ping =>
      context.reply(Pong)
    case message: BackpressureMessage =>
      // Reply true when the message was accepted and forwarded, false when validation rejected it,
      // so a synchronous caller learns its control message was dropped without any state mutation.
      context.reply(handle(message))
  }

  /**
   * Validates and then forwards a single control message to the corresponding
   * [[BackpressureProtocol]] method. Validation ([[BackpressureRpcEndpoint.validate]]) runs FIRST:
   * a malformed message (negative stream coordinates or a negative ack byte count) is logged once
   * at WARN and dropped WITHOUT touching the protocol, so a crafted or corrupt message can neither
   * create bogus per-stream state (the protocol indexes state via `computeIfAbsent`) nor alter the
   * executor's shared rate cap. A well-formed message is dispatched over the exhaustive sealed
   * [[BackpressureMessage]] hierarchy.
   *
   * There is no dedicated `onTimeout` hook on the protocol -- it detects stalls itself through
   * its background scan -- so an inbound [[Timeout]] (the peer has declared the stream dead past
   * recovery) is forwarded to [[BackpressureProtocol.unregisterStream]], releasing the stream's
   * backpressure state and relaxing the shared rate cap.
   *
   * @param message the decoded control message to dispatch
   * @return `true` if the message was well-formed and forwarded, `false` if it was rejected
   */
  private def handle(message: BackpressureMessage): Boolean = {
    validate(message) match {
      case Some(reason) =>
        // Reject malformed control messages WITHOUT mutating any protocol or rate-limit state, so a
        // crafted or corrupt message can neither create bogus per-stream state nor alter the
        // executor's shared rate cap. Logged once at WARN for observability; otherwise a no-op.
        logWarning(log"Rejecting malformed backpressure message: ${MDC(REASON, reason)} " +
          log"shuffle=${MDC(SHUFFLE_ID, message.shuffleId)} " +
          log"map=${MDC(MAP_ID, message.mapId)} " +
          log"reduce=${MDC(REDUCE_ID, message.reduceId)}")
        false
      case None =>
        traceMessage(message)
        message match {
          case Heartbeat(shuffleId, mapId, reduceId, _) =>
            protocol.onHeartbeat(StreamKey(shuffleId, mapId, reduceId))
          case Ack(shuffleId, mapId, reduceId, bytesAcked) =>
            protocol.onAck(StreamKey(shuffleId, mapId, reduceId), bytesAcked)
          case RateLimitRequest(shuffleId, mapId, reduceId, bytesPerSec) =>
            protocol.onRateLimitRequest(StreamKey(shuffleId, mapId, reduceId), bytesPerSec)
          case Timeout(shuffleId, mapId, reduceId) =>
            protocol.unregisterStream(StreamKey(shuffleId, mapId, reduceId))
        }
        true
    }
  }

  /**
   * Emits one structured trace line per message carrying the stream-identifying MDC keys.
   * Guarded by `isTraceEnabled` so the hot heartbeat/ack path pays nothing when tracing is off,
   * keeping the endpoint within the streaming backend's strict per-executor log-volume budget.
   *
   * @param message the control message being dispatched
   */
  private def traceMessage(message: BackpressureMessage): Unit = {
    if (log.isTraceEnabled) {
      logTrace(log"Backpressure endpoint dispatching for stream " +
        log"shuffle=${MDC(SHUFFLE_ID, message.shuffleId)} " +
        log"map=${MDC(MAP_ID, message.mapId)} " +
        log"reduce=${MDC(REDUCE_ID, message.reduceId)}")
    }
  }
}

/**
 * Companion object holding the shared endpoint name, the executor-only registration helper that
 * enforces the driver-rejection rule, and the serializable control-message ADT exchanged over the
 * backpressure RPC channel.
 */
private[spark] object BackpressureRpcEndpoint {

  /**
   * The name under which the single backpressure endpoint registers on the executor [[RpcEnv]],
   * sourced from [[StreamingShuffleConfig.BACKPRESSURE_ENDPOINT_NAME]] so producer and consumer
   * sides resolve the same mailbox: `"streaming-shuffle-backpressure"`.
   */
  val ENDPOINT_NAME: String = StreamingShuffleConfig.BACKPRESSURE_ENDPOINT_NAME

  /**
   * Registers the backpressure endpoint -- but only on executors.
   *
   * The streaming backpressure channel is an executor-to-executor concern; the driver neither
   * produces nor consumes shuffle blocks, so registering a mailbox there would be dead weight and
   * is explicitly rejected (AAP sections 0.3.3 and 0.6.1). On the driver this returns
   * [[scala.None]] and no endpoint is created; on an executor it registers a fresh
   * [[BackpressureRpcEndpoint]] under [[ENDPOINT_NAME]] and returns its [[RpcEndpointRef]].
   *
   * `StreamingShuffleManager` calls this once with its own `isDriver` flag, so the
   * driver-rejection rule is honored at the single point of registration.
   *
   * @param rpcEnv   the RPC environment to register against
   * @param isDriver whether the calling process is the driver; when true, registration is skipped
   * @param protocol the flow-control protocol the registered mailbox forwards messages to
   * @return `Some(ref)` on an executor, or [[scala.None]] on the driver
   */
  def registerIfExecutor(
      rpcEnv: RpcEnv,
      isDriver: Boolean,
      protocol: BackpressureProtocol): Option[RpcEndpointRef] = {
    if (isDriver) {
      None
    } else {
      Some(rpcEnv.setupEndpoint(ENDPOINT_NAME, new BackpressureRpcEndpoint(rpcEnv, protocol)))
    }
  }

  /**
   * Validates an inbound control message's field values BEFORE any protocol state is mutated. This
   * is the executor RPC channel's input-validation gate: it is a pure function (no side effects,
   * no protocol access) so it is unit-testable in isolation, and [[BackpressureRpcEndpoint.handle]]
   * consults it on every received message.
   *
   * The protocol indexes per-stream state by `(shuffleId, mapId, reduceId)` through a
   * `computeIfAbsent`, so a negative or otherwise impossible coordinate would silently create bogus
   * state -- and a malformed rate-limit request could even lower the executor's shared send cap.
   * Spark shuffle ids, map task-attempt ids, and reduce partition ids are all non-negative, and a
   * consumer cannot acknowledge a negative number of bytes, so those are rejected here.
   * `Heartbeat.tsNanos` is diagnostic-only, and `RateLimitRequest.bytesPerSec` is intentionally
   * unrestricted -- a positive value caps the rate and a non-positive value withdraws the cap
   * (clamped by [[BackpressureProtocol.onRateLimitRequest]]) -- so neither carries an out-of-domain
   * value to reject.
   *
   * @param message the decoded control message to validate
   * @return [[scala.None]] if the message is well-formed, or `Some(reason)` naming the first
   *         violation found, which the endpoint logs and uses to reject the message unmutated
   */
  def validate(message: BackpressureMessage): Option[String] = {
    if (message.shuffleId < 0) {
      Some(s"negative shuffleId ${message.shuffleId}")
    } else if (message.mapId < 0L) {
      Some(s"negative mapId ${message.mapId}")
    } else if (message.reduceId < 0) {
      Some(s"negative reduceId ${message.reduceId}")
    } else {
      message match {
        case Ack(_, _, _, bytesAcked) if bytesAcked < 0L =>
          Some(s"negative bytesAcked $bytesAcked")
        case _ =>
          None
      }
    }
  }

  /**
   * Base type of the control messages this endpoint forwards to [[BackpressureProtocol]]. Every
   * message carries the (shuffleId, mapId, reduceId) identity of the stream it concerns, so the
   * endpoint can rebuild a [[StreamKey]] and emit uniform structured logs. The concrete subtypes
   * are case classes of primitives and so are [[Serializable]] for transport over the RPC layer.
   */
  sealed trait BackpressureMessage extends Serializable {
    /** The shuffle this stream belongs to. */
    def shuffleId: Int
    /** The producing map task's attempt id. */
    def mapId: Long
    /** The consuming reduce partition. */
    def reduceId: Int
  }

  /**
   * Producer-liveness signal observed at the consumer (an idle keep-alive or an inbound block);
   * refreshes the producer-timeout clock via [[BackpressureProtocol.onHeartbeat]].
   *
   * @param shuffleId the shuffle this stream belongs to
   * @param mapId     the producing map task's attempt id
   * @param reduceId  the consuming reduce partition
   * @param tsNanos   the sender's `System.nanoTime()` stamp, carried for diagnostics
   */
  final case class Heartbeat(shuffleId: Int, mapId: Long, reduceId: Int, tsNanos: Long)
    extends BackpressureMessage

  /**
   * Consumer acknowledgement of received bytes; refreshes the consumer-timeout clock and
   * decrements the unacked-byte tally via [[BackpressureProtocol.onAck]].
   *
   * @param shuffleId  the shuffle this stream belongs to
   * @param mapId      the producing map task's attempt id
   * @param reduceId   the consuming reduce partition
   * @param bytesAcked bytes the consumer acknowledges; non-positive only refreshes liveness
   */
  final case class Ack(shuffleId: Int, mapId: Long, reduceId: Int, bytesAcked: Long)
    extends BackpressureMessage

  /**
   * Consumer request to cap the producer's send rate; applied via
   * [[BackpressureProtocol.onRateLimitRequest]], which arbitrates the lowest positive request
   * across concurrent shuffles.
   *
   * @param shuffleId   the shuffle this stream belongs to
   * @param mapId       the producing map task's attempt id
   * @param reduceId    the consuming reduce partition
   * @param bytesPerSec the requested ceiling in bytes/second; a non-positive value withdraws it
   */
  final case class RateLimitRequest(shuffleId: Int, mapId: Long, reduceId: Int, bytesPerSec: Long)
    extends BackpressureMessage

  /**
   * Notification that the peer has declared the stream timed out past recovery. Because the
   * protocol detects timeouts itself, this inbound signal is forwarded to
   * [[BackpressureProtocol.unregisterStream]] to release the stream's state and relax the cap.
   *
   * @param shuffleId the shuffle this stream belongs to
   * @param mapId     the producing map task's attempt id
   * @param reduceId  the consuming reduce partition
   */
  final case class Timeout(shuffleId: Int, mapId: Long, reduceId: Int)
    extends BackpressureMessage

  /** Liveness probe sent via `ask`; answered with [[Pong]]. */
  case object Ping extends Serializable

  /** Reply to a [[Ping]] liveness probe. */
  case object Pong extends Serializable
}
