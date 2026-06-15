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

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.shuffle.streaming.BackpressureProtocol.StreamKey

/**
 * Executor-only RPC mailbox for the streaming-shuffle backpressure protocol.
 *
 * This endpoint is a deliberately thin "mailbox": it owns no flow-control state and contains no
 * flow-control decision logic. Its only local logic is untrusted-input sanitization -- it
 * validates each inbound message's correlation IDs (and ack byte count) and drops a malformed
 * message without touching protocol state -- and every well-formed message is then forwarded to
 * the [[BackpressureProtocol]] "brain", which holds all per-stream liveness, token-bucket, and
 * timeout state. Keeping the endpoint otherwise logic-free guarantees a single source of truth
 * for backpressure decisions and lets the protocol be unit-tested without a live [[RpcEnv]].
 *
 * Executor-only registration (hard requirement): the backpressure endpoint is registered ONLY on
 * executors, never on the driver. The driver neither produces nor consumes streamed shuffle
 * blocks, so it has no backpressure to coordinate; an endpoint there would be dead weight and
 * could mask misconfiguration. The companion [[BackpressureRpcEndpoint.registerIfExecutor]]
 * factory enforces this by returning `None` on the driver and only calling `rpcEnv.setupEndpoint`
 * on executors (the backpressure RPC endpoint is rejected on the driver and registered on
 * executors only).
 *
 * Thread-safety: as a [[ThreadSafeRpcEndpoint]] the [[RpcEnv]] serializes message delivery, so at
 * most one of `receive` / `receiveAndReply` runs at a time for this instance. The delegated
 * [[BackpressureProtocol]] handlers are themselves lock-free and thread-safe, so no additional
 * synchronization is performed here.
 *
 * No private timer: the periodic timeout scan that drives producer/consumer liveness transitions
 * is owned by [[BackpressureProtocol]] (its `start()` launches one daemon scan thread). This
 * endpoint never schedules its own timer; `onStart` / `onStop` emit a single structured log line
 * each, and the high-frequency `Heartbeat` / `Ack` / `RateLimitRequest` messages are
 * intentionally NOT logged per message. Only the rare `Timeout` message is logged, at warn, so
 * the endpoint stays well within the streaming-shuffle per-executor log budget.
 *
 * Coexistence: this type is constructed only when the streaming shuffle path is active. It has no
 * effect on, and is never instantiated by, the sort-based fallback path; the two backends share
 * no state.
 *
 * @param rpcEnv
 *   the [[RpcEnv]] this endpoint is registered with (an executor environment)
 * @param protocol
 *   the backpressure protocol that every received message is delegated to
 */
private[spark] class BackpressureRpcEndpoint(
    override val rpcEnv: RpcEnv,
    protocol: BackpressureProtocol)
    extends ThreadSafeRpcEndpoint
    with Logging {

  import BackpressureRpcEndpoint._

  /**
   * Logs a single line when the endpoint becomes active. The backpressure timeout scan is owned
   * by [[BackpressureProtocol.start]], so no timer is started here.
   */
  override def onStart(): Unit = {
    logInfo(log"Streaming shuffle backpressure RPC endpoint started on executor")
  }

  /**
   * Handles one-way (fire-and-forget) backpressure messages. Each message's correlation IDs are
   * validated first (non-negative shuffleId, mapId, and reduceId, plus a non-negative ack byte
   * count); a message that fails validation is dropped with a single warn log and does NOT mutate
   * any protocol state, closing the untrusted-input vector (CWE-20) where a malformed message
   * could otherwise create a bogus per-stream entry or retune the shared rate limiter. The rate in
   * a `RateLimitRequest` needs no range check because a non-positive value is the documented
   * "unlimited" sentinel and any positive value is a valid throttle. Valid messages are delegated
   * to the matching [[BackpressureProtocol]] handler. No reply is produced; senders use
   * `RpcEndpointRef.send`.
   */
  override def receive: PartialFunction[Any, Unit] = {
    // Consumer liveness; the protocol refreshes against its own clock (tsNanos is wire-only).
    case Heartbeat(shuffleId, mapId, reduceId, _) =>
      if (validStreamIds(shuffleId, mapId, reduceId)) {
        protocol.onHeartbeat(StreamKey(shuffleId, mapId, reduceId))
      } else {
        logDroppedMessage("Heartbeat", shuffleId, mapId, reduceId)
      }

    // Consumer ack; the protocol decrements the unacked count and refreshes consumer liveness. A
    // negative bytesAcked is meaningless and rejected; zero is allowed (refreshes liveness only).
    case Ack(shuffleId, mapId, reduceId, bytesAcked) =>
      if (validStreamIds(shuffleId, mapId, reduceId) && bytesAcked >= 0L) {
        protocol.onAck(StreamKey(shuffleId, mapId, reduceId), bytesAcked)
      } else {
        logDroppedMessage("Ack", shuffleId, mapId, reduceId)
      }

    // Consumer-requested throttle; the protocol retunes the shared token-bucket rate limiter. Only
    // the IDs are validated: the rate's <= 0 "unlimited" sentinel and positive throttles are both
    // valid and the limiter accepts the full Long range, so there is no insane rate to reject.
    case RateLimitRequest(shuffleId, mapId, reduceId, bytesPerSec) =>
      if (validStreamIds(shuffleId, mapId, reduceId)) {
        protocol.onRateLimitRequest(StreamKey(shuffleId, mapId, reduceId), bytesPerSec)
      } else {
        logDroppedMessage("RateLimitRequest", shuffleId, mapId, reduceId)
      }

    // Rare explicit peer-timeout signal: after validation, deterministically mark the addressed
    // stream timed out so the signal can never be lost to scan timing, then run an opportunistic
    // scan to catch any other streams that have crossed their idle threshold.
    case Timeout(shuffleId, mapId, reduceId) =>
      if (validStreamIds(shuffleId, mapId, reduceId)) {
        logWarning(
          log"Streaming shuffle timeout signal received for " +
            log"shuffle ${MDC(LogKeys.SHUFFLE_ID, shuffleId)} " +
            log"map ${MDC(LogKeys.MAP_ID, mapId)} " +
            log"reduce ${MDC(LogKeys.REDUCE_ID, reduceId)}")
        protocol.markTimedOut(StreamKey(shuffleId, mapId, reduceId))
        protocol.scanForTimeouts(System.nanoTime())
      } else {
        logDroppedMessage("Timeout", shuffleId, mapId, reduceId)
      }
  }

  /**
   * Handles request/response messages. Only a `Ping` liveness probe is supported, answered with
   * `Pong`; this lets callers (and tests) confirm the executor endpoint is registered and
   * reachable. Any other asked message is left unmatched so the [[RpcEnv]] returns a failure to
   * the sender rather than silently hanging.
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Ping =>
      context.reply(Pong)
  }

  /**
   * Validates the correlation IDs carried by every backpressure message. The shuffle, map, and
   * reduce IDs index real Spark identities and are always non-negative, so a negative value marks
   * a malformed (or hostile) message whose [[StreamKey]] would be bogus. Returns `true` only when
   * all three IDs are non-negative.
   */
  private def validStreamIds(shuffleId: Int, mapId: Long, reduceId: Int): Boolean = {
    shuffleId >= 0 && mapId >= 0L && reduceId >= 0
  }

  /**
   * Logs (at warn, once per dropped message) that a malformed backpressure message was discarded
   * without mutating protocol state, tagging it with the message type and the correlation IDs it
   * carried. Malformed control messages are expected to be rare, so a single warn line per drop
   * stays well within the streaming-shuffle per-executor log budget.
   */
  private def logDroppedMessage(
      messageKind: String,
      shuffleId: Int,
      mapId: Long,
      reduceId: Int): Unit = {
    logWarning(
      log"Dropping malformed streaming shuffle backpressure " +
        log"${MDC(LogKeys.CLASS_NAME, messageKind)} message without mutating protocol state: " +
        log"shuffle ${MDC(LogKeys.SHUFFLE_ID, shuffleId)} " +
        log"map ${MDC(LogKeys.MAP_ID, mapId)} " +
        log"reduce ${MDC(LogKeys.REDUCE_ID, reduceId)}")
  }

  /**
   * Logs a single line when the endpoint stops. The protocol's scan thread is torn down by
   * [[BackpressureProtocol.stop]], not here.
   */
  override def onStop(): Unit = {
    logInfo(log"Streaming shuffle backpressure RPC endpoint stopped")
  }
}

/**
 * Companion holding the shared endpoint name, the serializable backpressure message ADT, and the
 * executor-only registration factory.
 */
private[spark] object BackpressureRpcEndpoint {

  /**
   * The [[RpcEnv]] name under which the endpoint is registered. Sourced from
   * [[StreamingShuffleConfig.BACKPRESSURE_ENDPOINT_NAME]] so registration (here) and lookup
   * (`StreamingShuffleManager` / readers) always agree on the same name.
   */
  val ENDPOINT_NAME: String = StreamingShuffleConfig.BACKPRESSURE_ENDPOINT_NAME

  /**
   * Base type for every backpressure control message. Sealed so the message set is closed and
   * known at compile time; `Serializable` so instances travel over the [[RpcEnv]] wire. Each
   * concrete message is a case class/object of primitives, serializable by construction.
   */
  sealed trait BackpressureMessage extends Serializable

  /**
   * Consumer-to-producer liveness heartbeat for a single stream.
   *
   * @param shuffleId
   *   the shuffle the stream belongs to
   * @param mapId
   *   the producing map task id
   * @param reduceId
   *   the consuming reduce partition id
   * @param tsNanos
   *   the sender's `System.nanoTime()` at send time, carried for diagnostics
   */
  case class Heartbeat(shuffleId: Int, mapId: Long, reduceId: Int, tsNanos: Long)
      extends BackpressureMessage

  /**
   * Consumer acknowledgement that `bytesAcked` bytes were received for a stream.
   *
   * @param shuffleId
   *   the shuffle the stream belongs to
   * @param mapId
   *   the producing map task id
   * @param reduceId
   *   the consuming reduce partition id
   * @param bytesAcked
   *   the number of bytes the consumer confirmed receiving
   */
  case class Ack(shuffleId: Int, mapId: Long, reduceId: Int, bytesAcked: Long)
      extends BackpressureMessage

  /**
   * Consumer-requested producer rate adjustment for a stream.
   *
   * @param shuffleId
   *   the shuffle the stream belongs to
   * @param mapId
   *   the producing map task id
   * @param reduceId
   *   the consuming reduce partition id
   * @param bytesPerSec
   *   the requested throttle in bytes per second (0 or less means unlimited)
   */
  case class RateLimitRequest(shuffleId: Int, mapId: Long, reduceId: Int, bytesPerSec: Long)
      extends BackpressureMessage

  /**
   * Explicit timeout notification for a stream. After validation the endpoint marks the addressed
   * stream timed out deterministically (via [[BackpressureProtocol.markTimedOut]]) so the explicit
   * signal cannot be lost to scan timing, and then triggers an on-demand timeout scan in the
   * protocol in addition to the protocol's own periodic scan.
   *
   * @param shuffleId
   *   the shuffle the stream belongs to
   * @param mapId
   *   the producing map task id
   * @param reduceId
   *   the consuming reduce partition id
   */
  case class Timeout(shuffleId: Int, mapId: Long, reduceId: Int) extends BackpressureMessage

  /** Liveness probe answered with [[Pong]] via `receiveAndReply`. */
  case object Ping extends BackpressureMessage

  /** Reply to a [[Ping]] liveness probe. */
  case object Pong extends Serializable

  /**
   * Registers the backpressure endpoint on executors only, enforcing the driver-rejection rule.
   *
   * On the driver this returns `None` and registers nothing: the driver coordinates no streamed
   * shuffle traffic, so it must not host the backpressure endpoint. On an executor this registers
   * a fresh [[BackpressureRpcEndpoint]] under [[ENDPOINT_NAME]] and returns its
   * [[RpcEndpointRef]]. `StreamingShuffleManager` calls this once, passing its own `isDriver`
   * flag.
   *
   * @param rpcEnv
   *   the executor (or driver) [[RpcEnv]]
   * @param isDriver
   *   `true` when running on the driver, in which case registration is rejected
   * @param protocol
   *   the backpressure protocol the endpoint will delegate messages to
   * @return
   *   `Some(ref)` to the registered endpoint on an executor, or `None` on the driver
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
}
