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
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

/**
 * Executor-only, thread-safe RPC endpoint that receives streaming-shuffle backpressure signals
 * from remote executors and forwards them to the local [[BackpressureProtocol]].
 *
 * '''Role.''' This endpoint is a thin decode-and-forward shell. It performs no flow-control logic
 * of its own: it decodes the incoming [[BackpressureRpcEndpoint.BackpressureMessage]] variants
 * (consumer acknowledgments, throttle requests, and heartbeats) and hands them to the protocol,
 * which owns all token-bucket accounting, liveness tracking, and metrics. Keeping the logic out of
 * the endpoint makes both halves independently testable and keeps the wire protocol trivial.
 *
 * '''Thread-safety.''' Because it extends [[ThreadSafeRpcEndpoint]], the RPC dispatcher guarantees
 * that messages are processed one-at-a-time. No locks or synchronization are therefore required
 * inside [[receive]] or [[receiveAndReply]]; the shared [[BackpressureProtocol]] additionally uses
 * lock-free concurrent structures so it tolerates being touched by producer threads as well.
 *
 * '''Executor-only binding.''' This endpoint is bound only on executors, under the name
 * [[BackpressureRpcEndpoint.ENDPOINT_NAME]]; the driver never registers it. Binding is performed by
 * the caller (the streaming shuffle manager / protocol) via `rpcEnv.setupEndpoint`, gated on the
 * process being an executor and on `SparkEnv.get != null` for local-mode safety. This class
 * deliberately does '''not''' call `setupEndpoint` in its constructor so that construction stays
 * side-effect free and the binding lifecycle remains under the caller's control.
 *
 * '''Coexistence and isolation.''' Like every type in the `org.apache.spark.shuffle.streaming`
 * package, this endpoint is created only when the streaming shuffle backend is active
 * (`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`). It touches no
 * existing shuffle code path and has zero effect on the production-stable sort-based shuffle, which
 * remains the default and the fallback.
 *
 * @param rpcEnv   the [[RpcEnv]] this endpoint is registered with (must be the first constructor
 *                 parameter per the Spark [[org.apache.spark.rpc.RpcEndpoint]] convention)
 * @param protocol the local flow-control engine that all decoded messages are forwarded to
 */
@Since("4.2.0")
private[spark] class BackpressureRpcEndpoint(
    override val rpcEnv: RpcEnv,
    protocol: BackpressureProtocol)
  extends ThreadSafeRpcEndpoint with Logging {

  import BackpressureRpcEndpoint._

  /**
   * Handle one-way (fire-and-forget) backpressure messages. Each case simply decodes the payload
   * and forwards it to [[BackpressureProtocol]]; per-message logging is emitted at DEBUG so it can
   * be enabled selectively via `spark.shuffle.streaming.debug` without adding hot-path overhead.
   */
  override def receive: PartialFunction[Any, Unit] = {
    case ConsumerAck(shuffleId, mapId, reduceId, bytesConsumed, seqNumber) =>
      logDebug(s"Received ConsumerAck(shuffleId=$shuffleId, mapId=$mapId, " +
        s"reduceId=$reduceId, bytesConsumed=$bytesConsumed, seqNumber=$seqNumber)")
      protocol.onConsumerAck(shuffleId, mapId, reduceId, bytesConsumed, seqNumber)

    case ThrottleRequest(shuffleId, targetBytesPerSec) =>
      logDebug(s"Received ThrottleRequest(shuffleId=$shuffleId, " +
        s"targetBytesPerSec=$targetBytesPerSec)")
      protocol.onThrottleRequest(shuffleId, targetBytesPerSec)

    case Heartbeat(executorId, timestampMillis) =>
      logDebug(s"Received Heartbeat(executorId=$executorId, " +
        s"timestampMillis=$timestampMillis)")
      protocol.onHeartbeat(executorId, timestampMillis)
  }

  /**
   * Handle request/reply probes. [[GetBackpressureStatus]] returns a snapshot of the protocol's
   * current status (active shuffles and available send credit) for tests and health checks.
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetBackpressureStatus =>
      val (activeShuffles, tokensAvailable) = protocol.status
      logDebug(s"Replying to GetBackpressureStatus with activeShuffles=$activeShuffles, " +
        s"tokensAvailable=$tokensAvailable")
      context.reply(BackpressureStatus(activeShuffles, tokensAvailable))
  }

  /**
   * Log endpoint start. Invoked once by the [[RpcEnv]] after registration; `self` is valid here.
   */
  override def onStart(): Unit = {
    logInfo(s"Started streaming shuffle backpressure endpoint: $ENDPOINT_NAME")
  }

  /**
   * Log endpoint stop. Invoked once by the [[RpcEnv]] during teardown; no messages arrive after.
   */
  override def onStop(): Unit = {
    logInfo(s"Stopped streaming shuffle backpressure endpoint: $ENDPOINT_NAME")
  }
}

/**
 * Companion object for [[BackpressureRpcEndpoint]]. It is the single source of truth for the
 * endpoint name and defines the sealed, serializable message protocol exchanged over the RPC
 * layer. The messages are intentionally small, immutable value types so they serialize cheaply.
 */
@Since("4.2.0")
private[spark] object BackpressureRpcEndpoint {

  /**
   * The name under which this endpoint is registered on every executor. The streaming shuffle
   * manager / protocol references this constant when calling `rpcEnv.setupEndpoint`, and the
   * producer side uses it to resolve the endpoint via `rpcEnv.setupEndpointRef`.
   */
  val ENDPOINT_NAME: String = "streaming-shuffle-backpressure"

  /**
   * Root of the sealed backpressure message hierarchy. Sealing keeps every wire message defined in
   * this file, and extending `Serializable` guarantees the payloads can cross the RPC boundary.
   */
  sealed trait BackpressureMessage extends Serializable

  /**
   * Sent by a consumer to acknowledge receipt of streamed blocks. Triggers producer-side buffer
   * reclamation (within 100 ms) and token-bucket refill in [[BackpressureProtocol.onConsumerAck]].
   *
   * @param shuffleId     the shuffle this acknowledgment belongs to
   * @param mapId         the producer (map task) whose output was consumed
   * @param reduceId      the reduce partition that consumed the data
   * @param bytesConsumed number of bytes drained by the consumer (send credit to release)
   * @param seqNumber     the acknowledged block sequence number (for correlation)
   */
  case class ConsumerAck(
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      bytesConsumed: Long,
      seqNumber: Int)
    extends BackpressureMessage

  /**
   * Sent by a consumer under backpressure to ask the producer to slow down to a target rate.
   *
   * @param shuffleId         the shuffle requesting the throttle
   * @param targetBytesPerSec the desired maximum send rate, in bytes per second
   */
  case class ThrottleRequest(shuffleId: Int, targetBytesPerSec: Long) extends BackpressureMessage

  /**
   * Periodic consumer liveness signal (10-second cadence). Refreshes the consumer's last-seen
   * timestamp so [[BackpressureProtocol]] does not evict it as timed out.
   *
   * @param executorId      the consumer executor id
   * @param timestampMillis the heartbeat time, in epoch milliseconds
   */
  case class Heartbeat(executorId: String, timestampMillis: Long) extends BackpressureMessage

  /**
   * Request/reply probe asking for the current backpressure status. Answered with a
   * [[BackpressureStatus]] via `receiveAndReply`. Primarily used by tests and health checks.
   */
  case object GetBackpressureStatus extends BackpressureMessage

  /**
   * Reply payload for [[GetBackpressureStatus]], carrying a snapshot of the protocol's state.
   *
   * @param activeShuffles  number of shuffles currently under flow control
   * @param tokensAvailable available send credit, in bytes
   */
  case class BackpressureStatus(activeShuffles: Int, tokensAvailable: Long) extends Serializable
}
