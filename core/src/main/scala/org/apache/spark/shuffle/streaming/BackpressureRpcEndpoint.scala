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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

/**
 * Executor-only [[ThreadSafeRpcEndpoint]] that carries the streaming-shuffle backpressure
 * [[BackpressureMessage]] envelopes across executor boundaries and routes each one to the
 * supplied [[BackpressureProtocol]] (F-108).
 *
 * '''Registered only on executors; the driver never hosts this endpoint.''' The streaming
 * shuffle manager (F-101) constructs and registers this endpoint exclusively when
 * `!isDriver && SparkEnv.get != null`. Use the [[BackpressureRpcEndpoint.register]] factory,
 * which enforces that contract by returning `None` on the driver. The endpoint always registers
 * under the exact name [[BackpressureRpcEndpoint.ENDPOINT_NAME]]
 * (`"streaming-shuffle-backpressure"`).
 *
 * '''Thread-safety.''' As a [[ThreadSafeRpcEndpoint]] the `RpcEnv` serializes message delivery:
 * processing of one message happens-before processing of the next, so the routing logic in
 * [[receive]]/[[receiveAndReply]] needs no manual synchronization. The observable counters are
 * nonetheless backed by [[java.util.concurrent.atomic.AtomicLong]] purely so that external
 * readers (the manager, metrics, and tests) observe up-to-date values from other threads.
 *
 * '''Message routing.'''
 *  - [[Ack]] advances the protocol's monotonic acknowledgment watermark via
 *    [[BackpressureProtocol.mergeAck]] and returns the consumer-reclaimed bytes to the credit
 *    window via [[BackpressureProtocol.refill]].
 *  - [[Heartbeat]] refreshes flow-control liveness via [[BackpressureProtocol.recordHeartbeat]].
 *  - [[RateUpdate]] is recorded as an advisory whose byte-rate is exposed through
 *    [[lastAdvisedRateBytesPerSec]]. The composed rate limiter is immutable for the application
 *    lifetime in v1 (no dynamic reconfiguration), so no live re-rating is performed here.
 *  - [[Timeout]] is surfaced at warn because it signals an unresponsive peer that may drive a
 *    partial-read invalidation or a fallback to the sort-based shuffle.
 *
 * Unexpected messages are never swallowed silently: they are logged at warn and, for the
 * request/reply path, answered with a [[org.apache.spark.SparkException]] failure.
 *
 * @param rpcEnv   the [[RpcEnv]] this endpoint is registered to (the abstract SPI member)
 * @param protocol the flow-control protocol (F-107) every received message is routed to
 * @param debug    when `true` (mirrors `spark.shuffle.streaming.debug`) per-message routing is
 *                 logged at info instead of debug
 */
private[spark] class BackpressureRpcEndpoint(
    override val rpcEnv: RpcEnv,
    protocol: BackpressureProtocol,
    debug: Boolean = false)
  extends ThreadSafeRpcEndpoint with Logging {

  import BackpressureRpcEndpoint.{ENDPOINT_NAME, UNSET_RATE}

  // Observable counters. Updated only inside the serialized message handlers, but read by other
  // threads (manager/metrics/tests), so atomics guarantee cross-thread visibility.
  private val heartbeatCount = new AtomicLong(0L)
  private val ackCount = new AtomicLong(0L)
  private val rateUpdateCount = new AtomicLong(0L)
  private val timeoutCount = new AtomicLong(0L)
  private val lastAdvisedRate = new AtomicLong(UNSET_RATE)

  /** The number of [[Heartbeat]] messages this endpoint has routed. */
  def heartbeatsReceived: Long = heartbeatCount.get()

  /** The number of [[Ack]] messages this endpoint has routed. */
  def acksReceived: Long = ackCount.get()

  /** The number of [[RateUpdate]] advisories this endpoint has recorded. */
  def rateUpdatesReceived: Long = rateUpdateCount.get()

  /** The number of [[Timeout]] messages this endpoint has observed. */
  def timeoutsReceived: Long = timeoutCount.get()

  /**
   * The byte-rate of the most recently received [[RateUpdate]] advisory, or
   * [[BackpressureRpcEndpoint.UNSET_RATE]] (`-1`) if none has been received. Exposed for
   * observability; the actual rate limiter is immutable in v1.
   */
  def lastAdvisedRateBytesPerSec: Long = lastAdvisedRate.get()

  /**
   * Emit a per-message routing log line. When the streaming-shuffle `debug` flag is enabled the
   * line is logged at info so operators see it without lowering the global log level; otherwise
   * it is logged at debug. The message is built lazily, so it costs nothing when suppressed.
   */
  private def logRouted(message: => String): Unit = {
    if (debug) {
      logInfo(message)
    } else {
      logDebug(message)
    }
  }

  /**
   * Route a single [[BackpressureMessage]] to the appropriate [[BackpressureProtocol]] action.
   *
   * The match is exhaustive over the sealed [[BackpressureMessage]] hierarchy, so adding a new
   * subtype to the ADT will surface here as a compile-time obligation.
   */
  private def handleMessage(message: BackpressureMessage): Unit = message match {
    case Ack(shuffleId, partitionId, seqNo, reclaimedBytes) =>
      protocol.mergeAck(seqNo)
      if (reclaimedBytes > 0L) {
        protocol.refill(reclaimedBytes)
      }
      ackCount.incrementAndGet()
      logRouted(s"$ENDPOINT_NAME routed Ack(shuffle=$shuffleId, partition=$partitionId, " +
        s"seqNo=$seqNo, reclaimedBytes=$reclaimedBytes); " +
        s"ackWatermark=${protocol.ackWatermark}, availableCredits=${protocol.availableCredits}")

    case Heartbeat(executorId, shuffleId, timestampMs) =>
      protocol.recordHeartbeat()
      heartbeatCount.incrementAndGet()
      logRouted(s"$ENDPOINT_NAME routed Heartbeat(executor=$executorId, shuffle=$shuffleId, " +
        s"timestampMs=$timestampMs)")

    case RateUpdate(shuffleId, partitionId, maxBytesPerSec) =>
      lastAdvisedRate.set(maxBytesPerSec)
      rateUpdateCount.incrementAndGet()
      // v1 keeps configuration immutable for the application lifetime; the advisory is recorded
      // for observability rather than mutating the composed (immutable) rate limiter.
      logRouted(s"$ENDPOINT_NAME recorded RateUpdate advisory(shuffle=$shuffleId, " +
        s"partition=$partitionId, maxBytesPerSec=$maxBytesPerSec); rate is immutable in v1")

    case Timeout(shuffleId, partitionId, reason) =>
      timeoutCount.incrementAndGet()
      // A timeout signals an unresponsive peer (potential partial-read invalidation / fallback),
      // so it is surfaced at warn regardless of the debug flag.
      logWarning(s"$ENDPOINT_NAME received Timeout(shuffle=$shuffleId, partition=$partitionId, " +
        s"reason=$reason); total timeouts observed=${timeoutCount.get()}")
  }

  /**
   * Handle fire-and-forget (`RpcEndpointRef.send`) backpressure messages. Any non-backpressure
   * message is logged at warn rather than throwing, so a stray message can never tear down the
   * endpoint.
   */
  override def receive: PartialFunction[Any, Unit] = {
    case message: BackpressureMessage =>
      handleMessage(message)

    case other =>
      logWarning(s"$ENDPOINT_NAME ignoring unexpected fire-and-forget message of type " +
        s"${other.getClass.getName}")
  }

  /**
   * Handle request/reply (`RpcEndpointRef.ask`) backpressure messages. Every backpressure message
   * is routed exactly as in [[receive]] and then acknowledged with `true` (a delivery ack, e.g. a
   * heartbeat ack-back). Unexpected messages are logged at warn and answered with a failure so
   * the caller's future completes rather than hanging.
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message: BackpressureMessage =>
      handleMessage(message)
      context.reply(true)

    case other =>
      logWarning(s"$ENDPOINT_NAME received unexpected request-reply message of type " +
        s"${other.getClass.getName}; replying with failure")
      context.sendFailure(new SparkException(
        s"$ENDPOINT_NAME does not handle messages of type ${other.getClass.getName}"))
  }

  /** Log endpoint startup. `self` is valid from this point until [[onStop]]. */
  override def onStart(): Unit = {
    logDebug(s"$ENDPOINT_NAME started (executor-only backpressure endpoint)")
  }

  /**
   * Release endpoint resources. The composed [[BackpressureProtocol]] holds only in-memory
   * atomics (no timers, threads, or sockets), so there is nothing to close here; a final state
   * snapshot is logged for operability.
   */
  override def onStop(): Unit = {
    logInfo(s"$ENDPOINT_NAME stopped after routing heartbeats=${heartbeatCount.get()}, " +
      s"acks=${ackCount.get()}, rateUpdates=${rateUpdateCount.get()}, " +
      s"timeouts=${timeoutCount.get()}; final protocol state: ${protocol.debugState}")
  }
}

/**
 * Factory and shared constants for [[BackpressureRpcEndpoint]].
 */
private[spark] object BackpressureRpcEndpoint extends Logging {

  /**
   * The exact `RpcEnv` endpoint name under which the backpressure endpoint registers. The
   * streaming shuffle manager (F-101) references this constant rather than re-spelling the
   * string.
   */
  val ENDPOINT_NAME: String = "streaming-shuffle-backpressure"

  /** Sentinel returned by [[BackpressureRpcEndpoint.lastAdvisedRateBytesPerSec]] before any
   *  [[RateUpdate]] advisory has been received. */
  val UNSET_RATE: Long = -1L

  /**
   * Register a [[BackpressureRpcEndpoint]] on the local `RpcEnv`, but only on executors.
   *
   * This is the single supported way to wire the endpoint up and it hard-enforces the
   * executor-only contract (AAP touchpoint 9): when `isDriver` is `true` no endpoint is created
   * or registered and `None` is returned, guaranteeing the driver never hosts this endpoint. On
   * an executor the endpoint is registered under [[ENDPOINT_NAME]] and the resulting
   * [[RpcEndpointRef]] is returned.
   *
   * @param rpcEnv   the local RPC environment to register with
   * @param protocol the flow-control protocol received messages are routed to
   * @param isDriver `true` on the driver (registration is skipped), `false` on an executor
   * @param debug    forwarded to the endpoint to gate verbose per-message routing logs
   * @return `Some(ref)` when registered on an executor, `None` on the driver
   */
  def register(
      rpcEnv: RpcEnv,
      protocol: BackpressureProtocol,
      isDriver: Boolean,
      debug: Boolean = false): Option[RpcEndpointRef] = {
    if (isDriver) {
      logDebug(s"Skipping $ENDPOINT_NAME registration on the driver (executor-only endpoint)")
      None
    } else {
      val endpoint = new BackpressureRpcEndpoint(rpcEnv, protocol, debug)
      val ref = rpcEnv.setupEndpoint(ENDPOINT_NAME, endpoint)
      logInfo(s"Registered $ENDPOINT_NAME endpoint on executor")
      Some(ref)
    }
  }
}
