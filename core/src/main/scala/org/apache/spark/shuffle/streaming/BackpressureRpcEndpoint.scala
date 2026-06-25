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

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import org.apache.spark.SparkException
import org.apache.spark.internal.{LogKeys, Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys._
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
  private val rejectedCount = new AtomicLong(0L)
  private val lastAdvisedRate = new AtomicLong(UNSET_RATE)

  /** The most recently observed (already sanitized) [[Timeout]] reason, for observability. */
  private val lastTimeoutReasonRef = new AtomicReference[String]("")

  /** The number of [[Heartbeat]] messages this endpoint has routed. */
  def heartbeatsReceived: Long = heartbeatCount.get()

  /** The number of [[Ack]] messages this endpoint has routed. */
  def acksReceived: Long = ackCount.get()

  /** The number of [[RateUpdate]] advisories this endpoint has recorded. */
  def rateUpdatesReceived: Long = rateUpdateCount.get()

  /** The number of [[Timeout]] messages this endpoint has observed. */
  def timeoutsReceived: Long = timeoutCount.get()

  /**
   * The number of inbound messages this endpoint rejected as malformed or out-of-scope (failing
   * the [[BackpressureRpcEndpoint.isValid]] field/identity checks). Rejected messages are dropped
   * before any [[BackpressureProtocol]] state is touched.
   */
  def messagesRejected: Long = rejectedCount.get()

  /**
   * The most recently observed [[Timeout]] reason after bounding and control-character
   * sanitization, or the empty string if no [[Timeout]] has been observed. Exposed for
   * observability; the raw, untrusted reason is never retained or logged verbatim.
   */
  def lastTimeoutReason: String = lastTimeoutReasonRef.get()

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
  private def logRouted(message: => MessageWithContext): Unit = {
    if (debug) {
      logInfo(message)
    } else {
      logDebug(message)
    }
  }

  /**
   * Validate and route a single [[BackpressureMessage]] to the appropriate
   * [[BackpressureProtocol]] action.
   *
   * Every inbound message is first checked by [[isValid]]: a message bearing a negative shuffle,
   * partition, attempt, seqNo, or reclaimed-byte count, an absent/over-long executor id, or an
   * absent/over-long reduce-partition range is rejected, tallied through
   * [[messagesRejected]], and dropped WITHOUT touching any [[BackpressureProtocol]] state. This
   * prevents a malformed or out-of-scope envelope from corrupting flow-control state and ensures
   * an [[Ack]] is only ever routed to a well-formed per-stream [[StreamKey]]. The untrusted,
   * free-text [[Timeout]] reason is bounded and stripped of control characters before logging.
   *
   * The match is exhaustive over the sealed [[BackpressureMessage]] hierarchy, so adding a new
   * subtype to the ADT will surface here as a compile-time obligation.
   */
  private def handleMessage(message: BackpressureMessage): Unit = {
    if (!isValid(message)) {
      rejectedCount.incrementAndGet()
      logWarning(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} rejected " +
        log"malformed/out-of-scope ${MDC(CLASS_NAME, message.getClass.getSimpleName)}; " +
        log"total rejected=${MDC(COUNT, rejectedCount.get())}")
      return
    }
    message match {
      case Ack(shuffleId, partitionId, attemptId, executorId, seqNo, reclaimedBytes) =>
        // Route the ack to exactly this stream's per-key watermark; an unrelated stream's
        // watermark can never be advanced by this message (see BackpressureProtocol.mergeAck).
        val key = StreamKey(shuffleId, partitionId, attemptId, executorId)
        protocol.mergeAck(key, seqNo)
        if (reclaimedBytes > 0L) {
          protocol.refill(reclaimedBytes)
        }
        ackCount.incrementAndGet()
        logRouted(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} routed " +
          log"Ack(shuffle=${MDC(SHUFFLE_ID, shuffleId)}, " +
          log"partition=${MDC(REDUCE_ID, partitionId)}, " +
          log"attempt=${MDC(TASK_ATTEMPT_ID, attemptId)}, " +
          log"executor=${MDC(EXECUTOR_ID, executorId)}, seqNo=${MDC(COUNT, seqNo)}, " +
          log"reclaimedBytes=${MDC(NUM_BYTES, reclaimedBytes)}); " +
          log"availableCredits=${MDC(MEMORY_SIZE, protocol.availableCredits)}")

      case Heartbeat(executorId, shuffleId, attemptId, reducePartitionRange, timestampMs) =>
        protocol.recordHeartbeat()
        heartbeatCount.incrementAndGet()
        logRouted(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} routed " +
          log"Heartbeat(executor=${MDC(EXECUTOR_ID, executorId)}, " +
          log"shuffle=${MDC(SHUFFLE_ID, shuffleId)}, " +
          log"attempt=${MDC(TASK_ATTEMPT_ID, attemptId)}, " +
          log"reduceRange=${MDC(RANGE, reducePartitionRange)}, " +
          log"timestampMs=${MDC(TIMESTAMP, timestampMs)})")

      case RateUpdate(shuffleId, partitionId, attemptId, maxBytesPerSec) =>
        lastAdvisedRate.set(maxBytesPerSec)
        rateUpdateCount.incrementAndGet()
        // v1 keeps configuration immutable for the application lifetime; the advisory is recorded
        // for observability rather than mutating the composed (immutable) rate limiter.
        logRouted(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} recorded RateUpdate " +
          log"advisory(shuffle=${MDC(SHUFFLE_ID, shuffleId)}, " +
          log"partition=${MDC(REDUCE_ID, partitionId)}, " +
          log"attempt=${MDC(TASK_ATTEMPT_ID, attemptId)}, " +
          log"maxBytesPerSec=${MDC(RATE_LIMIT, maxBytesPerSec)}); rate is immutable in v1")

      case Timeout(shuffleId, partitionId, attemptId, reason) =>
        // Bound and sanitize the untrusted reason BEFORE retaining or logging it (log-forging and
        // unbounded-log guard); a timeout signals an unresponsive peer so it is surfaced at warn.
        val safeReason = sanitizeReason(reason)
        lastTimeoutReasonRef.set(safeReason)
        timeoutCount.incrementAndGet()
        logWarning(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} received " +
          log"Timeout(shuffle=${MDC(SHUFFLE_ID, shuffleId)}, " +
          log"partition=${MDC(REDUCE_ID, partitionId)}, " +
          log"attempt=${MDC(TASK_ATTEMPT_ID, attemptId)}, " +
          log"reason=${MDC(REASON, safeReason)}); " +
          log"total timeouts observed=${MDC(COUNT, timeoutCount.get())}")
    }
  }

  /**
   * Whether `message` carries well-formed, in-scope identity and payload fields: identifiers and
   * counters must be non-negative, and the free-text identity strings (executor id and
   * reduce-partition range) must be present and within their length bounds. The [[Timeout]]
   * reason is NOT validated here; it is untrusted free text that [[sanitizeReason]] bounds and
   * sanitizes at log time rather than rejecting the whole timeout signal.
   */
  private def isValid(message: BackpressureMessage): Boolean = message match {
    case Ack(shuffleId, partitionId, attemptId, executorId, seqNo, reclaimedBytes) =>
      shuffleId >= 0 && partitionId >= 0 && attemptId >= 0L && isValidExecutorId(executorId) &&
        seqNo >= 0L && reclaimedBytes >= 0L
    case Heartbeat(executorId, shuffleId, attemptId, reducePartitionRange, timestampMs) =>
      isValidExecutorId(executorId) && shuffleId >= 0 && attemptId >= 0L &&
        isValidRange(reducePartitionRange) && timestampMs >= 0L
    case RateUpdate(shuffleId, partitionId, attemptId, maxBytesPerSec) =>
      shuffleId >= 0 && partitionId >= 0 && attemptId >= 0L && maxBytesPerSec >= 0L
    case Timeout(shuffleId, partitionId, attemptId, _) =>
      shuffleId >= 0 && partitionId >= 0 && attemptId >= 0L
  }

  /** A non-empty executor id within the length bound. */
  private def isValidExecutorId(executorId: String): Boolean =
    executorId != null && executorId.nonEmpty &&
      executorId.length <= BackpressureRpcEndpoint.MAX_EXECUTOR_ID_LENGTH

  /** A non-empty reduce-partition range string within the length bound. */
  private def isValidRange(range: String): Boolean =
    range != null && range.nonEmpty && range.length <= BackpressureRpcEndpoint.MAX_RANGE_LENGTH

  /**
   * Bound and sanitize an untrusted free-text timeout reason for safe logging: control characters
   * (including newlines that could otherwise forge log lines) are replaced with spaces and the
   * result is truncated to [[BackpressureRpcEndpoint.MAX_REASON_LENGTH]] characters. A `null`
   * reason becomes the empty string.
   */
  private def sanitizeReason(reason: String): String = {
    if (reason == null) {
      ""
    } else {
      reason.replaceAll("\\p{Cntrl}", " ").take(BackpressureRpcEndpoint.MAX_REASON_LENGTH)
    }
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
      logWarning(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} ignoring unexpected " +
        log"fire-and-forget message of type ${MDC(CLASS_NAME, other.getClass.getName)}")
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
      logWarning(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} received unexpected " +
        log"request-reply message of type ${MDC(CLASS_NAME, other.getClass.getName)}; " +
        log"replying with failure")
      context.sendFailure(new SparkException(
        s"$ENDPOINT_NAME does not handle messages of type ${other.getClass.getName}"))
  }

  /** Log endpoint startup. `self` is valid from this point until [[onStop]]. */
  override def onStart(): Unit = {
    logDebug(log"${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} started (executor-only " +
      log"backpressure endpoint)")
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

  /** Maximum accepted length of an untrusted [[Timeout]] reason; longer reasons are truncated. */
  val MAX_REASON_LENGTH: Int = 256

  /** Maximum accepted length of a [[Heartbeat]] reduce-partition-range string. */
  val MAX_RANGE_LENGTH: Int = 64

  /** Maximum accepted length of an executor id carried on an [[Ack]] or [[Heartbeat]]. */
  val MAX_EXECUTOR_ID_LENGTH: Int = 256

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
      logDebug(log"Skipping ${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} registration on the " +
        log"driver (executor-only endpoint)")
      None
    } else {
      val endpoint = new BackpressureRpcEndpoint(rpcEnv, protocol, debug)
      val ref = rpcEnv.setupEndpoint(ENDPOINT_NAME, endpoint)
      logInfo(log"Registered ${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)} endpoint on executor")
      Some(ref)
    }
  }
}
