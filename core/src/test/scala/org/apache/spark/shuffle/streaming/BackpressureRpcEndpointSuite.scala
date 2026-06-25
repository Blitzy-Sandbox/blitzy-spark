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

import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.{anyLong, eq => meq}
import org.mockito.Mockito.{mock, never, verify, verifyNoInteractions}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

/**
 * Unit tests for [[BackpressureRpcEndpoint]] (streaming shuffle feature F-108), the executor-only
 * [[ThreadSafeRpcEndpoint]] that carries [[BackpressureMessage]] envelopes across executor
 * boundaries and routes each one to the composed [[BackpressureProtocol]] (F-107).
 *
 * The suite asserts the three contractual invariants of the endpoint:
 *
 *  1. '''The exact endpoint name.''' [[BackpressureRpcEndpoint.ENDPOINT_NAME]] must be the
 *     literal `"streaming-shuffle-backpressure"`; the streaming shuffle manager (F-101) looks
 *     it up by this string, so any drift would silently break cross-executor flow control.
 *  2. '''Executor-only registration.''' [[BackpressureRpcEndpoint.register]] returns `None` on
 *     the driver (the driver hosts no shuffle buffers to flow-control) and `Some(ref)` on an
 *     executor, registered under [[BackpressureRpcEndpoint.ENDPOINT_NAME]].
 *  3. '''Message routing.''' [[BackpressureRpcEndpoint.receive]] must route every
 *     [[BackpressureMessage]] variant to the correct [[BackpressureProtocol]] action (an
 *     [[Ack]] to its per-stream [[StreamKey]]) and never throw on an unexpected message.
 *  4. '''Input validation.''' Malformed or out-of-scope messages are rejected before any
 *     protocol state is touched, and an untrusted [[Timeout]] reason is bounded and sanitized.
 *
 * '''Harness.''' A single real [[RpcEnv]] is created in `beforeAll` and torn down in `afterAll`
 * (mirroring `RpcEnvSuite`) so the suite never leaks the RPC dispatcher threads that
 * [[org.apache.spark.SparkFunSuite]]'s thread audit guards against. Because the endpoint always
 * registers under the single fixed [[BackpressureRpcEndpoint.ENDPOINT_NAME]], every registering
 * test frees that name again through [[withRegisteredEndpoint]] before the next test runs.
 *
 * '''Determinism.''' Routing is asserted by invoking [[BackpressureRpcEndpoint.receive]] directly
 * (synchronous, on the test thread) against a Mockito-mocked [[BackpressureProtocol]]; one
 * additional test performs a genuine asynchronous `send` round-trip through the [[RpcEnv]] and
 * verifies the routing inside an `eventually` block, since `send` is fire-and-forget.
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite {

  /** Shared, server-mode RPC environment; local sends are dispatched in-process (no I/O). */
  private var rpcEnv: RpcEnv = _

  private val conf = new SparkConf()

  override def beforeAll(): Unit = {
    super.beforeAll()
    rpcEnv = RpcEnv.create(
      "test-backpressure", "localhost", 0, conf, new SecurityManager(conf))
  }

  override def afterAll(): Unit = {
    try {
      if (rpcEnv != null) {
        rpcEnv.shutdown()
        rpcEnv.awaitTermination()
        rpcEnv = null
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Build an unregistered endpoint backed by a fresh Mockito-mocked [[BackpressureProtocol]].
   *
   * The endpoint is NOT registered with the [[RpcEnv]], so [[BackpressureRpcEndpoint.receive]]
   * can be invoked directly and synchronously for a deterministic routing assertion, and there
   * is no contention for the fixed [[BackpressureRpcEndpoint.ENDPOINT_NAME]].
   */
  private def newEndpointWithMock(): (BackpressureRpcEndpoint, BackpressureProtocol) = {
    val protocol = mock(classOf[BackpressureProtocol])
    (new BackpressureRpcEndpoint(rpcEnv, protocol), protocol)
  }

  /**
   * Register a [[BackpressureRpcEndpoint]] on the shared [[RpcEnv]] as if on an executor, run
   * `body` against the resulting [[RpcEndpointRef]], and always unregister it afterwards so the
   * fixed [[BackpressureRpcEndpoint.ENDPOINT_NAME]] is freed for the next test. `RpcEnv.stop`
   * removes the name from the dispatcher registry synchronously, so a subsequent registration
   * under the same name succeeds.
   */
  private def withRegisteredEndpoint(
      protocol: BackpressureProtocol)(body: RpcEndpointRef => Unit): Unit = {
    val refOpt = BackpressureRpcEndpoint.register(rpcEnv, protocol, isDriver = false)
    assert(refOpt.isDefined, "executor registration must yield an endpoint ref")
    val ref = refOpt.get
    try {
      body(ref)
    } finally {
      rpcEnv.stop(ref)
    }
  }

  test("ENDPOINT_NAME is the exact 'streaming-shuffle-backpressure' contract string") {
    // This name is a hard contract shared with StreamingShuffleManager (F-101); assert literally.
    assert(BackpressureRpcEndpoint.ENDPOINT_NAME === "streaming-shuffle-backpressure")
  }

  test("register returns None on the driver (executor-only)") {
    val protocol = mock(classOf[BackpressureProtocol])

    // The driver hosts no shuffle buffers, so registration must be a no-op returning None.
    assert(BackpressureRpcEndpoint.register(rpcEnv, protocol, isDriver = true).isEmpty)
    // Calling it again on the driver is equally a side-effect-free no-op.
    assert(BackpressureRpcEndpoint.register(rpcEnv, protocol, isDriver = true).isEmpty)

    // Prove the driver path never consumed ENDPOINT_NAME: an executor registration under the same
    // name immediately succeeds. Clean it up so the shared RpcEnv is left pristine.
    val executorRef = BackpressureRpcEndpoint.register(rpcEnv, protocol, isDriver = false)
    assert(executorRef.isDefined)
    rpcEnv.stop(executorRef.get)
  }

  test("register returns Some(ref) on an executor under ENDPOINT_NAME") {
    val protocol = mock(classOf[BackpressureProtocol])
    withRegisteredEndpoint(protocol) { ref =>
      // The ref the executor receives is bound to the exact endpoint name.
      assert(ref.name === BackpressureRpcEndpoint.ENDPOINT_NAME)
      // The endpoint is discoverable by name through the same RpcEnv.
      val looked = rpcEnv.setupEndpointRef(rpcEnv.address, BackpressureRpcEndpoint.ENDPOINT_NAME)
      assert(looked.name === BackpressureRpcEndpoint.ENDPOINT_NAME)
    }
  }

  test("receive routes Ack to protocol.mergeAck and refills only when bytes are reclaimed") {
    // reclaimedBytes == 0: only the monotonic watermark merge, no credit refill.
    val (endpointNoReclaim, protocolNoReclaim) = newEndpointWithMock()
    endpointNoReclaim.receive(
      Ack(shuffleId = 1, partitionId = 2, attemptId = 10L, executorId = "exec-c",
        seqNo = 42L, reclaimedBytes = 0L))
    // The ack is routed to exactly the stream identified by its (shuffle, partition, attempt,
    // executor) fields -- never an unrelated stream's watermark.
    verify(protocolNoReclaim).mergeAck(meq(StreamKey(1, 2, 10L, "exec-c")), meq(42L))
    verify(protocolNoReclaim, never()).refill(anyLong())
    assert(endpointNoReclaim.acksReceived === 1L)

    // reclaimedBytes > 0: merge the watermark AND return the reclaimed bytes to the window.
    val (endpointReclaim, protocolReclaim) = newEndpointWithMock()
    endpointReclaim.receive(
      Ack(shuffleId = 3, partitionId = 4, attemptId = 20L, executorId = "exec-d",
        seqNo = 7L, reclaimedBytes = 512L))
    verify(protocolReclaim).mergeAck(meq(StreamKey(3, 4, 20L, "exec-d")), meq(7L))
    verify(protocolReclaim).refill(meq(512L))
    assert(endpointReclaim.acksReceived === 1L)
  }

  test("receive routes a sent Ack to protocol.mergeAck (end-to-end send round-trip)") {
    val protocol = mock(classOf[BackpressureProtocol])
    withRegisteredEndpoint(protocol) { ref =>
      val seqNo = 4242L
      // `send` is fire-and-forget for a ThreadSafeRpcEndpoint receive: the routing happens on the
      // RpcEnv message loop, so the verification must be retried until the message is delivered.
      ref.send(Ack(shuffleId = 9, partitionId = 1, attemptId = 30L, executorId = "exec-e",
        seqNo = seqNo, reclaimedBytes = 0L))
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        verify(protocol).mergeAck(meq(StreamKey(9, 1, 30L, "exec-e")), meq(seqNo))
      }
    }
  }

  test("receive handles every BackpressureMessage variant and routes each correctly") {
    val (endpoint, protocol) = newEndpointWithMock()

    val heartbeat = Heartbeat(executorId = "exec-7", shuffleId = 5, attemptId = 40L,
      reducePartitionRange = "[0,2)", timestampMs = 123L)
    val ack = Ack(shuffleId = 5, partitionId = 1, attemptId = 40L, executorId = "exec-7",
      seqNo = 11L, reclaimedBytes = 64L)
    val rateUpdate = RateUpdate(shuffleId = 5, partitionId = 1, attemptId = 40L,
      maxBytesPerSec = 1000000L)
    val timeoutMsg = Timeout(shuffleId = 5, partitionId = 1, attemptId = 40L,
      reason = "producer unresponsive")

    // Every variant is accepted by the total receive partial function and routed without error.
    Seq[BackpressureMessage](heartbeat, ack, rateUpdate, timeoutMsg).foreach { message =>
      assert(endpoint.receive.isDefinedAt(message))
      endpoint.receive(message)
    }

    // Heartbeat -> protocol.recordHeartbeat; Ack -> protocol.mergeAck on the ack's stream key
    // (+ refill of reclaimed bytes). RateUpdate and Timeout carry no protocol side effect in v1.
    verify(protocol).recordHeartbeat()
    verify(protocol).mergeAck(meq(StreamKey(5, 1, 40L, "exec-7")), meq(11L))
    verify(protocol).refill(meq(64L))

    // Each variant advances exactly its own observable counter.
    assert(endpoint.heartbeatsReceived === 1L)
    assert(endpoint.acksReceived === 1L)
    assert(endpoint.rateUpdatesReceived === 1L)
    assert(endpoint.timeoutsReceived === 1L)
    // The RateUpdate advisory is recorded for observability (rate limiter is immutable in v1).
    assert(endpoint.lastAdvisedRateBytesPerSec === 1000000L)
  }

  test("receive ignores an unexpected message without throwing or touching the protocol") {
    val (endpoint, protocol) = newEndpointWithMock()

    // The catch-all branch makes receive total, so a stray (non-backpressure) message is handled
    // (logged) rather than raising a MatchError; a single bad message can never tear the endpoint
    // down, and nothing is routed to the flow-control protocol.
    assert(endpoint.receive.isDefinedAt("not-a-backpressure-message"))
    endpoint.receive("not-a-backpressure-message")
    verifyNoInteractions(protocol)
  }

  test("receive rejects malformed/out-of-scope messages without touching the protocol (M6)") {
    val (endpoint, protocol) = newEndpointWithMock()

    // Negative identifiers, an empty executor id, an empty reduce range, a negative sequence
    // number, and a negative reclaimed-byte count are each malformed/out-of-scope and must be
    // dropped before any flow-control state is touched.
    val malformed = Seq[BackpressureMessage](
      Ack(shuffleId = -1, partitionId = 0, attemptId = 0L, executorId = "e",
        seqNo = 1L, reclaimedBytes = 0L),
      Ack(shuffleId = 0, partitionId = 0, attemptId = 0L, executorId = "",
        seqNo = 1L, reclaimedBytes = 0L),
      Ack(shuffleId = 0, partitionId = 0, attemptId = 0L, executorId = "e",
        seqNo = -5L, reclaimedBytes = 0L),
      Ack(shuffleId = 0, partitionId = 0, attemptId = 0L, executorId = "e",
        seqNo = 1L, reclaimedBytes = -1L),
      Heartbeat(executorId = "", shuffleId = 0, attemptId = 0L,
        reducePartitionRange = "[0,1)", timestampMs = 0L),
      Heartbeat(executorId = "e", shuffleId = 0, attemptId = 0L,
        reducePartitionRange = "", timestampMs = 0L),
      RateUpdate(shuffleId = 0, partitionId = -1, attemptId = 0L, maxBytesPerSec = 1L),
      Timeout(shuffleId = -1, partitionId = 0, attemptId = 0L, reason = "x"))

    malformed.foreach(endpoint.receive(_))

    // Nothing reached the flow-control protocol, every message was tallied as rejected, and no
    // routed counter advanced.
    verifyNoInteractions(protocol)
    assert(endpoint.messagesRejected === malformed.size.toLong)
    assert(endpoint.acksReceived === 0L)
    assert(endpoint.heartbeatsReceived === 0L)
    assert(endpoint.rateUpdatesReceived === 0L)
    assert(endpoint.timeoutsReceived === 0L)
  }

  test("receive bounds and sanitizes the untrusted Timeout reason before logging (M6)") {
    val (endpoint, _) = newEndpointWithMock()

    // A reason carrying newlines/control chars (a log-forging risk) and exceeding the length
    // bound is accepted but bounded and stripped before it is retained or logged.
    val pad = "x" * (BackpressureRpcEndpoint.MAX_REASON_LENGTH * 2)
    val rawReason = "line1\nline2\tinjected\r" + pad
    endpoint.receive(
      Timeout(shuffleId = 1, partitionId = 0, attemptId = 0L, reason = rawReason))

    assert(endpoint.timeoutsReceived === 1L)
    val sanitized = endpoint.lastTimeoutReason
    assert(!sanitized.exists(_.isControl), "control characters must be stripped")
    assert(sanitized.length <= BackpressureRpcEndpoint.MAX_REASON_LENGTH)
  }

  test("endpoint is a ThreadSafeRpcEndpoint") {
    // Serialized, lock-free message delivery is a correctness assumption of the routing logic.
    val (endpoint, _) = newEndpointWithMock()
    assert(endpoint.isInstanceOf[ThreadSafeRpcEndpoint])
  }
}
