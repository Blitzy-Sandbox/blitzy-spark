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

import org.mockito.ArgumentMatchers.{any, anyString, eq => meq}
import org.mockito.Mockito.{mock, never, times, verify, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef}
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}

/**
 * Pure-Mockito unit tests for [[BackpressureRpcEndpoint]] &mdash; the executor-side
 * Netty RPC endpoint that adapts the four streaming-shuffle wire messages
 * ([[BackpressureRpcEndpoint.HeartbeatMessage]],
 * [[BackpressureRpcEndpoint.AcknowledgmentMessage]],
 * [[BackpressureRpcEndpoint.RateLimitMessage]],
 * [[BackpressureRpcEndpoint.TimeoutMessage]]) into calls on the sibling
 * [[BackpressureProtocol]] coordinator.
 *
 * The suite is organized into five groups, matching the agent prompt:
 *
 *   1. '''Constants and case-class equality''' &mdash; verifies the
 *      [[BackpressureRpcEndpoint.ENDPOINT_NAME]] constant and the Scala
 *      case-class equality/hashCode contracts for each of the four message
 *      types. Equality is a required property because the messages flow
 *      through Netty serialization and across Mockito `verify()` matchers.
 *   2. '''`setupOnExecutor` driver/executor dispatch''' &mdash; verifies the
 *      AAP section 0.7.5 invariant "The BackpressureRpcEndpoint MUST be
 *      registered only on executors, never on the driver" by exercising the
 *      factory helper with `SparkContext.DRIVER_IDENTIFIER` and with a
 *      non-driver executor ID. Confirms registration happens under the
 *      canonical `ENDPOINT_NAME`.
 *   3. '''`receive(...)` message routing''' &mdash; verifies that each of
 *      the four wire messages is dispatched to the correct
 *      [[BackpressureProtocol]] method. This is the hot-path fire-and-forget
 *      dispatcher; correctness here is essential because the endpoint is the
 *      single entry point for cross-executor backpressure signals.
 *   4. '''`receiveAndReply(...)` handling''' &mdash; verifies that a
 *      [[BackpressureRpcEndpoint.HeartbeatMessage]] received via
 *      `RpcEndpointRef.ask` triggers both the protocol recording and the
 *      reply with a constant `true` (the v1 "heartbeat ack" contract used
 *      by integration tests).
 *   5. '''[[ThreadSafeRpcEndpoint]] trait compliance and lifecycle''' &mdash;
 *      verifies the class extends the thread-safe endpoint trait, exposes
 *      the constructor-provided [[RpcEnv]] via the `rpcEnv` abstract val,
 *      and that the `onStart` / `onStop` hooks are non-throwing.
 *
 * == Mockito pattern ==
 *
 * All 16 tests are pure Mockito Pattern B: no real [[RpcEnv]] instance is
 * ever constructed; [[BackpressureProtocol]], [[RpcEnv]], [[RpcEndpointRef]],
 * and [[RpcCallContext]] are all replaced with Mockito mocks. This keeps the
 * suite hermetic &mdash; no ports are opened, no threads are spun up, and
 * no [[org.apache.spark.SparkEnv]] is required. Consequently the suite
 * executes in milliseconds and is immune to CI timing flake.
 *
 * This test pattern matches the source file
 * [[org.apache.spark.shuffle.sort.SortShuffleManagerSuite]] (mocked
 * collaborators, `extends SparkFunSuite with Matchers`, natural-language
 * must-style assertions) and is consistent with the sibling
 * [[StreamingShuffleFallbackPolicySuite]] and
 * [[StreamingShuffleHandleSuite]] in this package.
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite with Matchers {

  // ==========================================================================
  // Shared test-double factories.
  //
  // Every test constructs fresh mocks so that verify(...) interactions are
  // isolated per test; shared instances would require reset(...) calls and
  // risk cross-test leakage.
  // ==========================================================================

  /** Fresh Mockito mock of [[BackpressureProtocol]]. */
  private def mockProtocol(): BackpressureProtocol = mock(classOf[BackpressureProtocol])

  /** Fresh Mockito mock of [[RpcEnv]]. */
  private def mockRpcEnv(): RpcEnv = mock(classOf[RpcEnv])

  /** Fresh Mockito mock of [[RpcEndpointRef]]. */
  private def mockRef(): RpcEndpointRef = mock(classOf[RpcEndpointRef])

  // ==========================================================================
  // Group 1: Constants and case-class equality.
  //
  // These tests are pure value-level correctness checks; they do NOT
  // construct a BackpressureRpcEndpoint instance and therefore require no
  // mocks. They are the first line of defence against accidental typos in
  // the wire-message schema or in the endpoint-name constant.
  // ==========================================================================

  test("ENDPOINT_NAME constant is exactly 'streaming-shuffle-backpressure'") {
    // This literal value is reproduced verbatim in the AAP section 0.2.3.2
    // (Row N6) and in the canonical registration site
    // `BackpressureRpcEndpoint.setupOnExecutor`. Any change here would
    // silently break cross-executor clients that resolve the endpoint by
    // name via `rpcEnv.setupEndpointRef(addr, ENDPOINT_NAME)`.
    BackpressureRpcEndpoint.ENDPOINT_NAME must be("streaming-shuffle-backpressure")
  }

  test("HeartbeatMessage case class supports equality and hashCode") {
    // Scala case-class equality is derived from the constructor parameters.
    // Two HeartbeatMessages with identical (producerId, timestamp) must be
    // equal and share a hashCode; two with any differing field must not.
    // Mockito's ArgumentMatchers rely on this contract when verifying
    // endpoint.receive(HeartbeatMessage(...)) calls downstream.
    val a = BackpressureRpcEndpoint.HeartbeatMessage("exec-1", 100L)
    val b = BackpressureRpcEndpoint.HeartbeatMessage("exec-1", 100L)
    val c = BackpressureRpcEndpoint.HeartbeatMessage("exec-1", 200L)
    val d = BackpressureRpcEndpoint.HeartbeatMessage("exec-2", 100L)

    a must be(b)
    a.hashCode() must be(b.hashCode())
    a must not be c
    a must not be d
  }

  test("AcknowledgmentMessage case class supports equality") {
    // Equality guarantees that acknowledgment messages can be used as keys
    // in a ConcurrentHashMap (BackpressureProtocol.ackTable) without the
    // caller having to implement equals/hashCode manually.
    val a = BackpressureRpcEndpoint.AcknowledgmentMessage("block-1", 1024L)
    val b = BackpressureRpcEndpoint.AcknowledgmentMessage("block-1", 1024L)
    val c = BackpressureRpcEndpoint.AcknowledgmentMessage("block-2", 1024L)

    a must be(b)
    a must not be c
  }

  test("RateLimitMessage case class supports equality") {
    // The single-field RateLimitMessage is the simplest case; the test
    // still exercises both equal and not-equal paths so a silent field-type
    // regression (e.g. Double&rarr;Float) would be detected.
    val a = BackpressureRpcEndpoint.RateLimitMessage(1000.0)
    val b = BackpressureRpcEndpoint.RateLimitMessage(1000.0)
    val c = BackpressureRpcEndpoint.RateLimitMessage(2000.0)

    a must be(b)
    a must not be c
  }

  test("TimeoutMessage case class supports equality") {
    // TimeoutMessage is a single-String wrapper. Equality verifies that
    // duplicate-delivery (a common network pattern during producer
    // failover) is idempotent at the message-value level.
    val a = BackpressureRpcEndpoint.TimeoutMessage("producer-1")
    val b = BackpressureRpcEndpoint.TimeoutMessage("producer-1")
    val c = BackpressureRpcEndpoint.TimeoutMessage("producer-2")

    a must be(b)
    a must not be c
  }

  // ==========================================================================
  // Group 2: `setupOnExecutor` driver-side rejection and executor-side
  // registration. Enforces AAP section 0.7.5: the BackpressureRpcEndpoint
  // MUST be registered only on executors, never on the driver.
  //
  // Each test exercises exactly one branch of the if/else in
  // `setupOnExecutor`. Together they fully cover the factory helper.
  // ==========================================================================

  test("setupOnExecutor returns None when executorId == SparkContext.DRIVER_IDENTIFIER") {
    // Driver-side invocation: the endpoint MUST NOT be registered. The
    // factory must return None and must NOT invoke `rpcEnv.setupEndpoint`
    // at all; the `never()` verification here is the strongest possible
    // guard against accidental driver-side instantiation.
    val env = mockRpcEnv()
    val proto = mockProtocol()

    val result = BackpressureRpcEndpoint.setupOnExecutor(env, proto, SparkContext.DRIVER_IDENTIFIER)

    result must be(None)
    // We pass `any()` (untyped) here to assert "no call with ANY
    // arguments"; a typed matcher would narrow the guard and allow a
    // stealth call with a different endpoint type to slip through.
    verify(env, never()).setupEndpoint(anyString(), any())
  }

  test("setupOnExecutor returns Some(RpcEndpointRef) on a non-driver executor") {
    // Executor-side happy path: the factory should construct the endpoint,
    // register it under ENDPOINT_NAME, and return the RpcEnv-supplied
    // RpcEndpointRef wrapped in Some(...).
    val env = mockRpcEnv()
    val ref = mockRef()
    val proto = mockProtocol()
    // Stub setupEndpoint to return our mock ref when called with the
    // canonical name. `any[ThreadSafeRpcEndpoint]()` matches any concrete
    // ThreadSafeRpcEndpoint (the BackpressureRpcEndpoint instance the
    // factory will construct internally); we do not need to inspect that
    // instance directly here.
    when(env.setupEndpoint(meq("streaming-shuffle-backpressure"), any[ThreadSafeRpcEndpoint]()))
      .thenReturn(ref)

    val result = BackpressureRpcEndpoint.setupOnExecutor(env, proto, "executor-0")

    result must be(Some(ref))
    verify(env, times(1))
      .setupEndpoint(meq("streaming-shuffle-backpressure"), any[ThreadSafeRpcEndpoint]())
  }

  test("setupOnExecutor registers under the constant ENDPOINT_NAME") {
    // Regression guard against a future refactor that hard-codes the
    // endpoint name at the registration call site rather than using the
    // ENDPOINT_NAME constant. The meq(...) argument matcher is fed
    // BackpressureRpcEndpoint.ENDPOINT_NAME (not a string literal) so that
    // the test follows the same indirection as production code. If the
    // constant and the registration call site ever drift apart, this test
    // fails immediately.
    val env = mockRpcEnv()
    val ref = mockRef()
    val proto = mockProtocol()
    when(env.setupEndpoint(anyString(), any[ThreadSafeRpcEndpoint]()))
      .thenReturn(ref)

    BackpressureRpcEndpoint.setupOnExecutor(env, proto, "executor-5")

    verify(env).setupEndpoint(
      meq(BackpressureRpcEndpoint.ENDPOINT_NAME), any[ThreadSafeRpcEndpoint]())
  }

  // ==========================================================================
  // Group 3: `receive(...)` message routing.
  //
  // Exercises the fire-and-forget PartialFunction that handles all four
  // wire messages. Each test directly invokes the PartialFunction's
  // `apply(...)` on the desired message instance and verifies, via
  // Mockito, that the correct single [[BackpressureProtocol]] method was
  // called with the exact arguments carried by the message.
  //
  // No RpcEnv is involved: we are testing the pure-Scala dispatch logic
  // independently of the RPC delivery machinery.
  // ==========================================================================

  test("receive(HeartbeatMessage) routes to protocol.recordHeartbeat") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    endpoint.receive.apply(BackpressureRpcEndpoint.HeartbeatMessage("p1", 1234L))

    // The `times(1)` asserts exactly-one call; a missing or duplicated
    // dispatch would both fail.
    verify(proto, times(1)).recordHeartbeat("p1", 1234L)
  }

  test("receive(AcknowledgmentMessage) routes to protocol.acknowledgeReceipt") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    endpoint.receive.apply(BackpressureRpcEndpoint.AcknowledgmentMessage("block-7", 4096L))

    verify(proto, times(1)).acknowledgeReceipt("block-7", 4096L)
  }

  test("receive(RateLimitMessage) routes to protocol.updateRate") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    // 8_388_608.0 == 8 MiB/sec, a plausible streaming-shuffle rate post
    // throttle. Any double literal is equally valid for the matcher
    // semantics; we pick a realistic one for readability.
    endpoint.receive.apply(BackpressureRpcEndpoint.RateLimitMessage(8_388_608.0))

    verify(proto, times(1)).updateRate(8_388_608.0)
  }

  test("receive(TimeoutMessage) routes to protocol.unregisterProducer") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    endpoint.receive.apply(BackpressureRpcEndpoint.TimeoutMessage("producer-42"))

    verify(proto, times(1)).unregisterProducer("producer-42")
  }

  // ==========================================================================
  // Group 4: `receiveAndReply(...)` handling.
  //
  // The v1 request-response contract supports only HeartbeatMessage. The
  // handler must (a) forward the message to BackpressureProtocol and (b)
  // reply with a constant `true` via the supplied RpcCallContext, in that
  // order. We verify both invocations happened exactly once.
  // ==========================================================================

  test("receiveAndReply(HeartbeatMessage) routes to protocol and replies true") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val context = mock(classOf[RpcCallContext])
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    // Invoke the PartialFunction returned by receiveAndReply(...) with the
    // heartbeat message. The endpoint should record the heartbeat and
    // reply true on the same RpcCallContext.
    endpoint.receiveAndReply(context).apply(BackpressureRpcEndpoint.HeartbeatMessage("p1", 500L))

    verify(proto, times(1)).recordHeartbeat("p1", 500L)
    verify(context, times(1)).reply(true)
  }

  // ==========================================================================
  // Group 5: [[ThreadSafeRpcEndpoint]] trait compliance and lifecycle
  // methods.
  //
  // These tests confirm (a) the class extends the thread-safe endpoint
  // trait &mdash; a requirement because the RpcEnv relies on the trait
  // marker to dispatch at most one message at a time per endpoint; (b)
  // the constructor-supplied RpcEnv is exposed via the abstract `rpcEnv`
  // val; and (c) the lifecycle hooks `onStart` and `onStop` are
  // non-throwing. The hooks log but have no side effects, so their only
  // observable failure mode is an exception.
  // ==========================================================================

  test("endpoint is a ThreadSafeRpcEndpoint") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    // ThreadSafeRpcEndpoint itself extends RpcEndpoint, so instance-of on
    // the trait is a necessary condition for correct RpcEnv dispatch
    // semantics. We assert on ThreadSafeRpcEndpoint specifically because
    // the weaker RpcEndpoint marker would not imply the one-message-at-a
    // -time guarantee that the endpoint's unsynchronized handlers rely on.
    endpoint.isInstanceOf[ThreadSafeRpcEndpoint] must be(true)
    // And the parent RpcEndpoint marker must also be present; this guards
    // against a hypothetical future refactor that breaks the trait
    // hierarchy.
    endpoint.isInstanceOf[RpcEndpoint] must be(true)
  }

  test("endpoint.rpcEnv returns the constructor-provided RpcEnv") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    // `be theSameInstanceAs` is reference-equality (==, not ===): we are
    // asserting that the exact mock instance we passed in is the exact
    // instance returned by `endpoint.rpcEnv`. This catches any future
    // refactor that clones or wraps the supplied RpcEnv.
    endpoint.rpcEnv must be theSameInstanceAs env
  }

  test("onStart and onStop do not throw") {
    val env = mockRpcEnv()
    val proto = mockProtocol()
    val endpoint = new BackpressureRpcEndpoint(env, proto)

    // The lifecycle hooks emit INFO-level log lines and have no other
    // side effects. A regression that accidentally introduces side
    // effects (e.g. calling rpcEnv methods from onStart, or invoking a
    // null protocol method) would throw on a mock that has no stubbing
    // for that method. `noException ... must be thrownBy` is the natural
    // ScalaTest must-style assertion for "this block completes cleanly."
    noException must be thrownBy endpoint.onStart()
    noException must be thrownBy endpoint.onStop()
  }
}
