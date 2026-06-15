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

import org.mockito.ArgumentMatchers.{any, anyLong, eq => meq}
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv}

/**
 * Unit tests for [[BackpressureRpcEndpoint]], the executor-only RPC mailbox of the opt-in
 * streaming shuffle backend.
 *
 * The suite is pure and deterministic: it stands up no live [[org.apache.spark.rpc.RpcEnv]] and
 * opens no network sockets. The [[org.apache.spark.rpc.RpcEnv]], its
 * [[org.apache.spark.rpc.RpcEndpointRef]], the [[org.apache.spark.rpc.RpcCallContext]], and the
 * [[BackpressureProtocol]] "brain" are all mocked, so the tests assert two contracts in
 * isolation:
 *
 *   - the executor-only registration invariant - [[BackpressureRpcEndpoint.registerIfExecutor]]
 *     returns `None` and registers nothing on the driver, and registers exactly one endpoint
 *     under the canonical name on an executor (the backpressure RPC endpoint is rejected on the
 *     driver and registered on executors only, per the security-reuse rule); and
 *   - message dispatch - `receive` forwards each one-way `BackpressureMessage` to the matching
 *     [[BackpressureProtocol]] handler, and `receiveAndReply` answers a `Ping` liveness probe
 *     with `Pong`.
 *
 * Mocking the protocol keeps the endpoint's deliberately "thin mailbox" role under test without
 * exercising the flow-control state machine, which is covered by `BackpressureProtocolSuite`.
 * Note that the endpoint collapses each wire message to a [[BackpressureProtocol.StreamKey]]
 * before delegating, and the heartbeat's wire-only `tsNanos` is intentionally dropped.
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite with Matchers {

  import BackpressureProtocol.StreamKey

  /**
   * Builds an endpoint wired to a freshly mocked [[org.apache.spark.rpc.RpcEnv]] and a mocked
   * [[BackpressureProtocol]]. Constructing the endpoint has no side effects (`onStart` is driven
   * by the RpcEnv lifecycle, never by the constructor), so a mocked env is sufficient for the
   * dispatch tests and keeps them free of any network or threading.
   *
   * @return
   *   the endpoint under test paired with the mocked protocol it delegates to
   */
  private def newEndpoint(): (BackpressureRpcEndpoint, BackpressureProtocol) = {
    val rpcEnv = mock(classOf[RpcEnv])
    val protocol = mock(classOf[BackpressureProtocol])
    (new BackpressureRpcEndpoint(rpcEnv, protocol), protocol)
  }

  test("registerIfExecutor returns None on the driver and registers nothing") {
    val rpcEnv = mock(classOf[RpcEnv])
    val protocol = mock(classOf[BackpressureProtocol])

    // Security invariant: the driver coordinates no streamed shuffle, so it hosts no endpoint.
    val out = BackpressureRpcEndpoint.registerIfExecutor(rpcEnv, isDriver = true, protocol)

    out mustBe None
    // The driver path must never touch the RpcEnv: nothing is registered there.
    verify(rpcEnv, never()).setupEndpoint(any(), any())
  }

  test("registerIfExecutor registers on an executor under the canonical name") {
    val rpcEnv = mock(classOf[RpcEnv])
    val protocol = mock(classOf[BackpressureProtocol])
    val ref = mock(classOf[RpcEndpointRef])
    when(rpcEnv.setupEndpoint(meq(BackpressureRpcEndpoint.ENDPOINT_NAME), any()))
      .thenReturn(ref)

    val out = BackpressureRpcEndpoint.registerIfExecutor(rpcEnv, isDriver = false, protocol)

    // On an executor the endpoint is registered exactly once and its ref is handed back.
    out mustBe Some(ref)
    verify(rpcEnv).setupEndpoint(meq("streaming-shuffle-backpressure"), any())
  }

  test("ENDPOINT_NAME matches the shared config constant") {
    // Registration (here) and lookup (manager/readers) must agree on one canonical name.
    BackpressureRpcEndpoint.ENDPOINT_NAME mustBe
      StreamingShuffleConfig.BACKPRESSURE_ENDPOINT_NAME
    BackpressureRpcEndpoint.ENDPOINT_NAME mustBe "streaming-shuffle-backpressure"
  }

  test("receive dispatches Heartbeat to protocol.onHeartbeat") {
    val (ep, protocol) = newEndpoint()

    // The wire-only tsNanos is dropped; the protocol is keyed purely by stream identity.
    ep.receive.apply(BackpressureRpcEndpoint.Heartbeat(1, 2L, 3, 1234L))

    verify(protocol).onHeartbeat(StreamKey(1, 2L, 3))
  }

  test("receive dispatches Ack to protocol.onAck") {
    val (ep, protocol) = newEndpoint()

    ep.receive.apply(BackpressureRpcEndpoint.Ack(1, 2L, 3, 4096L))

    verify(protocol).onAck(StreamKey(1, 2L, 3), 4096L)
  }

  test("receive dispatches RateLimitRequest to protocol.onRateLimitRequest") {
    val (ep, protocol) = newEndpoint()

    // RateLimitRequest is a fire-and-forget message handled by receive, not receiveAndReply.
    ep.receive.apply(BackpressureRpcEndpoint.RateLimitRequest(1, 2L, 3, 8192L))

    verify(protocol).onRateLimitRequest(StreamKey(1, 2L, 3), 8192L)
  }

  test("receive handles Timeout by triggering a protocol scan") {
    val (ep, protocol) = newEndpoint()

    // An explicit Timeout signal must not throw and must drive an on-demand timeout scan.
    noException must be thrownBy {
      ep.receive.apply(BackpressureRpcEndpoint.Timeout(1, 2L, 3))
    }

    verify(protocol).scanForTimeouts(anyLong())
  }

  test("receiveAndReply answers a Ping liveness probe with Pong") {
    val (ep, _) = newEndpoint()
    val ctx = mock(classOf[RpcCallContext])

    // Ping/Pong is the only request/response message: it lets callers confirm reachability.
    ep.receiveAndReply(ctx).apply(BackpressureRpcEndpoint.Ping)

    verify(ctx).reply(BackpressureRpcEndpoint.Pong)
  }
}
