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

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, verify, verifyNoInteractions, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv}

/**
 * Unit tests for [[BackpressureRpcEndpoint]], the executor-only RPC mailbox of the streaming
 * shuffle backpressure protocol.
 *
 * The suite validates the two behaviors that make the endpoint correct and safe:
 *
 *   - '''Executor-only registration''' (a security invariant): `registerIfExecutor` returns
 *     [[scala.None]] on the driver and never calls `setupEndpoint` there, but on an executor it
 *     registers exactly one endpoint under the canonical name `"streaming-shuffle-backpressure"`
 *     and returns its [[org.apache.spark.rpc.RpcEndpointRef]]. This enforces the AAP
 *     security-reuse rule that the backpressure channel exists on executors only.
 *   - '''Faithful message dispatch''': `receive` and `receiveAndReply` forward each
 *     `BackpressureMessage` verbatim to the matching [[BackpressureProtocol]] callback --
 *     `Heartbeat` to `onHeartbeat`, `Ack` to `onAck`, `RateLimitRequest` to `onRateLimitRequest`,
 *     and `Timeout` to `unregisterStream` -- while the synchronous `ask` path additionally
 *     acknowledges delivery and answers a `Ping` with `Pong`.
 *
 * Both the [[org.apache.spark.rpc.RpcEnv]] and the [[BackpressureProtocol]] are mocked, so the
 * tests are deterministic and need no live RPC environment, no network, and no sleeps.
 * Registration is proven by verifying `setupEndpoint`; dispatch is proven by verifying the
 * forwarded protocol calls.
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite with Matchers {

  /** The canonical stream identity reused across the dispatch tests. */
  private val shuffleId = 1
  private val mapId = 2L
  private val reduceId = 3
  private val streamKey = BackpressureProtocol.StreamKey(shuffleId, mapId, reduceId)

  /**
   * Builds an endpoint wired to a mocked [[RpcEnv]] and a mocked [[BackpressureProtocol]]. The
   * endpoint needs no live RPC environment for `receive`/`receiveAndReply` dispatch, and returning
   * the protocol mock lets each test verify the forwarded call.
   *
   * @return the endpoint under test together with its mocked [[BackpressureProtocol]]
   */
  private def newEndpoint(): (BackpressureRpcEndpoint, BackpressureProtocol) = {
    val protocol = mock(classOf[BackpressureProtocol])
    val endpoint = new BackpressureRpcEndpoint(mock(classOf[RpcEnv]), protocol)
    (endpoint, protocol)
  }

  test("registerIfExecutor returns None on the driver") {
    val rpcEnv = mock(classOf[RpcEnv])
    val protocol = mock(classOf[BackpressureProtocol])
    BackpressureRpcEndpoint.registerIfExecutor(rpcEnv, isDriver = true, protocol) mustBe None
    // Security invariant: the driver must never register a backpressure mailbox.
    verify(rpcEnv, never()).setupEndpoint(any(), any())
  }

  test("registerIfExecutor registers on an executor under the canonical name") {
    val rpcEnv = mock(classOf[RpcEnv])
    val protocol = mock(classOf[BackpressureProtocol])
    val ref = mock(classOf[RpcEndpointRef])
    when(rpcEnv.setupEndpoint(meq(BackpressureRpcEndpoint.ENDPOINT_NAME), any()))
      .thenReturn(ref)
    val out = BackpressureRpcEndpoint.registerIfExecutor(rpcEnv, isDriver = false, protocol)
    out mustBe Some(ref)
    // Exactly one mailbox registers, under the name both producer and consumer resolve.
    verify(rpcEnv).setupEndpoint(meq("streaming-shuffle-backpressure"), any())
  }

  test("ENDPOINT_NAME matches the config constant") {
    BackpressureRpcEndpoint.ENDPOINT_NAME mustBe StreamingShuffleConfig.BACKPRESSURE_ENDPOINT_NAME
    BackpressureRpcEndpoint.ENDPOINT_NAME mustBe "streaming-shuffle-backpressure"
  }

  test("receive dispatches Heartbeat to protocol.onHeartbeat") {
    val (endpoint, protocol) = newEndpoint()
    endpoint.receive.apply(BackpressureRpcEndpoint.Heartbeat(shuffleId, mapId, reduceId, 1234L))
    verify(protocol).onHeartbeat(streamKey)
  }

  test("receive dispatches Ack to protocol.onAck") {
    val (endpoint, protocol) = newEndpoint()
    endpoint.receive.apply(BackpressureRpcEndpoint.Ack(shuffleId, mapId, reduceId, 4096L))
    verify(protocol).onAck(streamKey, 4096L)
  }

  test("receiveAndReply dispatches RateLimitRequest and acknowledges delivery") {
    val (endpoint, protocol) = newEndpoint()
    val context = mock(classOf[RpcCallContext])
    endpoint.receiveAndReply(context).apply(
      BackpressureRpcEndpoint.RateLimitRequest(shuffleId, mapId, reduceId, 8192L))
    verify(protocol).onRateLimitRequest(streamKey, 8192L)
    // The synchronous ask path positively acknowledges so the caller can confirm delivery.
    verify(context).reply(any())
  }

  test("receive accepts a Timeout and releases the stream via unregisterStream") {
    val (endpoint, protocol) = newEndpoint()
    noException must be thrownBy {
      endpoint.receive.apply(BackpressureRpcEndpoint.Timeout(shuffleId, mapId, reduceId))
    }
    // A peer-declared timeout has no dedicated protocol hook; it releases per-stream state.
    verify(protocol).unregisterStream(streamKey)
  }

  test("receiveAndReply answers a Ping with Pong") {
    val (endpoint, _) = newEndpoint()
    val context = mock(classOf[RpcCallContext])
    endpoint.receiveAndReply(context).apply(BackpressureRpcEndpoint.Ping)
    verify(context).reply(BackpressureRpcEndpoint.Pong)
  }

  test("onStart and onStop only log and never touch the protocol") {
    val (endpoint, protocol) = newEndpoint()
    noException must be thrownBy {
      endpoint.onStart()
      endpoint.onStop()
    }
    // The mailbox owns no timer or state, so lifecycle hooks invoke no protocol method.
    verifyNoInteractions(protocol)
  }
}
