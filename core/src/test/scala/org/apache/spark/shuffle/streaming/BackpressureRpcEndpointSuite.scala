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

import org.mockito.Mockito.{mock, never, verify, when}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}

/**
 * Unit tests for [[BackpressureRpcEndpoint]] -- the executor-only, thread-safe RPC endpoint
 * bound as `"streaming-shuffle-backpressure"` that decodes streaming-shuffle backpressure
 * messages and forwards them to the local [[BackpressureProtocol]].
 *
 * '''Strategy: direct partial-function invocation.''' The endpoint is a thin decode-and-forward
 * shell, so its behavior is validated deterministically by invoking the `receive` and
 * `receiveAndReply` partial functions directly rather than dispatching live RPC messages. This
 * avoids threading, timeouts, and wall-clock assertions entirely. A real [[RpcEnv]] is obtained
 * from a local [[SparkContext]] (the endpoint requires one as its first constructor argument),
 * while the collaborating [[BackpressureProtocol]] is a Mockito mock whose forwarded calls are
 * verified. Because construction is side-effect free (the endpoint never self-registers), the
 * mock never has its constructor invoked and no daemon threads are started.
 *
 * The inherited `sc` is stopped by [[LocalSparkContext]]'s `afterEach`; this suite therefore
 * never shuts the [[RpcEnv]] down itself, since that env belongs to `sc.env`.
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * Creates a fresh local [[SparkContext]] -- assigned to the inherited `sc` so
   * [[LocalSparkContext]] stops it after the test -- and returns its real [[RpcEnv]]. Each test
   * that needs an env calls this exactly once; the endpoint is then exercised by direct
   * partial-function invocation, so no live RPC dispatch ever occurs.
   */
  private def freshRpcEnv(): RpcEnv = {
    sc = new SparkContext("local", "test", new SparkConf(false))
    sc.env.rpcEnv
  }

  /**
   * Builds the standard fixture: a real [[RpcEnv]], a mocked [[BackpressureProtocol]] whose
   * forwarded calls are verified, and the [[BackpressureRpcEndpoint]] under test wired to both.
   */
  private def newFixture(): (RpcEnv, BackpressureProtocol, BackpressureRpcEndpoint) = {
    val rpcEnv = freshRpcEnv()
    val protocol = mock(classOf[BackpressureProtocol])
    val endpoint = new BackpressureRpcEndpoint(rpcEnv, protocol)
    (rpcEnv, protocol, endpoint)
  }

  test("ENDPOINT_NAME is the stable streaming-shuffle-backpressure name") {
    // The name is the single source of truth used by both setupEndpoint (bind) and
    // setupEndpointRef (resolve); pin it so an accidental rename is caught immediately.
    assert(BackpressureRpcEndpoint.ENDPOINT_NAME == "streaming-shuffle-backpressure")
  }

  test("receive forwards ConsumerAck to the protocol with identical fields") {
    val (_, protocol, endpoint) = newFixture()
    // Field order: ConsumerAck(shuffleId, mapId, reduceId, bytesConsumed, seqNumber).
    endpoint.receive.apply(BackpressureRpcEndpoint.ConsumerAck(1, 2L, 3, 4096L, 5))
    // The endpoint must forward every field, unaltered and in order, to the protocol.
    verify(protocol).onConsumerAck(1, 2L, 3, 4096L, 5)
  }

  test("receive forwards ThrottleRequest to the protocol with identical fields") {
    val (_, protocol, endpoint) = newFixture()
    endpoint.receive.apply(BackpressureRpcEndpoint.ThrottleRequest(1, 999L))
    verify(protocol).onThrottleRequest(1, 999L)
  }

  test("receive forwards Heartbeat to the protocol with identical fields") {
    val (_, protocol, endpoint) = newFixture()
    endpoint.receive.apply(BackpressureRpcEndpoint.Heartbeat("exec-1", 123L))
    verify(protocol).onHeartbeat("exec-1", 123L)
  }

  test("receive drops a ConsumerAck with a negative field at the trust boundary") {
    val (_, protocol, endpoint) = newFixture()
    // The endpoint is the trust boundary for remote backpressure signals. A negative shuffleId --
    // or a negative byte count, which is never legitimate -- marks a malformed message that must
    // be dropped and never forwarded to the protocol.
    endpoint.receive.apply(BackpressureRpcEndpoint.ConsumerAck(-1, 2L, 3, 4096L, 5))
    endpoint.receive.apply(BackpressureRpcEndpoint.ConsumerAck(1, 2L, 3, -1L, 5))
    verify(protocol, never()).onConsumerAck(-1, 2L, 3, 4096L, 5)
    verify(protocol, never()).onConsumerAck(1, 2L, 3, -1L, 5)
  }

  test("receive drops a ThrottleRequest with negative fields at the trust boundary") {
    val (_, protocol, endpoint) = newFixture()
    // A negative shuffleId or a negative target rate is structurally malformed and dropped before
    // it can reach the protocol. (The valid positive-rate range is enforced by the protocol.)
    endpoint.receive.apply(BackpressureRpcEndpoint.ThrottleRequest(-1, 999L))
    endpoint.receive.apply(BackpressureRpcEndpoint.ThrottleRequest(1, -5L))
    verify(protocol, never()).onThrottleRequest(-1, 999L)
    verify(protocol, never()).onThrottleRequest(1, -5L)
  }

  test("receive drops a Heartbeat with an empty id or non-positive timestamp") {
    val (_, protocol, endpoint) = newFixture()
    // An empty executor id cannot be correlated and a non-positive timestamp is nonsensical; both
    // are dropped at the boundary. (The forward-skew ceiling on the timestamp is enforced by the
    // protocol as defense-in-depth.)
    endpoint.receive.apply(BackpressureRpcEndpoint.Heartbeat("", 123L))
    endpoint.receive.apply(BackpressureRpcEndpoint.Heartbeat("exec-1", 0L))
    endpoint.receive.apply(BackpressureRpcEndpoint.Heartbeat("exec-1", -1L))
    verify(protocol, never()).onHeartbeat("", 123L)
    verify(protocol, never()).onHeartbeat("exec-1", 0L)
    verify(protocol, never()).onHeartbeat("exec-1", -1L)
  }

  test("receiveAndReply answers GetBackpressureStatus with a BackpressureStatus snapshot") {
    val (_, protocol, endpoint) = newFixture()
    val ctx = mock(classOf[RpcCallContext])
    // The endpoint reads protocol.status (activeShuffles, tokensAvailable) and maps it 1:1 onto
    // the BackpressureStatus reply; stub the tuple and assert the exact reply payload.
    when(protocol.status).thenReturn((2, 4096L))
    endpoint.receiveAndReply(ctx).apply(BackpressureRpcEndpoint.GetBackpressureStatus)
    verify(ctx).reply(BackpressureRpcEndpoint.BackpressureStatus(2, 4096L))
  }

  test("rpcEnv accessor returns the env supplied to the constructor") {
    val (rpcEnv, _, endpoint) = newFixture()
    // The endpoint exposes the exact RpcEnv instance it was constructed with (reference equality).
    assert(endpoint.rpcEnv eq rpcEnv)
  }

  test("endpoint registers on the RpcEnv under ENDPOINT_NAME") {
    // A fresh, isolated fixture so registration does not interfere with the direct-invocation
    // tests above. Registration triggers only logging in onStart; no messages are dispatched.
    val (rpcEnv, _, endpoint) = newFixture()
    val ref = rpcEnv.setupEndpoint(BackpressureRpcEndpoint.ENDPOINT_NAME, endpoint)
    try {
      assert(ref != null)
    } finally {
      // Unregister the endpoint (onStop logs). The RpcEnv itself belongs to sc.env and is torn
      // down by LocalSparkContext.afterEach, so it must not be shut down here.
      rpcEnv.stop(ref)
    }
  }
}
