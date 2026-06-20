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
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

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
 * The registration and dispatch tests mock both the [[org.apache.spark.rpc.RpcEnv]] and the
 * [[BackpressureProtocol]], so they are deterministic and need no live RPC environment, no network,
 * and no sleeps. Registration is proven by verifying `setupEndpoint`; dispatch is proven by
 * verifying the forwarded protocol calls.
 *
 * A final group of '''cross-`RpcEnv` integration tests''' spins up two real
 * [[org.apache.spark.rpc.RpcEnv]]s (a producer server and a consumer client) and exercises the
 * production control-plane sender, [[BackpressureRpcSender]], end to end -- the exact path
 * [[StreamingShuffleReader]] uses. They prove that a `Heartbeat`/`Ack`/`RateLimitRequest` sent from
 * a consumer executor actually reaches the producer's endpoint and drives its
 * [[BackpressureProtocol]] (closing the consumer->producer loop across executors), rather than only
 * mutating the consumer's local protocol instance. Because delivery is asynchronous fire-and-forget,
 * these tests await the effect with `eventually` (polling, never a fixed sleep).
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite with Matchers with Eventually {

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

  // ---- Cross-RpcEnv integration: the production sender drives the producer-side protocol --------

  /**
   * Runs `body` with the production control-plane sender wired across two REAL [[RpcEnv]]s: a
   * producer server hosting a [[BackpressureRpcEndpoint]] backed by `protocol`, and a consumer
   * client that resolves the producer endpoint. The resolved [[RpcEndpointRef]] is the same kind of
   * reference `StreamingShuffleManager` hands [[StreamingShuffleReader]]. Both environments are
   * always shut down, so the test leaks no ports or threads.
   *
   * @param protocol the producer-side protocol the hosted endpoint forwards messages to
   * @param body     receives the consumer-side reference to the producer endpoint
   */
  private def withCrossEnvSender(
      protocol: BackpressureProtocol)(body: RpcEndpointRef => Unit): Unit = {
    val conf = new SparkConf(false)
    val securityManager = new SecurityManager(conf)
    val producerEnv = RpcEnv.create("bp-itest-producer", "localhost", 0, conf, securityManager)
    val consumerEnv =
      RpcEnv.create("bp-itest-consumer", "localhost", 0, conf, securityManager, clientMode = true)
    try {
      producerEnv.setupEndpoint(
        BackpressureRpcEndpoint.ENDPOINT_NAME, new BackpressureRpcEndpoint(producerEnv, protocol))
      val ref =
        consumerEnv.setupEndpointRef(producerEnv.address, BackpressureRpcEndpoint.ENDPOINT_NAME)
      body(ref)
    } finally {
      consumerEnv.shutdown()
      consumerEnv.awaitTermination()
      producerEnv.shutdown()
      producerEnv.awaitTermination()
    }
  }

  test("production sender delivers Heartbeat/Ack/RateLimitRequest across RpcEnvs to the protocol") {
    val protocol = mock(classOf[BackpressureProtocol])
    withCrossEnvSender(protocol) { ref =>
      // Exactly the calls StreamingShuffleReader.maybeSendConsumerControl issues to a co-located
      // producer endpoint, plus a rate-limit request, sent over a real RpcEnv.
      BackpressureRpcSender.sendHeartbeat(ref, streamKey)
      BackpressureRpcSender.sendAck(ref, streamKey, 4096L)
      BackpressureRpcSender.sendRateLimitRequest(ref, streamKey, 8192L)
      // Fire-and-forget delivery is asynchronous; await dispatch on the producer-side protocol.
      eventually(timeout(10.seconds), interval(20.milliseconds)) {
        verify(protocol).onHeartbeat(streamKey)
        verify(protocol).onAck(streamKey, 4096L)
        verify(protocol).onRateLimitRequest(streamKey, 8192L)
      }
    }
  }

  test("production sender Ack across RpcEnvs decrements the producer-side unacked tally") {
    val protocol =
      new BackpressureProtocol(
        new StreamingShuffleConfig(new SparkConf(false)),
        new TokenBucketRateLimiter(0L),
        new StreamingShuffleMetrics)
    withCrossEnvSender(protocol) { ref =>
      protocol.registerStream(streamKey)
      protocol.recordSend(streamKey, 10000L)
      protocol.unackedBytes(streamKey) mustBe 10000L
      // A remote consumer ack must drive the PRODUCER's protocol state -- exactly the unacked-byte
      // tally the writer's consumer-timeout path polls -- proving the loop is wired across
      // executors rather than only mutating the consumer's local protocol instance.
      BackpressureRpcSender.sendAck(ref, streamKey, 4096L)
      eventually(timeout(10.seconds), interval(20.milliseconds)) {
        protocol.unackedBytes(streamKey) mustBe (10000L - 4096L)
      }
    }
  }
}
