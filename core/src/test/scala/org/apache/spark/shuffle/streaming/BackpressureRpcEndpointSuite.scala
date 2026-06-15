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

import org.mockito.Mockito.mock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * Unit tests for [[BackpressureRpcEndpoint]], the executor-only one-way control endpoint that
 * delegates inbound backpressure messages to [[BackpressureProtocol]].
 *
 * `receive` is a plain `PartialFunction[Any, Unit]` that never dereferences `rpcEnv`, so the
 * endpoint can be driven directly with a Mockito `RpcEnv` and no live RpcEnv/SparkContext. The
 * single test exercises the CP2 security finding (CWE-20): malformed control messages must be
 * dropped before touching protocol state, while well-formed messages take effect -- a heartbeat
 * creates exactly one tracked stream and an explicit timeout deterministically marks the addressed
 * stream timed out on both tracks.
 */
class BackpressureRpcEndpointSuite extends SparkFunSuite with Matchers {

  /** Builds a real protocol (unlimited limiter, real metrics) and the endpoint under test. */
  private def newEndpoint(): (BackpressureRpcEndpoint, BackpressureProtocol) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val protocol =
      new BackpressureProtocol(cfg, new TokenBucketRateLimiter(Long.MaxValue),
        new StreamingShuffleMetrics)
    (new BackpressureRpcEndpoint(mock(classOf[RpcEnv]), protocol), protocol)
  }

  test("the endpoint drops malformed control messages without mutating protocol state") {
    val (endpoint, protocol) = newEndpoint()

    // Each message below is malformed: a negative shuffle/map/reduce id, or a negative ack count.
    // The endpoint must drop every one BEFORE delegating, so no bogus per-stream state is created
    // and the shared rate limiter is never retuned (closes the CWE-20 untrusted-input vector).
    endpoint.receive(BackpressureRpcEndpoint.Heartbeat(-1, 0L, 0, 0L))
    endpoint.receive(BackpressureRpcEndpoint.Ack(0, -1L, 0, 5L))
    endpoint.receive(BackpressureRpcEndpoint.Ack(0, 0L, 0, -5L))
    endpoint.receive(BackpressureRpcEndpoint.RateLimitRequest(0, 0L, -1, 1000L))
    endpoint.receive(BackpressureRpcEndpoint.Timeout(-1, 0L, 0))
    assert(protocol.registeredStreamCount === 0)

    // A well-formed heartbeat is accepted and creates exactly one tracked stream.
    endpoint.receive(BackpressureRpcEndpoint.Heartbeat(0, 0L, 0, 0L))
    assert(protocol.registeredStreamCount === 1)

    // A well-formed explicit timeout deterministically marks the addressed stream on both tracks,
    // so the signal is never lost to scan timing.
    val timedOutKey = BackpressureProtocol.StreamKey(2, 2L, 2)
    endpoint.receive(BackpressureRpcEndpoint.Timeout(2, 2L, 2))
    assert(protocol.isProducerTimedOut(timedOutKey))
    assert(protocol.isConsumerTimedOut(timedOutKey))
  }
}
