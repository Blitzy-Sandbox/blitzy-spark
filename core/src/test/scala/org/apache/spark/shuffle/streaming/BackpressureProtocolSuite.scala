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

import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * Unit tests for [[BackpressureProtocol]], the token-bucket + heartbeat flow-control brain of the
 * opt-in streaming shuffle backend.
 *
 * The suite is pure and deterministic: it needs no SparkContext, no RpcEnv, and no real clock. It
 * drives the protocol's public API directly and asserts the three liveness/metric invariants that
 * the CP2 review found broken:
 *
 *  - a consumer-originated rate-limit request is treated as consumer activity (it refreshes
 *    liveness and clears a prior consumer-timeout), so sustained rate negotiation alone can never
 *    let the scan declare an otherwise-live consumer timed out;
 *  - an explicit `markTimedOut` deterministically marks both the producer and consumer tracks and
 *    is idempotent, so an explicit peer-timeout signal is never lost to scan timing;
 *  - each independently blocked send is counted as exactly one backpressure episode, so blocked
 *    sends are never collapsed into a single under-counted episode.
 */
class BackpressureProtocolSuite extends SparkFunSuite with Matchers {

  /**
   * Builds a protocol over a real [[TokenBucketRateLimiter]] and a real
   * [[StreamingShuffleMetrics]] (so backpressure events are actually counted). The default rate is
   * the unlimited sentinel so liveness tests never block; the episode-counting test passes a small
   * positive rate so the limiter empties after the first burst permit and subsequent sends block.
   *
   * @param rateBytesPerSec the limiter rate in bytes/sec; `<= 0` or `Long.MaxValue` is unlimited
   * @return the protocol under test paired with its metrics holder
   */
  private def newProtocol(
      rateBytesPerSec: Long = Long.MaxValue): (BackpressureProtocol, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val limiter = new TokenBucketRateLimiter(rateBytesPerSec)
    val metrics = new StreamingShuffleMetrics
    (new BackpressureProtocol(cfg, limiter, metrics), metrics)
  }

  test("onRateLimitRequest refreshes consumer liveness and clears a consumer timeout") {
    val (protocol, _) = newProtocol()
    val key = BackpressureProtocol.StreamKey(0, 0L, 0)

    // Force the consumer track into the timed-out state, then deliver a valid rate-limit request.
    protocol.markTimedOut(key)
    assert(protocol.isConsumerTimedOut(key))

    // A rate-limit request is consumer control traffic: it must refresh liveness and clear the
    // consumer-timeout flag (the MAJOR finding: retuning the limiter alone left liveness stale).
    protocol.onRateLimitRequest(key, 1000L)
    assert(!protocol.isConsumerTimedOut(key))
  }

  test("markTimedOut marks both tracks deterministically and is idempotent") {
    val (protocol, _) = newProtocol()
    val key = BackpressureProtocol.StreamKey(1, 2L, 3)

    // An unknown stream is not timed out on either track.
    assert(!protocol.isProducerTimedOut(key))
    assert(!protocol.isConsumerTimedOut(key))

    // The explicit signal marks BOTH tracks regardless of elapsed idle time.
    protocol.markTimedOut(key)
    assert(protocol.isProducerTimedOut(key))
    assert(protocol.isConsumerTimedOut(key))

    // Re-marking an already-timed-out stream is a no-op (no exception, flags stay set).
    protocol.markTimedOut(key)
    assert(protocol.isProducerTimedOut(key))
    assert(protocol.isConsumerTimedOut(key))
  }

  test("acquireSendPermit counts each blocked send as exactly one backpressure episode") {
    // 10 bytes/sec: the first 1-byte acquire is served from the initial burst, after which the
    // limiter must refill (~0.1 s/byte), so each subsequent 1-byte send blocks briefly.
    val (protocol, metrics) = newProtocol(rateBytesPerSec = 10L)
    val key = BackpressureProtocol.StreamKey(0, 0L, 0)

    // Drain the immediately-available burst permit; this send does not block.
    protocol.acquireSendPermit(key, 1)
    val baseline = metrics.backpressureEvents

    // Each of these two sends finds the bucket empty, opens its own throttle episode, blocks on
    // the limiter, and closes the episode when the blocking acquire returns. They must be counted
    // as two separate episodes (the MINOR finding: the episode used to stay open across blocked
    // sends, collapsing independent waits into a single under-counted event).
    protocol.acquireSendPermit(key, 1)
    protocol.acquireSendPermit(key, 1)

    (metrics.backpressureEvents - baseline) mustBe 2L
  }
}
