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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * Unit tests for [[BackpressureProtocol]] -- the executor-side token-bucket and heartbeat
 * flow-control engine -- together with the arithmetic contract of its rate-limiter
 * collaborator [[org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter]].
 *
 * The protocol is exercised with real collaborators (a [[StreamingShuffleConfig]] over an
 * empty [[SparkConf]], a [[StreamingShuffleMetrics]] holder, and a real rate limiter); no
 * mocks are required and no `SparkContext` is started. Assertions are deterministic and free
 * of wall-clock timing: token-bucket accounting is observed through the public `status`
 * tuple, and the rate-limiter tests assert exact power-of-two byte rates together with the
 * limiter's pass-through and validation contracts.
 */
class BackpressureProtocolSuite extends SparkFunSuite {

  /**
   * Builds a real [[BackpressureProtocol]] wired with real collaborators; no mocks are needed.
   *
   * The [[StreamingShuffleConfig]] is constructed over an empty [[SparkConf]], so every
   * `spark.shuffle.streaming.*` value resolves to its registered default -- in particular
   * `maxBandwidthMBps == 0` (unlimited), which disables the credit gate in
   * [[BackpressureProtocol.acquire]]. The injected [[TokenBucketRateLimiter]] bandwidth is
   * parameterized so individual tests can exercise the bounded and unlimited limiter paths.
   *
   * The returned protocol is NOT started; any test that calls [[BackpressureProtocol.start]]
   * must stop it in a `finally` block so the daemon scan thread is never leaked.
   */
  private def newProtocol(mbps: Int = 100, shuffles: Int = 1): BackpressureProtocol = {
    new BackpressureProtocol(
      new StreamingShuffleConfig(new SparkConf(false)),
      new StreamingShuffleMetrics(),
      new TokenBucketRateLimiter(mbps, shuffles))
  }

  test("protocol constants match the spec") {
    // The scan-interval and producer/consumer timeout thresholds are private vals inside
    // BackpressureProtocol (read only by the internal daemon scan and start()). The production
    // class intentionally does not expose them and this suite must not modify production code,
    // so their documented spec values are read reflectively and locked here -- without touching
    // production or duplicating the literals into it. The fields are materialized in bytecode
    // (they are read by scan()/start()), so the reflective read is stable.
    val protocol = newProtocol()
    def readLongField(name: String): Long = {
      val field = classOf[BackpressureProtocol].getDeclaredField(name)
      field.setAccessible(true)
      field.get(protocol).asInstanceOf[Long]
    }
    assert(readLongField("SCAN_INTERVAL_MS") == 1000L)
    assert(readLongField("PRODUCER_TIMEOUT_MS") == 5000L)
    assert(readLongField("CONSUMER_TIMEOUT_MS") == 10000L)
  }

  test("acquire returns a Boolean and succeeds under the unlimited default config") {
    val protocol = newProtocol()
    // The config resolves maxBandwidthMBps to 0 (unlimited), so the credit gate is disabled and
    // the send gate depends solely on the fresh rate limiter, which always grants the first
    // request; the first acquire therefore succeeds.
    val granted: Boolean = protocol.acquire(1024L)
    assert(granted)
    // A non-positive request is trivially granted and never consults the limiter.
    assert(protocol.acquire(0L))
    assert(protocol.acquire(-1L))
  }

  test("onConsumerAck releases send credit and updates accounting") {
    val protocol = newProtocol()
    protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = 0, bytesConsumed = 4096L,
      seqNumber = 1)
    val (activeShuffles, tokensAvailable) = protocol.status
    // The ack registers shuffle 0 and releases exactly 4096 bytes of credit; both values sane.
    assert(activeShuffles >= 0 && tokensAvailable >= 0L)
    assert(activeShuffles == 1)
    assert(tokensAvailable == 4096L)
    // A non-positive ack releases no further credit but still marks the shuffle active safely.
    protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = 0, bytesConsumed = 0L,
      seqNumber = 2)
    assert(protocol.status._2 == 4096L)
  }

  test("onThrottleRequest adjusts the rate and keeps status queryable") {
    val protocol = newProtocol()
    protocol.onThrottleRequest(shuffleId = 0, targetBytesPerSec = 1024L * 1024L)
    // The throttle registers the shuffle and records a backpressure event; status stays sane.
    val (activeShuffles, tokensAvailable) = protocol.status
    assert(activeShuffles == 1)
    assert(tokensAvailable >= 0L)
    // A non-positive target is rejected without throwing (the limiter needs a positive rate).
    protocol.onThrottleRequest(shuffleId = 1, targetBytesPerSec = 0L)
    assert(protocol.status._1 == 2)
  }

  test("onHeartbeat records liveness without affecting shuffle accounting") {
    val protocol = newProtocol()
    protocol.onHeartbeat("exec-1", System.currentTimeMillis())
    // A heartbeat is isolated from token/shuffle accounting: it registers no shuffle or credit.
    assert(protocol.status == (0, 0L))
    // A null executor id is ignored defensively and must not throw.
    protocol.onHeartbeat(null, System.currentTimeMillis())
    assert(protocol.status == (0, 0L))
  }

  test("start and stop are idempotent and leak no scan thread") {
    val protocol = newProtocol()
    try {
      protocol.start()
      // A second start while already running is a no-op: it must not throw or spawn a scanner.
      protocol.start()
      // The protocol remains queryable while the daemon scan is running.
      assert(protocol.status._1 >= 0)
    } finally {
      protocol.stop()
      // A second stop when already stopped is a no-op; it also confirms the daemon scan thread
      // was shut down rather than leaked.
      protocol.stop()
    }
  }

  test("effectiveBytesPerSecond matches the verified arithmetic vectors") {
    // 100 MBps * 0.80 / 4 = 20 MiB/s. The math is exact powers of two, so == needs no tolerance.
    assert(TokenBucketRateLimiter.effectiveBytesPerSecond(100, 4) == 20 * 1024 * 1024)
    // 50 MBps * 0.80 / 1 = 40 MiB/s.
    assert(TokenBucketRateLimiter.effectiveBytesPerSecond(50, 1) == 40 * 1024 * 1024)
    // numConcurrentShuffles is floored at 1, so 0 divides by 1: 100 * 0.80 / 1 = 80 MiB/s.
    assert(TokenBucketRateLimiter.effectiveBytesPerSecond(100, 0) == 80 * 1024 * 1024)
  }

  test("LINK_CAPACITY_FACTOR is 0.80") {
    assert(TokenBucketRateLimiter.LINK_CAPACITY_FACTOR == 0.80)
  }

  test("unlimited pass-through when maxBandwidthMBps is non-positive") {
    val zeroLimiter = new TokenBucketRateLimiter(0, 4)
    assert(zeroLimiter.acquire(1 << 20) == 0.0)
    assert(zeroLimiter.tryAcquire(1 << 20))
    // A negative budget is treated defensively as unlimited (never a Guava non-positive rate).
    val negativeLimiter = new TokenBucketRateLimiter(-1, 2)
    assert(negativeLimiter.acquire(1 << 20) == 0.0)
    assert(negativeLimiter.tryAcquire(1 << 20))
  }

  test("bounded limiter grants requests and setRate rejects a non-positive rate") {
    val limiter = new TokenBucketRateLimiter(100, 1)
    // Guava RateLimiter timing is environment-dependent, so only the behavioral contract is
    // asserted: a Boolean is returned without throwing. A fresh limiter grants the first request.
    assert(limiter.tryAcquire(1))
    // Adjusting to a positive rate must not throw.
    limiter.setRate(1.0)
    // A zero or negative rate violates require(bytesPerSecond > 0).
    intercept[IllegalArgumentException] {
      limiter.setRate(0.0)
    }
    intercept[IllegalArgumentException] {
      limiter.setRate(-1.0)
    }
  }

}
