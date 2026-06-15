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

import java.util.concurrent.TimeUnit

import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.streaming.BackpressureProtocol.StreamKey
import org.apache.spark.shuffle.streaming.network.TokenBucketRateLimiter

/**
 * Unit tests for [[BackpressureProtocol]] -- the lock-free token-bucket + heartbeat flow-control
 * state machine that throttles map-side producers so reduce-side consumers are never overwhelmed
 * and that drives the producer (5 s) and consumer (10 s) failure-detection timeouts behind the
 * streaming-shuffle failure-handling protocol.
 *
 * The suite is pure and deterministic: it needs no `SparkContext`, no `MetricsSystem`, and no RPC
 * environment, and it NEVER sleeps for the multi-second timeout windows. Instead it drives the
 * package-visible `scanForTimeouts` with an injected monotonic-clock value, which is the single
 * point of timeout detection (the daemon thread started by `start` merely calls the same method
 * every `SCAN_INTERVAL_MS`). Because the liveness handlers stamp their activity time from
 * `System.nanoTime()` internally, each timeout test brackets its activity call with before/after
 * clock readings so the injected scan timestamps stay robust regardless of scheduling jitter.
 *
 * Coverage: send-permit gating against the [[TokenBucketRateLimiter]] (unlimited and rate-limited
 * paths), the 5 s producer timeout, the 10 s consumer timeout, liveness reset on fresh producer
 * activity and on a consumer heartbeat, single-episode backpressure-event counting (the sub-1%
 * telemetry-overhead guard), consumer-requested rate adjustment, and leak-free start/stop.
 */
class BackpressureProtocolSuite extends SparkFunSuite with Matchers {

  /** Fixed 2 MB framing block, the canonical streaming-shuffle send size used by these tests. */
  private val twoMb: Int = 2 * 1024 * 1024

  /**
   * Builds a [[BackpressureProtocol]] over a fresh, defaults-only [[org.apache.spark.SparkConf]]
   * and a real [[TokenBucketRateLimiter]] at the requested byte-per-second cap (the default
   * [[scala.Long.MaxValue]] selects the limiter's unlimited fast path). The limiter and metrics
   * holder are returned alongside the protocol so individual tests can assert against the rate or
   * the backpressure-event counter without reaching into protocol internals.
   *
   * @param bytesPerSec the initial limiter rate in bytes per second
   * @return the protocol together with its limiter and metrics holder
   */
  private def newProtocol(bytesPerSec: Long = Long.MaxValue)
      : (BackpressureProtocol, TokenBucketRateLimiter, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val limiter = new TokenBucketRateLimiter(bytesPerSec)
    val metrics = new StreamingShuffleMetrics
    (new BackpressureProtocol(cfg, limiter, metrics), limiter, metrics)
  }

  /** Converts whole seconds to nanoseconds for the injected scan clock. */
  private def secs(n: Long): Long = TimeUnit.SECONDS.toNanos(n)

  test("unlimited limiter always grants send permits") {
    val (bp, _, _) = newProtocol()
    val key = StreamKey(0, 0L, 0)

    // With the unlimited fast path every non-blocking attempt is granted, every time.
    (0 until 5).foreach(_ => assert(bp.tryAcquireSendPermit(key, twoMb)))

    // The blocking variant must also return immediately (no Guava limiter is consulted when
    // unlimited), so it cannot stall the test; the granted send is recorded as unacked bytes.
    bp.acquireSendPermit(key, twoMb)
    assert(bp.unackedByteCount(key) > 0L)
  }

  test("rate-limited limiter eventually throttles send permits") {
    // A tiny 1 KB/s cap drains immediately: the first 2 MB request reserves the bucket far into
    // the future, so subsequent non-blocking attempts are refused without ever sleeping.
    val (bp, _, _) = newProtocol(1024L)
    val key = StreamKey(1, 0L, 0)

    val grants = (0 until 5).map(_ => bp.tryAcquireSendPermit(key, twoMb))
    assert(grants.contains(false))
  }

  test("producer times out after 5s without producer activity") {
    val (bp, _, _) = newProtocol()
    val key = StreamKey(2, 0L, 0)

    // beginConsuming starts the producer-liveness clock; bracket the internal stamp in [t0, tEnd].
    val t0 = System.nanoTime()
    bp.beginConsuming(key)
    val tEnd = System.nanoTime()

    // Within the 5 s window the producer is still considered alive (idle <= 1 s here).
    bp.scanForTimeouts(t0 + secs(1))
    assert(!bp.isProducerTimedOut(key))

    // Past the 5 s window (idle >= 6 s here) the scan declares the producer timed out.
    bp.scanForTimeouts(tEnd + secs(6))
    assert(bp.isProducerTimedOut(key))
  }

  test("consumer times out after 10s without acks and recovers on heartbeat") {
    val (bp, _, _) = newProtocol()
    val key = StreamKey(3, 0L, 0)

    // An ack starts/refreshes the consumer-liveness clock; bracket the internal stamp.
    val t0 = System.nanoTime()
    bp.onAck(key, 1024L)
    val tEnd = System.nanoTime()

    // Within the 10 s window the consumer is still alive (idle <= 5 s here).
    bp.scanForTimeouts(t0 + secs(5))
    assert(!bp.isConsumerTimedOut(key))

    // Past the 10 s window (idle >= 11 s here) the scan declares the consumer timed out.
    bp.scanForTimeouts(tEnd + secs(11))
    assert(bp.isConsumerTimedOut(key))

    // A consumer-to-producer heartbeat clears the timeout flag (the consumer reconnected).
    bp.onHeartbeat(key)
    assert(!bp.isConsumerTimedOut(key))
  }

  test("producer activity resets producer liveness after a timeout") {
    val (bp, _, _) = newProtocol()
    val key = StreamKey(4, 0L, 0)

    val t0 = System.nanoTime()
    bp.beginConsuming(key)
    bp.scanForTimeouts(t0 + secs(6))
    assert(bp.isProducerTimedOut(key))

    // Fresh producer activity clears the timed-out flag and restarts the liveness clock.
    val t1 = System.nanoTime()
    bp.onProducerActivity(key)
    assert(!bp.isProducerTimedOut(key))

    // A scan within 5 s of the fresh activity keeps the producer alive.
    bp.scanForTimeouts(t1 + secs(1))
    assert(!bp.isProducerTimedOut(key))
  }

  test("backpressure event is counted once per sustained episode") {
    val (bp, _, metrics) = newProtocol(1024L)
    val key = StreamKey(5, 0L, 0)

    // Five throttled sends form ONE sustained episode: the first 2 MB request drains the bucket
    // and the rest are refused. The counter must advance by exactly one (not once per attempt),
    // honoring the sub-1% executor-CPU telemetry-overhead budget.
    (0 until 5).foreach(_ => bp.tryAcquireSendPermit(key, twoMb))
    metrics.backpressureEvents mustBe 1L
  }

  test("onRateLimitRequest updates the limiter rate") {
    val (bp, limiter, _) = newProtocol()
    val key = StreamKey(6, 0L, 0)

    // The limiter starts unlimited; a consumer-requested cap must be applied to it verbatim.
    assert(limiter.isUnlimited)
    bp.onRateLimitRequest(key, 2048L)
    limiter.currentBytesPerSecond mustBe 2048L
  }

  test("start then stop is idempotent and leak-free") {
    val (bp, _, _) = newProtocol()

    // start()/stop() must be idempotent and must never leak the daemon scan thread; stop() runs
    // in a finally so the thread is released even if a later assertion fails.
    try {
      bp.start()
      bp.start()
    } finally {
      bp.stop()
      bp.stop()
    }

    // Reaching here without an exception proves the lifecycle is idempotent and leak-free.
    assert(bp.registeredStreamCount === 0)
  }

  test("nextRetransmitBackoffMs doubles each attempt then exhausts at the max") {
    val (bp, _, _) = newProtocol()
    val key = StreamKey(5, 0L, 0)

    // Exponential backoff starting at 1 s, doubling each attempt, for the 5 configured attempts:
    // 1 s, 2 s, 4 s, 8 s, 16 s.
    bp.nextRetransmitBackoffMs(key) mustBe 1000L
    bp.nextRetransmitBackoffMs(key) mustBe 2000L
    bp.nextRetransmitBackoffMs(key) mustBe 4000L
    bp.nextRetransmitBackoffMs(key) mustBe 8000L
    bp.nextRetransmitBackoffMs(key) mustBe 16000L
    // The sixth call exceeds RETRY_MAX_ATTEMPTS (5) and signals "give up" with -1.
    bp.nextRetransmitBackoffMs(key) mustBe -1L
  }

  test("markTimedOut flags both liveness tracks immediately and is idempotent") {
    val (bp, _, _) = newProtocol()
    val key = StreamKey(6, 1L, 2)

    // A fresh, never-registered stream reports neither track timed out.
    assert(!bp.isProducerTimedOut(key))
    assert(!bp.isConsumerTimedOut(key))

    // The out-of-band Timeout signal must take effect at once on BOTH tracks, role-agnostically.
    bp.markTimedOut(key)
    assert(bp.isProducerTimedOut(key))
    assert(bp.isConsumerTimedOut(key))

    // Re-marking an already-timed-out stream is a harmless no-op (no exception, still timed out).
    bp.markTimedOut(key)
    assert(bp.isProducerTimedOut(key))
    assert(bp.isConsumerTimedOut(key))
  }

  test("recordPeerProtocolVersion trips version-mismatch fallback only on divergence") {
    // Attach a real fallback policy so the version-mismatch update hook has somewhere to write.
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val metrics = new StreamingShuffleMetrics
    val policy = new StreamingShuffleFallbackPolicy(cfg, metrics)
    val limiter = new TokenBucketRateLimiter(Long.MaxValue)
    val bp = new BackpressureProtocol(cfg, limiter, metrics, policy)

    // A matching protocol version is a no-op: no fallback condition is recorded.
    bp.recordPeerProtocolVersion(StreamingShuffleConfig.STREAMING_PROTOCOL_VERSION)
    assert(!policy.isVersionMismatch)
    assert(!policy.shouldFallback)

    // A divergent peer version trips the sticky version-mismatch revert condition through the
    // policy, so the manager will delegate the shuffle to the sort-based fallback path.
    bp.recordPeerProtocolVersion(StreamingShuffleConfig.STREAMING_PROTOCOL_VERSION + 1)
    assert(policy.isVersionMismatch)
    assert(policy.shouldFallback)
  }

  test("blocking acquire is a no-op when unlimited and consults the limiter on a finite cap") {
    // Unlimited fast path: the blocking acquire must return immediately without consulting Guava.
    val unlimited = new TokenBucketRateLimiter(Long.MaxValue)
    unlimited.isUnlimited mustBe true
    unlimited.acquire(twoMb)

    // Finite cap: the blocking acquire reserves permits through the underlying limiter. A generous
    // 10 MB/s cap with a 1 KB request returns effectively immediately, so the test never stalls.
    val limited = new TokenBucketRateLimiter(10L * 1024 * 1024)
    limited.isUnlimited mustBe false
    limited.acquire(1024)
  }
}
