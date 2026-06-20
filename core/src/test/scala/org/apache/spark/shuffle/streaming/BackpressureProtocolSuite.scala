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
 * Unit tests for [[BackpressureProtocol]] -- the lock-free token-bucket plus heartbeat
 * flow-control state machine at the heart of the streaming shuffle backend. The suite validates
 * the behaviours the streaming design depends on:
 *
 *  1. Producer send gating -- the send-permit gates delegate to the composed
 *     [[TokenBucketRateLimiter]]: an unlimited limiter always admits, while a finite limiter
 *     eventually refuses once its byte budget is spent.
 *  2. Timeout detection -- a producer is declared stalled after
 *     [[StreamingShuffleConfig.PRODUCER_TIMEOUT_MS]] (5 s) without a heartbeat, and a consumer
 *     after [[StreamingShuffleConfig.CONSUMER_TIMEOUT_MS]] (10 s) without an ack; these are the
 *     streaming backend's failure-detection SLAs.
 *  3. Liveness reset -- a fresh heartbeat clears a prior producer timeout, so a transient blip
 *     that recovers within the window does not force a spurious partial-read invalidation.
 *  4. Single-episode accounting -- a contiguous throttle episode increments
 *     [[StreamingShuffleMetrics.backpressureEvents]] exactly once (never per byte and never per
 *     scan tick), guarding the < 1% telemetry-overhead budget.
 *
 * ==Determinism==
 *
 * The 5 s and 10 s timeouts are SLAs, yet the suite must run in milliseconds, so it NEVER sleeps
 * in real time. Production exposes the package-visible [[BackpressureProtocol.scanOnce]], which
 * runs one pass of the timeout state machine for an explicitly supplied `nowNanos`; that is the
 * seam a test uses to cross the 5 s / 10 s boundaries with an injected timestamp. Because
 * [[BackpressureProtocol.onHeartbeat]] and [[BackpressureProtocol.onAck]] stamp liveness with an
 * internal `System.nanoTime()` reading, each timeout test brackets the call with monotonic
 * readings (`before` / `after`) to bound the stamped instant, then evaluates `scanOnce` on both
 * sides of the threshold relative to those bounds.
 *
 * ==Fixtures==
 *
 * The suite is pure and deterministic: it needs no `SparkContext`, builds the protocol from a
 * standalone [[SparkConf]], and only the start/stop test spins up the background scan thread --
 * which it always tears down in a `finally`, so no daemon thread leaks.
 */
class BackpressureProtocolSuite extends SparkFunSuite with Matchers {

  // One second expressed in nanoseconds, the unit scanOnce compares heartbeat/ack deltas against.
  private val oneSecondNanos = TimeUnit.SECONDS.toNanos(1L)

  // The 5 s producer and 10 s consumer failure-detection windows, in nanoseconds, sourced from the
  // StreamingShuffleConfig constants rather than hard-coded, so the SLAs live in one place.
  private val producerTimeoutNanos =
    TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.PRODUCER_TIMEOUT_MS)
  private val consumerTimeoutNanos =
    TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.CONSUMER_TIMEOUT_MS)

  // The canonical 2 MB streaming block; every send permit in the suite reserves one block.
  private val blockBytes = 2 * 1024 * 1024

  /**
   * Builds a fresh protocol over a standalone [[SparkConf]] and a [[TokenBucketRateLimiter]] of the
   * requested rate, returning the protocol together with the limiter and the metrics holder so a
   * test can assert against any of the three. `bytesPerSec` defaults to `Long.MaxValue`, i.e. an
   * unlimited limiter that performs no throttling.
   */
  private def newProtocol(bytesPerSec: Long = Long.MaxValue)
      : (BackpressureProtocol, TokenBucketRateLimiter, StreamingShuffleMetrics) = {
    val cfg = new StreamingShuffleConfig(new SparkConf(false))
    val limiter = new TokenBucketRateLimiter(bytesPerSec)
    val metrics = new StreamingShuffleMetrics
    (new BackpressureProtocol(cfg, limiter, metrics), limiter, metrics)
  }

  test("unlimited limiter always grants send permits") {
    val (protocol, limiter, metrics) = newProtocol() // Long.MaxValue => unlimited
    limiter.isUnlimited mustBe true
    // The non-blocking gate must admit every 2 MB block when the limiter is unlimited ...
    (1 to 16).foreach(_ => protocol.tryAcquireSendPermit(blockBytes) mustBe true)
    // ... and the blocking gate must also return immediately, never waiting on the limiter.
    (1 to 16).foreach(_ => protocol.acquireSendPermit(blockBytes))
    // No send was ever throttled, so not a single backpressure episode may have been counted.
    metrics.backpressureEvents mustBe 0L
  }

  test("rate-limited limiter eventually throttles") {
    val (protocol, limiter, _) = newProtocol(bytesPerSec = 1024L)
    limiter.isUnlimited mustBe false
    // A 2 MB request dwarfs the 1 KB/s budget. Guava primes the bucket so the first attempt may be
    // admitted, but once the bucket is in debt further non-blocking attempts must be refused. We
    // require at least one refusal within a short, bounded burst -- no multi-second blocking.
    val refused = (1 to 8).exists(_ => !protocol.tryAcquireSendPermit(blockBytes))
    refused mustBe true
  }

  test("producer is declared timed out after 5s without a heartbeat") {
    val (protocol, _, _) = newProtocol()
    val key = StreamKey(0, 0L, 0)
    // Bracket the heartbeat so the stamped instant is known to lie in [before, after].
    val before = System.nanoTime()
    protocol.onHeartbeat(key)
    val after = System.nanoTime()
    // ~4 s after the heartbeat (one second inside the 5 s window): not yet timed out.
    protocol.scanOnce(before + producerTimeoutNanos - oneSecondNanos)
    protocol.isProducerTimedOut(key) mustBe false
    // ~6 s after the latest possible heartbeat instant (one second past the SLA): timed out.
    protocol.scanOnce(after + producerTimeoutNanos + oneSecondNanos)
    protocol.isProducerTimedOut(key) mustBe true
  }

  test("consumer is declared timed out after 10s without an ack") {
    val (protocol, _, _) = newProtocol()
    val key = StreamKey(0, 0L, 0)
    val before = System.nanoTime()
    protocol.onAck(key, 1024L)
    val after = System.nanoTime()
    // ~9 s after the ack (one second inside the 10 s window): not yet timed out.
    protocol.scanOnce(before + consumerTimeoutNanos - oneSecondNanos)
    protocol.isConsumerTimedOut(key) mustBe false
    // ~11 s after the latest possible ack instant (one second past the SLA): timed out.
    protocol.scanOnce(after + consumerTimeoutNanos + oneSecondNanos)
    protocol.isConsumerTimedOut(key) mustBe true
  }

  test("a fresh heartbeat resets producer liveness after a timeout") {
    val (protocol, _, _) = newProtocol()
    val key = StreamKey(1, 2L, 3)
    // Drive the stream into a producer timeout.
    protocol.onHeartbeat(key)
    val firstAfter = System.nanoTime()
    protocol.scanOnce(firstAfter + producerTimeoutNanos + oneSecondNanos)
    protocol.isProducerTimedOut(key) mustBe true
    // A fresh heartbeat clears the timeout flag immediately ...
    val reBefore = System.nanoTime()
    protocol.onHeartbeat(key)
    protocol.isProducerTimedOut(key) mustBe false
    // ... and a scan within 5 s of that heartbeat keeps the producer live.
    protocol.scanOnce(reBefore + producerTimeoutNanos - oneSecondNanos)
    protocol.isProducerTimedOut(key) mustBe false
  }

  test("a backpressure episode is counted once, not per scan tick") {
    val (protocol, limiter, metrics) = newProtocol(bytesPerSec = 1024L)
    limiter.isUnlimited mustBe false
    // Drive a single sustained throttle episode. `count` evaluates every element (it never
    // short-circuits), so all eight attempts run and several are refused after Guava's initial
    // prime -- yet the false->true throttle edge fires exactly once for the whole episode.
    val refusals = (1 to 8).count(_ => !protocol.tryAcquireSendPermit(blockBytes))
    refusals must be >= 1
    metrics.backpressureEvents mustBe 1L
    // The timeout scan never touches the backpressure-episode counter, so extra scan ticks must
    // NOT inflate it (this is what guards the < 1% telemetry-overhead budget).
    val now = System.nanoTime()
    val scanStep = TimeUnit.MILLISECONDS.toNanos(StreamingShuffleConfig.SCAN_INTERVAL_MS)
    (1 to 5).foreach(i => protocol.scanOnce(now + i * scanStep))
    metrics.backpressureEvents mustBe 1L
  }

  test("onRateLimitRequest lowers the shared limiter rate") {
    val (protocol, limiter, _) = newProtocol() // unlimited base; any positive request lowers it
    limiter.isUnlimited mustBe true
    val key = StreamKey(7, 8L, 9)
    // The consumer asks the producer to cap at 2048 B/s. With an unlimited base the effective
    // ceiling becomes exactly that request, so the shared limiter switches to a finite rate.
    protocol.onRateLimitRequest(key, 2048L)
    limiter.isUnlimited mustBe false
    limiter.currentBytesPerSecond mustBe 2048L
  }

  test("start then stop is idempotent and leak-free") {
    val (protocol, _, _) = newProtocol()
    try {
      protocol.start()
      protocol.start() // a second start while running must be a no-op
    } finally {
      protocol.stop()
      protocol.stop() // a second stop after stopping must be a no-op
    }
    // stop() clears all per-stream state and shuts the daemon scan scheduler down (no thread leak).
    protocol.activeStreamCount mustBe 0
  }
}
