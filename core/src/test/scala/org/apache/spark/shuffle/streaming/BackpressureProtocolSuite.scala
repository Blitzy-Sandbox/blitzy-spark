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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

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

  /**
   * Reflectively invokes the private daemon `scan()` so the timeout-driven liveness cleanup can be
   * exercised synchronously and deterministically -- without starting the background scheduler
   * (which would race the assertions) and without modifying production code to widen `scan`'s
   * visibility. The method is materialized in bytecode because the daemon scheduler invokes it.
   */
  private def invokeScan(protocol: BackpressureProtocol): Unit = {
    val method = classOf[BackpressureProtocol].getDeclaredMethod("scan")
    method.setAccessible(true)
    method.invoke(protocol)
  }

  /**
   * Reflectively reads the private producer-liveness map so a test can both seed stale/fresh
   * entries with explicit timestamps and assert the post-scan cleanup. The map is keyed by mapId
   * (a boxed [[java.lang.Long]]) with a last-active epoch-millis timestamp value.
   */
  private def producerMap(
      protocol: BackpressureProtocol): ConcurrentHashMap[java.lang.Long, java.lang.Long] = {
    val field = classOf[BackpressureProtocol].getDeclaredField("producerLastActive")
    field.setAccessible(true)
    field.get(protocol).asInstanceOf[ConcurrentHashMap[java.lang.Long, java.lang.Long]]
  }

  /**
   * Reflectively reads the private consumer-liveness map so a test can assert which executors
   * survive a scan. Consumer heartbeats are seeded through the public `onHeartbeat` API (whose
   * timestamp argument is settable), so only the read side needs reflection here.
   */
  private def consumerMap(
      protocol: BackpressureProtocol): ConcurrentHashMap[String, java.lang.Long] = {
    val field = classOf[BackpressureProtocol].getDeclaredField("consumerLastSeen")
    field.setAccessible(true)
    field.get(protocol).asInstanceOf[ConcurrentHashMap[String, java.lang.Long]]
  }

  /**
   * Reflectively reads one of the private security-bound `Long` constants (`MAX_ACK_BYTES`,
   * `MAX_CREDIT_BYTES`, `MAX_RATE_BYTES_PER_SEC`, `MAX_HEARTBEAT_SKEW_MS`). These bounds are
   * intentionally not exposed by the production class, so the input-validation tests read them
   * here rather than duplicating the literals -- keeping the assertions pinned to the exact
   * production values without modifying production code. The fields are materialized in bytecode
   * (they are read by the validation paths), so the reflective read is stable.
   */
  private def readPrivateLong(protocol: BackpressureProtocol, name: String): Long = {
    val field = classOf[BackpressureProtocol].getDeclaredField(name)
    field.setAccessible(true)
    field.get(protocol).asInstanceOf[Long]
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

  test("acquire enforces the send-credit gate when bandwidth limiting is enabled") {
    // With a positive maxBandwidthMBps the credit gate is enabled, and pairing it with an
    // unlimited pass-through rate limiter makes the token bucket the SOLE decider of acquire():
    // the rate limiter always grants, so any denial is unambiguously a credit exhaustion.
    val conf = new SparkConf(false).set("spark.shuffle.streaming.maxBandwidthMBps", "100")
    val metrics = new StreamingShuffleMetrics()
    val protocol = new BackpressureProtocol(
      new StreamingShuffleConfig(conf), metrics, new TokenBucketRateLimiter(0, 1))

    // No credit has been released yet, so a positive request is denied and recorded as a single
    // backpressure event; the credit balance stays at zero.
    assert(!protocol.acquire(1024L))
    assert(metrics.backpressureCounter.getCount == 1L)
    assert(protocol.status._2 == 0L)

    // A consumer acknowledgment releases send credit equal to the bytes it reports consuming.
    protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = 0, bytesConsumed = 4096L,
      seqNumber = 1)
    assert(protocol.status._2 == 4096L)

    // A request within the available credit is granted and debits the bucket by exactly that many
    // bytes; no additional backpressure event is recorded on the success path.
    assert(protocol.acquire(1024L))
    assert(protocol.status._2 == 3072L)
    assert(metrics.backpressureCounter.getCount == 1L)

    // A request exceeding the remaining credit is denied and recorded as a second backpressure
    // event, leaving the surviving credit untouched.
    assert(!protocol.acquire(4096L))
    assert(metrics.backpressureCounter.getCount == 2L)
    assert(protocol.status._2 == 3072L)
  }

  test("scan evicts producers idle past 5s and consumers silent past 10s") {
    // A visible metrics holder is constructed inline (newProtocol hides its own) so the consumer
    // eviction's backpressure-event side effect can be asserted. scan() is driven reflectively
    // instead of via start(), so the assertions never race the background scheduler.
    val metrics = new StreamingShuffleMetrics()
    val protocol = new BackpressureProtocol(
      new StreamingShuffleConfig(new SparkConf(false)), metrics, new TokenBucketRateLimiter(0, 1))
    val now = System.currentTimeMillis()

    // Producer liveness is tracking-only (no metric). Seed one producer idle beyond the 5s
    // timeout and one just inside it; the scan must drop only the stale entry.
    producerMap(protocol).put(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(now - 6000L))
    producerMap(protocol).put(java.lang.Long.valueOf(2L), java.lang.Long.valueOf(now - 4000L))
    // Consumer heartbeats are seeded through the public API with explicit stale/fresh timestamps.
    protocol.onHeartbeat("stale-consumer", now - 11000L)
    protocol.onHeartbeat("fresh-consumer", now - 9000L)

    invokeScan(protocol)

    // Stale producer dropped, fresh producer retained; producer eviction emits no metric.
    assert(!producerMap(protocol).containsKey(java.lang.Long.valueOf(1L)))
    assert(producerMap(protocol).containsKey(java.lang.Long.valueOf(2L)))
    // Stale consumer removed and recorded as exactly one backpressure event; fresh one retained.
    assert(!consumerMap(protocol).containsKey("stale-consumer"))
    assert(consumerMap(protocol).containsKey("fresh-consumer"))
    assert(metrics.backpressureCounter.getCount == 1L)
  }

  test("scan swallows a Throwable from metrics so the daemon is never disabled") {
    // The scan body is wrapped in a Throwable guard so a transient failure cannot cancel the
    // scheduled task and silently disable backpressure. A metrics stub throws exactly once from
    // incBackpressureEvents to simulate that transient failure during consumer eviction.
    val thrown = new AtomicBoolean(false)
    val metrics = new StreamingShuffleMetrics() {
      override def incBackpressureEvents(): Unit = {
        if (thrown.compareAndSet(false, true)) {
          throw new RuntimeException("injected transient metrics failure")
        }
        super.incBackpressureEvents()
      }
    }
    val protocol = new BackpressureProtocol(
      new StreamingShuffleConfig(new SparkConf(false)), metrics, new TokenBucketRateLimiter(0, 1))
    val now = System.currentTimeMillis()

    // First scan: evicting the stale consumer triggers incBackpressureEvents, which throws. The
    // guard must swallow it (invokeScan does not propagate) and the throwing call records nothing.
    protocol.onHeartbeat("stale-1", now - 11000L)
    invokeScan(protocol)
    assert(metrics.backpressureCounter.getCount == 0L)

    // Second scan: the injected failure has already fired, so metrics now delegate to super. A
    // fresh stale consumer is evicted and recorded, proving the scan survived the Throwable.
    protocol.onHeartbeat("stale-2", now - 11000L)
    invokeScan(protocol)
    assert(metrics.backpressureCounter.getCount == 1L)
  }

  test("onConsumerAck drops acks with negative identifiers") {
    val protocol = newProtocol()
    // Each field is a distinct malformed (possibly hostile) RPC payload: shuffle/map/reduce ids
    // and the sequence number are always non-negative in Spark. A negative value must be dropped
    // before any credit is released or any liveness / active-shuffle entry is created.
    protocol.onConsumerAck(shuffleId = -1, mapId = 0L, reduceId = 0, bytesConsumed = 4096L,
      seqNumber = 1)
    protocol.onConsumerAck(shuffleId = 0, mapId = -1L, reduceId = 0, bytesConsumed = 4096L,
      seqNumber = 1)
    protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = -1, bytesConsumed = 4096L,
      seqNumber = 1)
    protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = 0, bytesConsumed = 4096L,
      seqNumber = -1)
    // No credit released, no active shuffle recorded, no producer-liveness entry created.
    assert(protocol.status == (0, 0L))
    assert(producerMap(protocol).isEmpty)
  }

  test("onConsumerAck drops an ack above the per-ack byte ceiling") {
    val protocol = newProtocol()
    val maxAckBytes = readPrivateLong(protocol, "MAX_ACK_BYTES")
    // One byte over the documented per-ack maximum marks a malformed ack that would attempt to
    // release an impossible amount of credit in a single step; the whole message is dropped, so no
    // credit is released and no liveness / active-shuffle state is recorded.
    protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = 0,
      bytesConsumed = maxAckBytes + 1L, seqNumber = 1)
    assert(protocol.status == (0, 0L))
    assert(producerMap(protocol).isEmpty)
  }

  test("onConsumerAck saturates send credit and never overflows") {
    val protocol = newProtocol()
    val maxAckBytes = readPrivateLong(protocol, "MAX_ACK_BYTES")
    val maxCreditBytes = readPrivateLong(protocol, "MAX_CREDIT_BYTES")
    // Each ack carries the maximum permitted per-ack byte count. With a naive additive counter,
    // repeating it would overflow the credit AtomicLong into negative territory and defeat
    // backpressure. The saturating add must instead clamp at the ceiling and stay strictly
    // positive no matter how many maximal acks arrive.
    (1 to 8).foreach { seq =>
      protocol.onConsumerAck(shuffleId = 0, mapId = 0L, reduceId = 0,
        bytesConsumed = maxAckBytes, seqNumber = seq)
    }
    val (_, creditNow) = protocol.status
    assert(creditNow == maxCreditBytes)
    assert(creditNow > 0L)
  }

  test("onThrottleRequest drops a negative shuffleId and records no event") {
    val metrics = new StreamingShuffleMetrics()
    val protocol = new BackpressureProtocol(
      new StreamingShuffleConfig(new SparkConf(false)),
      metrics,
      new TokenBucketRateLimiter(100, 1))
    // A negative shuffle id is malformed and must be dropped before any state is recorded: it may
    // create neither an active-shuffle entry nor a backpressure event.
    protocol.onThrottleRequest(shuffleId = -1, targetBytesPerSec = 4096L)
    assert(protocol.status._1 == 0)
    assert(metrics.backpressureCounter.getCount == 0L)
    // A well-formed request (non-negative id) is still recorded as a backpressure event, proving
    // only the malformed request was rejected.
    protocol.onThrottleRequest(shuffleId = 0, targetBytesPerSec = 4096L)
    assert(protocol.status._1 == 1)
    assert(metrics.backpressureCounter.getCount == 1L)
  }

  test("onHeartbeat drops empty ids and implausible timestamps") {
    val protocol = newProtocol()
    val maxSkewMs = readPrivateLong(protocol, "MAX_HEARTBEAT_SKEW_MS")
    val now = System.currentTimeMillis()
    // An empty id cannot be correlated; a non-positive or far-future timestamp would corrupt the
    // silence computation in scan() (a future timestamp makes a dead consumer look alive). All are
    // dropped, leaving the consumer-liveness map empty.
    protocol.onHeartbeat(executorId = "", timestampMillis = now)
    protocol.onHeartbeat(executorId = "exec-zero", timestampMillis = 0L)
    protocol.onHeartbeat(executorId = "exec-neg", timestampMillis = -1L)
    protocol.onHeartbeat(executorId = "exec-future", timestampMillis = now + maxSkewMs + 60000L)
    assert(consumerMap(protocol).isEmpty)
    // A well-formed heartbeat (non-empty id, plausible timestamp) is still recorded.
    protocol.onHeartbeat(executorId = "exec-ok", timestampMillis = now)
    assert(consumerMap(protocol).containsKey("exec-ok"))
    assert(!consumerMap(protocol).containsKey("exec-future"))
  }

}
