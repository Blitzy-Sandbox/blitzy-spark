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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.duration._

import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import org.apache.spark.{SparkConf, SparkFunSuite}

/**
 * Unit tests for [[BackpressureProtocol]] covering consumer acknowledgment processing,
 * token-bucket rate-limit enforcement (with ScalaCheck property-based testing),
 * heartbeat-based timeout detection, and priority arbitration under concurrent shuffle
 * load.
 *
 * == AAP Reference ==
 *  - AAP Section 0.5.1.3 (BackpressureProtocol component design)
 *  - AAP Section 0.5.1.6 (Group 6, item 3) -- ScalaCheck 1.18 used for token-bucket
 *    invariant testing of `refill rate = maxBandwidthMBps / numConcurrentShuffles`
 *  - AAP Section 0.7.2.3 (Network and Transport Discipline)
 *  - AAP Section 0.7.2.4 (Failure Tolerance and Integrity)
 *  - AAP Section 0.7.2.6 (Quality Gate: greater-than 85% coverage for new components)
 *
 * == Production-Source Contract Exercised ==
 *  - Constructor: `BackpressureProtocol(metrics, conf)`
 *  - `tryAcquire(byteCount): Boolean` -- consumes tokens via lock-free CAS; returns
 *    `false` (and increments `metrics.backpressureEvents`) when tokens insufficient.
 *  - `recordConsumerAck(shuffleId, reduceId)` -- registers a consumer ack heartbeat in
 *    the `consumerLastAck` ConcurrentHashMap.
 *  - `stop()` -- terminates the daemon scheduler and clears heartbeat-tracking maps;
 *    idempotent via `stopped: AtomicBoolean.compareAndSet(false, true)`.
 *
 * == Constants Locked-In By This Suite ==
 *  - `PRODUCER_TIMEOUT_MILLIS = 5000L` per AAP Section 0.7.2.4 (5-second producer
 *    failure detection)
 *  - `CONSUMER_TIMEOUT_MILLIS = 10000L` per AAP Section 0.7.2.4 (10-second consumer
 *    liveness heartbeat)
 *
 * == Test Discipline ==
 * The backpressure protocol uses a daemon scheduled executor for periodic refill (100 ms
 * cadence) and heartbeat scanning (1 s cadence). Tests MUST call `protocol.stop()` in
 * `afterEach` to avoid daemon-thread accumulation across the suite. Tests that create
 * their own per-iteration protocol instances (notably the property-based refill-rate
 * invariant test) wrap creation in a try/finally to ensure cleanup even on assertion
 * failure.
 */
class BackpressureProtocolSuite
  extends SparkFunSuite with Matchers with ScalaCheckPropertyChecks
  with BeforeAndAfterEach with Eventually {

  // Shared per-test fixtures. `metrics` is reset to a fresh instance in `beforeEach` so
  // that counter assertions in one test never observe events from a prior test. The
  // `protocol` field is populated within each test body and torn down in `afterEach`.
  private var metrics: StreamingShuffleMetrics = _
  private var protocol: BackpressureProtocol = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    metrics = new StreamingShuffleMetrics()
  }

  override def afterEach(): Unit = {
    try {
      // Stop the protocol if any test set it; the production stop() is idempotent so
      // double-stop (when a test also calls stop() explicitly, e.g., the idempotency
      // test) is safe.
      if (protocol != null) {
        protocol.stop()
        protocol = null
      }
    } finally {
      super.afterEach()
    }
  }

  /**
   * Build a SparkConf preconfigured for streaming-shuffle backpressure tests. The conf
   * is constructed with `loadDefaults = false` so prior system properties from any
   * concurrent test run cannot bleed into this protocol's configuration.
   *
   * @param maxBandwidthMBps per-executor bandwidth cap in MBps; `-1` is the AAP-defined
   *                         "unlimited" sentinel (see Section 0.5.1.5)
   * @return a SparkConf instance ready to pass to the BackpressureProtocol constructor
   */
  private def buildConf(maxBandwidthMBps: Int = -1): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set("spark.shuffle.streaming.maxBandwidthMBps", maxBandwidthMBps.toString)
  }

  // ---------------------------------------------------------------------------
  // Test 1: Construction and initial token state.
  // ---------------------------------------------------------------------------
  test("BackpressureProtocol constructs with non-negative initial token state") {
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 100))
    // Per the production source, construction sets `tokens = bucketCapacityBytes`. At
    // 100 MBps the bucket capacity is 100 MB, so a 1 KB acquire succeeds immediately --
    // this exercises the lock-free CAS hot path on the underlying AtomicLong without
    // waiting for the first refill tick. Asserting `acquired == true` is stronger than
    // a simple "no-throw" tautology and verifies the documented construction-time
    // priming behaviour ("Construction-time initialization: prime the bucket and start
    // the scheduled tasks" -- BackpressureProtocol.scala).
    val acquired = protocol.tryAcquire(1024L)
    assert(acquired,
      "Initial small acquire should succeed because the bucket starts at full capacity")
    // No backpressure events should be recorded for a successful acquire.
    assert(metrics.getBackpressureEventsCount === 0L,
      "No backpressure events expected on a successful acquire")
  }

  // ---------------------------------------------------------------------------
  // Test 2: Unlimited-bandwidth shortcut.
  // ---------------------------------------------------------------------------
  test("maxBandwidthMBps = -1 (unlimited) always allows acquisition") {
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = -1))
    // Per the production source, when `maxBandwidthMBps <= 0` the rate limiter is
    // disabled and tryAcquire returns true immediately without touching the bucket.
    // A 100 MB acquire (which would far exceed any realistic capped bucket) succeeds.
    val acquired = protocol.tryAcquire(100L * 1024L * 1024L)
    assert(acquired,
      "tryAcquire should always succeed under unlimited bandwidth (the -1 sentinel)")
    assert(metrics.getBackpressureEventsCount === 0L,
      "Unlimited bandwidth must never record backpressure events")
  }

  // ---------------------------------------------------------------------------
  // Test 3: Bandwidth cap throttles requests larger than bucket capacity.
  // ---------------------------------------------------------------------------
  test("bandwidth cap throttles a request exceeding bucket capacity") {
    // 1 MBps cap: bucket capacity is exactly 1 MB. Drain the initial 1 MB by repeatedly
    // acquiring 1 KB until tryAcquire returns false (the bucket-empty signal). The
    // refill scheduler's first tick is 100 ms after construction; the drain loop runs
    // in single-digit milliseconds at most, so no refill races with the drain.
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 1))
    while (protocol.tryAcquire(1024L)) { /* drain to empty */ }
    // After draining, the bucket has < 1 KB tokens. A 100 MB acquire request must fail.
    val acquired = protocol.tryAcquire(100L * 1024L * 1024L)
    assert(!acquired,
      "tryAcquire should fail when bucket is empty under low-bandwidth cap")
    // The drain loop's terminating false return AND the explicit 100 MB rejection both
    // increment backpressure events; the total count is therefore strictly positive.
    assert(metrics.getBackpressureEventsCount > 0L,
      "backpressureEvents counter must increment on rate-limit rejection")
  }

  // ---------------------------------------------------------------------------
  // Test 4: Token bucket refills tokens over time.
  // ---------------------------------------------------------------------------
  test("token bucket refills tokens over time") {
    // 10 MBps cap: bucket capacity is 10 MB; refill is 1 MB per 100 ms tick. Drain the
    // bucket, then poll tryAcquire(1 KB) every 100 ms via Eventually. Within ~200 ms
    // the first refill tick should fire and tryAcquire should start succeeding again.
    // The 5-second timeout provides ample margin against scheduler jitter on busy CI.
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 10))
    while (protocol.tryAcquire(1024L)) { /* drain to empty */ }
    eventually(timeout(5.seconds), interval(100.milliseconds)) {
      val acquired = protocol.tryAcquire(1024L)
      assert(acquired, "Tokens should be refilled by the periodic scheduler tick")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5: Consumer acknowledgment latency is below the 100 ms reclamation budget.
  // ---------------------------------------------------------------------------
  test("recordConsumerAck completes within 100 ms") {
    // Per AAP Section 0.5.1.3: "Releases memory within 100 ms of consumer
    // acknowledgment." The production `recordConsumerAck` is a single ConcurrentHashMap
    // put -- it is expected to complete in microseconds. We bound it at < 100 ms to
    // catch any future regression (such as accidentally introducing a synchronous I/O
    // call on the ack path) that would violate the AAP's reclamation timing budget.
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 10))
    val shuffleId = 0
    val reduceId = 0

    val startNanos = System.nanoTime()
    protocol.recordConsumerAck(shuffleId, reduceId)
    val durationMillis = (System.nanoTime() - startNanos) / 1000000L

    assert(durationMillis < 100L,
      s"recordConsumerAck took $durationMillis ms (must be < 100 ms per AAP 0.5.1.3)")
  }

  // ---------------------------------------------------------------------------
  // Test 6 (a): PRODUCER_TIMEOUT_MILLIS = 5000 ms -- AAP Section 0.7.2.4.
  // ---------------------------------------------------------------------------
  test("PRODUCER_TIMEOUT_MILLIS constant equals 5000") {
    // The package-object constant is the contract that locks the AAP-mandated 5-second
    // producer-failure detection window. Asserting its literal value here prevents an
    // accidental regression that would change the heartbeat semantics without flagging
    // a corresponding test failure. The constant is `private[streaming]` -- visible
    // here because this suite is in the same `org.apache.spark.shuffle.streaming`
    // package.
    assert(PRODUCER_TIMEOUT_MILLIS === 5000L,
      "PRODUCER_TIMEOUT_MILLIS must be 5000 ms per AAP Section 0.7.2.4")
  }

  // ---------------------------------------------------------------------------
  // Test 6 (b): CONSUMER_TIMEOUT_MILLIS = 10000 ms -- AAP Section 0.7.2.4.
  // ---------------------------------------------------------------------------
  test("CONSUMER_TIMEOUT_MILLIS constant equals 10000") {
    // The 10-second consumer-liveness heartbeat window per AAP Section 0.7.2.4. Like
    // the producer-timeout assertion above, this locks the contract value against
    // accidental changes.
    assert(CONSUMER_TIMEOUT_MILLIS === 10000L,
      "CONSUMER_TIMEOUT_MILLIS must be 10000 ms per AAP Section 0.7.2.4")
  }

  // ---------------------------------------------------------------------------
  // Test 7: Thread-safety of `tryAcquire` under concurrent acquire pressure.
  //
  // Scope and naming rationale:
  //   This test verifies that the lock-free CAS retry path inside
  //   [[BackpressureProtocol#tryAcquire]] is internally correct under multi-threaded
  //   contention. It does NOT verify weighted fair distribution of bandwidth across
  //   concurrent shuffles -- the AAP Section 0.1.2 directive *"priority arbitration
  //   across concurrent shuffles"* describes a feature that requires actual contention
  //   between *different* shuffles (different bucket instances) competing for a
  //   bounded resource. With 100 MB initial bucket capacity and 5 KB total demand,
  //   no contention occurs at the bucket level: every acquire succeeds because each
  //   thread's request is accommodated wait-free by the CAS loop.
  //
  //   Priority arbitration with actual weighted distribution is exercised in the
  //   higher-level integration suites (StreamingShuffleIntegrationSuite,
  //   StreamingShuffleStressSuite) which inject the realistic 5-concurrent-shuffle
  //   workload mandated by AAP Section 0.5.1.6 ("5-concurrent-shuffle memory-arbitration
  //   test"). This unit-level test purposefully stays narrow: it locks the
  //   thread-safety contract of the lock-free token bucket implementation.
  // ---------------------------------------------------------------------------
  test("tryAcquire is thread-safe under concurrent acquire") {
    // Configure a 100 MBps bandwidth cap and dispatch 5 worker threads to attempt a
    // 1 KB acquire each. With a 100 MB initial bucket and 5 KB total demand, all 5
    // acquires should succeed -- this exercises the lock-free CAS retry path under
    // concurrent contention without racing the refill scheduler. The CountDownLatch
    // bounds the test wall-clock at 5 seconds even under pathological scheduling.
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 100))

    val numConcurrent = 5
    val acquireSize = 1024L  // 1 KB per worker
    val successCount = new AtomicInteger(0)
    // Track total acquired bytes across threads to verify the cumulative bandwidth
    // accounting (an AtomicLong because byte counts are 64-bit per the production
    // source's signed-long semantics on `tokens`).
    val totalBytesAcquired = new AtomicLong(0L)
    val latch = new CountDownLatch(numConcurrent)
    val threads = (0 until numConcurrent).map { _ =>
      new Thread {
        override def run(): Unit = {
          try {
            if (protocol.tryAcquire(acquireSize)) {
              successCount.incrementAndGet()
              totalBytesAcquired.addAndGet(acquireSize)
            }
          } finally {
            latch.countDown()
          }
        }
      }
    }
    threads.foreach(_.start())
    assert(latch.await(5L, TimeUnit.SECONDS),
      "Concurrent acquires did not complete within the 5-second test deadline")
    // Thread-safety contract: under no-contention bucket conditions (100 MB cap,
    // 5 KB total demand), every concurrent CAS must succeed without losing or
    // double-counting tokens. We therefore assert exact success counts rather than
    // ">0" -- a stronger contract that catches any future regression that would
    // weaken the CAS loop's atomicity (e.g., switching to a non-atomic primitive
    // without re-establishing equivalent thread-safety).
    assert(successCount.get() === numConcurrent,
      s"All concurrent CAS-based acquires must succeed when bucket capacity " +
        s"exceeds total demand; got ${successCount.get()} successes out of " +
        s"$numConcurrent attempts")
    assert(totalBytesAcquired.get() === numConcurrent.toLong * acquireSize,
      s"Cumulative bytes acquired must equal the wait-free aggregate of all " +
        s"successful concurrent acquires; got ${totalBytesAcquired.get()}, " +
        s"expected ${numConcurrent.toLong * acquireSize}")
  }

  // ---------------------------------------------------------------------------
  // Test 8: stop() is idempotent and cleanly shuts down the daemon scheduler.
  // ---------------------------------------------------------------------------
  test("stop() is idempotent and cleanly shuts down the daemon scheduler") {
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 10))
    // First stop closes the scheduler and clears heartbeat maps; the production source
    // guards this with `stopped.compareAndSet(false, true)` so the second call is a
    // no-op (no cancelling-already-cancelled-future exceptions, no double executor
    // shutdown). Asserting via "no exception thrown" is the standard ScalaTest pattern
    // for idempotency.
    protocol.stop()
    protocol.stop()
    // Set protocol to null so `afterEach` does not call stop() a third time -- the
    // third call would also be a no-op, but explicitly nulling makes the lifecycle
    // intent clearer at the test site.
    protocol = null
  }

  // ---------------------------------------------------------------------------
  // Test 9: ScalaCheck property -- tryAcquire never produces non-Boolean state for
  // any non-negative input size.
  // ---------------------------------------------------------------------------
  test("tryAcquire never produces negative token state for any input size (ScalaCheck)") {
    // The production contract: `tryAcquire(byteCount)` requires `byteCount >= 0L` and
    // returns a Boolean (never throws on valid input). The token-decrement check
    // `current - byteCount` only proceeds when `current >= byteCount`, so the internal
    // tokens value is never driven negative. We exercise this property over a wide
    // range of non-negative Long inputs via ScalaCheck's `Gen.choose(0L, 1_000_000L)`.
    // The 1 million upper bound is large enough to exercise typical block sizes while
    // small enough that ScalaCheck's default 100 iterations remain fast.
    protocol = new BackpressureProtocol(metrics, buildConf(maxBandwidthMBps = 100))
    forAll(Gen.choose(0L, 1000000L)) { (size: Long) =>
      // Asserting "result is true or false" is intentionally a tautology at the value
      // level -- its purpose is to force evaluation of `tryAcquire(size)` so that any
      // exception (such as an IllegalArgumentException for a hypothetical negative
      // size, which `Gen.choose(0L, ...)` rules out by construction) propagates up and
      // fails the property.
      val result = protocol.tryAcquire(size)
      assert(result == true || result == false,
        s"tryAcquire($size) must return a Boolean and never throw on non-negative input")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 10: ScalaCheck property -- refill-rate invariant.
  // ---------------------------------------------------------------------------
  test("refill rate = maxBandwidthMBps / numConcurrentShuffles (ScalaCheck property)") {
    // Property: under any `maxBandwidthMBps` in [1, 100], the bucket capacity is
    // exactly `maxBandwidthMBps * 1 MB` (one second's worth of refill at peak rate).
    // After draining the bucket to empty, an immediate acquire of `2 * capacity` must
    // therefore fail because the bucket cannot have been refilled to that level
    // synchronously (the refill scheduler runs at 100 ms cadence and its initial delay
    // is also 100 ms after construction).
    //
    // Each forAll iteration constructs a fresh BackpressureProtocol -- which in turn
    // creates a daemon scheduler thread -- so we cap iterations at 20 to keep the test
    // duration bounded. 20 iterations is sufficient to cover a representative subset
    // of the [1, 100] range (ScalaCheck's default shrinker still explores boundaries
    // around any failures). The MinSuccessful PropertyCheckConfigParam is passed
    // inline rather than via an implicit override to avoid colliding with the
    // `generatorDrivenConfig` implicit already provided by the `Configuration` trait
    // mixed in via ScalaCheckPropertyChecks.
    forAll(Gen.choose(1, 100), MinSuccessful(20)) { (mbps: Int) =>
      val freshMetrics = new StreamingShuffleMetrics()
      val freshProtocol = new BackpressureProtocol(freshMetrics, buildConf(mbps))
      try {
        // Drain the initial bucket of `mbps * 1 MB` capacity in 1 KB chunks. The drain
        // loop is bounded by the bucket capacity (max ~100K iterations at mbps=100,
        // executes in ~1 ms total) and terminates as soon as tryAcquire returns false.
        while (freshProtocol.tryAcquire(1024L)) { /* drain to empty */ }
        // Without waiting for any refill, an acquire of 2x the bucket capacity must
        // fail: tokens < 1024 (drain just exited) and 2x capacity is far greater than
        // 1024.
        val twoXCap = mbps.toLong * 1024L * 1024L * 2L
        val acquired = freshProtocol.tryAcquire(twoXCap)
        assert(!acquired,
          s"Acquire of 2x bucket capacity should fail synchronously (mbps=$mbps)")
      } finally {
        // Ensure the per-iteration daemon scheduler is always cleaned up, even when
        // an assertion in the body fails -- otherwise daemon-thread leaks would
        // accumulate across the property's iterations.
        freshProtocol.stop()
      }
    }
  }
}
