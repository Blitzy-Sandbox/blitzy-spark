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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

/**
 * Unit tests for [[BackpressureProtocol]] (F-107), the consumer-to-producer flow-control
 * protocol for the streaming shuffle data path. The protocol is a pure, lock-free in-memory
 * state machine, so these tests need neither a `SparkContext` nor any live Spark services.
 *
 * The suite covers the contractual behaviors of the protocol:
 *  - the 5 s flow-control heartbeat constant ([[BackpressureProtocol.HEARTBEAT_INTERVAL_MS]]);
 *  - the per-stream monotonic acknowledgment merge, exercised sequentially (out-of-order/
 *    duplicate), for cross-stream isolation, and under concurrent contention -- the linchpin
 *    correctness property;
 *  - the 10 s per-stream consumer-liveness detector
 *    ([[BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS]]), distinct from the 5 s heartbeat;
 *  - the lock-free credit window (token bucket, one token == one byte) whose capacity is capped
 *    at 80% of the supplied link capacity, including refill saturation;
 *  - the composed sustained-rate limiter wiring (F-110), exercised indirectly here;
 *  - edge-triggered backpressure activation telemetry; and
 *  - the [[BackpressureMessage]] algebraic data type: pattern-matchability and `Serializable`
 *    round-tripping, since the messages cross executor boundaries via RPC.
 */
class BackpressureProtocolSuite extends SparkFunSuite {

  /**
   * Builds a [[BackpressureProtocol]] together with the [[StreamingShuffleMetrics]] it tallies
   * backpressure activations through.
   *
   * `linkCapacityBytes` is a required constructor argument from which the 80%-capped credit
   * window is derived; `maxBandwidthMBps` configures the composed sustained-rate limiter (`0`
   * meaning unlimited). The utilization threshold is left at the production default (80%).
   */
  private def newProtocol(
      linkCapacityBytes: Long = 1000L,
      maxBandwidthMBps: Int = 0): (BackpressureProtocol, StreamingShuffleMetrics) = {
    val metrics = new StreamingShuffleMetrics
    val protocol = new BackpressureProtocol(metrics, linkCapacityBytes, maxBandwidthMBps)
    (protocol, metrics)
  }

  /** A representative consumer stream identity used to key acknowledgment/liveness assertions. */
  private def streamKey(
      shuffleId: Int = 1,
      partitionId: Int = 0,
      attemptId: Long = 0L,
      executorId: String = "exec-c"): StreamKey =
    StreamKey(shuffleId, partitionId, attemptId, executorId)

  test("HEARTBEAT_INTERVAL_MS is the contractual 5 s flow-control interval") {
    // AAP timing semantics: the flow-control heartbeat liveness window is exactly 5 seconds.
    assert(BackpressureProtocol.HEARTBEAT_INTERVAL_MS === 5000L)
  }

  test("mergeAck is monotonic per stream and never regresses on out-of-order/duplicate acks") {
    val (protocol, _) = newProtocol()
    val key = streamKey()
    assert(protocol.ackWatermark(key) === 0L)

    // mergeAck returns the stream's watermark after the merge.
    assert(protocol.mergeAck(key, 5L) === 5L)
    assert(protocol.ackWatermark(key) === 5L)

    // An older, out-of-order ack must never pull this stream's watermark backwards.
    assert(protocol.mergeAck(key, 3L) === 5L)
    assert(protocol.ackWatermark(key) === 5L)

    // A newer ack advances the watermark.
    assert(protocol.mergeAck(key, 10L) === 10L)
    assert(protocol.ackWatermark(key) === 10L)

    // A duplicate ack is idempotent.
    assert(protocol.mergeAck(key, 10L) === 10L)
    assert(protocol.ackWatermark(key) === 10L)
  }

  test("mergeAck is isolated per stream: an ack for one stream never advances another (M3)") {
    val (protocol, _) = newProtocol()
    val streamA = streamKey(partitionId = 0)
    val streamB = streamKey(partitionId = 1)

    // Advance stream A only; stream B is keyed independently and is unaffected.
    protocol.mergeAck(streamA, 100L)
    assert(protocol.ackWatermark(streamA) === 100L)
    assert(protocol.ackWatermark(streamB) === 0L)

    // A stale/out-of-scope ack targeting the unrelated stream B can never move A's watermark:
    // this is the core M3 protection -- per-stream keying contains cross-stream contamination.
    protocol.mergeAck(streamB, 1L)
    assert(protocol.ackWatermark(streamA) === 100L)
    assert(protocol.ackWatermark(streamB) === 1L)
    assert(protocol.trackedStreamCount === 2)
  }

  test("mergeAck stays monotonic under concurrent out-of-order merges for a stream") {
    val (protocol, _) = newProtocol()
    val key = streamKey()
    val numThreads = 8
    val numTasks = 256
    val maxSubmitted = 1000L
    val pool = Executors.newFixedThreadPool(numThreads)
    val startLatch = new CountDownLatch(1)
    try {
      // Task 0 guarantees the global maximum is submitted at least once; every other task
      // submits a random value strictly below it, exercising out-of-order/duplicate merges.
      val futures = (0 until numTasks).map { i =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            startLatch.await()
            val seqNo = if (i == 0) maxSubmitted else Random.nextInt(maxSubmitted.toInt).toLong
            protocol.mergeAck(key, seqNo)
          }
        })
      }
      // Release every worker simultaneously to maximize contention on the CAS loop.
      startLatch.countDown()
      // Joining via Future.get re-throws any exception raised inside a worker thread, so a
      // failure in any task fails the test instead of being silently swallowed.
      futures.foreach(_.get(30L, TimeUnit.SECONDS))
    } finally {
      pool.shutdown()
      assert(
        pool.awaitTermination(10L, TimeUnit.SECONDS),
        "backpressure concurrency pool did not terminate within the timeout")
    }
    // The stream's watermark equals the maximum value ever submitted and never regressed.
    assert(protocol.ackWatermark(key) === maxSubmitted)
  }

  test("consumer liveness: the 10 s missing-ack detector is keyed per stream (M4)") {
    val (protocol, _) = newProtocol()
    val key = streamKey()

    // AAP timing semantics: the consumer-liveness window is exactly 10 s, distinct from the 5 s
    // flow-control heartbeat.
    assert(BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS === 10000L)

    // A stream that has never signaled is treated as not-yet-started, not timed out (so a
    // consumer that has not begun cannot trigger a spurious fallback/invalidation).
    assert(!protocol.isConsumerTimedOut(key))
    assert(protocol.timedOutStreams.isEmpty)

    // A keyed ack stamps a fresh consumer signal, so the stream is live.
    protocol.mergeAck(key, 1L)
    assert(!protocol.isConsumerTimedOut(key))

    // Drive the last-signal timestamp to more than 10 s ago: the consumer is now timed out and
    // appears in the timed-out set the runtime fallback path consults.
    val staleNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(
      BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS + 1000L)
    protocol.markConsumerSignalAt(key, staleNanos)
    assert(protocol.isConsumerTimedOut(key))
    assert(protocol.timedOutStreams.contains(key))

    // A fresh signal clears the timeout.
    protocol.recordConsumerSignal(key)
    assert(!protocol.isConsumerTimedOut(key))
  }

  test("credit window admits within the 80%-capped budget and rejects beyond it") {
    // A 1000-byte link capacity yields an 800-byte credit window (the 80% BANDWIDTH_CAP_FACTOR).
    val (protocol, _) = newProtocol(linkCapacityBytes = 1000L)
    assert(protocol.capacityBytes === 800L)
    assert(protocol.availableCredits === 800L)

    // A request within budget is admitted and decrements the available credits.
    assert(protocol.tryAcquire(100L))
    assert(protocol.availableCredits === 700L)

    // A request larger than the remaining credits is rejected without mutating the window.
    assert(!protocol.tryAcquire(5000L))
    assert(protocol.availableCredits === 700L)

    // Draining the remaining budget succeeds; any further byte is then rejected.
    assert(protocol.tryAcquire(700L))
    assert(protocol.availableCredits === 0L)
    assert(!protocol.tryAcquire(1L))

    // Refilling returns credits and admission succeeds again.
    protocol.refill(500L)
    assert(protocol.availableCredits === 500L)
    assert(protocol.tryAcquire(1L))

    // Refill saturates at capacity and never exceeds it.
    protocol.refill(10000L)
    assert(protocol.availableCredits === protocol.capacityBytes)
  }

  test("sustained-rate cap is wired through the composed TokenBucketRateLimiter") {
    // A non-positive bandwidth ceiling yields an unlimited (no-op) limiter.
    val (unlimited, _) = newProtocol(linkCapacityBytes = 1024L, maxBandwidthMBps = 0)
    assert(!unlimited.isRateLimited)
    assert(unlimited.tryAcquireBandwidth(1))

    // A positive ceiling engages a real rate cap (the 80% factor is applied by the limiter's
    // factory). A freshly created bucket admits a modest non-blocking acquire immediately.
    val (limited, _) = newProtocol(linkCapacityBytes = 1024L, maxBandwidthMBps = 64)
    assert(limited.isRateLimited)
    assert(limited.tryAcquireBandwidth(1))
  }

  test("backpressure activation increments metrics.backpressureEvents on threshold crossing") {
    val (protocol, metrics) = newProtocol()
    assert(metrics.getBackpressureEvents === 0L)
    assert(!protocol.isBackpressureActive)

    // Crossing the default 80% threshold edge-triggers exactly one backpressure event.
    assert(protocol.updateUtilization(85))
    assert(protocol.isBackpressureActive)
    assert(metrics.getBackpressureEvents === 1L)

    // Staying above the threshold does not double-count the activation.
    assert(protocol.updateUtilization(90))
    assert(metrics.getBackpressureEvents === 1L)

    // Falling below releases the latch without counting a new event.
    assert(!protocol.updateUtilization(50))
    assert(!protocol.isBackpressureActive)
    assert(metrics.getBackpressureEvents === 1L)

    // Re-crossing the threshold counts a fresh activation.
    assert(protocol.updateUtilization(95))
    assert(metrics.getBackpressureEvents === 2L)
  }

  test("BackpressureMessage ADT is matchable and Serializable") {
    val messages: Seq[BackpressureMessage] = Seq(
      Heartbeat(
        executorId = "exec-1", shuffleId = 7, attemptId = 11L,
        reducePartitionRange = "[0,4)", timestampMs = 123L),
      Ack(
        shuffleId = 7, partitionId = 3, attemptId = 11L, executorId = "exec-1",
        seqNo = 42L, reclaimedBytes = 2048L),
      RateUpdate(
        shuffleId = 7, partitionId = 3, attemptId = 11L, maxBytesPerSec = 1000000L),
      Timeout(
        shuffleId = 7, partitionId = 3, attemptId = 11L, reason = "producer unresponsive"))

    // Every variant is an instance of the sealed supertype and is Serializable: the messages
    // cross executor boundaries via the backpressure RPC endpoint (F-108).
    messages.foreach { message =>
      assert(message.isInstanceOf[BackpressureMessage])
      assert(message.isInstanceOf[java.io.Serializable])
    }

    // The Ack variant pattern-matches and exposes its acknowledged sequence number.
    val ack: BackpressureMessage = Ack(
      shuffleId = 1, partitionId = 2, attemptId = 0L, executorId = "exec-1",
      seqNo = 99L, reclaimedBytes = 0L)
    val extractedSeqNo = ack match {
      case Ack(_, _, _, _, seqNo, _) => seqNo
      case _ => -1L
    }
    assert(extractedSeqNo === 99L)

    // A genuine Java-serialization round-trip preserves the payload (case-class equality).
    val original = Ack(
      shuffleId = 5, partitionId = 6, attemptId = 1L, executorId = "exec-2",
      seqNo = 7L, reclaimedBytes = 8L)
    val roundTripped = Utils.deserialize[Ack](Utils.serialize(original))
    assert(roundTripped === original)
  }

  test("heartbeat liveness: a freshly recorded heartbeat is not expired") {
    val (protocol, _) = newProtocol()
    protocol.recordHeartbeat()
    // The heartbeat was just stamped, so far less than the 5 s window has elapsed.
    assert(!protocol.isHeartbeatExpired)
    assert(protocol.millisSinceLastHeartbeat < BackpressureProtocol.HEARTBEAT_INTERVAL_MS)
  }

  test("arbitrate selects the most-starved stream and breaks ties by oldest") {
    val (protocol, _) = newProtocol()

    // No candidates yields no winner.
    assert(protocol.arbitrate(Seq.empty).isEmpty)

    val roomy = StreamPriority(partitionId = 1, remainingCapacityBytes = 500L, ageNanos = 10L)
    val young = StreamPriority(partitionId = 2, remainingCapacityBytes = 100L, ageNanos = 5L)
    val old = StreamPriority(partitionId = 3, remainingCapacityBytes = 100L, ageNanos = 50L)

    // young and old are equally starved (100 bytes); the older stream wins the tie.
    assert(protocol.arbitrate(Seq(roomy, young, old)).contains(old))

    // With no tie, the single most-starved stream wins.
    assert(protocol.arbitrate(Seq(roomy, young)).contains(young))
  }
}
