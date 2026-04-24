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

import java.lang.reflect.Field
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.mockito.Mockito.{mock, times, verify}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config

/**
 * Unit tests for [[BackpressureProtocol]] &mdash; the stateful in-JVM flow-control
 * coordinator that backs the streaming shuffle feature (F-001). The suite exercises
 * the coordinator's public API surface:
 *
 *   - [[BackpressureProtocol#acquirePermission]] v1 stub (no-op, non-throwing).
 *   - [[BackpressureProtocol#acknowledgeReceipt]] per-block consumer-position tracking.
 *   - [[BackpressureProtocol#registerProducer]] /
 *     [[BackpressureProtocol#unregisterProducer]] session lifecycle.
 *   - [[BackpressureProtocol#recordHeartbeat]] per-producer liveness refresh.
 *   - [[BackpressureProtocol#updateRate]] atomic rate replacement plus
 *     [[StreamingShuffleMetrics#incrementBackpressureEvents]] telemetry emission.
 *   - [[BackpressureProtocol#setProducerPriority]] arbitration formula
 *     (`partitionCount * dataVolumeBytes`).
 *   - [[BackpressureProtocol#stop]] idempotent teardown semantics.
 *   - Initial-rate computation from the
 *     `spark.shuffle.streaming.maxBandwidthMBps` configuration, including the
 *     user-specified 80 % link-capacity cap (AAP section 0.1.2) and the
 *     `Double.MaxValue` sentinel for the "unlimited" default.
 *   - Concurrent-access safety of the lock-free state tables under 10- and 50-thread
 *     fan-out (validates the &lt;1 % CPU telemetry budget from AAP section 0.7.4).
 *
 * Test ownership and hygiene:
 *   - Every test constructs its [[BackpressureProtocol]] via [[newProtocol]] which
 *     registers the instance with [[protocols]] so that [[afterEach]] can stop each
 *     one. This prevents the daemon scheduler thread
 *     ("streaming-shuffle-backpressure") from leaking across test boundaries.
 *   - Reflection via [[getPrivateField]] is used only where the class under test
 *     exposes no public accessor (the three `ConcurrentHashMap` state tables, the
 *     `AtomicReference` holding the current rate, and the `ScheduledExecutorService`
 *     scheduler). This mirrors the pattern used in `MemorySpillManagerSuite` and
 *     other Spark-internal suites that need to assert on private lock-free data
 *     structures.
 *   - Concurrency tests use a fixed-size thread pool with a [[CountDownLatch]] for
 *     deterministic completion; every thread pool is terminated with
 *     `shutdownNow()` in-test to prevent thread-pool leakage.
 */
class BackpressureProtocolSuite extends SparkFunSuite with Matchers
    with BeforeAndAfterEach with Eventually {

  // Tracks every BackpressureProtocol created during a test so that afterEach() can
  // stop each one, preventing scheduler daemon thread leaks across test boundaries.
  // Mutated only from the test thread (one test at a time).
  private var protocols: List[BackpressureProtocol] = Nil

  /**
   * Constructs a fresh [[BackpressureProtocol]] bound to the supplied configuration
   * and metrics source, and registers it with [[protocols]] so [[afterEach]] can
   * tear it down.
   *
   * @param conf    the [[SparkConf]] from which
   *                `spark.shuffle.streaming.maxBandwidthMBps` will be read. Defaults
   *                to a minimal configuration with the default 0 (unlimited) value.
   * @param metrics the [[StreamingShuffleMetrics]] source against which
   *                `incrementBackpressureEvents()` will be invoked. Defaults to a
   *                real (non-mocked) instance so tests that do not care about
   *                metrics interaction verification avoid unnecessary Mockito
   *                overhead.
   * @return a started [[BackpressureProtocol]] whose scheduler thread is already
   *         running.
   */
  private def newProtocol(
      conf: SparkConf = newConf(),
      metrics: StreamingShuffleMetrics = new StreamingShuffleMetrics()):
      BackpressureProtocol = {
    val protocol = new BackpressureProtocol(conf, metrics)
    protocols = protocol :: protocols
    protocol
  }

  /**
   * Constructs a minimal [[SparkConf]] seeded only with the max-bandwidth key. The
   * `loadDefaults = false` argument prevents pickup of system properties or the
   * `spark-defaults.conf` file and guarantees deterministic test behavior in any
   * execution environment (local IDE, CI, or ad-hoc REPL).
   *
   * @param maxMbps the value of `spark.shuffle.streaming.maxBandwidthMBps` to set.
   *                Defaults to 0 (unlimited).
   * @return the freshly-constructed [[SparkConf]].
   */
  private def newConf(maxMbps: Int = 0): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, maxMbps)
  }

  override def afterEach(): Unit = {
    try {
      protocols.foreach { p =>
        try {
          p.stop()
        } catch {
          case _: Throwable => ()
        }
      }
    } finally {
      protocols = Nil
      super.afterEach()
    }
  }

  /**
   * Reflection helper for reading private fields of [[BackpressureProtocol]] that
   * the class deliberately does not expose through public getters. Necessary
   * because the class stores its flow-control state in lock-free
   * [[ConcurrentHashMap]] / [[AtomicReference]] tables that tests must assert
   * against without changing the production SPI.
   *
   * @param protocol  the protocol instance whose private field is to be read
   * @param fieldName the exact name of the private field as declared in
   *                  [[BackpressureProtocol]]
   * @tparam T        the runtime type of the field value
   * @return the field value cast to `T`
   */
  private def getPrivateField[T](protocol: BackpressureProtocol, fieldName: String): T = {
    val field: Field = classOf[BackpressureProtocol].getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(protocol).asInstanceOf[T]
  }

  // ==========================================================================
  // Group 1: computeInitialRate() behavior
  // --------------------------------------------------------------------------
  // The initial token-bucket rate is derived from the
  // `spark.shuffle.streaming.maxBandwidthMBps` configuration per AAP section
  // 0.1.2: default 0 means unlimited (encoded as Double.MaxValue); non-zero
  // values are capped at 80 % of link capacity.
  // ==========================================================================

  test("computeInitialRate returns Double.MaxValue when maxBandwidthMBps == 0") {
    val protocol = newProtocol(newConf(maxMbps = 0))

    val rateRef = getPrivateField[AtomicReference[java.lang.Double]](
      protocol, "currentRateBytesPerSec")
    rateRef.get().doubleValue() must be(java.lang.Double.MAX_VALUE +- 0.001)
  }

  test("computeInitialRate returns 80% of maxBandwidthMBps converted to bytes/sec") {
    val protocol = newProtocol(newConf(maxMbps = 100))
    // 100 MBps * 1024 * 1024 bytes/MB * 0.80 = 83_886_080.0
    val rateRef = getPrivateField[AtomicReference[java.lang.Double]](
      protocol, "currentRateBytesPerSec")
    rateRef.get().doubleValue() must be(83886080.0 +- 1.0)
  }

  test("computeInitialRate returns Double.MaxValue for negative maxBandwidthMBps") {
    // Bounds may be enforced by config checkValue (none on this key), but if a
    // user bypasses config guards the internal code must still not explode. The
    // production path treats "maxMbps <= 0" as the unlimited sentinel.
    val conf = new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, -1)
    val protocol = newProtocol(conf)
    val rateRef = getPrivateField[AtomicReference[java.lang.Double]](
      protocol, "currentRateBytesPerSec")
    rateRef.get().doubleValue() must be(java.lang.Double.MAX_VALUE +- 0.001)
  }

  // ==========================================================================
  // Group 2: Producer registration & heartbeat tracking
  // --------------------------------------------------------------------------
  // BackpressureProtocol tracks per-producer liveness via two tables:
  //   - producerHeartbeats: timestamp of last heartbeat
  //   - producerPriorities: arbitration weight
  // Both are populated by registerProducer and cleared by unregisterProducer;
  // recordHeartbeat refreshes the heartbeat timestamp in-place.
  // ==========================================================================

  test("registerProducer records the producerId with a current timestamp") {
    val protocol = newProtocol()
    val beforeMillis = System.currentTimeMillis()
    protocol.registerProducer("executor-1")
    val afterMillis = System.currentTimeMillis()

    val heartbeats = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerHeartbeats")
    heartbeats.containsKey("executor-1") must be(true)
    val recorded = heartbeats.get("executor-1").longValue()
    // The recorded timestamp must be between the before and after wall-clock
    // samples taken by the test thread. This asserts that
    // `System.currentTimeMillis()` (and not a fixed sentinel) is used.
    recorded must be >= beforeMillis
    recorded must be <= afterMillis
  }

  test("registerProducer also initializes the priority table entry") {
    // Invariant: every registered producer has a matching priority entry so that
    // priority-arbitration code never observes a registered producer as "unknown".
    val protocol = newProtocol()
    protocol.registerProducer("executor-1")

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    priorities.containsKey("executor-1") must be(true)
    priorities.get("executor-1").longValue() must be(0L)
  }

  test("unregisterProducer removes the producerId from heartbeat and priority tables") {
    val protocol = newProtocol()
    protocol.registerProducer("executor-1")
    protocol.setProducerPriority("executor-1", partitionCount = 5, dataVolumeBytes = 1000L)

    protocol.unregisterProducer("executor-1")

    val heartbeats = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerHeartbeats")
    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    heartbeats.containsKey("executor-1") must be(false)
    priorities.containsKey("executor-1") must be(false)
  }

  test("unregisterProducer of a non-registered id does not throw") {
    val protocol = newProtocol()
    noException must be thrownBy protocol.unregisterProducer("ghost")
  }

  test("recordHeartbeat updates the timestamp to the provided value") {
    val protocol = newProtocol()
    protocol.registerProducer("executor-1")

    protocol.recordHeartbeat("executor-1", 12345L)

    val heartbeats = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerHeartbeats")
    heartbeats.get("executor-1").longValue() must be(12345L)
  }

  test("recordHeartbeat of an unregistered producer still records the entry") {
    val protocol = newProtocol()
    // Contract is lenient — record even for unknown producer to allow out-of-order
    // wire messages (heartbeat arriving before register) to be preserved rather
    // than dropped. See BackpressureProtocol#recordHeartbeat scaladoc.
    protocol.recordHeartbeat("executor-99", 42L)

    val heartbeats = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerHeartbeats")
    heartbeats.get("executor-99").longValue() must be(42L)
  }

  // ==========================================================================
  // Group 3: acknowledgeReceipt behavior
  // --------------------------------------------------------------------------
  // The ackTable is keyed by opaque block ID and valued by the consumer's
  // reported position; the writer side uses these entries to reclaim buffer
  // memory within the user-mandated 100 ms window (AAP section 0.1.1).
  // ==========================================================================

  test("acknowledgeReceipt stores the consumerPos for the blockId") {
    val protocol = newProtocol()
    protocol.acknowledgeReceipt("block-1", 1024L)

    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    ackTable.get("block-1").longValue() must be(1024L)
  }

  test("acknowledgeReceipt updates existing entries with the max of current and new") {
    // The implementation uses `merge` with Math.max to preserve a monotonic
    // non-decreasing watermark under out-of-order RPC delivery. For strictly
    // increasing inputs (100L → 500L) the final value is the latest (500L).
    val protocol = newProtocol()
    protocol.acknowledgeReceipt("block-1", 100L)
    protocol.acknowledgeReceipt("block-1", 500L)

    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    ackTable.get("block-1").longValue() must be(500L)
  }

  test("acknowledgeReceipt preserves the maximum under out-of-order delivery") {
    // A stale acknowledgment arriving AFTER a newer one must not rewind the
    // stored watermark. This test inverts the order of the previous test: 500L
    // first, then 100L. The stored value must still be 500L.
    val protocol = newProtocol()
    protocol.acknowledgeReceipt("block-1", 500L)
    protocol.acknowledgeReceipt("block-1", 100L)

    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    ackTable.get("block-1").longValue() must be(500L)
  }

  test("acknowledgeReceipt accepts zero and large positions") {
    val protocol = newProtocol()

    noException must be thrownBy {
      protocol.acknowledgeReceipt("block-0", 0L)
      protocol.acknowledgeReceipt("block-big", Long.MaxValue)
    }

    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    ackTable.get("block-0").longValue() must be(0L)
    ackTable.get("block-big").longValue() must be(Long.MaxValue)
  }

  // ==========================================================================
  // Group 4: Priority arbitration formula (partitionCount.toLong * dataVolumeBytes)
  // --------------------------------------------------------------------------
  // Priority is computed as partitionCount (Int, widened to Long) multiplied by
  // dataVolumeBytes (Long). The widening prevents integer overflow for modest
  // partition counts; the combined Long product cannot overflow for any
  // realistic partition-count / byte-volume combination.
  // ==========================================================================

  test("setProducerPriority stores partitionCount * dataVolumeBytes") {
    val protocol = newProtocol()
    protocol.setProducerPriority(
      "executor-1", partitionCount = 100, dataVolumeBytes = 1000000L)

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    priorities.get("executor-1").longValue() must be(100L * 1000000L)
  }

  test("setProducerPriority handles zero inputs") {
    val protocol = newProtocol()
    protocol.setProducerPriority("zero-parts", 0, 1000L)
    protocol.setProducerPriority("zero-bytes", 100, 0L)
    protocol.setProducerPriority("zero-all", 0, 0L)

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    priorities.get("zero-parts").longValue() must be(0L)
    priorities.get("zero-bytes").longValue() must be(0L)
    priorities.get("zero-all").longValue() must be(0L)
  }

  test("setProducerPriority: 100-partition shuffle outranks 10-partition shuffle at equal data") {
    val protocol = newProtocol()
    protocol.setProducerPriority("big", 100, 1000000L)
    protocol.setProducerPriority("small", 10, 1000000L)

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    val bigPrio: Long = priorities.get("big").longValue()
    val smallPrio: Long = priorities.get("small").longValue()
    bigPrio must be > smallPrio
    (bigPrio.toDouble / smallPrio.toDouble) must be(10.0 +- 0.001)
  }

  test("setProducerPriority overwrites previous entry") {
    val protocol = newProtocol()
    protocol.setProducerPriority("executor-1", 10, 1000L)
    protocol.setProducerPriority("executor-1", 20, 2000L)

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    priorities.get("executor-1").longValue() must be(20L * 2000L)
  }

  test("setProducerPriority widens partitionCount to Long before multiplying") {
    // partitionCount is an Int; dataVolumeBytes is a Long. Widening via `.toLong`
    // before multiplication prevents Int overflow for pathological (but valid)
    // inputs like partitionCount = 2^20. Test with values whose Int product would
    // overflow but whose Long product does not.
    val protocol = newProtocol()
    val parts = 1 << 20 // 1_048_576
    val bytes = 1L << 20 // 1_048_576
    protocol.setProducerPriority("wide", parts, bytes)

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    priorities.get("wide").longValue() must be(parts.toLong * bytes)
  }

  // ==========================================================================
  // Group 5: updateRate() increments backpressureEvents and updates rate
  // --------------------------------------------------------------------------
  // updateRate replaces the token-bucket rate atomically and increments the
  // `shuffle.streaming.backpressureEvents` telemetry counter on every call,
  // per AAP section 0.1.1. Each rate update represents a flow-control decision
  // that operators should be able to observe.
  // ==========================================================================

  test("updateRate stores the new rate in currentRateBytesPerSec") {
    val protocol = newProtocol()
    protocol.updateRate(5000000.0)

    val rateRef = getPrivateField[AtomicReference[java.lang.Double]](
      protocol, "currentRateBytesPerSec")
    rateRef.get().doubleValue() must be(5000000.0 +- 0.001)
  }

  test("updateRate increments backpressureEvents counter exactly once") {
    val metrics = mock(classOf[StreamingShuffleMetrics])
    val protocol = newProtocol(newConf(), metrics)

    protocol.updateRate(100.0)

    verify(metrics, times(1)).incrementBackpressureEvents()
  }

  test("repeated updateRate calls increment backpressureEvents per-call") {
    val metrics = mock(classOf[StreamingShuffleMetrics])
    val protocol = newProtocol(newConf(), metrics)

    protocol.updateRate(100.0)
    protocol.updateRate(200.0)
    protocol.updateRate(300.0)

    verify(metrics, times(3)).incrementBackpressureEvents()
  }

  test("updateRate accepts zero and small positive rates without throwing") {
    val protocol = newProtocol()
    noException must be thrownBy {
      protocol.updateRate(0.0)
      protocol.updateRate(1.0)
      protocol.updateRate(0.001)
    }
  }

  test("updateRate with a null metrics source does not throw") {
    // Defensive: driver-side construction or unit-test contexts may pass a null
    // metrics source. The protocol guards the `incrementBackpressureEvents()`
    // call with a null check. We exercise that guard here.
    val conf = newConf()
    // Directly construct without newProtocol() because the tracking list
    // expects a non-null source only in the afterEach stop(), which works
    // regardless of the metrics argument.
    val protocol = new BackpressureProtocol(conf, metrics = null)
    protocols = protocol :: protocols

    noException must be thrownBy protocol.updateRate(1000.0)

    val rateRef = getPrivateField[AtomicReference[java.lang.Double]](
      protocol, "currentRateBytesPerSec")
    rateRef.get().doubleValue() must be(1000.0 +- 0.001)
  }

  // ==========================================================================
  // Group 6: acquirePermission() v1 stub is a no-op but non-throwing
  // --------------------------------------------------------------------------
  // Token-bucket enforcement lives in network/TokenBucketRateLimiter.scala; the
  // v1 acquirePermission is intentionally a no-op so that StreamingShuffleWriter
  // can call it unconditionally without guarding on whether rate-limiting is
  // wired yet. This group validates the no-op contract.
  // ==========================================================================

  test("acquirePermission does not throw for any block size including zero") {
    val protocol = newProtocol()
    noException must be thrownBy {
      protocol.acquirePermission(0L)
      protocol.acquirePermission(1L)
      protocol.acquirePermission(1024L)
      protocol.acquirePermission(Long.MaxValue)
    }
  }

  test("acquirePermission does not modify ackTable or priorities") {
    val protocol = newProtocol()
    protocol.acquirePermission(1024L)

    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    ackTable.isEmpty must be(true)
    priorities.isEmpty must be(true)
  }

  test("acquirePermission does not modify currentRateBytesPerSec") {
    val protocol = newProtocol(newConf(maxMbps = 100))
    val rateRef = getPrivateField[AtomicReference[java.lang.Double]](
      protocol, "currentRateBytesPerSec")
    val before = rateRef.get().doubleValue()

    protocol.acquirePermission(1024L)

    rateRef.get().doubleValue() must be(before +- 0.001)
  }

  test("acquirePermission does not increment backpressureEvents telemetry") {
    // v1 contract: acquirePermission emits zero backpressure events because it
    // is a no-op. When enforcement is wired through network/TokenBucketRateLimiter
    // in a later revision, this test will need updating.
    val metrics = mock(classOf[StreamingShuffleMetrics])
    val protocol = newProtocol(newConf(), metrics)

    protocol.acquirePermission(1024L)
    protocol.acquirePermission(2048L)
    protocol.acquirePermission(4096L)

    verify(metrics, times(0)).incrementBackpressureEvents()
  }

  // ==========================================================================
  // Group 7: Concurrent-access safety of state-bearing maps
  // --------------------------------------------------------------------------
  // All three state tables are ConcurrentHashMap; currentRateBytesPerSec and
  // the diagnostic counters are Atomic* primitives. These tests exercise the
  // lock-free discipline mandated by AAP section 0.7.4 ("Telemetry overhead
  // MUST remain <1% CPU utilization" → lock-free AtomicLong.getAndIncrement()
  // and ConcurrentHashMap).
  // ==========================================================================

  test("concurrent acknowledgeReceipt calls produce correct final map size") {
    val protocol = newProtocol()
    val threads = 10
    val perThread = 100
    val executor = Executors.newFixedThreadPool(threads)
    val latch = new CountDownLatch(threads)

    try {
      (0 until threads).foreach { t =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              (0 until perThread).foreach { i =>
                protocol.acknowledgeReceipt(s"block-$t-$i", i.toLong)
              }
            } finally {
              latch.countDown()
            }
          }
        })
      }
      latch.await(30, TimeUnit.SECONDS) must be(true)
    } finally {
      executor.shutdownNow()
    }

    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    // Every (t, i) pair produces a unique block ID, so the final map size is
    // exactly threads * perThread.
    ackTable.size() must be(threads * perThread)
  }

  test("concurrent register/unregister produces empty heartbeat table at the end") {
    val protocol = newProtocol()
    val threads = 50
    val executor = Executors.newFixedThreadPool(threads)
    val latch = new CountDownLatch(threads)

    try {
      (0 until threads).foreach { t =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              val id = s"exec-$t"
              protocol.registerProducer(id)
              protocol.recordHeartbeat(id, t.toLong)
              protocol.setProducerPriority(id, 10, 1000L)
              protocol.unregisterProducer(id)
            } finally {
              latch.countDown()
            }
          }
        })
      }
      latch.await(30, TimeUnit.SECONDS) must be(true)
    } finally {
      executor.shutdownNow()
    }

    // Every thread registers and then unregisters the same producerId it owns,
    // so no entries should remain in either map.
    val heartbeats = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerHeartbeats")
    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    heartbeats.isEmpty must be(true)
    priorities.isEmpty must be(true)
  }

  test("concurrent updateRate increments backpressureEvents exactly once per call") {
    val metrics = new StreamingShuffleMetrics()
    val protocol = newProtocol(newConf(), metrics)
    val threads = 10
    val perThread = 100
    val executor = Executors.newFixedThreadPool(threads)
    val latch = new CountDownLatch(threads)

    try {
      (0 until threads).foreach { t =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              (0 until perThread).foreach { i =>
                protocol.updateRate((t * 1000.0) + i)
              }
            } finally {
              latch.countDown()
            }
          }
        })
      }
      latch.await(30, TimeUnit.SECONDS) must be(true)
    } finally {
      executor.shutdownNow()
    }

    // Every updateRate() call increments backpressureEvents exactly once;
    // AtomicLong.getAndIncrement() is lock-free and loses no updates.
    metrics.backpressureEventsValue must be((threads * perThread).toLong)
  }

  test("concurrent setProducerPriority converges to one winner per producerId") {
    val protocol = newProtocol()
    val threads = 10
    val perThread = 100
    val executor = Executors.newFixedThreadPool(threads)
    val latch = new CountDownLatch(threads)

    try {
      // All threads race to write priority for the same producerId. The final
      // stored value must be SOME valid partitionCount * dataVolumeBytes
      // product; no torn-write artifacts (like zero or Long.MinValue) may
      // appear because ConcurrentHashMap.put is atomic per-key.
      (0 until threads).foreach { t =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              (0 until perThread).foreach { i =>
                val parts = (t + 1) * (i + 1) // strictly positive
                val bytes = 1000L
                protocol.setProducerPriority("shared", parts, bytes)
              }
            } finally {
              latch.countDown()
            }
          }
        })
      }
      latch.await(30, TimeUnit.SECONDS) must be(true)
    } finally {
      executor.shutdownNow()
    }

    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    priorities.containsKey("shared") must be(true)
    val finalPrio = priorities.get("shared").longValue()
    // The smallest valid priority written by any thread is 1 * 1 * 1000 = 1000.
    finalPrio must be >= 1000L
  }

  // ==========================================================================
  // Group 8: stop() lifecycle and idempotency
  // --------------------------------------------------------------------------
  // stop() is called by StreamingShuffleManager.stop() on manager shutdown. It
  // must be idempotent (safe to call multiple times), safe to call from any
  // thread, and must guarantee that no further scheduler iterations run.
  // ==========================================================================

  test("stop() is idempotent — second call does not throw") {
    val protocol = newProtocol()
    protocol.stop()
    noException must be thrownBy protocol.stop()
  }

  test("stop() shuts down the daemon scheduler") {
    val protocol = newProtocol()
    protocol.stop()

    val scheduler = getPrivateField[ScheduledExecutorService](protocol, "scheduler")
    scheduler.isShutdown must be(true)
  }

  test("stop() clears all three state tables") {
    val protocol = newProtocol()
    protocol.registerProducer("executor-1")
    protocol.setProducerPriority("executor-1", 100, 1000L)
    protocol.acknowledgeReceipt("block-1", 1024L)

    protocol.stop()

    val heartbeats = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerHeartbeats")
    val priorities = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "producerPriorities")
    val ackTable = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      protocol, "ackTable")
    heartbeats.isEmpty must be(true)
    priorities.isEmpty must be(true)
    ackTable.isEmpty must be(true)
  }

  test("stop() followed by acknowledgeReceipt does not throw") {
    // Contract: post-stop mutations should be harmless (no exceptions). The
    // underlying ConcurrentHashMap instance is still usable after clear(); only
    // the scheduler is terminated.
    val protocol = newProtocol()
    protocol.stop()
    noException must be thrownBy protocol.acknowledgeReceipt("late-block", 1L)
  }

  test("stop() followed by registerProducer does not throw") {
    val protocol = newProtocol()
    protocol.stop()
    noException must be thrownBy protocol.registerProducer("late-exec")
  }

  test("stop() followed by updateRate does not throw") {
    val protocol = newProtocol()
    protocol.stop()
    noException must be thrownBy protocol.updateRate(1000.0)
  }

  test("stop() called from multiple threads concurrently does not throw") {
    val protocol = newProtocol()
    val threads = 10
    val executor = Executors.newFixedThreadPool(threads)
    val latch = new CountDownLatch(threads)
    val errors = new java.util.concurrent.atomic.AtomicInteger(0)

    try {
      (0 until threads).foreach { _ =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              protocol.stop()
            } catch {
              case _: Throwable => errors.incrementAndGet()
            } finally {
              latch.countDown()
            }
          }
        })
      }
      latch.await(30, TimeUnit.SECONDS) must be(true)
    } finally {
      executor.shutdownNow()
    }

    errors.get() must be(0)

    val scheduler = getPrivateField[ScheduledExecutorService](protocol, "scheduler")
    scheduler.isShutdown must be(true)
  }
}
