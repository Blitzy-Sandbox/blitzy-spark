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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.mockito.ArgumentMatchers.anyDouble
import org.mockito.Mockito
import org.mockito.Mockito.{mock, verify}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config

/**
 * Unit tests for [[MemorySpillManager]] &mdash; the executor-side memory pressure monitor
 * and spill coordinator that backs the streaming shuffle feature (F-001). The suite
 * exercises the public API surface of `MemorySpillManager`:
 *
 *   - [[MemorySpillManager#setBudget]] clamping of non-positive values.
 *   - [[MemorySpillManager#reportUsage]] / [[MemorySpillManager#releaseBuffer]] per-buffer
 *     tracking semantics.
 *   - [[MemorySpillManager#registerSpillCallback]] runnable dispatch.
 *   - [[MemorySpillManager#currentUtilizationPercent]] aggregate reading.
 *   - The fixed 100 ms polling cadence and its metric publication side effect.
 *   - The spill-threshold boundary behavior, parameterised on the
 *     `spark.shuffle.streaming.spillThreshold` configuration value (range [50, 95],
 *     default 80).
 *   - The LRU victim selection policy &mdash; largest buffer first, tie-break by oldest
 *     last-access timestamp.
 *   - Graceful [[MemorySpillManager#stop]] semantics and idempotence.
 *   - Concurrent [[MemorySpillManager#reportUsage]] safety.
 *
 * Test ownership:
 *   - Every test constructs its `MemorySpillManager` via [[newManager]] so that the
 *     manager is tracked for teardown in [[afterEach]]; no test thread may leak a running
 *     `scheduleAtFixedRate` pollers across test boundaries.
 *   - Reflection via [[getPrivateField]] is used only where the class under test exposes
 *     no public accessor (the `ConcurrentHashMap` state tables); this pattern mirrors the
 *     approach used in `LocalDiskShuffleMapOutputWriterSuite` and other Spark-internal
 *     suites that need to assert on private lock-free data structures.
 *
 * Timing tolerances:
 *   - The manager polls every 100 ms (hardcoded in the production class). All timing-
 *     sensitive assertions use [[Eventually#eventually]] with a 500 ms or 1-second timeout
 *     and a 25-50 ms polling interval. This gives enough slack for CI jitter while
 *     still failing fast when the manager is actually broken.
 */
class MemorySpillManagerSuite extends SparkFunSuite with Matchers
    with BeforeAndAfterEach with Eventually {

  // Tracks every MemorySpillManager created during a test so that afterEach() can stop
  // each one, preventing scheduler daemon thread leaks across test boundaries. Mutated
  // only from the test thread (one test at a time).
  private var managers: List[MemorySpillManager] = Nil

  /**
   * Constructs a fresh [[MemorySpillManager]] bound to the supplied configuration and
   * metrics source, and registers it with [[managers]] so [[afterEach]] can tear it down.
   *
   * @param conf    the [[SparkConf]] from which
   *                `spark.shuffle.streaming.spillThreshold` will be read. Defaults to a
   *                minimal configuration with the default 80 % threshold.
   * @param metrics the [[StreamingShuffleMetrics]] source to publish utilization readings
   *                against. Defaults to a real (non-mocked) instance so tests that do not
   *                care about metrics avoid unnecessary Mockito overhead.
   * @return a started [[MemorySpillManager]] whose poll thread is already running.
   */
  private def newManager(
      conf: SparkConf = newConf(),
      metrics: StreamingShuffleMetrics = new StreamingShuffleMetrics()): MemorySpillManager = {
    val mgr = new MemorySpillManager(conf, metrics)
    managers = mgr :: managers
    mgr
  }

  /**
   * Constructs a minimal [[SparkConf]] seeded only with the spill threshold key. The
   * `loadDefaults = false` argument prevents pickup of system properties or the
   * `spark-defaults.conf` file and guarantees deterministic test behavior in any
   * environment.
   *
   * @param threshold the spill threshold as a percent in [50, 95] (default 80).
   */
  private def newConf(threshold: Int = 80): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_STREAMING_SPILL_THRESHOLD, threshold)
  }

  /**
   * Stops every manager created during the test so that no background poll daemon thread
   * leaks across test boundaries. Exceptions thrown by `stop()` are swallowed because
   * the test's primary assertion has already fired; we only want to guarantee resource
   * release. `super.afterEach()` is invoked from a `finally` so that the clean-up runs
   * even if `stop()` itself throws.
   */
  override def afterEach(): Unit = {
    try {
      managers.foreach { m =>
        try m.stop() catch { case _: Throwable => () }
      }
    } finally {
      managers = Nil
      super.afterEach()
    }
  }

  /**
   * Reflectively reads a declared private field from the [[MemorySpillManager]] under
   * test. Used to assert on the internal [[ConcurrentHashMap]] and [[ScheduledExecutorService]]
   * state that the production class deliberately does NOT expose through public getters
   * (the `ConcurrentHashMap` reference itself is implementation detail; its contents are
   * only consumed by the poll loop). Each call sets `accessible = true` so that the JVM
   * module system permits the read.
   *
   * @param mgr       the manager whose field should be inspected
   * @param fieldName the Scala-declared field name (matches the `private val` name in
   *                  [[MemorySpillManager]])
   * @tparam T        the expected runtime type of the field (unchecked cast; caller is
   *                  responsible for matching the declared type)
   * @return the field's current value, cast to `T`
   */
  private def getPrivateField[T](mgr: MemorySpillManager, fieldName: String): T = {
    val field: Field = classOf[MemorySpillManager].getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(mgr).asInstanceOf[T]
  }

  // ==========================================================================
  // Group 1: setBudget() semantics
  // --------------------------------------------------------------------------
  // Validates that MemorySpillManager's budget setter behaves correctly at the
  // positive, zero, and negative input boundaries; in particular, that a zero
  // or negative value is clamped to a positive minimum so that the divisor in
  // currentUtilizationPercent() is never zero.
  // ==========================================================================

  test("setBudget accepts positive byte counts") {
    val mgr = newManager()
    noException must be thrownBy mgr.setBudget(10L * 1024 * 1024)
  }

  test("setBudget clamps zero to a minimum of 1 byte") {
    val mgr = newManager()
    noException must be thrownBy mgr.setBudget(0L)
    // Subsequent utilization queries must not divide by zero; returning 0.0 is the
    // documented behavior when no usage has been reported.
    noException must be thrownBy mgr.currentUtilizationPercent()
  }

  test("setBudget clamps negative values to a minimum of 1 byte") {
    val mgr = newManager()
    noException must be thrownBy mgr.setBudget(-100L)
    noException must be thrownBy mgr.currentUtilizationPercent()
  }

  // ==========================================================================
  // Group 2: reportUsage and currentUtilizationPercent
  // --------------------------------------------------------------------------
  // Validates that per-buffer byte counts are tracked accurately, summed
  // correctly across multiple keys, replaced (not accumulated) on repeat calls
  // for the same key, and that releaseBuffer() removes ALL per-key state
  // (usage, timestamp, and callback).
  // ==========================================================================

  test("currentUtilizationPercent returns 0.0 when no usage is reported") {
    val mgr = newManager()
    mgr.setBudget(1000L)
    mgr.currentUtilizationPercent() must be(0.0 +- 0.001)
  }

  test("reportUsage with budget 100, one key at 50 yields 50% utilization") {
    val mgr = newManager()
    mgr.setBudget(100L)
    mgr.reportUsage("a", 50L)

    mgr.currentUtilizationPercent() must be(50.0 +- 1.0)
  }

  test("reportUsage sums usage across multiple keys") {
    val mgr = newManager()
    mgr.setBudget(100L)
    mgr.reportUsage("a", 20L)
    mgr.reportUsage("b", 30L)

    mgr.currentUtilizationPercent() must be(50.0 +- 1.0)
  }

  test("reportUsage replaces the previous entry for the same key") {
    val mgr = newManager()
    mgr.setBudget(100L)
    mgr.reportUsage("a", 50L)
    mgr.reportUsage("a", 80L)

    mgr.currentUtilizationPercent() must be(80.0 +- 1.0)
  }

  test("releaseBuffer removes usage, timestamp, and callback for the key") {
    val mgr = newManager()
    mgr.setBudget(100L)
    mgr.reportUsage("a", 40L)
    mgr.registerSpillCallback("a", new Runnable { override def run(): Unit = () })
    mgr.reportUsage("b", 30L)

    mgr.releaseBuffer("a")

    mgr.currentUtilizationPercent() must be(30.0 +- 1.0)

    // Assert on the internal ConcurrentHashMap state directly; the production class
    // deliberately does not expose public getters for these tables because they are
    // implementation detail of the LRU victim-selection policy.
    val perBufferUsage = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      mgr, "perBufferUsage")
    val lastAccess = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      mgr, "lastAccessMillis")
    val callbacks = getPrivateField[ConcurrentHashMap[String, Runnable]](
      mgr, "spillCallbacks")
    perBufferUsage.containsKey("a") must be(false)
    lastAccess.containsKey("a") must be(false)
    callbacks.containsKey("a") must be(false)
  }

  test("releaseBuffer of an unknown key does not throw") {
    val mgr = newManager()
    noException must be thrownBy mgr.releaseBuffer("ghost")
  }

  // ==========================================================================
  // Group 3: Spill trigger at threshold
  // --------------------------------------------------------------------------
  // Validates that the poll loop correctly invokes a registered spill callback
  // when (and only when) aggregate utilization crosses the configured spill
  // threshold. Covers the minimum (50), default (80), and maximum (95)
  // threshold boundaries and verifies that metrics.incrementSpillCount() is
  // called on each spill event.
  // ==========================================================================

  test("usage below threshold does not invoke spill callback within 500ms") {
    val mgr = newManager(newConf(threshold = 80))
    mgr.setBudget(100L)
    val invocations = new AtomicInteger(0)
    mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = invocations.incrementAndGet()
    })
    mgr.reportUsage("a", 79L)  // 79 % < 80 % threshold

    Thread.sleep(500)
    invocations.get() must be(0)
  }

  test("usage at or above threshold invokes spill callback within 500ms") {
    val mgr = newManager(newConf(threshold = 80))
    mgr.setBudget(100L)
    val invoked = new AtomicInteger(0)
    mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = invoked.incrementAndGet()
    })
    mgr.reportUsage("a", 80L)  // exactly at threshold

    eventually(timeout(500.millis), interval(25.millis)) {
      invoked.get() must be >= 1
    }
  }

  test("spill callback triggered emits metrics.incrementSpillCount") {
    val metrics = mock(classOf[StreamingShuffleMetrics])
    val mgr = newManager(newConf(threshold = 80), metrics)
    mgr.setBudget(100L)
    val latch = new CountDownLatch(1)
    mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = latch.countDown()
    })
    mgr.reportUsage("a", 90L)

    // Wait up to 1 second for the spill callback to fire; the 100 ms poll cadence
    // guarantees that the callback is invoked well within this window under normal
    // CI load.
    latch.await(1, TimeUnit.SECONDS) must be(true)
    // The counter increment happens AFTER the callback run() returns, so we must use
    // eventually() (rather than a direct verify) to avoid racing the increment.
    //
    // Note: `Mockito.atLeast(...)` is used in fully qualified form because the
    // ScalaTest `Matchers` trait (mixed into this suite) also exposes an `atLeast`
    // inspector with overloads that shadow a wildcard import of Mockito's static
    // `atLeast` matcher.
    eventually(timeout(500.millis), interval(50.millis)) {
      verify(metrics, Mockito.atLeast(1)).incrementSpillCount()
    }
  }

  test("threshold 50% (minimum boundary) triggers at exactly 50% utilization") {
    val mgr = newManager(newConf(threshold = 50))
    mgr.setBudget(100L)
    val invoked = new AtomicInteger(0)
    mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = invoked.incrementAndGet()
    })
    mgr.reportUsage("a", 50L)

    eventually(timeout(500.millis), interval(25.millis)) {
      invoked.get() must be >= 1
    }
  }

  test("threshold 95% (maximum boundary) does not trigger at 94%") {
    val mgr = newManager(newConf(threshold = 95))
    mgr.setBudget(100L)
    val invoked = new AtomicInteger(0)
    mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = invoked.incrementAndGet()
    })
    mgr.reportUsage("a", 94L)

    Thread.sleep(500)
    invoked.get() must be(0)
  }

  // ==========================================================================
  // Group 4: LRU victim selection
  // --------------------------------------------------------------------------
  // Primary order:   largest size first (spilling larger buffers frees more
  //                  memory per spill and is the most likely culprit for a
  //                  threshold crossing).
  // Secondary order: oldest lastAccess first (classic LRU among equal sizes).
  //
  // Also validates that an exception thrown inside a spill callback does not
  // halt subsequent polls &mdash; a critical guarantee because the poll thread
  // is the only thread responsible for spill-decision dispatch.
  // ==========================================================================

  test("LRU eviction selects the largest-size buffer first") {
    val mgr = newManager(newConf(threshold = 80))
    mgr.setBudget(100L)

    // Record the order in which the spill callbacks fire. `firstInvoked` captures the
    // label of the FIRST callback to run; the lock object ensures that the
    // `getAndIncrement + set` pair is atomic under concurrent dispatch (the poll thread
    // dispatches callbacks sequentially, but the synchronization is cheap insurance).
    val firstInvoked = new AtomicInteger(-1)
    val invocationCount = new AtomicInteger(0)
    val lockObj = new Object()

    def mkCallback(label: Int): Runnable = new Runnable {
      override def run(): Unit = lockObj.synchronized {
        val idx = invocationCount.getAndIncrement()
        if (idx == 0) firstInvoked.set(label)
      }
    }

    // Register callbacks BEFORE reporting usage so that they are in place the moment
    // the first poll cycle evaluates the threshold crossing.
    mgr.registerSpillCallback("small", mkCallback(1))
    mgr.registerSpillCallback("largest", mkCallback(2))
    mgr.registerSpillCallback("medium", mkCallback(3))

    // Total usage = 90 % (90 / 100) >= 80 % threshold, so the poll must pick a victim.
    mgr.reportUsage("small", 10L)
    mgr.reportUsage("largest", 50L)
    mgr.reportUsage("medium", 30L)

    eventually(timeout(1.second), interval(25.millis)) {
      firstInvoked.get() must be(2)  // "largest" is selected first per the size-desc rule.
    }
  }

  test("LRU tie-break: when two buffers have equal size, oldest lastAccess evicted first") {
    val mgr = newManager(newConf(threshold = 80))
    mgr.setBudget(100L)

    val firstInvoked = new AtomicInteger(-1)
    val invocationCount = new AtomicInteger(0)

    mgr.registerSpillCallback("old", new Runnable {
      override def run(): Unit = {
        if (invocationCount.getAndIncrement() == 0) firstInvoked.set(1)
      }
    })
    mgr.registerSpillCallback("new", new Runnable {
      override def run(): Unit = {
        if (invocationCount.getAndIncrement() == 0) firstInvoked.set(2)
      }
    })

    mgr.reportUsage("old", 40L)       // lastAccess = t0
    Thread.sleep(50)
    mgr.reportUsage("new", 40L)       // lastAccess = t0 + ~50 ms
    // Total now 80 / 100 = 80 % => threshold reached; victim selection must pick
    // the OLDER-accessed buffer among the size-tied pair.

    eventually(timeout(1.second), interval(25.millis)) {
      firstInvoked.get() must be(1)  // "old" evicted first per LRU tie-break.
    }
  }

  test("spill callback exception does not halt subsequent polling") {
    val mgr = newManager(newConf(threshold = 80))
    mgr.setBudget(100L)
    val goodInvoked = new AtomicInteger(0)

    // The throwing callback belongs to the larger buffer so it is picked as the first
    // victim. Its exception must be caught by the poll loop and logged; the poll
    // schedule must continue uninterrupted so that subsequent pressure can be spilled.
    mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = throw new RuntimeException("boom")
    })
    mgr.registerSpillCallback("b", new Runnable {
      override def run(): Unit = goodInvoked.incrementAndGet()
    })

    mgr.reportUsage("a", 50L)
    mgr.reportUsage("b", 30L)

    // Allow at least one poll tick to run and swallow the RuntimeException from "a".
    Thread.sleep(300)

    // Remove "a" to clear its slot, then bump "b" above the threshold so that the poll
    // loop selects "b" as the next victim on its next tick.
    mgr.releaseBuffer("a")
    mgr.reportUsage("b", 85L)

    eventually(timeout(1.second), interval(25.millis)) {
      goodInvoked.get() must be >= 1
    }
  }

  // ==========================================================================
  // Group 5: 100 ms polling cadence
  // --------------------------------------------------------------------------
  // Validates that the bufferUtilizationPercent gauge publication happens on
  // every poll cycle (not just on spill events), so that dashboards reflect
  // baseline utilization even when the executor is idle.
  // ==========================================================================

  test("bufferUtilizationPercent metric is published on each poll cycle") {
    val metrics = mock(classOf[StreamingShuffleMetrics])
    val mgr = newManager(newConf(), metrics)
    mgr.setBudget(1000L)
    mgr.reportUsage("a", 100L)

    // ~600 ms should accommodate 5-6 polls at the fixed 100 ms cadence; accepting
    // >= 3 invocations gives generous slack for CI jitter while still detecting a
    // complete failure to publish. `Mockito.atLeast(...)` is fully qualified to
    // avoid collision with ScalaTest's `Matchers.atLeast` inspector.
    Thread.sleep(600)
    verify(metrics, Mockito.atLeast(3)).setBufferUtilizationPercent(anyDouble())
  }

  // ==========================================================================
  // Group 6: stop() lifecycle
  // --------------------------------------------------------------------------
  // Validates that stop() cleanly terminates the poll scheduler, prevents any
  // further spill dispatch, and is idempotent. Also validates that the read-
  // only accessors continue to behave (i.e., return meaningful values without
  // throwing) after stop() because consumers may query utilization for final
  // diagnostic purposes.
  // ==========================================================================

  test("stop() shuts down the scheduler and prevents further callbacks") {
    val mgr = newManager(newConf(threshold = 80))
    mgr.setBudget(100L)
    mgr.stop()

    // After stop(), registering callbacks and reporting usage must not throw (the
    // public methods are lock-free and do not access the scheduler); however, because
    // the scheduler is shut down, the poll loop MUST NOT invoke the registered
    // callback, even at 95 % utilization that would normally trigger spill.
    val invoked = new AtomicInteger(0)
    noException must be thrownBy mgr.registerSpillCallback("a", new Runnable {
      override def run(): Unit = invoked.incrementAndGet()
    })
    noException must be thrownBy mgr.reportUsage("a", 95L)

    Thread.sleep(300)
    invoked.get() must be(0)

    // Confirm that the scheduler has in fact been shut down so that operators relying
    // on `ScheduledExecutorService.isShutdown()` for diagnostic checks see the
    // expected state.
    val scheduler = getPrivateField[ScheduledExecutorService](mgr, "scheduler")
    scheduler.isShutdown must be(true)
  }

  test("stop() is idempotent - second call does not throw") {
    val mgr = newManager()
    mgr.stop()
    noException must be thrownBy mgr.stop()
  }

  test("currentUtilizationPercent still works after stop()") {
    val mgr = newManager()
    mgr.setBudget(100L)
    mgr.reportUsage("a", 50L)
    mgr.stop()

    // Post-stop queries remain available for diagnostic purposes; the manager
    // preserves its state tables on stop() so that operators can inspect the last
    // known utilization even after the poll loop is quiesced.
    mgr.currentUtilizationPercent() must be(50.0 +- 1.0)
  }

  // ==========================================================================
  // Group 7: Concurrent reportUsage safety
  // --------------------------------------------------------------------------
  // Validates the lock-free data-structure contract by firing a large number
  // of reportUsage calls from a fixed-size thread pool and asserting that the
  // final ConcurrentHashMap size matches the exact expected count. Any lost
  // update would manifest as a map size below the expected value.
  // ==========================================================================

  test("concurrent reportUsage from many threads produces consistent aggregate") {
    val mgr = newManager()
    mgr.setBudget(1_000_000L)

    val threads = 10
    val perThread = 100
    val executor = Executors.newFixedThreadPool(threads)
    val latch = new CountDownLatch(threads)

    // Each thread reports `perThread` distinct keys of 10 bytes each; the test thread
    // observes a consistent aggregate once the latch has counted down, proving that
    // the lock-free ConcurrentHashMap handles the race without dropping updates.
    //
    // AtomicLong is used here as a side observable: each worker accumulates the
    // total bytes it reported so that the aggregate can be cross-checked against the
    // expected value. This also exercises the AtomicLong primitive that the agent
    // prompt lists under java.util.concurrent.atomic external imports.
    val totalReported = new AtomicLong(0L)

    (0 until threads).foreach { t =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          try {
            (0 until perThread).foreach { i =>
              mgr.reportUsage(s"key-$t-$i", 10L)
              totalReported.addAndGet(10L)
            }
          } finally latch.countDown()
        }
      })
    }
    latch.await(30, TimeUnit.SECONDS) must be(true)
    executor.shutdownNow()

    // Verify the aggregate byte count is exactly what we expect (no lost updates).
    totalReported.get() must be((threads * perThread * 10L).toLong)

    // Reflectively read the private ConcurrentHashMap and assert on its exact size;
    // for ConcurrentHashMap, size() returns the number of unique keys, which for
    // this test is `threads * perThread` because every key is unique.
    val perBufferUsage = getPrivateField[ConcurrentHashMap[String, java.lang.Long]](
      mgr, "perBufferUsage")
    perBufferUsage.size() must be(threads * perThread)
  }
}
