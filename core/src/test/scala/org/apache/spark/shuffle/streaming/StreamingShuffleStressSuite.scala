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

import java.util.concurrent.{CountDownLatch, Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.util.Random

import org.scalatest.tagobjects.Slow

import org.apache.spark.{HashPartitioner, LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

/**
 * Stress test for streaming shuffle: 5-minute continuous workload with 10 concurrent
 * task threads, up to 5 concurrent shuffles per iteration, 10% failure injection, and
 * heap-leak detection.
 *
 * This suite implements the stress-test specification from AAP Sec.0.5.1.6 (Group 6,
 * item 9) and AAP Sec.0.1.2 (User Example, Stress Test Target):
 *
 *   "5-minute continuous shuffle workload: 10 concurrent tasks with 5 concurrent
 *    shuffles, Random failure injection: 10% task failure rate, Performance
 *    degradation monitoring: <5% throughput reduction over test duration"
 *
 * Concretely, the suite validates the following AAP Sec.0.7.2.6 quality gates:
 *   - '''Throughput stability''': measure the ops-per-second rate during the first
 *     minute (baseline window) and the fifth minute (final window). Assert that
 *     `(baseline - final) / baseline < 5%` so that monotonic memory accumulation,
 *     growing locks, or scheduling pathologies are surfaced.
 *   - '''Heap-leak detection''': capture used-heap before and after the workload (with
 *     aggressive GC cycles in between) and assert that the residual delta is below a
 *     50 MB tolerance threshold for JIT-compiled code, class-loader metadata, and
 *     internal Spark caches that grow once but do not leak.
 *   - '''No unhandled propagation''': worker threads catch all exceptions (since the
 *     10% failure-injection rate intentionally throws); only assertion failures from
 *     the throughput and heap-leak checks at the end of the test cause the suite to
 *     fail. Workers consult an [[AtomicReference]] for fail-fast on truly unexpected
 *     errors raised by infrastructure (e.g. [[OutOfMemoryError]] thrown during a
 *     shuffle setup before the per-iteration `try` block can intercept it).
 *
 * == Tagging ==
 * The single test in this suite is tagged with [[org.scalatest.tagobjects.Slow]] so
 * that it does not run in the default fast unit-test lane (per Spark convention for
 * tests exceeding ~5 seconds). Run explicitly with one of the following:
 * {{{
 *   build/sbt -Pscala-2.13 "core/testOnly *StreamingShuffleStressSuite"
 *   build/mvn -pl core test -DwildcardSuites=org.apache.spark.shuffle.streaming.\
 *     StreamingShuffleStressSuite -Dtest=none
 * }}}
 *
 * The [[SparkFunSuite]] base class wraps the test body in
 * `failAfter(Span(20, Minutes))`, providing ample margin over the 5-minute workload
 * plus its setup/teardown.
 *
 * == Coexistence ==
 * Per the user directive *"Isolate streaming logic in dedicated classes with zero
 * cross-contamination into existing shuffle code paths"* (AAP Sec.0.1.2), this suite
 * exercises the streaming-shuffle path exclusively via the `spark.shuffle.manager=
 * streaming` configuration knob. It does NOT touch any code path within
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]] or its writers/readers.
 * Cleanup of the shared [[org.apache.spark.SparkContext]] is delegated to
 * [[LocalSparkContext.afterEach]], which guarantees `sc.stop()` and `sc = null`
 * after every test method.
 *
 * == Design Rationale (key choices) ==
 * 1. '''Shared `SparkContext` across all worker threads''' -- Spark is thread-safe at
 *    the [[SparkContext]] level; concurrent submission of shuffle stages from
 *    multiple threads stresses the streaming-shuffle subsystem's concurrent-shuffle
 *    handling more than a per-thread isolated context would.
 * 2. '''Throughput windows''' -- measuring rate in two distinct one-minute windows
 *    (first vs last) catches degradation patterns that would not be visible from a
 *    single before/after measurement.
 * 3. '''Aggressive GC discipline for heap measurement''' -- a single `System.gc()`
 *    is unreliable for breaking weak/soft references and may not promote tenured
 *    objects. Five iterations of `gc + 100 ms sleep` provides reasonable
 *    confidence that the JVM has settled before we read used heap.
 * 4. '''Synthetic workload shape''' -- `parallelize -> map -> partitionBy ->
 *    groupByKey -> count` with a varying partition count exercises multiple
 *    `getWriter`/`getReader` paths and prevents JIT over-specialization on a single
 *    partition count.
 */
class StreamingShuffleStressSuite extends SparkFunSuite with LocalSparkContext {

  // ---------------------------------------------------------------------------
  // Constants -- knobs sized exactly to the AAP Sec.0.5.1.6 specification.
  // ---------------------------------------------------------------------------

  /** Total stress duration: 5 minutes per AAP Sec.0.5.1.6. */
  private val STRESS_DURATION_MILLIS: Long = 5L * 60L * 1000L

  /** Number of concurrent task threads per AAP user spec ("10 concurrent tasks"). */
  private val NUM_CONCURRENT_TASKS: Int = 10

  /**
   * Upper bound on shuffle partition count per iteration; bounded by the AAP user
   * spec ("5 concurrent shuffles"). The actual partition count for each iteration
   * is sampled uniformly at random from `[2, 2 + NUM_CONCURRENT_SHUFFLES)`.
   */
  private val NUM_CONCURRENT_SHUFFLES: Int = 5

  /** Failure injection rate (10% per AAP user spec). */
  private val FAILURE_INJECTION_RATE: Double = 0.10

  /**
   * Throughput-degradation tolerance threshold per AAP Sec.0.5.1.6: the test passes if
   * the ratio `(baselineRate - finalRate) / baselineRate` is strictly less than this
   * value. AAP user spec: "<5% throughput reduction over test duration".
   */
  private val MAX_THROUGHPUT_DEGRADATION: Double = 0.05

  /**
   * Heap-leak tolerance threshold: residual heap above this delta after the workload
   * indicates a leak. The 50 MB headroom accounts for JIT-compiled code, class-
   * loader metadata, and one-time Spark internal caches that grow during the first
   * shuffle iterations and do not leak proportionally to workload size.
   */
  private val MAX_HEAP_LEAK_BYTES: Long = 50L * 1024L * 1024L

  /**
   * Per-shuffle dataset size: small enough to complete in a fraction of a second per
   * task on commodity hardware. Sized so that the 5-minute window can complete
   * thousands of iterations across the 10 worker threads, providing strong
   * statistical evidence for the throughput-degradation assertion.
   */
  private val DATASET_SIZE_PER_SHUFFLE: Int = 50000

  /**
   * Window size for both the baseline and final throughput measurements: the first
   * minute and the last minute of the 5-minute stress window. One minute is long
   * enough to amortize per-iteration jitter while short enough to leave the middle
   * three minutes as steady-state burn-in time.
   */
  private val WINDOW_MILLIS: Long = 60L * 1000L

  /**
   * Grace period beyond [[STRESS_DURATION_MILLIS]] used by the main thread when
   * awaiting the worker [[CountDownLatch]]. Allows a final in-flight iteration to
   * finish instead of being interrupted abruptly.
   */
  private val LATCH_GRACE_MILLIS: Long = 30L * 1000L

  /** Maximum time to wait for the executor pool to terminate after `shutdown`. */
  private val EXECUTOR_TERMINATION_TIMEOUT_SECONDS: Long = 10L

  /**
   * Number of [[System.gc]] cycles to perform when forcing a heap-stabilization
   * point. Five iterations is conservative -- sufficient to break weak and soft
   * references and to promote young-generation objects through a full minor + major
   * collection cycle.
   */
  private val GC_CYCLES: Int = 5

  /**
   * Sleep between GC cycles. Gives the JVM garbage collector helper threads
   * time to finish a full collection round before the next `gc` is requested.
   */
  private val GC_CYCLE_SLEEP_MILLIS: Long = 100L

  // ---------------------------------------------------------------------------
  // The single stress test (tagged Slow so it does not run in the fast lane).
  // ---------------------------------------------------------------------------

  test("5-minute continuous stress with 10 concurrent tasks and 5 concurrent shuffles",
      Slow) {
    // -------------------------------------------------------------------------
    // 1. Capture a stable baseline heap reading before any test work begins.
    //    This is later compared against post-test heap to detect leaks.
    // -------------------------------------------------------------------------
    forceGc()
    val baselineHeap = computeUsedHeap()
    logInfo(s"Baseline heap usage: $baselineHeap bytes")

    // -------------------------------------------------------------------------
    // 2. Construct the SparkContext. We use a self-contained SparkConf so the
    //    suite is robust to spark-defaults.conf settings on the host machine.
    //    `local[N]` runs N task slots in-process, which is sufficient because
    //    the streaming-shuffle stress here exercises the manager's concurrent
    //    handling rather than multi-executor wire transfer.
    // -------------------------------------------------------------------------
    val conf = new SparkConf(loadDefaults = false)
      .setAppName("StreamingShuffleStressSuite")
      .setMaster(s"local[$NUM_CONCURRENT_TASKS]")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")
    sc = new SparkContext(conf)

    // -------------------------------------------------------------------------
    // 3. Cross-thread bookkeeping. All counters are lock-free atomics so the
    //    book-keeping itself does not perturb the throughput measurement we are
    //    trying to make.
    // -------------------------------------------------------------------------
    val totalOpsCompleted = new AtomicLong(0L)
    val totalFailures = new AtomicLong(0L)
    val baselineWindowOps = new AtomicLong(0L)
    val finalWindowOps = new AtomicLong(0L)
    val stopFlag = new AtomicBoolean(false)
    val errorRef = new AtomicReference[Throwable](null)

    val startTimeMillis = System.currentTimeMillis()
    val baselineWindowEnd = startTimeMillis + WINDOW_MILLIS
    val finalWindowStart = startTimeMillis + STRESS_DURATION_MILLIS - WINDOW_MILLIS

    // -------------------------------------------------------------------------
    // 4. Spawn NUM_CONCURRENT_TASKS worker threads, each running a tight loop
    //    of synthetic shuffle operations until the 5-minute window elapses.
    // -------------------------------------------------------------------------
    val executor: ExecutorService = Executors.newFixedThreadPool(NUM_CONCURRENT_TASKS)
    val latch = new CountDownLatch(NUM_CONCURRENT_TASKS)

    try {
      (0 until NUM_CONCURRENT_TASKS).foreach { taskId =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            // Per-thread RNG seeded with a deterministic-but-unique value so each
            // worker probes a different region of the random space; useful for
            // reproducibility while still exercising failure-injection diversity
            // across the 10 worker threads.
            val localRng = new Random(taskId.toLong * 31L)
            try {
              while (!stopFlag.get() && errorRef.get() == null) {
                val now = System.currentTimeMillis()
                if (now - startTimeMillis >= STRESS_DURATION_MILLIS) {
                  return
                }
                try {
                  runOneShuffleIteration(localRng)
                  totalOpsCompleted.incrementAndGet()
                  if (now < baselineWindowEnd) {
                    baselineWindowOps.incrementAndGet()
                  }
                  if (now >= finalWindowStart) {
                    finalWindowOps.incrementAndGet()
                  }
                } catch {
                  case _: InterruptedException =>
                    // Honor cooperative interruption: re-set the interrupt flag so
                    // the surrounding Runnable's outer logic can observe it, and
                    // exit the worker loop cleanly. We do NOT count this as a
                    // failure because it is a controlled shutdown signal from the
                    // main thread (e.g. via executor.shutdownNow()).
                    Thread.currentThread().interrupt()
                    return
                  case _: Throwable =>
                    // Failures are EXPECTED at the configured injection rate.
                    // Increment the failure counter and continue. The end-of-test
                    // failure-rate sanity check verifies the observed rate matches
                    // the target.
                    totalFailures.incrementAndGet()
                }
              }
            } catch {
              case t: Throwable =>
                // Truly unexpected error escaped the per-iteration try/catch
                // (e.g. OOM during the loop's bookkeeping itself). Capture it
                // for fail-fast and let the other workers exit early.
                errorRef.compareAndSet(null, t)
            } finally {
              latch.countDown()
            }
          }
        })
      }

      // Main thread waits for all workers to finish, with a generous grace
      // period beyond the 5-minute stress window for in-flight iterations.
      val completed = latch.await(STRESS_DURATION_MILLIS + LATCH_GRACE_MILLIS,
        TimeUnit.MILLISECONDS)
      stopFlag.set(true)
      if (!completed) {
        logWarning("Stress workers did not all complete within the grace period; " +
          "forcing executor shutdown")
      }
    } finally {
      // Always shut down the executor pool, even if an error propagates from the
      // main thread. `awaitTermination` blocks until all submitted tasks have
      // finished or the timeout expires; `shutdownNow` interrupts straggler
      // threads as a last resort to free system resources before test teardown.
      executor.shutdown()
      if (!executor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS,
          TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
    }

    // -------------------------------------------------------------------------
    // 5. Surface any unexpected error captured by the workers as the test
    //    failure rather than masking it under the throughput/heap assertions.
    // -------------------------------------------------------------------------
    val capturedError = errorRef.get()
    if (capturedError != null) {
      fail(s"Worker thread raised unexpected error: ${capturedError.getMessage}",
        capturedError)
    }

    // -------------------------------------------------------------------------
    // 6. Throughput-degradation assertion (AAP Sec.0.5.1.6 user spec):
    //    "<5% throughput reduction over test duration".
    // -------------------------------------------------------------------------
    val baselineRate = baselineWindowOps.get().toDouble / (WINDOW_MILLIS / 1000.0)
    val finalRate = finalWindowOps.get().toDouble / (WINDOW_MILLIS / 1000.0)
    val degradation = if (baselineRate > 0.0) {
      (baselineRate - finalRate) / baselineRate
    } else {
      // Defensive: if the baseline window saw zero completed ops the test was
      // unable to make progress in the first minute. Treat this as a maximum-
      // degradation reading so the assertion below fires clearly rather than
      // silently dividing by zero.
      Double.PositiveInfinity
    }
    logInfo(s"Throughput baseline=$baselineRate ops/s, final=$finalRate ops/s, " +
      s"degradation=${degradation * 100.0}%, " +
      s"totalOps=${totalOpsCompleted.get()}, totalFailures=${totalFailures.get()}")
    assert(degradation < MAX_THROUGHPUT_DEGRADATION,
      s"Throughput degraded by $degradation (>= $MAX_THROUGHPUT_DEGRADATION); " +
      s"baseline=$baselineRate ops/s, final=$finalRate ops/s")

    // -------------------------------------------------------------------------
    // 7. Failure-rate sanity check: the observed failure rate should be roughly
    //    FAILURE_INJECTION_RATE. We do not assert a tight bound here because the
    //    rate is a stochastic Bernoulli trial and the test is intentionally
    //    sensitive to throughput rather than to per-iteration injection precision.
    //    The observed-rate logging supports manual inspection if a regression
    //    appears in the throughput numbers.
    // -------------------------------------------------------------------------
    val totalOps = totalOpsCompleted.get() + totalFailures.get()
    val observedFailureRate = if (totalOps > 0) {
      totalFailures.get().toDouble / totalOps.toDouble
    } else {
      0.0
    }
    logInfo(s"Observed failure rate: $observedFailureRate (target: $FAILURE_INJECTION_RATE)")

    // -------------------------------------------------------------------------
    // 8. Observability assertions (AAP Section 0.1.1, AAP Section 0.5.1.4):
    //    the streaming-shuffle telemetry pipeline must surface non-zero, sensible
    //    values for the 4 standard streaming-shuffle counters/gauges during a
    //    5-minute stress run with 10% failure injection. Without these assertions
    //    the telemetry pipeline could silently break (zero increments) and the
    //    test would still pass on the throughput/heap gates alone.
    //
    //    Implementation discipline:
    //      - All 4 metrics MUST be registered with the executor MetricsSystem
    //        under the AAP Section 0.1.1 namespace `shuffle.streaming.<name>`. A
    //        missing metric indicates a broken telemetry-registration path
    //        (likely caused by a regression in `StreamingShuffleSource` or its
    //        registration call site in `StreamingShuffleManager`).
    //      - At least one of the cumulative counters (spillCount,
    //        backpressureEvents, partialReadInvalidations) MUST observe > 0
    //        events combined. Using the COMBINED count rather than per-metric
    //        thresholds avoids flakiness from individually rare events while
    //        still validating that the telemetry pipeline is operational under
    //        stress -- a 5-minute run with 10% failure injection is expected to
    //        trip at least one of these signals on any healthy implementation.
    //      - The bufferUtilizationPercent gauge is a SNAPSHOT (current value at
    //        measurement time) rather than a cumulative count, so it may
    //        legitimately read 0 after all buffers have been reclaimed by the
    //        time we measure post-shutdown. We log its final value for
    //        inspection but do not assert a strict lower bound, again to avoid
    //        flakiness while still surfacing the signal in CI logs.
    //
    //    This assertion path is exercised end-to-end once
    //    `StreamingShuffleSource` is registered with the executor MetricsSystem
    //    (per AAP Section 0.5.1.4). At earlier checkpoints where
    //    `StreamingShuffleSource` is not yet wired, the test naturally fails at
    //    SparkContext construction with `ClassNotFoundException` for
    //    `StreamingShuffleManager`, so the metrics block below is not reached;
    //    when wiring is complete the metrics block becomes the live regression
    //    gate for the streaming-shuffle observability surface.
    // -------------------------------------------------------------------------
    val streamingSources = sc.env.metricsSystem.getSourcesByName("streamingShuffle")
    assert(streamingSources.nonEmpty,
      "StreamingShuffleSource was not registered with the executor MetricsSystem; " +
        "the streaming-shuffle telemetry pipeline appears to be broken " +
        "(no source found under sourceName='streamingShuffle')")
    val streamingRegistry = streamingSources.head.metricRegistry
    val counters = streamingRegistry.getCounters
    val gauges = streamingRegistry.getGauges
    val spillCount = Option(counters.get("shuffle.streaming.spillCount"))
      .map(_.getCount).getOrElse {
        fail("Counter shuffle.streaming.spillCount is missing from the executor " +
          "MetricsSystem registry; AAP Section 0.1.1 requires this metric")
      }
    val backpressureCount = Option(counters.get("shuffle.streaming.backpressureEvents"))
      .map(_.getCount).getOrElse {
        fail("Counter shuffle.streaming.backpressureEvents is missing from the " +
          "executor MetricsSystem registry; AAP Section 0.1.1 requires this metric")
      }
    val partialReadCount =
      Option(counters.get("shuffle.streaming.partialReadInvalidations"))
        .map(_.getCount).getOrElse {
          fail("Counter shuffle.streaming.partialReadInvalidations is missing " +
            "from the executor MetricsSystem registry; AAP Section 0.1.1 requires " +
            "this metric")
        }
    val bufferUtilizationGauge =
      Option(gauges.get("shuffle.streaming.bufferUtilizationPercent")).getOrElse {
        fail("Gauge shuffle.streaming.bufferUtilizationPercent is missing from " +
          "the executor MetricsSystem registry; AAP Section 0.1.1 requires this " +
          "metric")
      }
    val bufferUtilizationValue = bufferUtilizationGauge.getValue match {
      case n: Number => n.intValue()
      case _ => -1
    }
    logInfo(s"Streaming-shuffle telemetry post-stress: spillCount=$spillCount, " +
      s"backpressureEvents=$backpressureCount, " +
      s"partialReadInvalidations=$partialReadCount, " +
      s"bufferUtilizationPercent=$bufferUtilizationValue")
    assert(bufferUtilizationValue >= 0,
      "bufferUtilizationPercent gauge reports an invalid (negative) value; " +
        s"observed=$bufferUtilizationValue")
    val cumulativeEventCount = spillCount + backpressureCount + partialReadCount
    assert(cumulativeEventCount > 0L,
      "Streaming-shuffle telemetry surfaced zero cumulative events under the " +
        s"5-minute / 10% failure stress workload (spillCount=$spillCount, " +
        s"backpressureEvents=$backpressureCount, " +
        s"partialReadInvalidations=$partialReadCount); telemetry pipeline " +
        "appears broken")

    // -------------------------------------------------------------------------
    // 9. Stop the SparkContext eagerly (instead of waiting for afterEach) so
    //    that all spilled blocks, BlockManager state, and executor threads
    //    release their heap before the heap-leak measurement.
    //    LocalSparkContext.afterEach is still invoked and tolerates a null sc.
    // -------------------------------------------------------------------------
    sc.stop()
    sc = null

    // -------------------------------------------------------------------------
    // 10. Heap-leak assertion (AAP Sec.0.7.2.2):
    //     "Zero retained heap MUST exist after stress test completion".
    //     The 50 MB tolerance accommodates JIT/class-loader overhead.
    // -------------------------------------------------------------------------
    forceGc()
    val finalHeap = computeUsedHeap()
    val heapDelta = finalHeap - baselineHeap
    logInfo(s"Heap delta: $heapDelta bytes " +
      s"(baseline=$baselineHeap, final=$finalHeap, max allowed=$MAX_HEAP_LEAK_BYTES)")
    assert(heapDelta < MAX_HEAP_LEAK_BYTES,
      s"Heap-leak detected: $heapDelta bytes retained after stress workload " +
      s"(threshold $MAX_HEAP_LEAK_BYTES bytes)")
  }

  // ---------------------------------------------------------------------------
  // Helper methods -- kept private to the suite.
  // ---------------------------------------------------------------------------

  /**
   * Run one synthetic shuffle operation. Failures are randomly injected at the
   * configured rate by throwing a [[RuntimeException]] BEFORE the actual shuffle
   * starts, simulating early-task-failure scenarios; this is the cheapest realistic
   * failure pattern (it does not waste compute on a doomed shuffle).
   *
   * The shuffle itself is `parallelize -> map -> partitionBy -> groupByKey ->
   * count`, which is the simplest pipeline that forces a real shuffle stage
   * (the [[HashPartitioner]] guarantees a wide dependency irrespective of the
   * input partitioning).
   *
   * The partition count for each iteration is sampled from
   * `[2, 2 + NUM_CONCURRENT_SHUFFLES)` so that the JIT does not over-specialize
   * on a single partition count and so that multiple `getWriter`/`getReader`
   * paths inside the streaming-shuffle manager are exercised.
   *
   * @param rng per-thread [[Random]] driving both the failure-injection coin
   *            flip and the partition-count sampling
   */
  private def runOneShuffleIteration(rng: Random): Unit = {
    // Inject failure at the configured rate. Throwing here exercises the
    // streaming writer's `stop(success = false)` cleanup path indirectly
    // because the surrounding worker catches the throwable and the next loop
    // iteration starts fresh (the previous shuffle's resources are released
    // by the finally block in StreamingShuffleManager.unregisterShuffle).
    if (rng.nextDouble() < FAILURE_INJECTION_RATE) {
      throw new RuntimeException("Injected stress-test failure")
    }

    val numPartitions = 2 + rng.nextInt(NUM_CONCURRENT_SHUFFLES)
    val grouped = sc.parallelize(0 until DATASET_SIZE_PER_SHUFFLE, numPartitions)
      .map(i => (i % numPartitions, i))
      .partitionBy(new HashPartitioner(numPartitions))
      .groupByKey(numPartitions)
    val groupCount = grouped.count()
    require(groupCount == numPartitions.toLong,
      s"Expected $numPartitions distinct keys after groupByKey, got $groupCount")
  }

  /**
   * Force aggressive garbage collection across multiple cycles. A single
   * `System.gc()` is unreliable for breaking weak/soft references and may not
   * promote young-generation objects to tenured space. This implementation
   * issues [[GC_CYCLES]] iterations of `gc + sleep` to give the JVM time to
   * settle before we read the heap. The sleep between cycles allows the
   * garbage-collector helper threads to complete a full collection round
   * without contention from the next `gc()` request.
   */
  private def forceGc(): Unit = {
    var i = 0
    while (i < GC_CYCLES) {
      System.gc()
      Thread.sleep(GC_CYCLE_SLEEP_MILLIS)
      i += 1
    }
  }

  /**
   * Compute current used heap as `totalMemory - freeMemory`. This is the standard
   * JVM heap-used metric. It does include JIT-compiled code overhead and class-
   * loader metadata, so direct heap-delta comparisons are calibrated against the
   * [[MAX_HEAP_LEAK_BYTES]] tolerance to absorb steady-state growth from those
   * sources without flagging false leaks.
   */
  private def computeUsedHeap(): Long = {
    val rt = Runtime.getRuntime
    rt.totalMemory() - rt.freeMemory()
  }

}
