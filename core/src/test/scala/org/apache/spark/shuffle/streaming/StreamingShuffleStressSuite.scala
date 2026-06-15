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

import scala.util.Random

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, UNSAFE_EXCEPTION_ON_MEMORY_LEAK}

/**
 * Long-running stress + soak suite for the streaming shuffle backend.
 *
 * This is one of the F-121 streaming-shuffle suites and a merge-gate artifact: it drives the
 * streaming backend continuously for five minutes under a steady churn of RETRIABLE task failures
 * (~10% of iterations) and proves that the backend retains ZERO heap -- i.e. no task leaks
 * managed execution memory -- while every shuffle job still produces byte-for-byte correct
 * results (zero data loss under churn).
 *
 * ==Why the run is guarded==
 * A literal five-minute run cannot execute in the normal unit build, so the body is guarded by
 * `assume(stressEnabled, ...)`: the test is CANCELLED (a no-op) unless the stress profile is
 * explicitly enabled via `-Dspark.test.stress=true` (or the `SPARK_STREAMING_STRESS=1` env var).
 * The duration is parameterised through `-Dspark.test.stress.durationMs` and defaults to five
 * minutes when the profile is on; a small value can be supplied to smoke-exercise the harness.
 *
 * ==How "zero retained heap" is proven==
 * The proof is structural rather than assertion-based. The conf sets
 * `spark.unsafe.exceptionOnMemoryLeak=true`, so Spark's executor leak detector throws if any task
 * completes successfully while still holding acquired execution memory. Combined with
 * [[LocalSparkContext]] stopping the context after the test, a clean completion under sustained
 * 10% failure churn IS the zero-retained-heap assertion.
 *
 * ==Coexistence with the sort-based path==
 * The suite activates the streaming backend through the public configuration contract only
 * (`spark.shuffle.manager=streaming` plus `spark.shuffle.streaming.enabled=true`). The streaming
 * manager transparently falls back to the unchanged sort-based shuffle manager when its fallback
 * conditions trip; either way the job must finish correctly and without leaks, which is exactly
 * what this suite verifies.
 */
class StreamingShuffleStressSuite extends SparkFunSuite with LocalSparkContext {

  // The five-minute body runs ONLY when the stress profile is explicitly enabled, so the normal
  // unit build never spends five minutes here. Either a system property or an env var turns it on.
  private val stressEnabled =
    sys.props.get("spark.test.stress").contains("true") ||
      sys.env.get("SPARK_STREAMING_STRESS").contains("1")

  // Total wall-clock budget for the churn loop. Defaults to five minutes when the stress profile
  // is enabled; override with `-Dspark.test.stress.durationMs=<ms>` (e.g. a few seconds) to
  // smoke-exercise the harness itself without paying the full five-minute cost.
  private val durationMs =
    sys.props.get("spark.test.stress.durationMs").map(_.toLong).getOrElse(5L * 60 * 1000)

  // A fixed seed makes both the generated data and the 10% failure schedule reproducible, so a
  // failure observed in one run can be replayed deterministically in the next.
  private val rngSeed = 0x5ca1ab1eL

  // Roughly one in ten iterations injects a (retriable) failure.
  private val failureInjectionPercent = 10

  // Per-iteration sizing is deliberately modest so a five-minute budget yields MANY small shuffle
  // jobs (broad churn) rather than one giant job.
  private val recordsPerIteration = 4000
  private val numKeys = 128
  private val numPartitions = 8

  test("5-minute streaming shuffle stress with 10% failure injection retains zero heap") {
    // Guard first: cancel (no-op) in the normal build; only run under the stress profile.
    assume(
      stressEnabled,
      "stress profile disabled; set -Dspark.test.stress=true (optionally " +
        "-Dspark.test.stress.durationMs=<ms>) to run this long-running stress suite")

    // local[4, 4] = four worker threads AND up to four task attempts. The plain local[N] form
    // hard-codes maxTaskFailures=1 (no retry in local mode), which would turn an injected
    // attempt-0 failure into a permanent job failure. The local[N, maxFailures] form (Spark's
    // LOCAL_N_FAILURES_REGEX, intended "for tests with failing tasks") lets Spark recompute the
    // failed attempt so every job still completes -- exactly the producer-recompute churn this
    // suite must survive without leaking memory or losing data.
    val conf = new SparkConf()
      .setMaster("local[4, 4]")
      .setAppName("streaming-shuffle-stress")
      .set(SHUFFLE_MANAGER, "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      // The zero-retained-heap proof: with leak detection on, any task that completes
      // successfully while still holding acquired execution memory throws and fails this test.
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
    sc = new SparkContext(conf)

    // The activation pair above must actually select the streaming backend (rather than silently
    // resolving to sort at construction time), otherwise this suite would test nothing.
    assert(
      sc.env.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "streaming shuffle backend must be active under spark.shuffle.manager=streaming")

    val rng = new Random(rngSeed)
    val start = System.currentTimeMillis()
    var iterations = 0
    var injections = 0

    // Churn loop: keep launching small randomized shuffle jobs until the time budget is spent.
    while (System.currentTimeMillis() - start < durationMs) {
      val inject = rng.nextInt(100) < failureInjectionPercent
      if (inject) injections += 1
      // Rotate which partition's first attempt fails so the churn is spread across the stage.
      val failingPartition = iterations % numPartitions
      runRandomizedShuffleJob(rng, inject, failingPartition, iterations)
      iterations += 1
    }

    // The loop must have done real work; with the default five-minute budget this is many jobs.
    assert(iterations > 0, "stress loop did not complete a single iteration")
    logInfo(
      s"streaming shuffle stress finished: iterations=$iterations, " +
        s"failureInjections=$injections, durationMs=$durationMs")
    // Reaching here cleanly -- under spark.unsafe.exceptionOnMemoryLeak=true plus the sc.stop()
    // teardown performed by LocalSparkContext -- IS the zero-retained-heap proof: no successful
    // task leaked managed memory across the entire churn run.
  }

  /**
   * Run a single randomized shuffle job and assert its result is EXACTLY correct (zero data
   * loss), optionally injecting one retriable attempt-0 failure into a chosen map partition.
   *
   * The failure is injected inside the shuffle MAP stage: the first attempt of `failingPartition`
   * throws, so Spark recomputes that task (the retry, attempt > 0, succeeds). Because the task
   * threw, Spark's leak detector intentionally skips the leak check for that failed attempt, so
   * an injected failure never produces a false-positive leak; the guarantee under test is that
   * SUCCESSFUL attempts -- including the reduce-side reads of streaming output -- never leak.
   *
   * The shuffle operator is rotated across `reduceByKey`, `groupByKey`, and `sortByKey` so all
   * three reduce-side code paths are exercised over the run.
   */
  private def runRandomizedShuffleJob(
      rng: Random,
      inject: Boolean,
      failingPartition: Int,
      iteration: Int): Unit = {
    // Fresh, modest, reproducible batch of (key, value) records for this iteration.
    val pairs = IndexedSeq.fill(recordsPerIteration)((rng.nextInt(numKeys), rng.nextInt(1000)))
    val base = sc.parallelize(pairs, numPartitions)

    // Insert the retriable failure into the map stage. The closure captures only primitives
    // (inject / failingPartition / iteration); the driver-side `rng` is never serialized into it.
    val mapped = base.map { kv =>
      if (inject && TaskContext.get().attemptNumber() == 0 &&
        TaskContext.getPartitionId() == failingPartition) {
        throw new RuntimeException(
          s"injected retriable stress failure (iteration=$iteration, attempt=0)")
      }
      kv
    }

    iteration % 3 match {
      case 0 =>
        // reduceByKey: the per-key sum must match the locally computed histogram of values.
        val expected = pairs.groupBy(_._1).map { case (k, vs) => k -> vs.map(_._2).sum }
        val actual = mapped.reduceByKey(_ + _).collect().toMap
        assert(actual == expected, s"reduceByKey result mismatch on iteration $iteration")
      case 1 =>
        // groupByKey: every key's group must contain exactly the expected number of values.
        val expected = pairs.groupBy(_._1).map { case (k, vs) => k -> vs.size }
        val actual = mapped.groupByKey().collect().map { case (k, vs) => k -> vs.size }.toMap
        assert(actual == expected, s"groupByKey group sizes mismatch on iteration $iteration")
      case _ =>
        // sortByKey: keys must come out non-decreasing AND the full (k, v) multiset must be
        // preserved (no record lost, duplicated, or altered under churn).
        val collected = mapped.sortByKey().collect().toSeq
        val keys = collected.map(_._1)
        assert(keys == keys.sorted, s"sortByKey produced unordered keys on iteration $iteration")
        assert(
          collected.sorted.toList == pairs.sorted.toList,
          s"sortByKey lost or altered records on iteration $iteration")
    }
  }
}
