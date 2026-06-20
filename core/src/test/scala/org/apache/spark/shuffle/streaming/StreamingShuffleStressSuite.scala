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
 * A long-running stress suite for the opt-in streaming shuffle backend
 * ([[StreamingShuffleManager]]). It is one of the F-121 merge-gate suites (AAP 0.4.4): it runs a
 * continuous churn of randomized shuffle jobs under the streaming manager with ~10% retriable
 * task-failure injection and asserts both that every job produces correct results despite the churn
 * (zero data loss) and that nothing leaks managed memory.
 *
 * ==Why a clean finish proves "zero retained heap"==
 *
 * The conf sets `spark.unsafe.exceptionOnMemoryLeak=true`, so any executor task that finishes with
 * un-freed managed memory makes Spark throw a `SparkException` ("memory leak"). A failed task
 * attempt has its memory reclaimed without masking the original failure (see `FailureSuite`), so
 * only a genuine leak on a *successful* task can fail the run. Reaching the end of the loop with
 * every result verified -- and `LocalSparkContext` then stopping the context in `afterEach` -- is
 * therefore the structural proof that the streaming writer/reader and their buffer/spill paths
 * retain zero heap under churn. This mirrors the leak-detector contract exercised by `FailureSuite`
 * and `SortShuffleWriterSuite`, the models for this suite.
 *
 * ==Why this does not run in the normal unit build==
 *
 * A literal 5-minute run cannot execute unconditionally in CI. The single test is therefore guarded
 * by `assume(stressEnabled, ...)`, which cancels it (a no-op) unless the stress profile is enabled
 * with `-Dspark.test.stress=true` (or `SPARK_STREAMING_STRESS=1`). The run duration is set by
 * `-Dspark.test.stress.durationMs` (default 5 minutes when the profile is on), so the same
 * harness can be exercised quickly as a smoke run and as a full 5-minute soak.
 */
class StreamingShuffleStressSuite extends SparkFunSuite with LocalSparkContext {

  // The stress profile gate. The 5-minute body runs ONLY when explicitly opted in, so the normal
  // unit build never spends minutes here (the test cancels via `assume`). Either a JVM system
  // property (-Dspark.test.stress=true) or an environment variable (SPARK_STREAMING_STRESS=1) arms
  // it, matching the two common ways CI stress lanes are configured.
  private val stressEnabled =
    sys.props.get("spark.test.stress").contains("true") ||
      sys.env.get("SPARK_STREAMING_STRESS").contains("1")

  // The soak duration. Defaults to the AAP-mandated 5 minutes when the profile is on; a smaller
  // value (e.g. -Dspark.test.stress.durationMs=8000) exercises the same harness quickly so the loop
  // itself can be validated without a full soak.
  private val durationMs =
    sys.props.get("spark.test.stress.durationMs").map(_.toLong).getOrElse(5L * 60 * 1000)

  // A fixed default seed makes the churn (data, partition counts, and the ~10% failure-injection
  // decisions) reproducible across runs; override with -Dspark.test.stress.seed for variety.
  private val stressSeed =
    sys.props.get("spark.test.stress.seed").map(_.toLong).getOrElse(20240601L)

  // ~10% of iterations inject a failure. Named so the intent is explicit at the call site.
  private val failureInjectionRate = 0.10

  /**
   * Runs one randomized shuffle job under the streaming manager and asserts its result is correct.
   *
   * The job's lineage starts with a `map` that, on the FIRST attempt of a subset of partitions and
   * only when `injectFailure` is set, throws a retriable exception. Because the conf uses the
   * `local[N, M]` master (M > 1), Spark retries the failed task; the second attempt
   * (`attemptNumber > 0`) does not throw, so the job ultimately succeeds. This is "churn + recovery
   * without leaks", not permanent failure. The closure captures only the two primitives `inject`
   * and `it` (never `this`), so it stays serializable.
   *
   * @param data          the key/value records to shuffle (also the source of the expected result)
   * @param nPartitions   the number of input partitions for this iteration
   * @param jobType       selects the shuffle operator: 0 = reduceByKey, 1 = groupByKey, else
   *                      sortByKey -- rotated across iterations so all three read/write paths run
   * @param injectFailure whether this iteration injects a retriable first-attempt task failure
   * @param it            the iteration index, used only for diagnostic messages
   */
  private def runShuffleJob(
      data: Array[(Int, Int)],
      nPartitions: Int,
      jobType: Int,
      injectFailure: Boolean,
      it: Int): Unit = {
    val inject = injectFailure
    val base = sc.parallelize(data.toIndexedSeq, nPartitions).map { kv =>
      // Fail only the first attempt of even-numbered partitions, and only for injecting iterations,
      // so Spark's retry recomputes the lost task and the job still completes correctly.
      if (inject && TaskContext.get().attemptNumber() == 0 &&
          TaskContext.get().partitionId() % 2 == 0) {
        throw new RuntimeException(s"injected retriable stress failure (it=$it)")
      }
      kv
    }
    jobType match {
      case 0 =>
        val result = base.reduceByKey(_ + _).collect().toMap
        val expected = data.groupBy(_._1).map { case (k, vs) => k -> vs.map(_._2).sum }
        assert(result == expected, s"reduceByKey result mismatch at iteration $it")
      case 1 =>
        val result = base.groupByKey().collect()
          .map { case (k, vs) => k -> vs.toList.sorted }.toMap
        val expected =
          data.groupBy(_._1).map { case (k, vs) => k -> vs.map(_._2).toList.sorted }
        assert(result == expected, s"groupByKey result mismatch at iteration $it")
      case _ =>
        val result = base.sortByKey().collect()
        val keys = result.map(_._1).toList
        assert(keys == keys.sorted, s"sortByKey output not ordered at iteration $it")
        assert(result.sorted.toList == data.sorted.toList,
          s"sortByKey lost or duplicated records at iteration $it")
    }
  }

  // The single, assume-guarded stress test. NOTE on the master string: `local[N]` forces
  // maxFailures=1 (no task retries) via SparkContext.MAX_LOCAL_TASK_FAILURES, which would make the
  // injected first-attempt failures abort the job. The `local[N, M]` form sets maxFailures=M, so we
  // use `local[4, 4]` to give Spark the retry budget the failure-injection scenario depends on.
  test("5-minute streaming shuffle stress with 10% failure injection retains zero heap") {
    assume(stressEnabled, "stress profile disabled; set -Dspark.test.stress=true to run")

    val conf = new SparkConf()
      .setMaster("local[4, 4]")
      .setAppName("StreamingShuffleStressSuite")
      // Activation requires BOTH signals: select the manager alias AND flip the feature flag.
      .set(SHUFFLE_MANAGER, "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      // The zero-retained-heap contract: a leaked task makes Spark throw, failing this test.
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Keep the long soak lean and avoid UI port churn across parallel stress lanes.
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)

    // The streaming backend must actually be installed. Even when it internally falls back to the
    // sort path for a given shuffle, the registered manager is still the streaming manager.
    assert(sc.env.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "expected spark.shuffle.manager=streaming to install StreamingShuffleManager")

    val injectRng = new Random(stressSeed)
    val start = System.currentTimeMillis()
    var iterations = 0
    var injected = 0
    while (System.currentTimeMillis() - start < durationMs) {
      // Per-iteration data is seeded off the iteration index so the churn is reproducible while
      // still varying record counts, key cardinality, and partition counts across iterations.
      val dataRng = new Random(stressSeed + iterations)
      val numKeys = 8 + dataRng.nextInt(40)
      val n = 256 + dataRng.nextInt(1024)
      val data = Array.fill(n)((dataRng.nextInt(numKeys), dataRng.nextInt(1000)))
      val nPartitions = 4 + dataRng.nextInt(9)
      val jobType = iterations % 3
      val injectFailure = injectRng.nextDouble() < failureInjectionRate
      if (injectFailure) {
        injected += 1
      }
      runShuffleJob(data, nPartitions, jobType, injectFailure, iterations)
      iterations += 1
    }

    // The loop must have done real work; a zero-iteration run would prove nothing.
    assert(iterations > 0, "stress loop completed zero iterations")
    logInfo(s"streaming shuffle stress ran $iterations iterations " +
      s"($injected injected) durationMs=$durationMs")
    // No explicit leak assertion is needed: LocalSparkContext.afterEach stops `sc`, and any task
    // that leaked managed memory would already have thrown because exceptionOnMemoryLeak=true.
    // Arriving here with every iteration verified IS the zero-retained-heap proof.
  }
}
