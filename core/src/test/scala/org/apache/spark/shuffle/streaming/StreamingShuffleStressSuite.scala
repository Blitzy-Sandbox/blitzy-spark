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
 * ==Two lanes: an always-run smoke and an opt-in 5-minute soak==
 *
 * A literal 5-minute soak cannot execute on every unit build, but the merge gate must still have
 * executed -- not skipped -- proof that the churn harness runs, injects failures, recovers, and
 * retains no heap. This suite therefore exposes TWO tests:
 *
 *   1. an ALWAYS-RUN bounded smoke ("streaming shuffle stress smoke ...") that runs a fixed number
 *      of churn iterations ([[smokeIterations]]) with deterministic ~10% failure injection. It runs
 *      on the normal unit build (it is NOT guarded by `assume`, so it never reports `<skipped/>`),
 *      and it asserts the gate explicitly: iterations > 0, injected > 0, the injection ratio is
 *      ~10%, and -- as direct retained-heap evidence -- the executor's managed execution memory is
 *      back to zero after the run (complementing the per-task `exceptionOnMemoryLeak` check); and
 *   2. an opt-in full soak ("5-minute streaming shuffle soak ...") guarded by
 *      `assume(stressEnabled, ...)`, which runs for [[durationMs]] (default 5 minutes) when the
 *      stress profile is armed with `-Dspark.test.stress=true` (or `SPARK_STREAMING_STRESS=1`),
 *      asserting the same invariants over the full duration.
 *
 * Both lanes share one churn loop ([[runStressLoop]]) and one retained-heap check
 * ([[assertZeroRetainedManagedMemory]]), so the smoke validates the exact harness the soak uses.
 */
class StreamingShuffleStressSuite extends SparkFunSuite with LocalSparkContext {

  // The stress profile gate for the OPT-IN soak only. The 5-minute body runs only when explicitly
  // opted in (the always-run smoke below is never gated by this). Either a JVM system property
  // (-Dspark.test.stress=true) or an environment variable (SPARK_STREAMING_STRESS=1) arms it,
  // matching the two common ways CI stress lanes are configured.
  private val stressEnabled =
    sys.props.get("spark.test.stress").contains("true") ||
      sys.env.get("SPARK_STREAMING_STRESS").contains("1")

  // The opt-in soak duration. Defaults to the AAP-mandated 5 minutes when the profile is on; a
  // smaller value (e.g. -Dspark.test.stress.durationMs=8000) exercises the same harness quickly.
  private val durationMs =
    sys.props.get("spark.test.stress.durationMs").map(_.toLong).getOrElse(5L * 60 * 1000)

  // Fixed iteration count for the ALWAYS-RUN smoke lane. A multiple of 10 so deterministic 10%
  // failure injection (every 10th iteration) yields an exact ~10% ratio; override with
  // -Dspark.test.stress.smokeIterations. Kept modest so the normal unit build stays fast.
  private val smokeIterations =
    sys.props.get("spark.test.stress.smokeIterations").map(_.toInt).getOrElse(30)

  // A fixed default seed makes the data churn (record counts, key cardinality, and partition
  // counts) reproducible across runs; override with -Dspark.test.stress.seed for variety.
  private val stressSeed =
    sys.props.get("spark.test.stress.seed").map(_.toLong).getOrElse(20240601L)

  // Deterministic ~10% failure injection: exactly every 10th iteration injects a retriable
  // first-attempt task failure. Deterministic (not RNG) so the injected count and ratio are exact
  // and the merge gate never flakes on a run that happened to inject zero failures.
  private def injectAt(iteration: Int): Boolean = iteration % 10 == 0

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

  /**
   * Builds the streaming-shuffle `SparkContext` shared by both lanes. NOTE on the master string:
   * `local[N]` forces maxFailures=1 (no task retries) via `SparkContext.MAX_LOCAL_TASK_FAILURES`,
   * which would make the injected first-attempt failures abort the job. The `local[N, M]` form sets
   * maxFailures=M, so `local[4, 4]` gives Spark the retry budget the failure injection depends on.
   */
  private def newStreamingContext(appName: String): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[4, 4]")
      .setAppName(appName)
      // Activation requires BOTH signals: select the manager alias AND flip the feature flag.
      .set(SHUFFLE_MANAGER, "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      // The zero-retained-heap contract: a leaked task makes Spark throw, failing the test.
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Keep the runs lean and avoid UI port churn across parallel stress lanes.
      .set("spark.ui.enabled", "false")
    val context = new SparkContext(conf)
    // The streaming backend must actually be installed. Even when it internally falls back to the
    // sort path for a given shuffle, the registered manager is still the streaming manager.
    assert(context.env.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "expected spark.shuffle.manager=streaming to install StreamingShuffleManager")
    context
  }

  /**
   * The single churn loop shared by BOTH the always-run smoke and the opt-in soak. It repeatedly
   * runs randomized shuffle jobs under the streaming manager until `shouldContinue` returns
   * false, injecting a deterministic retriable failure on exactly every 10th iteration (see
   * [[injectAt]]). Each job verifies its own result inside [[runShuffleJob]], so a returned
   * iteration count is also a count of fully-verified, zero-data-loss shuffles.
   *
   * @param shouldContinue predicate over (completedIterations, elapsedMs); the loop runs while it
   *                       holds, letting the smoke lane bound by iteration count and the soak lane
   *                       bound by wall-clock duration through one identical body
   * @return the (iterations, injected) pair actually executed
   */
  private def runStressLoop(shouldContinue: (Int, Long) => Boolean): (Int, Int) = {
    val start = System.currentTimeMillis()
    var iterations = 0
    var injected = 0
    while (shouldContinue(iterations, System.currentTimeMillis() - start)) {
      // Per-iteration data is seeded off the iteration index so the churn is reproducible while
      // still varying record counts, key cardinality, and partition counts across iterations.
      val dataRng = new Random(stressSeed + iterations)
      val numKeys = 8 + dataRng.nextInt(40)
      val n = 256 + dataRng.nextInt(1024)
      val data = Array.fill(n)((dataRng.nextInt(numKeys), dataRng.nextInt(1000)))
      val nPartitions = 4 + dataRng.nextInt(9)
      val jobType = iterations % 3
      val injectFailure = injectAt(iterations)
      if (injectFailure) {
        injected += 1
      }
      runShuffleJob(data, nPartitions, jobType, injectFailure, iterations)
      iterations += 1
    }
    (iterations, injected)
  }

  /**
   * Asserts the deterministic failure-injection contract. [[injectAt]] fires on exactly every 10th
   * iteration, so over `iterations` runs the injected count must equal `ceil(iterations / 10)` and
   * be strictly positive. That exact every-10th rate IS the AAP's "~10% failure injection" stated
   * deterministically, so the merge gate never flakes on a run that injects too few or too many.
   */
  private def assertInjectionContract(iterations: Int, injected: Int): Unit = {
    assert(iterations > 0, "stress loop completed zero iterations")
    val expectedInjected = (iterations + 9) / 10
    assert(injected == expectedInjected,
      s"expected $expectedInjected injected failures over $iterations iterations (every 10th), " +
        s"got $injected")
    assert(injected > 0, "stress loop injected zero failures; the failure path was never exercised")
  }

  /**
   * Direct, explicit retained-heap evidence that complements the per-task `exceptionOnMemoryLeak`
   * detector. After the loop finishes, the executor's managed execution memory -- the pool the
   * streaming writer's `MemoryConsumer` acquires from for its per-partition buffers, and the pool a
   * spill must release back -- must be fully released. Because task cleanup
   * (`TaskMemoryManager.cleanUpAllAllocatedMemory`) runs slightly after the result reaches the
   * driver, we poll briefly rather than asserting a single instantaneous read.
   */
  private def assertZeroRetainedManagedMemory(): Unit = {
    val memoryManager = sc.env.memoryManager
    val deadline = System.currentTimeMillis() + 5000
    while (memoryManager.executionMemoryUsed != 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    assert(memoryManager.executionMemoryUsed == 0,
      s"streaming shuffle retained ${memoryManager.executionMemoryUsed} bytes of managed " +
        "execution memory after the stress loop; expected zero (no buffer/spill leak)")
  }

  // Lane 1 -- ALWAYS RUN on the normal unit build. It is NOT guarded by `assume`, so it never
  // reports `<skipped/>`: it is the executed merge-gate proof that the churn harness runs, injects
  // ~10% retriable failures, recovers them (every result is verified), and retains zero heap.
  test("streaming shuffle stress smoke: bounded churn injects, recovers, and retains zero heap") {
    sc = newStreamingContext("StreamingShuffleStressSuite-smoke")
    val (iterations, injected) = runStressLoop((completed, _) => completed < smokeIterations)

    // The harness ran the full bounded set, injected the deterministic ~10%, and recovered each.
    assert(iterations == smokeIterations, s"expected $smokeIterations iterations, ran $iterations")
    assertInjectionContract(iterations, injected)
    // Explicit retained-heap evidence, in addition to the per-task exceptionOnMemoryLeak detector.
    assertZeroRetainedManagedMemory()
    logInfo(s"streaming shuffle stress smoke ran $iterations iterations ($injected injected)")
  }

  // Lane 2 -- OPT-IN soak, armed with -Dspark.test.stress=true (or SPARK_STREAMING_STRESS).
  // It runs the identical harness the smoke lane validated, for the full AAP-mandated duration.
  test("5-minute streaming shuffle soak with 10% failure injection retains zero heap") {
    assume(stressEnabled, "stress profile disabled; set -Dspark.test.stress=true to run")
    sc = newStreamingContext("StreamingShuffleStressSuite-soak")
    val (iterations, injected) = runStressLoop((_, elapsedMs) => elapsedMs < durationMs)

    assertInjectionContract(iterations, injected)
    assertZeroRetainedManagedMemory()
    logInfo(s"streaming shuffle soak ran $iterations iterations " +
      s"($injected injected) durationMs=$durationMs")
  }
}
