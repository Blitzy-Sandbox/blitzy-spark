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

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{LocalSparkContext, Partitioner, ShuffleDependency, SparkConf,
  SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

/**
 * Stress / soak suite for the streaming shuffle data path (test #13 of feature F-121).
 *
 * This suite validates the durability and no-leak targets recorded in the Agent Action Plan
 * (AAP section 0.9.2): a continuous run with roughly 10 percent failure injection that produces
 * correct results across the entire run (zero data loss) and leaves '''no retained heap''' once
 * the run completes.
 *
 * '''Runtime gating (must fit the 20-minute `SparkFunSuite` per-test timeout).''' `SparkFunSuite`
 * fails any single test that runs longer than `spark.test.timeout` minutes (default 20). The
 * sustained-run duration is therefore configurable through the
 * `spark.test.streamingStressDurationMs` system property and defaults to 300000 ms (5 minutes),
 * which both satisfies the AAP soak target and stays well under the timeout.
 * Continuous-integration runs override the property with a short value (for example
 * 5000-10000 ms) so the suite executes a quick smoke without approaching the timeout. The
 * effective duration is additionally clamped to [[StreamingShuffleStressSuite.MAX_RUN_MS]] so
 * that even an oversized override cannot push a single test past the budget, and every loop
 * runs at least once so the suite always has real coverage by default.
 *
 * '''Soak coverage with the streaming data path ACTIVE.''' In v1 the only documented, intentional
 * deviation (feature F-115) is the absence of an in-flight Netty '''push''' transport: there is
 * no mid-task push of in-progress blocks. Data still moves, because the producer-side
 * [[StreamingShuffleWriter]] frames each per-partition output as CRC32C-protected envelopes and
 * commits them through the shared `IndexShuffleBlockResolver` at map completion, and the
 * consumer-side [[StreamingShuffleReader]] fetches those committed blocks over the standard block
 * transfer service. An end-to-end `reduceByKey` therefore round-trips correctly through the
 * active streaming path, so this suite stresses both halves with the data path '''active''':
 *
 *  1. '''Correctness + zero data loss under failure injection''' is validated with the streaming
 *     data path ACTIVE (`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=
 *     true`). A sustained loop of real `reduceByKey` jobs (>= 16 partitions) injects roughly 10
 *     percent transient map-task faults; because the job runs on a `local[N, F]` master the
 *     faulted task is retried and the streaming write recomputed, so every iteration must still
 *     produce the correct aggregate -- proving zero data loss under sustained streaming retries.
 *  2. '''No retained heap''' is validated against the streaming '''producer''' path. With both
 *     flags on the streaming writer is driven through many allocate / write / spill / stop
 *     cycles; after each cycle the task's memory consumption is asserted to return to zero, so a
 *     buffer or execution-memory leak would surface immediately as monotonic growth.
 *
 * The choice of a buffer-count / task-memory accessor (rather than a brittle
 * absolute-JVM-memory measurement) follows the AAP's no-leak guidance: in local mode the
 * executor-only [[MemorySpillManager]] is not constructed, so its buffer registry is
 * unavailable; the suite instead asserts the writer's `TaskMemoryManager` consumption is
 * zero and that the manager's registered-shuffle bookkeeping returns to its baseline,
 * degrading gracefully to a clean-teardown assertion where a direct buffer-count accessor
 * is not exposed.
 */
class StreamingShuffleStressSuite extends SparkFunSuite with LocalSparkContext {

  import StreamingShuffleStressSuite._

  /**
   * Sustained-run duration in milliseconds. Defaults to 5 minutes (the AAP soak target) and is
   * overridable via the `spark.test.streamingStressDurationMs` system property so CI can run a
   * short smoke. See the class-level documentation for the gating rationale.
   */
  private val durationMs: Long =
    System.getProperty("spark.test.streamingStressDurationMs", "300000").toLong

  /**
   * Effective sustained-run duration: the configured duration clamped to [[MAX_RUN_MS]] so a
   * single test can never approach the 20-minute `SparkFunSuite` timeout regardless of the
   * override.
   */
  private val effectiveDurationMs: Long = math.min(math.max(durationMs, 0L), MAX_RUN_MS)

  /**
   * Maximum number of producer allocate/write/stop cycles in the no-leak stress loop, overridable
   * via `spark.test.streamingStressCycles`. The loop is bounded by both this count and the
   * effective duration so it stays fast and deterministic while still exercising many cycles.
   */
  private val maxLeakCycles: Int =
    System.getProperty("spark.test.streamingStressCycles", "200").toInt

  /** Number of reduce partitions for the shuffle workload; the AAP requires at least 16. */
  private val numShufflePartitions: Int = 16

  /** Number of partitions for the producer-path no-leak workload. */
  private val numWriterPartitions: Int = 4

  /**
   * A fixed, deterministic key/value dataset. Fifty distinct keys each receive forty records
   * whose values are the original record index, so the per-key aggregate is non-trivial yet
   * bounded well within `Int` range.
   */
  private val inputData: Seq[(Int, Int)] = (0 until 2000).map(i => (i % 50, i)).toSeq

  /**
   * The known-correct `reduceByKey(_ + _)` aggregate of [[inputData]], computed independently.
   */
  private val expectedAggregate: Map[Int, Int] =
    inputData.groupBy(_._1).map { case (key, kvs) => key -> kvs.map(_._2).sum }

  /**
   * Build a [[SparkConf]] that installs the streaming shuffle manager and toggles the streaming
   * data path. The Spark UI is disabled to keep the sustained run lightweight.
   *
   * @param master  the Spark master URL (use `local[N, F]` to permit retried task failures)
   * @param enabled the value of `spark.shuffle.streaming.enabled`
   */
  private def streamingConf(master: String, enabled: Boolean): SparkConf = {
    new SparkConf()
      .setMaster(master)
      .setAppName("streaming-shuffle-stress")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", enabled.toString)
      .set("spark.ui.enabled", "false")
  }

  /** A hash partitioner mirroring Spark's production routing (`Utils.nonNegativeMod`). */
  private def newPartitioner(numParts: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  /**
   * Build a mocked `ShuffleDependency[Int, Array[Byte], Array[Byte]]` stubbed with exactly the
   * members the streaming writer reads: the serializer and partitioner, plus an empty aggregator
   * and key ordering. Mirrors the established pattern in `StreamingShuffleWriterSuite`.
   */
  private def newDependency(
      numParts: Int,
      serializer: JavaSerializer): ShuffleDependency[Int, Array[Byte], Array[Byte]] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Array[Byte], Array[Byte]]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.partitioner).thenReturn(newPartitioner(numParts))
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    dependency
  }

  /** Build a [[StreamingShuffleHandle]] carrying the three per-shuffle tuning values. */
  private def newStreamingHandle(
      numParts: Int,
      bufferSizePercent: Int,
      spillThreshold: Int,
      serializer: JavaSerializer): StreamingShuffleHandle[Int, Array[Byte], Array[Byte]] = {
    new StreamingShuffleHandle[Int, Array[Byte], Array[Byte]](
      shuffleId = 0,
      dependency = newDependency(numParts, serializer),
      bufferSizePercent = bufferSizePercent,
      spillThreshold = spillThreshold,
      maxBandwidthMBps = 0)
  }

  /** Resolve the active shuffle manager as a [[StreamingShuffleManager]], failing otherwise. */
  private def streamingManagerOf(context: SparkContext): StreamingShuffleManager = {
    context.env.shuffleManager match {
      case manager: StreamingShuffleManager => manager
      case other =>
        fail(s"expected a StreamingShuffleManager but got ${other.getClass.getName}")
    }
  }

  /** A finite stream of distinct 2 KiB byte-array records keyed by their index. */
  private def byteRecords(n: Int): Iterator[(Int, Array[Byte])] = {
    // Fill each value with INCOMPRESSIBLE random bytes. The writer wraps its output with the
    // default-on shuffle compression (LZ4), so a zero-filled or shared array would be compressed
    // to almost nothing and the per-partition buffer would never cross the spill threshold. A
    // deterministic seed keeps the run reproducible while still defeating the compressor.
    val rng = new scala.util.Random(0)
    (0 until n).iterator.map { i =>
      val value = new Array[Byte](RECORD_SIZE_BYTES)
      rng.nextBytes(value)
      (i, value)
    }
  }

  /**
   * Run one `reduceByKey` shuffle job and assert it produces [[expectedAggregate]]. When
   * `injectFailure` is true, the very first attempt of the map task for partition 0 throws once;
   * because the job runs on a `local[N, F]` master (F > 1) the task is retried and recomputed, so
   * the job still completes with the correct result -- demonstrating zero data loss under a
   * transient fault.
   *
   * @param context       the active streaming-manager-backed `SparkContext`
   * @param injectFailure whether to inject a single retried task failure into this job
   * @param iteration     the loop iteration index, used only for the failure message
   */
  private def runReduceByKeyJob(
      context: SparkContext,
      injectFailure: Boolean,
      iteration: Int): Unit = {
    val inject = injectFailure
    val result = context
      .parallelize(inputData, numShufflePartitions)
      .map { case (key, value) =>
        if (inject) {
          val tc = TaskContext.get()
          if (tc.partitionId() == 0 && tc.attemptNumber() == 0) {
            throw new RuntimeException(
              s"injected transient streaming-shuffle stress fault (iteration $iteration)")
          }
        }
        (key, value)
      }
      .reduceByKey(_ + _, numShufflePartitions)
      .collect()
      .toMap
    assert(
      result === expectedAggregate,
      s"iteration $iteration (injectFailure=$inject) produced an incorrect aggregate")
  }

  test("continuous streaming shuffle with 10% failure injection completes with correct results") {
    // The streaming data path is ACTIVE (spark.shuffle.manager=streaming and
    // spark.shuffle.streaming.enabled=true), so every reduceByKey in the sustained loop runs a
    // real streaming shuffle: the writer commits framed per-partition blocks and the reader
    // fetches and validates them. A local[4, 4] master permits the injected transient map-task
    // faults to be retried and the streaming write recomputed (a plain local[N] master forces
    // maxFailures=1 and would not retry).
    sc = new SparkContext(streamingConf("local[4, 4]", enabled = true))
    val manager = streamingManagerOf(sc)
    assert(
      manager.isStreamingActive,
      "both flags must activate the streaming data path for the active stress run")

    val deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(effectiveDurationMs)
    var iterations = 0
    var injectedFailures = 0
    do {
      val injectFailure = iterations % FAILURE_INJECTION_PERIOD == 0
      if (injectFailure) {
        injectedFailures += 1
      }
      runReduceByKeyJob(sc, injectFailure, iterations)
      iterations += 1
    } while (System.nanoTime() < deadlineNanos)

    assert(iterations > 0, "the stress loop must run at least one iteration")
    assert(
      injectedFailures > 0,
      "at least one iteration must inject a transient failure that recovers")
    logInfo(
      s"Streaming shuffle correctness stress completed $iterations iteration(s) " +
        s"($injectedFailures with injected, recovered failures) over $effectiveDurationMs ms")
  }

  test("no retained heap / buffer leak after the stress run") {
    // Both flags on: the streaming data path is ACTIVE. Executor memory is pinned small and
    // deterministic (mirroring StreamingShuffleWriterSuite) so per-partition budgets are tiny and
    // the spill path is cheap to exercise.
    sc = new SparkContext(
      streamingConf("local[4]", enabled = true)
        .set("spark.testing.memory", TESTING_MEMORY_BYTES.toString)
        .set("spark.testing.reservedMemory", "0"))
    val manager = streamingManagerOf(sc)
    assert(manager.isStreamingActive, "streaming data path must be active with both flags on")
    // In local mode the manager runs as the driver, so the executor-only spill manager is absent;
    // the no-leak assertions therefore rely on the writer's TaskMemoryManager accounting and the
    // manager's registered-shuffle bookkeeping, degrading gracefully where no direct buffer-count
    // accessor is exposed.
    assert(
      manager.memorySpillManager.isEmpty,
      "the executor-only spill manager must not be constructed on the driver (local mode)")

    val serializer = new JavaSerializer(sc.getConf)

    // Dispatch coverage: registerShuffle yields a StreamingShuffleHandle and getWriter yields a
    // StreamingShuffleWriter when streaming is enabled, and unregistering returns the bookkeeping
    // to its baseline.
    val dispatchDependency = newDependency(numWriterPartitions, serializer)
    val dispatchHandle = manager.registerShuffle(DISPATCH_SHUFFLE_ID, dispatchDependency)
    assert(
      dispatchHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      s"expected a StreamingShuffleHandle but got ${dispatchHandle.getClass.getName}")
    assert(manager.registeredStreamingShuffleCount === 1)
    val dispatchContext = MemoryTestingUtils.fakeTaskContext(sc.env)
    val dispatchWriter = manager.getWriter[Int, Array[Byte]](
      dispatchHandle, 0L, dispatchContext, dispatchContext.taskMetrics().shuffleWriteMetrics)
    assert(
      dispatchWriter.isInstanceOf[StreamingShuffleWriter[_, _, _]],
      s"expected a StreamingShuffleWriter but got ${dispatchWriter.getClass.getName}")
    dispatchWriter.write(byteRecords(64))
    assert(dispatchWriter.stop(success = true).isDefined)
    assert(dispatchContext.taskMemoryManager().getMemoryConsumptionForThisTask() === 0L)
    manager.unregisterShuffle(DISPATCH_SHUFFLE_ID)
    assert(
      manager.registeredStreamingShuffleCount === 0,
      "unregisterShuffle must return the streaming bookkeeping to its baseline")

    // No-leak stress: drive the streaming producer path through many allocate/write/spill/stop
    // cycles on a single reused task context. After every cycle the task must hold zero
    // bytes, so a leak would surface as monotonic growth in the tracked maximum.
    val normalHandle = newStreamingHandle(numWriterPartitions, 20, 80, serializer)
    val spillHandle = newStreamingHandle(numWriterPartitions, 1, 50, serializer)
    val config = new StreamingShuffleConfig(sc.getConf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(effectiveDurationMs)

    var cycles = 0
    var spillCycles = 0
    var maxTaskMemoryAfterStop = 0L
    do {
      val forceSpill = cycles % SPILL_INJECTION_PERIOD == 0
      val handle = if (forceSpill) spillHandle else normalHandle
      val writer = new StreamingShuffleWriter[Int, Array[Byte], Array[Byte]](
        handle, cycles.toLong, context,
        context.taskMetrics().shuffleWriteMetrics, config)
      val recordCount =
        if (forceSpill) 2 * StreamingShuffleWriter.SPILL_CHECK_RECORD_INTERVAL
        else NORMAL_CYCLE_RECORDS
      writer.write(byteRecords(recordCount))
      assert(writer.stop(success = true).isDefined)
      val held = context.taskMemoryManager().getMemoryConsumptionForThisTask()
      assert(
        held === 0L,
        s"cycle $cycles leaked $held bytes of task memory after the writer stopped")
      maxTaskMemoryAfterStop = math.max(maxTaskMemoryAfterStop, held)
      if (forceSpill) {
        assert(writer.numSpills > 0, s"forced-spill cycle $cycles did not spill")
        spillCycles += 1
      }
      cycles += 1
    } while (cycles < maxLeakCycles && System.nanoTime() < deadlineNanos)

    assert(cycles > 0, "the no-leak stress loop must run at least one cycle")
    assert(spillCycles > 0, "the no-leak stress must exercise the spill path at least once")

    // Best-effort: a GC must not reveal any retained streaming buffers, and the tracked
    // task-memory high-water mark must never have grown above zero.
    System.gc()
    assert(
      maxTaskMemoryAfterStop === 0L,
      s"task memory grew to $maxTaskMemoryAfterStop bytes across the stress run (buffer leak)")
    assert(
      context.taskMemoryManager().getMemoryConsumptionForThisTask() === 0L,
      "no streaming buffers may be retained after the stress run")
    assert(
      manager.registeredStreamingShuffleCount === 0,
      "no streaming shuffles may remain registered after the stress run")

    // Metrics counters must remain internally consistent (non-negative tallies, bounded gauge).
    val metrics = manager.streamingMetricsHolder
    assert(metrics.getSpillCount >= 0L)
    assert(metrics.getBackpressureEvents >= 0L)
    assert(metrics.getPartialReadInvalidations >= 0L)
    assert(metrics.getBufferUtilizationPercent >= 0 && metrics.getBufferUtilizationPercent <= 100)
    logInfo(
      s"Streaming shuffle no-leak stress completed $cycles cycle(s) ($spillCycles spill " +
        s"cycle(s)); task memory returned to zero after every cycle")
  }

  test("stress run stays within the SparkFunSuite per-test time budget") {
    // Sanity guard demonstrating the runtime gating: the effective sustained-run duration is
    // strictly below the SparkFunSuite per-test timeout, so neither stress loop can time out
    // regardless of how the duration property is overridden.
    assert(
      MAX_RUN_MS < SPARK_FUN_SUITE_TIMEOUT_MS,
      "the safety cap must be below the SparkFunSuite per-test timeout")
    assert(
      effectiveDurationMs < SPARK_FUN_SUITE_TIMEOUT_MS,
      s"effective duration $effectiveDurationMs ms must be below the " +
        s"$SPARK_FUN_SUITE_TIMEOUT_MS ms SparkFunSuite timeout")
    assert(
      effectiveDurationMs <= MAX_RUN_MS,
      "the effective duration must respect the safety cap")
  }
}

private object StreamingShuffleStressSuite {

  /** Inject a transient failure on every Nth iteration (roughly 10 percent). */
  private val FAILURE_INJECTION_PERIOD: Int = 10

  /** Force a spill on every Nth producer cycle in the no-leak stress loop. */
  private val SPILL_INJECTION_PERIOD: Int = 20

  /** Records written on a normal (non-spilling) producer cycle. */
  private val NORMAL_CYCLE_RECORDS: Int = 256

  /** Size of each byte-array record value, in bytes (2 KiB). */
  private val RECORD_SIZE_BYTES: Int = 2 * 1024

  /** Executor memory pinned for the producer no-leak stress, in bytes (64 MiB). */
  private val TESTING_MEMORY_BYTES: Long = 64L * 1024L * 1024L

  /** Shuffle id used for the one-shot dispatch-coverage assertion. */
  private val DISPATCH_SHUFFLE_ID: Int = 7

  /** Hard safety cap on a single stress loop (18 minutes), below the 20-minute test timeout. */
  private val MAX_RUN_MS: Long = TimeUnit.MINUTES.toMillis(18)

  /** The `SparkFunSuite` per-test timeout (20 minutes) that the gating must stay below. */
  private val SPARK_FUN_SUITE_TIMEOUT_MS: Long = TimeUnit.MINUTES.toMillis(20)
}
