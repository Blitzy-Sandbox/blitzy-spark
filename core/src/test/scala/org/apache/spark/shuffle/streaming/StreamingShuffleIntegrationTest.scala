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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

/**
 * Broad, end-to-end integration scenario harness for the opt-in streaming shuffle subsystem
 * (feature F-121, test #11 of 14). This suite is the scenario-breadth companion to
 * `StreamingShuffleIntegrationSuite` (#10): where that suite proves the basic activation and
 * parity baseline, this one exercises larger and skewed datasets, multiple chained shuffles, a
 * configuration-matrix sweep (buffer size, spill threshold, bandwidth), large partition counts,
 * and fail-fast configuration validation, all over a live [[SparkContext]].
 *
 * '''How parity is verified (active dual-flag streaming vs. sort).''' The behavior-preserving
 * guarantee the user requires -- selecting the streaming manager never changes results, and spill
 * or bandwidth tuning never changes results -- is proven by running each scenario through the
 * '''active''' streaming data path (`spark.shuffle.manager=streaming` and
 * `spark.shuffle.streaming.enabled=true`) and asserting the result is byte-for-byte equal to both
 * a pure sort-manager baseline and an independently computed ground truth. With the streaming
 * data path active, the [[StreamingShuffleWriter]] frames each per-partition output as
 * CRC32C-protected block envelopes and commits them through the shared
 * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] at map completion; the
 * [[StreamingShuffleReader]] then fetches those committed blocks over the standard block-transfer
 * path, validates the CRC32C, and runs the same deserialize/aggregate/sort tail as the sort
 * reader. A real multi-stage job therefore round-trips its full results with zero data loss.
 *
 * '''v1 data-plane note (AAP F-115).''' The v1 streaming on-the-wire transport
 * (`StreamingShuffleTransport`) is, by design, a logging-only stub: there is no in-flight Netty
 * '''push''' of in-progress blocks from producers to consumers during a map task. This is the
 * single documented, intentional v1 deviation (AAP 0.9.1) -- it is the absence of the mid-task
 * push transport, '''not''' an inability to move data: data parity is achieved through the
 * commit-then-fetch path described above. A separate coexistence anchor below also confirms that
 * with `spark.shuffle.streaming.enabled=false` the [[StreamingShuffleManager]] delegates every
 * register/write/read to its inner [[org.apache.spark.shuffle.sort.SortShuffleManager]].
 *
 * The configuration-matrix scenarios run with the streaming data path active and spill-forcing or
 * bandwidth-limited tuning, so they genuinely exercise the streaming writer's spill path (the
 * writer publishes spilled segments ahead of resident bytes, preserving every record) and prove
 * the manager accepts and resolves those tuning values while still producing sort-identical
 * output.
 *
 * All datasets are small and deterministic so the suite is fast and exhibits zero flakiness
 * within the [[SparkFunSuite]] timeout. Because Spark permits only one live [[SparkContext]] per
 * JVM, the parity scenarios run the streaming context first, stop it via [[resetSparkContext]],
 * then run the sort baseline; the inherited `sc` is always the most recently created context, so
 * [[LocalSparkContext]] tears it down after each test.
 */
class StreamingShuffleIntegrationTest extends SparkFunSuite with LocalSparkContext {

  // ------------------------------------------------------------------------------------------
  // Deterministic fixtures
  // ------------------------------------------------------------------------------------------

  /** Evenly distributed key/value pairs: 1000 records spread across 50 distinct keys. */
  private val evenData: Seq[(Int, Int)] = (0 until 1000).map(i => (i % 50, i))

  /**
   * Heavily skewed key/value pairs: 80% of the 2000 records share the single dominant key `0`,
   * with the remaining 20% spread across keys `1..199`. Exercises per-partition buffer sizing and
   * the spill decision when the streaming data path is active, and a hot reduce partition on the
   * sort/delegated path.
   */
  private val skewData: Seq[(Int, Int)] =
    (0 until 2000).map(i => if (i < 1600) (0, i) else ((i % 199) + 1, i))

  // ------------------------------------------------------------------------------------------
  // Context builders
  // ------------------------------------------------------------------------------------------

  /**
   * Build a [[SparkContext]] driven by the [[StreamingShuffleManager]] with a tunable streaming
   * configuration. When `enabled` is `true` the streaming data path engages and real shuffle jobs
   * run to completion with sort-identical output (the writer commits framed per-partition blocks
   * that the reader fetches and validates); when `false` the manager delegates to its inner sort
   * manager. The streaming configuration is validated at manager construction in both modes.
   */
  private def streamingSc(
      enabled: Boolean,
      bufferSizePercent: Int = 20,
      spillThreshold: Int = 80,
      maxBandwidthMBps: Int = 0): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("streaming-shuffle-integration-test")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", enabled.toString)
      .set("spark.shuffle.streaming.bufferSizePercent", bufferSizePercent.toString)
      .set("spark.shuffle.streaming.spillThreshold", spillThreshold.toString)
      .set("spark.shuffle.streaming.maxBandwidthMBps", maxBandwidthMBps.toString)
    new SparkContext(conf)
  }

  /** Build the pure sort-based baseline [[SparkContext]] used for parity comparisons. */
  private def sortSc(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("streaming-shuffle-integration-sort-baseline")
      .set("spark.shuffle.manager", "sort")
    new SparkContext(conf)
  }

  // ------------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------------

  /** Resolve the active shuffle manager as a [[StreamingShuffleManager]] or fail the test. */
  private def streamingManagerOf(ctx: SparkContext): StreamingShuffleManager = {
    ctx.env.shuffleManager match {
      case m: StreamingShuffleManager => m
      case other => fail(s"expected a StreamingShuffleManager but got ${other.getClass.getName}")
    }
  }

  /** Run `reduceByKey(_ + _, numParts)` over `data` and collect the result as a map. */
  private def collectReduce(
      ctx: SparkContext,
      data: Seq[(Int, Int)],
      numParts: Int): Map[Int, Int] =
    ctx.parallelize(data, 4).reduceByKey(_ + _, numParts).collect().toMap

  /**
   * Run a two-stage `reduceByKey` then `sortByKey` pipeline and collect the ordered result. Both
   * shuffle stages use `numParts` partitions so the pipeline runs at the shuffle-heavy partition
   * profile (>= 10) the AAP targets.
   */
  private def collectChained(
      ctx: SparkContext,
      data: Seq[(Int, Int)],
      numParts: Int): List[(Int, Int)] =
    ctx.parallelize(data, 4).reduceByKey(_ + _, numParts).sortByKey(numPartitions = numParts)
      .collect().toList

  /** Independently compute the expected `reduceByKey` ground truth in plain Scala. */
  private def expectedReduce(data: Seq[(Int, Int)]): Map[Int, Int] =
    data.groupBy(_._1).map { case (k, kvs) => (k, kvs.map(_._2).sum) }

  /** The chain of an exception and all of its causes (bounded to guard against cycles). */
  private def causeChain(t: Throwable): List[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(20).toList

  /** Flatten an exception cause chain into one searchable string of class names and messages. */
  private def fullText(t: Throwable): String =
    causeChain(t).map(e => s"${e.getClass.getName}: ${e.getMessage}").mkString(" | ")

  // ------------------------------------------------------------------------------------------
  // Coexistence anchor (manager=streaming, enabled=false -> delegates to inner sort manager)
  // ------------------------------------------------------------------------------------------

  test("disabled streaming delegates to sort: chained shuffles match the sort baseline") {
    sc = streamingSc(enabled = false)
    assert(!streamingManagerOf(sc).isStreamingActive,
      "the streaming data path must be inactive when enabled=false")
    val streamingResult = collectChained(sc, evenData, 16)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectChained(sc, evenData, 16)

    // Anchor: the streaming manager (delegating to sort) is identical to the pure sort path.
    assert(streamingResult === sortResult)
    // The two-stage pipeline genuinely sorts by key and covers every distinct key.
    assert(streamingResult === streamingResult.sortBy(_._1))
    assert(streamingResult.map(_._1) === (0 until 50).toList)
    assert(streamingResult.toMap === expectedReduce(evenData))
  }

  // ------------------------------------------------------------------------------------------
  // Active dual-flag parity (manager=streaming, enabled=true): real streaming shuffles at the
  // shuffle-heavy partition profile (>= 10) produce output identical to the sort baseline across
  // multiple shuffle shapes (reduceByKey and the chained reduceByKey -> sortByKey pipeline).
  // ------------------------------------------------------------------------------------------

  test("active streaming: skewed reduceByKey (16 partitions) matches the sort baseline") {
    sc = streamingSc(enabled = true)
    assert(streamingManagerOf(sc).isStreamingActive,
      "both flags set (manager=streaming, enabled=true) must activate the streaming data path")
    val streamingResult = collectReduce(sc, skewData, 16)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, skewData, 16)

    assert(streamingResult === sortResult,
      "active streaming reduceByKey must equal the sort baseline exactly (zero data loss)")
    assert(streamingResult === expectedReduce(skewData))
    // The dominant key 0 aggregates the first 1600 records (80% of the dataset).
    assert(streamingResult(0) === (0 until 1600).sum)
  }

  test("active streaming: chained reduceByKey then sortByKey (16 partitions) matches sort") {
    sc = streamingSc(enabled = true)
    assert(streamingManagerOf(sc).isStreamingActive)
    val streamingResult = collectChained(sc, evenData, 16)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectChained(sc, evenData, 16)

    assert(streamingResult === sortResult,
      "active streaming chained shuffles must equal the sort baseline exactly")
    // The two-stage pipeline genuinely sorts by key and covers every distinct key.
    assert(streamingResult === streamingResult.sortBy(_._1))
    assert(streamingResult.map(_._1) === (0 until 50).toList)
    assert(streamingResult.toMap === expectedReduce(evenData))
  }

  test("active streaming: large partition count (64) matches the sort baseline") {
    sc = streamingSc(enabled = true)
    assert(streamingManagerOf(sc).isStreamingActive)
    val streamingResult = collectReduce(sc, evenData, 64)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, evenData, 64)

    assert(streamingResult === sortResult,
      "active streaming at a high partition count must equal the sort baseline exactly")
    assert(streamingResult === expectedReduce(evenData))
    assert(streamingResult.size === 50)
  }

  test("active streaming: small buffer + low spill threshold stays correct (16 partitions)") {
    sc = streamingSc(enabled = true, bufferSizePercent = 1, spillThreshold = 50)
    val mgr = streamingManagerOf(sc)
    assert(mgr.isStreamingActive)
    // The spill-forcing tuning is validated and resolved by the manager at construction time.
    assert(mgr.streamingShuffleConfig.bufferSizePercent === 1)
    assert(mgr.streamingShuffleConfig.spillThreshold === 50)
    val streamingResult = collectReduce(sc, skewData, 16)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, skewData, 16)

    // Active streaming with a tiny buffer exercises the writer's spill path, which publishes
    // spilled segments ahead of resident bytes so every record survives; output is unchanged.
    assert(streamingResult === sortResult)
    assert(streamingResult === expectedReduce(skewData))
  }

  test("active streaming: bandwidth-limited configuration stays correct (16 partitions)") {
    sc = streamingSc(enabled = true, maxBandwidthMBps = 1)
    val mgr = streamingManagerOf(sc)
    assert(mgr.isStreamingActive)
    assert(mgr.streamingShuffleConfig.maxBandwidthMBps === 1)
    // The effective ceiling applies the 80%-of-link-capacity cap (1 * 0.8).
    assert(mgr.streamingShuffleConfig.effectiveBandwidthMBps === 0.8)
    val streamingResult = collectReduce(sc, evenData, 16)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, evenData, 16)

    assert(streamingResult === sortResult)
    assert(streamingResult === expectedReduce(evenData))
  }

  // ------------------------------------------------------------------------------------------
  // Configuration validation: out-of-range tuning is rejected fast at manager construction
  // ------------------------------------------------------------------------------------------

  test("invalid bufferSizePercent is rejected at startup") {
    val ex = intercept[Exception] {
      sc = streamingSc(enabled = true, bufferSizePercent = 99)
    }
    assert(
      causeChain(ex).exists(_.isInstanceOf[IllegalArgumentException]),
      s"expected an IllegalArgumentException in the cause chain, but was: ${fullText(ex)}")
    assert(fullText(ex).contains("bufferSizePercent"), fullText(ex))
  }

  test("invalid spillThreshold is rejected at startup") {
    val ex = intercept[Exception] {
      sc = streamingSc(enabled = true, spillThreshold = 10)
    }
    assert(
      causeChain(ex).exists(_.isInstanceOf[IllegalArgumentException]),
      s"expected an IllegalArgumentException in the cause chain, but was: ${fullText(ex)}")
    assert(fullText(ex).contains("spillThreshold"), fullText(ex))
  }
}
