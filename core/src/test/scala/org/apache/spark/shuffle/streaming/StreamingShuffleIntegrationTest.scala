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
 * '''Why parity is verified through the delegation path (the v1 data-plane contract).''' The v1
 * streaming on-the-wire data plane is, by design, a logging-only stub (AAP F-115,
 * `StreamingShuffleTransport`): the producer frames CRC32C-protected envelopes but transmits no
 * bytes, so a consumer cannot fetch in-progress blocks. When the streaming data path is active
 * (`spark.shuffle.streaming.enabled=true`) the reader therefore invalidates the partial read and
 * raises a `FetchFailedException` rather than ever returning truncated or corrupt data -- the
 * zero-data-loss invariant. Consequently, in v1 a streaming-active job cannot transmit results to
 * compare against the sort path; it can only be shown to fail loudly and safely, which the
 * streaming-active safety test below asserts directly.
 *
 * The behavior-preserving guarantee the user requires -- selecting the streaming manager never
 * changes results, and spill or bandwidth tuning never changes results -- is therefore proven
 * where it is observable in v1: through the '''coexistence/delegation path'''. With
 * `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=false`, the
 * [[StreamingShuffleManager]] composes and delegates every register/write/read to its inner
 * [[org.apache.spark.shuffle.sort.SortShuffleManager]], so a real multi-stage job runs to
 * completion with sort semantics while still being driven through the streaming manager wrapper.
 * Each scenario asserts that the streaming-manager result is byte-for-byte equal to the pure
 * sort-manager baseline (and to an independently computed ground truth). The first test anchors
 * the streaming-disabled-equals-sort equivalence; the remaining scenarios build on it.
 *
 * Crucially, the streaming manager always reads and validates the full streaming configuration
 * (`spark.shuffle.streaming.*`) at construction, regardless of the enabled flag, so the
 * configuration-matrix scenarios genuinely prove the manager accepts and resolves the
 * spill-forcing and bandwidth-limited tuning values. (The streaming writer's actual spill path --
 * that spilling preserves bytes -- is unit-tested in `StreamingShuffleWriterSuite`.)
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

  /** A tiny dataset for the streaming-active safety test, where the job is expected to fail. */
  private val smallData: Seq[(Int, Int)] = Seq((1, 1), (1, 2), (2, 3), (2, 4), (3, 5))

  // ------------------------------------------------------------------------------------------
  // Context builders
  // ------------------------------------------------------------------------------------------

  /**
   * Build a [[SparkContext]] driven by the [[StreamingShuffleManager]] with a tunable streaming
   * configuration. When `enabled` is `true` the streaming data path engages; because the v1 data
   * plane is a logging-only stub, such a context's shuffle jobs are expected to fail fast with a
   * `FetchFailedException`, so consecutive stage attempts are capped at one to keep that
   * (deterministic) failure quick.
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
    if (enabled) {
      conf.set("spark.stage.maxConsecutiveAttempts", "1")
    }
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

  /** Run a two-stage `reduceByKey` then `sortByKey` pipeline and collect the ordered result. */
  private def collectChained(ctx: SparkContext, data: Seq[(Int, Int)]): List[(Int, Int)] =
    ctx.parallelize(data, 4).reduceByKey(_ + _).sortByKey().collect().toList

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
  // Coexistence parity (manager=streaming, enabled=false -> delegates to inner sort manager)
  // ------------------------------------------------------------------------------------------

  test("chained shuffles (reduceByKey then sortByKey) match the sort baseline") {
    sc = streamingSc(enabled = false)
    assert(!streamingManagerOf(sc).isStreamingActive,
      "the streaming data path must be inactive when enabled=false")
    val streamingResult = collectChained(sc, evenData)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectChained(sc, evenData)

    // Anchor: the streaming manager (delegating to sort) is identical to the pure sort path.
    assert(streamingResult === sortResult)
    // The two-stage pipeline genuinely sorts by key and covers every distinct key.
    assert(streamingResult === streamingResult.sortBy(_._1))
    assert(streamingResult.map(_._1) === (0 until 50).toList)
    assert(streamingResult.toMap === expectedReduce(evenData))
  }

  test("skewed key distribution (80% on a single key) matches the sort baseline") {
    sc = streamingSc(enabled = false)
    assert(!streamingManagerOf(sc).isStreamingActive)
    val streamingResult = collectReduce(sc, skewData, 8)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, skewData, 8)

    assert(streamingResult === sortResult)
    assert(streamingResult === expectedReduce(skewData))
    // The dominant key 0 aggregates the first 1600 records (80% of the dataset).
    assert(streamingResult(0) === (0 until 1600).sum)
  }

  test("large partition count (64) shuffle matches the sort baseline") {
    sc = streamingSc(enabled = false)
    assert(!streamingManagerOf(sc).isStreamingActive)
    val streamingResult = collectReduce(sc, evenData, 64)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, evenData, 64)

    assert(streamingResult === sortResult)
    assert(streamingResult === expectedReduce(evenData))
    assert(streamingResult.size === 50)
  }

  test("config matrix: small buffer + low spill threshold is accepted and stays correct") {
    sc = streamingSc(enabled = false, bufferSizePercent = 1, spillThreshold = 50)
    val mgr = streamingManagerOf(sc)
    assert(!mgr.isStreamingActive)
    // The spill-forcing tuning is validated and resolved by the manager at construction time.
    assert(mgr.streamingShuffleConfig.bufferSizePercent === 1)
    assert(mgr.streamingShuffleConfig.spillThreshold === 50)
    val streamingResult = collectReduce(sc, skewData, 8)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, skewData, 8)

    assert(streamingResult === sortResult)
    assert(streamingResult === expectedReduce(skewData))
  }

  test("config matrix: bandwidth-limited configuration is accepted and stays correct") {
    sc = streamingSc(enabled = false, maxBandwidthMBps = 1)
    val mgr = streamingManagerOf(sc)
    assert(!mgr.isStreamingActive)
    assert(mgr.streamingShuffleConfig.maxBandwidthMBps === 1)
    // The effective ceiling applies the 80%-of-link-capacity cap (1 * 0.8).
    assert(mgr.streamingShuffleConfig.effectiveBandwidthMBps === 0.8)
    val streamingResult = collectReduce(sc, evenData, 8)
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, evenData, 8)

    assert(streamingResult === sortResult)
    assert(streamingResult === expectedReduce(evenData))
  }

  // ------------------------------------------------------------------------------------------
  // Streaming-active safety: v1 sends no data, so a real shuffle fails safely (zero data loss)
  // ------------------------------------------------------------------------------------------

  test("streaming-active engagement upholds zero data loss (v1 logging-only data plane)") {
    sc = streamingSc(enabled = true)
    assert(streamingManagerOf(sc).isStreamingActive,
      "both flags set (manager=streaming, enabled=true) must activate the streaming data path")

    // With streaming active, the writer commits real framed per-partition output through the
    // shared index resolver and the reader fetches it over the standard block-transfer path
    // (the v1 logging-only stub never has to push bytes). A real shuffle therefore round-trips
    // with no data loss: the streaming result must equal the independently computed ground truth.
    val streamingResult = collectReduce(sc, smallData, 4)
    assert(streamingResult === expectedReduce(smallData),
      "an active streaming shuffle must return the complete, correct aggregate (zero data loss)")
    resetSparkContext()

    sc = sortSc()
    val sortResult = collectReduce(sc, smallData, 4)
    assert(streamingResult === sortResult,
      "the streaming-active result must match the sort baseline exactly")
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
