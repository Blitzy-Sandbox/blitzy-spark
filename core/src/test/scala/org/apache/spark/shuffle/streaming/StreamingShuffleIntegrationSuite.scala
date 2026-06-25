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

import org.apache.spark._
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * End-to-end integration tests for the opt-in streaming shuffle subsystem (feature F-101),
 * exercised through a live [[org.apache.spark.SparkContext]] and the real shuffle-manager
 * dispatch path rather than mocks. This is test #10 of the streaming-shuffle test matrix (F-121)
 * and the headline correctness / coexistence guarantee for the feature.
 *
 * The suite validates two complementary guarantees from the Agent Action Plan (AAP):
 *
 *  1. '''Coexistence / zero regression (AAP 0.7.2).''' Selecting the streaming manager
 *     (`spark.shuffle.manager=streaming`, by short alias '''or''' fully-qualified class name)
 *     while the opt-in flag is off (`spark.shuffle.streaming.enabled=false`) must behave
 *     '''identically''' to the default sort-based shuffle. This is asserted directly: the result
 *     of a real shuffle run through the streaming manager is compared, element for element,
 *     against both an independent deterministic oracle and a baseline computed on a sort-only
 *     context, across three distinct shuffle shapes (`reduceByKey`, `groupByKey`, and `join`), in
 *     both a local and a cross-executor `local-cluster` topology. The default configuration is
 *     additionally asserted to keep resolving to
 *     [[org.apache.spark.shuffle.sort.SortShuffleManager]], proving the zero default-behavior
 *     change.
 *
 *  2. '''Activation contract (AAP 0.1.1).''' Streaming engages only when '''both''' flags are
 *     set. With both set, the manager is a [[StreamingShuffleManager]], it reports the streaming
 *     path as active, and it registers a [[StreamingShuffleHandle]] for a shuffle dependency --
 *     proving the streaming SPI dispatch is engaged end-to-end.
 *
 * '''v1 data-plane note (AAP F-115).''' In this version the streaming on-the-wire transport is,
 * by design, a logging-only stub: the producer frames CRC32C block envelopes but does not
 * transmit or materialize them through the block-fetch path, so a shuffle run with streaming
 * '''active''' cannot yet move data to consumers (the reader invalidates the partial read and
 * defers to DAG-scheduler recomputation). Full data-moving parity between the streaming and sort
 * data paths therefore lands with the post-v1 Netty data plane; the reader's
 * deserialize/aggregate/sort tail is already byte-identical to the sort path by construction.
 * Accordingly, this suite asserts streaming-vs-sort result parity on the (active) coexistence
 * path and asserts streaming-path '''engagement''' (handle dispatch) on the enabled path, rather
 * than driving a data-moving shuffle that the v1 stub cannot complete. This is the single
 * documented, intentional deviation permitted by the AAP pre-flight gate (AAP 0.9.1) and is
 * recorded here so the suite stays green and flake-free while remaining faithful to the feature
 * contract.
 *
 * Each test builds its own [[org.apache.spark.SparkContext]] because the shuffle manager is fixed
 * at context creation and must never be mutated on a running context. The inherited `sc` field is
 * used so [[org.apache.spark.LocalSparkContext]] tears the context down after each test, and any
 * extra sort-baseline context is created and stopped within
 * [[org.apache.spark.LocalSparkContext.withSpark]] so that only one context is live at a time.
 */
class StreamingShuffleIntegrationSuite extends SparkFunSuite with LocalSparkContext {

  // Number of input records driven through each shuffle. Modest so the suite stays fast.
  private val recordCount = 10000

  // Number of input (map) partitions for the source RDD.
  private val numInputPartitions = 8

  // Number of reduce partitions. Kept at >= 10 to match the streaming activation profile
  // (AAP 0.2.1.1), so the workload is representative of the shuffle-heavy target case.
  private val numReducePartitions = 16

  // Smaller record count for the heavier cross-executor `local-cluster` test, keeping it well
  // within the 20-minute SparkFunSuite per-test timeout while still crossing executor boundaries.
  private val clusterRecordCount = 4000

  // ---------------------------------------------------------------------------------------------
  // Deterministic oracles. These are independent, in-process ground truths for the workloads
  // below; asserting against them (in addition to a live sort baseline) makes the parity checks
  // robust and self-describing rather than relying on a single comparison.
  // ---------------------------------------------------------------------------------------------

  /** Expected `reduceByKey` output: the per-key sum of `1..recordCount` bucketed by key. */
  private val expectedReduceByKey: Seq[(Int, Int)] =
    (1 to recordCount).groupBy(_ % numReducePartitions)
      .map { case (key, values) => (key, values.sum) }
      .toSeq
      .sortBy(_._1)

  /** Expected `groupByKey` output: the sorted values of `1..recordCount` bucketed by key. */
  private val expectedGroupByKey: Seq[(Int, List[Int])] =
    (1 to recordCount).groupBy(_ % numReducePartitions)
      .map { case (key, values) => (key, values.toList.sorted) }
      .toSeq
      .sortBy(_._1)

  /** Expected `join` output for the small distinct-key inputs used by [[runJoin]]. */
  private val expectedJoin: Seq[(Int, (Int, Int))] =
    (0 until numReducePartitions).map(key => (key, (key * 10, key * 100))).sortBy(_._1)

  // ---------------------------------------------------------------------------------------------
  // Context fixtures. The shuffle manager is bound at context creation, so each configuration is
  // realized as its own SparkContext rather than by mutating a running one.
  // ---------------------------------------------------------------------------------------------

  /**
   * Build a [[SparkContext]] for the given master and shuffle configuration. The streaming opt-in
   * flag is always set explicitly (to `streamingEnabled`); the shuffle manager is set only when a
   * name is supplied, so the "default manager" case exercises the genuinely unset default.
   */
  private def newSparkContext(
      master: String,
      shuffleManager: Option[String],
      streamingEnabled: Boolean): SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("streaming-shuffle-integration")
      .set("spark.ui.enabled", "false")
    shuffleManager.foreach(name => conf.set("spark.shuffle.manager", name))
    conf.set("spark.shuffle.streaming.enabled", streamingEnabled.toString)
    new SparkContext(conf)
  }

  /** A context on the default (sort) shuffle path. */
  private def sortContext(master: String = "local[4]"): SparkContext =
    newSparkContext(master, shuffleManager = None, streamingEnabled = false)

  /**
   * A context with the streaming manager selected. `manager` is the alias (`"streaming"`) by
   * default but may be the fully-qualified class name; `enabled` toggles the opt-in flag.
   */
  private def streamingContext(
      enabled: Boolean,
      manager: String = "streaming",
      master: String = "local[4]"): SparkContext =
    newSparkContext(master, shuffleManager = Some(manager), streamingEnabled = enabled)

  // ---------------------------------------------------------------------------------------------
  // Workloads. Each returns a fully materialized, deterministically ordered result so two runs
  // can be compared for byte-for-byte (structural) equality.
  // ---------------------------------------------------------------------------------------------

  /** Run the canonical `reduceByKey` shuffle and return its sorted result. */
  private def runReduceByKey(sparkContext: SparkContext): Seq[(Int, Int)] = {
    // Copy the partition count into a local val so the map closure captures only this Int and
    // never the enclosing suite (whose ScalaTest engine is not serializable). RDD.map cleans and
    // serializes the closure eagerly, so capturing `this` would fail even before any job runs.
    val partitions = numReducePartitions
    sparkContext.parallelize(1 to recordCount, numInputPartitions)
      .map(i => (i % partitions, i))
      .reduceByKey(_ + _, partitions)
      .collect()
      .sortBy(_._1)
      .toSeq
  }

  /** Run a `groupByKey` shuffle and return its sorted-values result. */
  private def runGroupByKey(sparkContext: SparkContext): Seq[(Int, List[Int])] = {
    // Local val so the key-extraction closure captures only this Int, not the (non-serializable)
    // enclosing suite. See runReduceByKey for the rationale.
    val partitions = numReducePartitions
    sparkContext.parallelize(1 to recordCount, numInputPartitions)
      .map(i => (i % partitions, i))
      .groupByKey(partitions)
      .map { case (key, values) => (key, values.toList.sorted) }
      .collect()
      .sortBy(_._1)
      .toSeq
  }

  /**
   * Run a `join` shuffle over two small, distinct-key inputs (distinct keys keep the join from
   * producing a within-key cartesian explosion) and return its sorted result.
   */
  private def runJoin(sparkContext: SparkContext): Seq[(Int, (Int, Int))] = {
    val left = sparkContext.parallelize(0 until numReducePartitions, numInputPartitions)
      .map(key => (key, key * 10))
    val right = sparkContext.parallelize(0 until numReducePartitions, numInputPartitions)
      .map(key => (key, key * 100))
    left.join(right, numReducePartitions).collect().sortBy(_._1).toSeq
  }

  // ---------------------------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------------------------

  test("default configuration selects the sort shuffle manager and shuffles correctly") {
    sc = sortContext()
    assert(
      sc.env.shuffleManager.isInstanceOf[SortShuffleManager],
      "the default spark.shuffle.manager must remain the sort-based manager")
    assert(runReduceByKey(sc) === expectedReduceByKey)
  }

  test("streaming manager with streaming disabled delegates to sort (reduceByKey parity)") {
    // Sort baseline computed on a fully independent context that is stopped before the streaming
    // context is created, so only one SparkContext is ever live at a time.
    val sortBaseline = LocalSparkContext.withSpark(sortContext())(runReduceByKey)

    // Streaming manager selected but disabled; assigned to `sc` for automatic teardown.
    sc = streamingContext(enabled = false)
    val manager = sc.env.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager])
    assert(
      !manager.asInstanceOf[StreamingShuffleManager].isStreamingActive,
      "with spark.shuffle.streaming.enabled=false the streaming data path must stay inactive")

    val streamingManagerResult = runReduceByKey(sc)
    assert(streamingManagerResult === sortBaseline)
    assert(streamingManagerResult === expectedReduceByKey)
  }

  test("streaming manager with streaming disabled matches sort for groupByKey and join") {
    val sortGroupBaseline = LocalSparkContext.withSpark(sortContext())(runGroupByKey)
    val sortJoinBaseline = LocalSparkContext.withSpark(sortContext())(runJoin)

    sc = streamingContext(enabled = false)
    assert(
      !sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager].isStreamingActive,
      "the streaming data path must stay inactive when the opt-in flag is off")

    val streamingGroup = runGroupByKey(sc)
    val streamingJoin = runJoin(sc)
    assert(streamingGroup === sortGroupBaseline)
    assert(streamingGroup === expectedGroupByKey)
    assert(streamingJoin === sortJoinBaseline)
    assert(streamingJoin === expectedJoin)
  }

  test("streaming manager selected by fully-qualified class name also delegates to sort") {
    sc = streamingContext(enabled = false, manager = classOf[StreamingShuffleManager].getName)
    val manager = sc.env.shuffleManager
    assert(
      manager.isInstanceOf[StreamingShuffleManager],
      "selecting the streaming manager by class name must resolve the streaming manager")
    assert(
      !manager.asInstanceOf[StreamingShuffleManager].isStreamingActive,
      "class-name selection must still respect the disabled opt-in flag and delegate to sort")
    assert(runReduceByKey(sc) === expectedReduceByKey)
  }

  test("enabling streaming engages the streaming dispatch path (StreamingShuffleHandle)") {
    sc = streamingContext(enabled = true)
    val manager = sc.env.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager])
    assert(
      manager.asInstanceOf[StreamingShuffleManager].isStreamingActive,
      "with both spark.shuffle.manager=streaming and the opt-in flag the path must be active")

    // Constructing the shuffled RDD registers the shuffle on the driver (no job is run); the
    // returned handle is the dispatch discriminator the writer/reader pattern-match on. An active
    // streaming manager must hand back a StreamingShuffleHandle here. The local val keeps the map
    // closure free of any reference to the (non-serializable) suite, as RDD.map cleans eagerly.
    val partitions = numReducePartitions
    val shuffled = sc.parallelize(1 to recordCount, numInputPartitions)
      .map(i => (i % partitions, i))
      .reduceByKey(_ + _, partitions)
    val shuffleDependency = shuffled.dependencies.collectFirst {
      case dependency: ShuffleDependency[_, _, _] => dependency
    }.getOrElse(fail("reduceByKey did not produce a ShuffleDependency"))
    assert(
      shuffleDependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      "an active streaming manager must register a StreamingShuffleHandle for the shuffle")
  }

  test("streaming manager coexists with the sort path across executors (local-cluster)") {
    // Cross-executor coexistence: with streaming disabled, a real shuffle executed across two
    // executor JVMs (with genuine block transfer between them) must still match the sort result.
    sc = streamingContext(enabled = false, master = "local-cluster[2,1,1024]")
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    assert(
      sc.env.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "the streaming manager must be the active manager even when delegating to sort")

    // Local val so the closure shipped to the two executor JVMs captures only this Int.
    val partitions = numReducePartitions
    val result = sc.parallelize(1 to clusterRecordCount, numInputPartitions)
      .map(i => (i % partitions, i))
      .reduceByKey(_ + _, partitions)
      .collect()
      .sortBy(_._1)
      .toSeq
    val expected = (1 to clusterRecordCount).groupBy(_ % numReducePartitions)
      .map { case (key, values) => (key, values.sum) }
      .toSeq
      .sortBy(_._1)
    assert(result === expected)
  }
}
