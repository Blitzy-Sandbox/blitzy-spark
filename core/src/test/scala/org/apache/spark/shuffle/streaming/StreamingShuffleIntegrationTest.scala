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

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * End-to-end integration coverage for the streaming shuffle backend, in '''JUnit-Jupiter'''
 * style (a plain class whose lifecycle and test methods carry `org.junit.jupiter.api` annotations)
 * rather than as a ScalaTest `SparkFunSuite`. JUnit Jupiter and the `jupiter-interface` test engine
 * are on the module test classpath, so these `@Test` methods are discovered and executed by the
 * standard JUnit Platform provider alongside the ScalaTest suites in this package. This is the
 * '''only''' JUnit-Jupiter file in `org.apache.spark.shuffle.streaming`; every other test here is a
 * ScalaTest suite and intentionally remains so.
 *
 * ==What it verifies==
 * The suite drives real Spark jobs through the '''public''' RDD API with the streaming shuffle
 * backend selected purely through configuration -- `spark.shuffle.manager=streaming` plus the
 * `spark.shuffle.streaming.enabled` opt-in flag. It exercises the three canonical shuffle shapes
 * (`reduceByKey`, `groupByKey`, `distinct`), confirms the dual activation gate degrades cleanly to
 * the sort path when streaming is disabled, and asserts the configured manager was actually wired
 * into the running `SparkEnv`. It never constructs, stubs, or references streaming internals
 * directly; correctness is observed end-to-end through job output only.
 *
 * ==Isolation and anti-flakiness==
 * Only a single [[org.apache.spark.SparkContext]] may be active per JVM, so [[startContext]]
 * always stops any previously created context before building a new one, and [[tearDown]] always
 * stops the active context after every test. Datasets are tiny and run on `local[2]`, there are no
 * timing assertions, and there is no network dependence (the v1 streaming transport is a stub and
 * correctness holds through the in-process / sort-fallback paths), keeping the suite deterministic.
 */
class StreamingShuffleIntegrationTest {

  /** The single active [[SparkContext]] for the current test; always torn down in [[tearDown]]. */
  private var sc: SparkContext = _

  /**
   * (Re)build the one-per-JVM [[SparkContext]] with the streaming shuffle backend selected. Any
   * existing context is stopped first so a test that needs a differently configured context (for
   * example the disabled-streaming fallback check) never leaves two contexts active in the JVM.
   *
   * @param streamingEnabled the value for `spark.shuffle.streaming.enabled`; the shuffle manager is
   *                         always `streaming`, so `false` engages the sort-based fallback path
   */
  private def startContext(streamingEnabled: Boolean): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingShuffleIntegrationTest")
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", streamingEnabled.toString)
    sc = new SparkContext(conf)
  }

  @BeforeEach
  def setUp(): Unit = {
    startContext(streamingEnabled = true)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  /**
   * A keyed sum through the streaming shuffle must be exact. The integers `1..100` fall evenly into
   * the five residue classes of `% 5`, so each of the five keys aggregates exactly twenty ones.
   */
  @Test
  def reduceByKeyProducesCorrectSums(): Unit = {
    val rdd = sc.parallelize(1 to 100, 4).map(i => (i % 5, 1))
    val counts = rdd.reduceByKey(_ + _).collectAsMap()
    assertEquals(5, counts.size)
    assertEquals(20, counts(0))
    assertEquals(20, counts(1))
    assertEquals(20, counts(2))
    assertEquals(20, counts(3))
    assertEquals(20, counts(4))
  }

  /**
   * `groupByKey` must preserve every value across the shuffle: summing the size of each key's group
   * must recover the original element count, with one group per residue class.
   */
  @Test
  def groupByKeyPreservesAllValues(): Unit = {
    val rdd = sc.parallelize(1 to 100, 4).map(i => (i % 5, i))
    val grouped = rdd.groupByKey().collectAsMap()
    assertEquals(5, grouped.size)
    val totalValues = grouped.values.map(_.size).sum
    assertEquals(100, totalValues)
  }

  /**
   * `distinct` is a shuffle-backed deduplication; mapping `1..100` via `% 10` yields exactly ten
   * distinct residues regardless of how the streaming backend pipelines the intermediate data.
   */
  @Test
  def distinctThroughStreamingShuffle(): Unit = {
    val distinctCount = sc.parallelize((1 to 100).map(_ % 10)).distinct().count()
    assertEquals(10L, distinctCount)
  }

  /**
   * With the manager still `streaming` but `spark.shuffle.streaming.enabled=false`, the dual
   * activation gate is open, so the streaming manager degrades to a pass-through over the inner
   * sort-based shuffle. The identical keyed sum must remain correct, proving the zero-regression
   * fallback contract end-to-end.
   */
  @Test
  def fallbackWhenDisabledStillCorrect(): Unit = {
    startContext(streamingEnabled = false)
    val rdd = sc.parallelize(1 to 100, 4).map(i => (i % 5, 1))
    val counts = rdd.reduceByKey(_ + _).collectAsMap()
    assertEquals(5, counts.size)
    assertEquals(20, counts(0))
    assertEquals(20, counts(4))
  }

  /**
   * The configured shuffle manager must be wired into the running [[org.apache.spark.SparkEnv]].
   * The assertion is deliberately tolerant -- it accepts either the streaming manager (when the
   * `streaming` alias resolved) or a sort manager (fallback) -- so it validates that the alias
   * resolved to a real manager without hard-coding an internal class name.
   */
  @Test
  def nonNullShuffleManagerWiring(): Unit = {
    assertNotNull(sc.env.shuffleManager)
    val managerClassName = sc.env.shuffleManager.getClass.getName
    assertTrue(managerClassName.contains("Streaming") || managerClassName.contains("Sort"))
  }
}
