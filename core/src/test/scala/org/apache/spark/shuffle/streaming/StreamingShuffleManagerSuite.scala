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

import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{
  HashPartitioner, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkEnv,
  SparkFunSuite, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{
  BaseShuffleHandle, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}

/**
 * Unit tests for [[StreamingShuffleManager]] -- the opt-in streaming shuffle backend's
 * [[org.apache.spark.shuffle.ShuffleManager]] entry point.
 *
 * The suite uses the mock-only pattern modeled on the sort-based `SortShuffleManagerSuite`
 * (a `doReturn` helper plus a [[RuntimeExceptionAnswer]]-guarded dependency mock) to assert the
 * manager's defining behaviors:
 *
 *  - the lazy inner [[org.apache.spark.shuffle.sort.SortShuffleManager]] FALLBACK delegation:
 *    when streaming is disabled (the default) registration yields a sort [[BaseShuffleHandle]]
 *    and `getWriter` yields the sort writer -- never a streaming handle/writer;
 *  - the streaming-active path: when `spark.shuffle.manager=streaming` AND
 *    `spark.shuffle.streaming.enabled=true`, registration yields a [[StreamingShuffleHandle]]
 *    and the 7-arg `getReader` yields a [[StreamingShuffleReader]];
 *  - the [[StreamingShuffleFallbackPolicy]] gate that drives the disabled-vs-streaming decision;
 *  - the local-mode safety guarantee that construction, registration, and teardown never touch
 *    [[org.apache.spark.SparkEnv]] and so cannot NPE when `SparkEnv.get == null`; and
 *  - the idempotent, no-throw `stop()` teardown.
 *
 * Tests that build the streaming writer/reader (which the manager only does on a live executor)
 * spin up a real local [[SparkContext]] through [[LocalSparkContext]]; the remaining tests are
 * pure and need no Spark runtime. Every constructed manager is `stop()`-ed in a `finally` so no
 * backpressure endpoint, spill poller, or scan thread leaks between tests.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with LocalSparkContext with Matchers {

  /**
   * Mirrors the `SortShuffleManagerSuite` helper: a thin wrapper around the varargs
   * `Mockito.doReturn` overload so a value can be stubbed without a Scala ambiguity warning.
   */
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * A Mockito default answer that fails loudly on any method the production code calls but the
   * test did not explicitly stub. This surfaces unexpected interactions with the shuffle
   * dependency rather than silently returning `null`/`0`, exactly as the model suite does.
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object =
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
  }

  /**
   * Builds a [[ShuffleDependency]] mock guarded by [[RuntimeExceptionAnswer]], stubbing the
   * accessors the sort-based registration/writer path reads when the streaming backend falls
   * back. A two-partition [[HashPartitioner]] with no map-side combine routes the inner sort
   * manager down the bypass-merge-sort path, whose handle is a [[BaseShuffleHandle]] and whose
   * writer is intentionally NOT a [[StreamingShuffleWriter]].
   */
  private def defaultDep(): ShuffleDependency[Any, Any, Any] = {
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    doReturn(0).when(dep).shuffleId
    doReturn(new HashPartitioner(2)).when(dep).partitioner
    doReturn(mock(classOf[Serializer])).when(dep).serializer
    doReturn(None).when(dep).keyOrdering
    doReturn(None).when(dep).aggregator
    doReturn(false).when(dep).mapSideCombine
    // The sort bypass-merge writer reads the per-partition row-based checksums at construction.
    doReturn(ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS).when(dep).rowBasedChecksums
    dep
  }

  /**
   * Builds a [[SparkConf]] that selects the streaming manager alias and toggles the streaming
   * feature flag. `spark.app.id` is set because the inner sort manager's executor components read
   * it when the disabled/fallback path builds a sort writer.
   */
  private def newConf(streaming: Boolean): SparkConf = {
    new SparkConf(false)
      .set(config.SHUFFLE_MANAGER, "streaming")
      .set(config.SHUFFLE_STREAMING_ENABLED, streaming)
      .set("spark.app.id", "test-streaming-shuffle")
  }

  test("streaming disabled: registerShuffle returns a sort (non-streaming) handle") {
    val mgr = new StreamingShuffleManager(newConf(streaming = false), isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, defaultDep())
      // The disabled path delegates to the inner SortShuffleManager, whose handle is a
      // BaseShuffleHandle subtype and is never a StreamingShuffleHandle.
      handle.isInstanceOf[StreamingShuffleHandle[_, _, _]] mustBe false
      handle.isInstanceOf[BaseShuffleHandle[_, _, _]] mustBe true
    } finally {
      mgr.stop()
    }
  }

  test("streaming enabled and no fallback: registerShuffle returns a StreamingShuffleHandle") {
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, defaultDep())
      // Feature flag on and a fresh (untripped) fallback policy => useStreaming is true.
      handle.isInstanceOf[StreamingShuffleHandle[_, _, _]] mustBe true
    } finally {
      mgr.stop()
    }
  }

  test("getReader exposes the 7-arg overload and returns a non-null streaming reader") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, defaultDep())
      // Only the 7-arg getReader is overridden; the 5-arg overload is final in the trait.
      val reader = mgr.getReader(
        handle, 0, 1, 0, 1, TaskContext.empty(), mock(classOf[ShuffleReadMetricsReporter]))
      assert(reader != null)
      reader.isInstanceOf[StreamingShuffleReader[_, _]] mustBe true
    } finally {
      mgr.stop()
    }
  }

  test("getWriter delegates to the sort manager when streaming is disabled") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val mgr = new StreamingShuffleManager(newConf(streaming = false), isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, defaultDep())
      val writer = mgr.getWriter(
        handle, 0L, TaskContext.empty(), mock(classOf[ShuffleWriteMetricsReporter]))
      // The disabled path produces the sort writer, never a StreamingShuffleWriter.
      assert(writer != null)
      writer.isInstanceOf[StreamingShuffleWriter[_, _]] mustBe false
    } finally {
      mgr.stop()
    }
  }

  test("fallback policy gate: a tripped policy forces the sort fallback path") {
    // The manager holds its StreamingShuffleFallbackPolicy privately, so the gate is verified on
    // a directly-constructed policy. useStreaming = enabled && !shouldFallback, so once
    // shouldFallback is true the manager delegates to sort exactly as the disabled path does.
    val policy = new StreamingShuffleFallbackPolicy(new StreamingShuffleConfig(newConf(true)))
    policy.shouldFallback mustBe false
    policy.markVersionMismatch()
    policy.shouldFallback mustBe true
  }

  test("shuffleBlockResolver returns a non-null resolver") {
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    try {
      assert(mgr.shuffleBlockResolver != null)
    } finally {
      mgr.stop()
    }
  }

  test("unregisterShuffle returns true after registering a shuffle") {
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    try {
      mgr.registerShuffle(0, defaultDep())
      mgr.unregisterShuffle(0) mustBe true
    } finally {
      mgr.stop()
    }
  }

  test("construction, registration and stop do not NPE when SparkEnv.get is null") {
    // Local-mode safety: metrics-source and RPC wiring are gated on SparkEnv.get != null, so a
    // manager built without a live environment must register no components and must not NPE.
    val previousEnv = SparkEnv.get
    SparkEnv.set(null)
    try {
      val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
      try {
        val handle = mgr.registerShuffle(0, defaultDep())
        handle.isInstanceOf[StreamingShuffleHandle[_, _, _]] mustBe true
      } finally {
        mgr.stop()
      }
    } finally {
      SparkEnv.set(previousEnv)
    }
  }

  test("stop is idempotent and never throws") {
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    mgr.stop()
    // A second stop must be a safe no-op: teardown is guarded by an AtomicBoolean.
    mgr.stop()
  }
}
