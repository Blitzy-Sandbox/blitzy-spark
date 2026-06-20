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

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Unit tests for [[StreamingShuffleManager]], the opt-in streaming shuffle backend's
 * [[org.apache.spark.shuffle.ShuffleManager]] implementation.
 *
 * Modeled on `SortShuffleManagerSuite`, these are mock-only unit tests: the shuffle dependency is a
 * strict mock guarded by [[RuntimeExceptionAnswer]] so that any dependency method the manager calls
 * but the test did not stub surfaces immediately as a failure rather than a silent `null`.
 *
 * The suite proves the manager's defining behaviors:
 *   - dispatch by handle TYPE -- a streaming-enabled, non-fallback registration mints a
 *     [[StreamingShuffleHandle]], while a disabled (or fallback-tripped) manager delegates to a
 *     lazily-instantiated inner [[SortShuffleManager]] and therefore mints a sort
 *     [[BaseShuffleHandle]];
 *   - getWriter / the 7-arg getReader follow the handle type to the matching backend;
 *   - the metrics-source registration and all executor-side wiring are gated on
 *     `SparkEnv.get != null`, so the manager constructs, registers, and stops without a `SparkEnv`
 *     (local-mode / bare-unit-test safety) and never throws a `NullPointerException`;
 *   - stop() is a safe, idempotent teardown.
 *
 * [[LocalSparkContext]] is mixed in only for the three tests that exercise the executor entry
 * points (getReader / getWriter / shuffleBlockResolver), which require a live `SparkEnv`; it resets
 * the context after every test so the remaining tests reliably observe `SparkEnv.get == null`.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with LocalSparkContext with Matchers {

  /**
   * Mirrors `SortShuffleManagerSuite.doReturn`: stubs a value WITHOUT first invoking the (strict)
   * mock's real method, which [[RuntimeExceptionAnswer]] would otherwise reject.
   */
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * Answer that throws on any non-stubbed invocation, so a dependency method the manager calls
   * unexpectedly is reported instead of silently returning `null` (copied from the model suite).
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  /**
   * Builds a strict [[ShuffleDependency]] mock stubbing only the accessors the manager (and, on the
   * fallback path, the inner [[SortShuffleManager]]) consult during registration. On the streaming
   * path these stubs are simply never read, because the handle just captures the dependency.
   */
  private def shuffleDep(
      partitioner: Partitioner,
      serializer: Serializer,
      keyOrdering: Option[Ordering[Any]],
      aggregator: Option[Aggregator[Any, Any, Any]],
      mapSideCombine: Boolean): ShuffleDependency[Any, Any, Any] = {
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    doReturn(0).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(keyOrdering).when(dep).keyOrdering
    doReturn(aggregator).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine
    dep
  }

  /**
   * A dependency that resolves to a sort `BypassMergeSortShuffleHandle` on the fallback path (no
   * map-side combine, two partitions), and is otherwise ignored on the streaming path.
   */
  private def bypassDep(conf: SparkConf): ShuffleDependency[Any, Any, Any] =
    shuffleDep(
      new HashPartitioner(2), new JavaSerializer(conf), None, None, mapSideCombine = false)

  /**
   * A dependency that resolves to a deserialized sort `BaseShuffleHandle` (served by a
   * `SortShuffleWriter`): map-side combine disqualifies the bypass path and the Java serializer
   * (no object relocation) disqualifies the serialized path. Its writer's constructor reads no
   * dependency methods, so it composes cleanly with the strict mock.
   */
  private def baseHandleDep(conf: SparkConf): ShuffleDependency[Any, Any, Any] =
    shuffleDep(
      new HashPartitioner(2), new JavaSerializer(conf), None, None, mapSideCombine = true)

  /**
   * A [[SparkConf]] that selects the streaming manager (`spark.shuffle.manager=streaming`) and
   * toggles the streaming feature flag. `spark.app.id` is set so the inner sort manager's
   * executor-component loading (exercised by the getWriter-delegation test) can resolve an app id.
   */
  private def newConf(streaming: Boolean): SparkConf = {
    val c = new SparkConf(false)
    c.set(config.SHUFFLE_MANAGER, "streaming")
    c.set(config.SHUFFLE_STREAMING_ENABLED, streaming)
    c.set("spark.app.id", "streaming-shuffle-manager-suite")
    c
  }

  test("streaming disabled returns a non-streaming (sort) handle") {
    val conf = newConf(streaming = false)
    val dep = bypassDep(conf)
    // A standalone, identically-configured SortShuffleManager is the oracle for the handle class
    // the disabled streaming manager must produce, since registration is delegated to an inner
    // SortShuffleManager. Both are safe to build/stop without a SparkEnv (lazy blockManager).
    val sortMgr = new SortShuffleManager(conf)
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    try {
      val expectedHandleClass = sortMgr.registerShuffle(0, dep).getClass
      val handle = mgr.registerShuffle(0, dep)
      // Defining proof: the disabled path never mints a streaming handle ...
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      // ... it is a sort BaseShuffleHandle, in fact the exact class the sort manager produces.
      assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
      assert(handle.getClass == expectedHandleClass)
    } finally {
      mgr.stop()
      sortMgr.stop()
    }
  }

  test("streaming enabled with no fallback returns a StreamingShuffleHandle") {
    val conf = newConf(streaming = true)
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, bypassDep(conf))
      // With the feature flag on and a fresh (untripped) fallback policy, useStreaming holds, so
      // the handle TYPE is streaming -- the source of truth for this shuffle's backend.
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    } finally {
      mgr.stop()
    }
  }

  test("getReader exposes the 7-arg overload and returns a streaming reader") {
    // The streaming reader sources its serializerManager/blockManager/mapOutputTracker from the
    // running SparkEnv (default ctor args), so a live local SparkContext is required.
    sc = new SparkContext("local", "test", new SparkConf(false))
    val conf = newConf(streaming = true)
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, bypassDep(conf))
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      val context = MemoryTestingUtils.fakeTaskContext(sc.env)
      val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
      // The 7-arg getReader is the only reader overload the manager overrides (the 5-arg form is
      // final and forwards here). Reading is lazy, so constructing the reader does not fetch.
      val reader = mgr.getReader(handle, 0, 1, 0, 1, context, readMetrics)
      assert(reader != null)
      assert(reader.isInstanceOf[StreamingShuffleReader[_, _]])
    } finally {
      mgr.stop()
    }
  }

  test("getWriter delegates to the sort manager when streaming is disabled") {
    // The sort writer construction reads SparkEnv.get.blockManager and forces executor components,
    // so a live local SparkContext is required for the delegated path.
    sc = new SparkContext("local", "test", new SparkConf(false))
    val conf = newConf(streaming = false)
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, baseHandleDep(conf))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      val context = MemoryTestingUtils.fakeTaskContext(sc.env)
      val writeMetrics = context.taskMetrics().shuffleWriteMetrics
      val writer = mgr.getWriter(handle, 0L, context, writeMetrics)
      // The disabled manager serves the sort handle from the inner SortShuffleManager, so the
      // writer is a sort writer -- never a StreamingShuffleWriter.
      assert(writer != null)
      assert(!writer.isInstanceOf[StreamingShuffleWriter[_, _]])
    } finally {
      mgr.stop()
    }
  }

  test("fallback decision trips the sort path") {
    // The manager holds its StreamingShuffleFallbackPolicy privately and exposes no injection point
    // (its constructor is (conf, isDriver)), so the fallback contract is proven in two parts.
    //
    // Part 1 -- the decision object the manager gates useStreaming on: a fresh policy does not fall
    // back, and a tripped condition (a producer/consumer version mismatch) flips shouldFallback to
    // true. Since useStreaming = enabled && !shouldFallback, a true value forces the sort branch.
    val policy = new StreamingShuffleFallbackPolicy(new StreamingShuffleConfig(newConf(true)))
    assert(!policy.shouldFallback)
    policy.markVersionMismatch()
    assert(policy.shouldFallback)
    // Part 2 -- the manager's sort delegation: the streaming-disabled path reaches the identical
    // `else` branch of useStreaming that a tripped fallback reaches, and yields a sort handle.
    val conf = newConf(streaming = false)
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    try {
      val handle = mgr.registerShuffle(0, bypassDep(conf))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    } finally {
      mgr.stop()
    }
  }

  test("shuffleBlockResolver returns a non-null streaming resolver") {
    // The streaming resolver's convenience constructor resolves SparkEnv.get.blockManager eagerly,
    // so a live local SparkContext is required.
    sc = new SparkContext("local", "test", new SparkConf(false))
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    try {
      // The manager always exposes the streaming resolver (it delegates .data/.index/migration to
      // an inner IndexShuffleBlockResolver), so it is correct in both streaming and fallback modes.
      val resolver = mgr.shuffleBlockResolver
      assert(resolver != null)
      assert(resolver.isInstanceOf[StreamingShuffleBlockResolver])
    } finally {
      mgr.stop()
    }
  }

  test("unregisterShuffle returns true") {
    val conf = newConf(streaming = false)
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    try {
      mgr.registerShuffle(0, bypassDep(conf))
      // Cleanup is best-effort and idempotent; the manager mirrors the sort manager and always
      // reports success, delegating the unregister to the inner SortShuffleManager on this path.
      assert(mgr.unregisterShuffle(0))
    } finally {
      mgr.stop()
    }
  }

  test("construct, registerShuffle, and stop are SparkEnv-null safe (no NPE)") {
    // A bare unit test installs no SparkEnv; LocalSparkContext also nulls it after every test.
    assert(SparkEnv.get == null)
    val conf = newConf(streaming = true)
    // Even with streaming enabled, none of construct / registerShuffle / stop touches SparkEnv: the
    // executor-side wiring (metrics source, spill poller, backpressure RPC) is built lazily by
    // getWriter/getReader and is itself gated on SparkEnv.get != null. So nothing here may NPE.
    val mgr = new StreamingShuffleManager(conf, isDriver = false)
    val handle = mgr.registerShuffle(0, bypassDep(conf))
    assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    mgr.stop()
    // A second stop on a manager that never built executor components is also safe.
    mgr.stop()
  }

  test("stop() is idempotent") {
    val mgr = new StreamingShuffleManager(newConf(streaming = true), isDriver = false)
    // No getWriter/getReader ran, so no executor components were started; stop() must be a safe
    // no-op invokable repeatedly. A post-stop unregister still reports success (best-effort).
    mgr.stop()
    mgr.stop()
    assert(mgr.unregisterShuffle(0))
  }
}
