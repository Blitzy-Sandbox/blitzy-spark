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
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, MigratableResolver, ShuffleBlockResolver,
  ShuffleReader, ShuffleWriter}

/**
 * Unit tests for [[StreamingShuffleManager]], the opt-in streaming `ShuffleManager` SPI
 * implementation selected by `spark.shuffle.manager=streaming`.
 *
 * The suite mirrors `org.apache.spark.shuffle.sort.SortShuffleManagerSuite` for its mocking idiom
 * (a [[ShuffleDependency]] mocked with a [[RuntimeExceptionAnswer]] so any un-stubbed accessor
 * fails loudly) and validates the manager's coexistence contract with the production-stable sort
 * path:
 *
 *  - '''Local-mode safety''' -- the manager constructs cleanly (no daemons, no metrics, no
 *    endpoint) when `SparkEnv.get == null`, degrading to a thin pass-through over the inner
 *    `SortShuffleManager`.
 *  - '''Dual activation gate''' -- `registerShuffle` returns a [[StreamingShuffleHandle]] only when
 *    `spark.shuffle.manager=streaming` AND `spark.shuffle.streaming.enabled=true` AND a live
 *    [[SparkEnv]] is present; otherwise it delegates to the inner `SortShuffleManager`, which
 *    returns a non-streaming [[BaseShuffleHandle]] subtype.
 *  - '''Handle-type dispatch''' -- `getWriter` / `getReader` route a [[StreamingShuffleHandle]] to
 *    the streaming components and every other handle to the sort path.
 *  - '''Metrics-source registration''' -- a `Source` named `streamingShuffle` is registered with
 *    the active `MetricsSystem` when streaming is engaged, and removed on `stop`.
 *  - '''Resolver + lifecycle''' -- `shuffleBlockResolver` is a migration-capable
 *    [[ShuffleBlockResolver]], `unregisterShuffle` returns `true`, and `stop` is ordered and
 *    idempotent.
 *
 * Every collaborator referenced here is the real, same-package production class; none is redefined
 * or stubbed. Tests that require a `SparkEnv` create a local [[SparkContext]] (managed by
 * [[LocalSparkContext]]) and construct the manager directly, so the suite does not depend on the
 * `streaming` short-name alias being registered in the `ShuffleManager` factory (that alias is an
 * independent, additive edit to `ShuffleManager.scala`).
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with LocalSparkContext with Matchers {

  /**
   * Mirrors the sort-path template's positional `doReturn` helper, pinning Mockito's varargs
   * overload so `doReturn(value).when(mock).accessor` stubs a zero-argument accessor without
   * invoking the (non-stubbed) real method.
   */
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * A Mockito [[Answer]] that fails any un-stubbed invocation, so a test only ever exercises the
   * dependency accessors it explicitly stubbed (copied from `SortShuffleManagerSuite`).
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  /**
   * Build a mocked [[ShuffleDependency]] exposing only the accessors the manager and the writers
   * it dispatches to read: `shuffleId`, `partitioner`, `serializer`, `keyOrdering`, `aggregator`,
   * `mapSideCombine`, and `rowBasedChecksums` (the last consumed by the sort-fallback
   * `BypassMergeSortShuffleWriter`). Any other accessor throws via [[RuntimeExceptionAnswer]].
   */
  private def shuffleDep(
      shuffleId: Int,
      partitioner: Partitioner,
      serializer: Serializer,
      keyOrdering: Option[Ordering[Any]],
      aggregator: Option[Aggregator[Any, Any, Any]],
      mapSideCombine: Boolean): ShuffleDependency[Any, Any, Any] = {
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    doReturn(shuffleId).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(keyOrdering).when(dep).keyOrdering
    doReturn(aggregator).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine
    // The sort-fallback getWriter path builds a real BypassMergeSortShuffleWriter, whose
    // constructor reads dep.rowBasedChecksums(); stub it with the same empty array the real
    // ShuffleDependency defaults to so the fallback writer constructs cleanly against the mock.
    doReturn(ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS).when(dep).rowBasedChecksums
    dep
  }

  /**
   * A representative, streaming-eligible dependency: a hash partitioner (whose `numPartitions` the
   * streaming writer reads for buffer sizing), a real Java serializer, no ordering, no aggregator,
   * and no map-side combine.
   */
  private def sampleDep(
      shuffleId: Int = 0,
      numPartitions: Int = 2): ShuffleDependency[Any, Any, Any] = {
    shuffleDep(
      shuffleId,
      new HashPartitioner(numPartitions),
      new JavaSerializer(new SparkConf(false)),
      keyOrdering = None,
      aggregator = None,
      mapSideCombine = false)
  }

  /**
   * A configuration with the dual activation gate fully ON: `spark.shuffle.manager=streaming` and
   * `spark.shuffle.streaming.enabled=true`. `base` defaults to an empty conf for no-`SparkEnv`
   * tests; `SparkEnv`-present tests pass `sc.getConf` so the inner sort path inherits a valid
   * application id when it lazily initializes its executor components.
   */
  private def streamingConf(base: SparkConf = new SparkConf(false)): SparkConf =
    base.clone
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")

  /**
   * A configuration that selects the streaming manager but leaves the feature flag OFF, so the dual
   * gate is not satisfied and every shuffle must fall back to the inner sort path.
   */
  private def disabledConf(base: SparkConf = new SparkConf(false)): SparkConf =
    base.clone
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "false")

  /**
   * Run `body` with a guaranteed-absent `SparkEnv`, restoring whatever env was installed before.
   * Local-mode safety of the manager hinges on `SparkEnv.get == null`, so these tests must not
   * observe an env leaked by a `SparkContext`-based test.
   */
  private def withoutSparkEnv(body: => Unit): Unit = {
    val previousEnv = SparkEnv.get
    SparkEnv.set(null)
    try {
      body
    } finally {
      SparkEnv.set(previousEnv)
    }
  }

  test("constructs cleanly in local mode when SparkEnv.get == null") {
    withoutSparkEnv {
      assert(SparkEnv.get == null)
      // Construction must not throw even though the dual gate is on: with no SparkEnv the streaming
      // machinery stays dormant (no daemons, no metrics source, no endpoint).
      val manager = new StreamingShuffleManager(streamingConf(), isDriver = true)
      try {
        // The SPI-required block resolver is always available (sort-fallback reads use it).
        assert(manager.shuffleBlockResolver != null)
        assert(manager.shuffleBlockResolver.isInstanceOf[ShuffleBlockResolver])
      } finally {
        manager.stop()
      }
    }
  }

  test("registerShuffle delegates to the inner sort path when the feature is disabled") {
    withoutSparkEnv {
      val manager = new StreamingShuffleManager(disabledConf(), isDriver = true)
      try {
        val handle = manager.registerShuffle(1, sampleDep(shuffleId = 1))
        // Feature disabled -> the inner SortShuffleManager registers the shuffle and returns one of
        // its own (non-streaming) handle types, all of which extend BaseShuffleHandle.
        assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
        assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
      } finally {
        manager.stop()
      }
    }
  }

  test("shuffleBlockResolver is a migration-capable ShuffleBlockResolver") {
    withoutSparkEnv {
      val manager = new StreamingShuffleManager(streamingConf(), isDriver = true)
      try {
        val resolver = manager.shuffleBlockResolver
        assert(resolver != null)
        assert(resolver.isInstanceOf[ShuffleBlockResolver])
        // Decommission block migration is preserved by delegating to the sort-path resolver, so the
        // streaming resolver must itself be a MigratableResolver.
        assert(resolver.isInstanceOf[MigratableResolver])
      } finally {
        manager.stop()
      }
    }
  }

  test("unregisterShuffle returns true and stop is idempotent") {
    withoutSparkEnv {
      val manager = new StreamingShuffleManager(disabledConf(), isDriver = true)
      manager.registerShuffle(2, sampleDep(shuffleId = 2))
      assert(manager.unregisterShuffle(2))
      // The ordered shutdown is guarded by an AtomicBoolean, so a second stop() must be a safe
      // no-op rather than throwing or double-releasing collaborators.
      manager.stop()
      manager.stop()
    }
  }

  test("registerShuffle falls back to the sort path in v1 even when the dual gate is on") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val manager = new StreamingShuffleManager(streamingConf(), isDriver = true)
    try {
      // v1 zero-regression guarantee: the streaming wire transport is a logging-only stub
      // (StreamingShuffleTransport.isWireTransferAvailable == false), so canUseStreaming is always
      // false and registerShuffle delegates every shuffle to the inner SortShuffleManager -- even
      // with the dual activation gate (manager=streaming AND streaming.enabled=true) fully on. The
      // returned handle is therefore a non-streaming BaseShuffleHandle subtype, never a
      // StreamingShuffleHandle. The streaming registration branch is retained as the v2 data path
      // and is exercised directly by the getWriter/getReader dispatch tests below.
      val handle = manager.registerShuffle(0, sampleDep(shuffleId = 0))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
    } finally {
      manager.stop()
    }
  }

  test("getWriter dispatches by handle type") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    // Build both managers from the live conf (which carries spark.app.id) so the inner sort path's
    // executor components initialize cleanly when the sort-delegation writer is constructed.
    val streamingManager =
      new StreamingShuffleManager(streamingConf(sc.getConf), isDriver = true)
    val sortManager =
      new StreamingShuffleManager(disabledConf(sc.getConf), isDriver = true)
    try {
      val dep = sampleDep(shuffleId = 0)

      // A streaming handle is dispatched to the streaming writer. In v1 registerShuffle never
      // yields a StreamingShuffleHandle (the stub transport forces the sort fallback), so construct
      // the dispatch signal directly to exercise getWriter's handle-type routing -- the v2 data
      // path that hands a StreamingShuffleHandle to the StreamingShuffleWriter whenever streaming
      // is enabled on the executor.
      val streamingHandle = new StreamingShuffleHandle[Any, Any, Any](
        shuffleId = 0, dependency = dep,
        bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = 0)
      val streamingCtx = MemoryTestingUtils.fakeTaskContext(sc.env)
      val writer = streamingManager.getWriter[Any, Any](
        streamingHandle, 0L, streamingCtx, streamingCtx.taskMetrics().shuffleWriteMetrics)
      assert(writer.isInstanceOf[StreamingShuffleWriter[_, _, _]])
      // Release the buffers / execution memory the writer reserved via its inner MemoryConsumer.
      writer.stop(success = false)

      // A non-streaming handle (feature off) falls through to the sort path's writer.
      val sortHandle = sortManager.registerShuffle(1, dep)
      assert(!sortHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      val sortCtx = MemoryTestingUtils.fakeTaskContext(sc.env)
      val sortWriter = sortManager.getWriter[Any, Any](
        sortHandle, 1L, sortCtx, sortCtx.taskMetrics().shuffleWriteMetrics)
      assert(sortWriter.isInstanceOf[ShuffleWriter[_, _]])
      assert(!sortWriter.isInstanceOf[StreamingShuffleWriter[_, _, _]])
    } finally {
      streamingManager.stop()
      sortManager.stop()
    }
  }

  test("getReader returns a ShuffleReader via the 7-arg overload") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val manager = new StreamingShuffleManager(streamingConf(), isDriver = true)
    try {
      // In v1 registerShuffle returns a sort (base) handle, so construct a StreamingShuffleHandle
      // directly to exercise getReader's handle-type dispatch to the StreamingShuffleReader (the v2
      // read path).
      val handle = new StreamingShuffleHandle[Any, Any, Any](
        shuffleId = 0, dependency = sampleDep(shuffleId = 0, numPartitions = 1),
        bufferSizePercent = 20, spillThreshold = 80, maxBandwidthMBps = 0)
      val context = MemoryTestingUtils.fakeTaskContext(sc.env)
      val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
      // The shuffle was never run, so the unmodified MapOutputTracker reports no producer blocks in
      // local mode; the streaming reader is still constructed over the resulting empty block set.
      val reader = manager.getReader[Any, Any](handle, 0, 1, 0, 1, context, readMetrics)
      assert(reader != null)
      assert(reader.isInstanceOf[ShuffleReader[_, _]])
      assert(reader.isInstanceOf[StreamingShuffleReader[_, _]])
    } finally {
      manager.stop()
    }
  }

  test("registers a streamingShuffle metrics source when SparkEnv is present") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val manager = new StreamingShuffleManager(streamingConf(), isDriver = true)
    try {
      val registered = sc.env.metricsSystem.getSourcesByName("streamingShuffle")
      assert(registered.nonEmpty)
      assert(registered.head.isInstanceOf[StreamingShuffleSource])
      // stop() must deregister the source so a later manager in the same JVM does not observe a
      // stale duplicate.
      manager.stop()
      assert(sc.env.metricsSystem.getSourcesByName("streamingShuffle").isEmpty)
    } finally {
      manager.stop()
    }
  }
}
