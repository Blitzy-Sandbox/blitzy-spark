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
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{ShuffleDependency, SparkFunSuite}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle}

/**
 * Unit tests for [[StreamingShuffleHandle]] &mdash; the marker subclass of
 * [[org.apache.spark.shuffle.BaseShuffleHandle]] that identifies shuffles routed
 * to the streaming path by `StreamingShuffleManager`. The handle carries no state
 * beyond the base fields, so this suite validates exactly the behaviors that
 * `StreamingShuffleManager.getWriter` / `getReader` rely on to dispatch:
 *
 *   1. Construction accepts `(shuffleId: Int, dependency: ShuffleDependency[K, V, V])`
 *      and the arguments flow through to the base-class fields.
 *   2. The handle is a subtype of both `BaseShuffleHandle[_, _, _]` and the
 *      `ShuffleHandle` root, so any `case _: BaseShuffleHandle[_, _, _] =>`
 *      branch in legacy code continues to match it as a safety net.
 *   3. Pattern-matching `case _: StreamingShuffleHandle[_, _] =>` distinguishes
 *      streaming handles from vanilla `BaseShuffleHandle` instances &mdash; the
 *      exact dispatch shape used by `StreamingShuffleManager`.
 *   4. The third type parameter of `BaseShuffleHandle` collapses from `C` to
 *      `V` at compile time because the streaming writer pipelines records
 *      without an intermediate combiner type. The type-ascription test in
 *      Group 2 is therefore a compile-time contract as much as a runtime one.
 *
 * This suite is pure in-JVM logic: it creates no `SparkContext`, starts no
 * executor or RPC machinery, and holds no file-system or network resources.
 * Runtime is expected to be well under one second.
 */
class StreamingShuffleHandleSuite extends SparkFunSuite with Matchers {

  // Helper that produces a Mockito stub for `ShuffleDependency[String, Int, Int]`.
  // The value and combiner types are identical (both `Int`) so that the returned
  // mock satisfies `BaseShuffleHandle[K, V, V]` &mdash; the parent-class shape
  // required by `StreamingShuffleHandle[K, V]` after the third type parameter
  // is collapsed. No RDD lineage, partitioner, or serializer is stubbed because
  // this suite never exercises any method on the dependency &mdash; the mock
  // only needs to satisfy the type system.
  private def mockDep(): ShuffleDependency[String, Int, Int] =
    mock(classOf[ShuffleDependency[String, Int, Int]])

  // ==========================================================================
  // Group 1: Construction and inherited field access
  // ==========================================================================

  test("constructs with shuffleId and dependency arguments") {
    val dep = mockDep()
    val handle = new StreamingShuffleHandle[String, Int](42, dep)

    handle.shuffleId must be(42)
    handle.dependency must be theSameInstanceAs dep
  }

  test("shuffleId field is inherited from BaseShuffleHandle") {
    val dep = mockDep()
    val handle = new StreamingShuffleHandle[String, Int](7, dep)
    handle.shuffleId must be(7)
  }

  test("dependency field is inherited from BaseShuffleHandle") {
    val dep = mockDep()
    val handle = new StreamingShuffleHandle[String, Int](1, dep)
    (handle.dependency eq dep) must be(true)
  }

  test("handle accepts shuffleId values of 0 and Int.MaxValue") {
    val dep = mockDep()
    val zero = new StreamingShuffleHandle[String, Int](0, dep)
    zero.shuffleId must be(0)
    val big = new StreamingShuffleHandle[String, Int](Int.MaxValue, dep)
    big.shuffleId must be(Int.MaxValue)
  }

  // ==========================================================================
  // Group 2: Type hierarchy and subtype conformance
  // ==========================================================================

  test("is an instance of BaseShuffleHandle") {
    val dep = mockDep()
    val handle = new StreamingShuffleHandle[String, Int](1, dep)
    handle.isInstanceOf[BaseShuffleHandle[_, _, _]] must be(true)
  }

  test("is an instance of ShuffleHandle") {
    val dep = mockDep()
    val handle = new StreamingShuffleHandle[String, Int](1, dep)
    handle.isInstanceOf[ShuffleHandle] must be(true)
  }

  test("type-match pattern recognizes StreamingShuffleHandle") {
    val dep = mockDep()
    val base: BaseShuffleHandle[_, _, _] = new StreamingShuffleHandle[String, Int](1, dep)

    val matched = base match {
      case _: StreamingShuffleHandle[_, _] => true
      case _ => false
    }
    matched must be(true)
  }

  test("vanilla BaseShuffleHandle does NOT match StreamingShuffleHandle pattern") {
    val dep = mock(classOf[ShuffleDependency[String, Int, Int]])
    val vanilla = new BaseShuffleHandle[String, Int, Int](42, dep)

    val matched = vanilla match {
      case _: StreamingShuffleHandle[_, _] => true
      case _ => false
    }
    matched must be(false)
  }

  test("type collapse: StreamingShuffleHandle[K, V] extends BaseShuffleHandle[K, V, V]") {
    // This test doubles as a compile-time type ascription: if the handle does not
    // extend `BaseShuffleHandle[K, V, V]` (i.e. if the third type parameter were
    // not collapsed to V), this line would fail to type-check.
    val dep = mock(classOf[ShuffleDependency[String, Int, Int]])
    val handle: BaseShuffleHandle[String, Int, Int] =
      new StreamingShuffleHandle[String, Int](1, dep)
    handle must not be null
  }

  // ==========================================================================
  // Group 3: Dispatch-level pattern matching as used in
  // StreamingShuffleManager.getWriter / getReader
  // ==========================================================================

  test("pattern dispatch: StreamingShuffleHandle branch is selected") {
    val dep = mockDep()
    val handle: BaseShuffleHandle[_, _, _] = new StreamingShuffleHandle[String, Int](1, dep)

    val dispatchResult = handle match {
      case _: StreamingShuffleHandle[_, _] => "streaming"
      case _ => "fallback"
    }
    dispatchResult must be("streaming")
  }

  test("pattern dispatch: vanilla BaseShuffleHandle falls through to fallback branch") {
    val dep = mock(classOf[ShuffleDependency[String, Int, Int]])
    val handle: BaseShuffleHandle[_, _, _] = new BaseShuffleHandle[String, Int, Int](2, dep)

    val dispatchResult = handle match {
      case _: StreamingShuffleHandle[_, _] => "streaming"
      case _ => "fallback"
    }
    dispatchResult must be("fallback")
  }

  test("multiple StreamingShuffleHandle instances are distinct") {
    val dep1 = mockDep()
    val dep2 = mockDep()
    val h1 = new StreamingShuffleHandle[String, Int](1, dep1)
    val h2 = new StreamingShuffleHandle[String, Int](2, dep2)

    (h1 eq h2) must be(false)
    h1.shuffleId must not be h2.shuffleId
  }
}
