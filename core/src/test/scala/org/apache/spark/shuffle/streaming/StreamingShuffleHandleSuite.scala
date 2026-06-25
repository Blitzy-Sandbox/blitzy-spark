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

import org.apache.spark.{ShuffleDependency, SparkFunSuite}
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * Unit tests for [[StreamingShuffleHandle]], the [[BaseShuffleHandle]] subtype that the
 * streaming shuffle manager uses both as a dispatch discriminator and as the carrier for the
 * three per-shuffle streaming tuning values.
 *
 * The handle is a pure data carrier, so these tests need neither a `SparkContext` nor any live
 * Spark services. The shuffle dependency is a lightweight Mockito mock because the handle only
 * retains the reference and never invokes a method on it.
 */
class StreamingShuffleHandleSuite extends SparkFunSuite {

  /**
   * Builds a `StreamingShuffleHandle[Int, Int, Int]` backed by a mocked `ShuffleDependency`.
   *
   * The dependency is mocked rather than constructed because `BaseShuffleHandle` only retains
   * the reference; no member of the dependency is read by the handle, so nothing is stubbed.
   */
  private def newHandle(
      shuffleId: Int = 7,
      bufferSizePercent: Int = 20,
      spillThreshold: Int = 80,
      maxBandwidthMBps: Int = 0): StreamingShuffleHandle[Int, Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dep, bufferSizePercent, spillThreshold, maxBandwidthMBps)
  }

  test("is a BaseShuffleHandle subtype (dispatch discriminator)") {
    val handle = newHandle()
    // The streaming manager dispatches by pattern-matching on the handle type, so the handle
    // must be both a BaseShuffleHandle and a ShuffleHandle.
    assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
    assert(handle.isInstanceOf[org.apache.spark.shuffle.ShuffleHandle])
  }

  test("carries the three tuning vals") {
    val handle = newHandle(
      bufferSizePercent = 25,
      spillThreshold = 85,
      maxBandwidthMBps = 128)
    assert(handle.bufferSizePercent === 25)
    assert(handle.spillThreshold === 85)
    assert(handle.maxBandwidthMBps === 128)
  }

  test("inherits shuffleId and dependency from BaseShuffleHandle") {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle[Int, Int, Int](42, dep, 20, 80, 0)
    assert(handle.shuffleId === 42)
    // `dependency` is the exact reference passed in (a val inherited from BaseShuffleHandle).
    assert(handle.dependency eq dep)
  }

  test("is Serializable") {
    // The handle is shipped to tasks, so it must be Serializable (inherited from
    // ShuffleHandle). We assert the type contract instead of a serialize/deserialize
    // round-trip: a Mockito-mocked dependency is not serializable, and constructing a real
    // one would turn this otherwise fast, pure-unit suite into a heavyweight test.
    val handle = newHandle()
    assert(handle.isInstanceOf[java.io.Serializable])
    assert(classOf[java.io.Serializable]
      .isAssignableFrom(classOf[StreamingShuffleHandle[Int, Int, Int]]))
  }

  test("different tuning values are independent") {
    val first = newHandle(
      shuffleId = 1,
      bufferSizePercent = 10,
      spillThreshold = 50,
      maxBandwidthMBps = 64)
    val second = newHandle(
      shuffleId = 2,
      bufferSizePercent = 40,
      spillThreshold = 95,
      maxBandwidthMBps = 256)

    assert(first.bufferSizePercent === 10)
    assert(first.spillThreshold === 50)
    assert(first.maxBandwidthMBps === 64)

    assert(second.bufferSizePercent === 40)
    assert(second.spillThreshold === 95)
    assert(second.maxBandwidthMBps === 256)
  }
}
