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

import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * Unit tests for [[StreamingShuffleHandle]].
 *
 * Validates that the handle carries the per-shuffle streaming resource envelope
 * (`bufferSizePercent`, `spillThreshold`, `maxBandwidthMBps`), that it extends
 * [[org.apache.spark.shuffle.BaseShuffleHandle]], and that `shuffleId` and `dependency`
 * propagate to the base handle. The handle is exercised in isolation with a mocked
 * [[org.apache.spark.ShuffleDependency]]; no `SparkContext` or shuffle machinery is required.
 */
class StreamingShuffleHandleSuite extends SparkFunSuite {

  /**
   * Builds a minimal mocked [[org.apache.spark.ShuffleDependency]] for handle construction.
   *
   * `BaseShuffleHandle` only stores the dependency reference and the passed-in `shuffleId`, so the
   * mock requires no behavioral stubbing. `shuffleId` is stubbed defensively so the dependency
   * remains self-consistent with the id handed to the handle, should any consumer read it back.
   */
  private def newDependency(shuffleId: Int): ShuffleDependency[Int, Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dep.shuffleId).thenReturn(shuffleId)
    dep
  }

  test("handle carries the streaming resource envelope") {
    val dep = newDependency(7)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = 7,
      dependency = dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = 0)
    assert(handle.bufferSizePercent == 20)
    assert(handle.spillThreshold == 80)
    assert(handle.maxBandwidthMBps == 0)
  }

  test("handle extends BaseShuffleHandle") {
    val dep = newDependency(7)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = 7,
      dependency = dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = 0)
    assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
  }

  test("shuffleId and dependency propagate to the base") {
    val dep = newDependency(7)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = 7,
      dependency = dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = 0)
    assert(handle.shuffleId == 7)
    assert(handle.dependency eq dep)
  }

  test("non-default envelope values are preserved") {
    val dep = newDependency(3)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = 3,
      dependency = dep,
      bufferSizePercent = 50,
      spillThreshold = 95,
      maxBandwidthMBps = 256)
    assert(handle.bufferSizePercent == 50)
    assert(handle.spillThreshold == 95)
    assert(handle.maxBandwidthMBps == 256)
  }

}
