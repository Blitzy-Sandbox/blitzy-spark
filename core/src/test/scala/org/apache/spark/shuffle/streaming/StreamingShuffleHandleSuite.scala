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
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * Unit tests for [[StreamingShuffleHandle]].
 *
 * These tests are pure and deterministic (no `SparkContext` is required): they verify that the
 * streaming handle forwards its `shuffleId`/`dependency` to [[BaseShuffleHandle]], carries the
 * three streaming tuning values unchanged, remains a [[BaseShuffleHandle]] so the streaming
 * manager can dispatch by handle type, and stays serializable so it can be shipped to tasks.
 */
class StreamingShuffleHandleSuite extends SparkFunSuite with Matchers {

  /**
   * Builds a Mockito mock of a [[ShuffleDependency]]. The handle only stores the reference and
   * never invokes a method on it, so an unstubbed mock is sufficient and keeps the tests fast.
   */
  private def mockDep(): ShuffleDependency[Int, Int, Int] =
    mock(classOf[ShuffleDependency[Int, Int, Int]])

  test("handle carries tuning values and shuffleId") {
    val dep = mockDep()
    val handle = new StreamingShuffleHandle(
      7,
      dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)

    assert(handle.shuffleId === 7)
    assert(handle.bufferSizePercent === 20)
    assert(handle.spillThreshold === 80)
    assert(handle.maxBandwidthMBps === -1)
    // The base class stores the dependency by reference; verify it is the exact instance.
    assert(handle.dependency eq dep)
  }

  test("handle is a BaseShuffleHandle") {
    val handle = new StreamingShuffleHandle(
      1,
      mockDep(),
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)

    // The streaming manager dispatches by handle type, so this inheritance contract must hold.
    assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
  }

  test("distinct tuning values are preserved") {
    // Three distinct values guard against field-ordering bugs between constructor and getters.
    val handle = new StreamingShuffleHandle(
      3,
      mockDep(),
      bufferSizePercent = 35,
      spillThreshold = 70,
      maxBandwidthMBps = 128)

    assert(handle.bufferSizePercent === 35)
    assert(handle.spillThreshold === 70)
    assert(handle.maxBandwidthMBps === 128)
  }

  test("handle is serializable") {
    val handle = new StreamingShuffleHandle(
      9,
      mockDep(),
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)

    // The handle inherits Serializable from ShuffleHandle so it can be shipped to tasks. Full
    // object-graph serialization additionally requires a serializable dependency and is covered
    // by the integration suites (a Mockito mock is intentionally not serializable here).
    assert(handle.isInstanceOf[java.io.Serializable])
  }
}
