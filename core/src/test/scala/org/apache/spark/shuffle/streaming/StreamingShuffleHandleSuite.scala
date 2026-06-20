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
 * The handle is the streaming backend's [[BaseShuffleHandle]] specialization: beyond the shuffle
 * id and [[org.apache.spark.ShuffleDependency]] captured by the superclass, it carries the three
 * streaming tuning values resolved once on the driver at registration time. These tests assert
 * that the handle:
 *
 *   - faithfully exposes the three `Int` tuning fields plus the inherited `shuffleId` and
 *     `dependency`;
 *   - remains a [[BaseShuffleHandle]] so the manager can dispatch by handle type (streaming vs.
 *     sort);
 *   - preserves distinct tuning values through construction (guarding against constructor
 *     field-ordering bugs);
 *   - inherits `Serializable` so it can be shipped to tasks.
 *
 * The suite is a pure, deterministic unit test: it needs no `SparkContext` and only mocks the
 * shuffle dependency, which is never exercised beyond reference-identity comparison.
 */
class StreamingShuffleHandleSuite extends SparkFunSuite with Matchers {

  test("handle carries tuning values and shuffleId") {
    // The dependency is only used for identity comparison, so an unstubbed mock is sufficient.
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle(
      7,
      dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)

    assert(handle.shuffleId === 7)
    assert(handle.bufferSizePercent === 20)
    assert(handle.spillThreshold === 80)
    // A non-positive bandwidth is the documented sentinel for "unlimited" and must round-trip.
    assert(handle.maxBandwidthMBps === -1)
    // The handle must retain the exact dependency instance handed to it (reference identity).
    assert(handle.dependency eq dep)
  }

  test("handle is a BaseShuffleHandle") {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle(
      1,
      dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)

    // The manager dispatches by handle type (streaming vs. sort), so the inheritance contract
    // with BaseShuffleHandle must hold.
    assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
  }

  test("distinct tuning values are preserved") {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    // Three mutually distinct values guard against a constructor that transposes the fields.
    val handle = new StreamingShuffleHandle(
      3,
      dep,
      bufferSizePercent = 35,
      spillThreshold = 70,
      maxBandwidthMBps = 128)

    assert(handle.bufferSizePercent === 35)
    assert(handle.spillThreshold === 70)
    assert(handle.maxBandwidthMBps === 128)
  }

  test("handle is serializable") {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handle = new StreamingShuffleHandle(
      5,
      dep,
      bufferSizePercent = 20,
      spillThreshold = 80,
      maxBandwidthMBps = -1)

    // Handles are shipped to tasks, so the type must inherit Serializable from ShuffleHandle.
    // Full object-graph serialization additionally requires a serializable ShuffleDependency,
    // which is exercised by the integration suites rather than mocked here.
    assert(handle.isInstanceOf[java.io.Serializable])
  }
}
