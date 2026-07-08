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

import org.apache.spark.{SparkConf, SparkFunSuite}

/**
 * Unit tests for [[StreamingShuffleConfig]], the typed read-only accessor for the
 * `spark.shuffle.streaming.*` configuration surface.
 *
 * The focus is the range-validation contract, whose rejection / boundary path previously had no
 * direct test: [[StreamingShuffleConfig.validate]] must ACCEPT the exact documented bounds
 * (`bufferSizePercent` in `[1, 50]`, `spillThreshold` in `[50, 95]`, `maxBandwidthMBps >= 0`) and
 * REJECT values just outside them with an `IllegalArgumentException`. Both enforcement layers are
 * covered: the typed `ConfigEntry.checkValue` guard that fires at read time for the two percent
 * keys, and the accessor/`validate()` `require` guard that is the sole rejection path for the
 * unchecked `maxBandwidthMBps` key (which has no upper bound and treats `0` as the "unlimited"
 * sentinel). The dual activation gate, the unlimited-sentinel bandwidth division, and the
 * documented defaults are also asserted so the whole public accessor surface is exercised.
 */
class StreamingShuffleConfigSuite extends SparkFunSuite {

  /**
   * Build a [[StreamingShuffleConfig]] over a fresh, defaults-free [[SparkConf]] carrying the given
   * `spark.shuffle.streaming.*` (and `spark.shuffle.manager`) string overrides. String keys are
   * used deliberately so each value flows through the same typed `ConfigEntry` (and its read-time
   * `checkValue` range guard) that production uses at runtime -- construction never parses the
   * values, so an out-of-range value surfaces only when the corresponding accessor is read.
   */
  private def configWith(pairs: (String, String)*): StreamingShuffleConfig = {
    val conf = new SparkConf(false)
    pairs.foreach { case (key, value) => conf.set(key, value) }
    new StreamingShuffleConfig(conf)
  }

  // -------------------------------------------------------------------------------------------
  // validate(): accept the exact bounds.
  // -------------------------------------------------------------------------------------------

  test("validate accepts the default configuration") {
    // Defaults (bufferSizePercent=20, spillThreshold=80, maxBandwidthMBps=0) are in range, so
    // validate() must return normally.
    configWith().validate()
  }

  test("validate accepts the exact lower range bounds") {
    configWith(
      "spark.shuffle.streaming.bufferSizePercent" -> "1",
      "spark.shuffle.streaming.spillThreshold" -> "50",
      "spark.shuffle.streaming.maxBandwidthMBps" -> "0").validate()
  }

  test("validate accepts the exact upper range bounds") {
    configWith(
      "spark.shuffle.streaming.bufferSizePercent" -> "50",
      "spark.shuffle.streaming.spillThreshold" -> "95",
      "spark.shuffle.streaming.maxBandwidthMBps" -> "1000").validate()
  }

  // -------------------------------------------------------------------------------------------
  // validate(): reject just outside the bounds.
  // -------------------------------------------------------------------------------------------

  test("validate rejects bufferSizePercent of 0 (below the [1, 50] range)") {
    intercept[IllegalArgumentException] {
      configWith("spark.shuffle.streaming.bufferSizePercent" -> "0").validate()
    }
  }

  test("validate rejects bufferSizePercent of 51 (above the [1, 50] range)") {
    intercept[IllegalArgumentException] {
      configWith("spark.shuffle.streaming.bufferSizePercent" -> "51").validate()
    }
  }

  test("validate rejects spillThreshold of 49 (below the [50, 95] range)") {
    intercept[IllegalArgumentException] {
      configWith("spark.shuffle.streaming.spillThreshold" -> "49").validate()
    }
  }

  test("validate rejects spillThreshold of 96 (above the [50, 95] range)") {
    intercept[IllegalArgumentException] {
      configWith("spark.shuffle.streaming.spillThreshold" -> "96").validate()
    }
  }

  test("validate rejects a negative maxBandwidthMBps") {
    // maxBandwidthMBps has no ConfigEntry checkValue (0 is the unlimited sentinel, no upper bound),
    // so validate()'s own require is the sole guard that rejects a negative budget.
    intercept[IllegalArgumentException] {
      configWith("spark.shuffle.streaming.maxBandwidthMBps" -> "-1").validate()
    }
  }

  test("maxBandwidthMBps accessor fails fast on a negative budget") {
    val conf = configWith("spark.shuffle.streaming.maxBandwidthMBps" -> "-5")
    // The accessor is the single non-bypassable guard: reading a negative value throws directly,
    // so a negative budget can never silently propagate into the rate computation.
    intercept[IllegalArgumentException] {
      conf.maxBandwidthMBps
    }
  }

  // -------------------------------------------------------------------------------------------
  // Defaults, dual activation gate, and effective-bandwidth division.
  // -------------------------------------------------------------------------------------------

  test("accessors expose the documented defaults") {
    val conf = configWith()
    assert(!conf.enabled)
    assert(conf.bufferSizePercent == 20)
    assert(conf.spillThreshold == 80)
    assert(conf.maxBandwidthMBps == 0)
    assert(!conf.debug)
    assert(conf.shuffleManager == "sort")
  }

  test("isStreamingActive requires manager=streaming AND enabled=true (dual gate)") {
    assert(configWith(
      "spark.shuffle.manager" -> "streaming",
      "spark.shuffle.streaming.enabled" -> "true").isStreamingActive)
    // Manager match is case-insensitive.
    assert(configWith(
      "spark.shuffle.manager" -> "STREAMING",
      "spark.shuffle.streaming.enabled" -> "true").isStreamingActive)
    // Any single surface off leaves the production-stable sort path active.
    assert(!configWith(
      "spark.shuffle.manager" -> "streaming",
      "spark.shuffle.streaming.enabled" -> "false").isStreamingActive)
    assert(!configWith(
      "spark.shuffle.manager" -> "sort",
      "spark.shuffle.streaming.enabled" -> "true").isStreamingActive)
    assert(!configWith(
      "spark.shuffle.manager" -> "sort",
      "spark.shuffle.streaming.enabled" -> "false").isStreamingActive)
  }

  test("effectiveBandwidthMBps divides the link budget across concurrent shuffles") {
    val conf = configWith("spark.shuffle.streaming.maxBandwidthMBps" -> "100")
    assert(conf.effectiveBandwidthMBps(1) == 100)
    assert(conf.effectiveBandwidthMBps(4) == 25)
    // Integer division intentionally discards the remainder to stay at or under the link budget.
    assert(conf.effectiveBandwidthMBps(3) == 33)
    // Non-positive concurrency is treated as 1 to avoid a divide-by-zero.
    assert(conf.effectiveBandwidthMBps(0) == 100)
    assert(conf.effectiveBandwidthMBps(-2) == 100)
  }

  test("effectiveBandwidthMBps propagates the unlimited (0) sentinel regardless of concurrency") {
    val conf = configWith("spark.shuffle.streaming.maxBandwidthMBps" -> "0")
    assert(conf.effectiveBandwidthMBps(1) == 0)
    assert(conf.effectiveBandwidthMBps(8) == 0)
  }
}
