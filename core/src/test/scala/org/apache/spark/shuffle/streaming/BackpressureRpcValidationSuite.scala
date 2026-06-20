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

import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.shuffle.streaming.BackpressureRpcEndpoint.{Ack, Heartbeat, RateLimitRequest,
  Timeout, validate}

/**
 * Unit tests for [[BackpressureRpcEndpoint.validate]] -- the executor RPC channel's input-
 * validation gate. The endpoint forwards inbound control messages to [[BackpressureProtocol]],
 * whose per-stream state is created lazily via `computeIfAbsent`; without validation a crafted or
 * corrupt message carrying a negative coordinate (or a negative ack byte count) would silently
 * create bogus state or alter the executor's shared rate cap. `validate` is a pure function, so
 * these tests exercise it directly -- no [[org.apache.spark.rpc.RpcEnv]] is required.
 *
 * The accepted domain is: non-negative `shuffleId`/`mapId`/`reduceId` for every message, plus a
 * non-negative `Ack.bytesAcked`. `Heartbeat.tsNanos` is diagnostic-only and
 * `RateLimitRequest.bytesPerSec` is intentionally unrestricted (a non-positive value withdraws the
 * cap), so neither is range-checked here.
 */
class BackpressureRpcValidationSuite extends SparkFunSuite with Matchers {

  test("well-formed messages with non-negative coordinates are accepted") {
    validate(Heartbeat(0, 0L, 0, tsNanos = 123L)) mustBe None
    validate(Heartbeat(7, 42L, 3, tsNanos = System.nanoTime())) mustBe None
    validate(Ack(1, 2L, 3, bytesAcked = 0L)) mustBe None
    validate(Ack(1, 2L, 3, bytesAcked = 4096L)) mustBe None
    validate(Timeout(5, 6L, 7)) mustBe None
  }

  test("RateLimitRequest accepts both a positive cap and a non-positive withdrawal") {
    // A positive value caps the rate; a non-positive value withdraws the cap (clamped by the
    // protocol). Both are legitimate, so neither is rejected by the validation gate.
    validate(RateLimitRequest(1, 2L, 3, bytesPerSec = 1024L)) mustBe None
    validate(RateLimitRequest(1, 2L, 3, bytesPerSec = 0L)) mustBe None
    validate(RateLimitRequest(1, 2L, 3, bytesPerSec = -1L)) mustBe None
  }

  test("a negative shuffleId is rejected for every message type without mutation") {
    validate(Heartbeat(-1, 0L, 0, tsNanos = 0L)).get must include("shuffleId")
    validate(Ack(-1, 0L, 0, bytesAcked = 1L)).get must include("shuffleId")
    validate(RateLimitRequest(-1, 0L, 0, bytesPerSec = 1L)).get must include("shuffleId")
    validate(Timeout(-1, 0L, 0)).get must include("shuffleId")
  }

  test("a negative mapId is rejected") {
    validate(Heartbeat(0, -1L, 0, tsNanos = 0L)).get must include("mapId")
    validate(Ack(0, -5L, 0, bytesAcked = 1L)).get must include("mapId")
  }

  test("a negative reduceId is rejected") {
    validate(Heartbeat(0, 0L, -1, tsNanos = 0L)).get must include("reduceId")
    validate(RateLimitRequest(0, 0L, -9, bytesPerSec = 1L)).get must include("reduceId")
  }

  test("a negative Ack.bytesAcked is rejected as an impossible value") {
    // A consumer cannot acknowledge a negative number of bytes; the coordinates are valid here, so
    // the violation must be attributed specifically to the byte count.
    validate(Ack(1, 2L, 3, bytesAcked = -1L)).get must include("bytesAcked")
  }

  test("coordinate violations are reported before the per-message byte check") {
    // When both a coordinate and the byte count are invalid, the coordinate (the state-creation
    // risk) is reported first so the rejection reason is deterministic.
    validate(Ack(-1, 0L, 0, bytesAcked = -1L)).get must include("shuffleId")
  }
}
