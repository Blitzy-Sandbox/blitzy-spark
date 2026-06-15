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

import scala.util.Success

import org.apache.logging.log4j.Level
import org.mockito.Mockito.mock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.BlockManagerId

/**
 * Unit tests for [[StreamingShuffleTransport]] -- the v1 logging-only integration seam between the
 * opt-in streaming-shuffle backend and Spark's existing network data plane.
 *
 * These tests lock in the DOCUMENTED v1 contract (AAP 0.4.4): because the real data plane is the
 * existing reduce-side [[org.apache.spark.network.BlockTransferService.fetchBlockSync]] pull path,
 * `sendBlock` returns an already-completed `Future` and `openConsumerStream` returns an empty
 * iterator. Asserting that intended behavior here guards against a future change silently turning
 * the seam into a half-implemented push plane, and it documents (in executable form) that the v1
 * stub is by design, not an unfinished implementation. The suite is pure and deterministic: it
 * needs no `SparkContext`, no `MetricsSystem`, and no RPC environment.
 */
class StreamingShuffleTransportSuite extends SparkFunSuite with Matchers {

  /** A defaults-only typed configuration accessor; no streaming overrides are needed here. */
  private def newConfig(): StreamingShuffleConfig =
    new StreamingShuffleConfig(new SparkConf(false))

  test("v1 sendBlock returns an already-completed Future (logging-only data plane)") {
    val transport = new StreamingShuffleTransport(newConfig(), None)
    val envelope = StreamingBlockEnvelope.create(1, 0L, 2, 7L, Array.fill[Byte](64)(3.toByte))
    val target = BlockManagerId("exec-1", "host-1", 7337)

    // v1 never blocks or fails: the producer treats the send as immediately complete because the
    // bytes are actually served by the reader-side fetch path.
    val future = transport.sendBlock(envelope, target)
    future.isCompleted mustBe true
    future.value mustBe Some(Success(()))
  }

  test("v1 openConsumerStream returns an empty iterator (reader uses the existing fetch path)") {
    val transport = new StreamingShuffleTransport(newConfig(), None)

    // The reduce side reads through BlockTransferService.fetchBlockSync, so the v1 push-side
    // consumer stream is intentionally empty for any map/partition range.
    val stream = transport.openConsumerStream(
      shuffleId = 3,
      startMapIndex = 0,
      endMapIndex = 4,
      startPartition = 1,
      endPartition = 2)
    stream.hasNext mustBe false
    stream.isEmpty mustBe true
  }

  test("transferService accessor reflects the bound service") {
    // No env / local mode: the transport holds no service.
    new StreamingShuffleTransport(newConfig(), None).transferService mustBe None

    // When an executor service is supplied it is exposed unchanged so the manager/reader can reach
    // the same instance used by the real fetch path.
    val svc = mock(classOf[BlockTransferService])
    val bound = new StreamingShuffleTransport(newConfig(), Some(svc))
    bound.transferService mustBe Some(svc)
  }

  test("apply factory builds a transport even when no active SparkEnv is present") {
    // The factory is gated on SparkEnv.get != null so the manager can build the transport safely
    // in local mode and in tests with no env; the accessor must never throw in that case.
    val transport = StreamingShuffleTransport(newConfig())
    noException must be thrownBy transport.transferService
  }

  test("v1 methods emit MDC-tagged debug correlation logging when debug is enabled") {
    val transport = new StreamingShuffleTransport(newConfig(), None)
    val envelope = StreamingBlockEnvelope.create(11, 5L, 3, 9L, Array.fill[Byte](128)(1.toByte))
    val target = BlockManagerId("exec-7", "host-7", 7337)

    // The v1 seam records correlation context at DEBUG only, so the structured (MDC-tagged)
    // message closures are skipped unless the transport's own logger is at DEBUG. Capturing those
    // events both exercises that correlation-logging path and proves the MDC keys render without
    // error. LogAppender's internal threshold defaults to INFO, so it is lowered to DEBUG too.
    val appender = new LogAppender("streaming transport debug logging")
    appender.setThreshold(Level.DEBUG)
    withLogAppender(
        appender,
        loggerNames =
          Seq("org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport"),
        level = Some(Level.DEBUG)) {
      transport.sendBlock(envelope, target)
      transport.openConsumerStream(
        shuffleId = 11,
        startMapIndex = 0,
        endMapIndex = 2,
        startPartition = 3,
        endPartition = 5)
    }

    val messages = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(messages.exists(_.contains("v1 logging-only sendBlock")))
    assert(messages.exists(_.contains("v1 logging-only openConsumerStream")))
  }
}
