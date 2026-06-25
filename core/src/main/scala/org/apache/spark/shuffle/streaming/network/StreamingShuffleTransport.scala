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

package org.apache.spark.shuffle.streaming.network

import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer

/**
 * v1 logging-only network transport for the streaming shuffle.
 *
 * This is the single, intentional logging-only stub in the streaming shuffle subtree: it
 * performs structured logging and delegation only, reusing the executor's existing
 * [[org.apache.spark.network.BlockTransferService]] and introducing no new transport context,
 * ports, or Netty bootstrap. The data-plane is deferred beyond v1; see decision log ADR-15.
 *
 * Every method below is real, side-effect-safe code: [[send]] logs the intended transmission
 * without sending bytes, [[receive]] decodes an already-delivered wire buffer, and [[fetch]]
 * performs real I/O by delegating to the executor's existing block transfer service.
 *
 * @param blockTransferService the executor's existing block transfer service that [[fetch]]
 *                             delegates single-block reads to
 */
private[spark] class StreamingShuffleTransport(blockTransferService: BlockTransferService)
  extends Logging {

  /**
   * Logs the intended send of a streaming shuffle block and returns immediately.
   *
   * v1 logging-only stub: this method does not transmit any bytes and the block contents remain
   * owned by the caller. The streaming-specific data-plane is a future-version concern.
   *
   * @param envelope the decoded block that would be streamed to the consumer
   * @param host the destination executor host
   * @param port the destination executor port
   * @param execId the destination executor id
   */
  def send(envelope: StreamingBlockEnvelope, host: String, port: Int, execId: String): Unit = {
    logDebug(
      log"[streaming-shuffle v1 stub] send shuffle=${MDC(SHUFFLE_ID, envelope.shuffleId)} " +
        log"map=${MDC(MAP_ID, envelope.mapId)} reduce=${MDC(REDUCE_ID, envelope.reduceId)} " +
        log"bytes=${MDC(NUM_BYTES, envelope.payloadLength)} " +
        log"to=${MDC(HOST_PORT, host + ":" + port)} exec=${MDC(EXECUTOR_ID, execId)}")
  }

  /**
   * Fetches a single shuffle block, blocking until it arrives.
   *
   * Delegates to the executor's existing
   * [[org.apache.spark.network.BlockTransferService.fetchBlockSync]]; the streaming-specific
   * data-plane is a future-version concern. A `null` temp-file manager is passed so the result is
   * returned as an in-memory [[org.apache.spark.network.buffer.ManagedBuffer]].
   *
   * @param host the source executor host
   * @param port the source executor port
   * @param execId the source executor id
   * @param blockId the id of the block to fetch
   * @return the fetched block as a [[org.apache.spark.network.buffer.ManagedBuffer]]
   */
  def fetch(host: String, port: Int, execId: String, blockId: String): ManagedBuffer = {
    logDebug(
      log"[streaming-shuffle v1 stub] fetch block=${MDC(BLOCK_ID, blockId)} " +
        log"from=${MDC(HOST_PORT, host + ":" + port)} exec=${MDC(EXECUTOR_ID, execId)}")
    blockTransferService.fetchBlockSync(host, port, execId, blockId, null)
  }

  /**
   * Decodes an incoming wire buffer into a [[StreamingBlockEnvelope]] and logs its header.
   *
   * v1 logging-only stub: this performs no network reads; it only parses a buffer the caller has
   * already received, validating the structural header via [[StreamingBlockEnvelope.decode]].
   *
   * @param buf a buffer positioned at the start of an encoded envelope
   * @return the decoded [[StreamingBlockEnvelope]]
   */
  def receive(buf: ByteBuffer): StreamingBlockEnvelope = {
    val envelope = StreamingBlockEnvelope.decode(buf)
    logDebug(
      log"[streaming-shuffle v1 stub] received shuffle=${MDC(SHUFFLE_ID, envelope.shuffleId)} " +
        log"map=${MDC(MAP_ID, envelope.mapId)} reduce=${MDC(REDUCE_ID, envelope.reduceId)} " +
        log"bytes=${MDC(NUM_BYTES, envelope.payloadLength)}")
    envelope
  }
}

/**
 * Factory for [[StreamingShuffleTransport]].
 */
private[spark] object StreamingShuffleTransport {

  /**
   * Creates a transport wired to the executor's existing block transfer service.
   *
   * Requires an active [[org.apache.spark.SparkEnv]] (executor runtime): it reads
   * `SparkEnv.get.blockManager.blockTransferService`. Callers without an initialized environment
   * should instead construct the class directly with an explicit block transfer service.
   *
   * @return a transport bound to the current executor's block transfer service
   */
  def apply(): StreamingShuffleTransport =
    new StreamingShuffleTransport(SparkEnv.get.blockManager.blockTransferService)
}
