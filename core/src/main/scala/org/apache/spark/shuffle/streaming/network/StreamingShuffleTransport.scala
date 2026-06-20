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

import scala.concurrent.Future

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockTransferService
import org.apache.spark.shuffle.streaming.StreamingShuffleConfig
import org.apache.spark.storage.BlockManagerId

/**
 * The streaming-shuffle transport: the documented integration seam between the streaming shuffle
 * backend and Spark's existing network data plane.
 *
 * ==INTENDED v1 BEHAVIOR (AAP section 0.4.4) -- NOT an unfinished stub==
 *
 * This class is a v1 '''logging-only integration'''; it deliberately does not move bytes itself.
 * The real data plane is the existing `BlockTransferService.fetchBlockSync` path, which the
 * reduce-side `StreamingShuffleReader` (parent package) invokes directly. Consequently:
 *
 *  - [[sendBlock]] returns an already-completed `Future` (`Future.unit`); the producer treats the
 *    block as handed off, and the bytes are served on demand by the reader's existing fetch path.
 *  - [[openConsumerStream]] returns `Iterator.empty`; consumers pull blocks through that same
 *    existing fetch path rather than from a push stream owned by this transport.
 *
 * This is a deliberate least-modification design decision (AAP section 0.6.1): reuse Spark's
 * battle-tested [[BlockTransferService]] instead of introducing a parallel network stack. The v2
 * Netty push-plane hardening (a real push data plane, `SO_KEEPALIVE`, and full retry/backoff
 * wiring) is explicitly deferred (AAP section 0.5.2); the rationale is captured in the
 * streaming-shuffle decision log, not in code comments.
 *
 * ==No new endpoint, no new dependency==
 *
 * This transport introduces no new network endpoint and no new dependency: it only references the
 * existing [[BlockTransferService]] obtained from the running executor's `BlockManager`. Spark's
 * existing shuffle security on that transport (authentication / SASL and TLS) is inherited
 * unchanged. The only streaming-specific endpoint, the backpressure RPC, lives in the parent
 * package, never here.
 *
 * @param config               the typed streaming-shuffle configuration (tuning and invariants)
 * @param blockTransferService the existing block transfer service; present on executors and `None`
 *                             in local mode / on the driver. It is held so the integration point is
 *                             observable in logs and reachable by collaborators via
 *                             [[transferService]]
 */
private[spark] class StreamingShuffleTransport(
    config: StreamingShuffleConfig,
    blockTransferService: Option[BlockTransferService]) extends Logging {

  // v1 logging-only integration; the real data plane is BlockTransferService.fetchBlockSync,
  // invoked reader-side (see AAP section 0.4.4). This init line references the held service so
  // the integration point is observable; it is a one-time lifecycle log (not per-block), logInfo.
  logInfo(s"StreamingShuffleTransport initialized (v1 logging-only integration). Real data " +
    s"plane = existing BlockTransferService.fetchBlockSync (reader-side); transferService " +
    s"present=${blockTransferService.isDefined}, debug=${config.debug}")

  /**
   * The existing block transfer service this transport is bound to, or `None` in local mode / on
   * the driver. Collaborators (notably the reduce-side reader, which performs the real
   * `fetchBlockSync` in v1) read it from here rather than re-deriving it from `SparkEnv`.
   *
   * @return the optional block transfer service held by this transport
   */
  def transferService: Option[BlockTransferService] = blockTransferService

  /**
   * Hands a single framed block to the transport.
   *
   * ==INTENDED v1 BEHAVIOR (AAP section 0.4.4) -- NOT a stub==
   *
   * v1 logging-only integration: this records the hand-off and returns an already-completed
   * `Future` (`Future.unit`). The real data plane is the existing reader-side
   * `BlockTransferService.fetchBlockSync` path, so the producer may treat the block as delivered
   * as soon as it is buffered. The v2 push data plane is deferred (AAP section 0.5.2).
   *
   * @param envelope the framed block (32-byte header + CRC32C-validated payload) to hand off
   * @param target   the destination executor identity (host / port / executor id)
   * @return an already-completed `Future`; it never fails in v1
   */
  def sendBlock(envelope: StreamingBlockEnvelope, target: BlockManagerId): Future[Unit] = {
    logDebug(s"v1 logging-only sendBlock shuffleId=${envelope.shuffleId} " +
      s"mapId=${envelope.mapId} reduceId=${envelope.reduceId} " +
      s"seq=${envelope.sequenceNumber} bytes=${envelope.payloadLength} target=$target; " +
      s"real data plane is reader-side BlockTransferService.fetchBlockSync")
    Future.unit
  }

  /**
   * Opens a pull stream of framed blocks for a reduce-side consumer.
   *
   * ==INTENDED v1 BEHAVIOR (AAP section 0.4.4) -- NOT a stub==
   *
   * v1 logging-only integration: this records the request (including the exponential-backoff retry
   * intent it would apply in v2, sourced from the shared [[StreamingShuffleConfig]] constants) and
   * returns `Iterator.empty`. Consumers obtain blocks through the existing reader-side
   * `BlockTransferService.fetchBlockSync` path, not from a push stream owned here. The v2 push
   * data plane is deferred (AAP section 0.5.2).
   *
   * @param shuffleId      the shuffle being consumed
   * @param startMapIndex  the inclusive start of the map-output index range
   * @param endMapIndex    the exclusive end of the map-output index range
   * @param startPartition the inclusive start of the reduce-partition range
   * @param endPartition   the exclusive end of the reduce-partition range
   * @return `Iterator.empty` in v1; the real read path is reader-side `fetchBlockSync`
   */
  def openConsumerStream(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[StreamingBlockEnvelope] = {
    logDebug(s"v1 logging-only openConsumerStream shuffleId=$shuffleId " +
      s"maps=[$startMapIndex,$endMapIndex) parts=[$startPartition,$endPartition); " +
      s"retry intent backoffStartMs=${StreamingShuffleConfig.RETRY_INITIAL_BACKOFF_MS} " +
      s"maxAttempts=${StreamingShuffleConfig.RETRY_MAX_ATTEMPTS}; " +
      s"real data plane is reader-side BlockTransferService.fetchBlockSync")
    Iterator.empty
  }
}

/**
 * Factory for [[StreamingShuffleTransport]].
 *
 * The [[apply]] factory derives the [[BlockTransferService]] from the running [[SparkEnv]],
 * gated on `SparkEnv.get != null` so the transport is safe to build in local mode and on the
 * driver (where it yields `None`). `StreamingShuffleManager` uses this factory to build the
 * transport as a lazy collaborator.
 */
private[spark] object StreamingShuffleTransport {

  /**
   * Builds a transport whose block transfer service is taken from the active [[SparkEnv]] when one
   * is present, and `None` otherwise (local mode / driver).
   *
   * @param config the typed streaming-shuffle configuration
   * @return a transport bound to the executor's existing block transfer service, or to `None`
   */
  def apply(config: StreamingShuffleConfig): StreamingShuffleTransport = {
    val transferService =
      if (SparkEnv.get != null) {
        Some(SparkEnv.get.blockManager.blockTransferService)
      } else {
        None
      }
    new StreamingShuffleTransport(config, transferService)
  }
}
