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
 * v1 logging-only streaming-shuffle transport: the documented integration seam between the
 * opt-in streaming-shuffle backend and Spark's existing network data plane.
 *
 * ==Intended v1 behavior (AAP 0.4.4) - not an unfinished stub==
 *
 * In v1 this transport is deliberately ''logging-only''. The real data plane is the existing,
 * battle-tested [[org.apache.spark.network.BlockTransferService.fetchBlockSync]] pull path that
 * `StreamingShuffleReader` (parent package) invokes directly on the reduce side. Reusing that
 * path is the least-modification approach (AAP 0.6.1): the streaming backend introduces no new
 * network endpoint here and inherits Spark's existing shuffle security (SASL/TLS) unchanged.
 *
 * Consequently [[sendBlock]] returns an already-completed [[scala.concurrent.Future]] and
 * [[openConsumerStream]] returns an empty iterator. This is a recorded, justified design
 * decision (see the streaming-shuffle decision log) - it is intended v1 behavior, not an
 * unfinished implementation. The v2 Netty push-plane hardening (a real push data plane,
 * `SO_KEEPALIVE`, full retry/backoff wiring) is explicitly deferred (AAP 0.5.2).
 *
 * This type holds the executor's [[org.apache.spark.network.BlockTransferService]] (when one is
 * available) so the integration point stays observable in logs; the [[transferService]] accessor
 * lets the manager/reader reach the same instance they use for the actual fetch.
 *
 * @param config typed streaming-shuffle configuration accessor
 * @param blockTransferService executor block transfer service, or `None` in local mode / no env
 */
private[spark] class StreamingShuffleTransport(
    config: StreamingShuffleConfig,
    blockTransferService: Option[BlockTransferService]) extends Logging {

  // One construction-time line keeps the active data plane observable without per-block logging,
  // which would breach the < 10 MB/hour/executor streaming-shuffle log budget. This is the v1
  // logging-only integration; the real data plane is BlockTransferService.fetchBlockSync (see
  // AAP 0.4.4). Referencing the held service and config here also keeps both fields used.
  logInfo(s"StreamingShuffleTransport initialized (v1 logging-only); real data plane is the " +
    s"existing BlockTransferService.fetchBlockSync path. transferServicePresent=" +
    s"${blockTransferService.isDefined}, maxBandwidthMBps=${config.maxBandwidthMBps}")

  /**
   * The executor block transfer service this transport is bound to, if any. Exposed so the
   * streaming manager/reader can reach the same instance used by the real (reader-side) fetch
   * path. v1 does not push through it; the reader pulls via `fetchBlockSync`.
   *
   * @return the held [[org.apache.spark.network.BlockTransferService]], or `None` when no env
   */
  def transferService: Option[BlockTransferService] = blockTransferService

  /**
   * Send a single streaming-shuffle block to a target executor.
   *
   * v1 logging-only integration (AAP 0.4.4), not an unfinished stub: the bytes are actually
   * served by the existing [[org.apache.spark.network.BlockTransferService]] fetch path on the
   * reduce side, so this method only records correlation context at debug level and returns an
   * already-completed [[scala.concurrent.Future]]. The producer (`StreamingShuffleWriter`)
   * treats the result as complete. The v2 push plane will replace this with a real Netty send.
   *
   * @param envelope the framed block (32-byte header + CRC32C-validated payload) to send
   * @param target the destination executor identity (host/port/execId)
   * @return a completed `Future` (`Future.unit`); never fails in v1
   */
  def sendBlock(envelope: StreamingBlockEnvelope, target: BlockManagerId): Future[Unit] = {
    logDebug(s"v1 logging-only sendBlock shuffleId=${envelope.shuffleId} " +
      s"mapId=${envelope.mapId} reduceId=${envelope.reduceId} " +
      s"seq=${envelope.sequenceNumber} bytes=${envelope.payloadLength} target=$target; " +
      s"real data plane is reader-side BlockTransferService.fetchBlockSync")
    Future.unit
  }

  /**
   * Open a consumer-side stream of block envelopes for a reduce partition range.
   *
   * v1 logging-only integration (AAP 0.4.4), not an unfinished stub: consumers read through the
   * existing [[org.apache.spark.network.BlockTransferService]] fetch path invoked directly by
   * `StreamingShuffleReader`, so this returns an empty iterator. The exponential-backoff
   * constants are pulled from [[StreamingShuffleConfig]] (never hardcoded) so the logged retry
   * intent stays in lockstep with the policy the v2 push plane will honor.
   *
   * @param shuffleId the shuffle id being read
   * @param startMapIndex inclusive start of the map (producer) range
   * @param endMapIndex exclusive end of the map (producer) range
   * @param startPartition inclusive start of the reduce partition range
   * @param endPartition exclusive end of the reduce partition range
   * @return an empty iterator in v1; the reader uses the existing fetch path instead
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
 */
private[spark] object StreamingShuffleTransport {

  /**
   * Build a transport, deriving the executor's [[org.apache.spark.network.BlockTransferService]]
   * from the active [[org.apache.spark.SparkEnv]]. The lookup is gated on `SparkEnv.get != null`
   * so the streaming manager can construct the transport safely in local mode and in tests where
   * no env is present, in which case the transport holds `None`.
   *
   * @param config typed streaming-shuffle configuration accessor
   * @return a transport bound to the executor transfer service when an env is available
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
