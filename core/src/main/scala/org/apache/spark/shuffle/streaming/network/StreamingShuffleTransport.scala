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

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockTransferService
import org.apache.spark.shuffle.streaming.StreamingShuffleConfig
import org.apache.spark.storage.BlockManagerId

/**
 * Transport integration for streaming shuffle.
 *
 * ==Coexistence strategy==
 * This class deliberately REUSES the executor-scoped `BlockTransferService` obtained from the
 * running `SparkEnv` (`SparkEnv.get.blockManager.blockTransferService`) instead of instantiating a
 * new `org.apache.spark.network.TransportContext`. Reusing the existing transport honors the
 * feature's "least modification to the network transport layer" discipline and inherits the
 * cluster's authentication (`spark.authenticate`), SASL, and TLS settings for free (Architectural
 * Decision Log #2). No new socket stack, port, or transport thread pool is created here.
 *
 * ==v1 scope (this class) and the v2 plan==
 * In v1 this is a LOGGING-ONLY STUB: [[send]] records the block it ''would'' transfer (only when
 * `spark.shuffle.streaming.debug` is enabled) and returns without putting any bytes on the wire.
 * The end-to-end wire streaming -- chunking a [[StreamingBlockEnvelope]] over the reused
 * `BlockTransferService`, applying the token-bucket rate limit, and verifying the CRC32C on the
 * read side -- is deferred to v2. Because v1 does not stream over the wire, the streaming manager's
 * fallback policy routes real traffic through the inner `SortShuffleManager`, so the sort path
 * stays the production-stable default and guarantees zero regression while the transport matures.
 *
 * ==Isolation==
 * All logic stays inside the streaming `network` subpackage; nothing here is injected into the
 * existing shuffle code paths, and this class has no effect on the sort path other than being the
 * component the manager bypasses (via fallback) until v2 lands.
 *
 * @param conf typed streaming-shuffle configuration; used here to gate debug logging
 */
@Since("4.2.0")
private[spark] class StreamingShuffleTransport(conf: StreamingShuffleConfig) extends Logging {

  /**
   * Resolve the executor-scoped [[BlockTransferService]] from the active `SparkEnv`, reusing the
   * existing transport rather than creating a new one. Returns `None` in local/driver-only or test
   * contexts where `SparkEnv` (or its `BlockManager`) is not yet initialized, keeping construction
   * and [[send]] safe in local mode.
   */
  private def resolveTransferService(): Option[BlockTransferService] = {
    val env = SparkEnv.get
    if (env != null && env.blockManager != null) {
      Some(env.blockManager.blockTransferService)
    } else {
      None
    }
  }

  /**
   * v1 logging-only stub for shipping a framed block to its consumer.
   *
   * When `spark.shuffle.streaming.debug` is enabled this logs the transfer it would perform over
   * the reused `BlockTransferService`; otherwise it is a no-op. It never opens a new transport and
   * never blocks. Actual wire streaming is a v2 concern (see the class Scaladoc); until then the
   * manager's fallback delegates real traffic to the sort path.
   *
   * @param envelope    the framed block (fixed 32-byte header + &le;2 MB CRC32C-checked payload)
   * @param destination the consumer's block-manager location, when known to the caller
   */
  def send(
      envelope: StreamingBlockEnvelope,
      destination: Option[BlockManagerId] = None): Unit = {
    if (conf.debug) {
      val dest = destination.map(_.toString).getOrElse("<consumer-driven pull>")
      val blockDesc = s"shuffle=${envelope.shuffleId} map=${envelope.mapId} " +
        s"reduce=${envelope.reduceId} seq=${envelope.sequenceNumber} " +
        s"bytes=${envelope.payloadLength}"
      resolveTransferService() match {
        case Some(service) =>
          logDebug(s"streaming-shuffle transport (v1 stub) would send block [$blockDesc] " +
            s"to $dest via reused ${service.getClass.getSimpleName}; no bytes transferred in v1")
        case None =>
          logDebug(s"streaming-shuffle transport (v1 stub): no BlockTransferService " +
            s"available (SparkEnv uninitialized); skipping block [$blockDesc]")
      }
    }
  }
}
