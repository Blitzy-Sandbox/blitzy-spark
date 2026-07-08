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
   * Whether this transport can actually stream framed blocks producer-to-consumer over the wire.
   *
   * In v1 this is '''`false`''': [[send]] is a logging-only stub that never puts bytes on the wire
   * (see the class Scaladoc and Architectural Decision Log #2). The streaming manager consults this
   * flag as the authoritative capability gate: while it is `false`, `StreamingShuffleManager`
   * '''must''' route every production shuffle through the inner `SortShuffleManager` so that
   * shuffle output is always durably materialized and reducer-fetchable through the
   * production-stable sort path. This is what makes the manager's "force sort fallback in v1"
   * behavior honest and testable rather than reporting a successful `MapStatus` for bytes that
   * were never transferred.
   *
   * When the v2 wire path lands (chunking a [[StreamingBlockEnvelope]] over the reused
   * `BlockTransferService`, applying the token-bucket rate limit, and verifying the read-side
   * CRC32C), this becomes `true` and the manager's per-shuffle streaming-eligibility check begins
   * to take effect. It is a stable, immutable capability constant for the lifetime of the transport
   * (configuration changes require an executor restart), so callers may read it without locking.
   */
  val isWireTransferAvailable: Boolean = false

  /**
   * Exponential-backoff retry policy governing producer-connection failures on the send path.
   *
   * [[send]] routes through `retryPolicy.withRetry`, so when the v2 wire transport raises a
   * transient connection failure the block transfer is retried on the mandated schedule (1 s
   * start, doubling, up to 5 attempts) before the failure is surfaced and the reduce-side read
   * turns it into a [[org.apache.spark.shuffle.FetchFailedException]] for DAG recomputation. In v1
   * the send body is a logging-only stub that never raises a retriable failure, so `withRetry`
   * executes it exactly once with no backoff sleeps and no behavioral change. Exposed to the
   * streaming package so tests can assert the wiring and the retry contract.
   */
  private[streaming] val retryPolicy: StreamingShuffleRetryPolicy =
    new StreamingShuffleRetryPolicy()

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
   * Ship a framed block to its consumer, retrying transient producer-connection failures with
   * exponential backoff (see [[retryPolicy]]).
   *
   * The actual transfer is delegated to [[sendOnce]] through `retryPolicy.withRetry`, which retries
   * only non-fatal [[java.io.IOException]]-family failures on the mandated 1 s / doubling / max-5
   * schedule and re-throws anything else (or the final failure) to the caller. In v1 [[sendOnce]]
   * is a logging-only stub that never raises such a failure, so `withRetry` invokes it exactly once
   * with no backoff sleeps -- the retry loop only turns over once the v2 wire transport can raise a
   * real connection failure. Actual wire streaming is a v2 concern (see the class Scaladoc); until
   * then the manager's fallback delegates real traffic to the sort path.
   *
   * @param envelope    the framed block (fixed 32-byte header + &le;2 MB CRC32C-checked payload)
   * @param destination the consumer's block-manager location, when known to the caller
   */
  def send(
      envelope: StreamingBlockEnvelope,
      destination: Option[BlockManagerId] = None): Unit = {
    retryPolicy.withRetry(StreamingShuffleRetryPolicy.isRetriableConnectionFailure) {
      sendOnce(envelope, destination)
    }
  }

  /**
   * Perform a single (non-retried) send attempt.
   *
   * When `spark.shuffle.streaming.debug` is enabled this logs the transfer it would perform over
   * the reused `BlockTransferService`; otherwise it is a no-op. It never opens a new transport and
   * never blocks. This is the v1 logging-only stub body invoked (once) by [[send]] via the retry
   * policy; the v2 wire transport will replace the body here with the real chunked, rate-limited
   * transfer that can raise the retriable connection failures [[send]] is prepared to retry.
   *
   * @param envelope    the framed block (fixed 32-byte header + &le;2 MB CRC32C-checked payload)
   * @param destination the consumer's block-manager location, when known to the caller
   */
  private def sendOnce(
      envelope: StreamingBlockEnvelope,
      destination: Option[BlockManagerId]): Unit = {
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

  /**
   * The TCP keepalive probe interval the v2 wire path applies to its producer-to-consumer
   * connections, exposed as an instance accessor for symmetry with the other transport tunables.
   * Delegates to the companion constant [[StreamingShuffleTransport.TCP_KEEPALIVE_INTERVAL_MS]];
   * see that constant for the full rationale and the v1-vs-v2 coexistence semantics.
   */
  def tcpKeepAliveIntervalMs: Long = StreamingShuffleTransport.TCP_KEEPALIVE_INTERVAL_MS
}

/**
 * Companion object holding the transport-layer protocol constants for streaming shuffle.
 */
@Since("4.2.0")
private[spark] object StreamingShuffleTransport {

  /**
   * TCP keepalive probe interval, in milliseconds, for streaming-shuffle producer-to-consumer
   * connections (AAP network discipline: "TCP keepalive enabled with 5-second interval").
   *
   * ==Coexistence semantics (v1 vs. v2)==
   * In v1 this class is a logging-only stub that never opens a socket (see
   * [[StreamingShuffleTransport.isWireTransferAvailable]] `== false`), so there is no streaming
   * connection on which to set a keepalive option yet. Any real traffic in v1 flows through the
   * reused executor-scoped `BlockTransferService`, whose Netty channels already honor Spark's
   * existing keepalive ''enablement'' switch `spark.<module>.io.enableTcpKeepAlive` via
   * `org.apache.spark.network.util.TransportConf.enableTcpKeepAlive` -- so v1 inherits keepalive
   * behavior from the existing transport rather than introducing a parallel one, consistent with
   * the feature's "least modification to the network transport layer" discipline (Architectural
   * Decision Log #2).
   *
   * When the v2 wire path lands (chunking a [[StreamingBlockEnvelope]] over the reused transport
   * and verifying the read-side CRC32C), it applies this 5-second probe interval to the streaming
   * connections it manages, keeping the mandated keepalive cadence explicit and colocated with the
   * other streaming protocol constants (5 s producer timeout, 10 s consumer heartbeat, 2 MB block
   * size). It is a stable, immutable constant for the lifetime of the transport (configuration
   * changes require an executor restart), so callers may read it without locking.
   */
  val TCP_KEEPALIVE_INTERVAL_MS: Long = 5000L
}
