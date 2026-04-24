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

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.storage.{BlockManager, BlockManagerId}

/**
 * Thin wrapper around the executor-scoped Netty transport layer used for streaming shuffle.
 *
 * This class is part of the Streaming Shuffle feature (F-001) &mdash; an opt-in, coexisting
 * alternative to the sort-based shuffle that is selected only when
 * `spark.shuffle.manager=streaming`. The sort path is the default and remains the
 * production-stable fallback; this transport is loaded only when the streaming manager is
 * active.
 *
 * Coexistence strategy:
 *   - OBTAINS the pre-existing executor-scoped
 *     [[org.apache.spark.network.BlockTransferService]] from [[BlockManager]] (via
 *     `BlockManager.blockTransferService`); does NOT instantiate a new
 *     [[org.apache.spark.network.TransportContext]]. This inherits `spark.authenticate`,
 *     SASL, and TLS protection from the already-authenticated transport with zero new
 *     security wiring.
 *   - DOES NOT add, remove, or reorder any member on the existing
 *     [[org.apache.spark.network.BlockTransferService]] public surface.
 *   - DOES NOT participate in the External Shuffle Service (ESS) protocol on port 7337 for
 *     in-progress reads; ESS serves only materialized, index-committed blocks from the sort
 *     path. Streaming reads target in-progress blocks on producer executors directly.
 *
 * Netty OOM protection:
 *   The global `ShuffleBlockFetcherIterator.isNettyOOMOnShuffle` `AtomicBoolean` (ADR-004)
 *   is declared with `private[storage]` visibility and is NOT directly referenceable from
 *   `org.apache.spark.shuffle.streaming.network`. For v1, this transport uses
 *   [[org.apache.spark.network.util.NettyUtils#freeDirectMemory]] as a proximate guard and
 *   logs a warning if available direct memory falls below the pending payload size. The
 *   real transport in v2 will add explicit backoff when the check fails.
 *
 * v1 implementation:
 *   The writer/reader integration is a two-phase rollout (see
 *   `blitzy-docs/streaming-shuffle-decision-log.md`). The v1 transport is a LOGGING-ONLY
 *   STUB: [[sendBlock]] acquires rate-limiter tokens, logs the envelope metadata, and
 *   returns a completed [[scala.concurrent.Future]]; [[openConsumerStream]] logs the
 *   request and returns [[scala.collection.Iterator#empty]]. This lets the rest of the
 *   streaming feature (manager, writer, reader, backpressure, spill manager) compile and
 *   boot without data-plane functionality. The real Netty-based transport is deferred to
 *   v2 where outbound channels will set
 *   [[io.netty.channel.ChannelOption#SO_KEEPALIVE]] `= true` with a 5-second interval (per
 *   user spec) and will retry with exponential backoff starting at 1 s, max 5 attempts.
 *
 * @param conf            The Spark configuration carrying `spark.shuffle.streaming.*` keys.
 * @param blockManager    The executor-scoped block manager providing
 *                        `blockTransferService` and `shuffleServerId`.
 * @param securityManager The executor security manager; retained for future use in
 *                        explicit channel authentication hooks.
 */
private[spark] class StreamingShuffleTransport(
    conf: SparkConf,
    blockManager: BlockManager,
    securityManager: SecurityManager) extends Logging {

  import StreamingShuffleTransport._

  /** Cached max-bandwidth budget in MB/s. A value `<= 0` means unlimited. */
  private val maxBandwidthMBps: Int = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)

  /**
   * Token-bucket rate limiter applied before every [[sendBlock]] transmission.
   *
   * Initial rate: `maxBandwidthMBps * 1_048_576 bytes/sec * 0.80` (link-capacity cap). When
   * `maxBandwidthMBps <= 0`, the rate limiter treats transmissions as unlimited. The
   * concurrent shuffle count is updated at runtime via
   * [[TokenBucketRateLimiter.updateRate]] by the backpressure coordinator as new shuffles
   * register and unregister.
   */
  private val rateLimiter: TokenBucketRateLimiter = {
    val initialRate =
      if (maxBandwidthMBps <= 0) 0.0
      else maxBandwidthMBps.toLong * BYTES_PER_MB.toDouble * LINK_CAPACITY_FACTOR
    new TokenBucketRateLimiter(initialRate)
  }

  {
    val localServer = blockManager.shuffleServerId
    val host = if (localServer != null) localServer.host else "<unbound>"
    val port = if (localServer != null) localServer.port else -1
    val hostPort = s"$host:$port"
    val bandwidthBytes = if (maxBandwidthMBps <= 0) 0L
      else maxBandwidthMBps.toLong * BYTES_PER_MB
    logInfo(log"StreamingShuffleTransport v1 stub initialized on executor " +
      log"${MDC(HOST_PORT, hostPort)} with maxBandwidth=" +
      log"${MDC(NUM_BYTES, bandwidthBytes)} bytes/s (0 = unlimited).")
  }

  /**
   * Ships one [[StreamingBlockEnvelope]] to the target consumer executor.
   *
   * Contract:
   *   1. Blocks until the [[rateLimiter]] permits the payload's byte cost (honors the 80%
   *      link-capacity cap). On unlimited rate, returns immediately.
   *   2. Checks [[org.apache.spark.network.util.NettyUtils#freeDirectMemory]] and logs a
   *      warning if available direct memory is below the pending payload size.
   *   3. v1: logs the envelope metadata at DEBUG and returns [[Future#successful]] (no
   *      data plane).
   *   4. v2 (deferred): serialises via [[StreamingBlockEnvelope#toByteBuf]], writes to the
   *      target's channel with
   *      [[io.netty.channel.ChannelOption#SO_KEEPALIVE]] `= true`, retries with exponential
   *      backoff on transient failures (start 1 s, max 5 attempts).
   *
   * @param target The consumer's [[BlockManagerId]].
   * @param env    The envelope to transmit. Payload must be &le; 2 MB per the streaming
   *               block-size constraint; that invariant is enforced by
   *               [[StreamingBlockEnvelope#toByteBuf]].
   * @return A [[Future]] that completes when the block has been acknowledged by the
   *         consumer (in v2) or is immediately completed (in v1 stub).
   */
  def sendBlock(target: BlockManagerId, env: StreamingBlockEnvelope): Future[Unit] = {
    // 1. Rate-limit. acquire(permits) requires permits > 0; guard against empty payloads.
    val permits = math.max(1, env.payload.length)
    rateLimiter.acquire(permits)

    // 2. Netty direct-memory guard (ADR-004). We cannot reference the private[storage]
    //    ShuffleBlockFetcherIterator.isNettyOOMOnShuffle AtomicBoolean from this package,
    //    so we consult NettyUtils.freeDirectMemory() as a proximate check and only warn
    //    in v1.
    val freeDirect = NettyUtils.freeDirectMemory()
    if (freeDirect >= 0L && freeDirect < env.payload.length.toLong) {
      logWarning(log"Netty direct memory low before streaming send: free=" +
        log"${MDC(NUM_BYTES, freeDirect)} bytes, required=" +
        log"${MDC(COUNT, env.payload.length)} bytes.")
    }

    // 3. v1 STUB: log the envelope metadata at DEBUG and return a completed future.
    val targetHostPort = s"${target.host}:${target.port}"
    logDebug(log"[v1 stub] sendBlock shuffleId=${MDC(SHUFFLE_ID, env.shuffleId)} " +
      log"mapId=${MDC(MAP_ID, env.mapId)} reduceId=${MDC(REDUCE_ID, env.reduceId)} " +
      log"target=${MDC(HOST_PORT, targetHostPort)} " +
      log"bytes=${MDC(NUM_BYTES, env.payload.length)} " +
      log"checksum=${MDC(CHECKSUM, env.checksum)}")

    Future.successful(())
  }

  /**
   * Opens a consumer-side iterator that receives in-progress envelopes from the named
   * producer.
   *
   * Contract:
   *   1. v1: logs the request and returns [[scala.collection.Iterator#empty]]. The sibling
   *      [[org.apache.spark.shuffle.streaming.StreamingShuffleReader]] (v1) returns empty
   *      results, so the streaming read path is a no-op end-to-end in v1.
   *   2. v2 (deferred): establishes a Netty channel to the producer with
   *      [[io.netty.channel.ChannelOption#SO_KEEPALIVE]] `= true` (5-second interval per
   *      user spec), registers as a consumer with the producer's streaming endpoint, and
   *      delivers envelopes through a bounded queue; on connection timeout (5 s), the
   *      iterator terminates so the reader can invalidate partial reads and trigger
   *      upstream recomputation via the existing DAG scheduler.
   *
   * @param producer    The [[BlockManagerId]] of the upstream producer executor.
   * @param shuffleId   The shuffle identifier being consumed.
   * @param reduceRange The inclusive range of reducer partition ids requested.
   * @return An iterator of [[StreamingBlockEnvelope]] instances delivered as they are
   *         produced by the upstream map tasks.
   */
  def openConsumerStream(
      producer: BlockManagerId,
      shuffleId: Int,
      reduceRange: Range): Iterator[StreamingBlockEnvelope] = {
    val producerHostPort = s"${producer.host}:${producer.port}"
    logDebug(log"[v1 stub] openConsumerStream shuffleId=${MDC(SHUFFLE_ID, shuffleId)} " +
      log"producer=${MDC(HOST_PORT, producerHostPort)} " +
      log"reduceCount=${MDC(COUNT, reduceRange.size)}")
    Iterator.empty[StreamingBlockEnvelope]
  }

  /**
   * Exposes the rate limiter so the backpressure coordinator can adjust its rate when the
   * concurrent-shuffle count changes.
   */
  def getRateLimiter: TokenBucketRateLimiter = rateLimiter

  /**
   * Releases any transport-owned resources. v1 is a no-op because the v1 stub holds no
   * channels, buffers, or schedulers; v2 will close channels and cancel schedulers created
   * lazily.
   */
  def close(): Unit = {
    logInfo("StreamingShuffleTransport v1 stub closed.")
  }
}

private[spark] object StreamingShuffleTransport {
  /** TCP keepalive interval in seconds, per user specification. Applied by the v2
   * implementation when creating outbound channels. */
  val TCP_KEEPALIVE_INTERVAL_SEC: Int = 5

  /** Maximum retry attempts on transient transport failure, per user specification. */
  val MAX_RETRY_ATTEMPTS: Int = 5

  /** Initial exponential backoff duration in milliseconds, per user specification. */
  val INITIAL_BACKOFF_MS: Long = 1000L

  /** Link-capacity factor (80%), per user specification: "Transfer rate dynamically
   * adjusted, capped at 80% link capacity". */
  val LINK_CAPACITY_FACTOR: Double = 0.80

  /** Bytes per MB (1024 * 1024). */
  val BYTES_PER_MB: Long = 1024L * 1024L
}
