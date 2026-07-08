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

import java.io.IOException

import scala.util.control.NonFatal

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging

/**
 * Exponential-backoff retry policy for streaming-shuffle producer connections.
 *
 * ==Failure-tolerance contract==
 * The streaming-shuffle failure-tolerance rule mandates that a transient producer-connection
 * failure is retried with '''exponential backoff starting at 1 second and capped at 5 total
 * attempts'''. This class is the single, isolated home for that contract. The schedule is a pure
 * function of the (1-based) attempt index:
 *
 * {{{
 *   backoffMillis(attempt) = INITIAL_BACKOFF_MS * BACKOFF_MULTIPLIER ^ (attempt - 1)
 *   //  attempt: 1      2      3      4       5
 *   //  ms:      1000   2000   4000   8000    16000
 * }}}
 *
 * [[withRetry]] executes an operation up to [[maxAttempts]] times. Between a failed attempt `i` and
 * the next attempt it sleeps `backoffMillis(i)`, so a run that exhausts the default five attempts
 * performs four backoffs (1000, 2000, 4000, 8000 ms) and re-throws the last failure to the caller.
 * Only '''retriable''' throwables (as judged by the caller-supplied classifier, and only when
 * non-fatal) are retried; a non-retriable or fatal throwable is surfaced immediately without
 * consuming further attempts, so a deterministic error (for example a serialization bug) is never
 * masked by pointless retries.
 *
 * ==v1 scope and the v2 plan==
 * This mirrors the treatment of
 * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]]: it is a fully implemented
 * and unit-tested transport primitive that is '''wired''' into [[StreamingShuffleTransport.send]]
 * but whose retry loop does not yet turn over on the wire in v1. Because the v1 transport is a
 * logging-only stub (Architectural Decision Log #2) that never raises a retriable connection
 * failure, [[withRetry]] runs the send operation '''exactly once''' in v1 -- no backoff sleeps
 * occur and there is no behavioral change to the v1 stub. When the v2 wire transport lands (framing
 * a [[StreamingBlockEnvelope]] over the reused `BlockTransferService`), transient producer-side
 * connection failures raised by that path are retried on this schedule before the reduce-side read
 * surfaces a [[org.apache.spark.shuffle.FetchFailedException]] and the DAGScheduler recomputes the
 * upstream stage. The v1 reader itself performs no per-block retry: on a producer-connection
 * timeout it invalidates the partial read and relies on standard DAG recomputation (see
 * `StreamingShuffleReader`).
 *
 * ==Determinism and isolation==
 * The blocking primitive is injected as a `sleeper` function (defaulting to `Thread.sleep`) so
 * tests can assert the exact backoff schedule and attempt cap with a recording no-op sleeper and
 * zero wall-clock waits. This class lives entirely in the streaming `network` subpackage, depends
 * only on the JDK and `Logging`, and has no coupling to -- and no effect on -- the existing
 * sort-based shuffle code path.
 *
 * @param maxAttempts      maximum total attempts (initial try plus retries); must be &ge; 1.
 *                         Defaults to [[StreamingShuffleRetryPolicy.MAX_ATTEMPTS]] (5).
 * @param initialBackoffMs backoff before the first retry, in milliseconds; must be &ge; 0.
 *                         Defaults to [[StreamingShuffleRetryPolicy.INITIAL_BACKOFF_MS]] (1000).
 * @param multiplier       geometric growth factor applied per attempt; must be &ge; 1. Defaults to
 *                         [[StreamingShuffleRetryPolicy.BACKOFF_MULTIPLIER]] (2).
 * @param sleeper          the blocking primitive invoked between attempts with the backoff in ms;
 *                         defaults to `Thread.sleep`. Tests inject a recording no-op to stay
 *                         deterministic and instant.
 */
@Since("4.2.0")
private[spark] class StreamingShuffleRetryPolicy(
    val maxAttempts: Int = StreamingShuffleRetryPolicy.MAX_ATTEMPTS,
    val initialBackoffMs: Long = StreamingShuffleRetryPolicy.INITIAL_BACKOFF_MS,
    val multiplier: Int = StreamingShuffleRetryPolicy.BACKOFF_MULTIPLIER,
    sleeper: Long => Unit = StreamingShuffleRetryPolicy.defaultSleeper) extends Logging {

  require(maxAttempts >= 1, s"maxAttempts must be >= 1 but was $maxAttempts")
  require(initialBackoffMs >= 0, s"initialBackoffMs must be >= 0 but was $initialBackoffMs")
  require(multiplier >= 1, s"multiplier must be >= 1 but was $multiplier")

  /**
   * Backoff delay, in milliseconds, to wait before the next attempt following the given 1-based
   * `attempt`. This is a pure, side-effect-free function: `initialBackoffMs * multiplier^(attempt
   * - 1)`. With the defaults it yields `1000, 2000, 4000, 8000, 16000` for attempts `1..5`.
   *
   * The value is computed by repeated multiplication in `Long` arithmetic with a saturation guard,
   * so an unusually large `attempt` can never overflow into a negative delay; it saturates at
   * `Long.MaxValue` instead. In normal operation `attempt` is bounded by [[maxAttempts]], so the
   * guard never triggers.
   *
   * @param attempt the 1-based attempt index; must be &ge; 1.
   * @return the non-negative backoff in milliseconds.
   */
  def backoffMillis(attempt: Int): Long = {
    require(attempt >= 1, s"attempt must be >= 1 but was $attempt")
    var backoff = initialBackoffMs
    var i = 1
    while (i < attempt) {
      // Saturate rather than overflow if a caller ever passes a pathologically large attempt.
      if (backoff > Long.MaxValue / multiplier) {
        return Long.MaxValue
      }
      backoff *= multiplier
      i += 1
    }
    backoff
  }

  /**
   * Execute `op`, retrying on retriable failures with exponential backoff up to [[maxAttempts]]
   * total attempts.
   *
   * On each failure the throwable is retried only if it is non-fatal ''and'' `isRetriable` returns
   * `true` ''and'' attempts remain; the policy then sleeps [[backoffMillis]] for the just-completed
   * attempt and tries again. Any non-retriable throwable, any fatal error, and the final failure
   * after the attempt budget is exhausted are all re-thrown to the caller unchanged so the standard
   * fault path (e.g. a reduce-side `FetchFailedException`) can drive recovery.
   *
   * @param isRetriable classifier deciding whether a given throwable warrants a retry.
   * @param op          the operation to execute; re-evaluated from scratch on each attempt.
   * @tparam T the operation's result type.
   * @return the result of the first successful attempt.
   */
  def withRetry[T](isRetriable: Throwable => Boolean)(op: => T): T = {
    var attempt = 1
    var result: Option[T] = None
    while (result.isEmpty) {
      try {
        result = Some(op)
      } catch {
        case NonFatal(t) if isRetriable(t) =>
          if (attempt >= maxAttempts) {
            logDebug(s"streaming-shuffle retry exhausted after $maxAttempts attempt(s); " +
              s"surfacing ${t.getClass.getSimpleName} to the caller")
            throw t
          }
          val backoff = backoffMillis(attempt)
          logDebug(s"streaming-shuffle retriable failure on attempt $attempt " +
            s"(${t.getClass.getSimpleName}); backing off ${backoff} ms before retry")
          sleeper(backoff)
          attempt += 1
      }
    }
    result.get
  }
}

/**
 * Companion object holding the streaming-shuffle retry constants (the mandated 1 s start, doubling
 * schedule, and 5-attempt cap), the default `Thread.sleep` sleeper, and the default
 * producer-connection retriability classifier.
 */
@Since("4.2.0")
private[spark] object StreamingShuffleRetryPolicy {

  /** Backoff before the first retry, in milliseconds (the mandated 1-second start). */
  val INITIAL_BACKOFF_MS: Long = 1000L

  /** Geometric growth factor applied to the backoff per attempt (exponential doubling). */
  val BACKOFF_MULTIPLIER: Int = 2

  /** Maximum total attempts (the initial try plus up to four retries), i.e. the 5-attempt cap. */
  val MAX_ATTEMPTS: Int = 5

  /** Default sleeper: block the calling thread for the given number of milliseconds (>0 only). */
  private val defaultSleeper: Long => Unit = ms => if (ms > 0) Thread.sleep(ms)

  /**
   * Default retriability classifier for producer-connection failures. Walks the throwable's cause
   * chain (bounded to avoid pathological cycles) and treats any [[java.io.IOException]] -- which
   * covers connection resets, connect timeouts, and socket errors -- as a transient, retriable
   * producer-connection failure. Everything else (for example a serialization or logic error) is
   * non-retriable and is surfaced to the caller on the first occurrence.
   *
   * @param t the throwable raised by a transport operation.
   * @return `true` if `t` (or one of its causes) is an [[java.io.IOException]].
   */
  def isRetriableConnectionFailure(t: Throwable): Boolean = {
    var cause: Throwable = t
    var depth = 0
    val maxDepth = 16
    while (cause != null && depth < maxDepth) {
      if (cause.isInstanceOf[IOException]) {
        return true
      }
      cause = cause.getCause
      depth += 1
    }
    false
  }
}
