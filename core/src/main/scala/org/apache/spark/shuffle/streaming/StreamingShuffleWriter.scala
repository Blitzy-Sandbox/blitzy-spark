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

import java.io.{ByteArrayOutputStream, IOException, OutputStream}

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope
import org.apache.spark.shuffle.streaming.network.StreamingShuffleTransport
import org.apache.spark.storage.ShuffleBlockId

/**
 * The map-side writer of the opt-in streaming shuffle backend.
 *
 * Instead of fully materializing a map task's output to local disk before any reduce-side fetch can
 * begin (the sort-based path), this writer buffers each partition's serialized output in a bounded
 * in-memory [[StreamingBuffer]], frames it into 2 MB CRC32C blocks, applies token-bucket
 * backpressure, coordinates disk spill under memory pressure, and (in v1) hands each block to
 * the logging-only [[StreamingShuffleTransport]]. The buffered bytes are served to the reduce side
 * directly from memory by `StreamingShuffleBlockResolver`, which is precisely what lets a reducer
 * read map output before it is ever written to disk.
 *
 * ==Composition over inheritance (the two-abstract-classes constraint)==
 *
 * Participating in the executor memory model requires being a [[MemoryConsumer]] so the
 * [[TaskMemoryManager]] can ask this writer to spill under pressure. But both [[ShuffleWriter]] and
 * [[MemoryConsumer]] are abstract '''classes''', and Scala permits only one class parent. This
 * writer therefore extends [[ShuffleWriter]] and '''composes''' a private inner
 * [[BufferMemoryConsumer]] (constructed from `context.taskMemoryManager()`) rather than extending
 * both. The inner consumer's `spill` delegates to the shared [[MemorySpillManager]].
 *
 * ==Write algorithm==
 *
 *   1. For each record, the [[org.apache.spark.Partitioner]] selects a reduce partition; the record
 *      is serialized through that partition's CURRENT block stream -- an independent serialization
 *      stream layered over `serializerManager.wrapStream` (the SAME compression/encryption wrapping
 *      the reader applies per envelope) over a fresh per-block staging buffer. A single
 *      `SerializerInstance` per partition keeps interleaved partition writes from corrupting
 *      serializer state, while a new wrapped stream per block keeps each block self-contained.
 *   1. When a block's UNCOMPRESSED serialized bytes reach the 2 MB block size (less a small
 *      margin), the block stream is CLOSED -- flushing the serializer and the codec's
 *      end-of-stream marker so the staged bytes form a complete, independently-decodable wrapped
 *      stream -- and the resulting payload (at most 2 MB) is appended to the partition's
 *      [[StreamingBuffer]] as exactly one envelope, counted toward the partition length, and framed
 *      into a wire envelope sent under backpressure. A new block stream is then opened for
 *      subsequent records.
 *   1. The final partially-filled block of each partition is sealed the same way when the partition
 *      is finalized at end of write.
 *
 * ==Compression/encryption symmetry (per-block wrapping)==
 *
 * The reduce-side [[StreamingShuffleReader]] wraps and deserializes EACH 2 MB envelope payload
 * independently via `serializerManager.wrapStream`, so every block this writer emits must itself be
 * a complete `wrapStream` stream -- never a slice of one continuous compressed stream split at a
 * 2 MB byte boundary, which would not decode independently. The writer therefore opens a fresh
 * wrapped serialization stream per block and closes it before sealing, mirroring how
 * `DiskBlockObjectWriter` wraps the sort path's stream. This makes streamed bytes round-trip
 * identically to the sort path under the default `spark.shuffle.compress=true` and under
 * `spark.io.encryption.enabled=true`; when both are off, `wrapStream` is a no-op and raw serialized
 * bytes round-trip unchanged.
 *
 * ==Dual-channel wire/persist invariant==
 *
 * The [[StreamingBuffer]] encodes its bytes as the exact same canonical
 * [[StreamingBlockEnvelope]] frames that travel on the wire, so spilled and streamed bytes are
 * byte-for-byte interchangeable: a reducer cannot tell if a partition was served from memory or
 * rehydrated from a disk spill. Because each block payload is already an independently-decodable
 * wrapped stream, this interchangeability holds with compression/encryption enabled.
 *
 * ==Memory model and the no-leak guarantee==
 *
 * As buffers grow the writer reserves execution memory through the composed consumer; a shortfall
 * triggers `spill`, which drains the largest buffers to disk via [[MemorySpillManager]]. On
 * [[stop]] the writer releases '''all''' execution memory it reserved (whether the map succeeded or
 * failed), so the task leaves no leaked allocation behind. This is verified by the stress suite
 * under `spark.unsafe.exceptionOnMemoryLeak=true`.
 *
 * ==Coexistence with the sort-based shuffle==
 *
 * This writer is constructed only on the streaming path and never touches the sort-based
 * `SortShuffleManager`, which remains the automatic fallback. When the streaming backend falls
 * back, this class is simply not instantiated.
 *
 * @param handle           the streaming shuffle handle carrying the dependency and tuning values
 * @param mapId            the map task id (a Long task-attempt id) this writer produces output for
 * @param context          the task context, source of the [[TaskMemoryManager]] and attempt id
 * @param metrics          the write-metrics reporter for bytes/records/time accounting
 * @param config           the typed streaming configuration and shared invariants
 * @param streamingMetrics the shared streaming telemetry holder (observed for debug logging)
 * @param backpressure     the flow-control protocol gating sends and tracking consumer liveness
 * @param spillManager     the memory spill manager buffers are registered with for disk spill
 * @param transport        the v1 logging-only transport the framed blocks are handed to
 * @param blockResolver    the block resolver buffers are tracked with so the reduce side can read
 *                         them from memory before they are ever spilled
 */
private[spark] class StreamingShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter,
    config: StreamingShuffleConfig,
    streamingMetrics: StreamingShuffleMetrics,
    backpressure: BackpressureProtocol,
    spillManager: MemorySpillManager,
    transport: StreamingShuffleTransport,
    blockResolver: StreamingShuffleBlockResolver)
  extends ShuffleWriter[K, V] with Logging {

  import StreamingShuffleWriter._

  /**
   * The composed [[MemoryConsumer]] through which this writer participates in the executor memory
   * model. It registers with the task's [[TaskMemoryManager]] when it first acquires memory so the
   * manager can later ask this writer to spill under pressure; the spill is
   * satisfied by draining buffers to disk through the shared [[MemorySpillManager]].
   *
   * @param tmm the task's memory manager, from `TaskContext.taskMemoryManager()`
   */
  private class BufferMemoryConsumer(tmm: TaskMemoryManager)
    extends MemoryConsumer(tmm, MemoryMode.ON_HEAP) {

    /**
     * Releases memory at the task memory manager's request by draining the largest buffered
     * partitions to disk via the shared spill manager, then freeing the matching execution-memory
     * reservation. Never calls `acquireMemory` (which is forbidden from within `spill`).
     *
     * @param size    the number of bytes the memory manager is asking to release
     * @param trigger the consumer that triggered this spill (possibly this consumer)
     * @return the number of execution-memory bytes actually released
     */
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      val reclaimed = spillManager.maybeSpill()
      if (reclaimed <= 0L) {
        0L
      } else {
        val release = math.min(reclaimed, getUsed)
        if (release > 0L) {
          freeMemory(release)
        }
        release
      }
    }
  }

  /** The shuffle dependency: the single source of the partitioner, serializer, and shuffle id. */
  private val dep = handle.dependency

  /** The id of the shuffle this map task contributes output to. */
  private val shuffleId: Int = dep.shuffleId

  /** Maps each record key to its destination reduce partition. */
  private val partitioner = dep.partitioner

  /** The number of reduce partitions; the length of [[partitionLengths]] and the MapStatus. */
  private val numPartitions: Int = partitioner.numPartitions

  /** The running SparkEnv, or null in local-mode / unit-test contexts without an executor env. */
  private val sparkEnv = SparkEnv.get

  /** The executor BlockManager (for the map-output location), or null when no SparkEnv exists. */
  private val blockManager = if (sparkEnv != null) sparkEnv.blockManager else null

  /** The MemoryManager whose maxOnHeapStorageMemory sizes per-partition buffers; may be null. */
  private val memoryManager = if (sparkEnv != null) sparkEnv.memoryManager else null

  /**
   * The serializer manager used to wrap each block's serialized output for compression and
   * encryption, EXACTLY as the reduce-side reader wraps each fetched envelope payload. Null only in
   * env-less unit construction (a real shuffle always runs with a live SparkEnv); when null the
   * writer falls back to raw, unwrapped bytes, which is symmetric with a reader whose own
   * serializer manager has compression and encryption off.
   */
  private val serializerManager = if (sparkEnv != null) sparkEnv.serializerManager else null

  /** Composed memory consumer: the composition-over-inheritance answer to two abstract classes. */
  private val memoryConsumer = new BufferMemoryConsumer(context.taskMemoryManager())

  /** Per-partition in-memory buffers (the read-side / spill source), created lazily. */
  private val buffers = new Array[StreamingBuffer](numPartitions)

  /**
   * Per-partition serializer instances, created once on first use. A single instance per partition
   * isolates interleaved partition writes (so concurrent partitions never corrupt each other's
   * serializer state); within a partition the instance is reused to open a fresh serialization
   * stream for each successive block.
   */
  private val partitionSerializerInstances = new Array[SerializerInstance](numPartitions)

  /**
   * Per-partition serialization stream for the partition's CURRENT (open) block. It is closed and
   * replaced each time a block is sealed, so each block is an independent, self-contained stream.
   * Null between sealing one block and opening the next.
   */
  private val partitionSerializers = new Array[SerializationStream](numPartitions)

  /**
   * Per-partition raw staging buffer backing the CURRENT block. The block's serializer writes
   * through [[serializerManager]].wrapStream into this buffer, so once the block stream is closed
   * this holds the complete, independently-decodable wrapped payload. A fresh buffer is allocated
   * per block. Null between sealing one block and opening the next.
   */
  private val stagingStreams = new Array[ByteArrayOutputStream](numPartitions)

  /**
   * Per-partition counter of the UNCOMPRESSED bytes written into the CURRENT block (before
   * compression). The write loop seals a block once this reaches [[BLOCK_SEAL_THRESHOLD_BYTES]],
   * which bounds each block by its pre-compression feed and keeps the sealed payload within the
   * 2 MB envelope cap for every codec. Null between sealing one block and opening the next.
   */
  private val blockCounters = new Array[CountingOutputStream](numPartitions)

  /** Per-partition monotonically increasing block sequence numbers for the wire envelopes. */
  private val sequenceNumbers = new Array[Long](numPartitions)

  /** Tracks which partitions have a backpressure stream registered, for clean teardown. */
  private val streamRegistered = new Array[Boolean](numPartitions)

  /** Per-partition written byte counts, returned by [[getPartitionLengths]] for the MapStatus. */
  private val partitionLengths = new Array[Long](numPartitions)

  /** Total payload bytes written across all partitions (for metric rollback on failure). */
  private var totalBytesWritten: Long = 0L

  /** Total records written across all partitions (for metric rollback on failure). */
  private var totalRecordsWritten: Long = 0L

  /** Guards against a double stop: stop(true) followed by stop(false) on the exception path. */
  private var stopping: Boolean = false

  /** The MapStatus produced by a successful [[write]]; null until write completes. */
  private var mapStatus: MapStatus = null

  /**
   * Writes a map task's records to the streaming shuffle output. Each record is routed to its
   * reduce partition, serialized through that partition's stream, buffered in memory, framed into
   * 2 MB CRC32C blocks, and sent under backpressure. On return every partition's bytes are buffered
   * (and observable by the reduce side) and the [[MapStatus]] for this map output is built.
   *
   * @param records the (key, value) records produced by this map task, in arbitrary partition order
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val startNanos = System.nanoTime()
    logInfo(log"Streaming shuffle write starting " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
      log"attempt=${MDC(TASK_ATTEMPT_ID, context.taskAttemptId())} " +
      log"partitions=${MDC(NUM_PARTITIONS, numPartitions)}")
    while (records.hasNext) {
      val record = records.next()
      val key = record._1
      val value = record._2
      val partition = partitioner.getPartition(key)
      ensurePartition(partition)
      partitionSerializers(partition).writeKey(key: Any).writeValue(value: Any)
      totalRecordsWritten += 1L
      metrics.incRecordsWritten(1L)
      // Seal the current block once it has been fed ~2 MB of UNCOMPRESSED serialized bytes (one
      // margin below the 2 MB envelope cap). Bounding the pre-compression feed -- rather than the
      // flushed compressed size -- keeps each sealed block within the cap for every codec
      // regardless of its internal block size, since compression/encryption can only grow
      // incompressible data by a small, bounded overhead.
      if (blockCounters(partition).byteCount >= BLOCK_SEAL_THRESHOLD_BYTES) {
        sealBlock(partition)
      }
    }
    finalizeAllPartitions()
    mapStatus = buildMapStatus()
    metrics.incWriteTime(System.nanoTime() - startNanos)
    logInfo(log"Streaming shuffle write completed " +
      log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
      log"bytes=${MDC(NUM_BYTES, totalBytesWritten)} records=${MDC(COUNT, totalRecordsWritten)}")
    if (config.debug) {
      logDebug(s"Streaming shuffle observed spills=${streamingMetrics.spillCount} " +
        s"backpressureEvents=${streamingMetrics.backpressureEvents}")
    }
  }

  /**
   * Closes this writer. On success the buffered partitions are intentionally left registered with
   * the spill manager and block resolver so the reduce side can fetch them; on failure the partial
   * output is discarded and the reported write metrics are rolled back. In both cases every
   * execution-memory reservation is released so the task leaves no memory leak.
   *
   * @param success whether the map task completed successfully
   * @return `Some(mapStatus)` on success (or `None` without a location); `None` on failure
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    if (success) {
      stopOnSuccess()
    } else {
      stopOnFailure()
    }
  }

  /** @return the per-partition written byte counts backing the MapStatus. */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  /**
   * Ensures `partition` is ready to accept a record. On first use it lazily creates the buffer and
   * the partition's serializer instance, registering the buffer with the spill manager and block
   * resolver and the stream with the backpressure protocol. It then ensures an open block stream
   * exists -- opening one (via [[startBlock]]) whenever the partition has none, which is the case
   * both on first use and immediately after a block was sealed. A re-entrant call with an open
   * block and an already-initialized partition is a no-op.
   *
   * @param partition the reduce partition to initialize / open a block for
   */
  private def ensurePartition(partition: Int): Unit = {
    if (buffers(partition) == null) {
      val buffer = new StreamingBuffer(shuffleId, mapId, partition, perPartitionBufferBytes())
      buffers(partition) = buffer
      spillManager.register(buffer)
      blockResolver.trackBuffer(buffer)
      partitionSerializerInstances(partition) = dep.serializer.newInstance()
      backpressure.registerStream(BackpressureProtocol.StreamKey(shuffleId, mapId, partition))
      streamRegistered(partition) = true
    }
    if (partitionSerializers(partition) == null) {
      startBlock(partition)
    }
  }

  /**
   * Opens a fresh block for `partition`: a new raw staging buffer wrapped by
   * [[serializerManager]].wrapStream (the SAME compression/encryption layer the reader applies per
   * envelope) and a serialization stream over an uncompressed-byte counter. Each block is therefore
   * an INDEPENDENT, self-contained wrapped stream -- exactly what the reader requires, since it
   * wraps and deserializes every 2 MB envelope payload on its own. Wrapping per block (rather than
   * once per partition and slicing) is what keeps multi-block partitions correct under
   * compression/encryption: a slice of one continuous compressed stream split mid-frame would not
   * decode independently. When no serializer manager is available (env-less unit construction) the
   * staging buffer is used raw, symmetric with a compression/encryption-off reader.
   *
   * @param partition the reduce partition to open a new block for
   */
  private def startBlock(partition: Int): Unit = {
    val staging = new ByteArrayOutputStream(STAGING_BUFFER_INITIAL_BYTES)
    stagingStreams(partition) = staging
    // Wrap with the same ShuffleBlockId TYPE the reader fetches, so shouldCompress / encryption
    // decisions match symmetrically; the id field values do not affect those decisions.
    val wrapped: OutputStream =
      if (serializerManager != null) {
        serializerManager.wrapStream(ShuffleBlockId(shuffleId, mapId, partition), staging)
      } else {
        staging
      }
    val counting = new CountingOutputStream(wrapped)
    blockCounters(partition) = counting
    partitionSerializers(partition) =
      partitionSerializerInstances(partition).serializeStream(counting)
  }

  /**
   * Computes the per-partition buffer capacity in bytes from the executor on-heap storage memory
   * budget (the same denominator the spill manager measures against), applying the configured
   * percentage and the 2 MB floor. Falls back to the JVM max heap when no SparkEnv is present.
   *
   * @return the per-partition buffer capacity in bytes (never below 2 MB)
   */
  private def perPartitionBufferBytes(): Long = {
    val executorMemoryBytes =
      if (memoryManager != null) memoryManager.maxOnHeapStorageMemory
      else Runtime.getRuntime.maxMemory()
    config.perPartitionBufferBytes(executorMemoryBytes, numPartitions)
  }

  /**
   * Seals a partition's current block: CLOSES its serialization stream -- which flushes the
   * serializer footer AND the compression/encryption codec's end-of-stream marker into the staging
   * buffer, so the staged bytes form a complete, independently-decodable wrapped stream -- captures
   * the resulting payload, clears the per-block state, and drains the payload into the buffer.
   * Closing (rather than merely flushing) is essential: a flushed-but-open compression stream lacks
   * its end marker and cannot be decoded on its own by the reader, which deserializes each envelope
   * payload independently. The per-block state is cleared so the next record re-opens a fresh block
   * via [[ensurePartition]] / [[startBlock]]; a no-op when the partition has no open block.
   *
   * @param partition the reduce partition whose current block is sealed
   */
  private def sealBlock(partition: Int): Unit = {
    val serializer = partitionSerializers(partition)
    if (serializer != null) {
      serializer.close()
      val payload = stagingStreams(partition).toByteArray
      partitionSerializers(partition) = null
      stagingStreams(partition) = null
      blockCounters(partition) = null
      drain(partition, payload)
    }
  }

  /**
   * Drains one sealed block `payload` into its [[StreamingBuffer]] (which frames and checksums it),
   * updates the partition length and write metrics, and frames the bytes into a wire envelope sent
   * under backpressure. The payload is a complete, independently-decodable wrapped stream of at
   * most 2 MB, so `append` and `sendFramed` frame it as exactly one envelope. An empty payload (a
   * block with no records) is a no-op. Passing the local `payload` array (rather than re-reading
   * the buffer) keeps draining safe even if the spill manager's poll loop concurrently drains the
   * buffer.
   *
   * @param partition the reduce partition the block belongs to
   * @param payload   the sealed block bytes (a complete wrapped stream, at most 2 MB)
   */
  private def drain(partition: Int, payload: Array[Byte]): Unit = {
    if (payload.length > 0) {
      acquireMemoryFor(payload.length)
      buffers(partition).append(payload)
      // MapStatus must report the PHYSICAL block size the resolver/spill path serves, not the raw
      // payload: both `StreamingBuffer.toByteArray` (in-memory serve) and the disk spill frame the
      // bytes as the in-order concatenation of one StreamingBlockEnvelope per block, adding a fixed
      // 32-byte header per block. Each sealed block is at most 2 MB, so `append` and `sendFramed`
      // frame it as exactly one block (ceil(len / 2 MB) == 1); reporting the framed size keeps
      // `partitionLengths` consistent with the bytes a reduce task actually fetches, avoiding a
      // 32-byte-per-block under-count in fetch/scheduling accounting.
      val numBlocks = (payload.length + StreamingShuffleConfig.BLOCK_SIZE_BYTES - 1) /
        StreamingShuffleConfig.BLOCK_SIZE_BYTES
      val framedLength =
        payload.length.toLong + numBlocks.toLong * StreamingBlockEnvelope.HEADER_BYTES.toLong
      partitionLengths(partition) += framedLength
      // Write-volume telemetry tracks the (post-compression) block payload actually buffered and
      // served, consistent with the sort path's post-compression byte accounting; envelope framing
      // headers are excluded so the metric reflects shuffle data rather than the wire format.
      totalBytesWritten += payload.length.toLong
      metrics.incBytesWritten(payload.length.toLong)
      val streamKey = BackpressureProtocol.StreamKey(shuffleId, mapId, partition)
      sendFramed(streamKey, partition, payload)
      maybeHandleConsumerTimeout(streamKey, partition)
    }
  }

  /**
   * Frames `bytes` into blocks of at most 2 MB, acquiring a backpressure send permit per block,
   * building the canonical [[StreamingBlockEnvelope]], handing it to the v1 logging-only transport,
   * and recording the unacknowledged byte count. The real data plane is the reduce side's
   * `BlockTransferService.fetchBlockSync`, so the transport hand-off is observational in v1.
   *
   * @param streamKey the (shuffleId, mapId, reduceId) identity of this block stream
   * @param reduceId  the destination reduce partition
   * @param bytes     the serialized bytes to frame and send
   */
  private def sendFramed(
      streamKey: BackpressureProtocol.StreamKey,
      reduceId: Int,
      bytes: Array[Byte]): Unit = {
    val target = shuffleServerLocation
    var offset = 0
    while (offset < bytes.length) {
      val len = math.min(StreamingShuffleConfig.BLOCK_SIZE_BYTES, bytes.length - offset)
      val payload = bytes.slice(offset, offset + len)
      backpressure.acquireSendPermit(len)
      val seq = sequenceNumbers(reduceId)
      sequenceNumbers(reduceId) = seq + 1L
      val envelope = StreamingBlockEnvelope.create(shuffleId, mapId, reduceId, seq, payload)
      target.foreach(location => transport.sendBlock(envelope, location))
      backpressure.recordSend(streamKey, len.toLong)
      offset += len
    }
  }

  /**
   * Implements the consumer-failure protocol: when the backpressure protocol has flagged the
   * consumer as timed out (no acks within the 10 s window), the unacknowledged data is persisted to
   * disk (spilling if the buffer is above its threshold) so it is not lost, and a retransmit is
   * recorded under exponential backoff. The actual re-delivery rides the reduce-side fetch path in
   * v1. This path only engages once the protocol's background scan is running and the timeout has
   * elapsed, so it is inert for short-lived writes.
   *
   * @param streamKey the (shuffleId, mapId, reduceId) identity of this block stream
   * @param reduceId  the destination reduce partition
   */
  private def maybeHandleConsumerTimeout(
      streamKey: BackpressureProtocol.StreamKey, reduceId: Int): Unit = {
    if (backpressure.isConsumerTimedOut(streamKey)) {
      val unacked = backpressure.unackedBytes(streamKey)
      spillManager.maybeSpill()
      if (backpressure.isRetransmitDue(streamKey)) {
        backpressure.recordRetransmit(streamKey)
        logWarning(log"Streaming consumer unresponsive; buffered for retransmit " +
          log"shuffle=${MDC(SHUFFLE_ID, shuffleId)} map=${MDC(MAP_ID, mapId)} " +
          log"reduce=${MDC(REDUCE_ID, reduceId)} unacked=${MDC(NUM_BYTES, unacked)}")
      }
    }
  }

  /**
   * Reserves execution memory to reflect heap growth as buffers fill. A shortfall causes the task
   * memory manager to invoke spilling (possibly this consumer), which the spill manager
   * satisfies by draining buffers to disk. The granted amount may be less than asked; the buffer
   * still holds the bytes regardless.
   *
   * @param bytes the number of bytes just buffered
   */
  private def acquireMemoryFor(bytes: Int): Unit = {
    if (bytes > 0) {
      memoryConsumer.acquireMemory(bytes.toLong)
    }
  }

  /** @return the local map-output location, or `None` when no BlockManager is available. */
  private def shuffleServerLocation =
    if (blockManager != null) Option(blockManager.shuffleServerId) else None

  /**
   * Seals each partition's final, partially-filled block via [[sealBlock]] -- closing its stream so
   * the staged bytes form a complete, independently-decodable wrapped payload, then draining that
   * payload into the buffer. No new block is opened afterward (the write loop is done). The buffers
   * themselves are retained so the reduce side can read them.
   */
  private def finalizeAllPartitions(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      if (partitionSerializers(partition) != null) {
        sealBlock(partition)
      }
      partition += 1
    }
  }

  /** @return the MapStatus for this map output, or null when no location is available. */
  private def buildMapStatus(): MapStatus = {
    shuffleServerLocation match {
      case Some(location) => MapStatus(location, partitionLengths, mapId)
      case None => null
    }
  }

  /**
   * Success teardown: returns the produced MapStatus while keeping the buffers registered for the
   * reduce side, and releases this task's execution-memory reservation.
   *
   * @return `Some(mapStatus)`, or `None` if no map-output location was available
   */
  private def stopOnSuccess(): Option[MapStatus] = {
    try {
      Option(mapStatus)
    } finally {
      cleanup(discardBuffers = false)
    }
  }

  /**
   * Failure teardown: rolls back the reported write metrics so a failed attempt does not inflate
   * totals, discards the partial buffered output, and releases all execution memory.
   *
   * @return always `None`
   */
  private def stopOnFailure(): Option[MapStatus] = {
    try {
      if (totalBytesWritten > 0L) {
        metrics.decBytesWritten(totalBytesWritten)
      }
      if (totalRecordsWritten > 0L) {
        metrics.decRecordsWritten(totalRecordsWritten)
      }
      None
    } finally {
      cleanup(discardBuffers = true)
    }
  }

  /**
   * Common teardown: closes any open streams, unregisters the backpressure streams, optionally
   * discards the buffered output (on failure), and always releases all reserved execution memory so
   * the task leaves no leak.
   *
   * @param discardBuffers whether to clear and unregister the in-memory buffers (true on failure)
   */
  private def cleanup(discardBuffers: Boolean): Unit = {
    closeOpenStreams()
    unregisterStreams()
    if (discardBuffers) {
      discardTrackedBuffers()
    }
    releaseAllMemory()
  }

  /**
   * Closes any current block serialization stream still open (e.g. on a failure mid-write),
   * ignoring errors, and clears the per-block state. Closing the stream releases the
   * compression/encryption codec's resources even though the partial bytes are discarded on the
   * failure path.
   */
  private def closeOpenStreams(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val serializer = partitionSerializers(partition)
      if (serializer != null) {
        try {
          serializer.close()
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to close streaming serializer for partition " +
              log"${MDC(PARTITION_ID, partition)} during teardown", e)
        }
        partitionSerializers(partition) = null
        stagingStreams(partition) = null
        blockCounters(partition) = null
      }
      partition += 1
    }
  }

  /** Unregisters every backpressure stream this writer registered. */
  private def unregisterStreams(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      if (streamRegistered(partition)) {
        backpressure.unregisterStream(
          BackpressureProtocol.StreamKey(shuffleId, mapId, partition))
        streamRegistered(partition) = false
      }
      partition += 1
    }
  }

  /** Clears and unregisters the in-memory buffers; used on failure to drop the partial output. */
  private def discardTrackedBuffers(): Unit = {
    var partition = 0
    while (partition < numPartitions) {
      val buffer = buffers(partition)
      if (buffer != null) {
        spillManager.unregister(MemorySpillManager.keyFor(buffer))
        buffer.clear()
        buffers(partition) = null
      }
      partition += 1
    }
  }

  /** Releases all execution memory reserved by the composed consumer (the no-leak guarantee). */
  private def releaseAllMemory(): Unit = {
    val used = memoryConsumer.getUsed
    if (used > 0L) {
      memoryConsumer.freeMemory(used)
    }
  }
}

/**
 * Companion holding the writer's compile-time constants and the uncompressed-byte counting stream.
 */
private[spark] object StreamingShuffleWriter {

  /** Initial capacity, in bytes, of each partition's serialization staging stream (64 KB). */
  private val STAGING_BUFFER_INITIAL_BYTES: Int = 64 * 1024

  /**
   * Headroom, in bytes, kept below the 2 MB block cap when deciding to seal a block. Compression
   * and encryption can grow incompressible data by only a small, bounded overhead (well under this
   * margin for any Spark codec over a ~2 MB feed), so sealing once the UNCOMPRESSED feed reaches
   * `BLOCK_SIZE_BYTES - this` guarantees the sealed (wrapped) payload stays within the 2 MB
   * envelope cap that [[StreamingBlockEnvelope.create]] enforces.
   */
  private val BLOCK_SEAL_MARGIN_BYTES: Int = 64 * 1024

  /**
   * Uncompressed-byte threshold at which the current block is sealed. Bounding the pre-compression
   * feed (rather than the flushed compressed size) keeps the trigger correct for every codec
   * independent of its internal block size, while the [[BLOCK_SEAL_MARGIN_BYTES]] headroom keeps
   * the resulting compressed/encrypted payload within the 2 MB cap.
   */
  private val BLOCK_SEAL_THRESHOLD_BYTES: Int =
    StreamingShuffleConfig.BLOCK_SIZE_BYTES - BLOCK_SEAL_MARGIN_BYTES

  /**
   * A thin [[java.io.OutputStream]] decorator that counts the UNCOMPRESSED bytes written through it
   * before they reach the wrapped compression/encryption stream. The block-seal trigger reads
   * [[byteCount]] to bound each block by the bytes fed to the codec -- independent of the codec's
   * compression ratio or internal block size -- which keeps every sealed block within the 2 MB
   * envelope payload cap. It owns no buffering; `flush` and `close` simply forward to the wrapped
   * stream (closing it flushes the codec's end-of-stream marker).
   *
   * @param out the wrapped (compression/encryption) output stream bytes are forwarded to
   */
  private final class CountingOutputStream(out: OutputStream) extends OutputStream {

    /** Running total of bytes written through this stream. */
    private var count: Long = 0L

    /** @return the number of bytes written through this stream so far. */
    def byteCount: Long = count

    override def write(b: Int): Unit = {
      out.write(b)
      count += 1L
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      out.write(b, off, len)
      count += len.toLong
    }

    override def flush(): Unit = out.flush()

    override def close(): Unit = out.close()
  }
}
