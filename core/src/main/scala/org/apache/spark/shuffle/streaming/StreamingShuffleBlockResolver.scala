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

import java.io.{BufferedOutputStream, FileOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, MigratableResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.shuffle.streaming.MemorySpillManager.BufferKey
import org.apache.spark.storage.{BlockId, ShuffleBlockId, ShuffleMergedBlockId}
import org.apache.spark.util.Utils

/**
 * Resolves shuffle block data for the opt-in streaming shuffle backend, serving
 * still-in-memory streamed partitions directly while delegating every on-disk concern to an
 * inner [[org.apache.spark.shuffle.IndexShuffleBlockResolver]].
 *
 * ==Why delegation==
 * Delegating `.data`/`.index` resolution, merged (push-based) shuffle, and all block-migration
 * concerns to an inner [[IndexShuffleBlockResolver]] is the least-modification path (AAP 0.4.2):
 * it reuses the battle-tested on-disk shuffle format and the existing decommission/migration
 * machinery unchanged, so the streaming backend inherits correct behavior for those paths for
 * free and never touches the block-manager storage interface contracts. The streaming resolver
 * adds exactly one capability on top: it can serve a reduce partition's bytes straight from the
 * producer's in-memory [[StreamingBuffer]] before those bytes are ever written to disk.
 *
 * ==Tracking and the single source of spill truth==
 * The [[buffers]] map, keyed by the logical `(shuffleId, mapId, reduceId)` triple, holds the
 * partitions still resident in a producer-side [[StreamingBuffer]]; the map-side writer registers
 * them via [[trackBuffer]]. Spilled partitions are NOT tracked here: the [[MemorySpillManager]]
 * is the single owner of the ordered disk-spill segment list and the only component that reads
 * spilled bytes back (via the public `BlockManager.getLocalBytes` on the non-shuffle
 * [[org.apache.spark.storage.TempLocalBlockId]]s it stored). The resolver consults it through the
 * reference installed by [[setSpillManager]]; spill registration, the persisted block format, and
 * the read path are thus one atomic design owned by the spill manager, with no second,
 * drift-prone copy of spill state in the resolver. Under sustained pressure a partition can have
 * BOTH spilled segments on disk and freshly re-buffered bytes in memory, so the two views are
 * combined rather than treated as mutually exclusive.
 *
 * ==getBlockData lookup order==
 * For a [[ShuffleBlockId]] the resolver assembles the partition's bytes in deterministic order:
 * every spilled segment (oldest first, read back through the spill manager) followed by whatever
 * remains in the in-memory [[StreamingBuffer]]. The spilled-segment id list and the in-memory
 * frames are captured in one atomic snapshot under the buffer's lock (see
 * [[StreamingBuffer.snapshotEnvelopedWith]]) so a concurrent spill cannot make a block appear in
 * both views or neither. All bytes are the canonical
 * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] frames (the dual-channel
 * wire/persist view); the reduce-side [[StreamingShuffleReader]] parses and CRC-validates them
 * and strips the 32-byte headers before deserialization. When a block is not streaming-tracked
 * (for example, output produced by the sort-based fallback path) it is delegated unchanged to the
 * inner [[IndexShuffleBlockResolver]], as is any merged/batched block id shape.
 *
 * ==Concurrency==
 * Reduce-side fetches call [[getBlockData]] concurrently with the map-side writer and the spill
 * manager mutating the [[buffers]] map. The map is a [[ConcurrentHashMap]], so reads observe a
 * consistent per-key snapshot without external locking, and [[untrackShuffle]] uses its
 * weakly-consistent iterator to purge a shuffle without blocking concurrent lookups. The
 * authoritative spilled-segment list lives in the [[MemorySpillManager]] and is queried through
 * the volatile [[spillManager]] reference; the atomic combine of spilled-segment ids with the
 * live in-memory frames happens under the per-buffer lock via
 * [[StreamingBuffer.snapshotEnvelopedWith]].
 *
 * This type coexists with the sort-based shuffle path and is constructed only when the streaming
 * backend is active; when streaming is disabled the manager never routes resolution here.
 *
 * @param conf the [[org.apache.spark.SparkConf]] used by the auxiliary constructor to build the
 *             default inner [[IndexShuffleBlockResolver]]; retained on the signature for API
 *             symmetry with the delegate so a manager can construct either resolver uniformly
 * @param indexResolver the inner resolver that owns the on-disk `.data`/`.index` representation
 *                      and all migration behavior
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    indexResolver: IndexShuffleBlockResolver)
  extends ShuffleBlockResolver
  with MigratableResolver
  with Logging {

  import StreamingShuffleBlockResolver.BlockKey

  /**
   * Convenience constructor that builds a private inner [[IndexShuffleBlockResolver]] from the
   * given configuration. Prefer the primary constructor when the `StreamingShuffleManager`
   * already holds a shared index resolver, so the streaming and fallback paths resolve `.data`/
   * `.index` blocks through the very same delegate.
   */
  def this(conf: SparkConf) = this(conf, new IndexShuffleBlockResolver(conf))

  // Partitions still resident in a producer-side StreamingBuffer, keyed by the logical
  // (shuffleId, mapId, reduceId) triple. Registered by the writer through `trackBuffer`.
  private val buffers = new ConcurrentHashMap[BlockKey, StreamingBuffer]()

  // The MemorySpillManager is the single owner of the ordered disk-spill segment list and the
  // only component that reads spilled bytes back (it stored them under non-shuffle
  // TempLocalBlockIds via BlockManager.putBytes, and only it can read them back through
  // BlockManager.getLocalBytes, which asserts the id is non-shuffle). The resolver holds a
  // reference rather than a private copy of spill state so spill registration, the persisted
  // block format, and the read path stay one atomic design with no drift. The writer installs
  // this via `setSpillManager` immediately after constructing both collaborators; it is read on
  // the concurrent `getBlockData` path, hence @volatile for safe publication.
  @volatile private var spillManager: MemorySpillManager = _

  // ---------------------------------------------------------------------------------------
  // Tracking API consumed by the writer, the spill manager, and the manager's
  // `unregisterShuffle`. These are the hooks that let `getBlockData` serve still-in-memory
  // streamed blocks before (and after) they reach disk.
  // ---------------------------------------------------------------------------------------

  /**
   * Registers a producer-side [[StreamingBuffer]] so its bytes can be served in-memory by
   * [[getBlockData]]. The logical key is derived from the buffer's own `shuffleId`, `mapId`,
   * and `partitionId`, so the writer never has to restate it. Idempotent: re-tracking the same
   * key replaces the previous buffer reference.
   *
   * @param buffer the per-partition buffer holding the map-side output for one reduce partition
   */
  def trackBuffer(buffer: StreamingBuffer): Unit = {
    val key = BlockKey(buffer.shuffleId, buffer.mapId, buffer.partitionId)
    buffers.put(key, buffer)
    logTrace(s"Tracking in-memory streaming buffer for $key (${buffer.size} bytes)")
  }

  /**
   * Installs the [[MemorySpillManager]] the resolver consults for spilled-segment ids and for
   * reading spilled bytes back. The writer wires this immediately after constructing both the
   * resolver and the spill manager (they hold the same `(shuffleId, mapId, partitionId)` view of
   * each partition), so [[getBlockData]] can combine on-disk segments with live in-memory frames.
   *
   * Spill state deliberately lives only in the spill manager: it owns the ordered segment list,
   * the persisted (non-shuffle [[org.apache.spark.storage.TempLocalBlockId]]) block format, and
   * the read path, so there is no second copy here to drift out of sync. Safe to call once during
   * writer construction; the field is `@volatile` so the value publishes to the concurrent
   * `getBlockData` readers.
   *
   * @param manager the spill manager that owns this shuffle's spilled segments
   */
  def setSpillManager(manager: MemorySpillManager): Unit = {
    spillManager = manager
  }

  /**
   * Drops every in-memory buffer tracking entry for the given shuffle. Invoked by
   * `StreamingShuffleManager.unregisterShuffle` so a completed or cleaned-up shuffle no longer
   * retains buffer references. Uses the map's weakly-consistent iterator, so concurrent
   * [[getBlockData]] lookups are never blocked. Spilled segments are owned and reclaimed by the
   * [[MemorySpillManager]] (the resolver keeps no spill state of its own), so its companion
   * cleanup releases the on-disk segments for the same shuffle.
   *
   * @param shuffleId the shuffle whose tracked buffers should be removed
   */
  def untrackShuffle(shuffleId: Int): Unit = {
    removeShuffleEntries(buffers, shuffleId)
    logDebug(s"Untracked all in-memory streaming buffers for shuffle $shuffleId")
  }

  // ---------------------------------------------------------------------------------------
  // ShuffleBlockResolver
  // ---------------------------------------------------------------------------------------

  /**
   * Retrieves the data for the requested block. For a streaming-tracked [[ShuffleBlockId]] it
   * assembles the partition's canonical enveloped bytes; every other block id (for example output
   * produced by the sort-based fallback path, or a merged/batched id) is delegated unchanged to
   * the inner [[IndexShuffleBlockResolver]].
   *
   * A partition is streaming-tracked iff the writer registered a [[StreamingBuffer]] for it via
   * [[trackBuffer]]; that buffer stays tracked for the shuffle's lifetime (it is emptied, not
   * removed, on spill), so the presence of the buffer entry -- not the absence of spilled data --
   * is the discriminator between the streaming path and fallback delegation. The actual byte
   * assembly (spilled segments oldest-first, then the live in-memory frames, captured under one
   * lock) is performed by [[serveStreamingPartition]].
   *
   * Note: the `dirs` parameter intentionally carries no default here. The default
   * (`None`) is declared once on the [[ShuffleBlockResolver]] trait; Scala forbids an
   * overriding method from restating it.
   *
   * @param blockId the logical shuffle block to resolve
   * @param dirs optional explicit local directories to read from, forwarded to the delegate
   * @return a managed buffer over the block's bytes
   */
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        val key = BlockKey(shuffleId, mapId, reduceId)
        val buffer = buffers.get(key)
        if (buffer != null) {
          // Streaming-tracked partition: assemble spilled segments (oldest first) followed by the
          // bytes still live in memory. Both views are captured in ONE atomic snapshot under the
          // buffer's lock (see serveStreamingPartition / StreamingBuffer.snapshotEnvelopedWith)
          // so a concurrent spill -- whose segment-add and buffer-clear run under the same lock
          // (see MemorySpillManager.spillBufferInternal) -- can never make this block appear in
          // both views or in neither, preserving the zero-data-loss guarantee.
          serveStreamingPartition(blockId, key, buffer)
        } else {
          // Not streaming-tracked (e.g. produced by the sort-based fallback path): delegate the
          // original block id to the index resolver, which owns the on-disk .data/.index format.
          indexResolver.getBlockData(blockId, dirs)
        }
      case _ =>
        // Batched or other block-id shapes are resolved entirely by the index resolver.
        indexResolver.getBlockData(blockId, dirs)
    }
  }

  /**
   * Retrieves the [[BlockId]]s for a given shuffle map. Delegated to the inner resolver so any
   * on-disk (spilled or fallback) shuffle files are reported for external-shuffle-service
   * cleanup; purely in-memory streamed partitions have no files and are a no-op for cleanup.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    indexResolver.getBlocksForShuffle(shuffleId, mapId)
  }

  /**
   * Retrieves a merged (push-based) shuffle block as multiple chunks. Merged shuffle is not a
   * streaming concern, so this delegates unchanged to the inner resolver to preserve behavior.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    indexResolver.getMergedBlockData(blockId, dirs)
  }

  /**
   * Retrieves the metadata for a merged (push-based) shuffle block. Delegated unchanged to the
   * inner resolver for the same reason as [[getMergedBlockData]].
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    indexResolver.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Releases all tracking state and stops the delegate. Clears the in-memory buffer map first so
   * no buffer references survive teardown, then stops the inner [[IndexShuffleBlockResolver]].
   * Spilled segments are owned by the [[MemorySpillManager]]; its own teardown reclaims the
   * on-disk blocks, so the resolver holds nothing to clear for them.
   */
  override def stop(): Unit = {
    buffers.clear()
    indexResolver.stop()
  }

  // ---------------------------------------------------------------------------------------
  // MigratableResolver -- pure delegation to the inner IndexShuffleBlockResolver so the
  // streaming backend inherits Spark's existing block-migration / decommission behavior
  // unchanged (AAP 0.4.2: "delegating migration concerns to IndexShuffleBlockResolver").
  // ---------------------------------------------------------------------------------------

  /**
   * Returns the shuffle ids stored locally for migration. Pure delegation: the inner resolver
   * enumerates on-disk shuffle index files, which is the authoritative source for migratable
   * blocks. (Purely in-memory streamed partitions are transient and not migration candidates.)
   */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    indexResolver.getStoredShuffles()
  }

  /**
   * Marks a shuffle that should not be migrated. Delegated to the inner resolver so skip state
   * is honored by the same machinery that enumerates and serves migration blocks.
   */
  override def addShuffleToSkip(shuffleId: Int): Unit = {
    indexResolver.addShuffleToSkip(shuffleId)
  }

  /**
   * Accepts a shuffle block written as a stream during migration. Delegated to the inner
   * resolver, which persists the incoming bytes into the standard `.data`/`.index` files.
   */
  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    indexResolver.putShuffleBlockAsStream(blockId, serializerManager)
  }

  /**
   * Returns the index and data blocks for migrating a particular shuffle map. Delegated to the
   * inner resolver, which reads them from the on-disk consolidated files.
   */
  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    indexResolver.getMigrationBlocks(shuffleBlockInfo)
  }

  // ---------------------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------------------

  // Removes every entry whose key belongs to the given shuffle from the supplied concurrent
  // map. Generic over the value type so it can serve any future per-shuffle map; today it backs
  // the in-memory buffer map. The ConcurrentHashMap entry iterator is weakly consistent and
  // supports in-place removal, so this never blocks concurrent `getBlockData` lookups.
  private def removeShuffleEntries[V](
      map: ConcurrentHashMap[BlockKey, V],
      shuffleId: Int): Unit = {
    val it = map.entrySet().iterator()
    while (it.hasNext) {
      if (it.next().getKey.shuffleId == shuffleId) {
        it.remove()
      }
    }
  }

  /**
   * Assembles and serves the canonical enveloped bytes for one streaming-tracked reduce
   * partition.
   *
   * The served bytes are the concatenation, in deterministic order, of every disk-spill segment
   * (oldest first) followed by the frames still live in the producer's [[StreamingBuffer]]. The
   * ordered spilled-segment id list and the in-memory frames are captured together under the
   * buffer's lock via [[StreamingBuffer.snapshotEnvelopedWith]], so the pair is consistent with
   * any concurrent spill (whose segment-add and buffer-clear run under the same lock): a block can
   * never appear in both views or in neither, and the captured in-memory frames equal the bytes a
   * subsequent spill would persist, so there is no loss and no double-count. Disk reads happen
   * after the lock is released; the spilled segments are read back through the
   * [[MemorySpillManager]], the single owner of the on-disk (non-shuffle
   * [[org.apache.spark.storage.TempLocalBlockId]]) format.
   *
   * The bytes are served as-is: they are the
   * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] frames (the
   * dual-channel wire/persist view). The reduce-side [[StreamingShuffleReader]] parses each frame,
   * verifies its CRC32C, and strips the 32-byte header before deserialization, so spilled and
   * streamed bytes are byte-identical and interchangeable.
   *
   * @param blockId the logical shuffle block being served (for logging/diagnostics)
   * @param key the in-memory buffer key for the partition
   * @param buffer the producer-side buffer whose lock guards the atomic snapshot
   * @return a managed buffer over the assembled enveloped bytes
   * @throws IllegalStateException if a recorded spill segment can no longer be read back, failing
   *         the fetch loudly so the partition is recomputed via lineage rather than served short
   */
  private def serveStreamingPartition(
      blockId: BlockId,
      key: BlockKey,
      buffer: StreamingBuffer): ManagedBuffer = {
    // Collect the ordered enveloped byte parts (spilled segments oldest-first, then the live
    // in-memory frames) under the buffer's atomic snapshot, then flatten them into one array.
    val parts = collectEnvelopedParts(blockId, key, buffer)
    val totalLen = parts.iterator.map(_.length.toLong).sum
    require(totalLen <= Int.MaxValue,
      s"Streaming block $blockId is $totalLen bytes, exceeding the single-buffer byte limit")
    val assembled = new Array[Byte](totalLen.toInt)
    var offset = 0
    parts.foreach { part =>
      System.arraycopy(part, 0, assembled, offset, part.length)
      offset += part.length
    }

    logTrace(log"Serving streaming block ${MDC(LogKeys.BLOCK_ID, blockId)} for shuffle " +
      log"${MDC(LogKeys.SHUFFLE_ID, key.shuffleId)} map ${MDC(LogKeys.MAP_ID, key.mapId)} " +
      log"reduce ${MDC(LogKeys.REDUCE_ID, key.reduceId)} " +
      log"(${MDC(LogKeys.NUM_BYTES, totalLen)} bytes)")
    new NioManagedBuffer(ByteBuffer.wrap(assembled))
  }

  /**
   * Captures the canonical enveloped bytes of one streamed reduce partition as an ordered list of
   * byte arrays: each recorded spill segment (oldest first), then the live in-memory frames as a
   * single trailing array. This is the single assembly primitive shared by the in-memory serving
   * path ([[serveStreamingPartition]]) and the durable publication path
   * ([[commitDurableMapOutput]]), so the bytes a reduce task fetches while the producer is alive
   * are byte-for-byte identical to the bytes committed to the standard `.data`/`.index` files for
   * remote, external-shuffle-service, and post-cleanup fetches.
   *
   * The spilled-id list and the in-memory frames are captured together inside one
   * [[StreamingBuffer.snapshotEnvelopedWith]] critical section so a concurrent spill -- whose
   * segment-add and buffer-clear run under the same lock -- can never make a block appear in both
   * views or in neither, preserving the zero-data-loss guarantee.
   *
   * @param blockId the logical shuffle block being assembled (for diagnostics)
   * @param key the in-memory buffer key for the partition
   * @param buffer the producer-side buffer whose lock guards the atomic snapshot
   * @return the ordered enveloped byte parts; empty when the partition holds no bytes
   * @throws IllegalStateException if a recorded spill segment can no longer be read back, failing
   *         the fetch loudly so the partition is recomputed via lineage rather than served short
   */
  private def collectEnvelopedParts(
      blockId: BlockId,
      key: BlockKey,
      buffer: StreamingBuffer): ArrayBuffer[Array[Byte]] = {
    val manager = spillManager
    val bufferKey = BufferKey(key.shuffleId, key.mapId, key.reduceId)
    // Atomic, lock-consistent capture of (ordered spilled ids, live in-memory frames). The
    // capture closure only reads the lock-free spilled-segment list, honoring the
    // snapshotEnvelopedWith contract (no blocking, no inverse lock acquisition).
    val (spilledIds, inMemoryFrames) = buffer.snapshotEnvelopedWith {
      if (manager != null) manager.spilledBlockIds(bufferKey) else Seq.empty[BlockId]
    }

    // Read each spilled segment back through the spill manager (TempLocalBlockId via
    // BlockManager.getLocalBytes), preserving spill order, then append the live in-memory frames.
    val parts = new ArrayBuffer[Array[Byte]](spilledIds.size + 1)
    spilledIds.foreach { id =>
      manager.readSpilledSegment(id) match {
        case Some(bytes) => parts += bytes
        case None =>
          // A recorded segment is gone: serving a short read would silently lose data, so fail
          // the fetch and let the consumer surface it for lineage-based recompute.
          throw new IllegalStateException(
            s"Spilled segment $id for streaming block $blockId is no longer readable")
      }
    }
    val inMemoryBytes = inMemoryFrames.toArray
    if (inMemoryBytes.length > 0) {
      parts += inMemoryBytes
    }
    parts
  }

  /**
   * Publishes a completed map task's streamed output to the standard durable shuffle `.data` and
   * `.index` files, returning the per-reduce-partition ENVELOPED byte lengths.
   *
   * ==Why this exists (the streaming data plane)==
   * While the producer executor is alive, a reduce task fetches a partition straight from the
   * producer's in-memory [[StreamingBuffer]] (and spill segments) via [[getBlockData]] ->
   * [[serveStreamingPartition]], which is the low-latency streaming path. That in-memory state,
   * however, is executor-local: a remote executor reaching the producer through the external
   * shuffle service, a producer that has been decommissioned, or any fetch after the shuffle's
   * in-memory buffers are released would otherwise find nothing to serve. Writing the SAME
   * canonical enveloped bytes to the standard `.data`/`.index` files (through the composed
   * [[IndexShuffleBlockResolver]], the least-modification path of AAP 0.4.2) makes the streamed
   * output remotely fetchable by the existing shuffle services and recoverable across executor
   * restarts -- closing the local-mode-bias gap without a bespoke streaming transport service.
   * The map-side writer calls this exactly once on a successful write, after all partitions are
   * finalized and before the buffers are released, so both the live and durable views serve
   * identical bytes for the shuffle's whole lifetime.
   *
   * ==Format invariant==
   * For each reduce partition the bytes written here are exactly the
   * [[org.apache.spark.shuffle.streaming.network.StreamingBlockEnvelope]] frames produced by
   * [[collectEnvelopedParts]] -- the same frames [[serveStreamingPartition]] serves in memory and
   * the same frames written to disk on spill (the dual-channel wire/persist invariant). The
   * returned lengths are therefore the enveloped (header-inclusive) sizes the reduce side must
   * fetch; the writer ships them in the `MapStatus`, and the `.index` offsets the inner resolver
   * writes (a running prefix sum of these lengths) align exactly with the `.data` layout. CRC32C
   * integrity travels inside each envelope, so no separate `.checksum` file is written
   * (`checksums = Array.empty`); the reduce-side reader verifies every frame on fetch.
   *
   * @param shuffleId the shuffle being committed
   * @param mapId the map (producer) task id
   * @param numPartitions the number of reduce partitions (the length of the returned array)
   * @return the enveloped byte length of every reduce partition, indexed by reduce id
   */
  def commitDurableMapOutput(shuffleId: Int, mapId: Long, numPartitions: Int): Array[Long] = {
    val dataFile = indexResolver.getDataFile(shuffleId, mapId)
    val dataTmp = indexResolver.createTempFile(dataFile)
    val lengths = new Array[Long](numPartitions)
    val out: OutputStream = new BufferedOutputStream(new FileOutputStream(dataTmp))
    Utils.tryWithSafeFinally {
      var reduceId = 0
      while (reduceId < numPartitions) {
        val key = BlockKey(shuffleId, mapId, reduceId)
        val buffer = buffers.get(key)
        if (buffer != null) {
          // Serve the SAME enveloped parts the in-memory path serves, written straight to the
          // data stream so a large partition never has to be flattened into one array on disk.
          val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
          val parts = collectEnvelopedParts(blockId, key, buffer)
          var partitionLen = 0L
          parts.foreach { part =>
            out.write(part)
            partitionLen += part.length.toLong
          }
          lengths(reduceId) = partitionLen
        }
        // An untracked (or never-written) partition contributes a zero-length entry, exactly as
        // the sort-based path records empty partitions; the index still gets a valid offset.
        reduceId += 1
      }
    } {
      out.close()
    }
    // Atomically install the .index (prefix-sum offsets of `lengths`) and rename the temp .data
    // into place through the inner resolver, reusing the battle-tested commit + dedup logic.
    indexResolver.writeMetadataFileAndCommit(shuffleId, mapId, lengths, Array.empty[Long], dataTmp)
    logDebug(log"Committed durable streaming map output shuffle=" +
      log"${MDC(LogKeys.SHUFFLE_ID, shuffleId)} map=${MDC(LogKeys.MAP_ID, mapId)} partitions=" +
      log"${MDC(LogKeys.NUM_PARTITIONS, numPartitions)} totalBytes=" +
      log"${MDC(LogKeys.NUM_BYTES, lengths.sum)}")
    lengths
  }
}

/**
 * Companion holding the compound tracking key used by [[StreamingShuffleBlockResolver]].
 */
private[spark] object StreamingShuffleBlockResolver {

  /**
   * Logical identity of a single streamed reduce partition: the `(shuffleId, mapId, reduceId)`
   * triple. A case class gives correct structural `equals`/`hashCode` for use as a
   * [[java.util.concurrent.ConcurrentHashMap]] key.
   *
   * @param shuffleId the shuffle id
   * @param mapId the map (producer) task id
   * @param reduceId the reduce partition id
   */
  private final case class BlockKey(shuffleId: Int, mapId: Long, reduceId: Int)
}
