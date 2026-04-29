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

package org.apache.spark.shuffle

import java.util.Locale

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.util.Utils

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE:
 * 1. This will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 * 2. This contains a method ShuffleBlockResolver which interacts with External Shuffle Service
 * when it is enabled. Need to pay attention to that, if implementing a custom ShuffleManager, to
 * make sure the custom ShuffleManager could co-exist with External Shuffle Service.
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]


  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from all map outputs of the shuffle.
   *
   * Called on executors by reduce tasks.
   */
  final def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReader(handle, 0, Int.MaxValue, startPartition, endPartition, context, metrics)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from a range of map outputs(startMapIndex to endMapIndex-1, inclusive).
   * If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length of total map
   * outputs of the shuffle in `getMapSizesByExecutorId`.
   *
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}

/**
 * Utility companion object to create a ShuffleManager given a spark configuration.
 */
private[spark] object ShuffleManager {
  def create(conf: SparkConf, isDriver: Boolean): ShuffleManager = {
    Utils.instantiateSerializerOrShuffleManager[ShuffleManager](
      getShuffleManagerClassName(conf), conf, isDriver)
  }

  def getShuffleManagerClassName(conf: SparkConf): String = {
    // Coexistence strategy: this map registers short-name aliases for ShuffleManager
    // implementations selectable via spark.shuffle.manager. The default "sort" continues
    // to dispatch to SortShuffleManager (production-stable, unchanged) per the user
    // directive "Preserve existing sort-based shuffle as production-stable fallback."
    // The "streaming" alias enables opt-in selection of the StreamingShuffleManager via
    // spark.shuffle.manager=streaming and coexists with the default "sort" registration.
    // The class name is stored as a String literal (rather than classOf[...].getName) so
    // that this dispatch table compiles independently of whether the streaming-shuffle
    // implementation classes are present at compile time -- the actual class is loaded
    // reflectively at runtime by Utils.instantiateSerializerOrShuffleManager only when
    // streaming shuffle is opted in. This preserves the "zero cross-contamination"
    // boundary between sort-shuffle and streaming-shuffle code paths and supports
    // gradual / phased introduction of the streaming components.
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      // "streaming" alias for opt-in StreamingShuffleManager (coexists with sort default).
      "streaming" -> "org.apache.spark.shuffle.streaming.StreamingShuffleManager")

    val shuffleMgrName = conf.get(config.SHUFFLE_MANAGER)

    // Boolean-flag activation per AAP Section 0.1.1: streaming shuffle is selected via
    // spark.shuffle.manager=streaming "equivalently via the new boolean
    // spark.shuffle.streaming.enabled=true". The boolean activation path applies only
    // when the operator has not explicitly set spark.shuffle.manager (i.e. the value is
    // the default "sort"). An explicit operator choice always wins so that a user who
    // pinned spark.shuffle.manager=tungsten-sort or any other manager continues to
    // observe their explicit selection regardless of streaming.enabled. When activation
    // is via the boolean flag alone, the dispatch table maps the canonical short name
    // "streaming" to the StreamingShuffleManager FQCN. Coexistence: this branch never
    // changes the dispatch for "tungsten-sort", any FQCN-shaped value, or any explicit
    // operator-chosen alias; the SortShuffleManager remains the production-stable
    // fallback and the default for all unmodified deployments.
    val effectiveMgrName = if (conf.get(config.STREAMING_SHUFFLE_ENABLED) &&
        !conf.contains(config.SHUFFLE_MANAGER.key)) {
      "streaming"
    } else {
      shuffleMgrName
    }

    shortShuffleMgrNames.getOrElse(effectiveMgrName.toLowerCase(Locale.ROOT), effectiveMgrName)
  }
}

