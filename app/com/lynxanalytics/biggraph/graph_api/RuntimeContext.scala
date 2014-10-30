package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import scala.collection.mutable.HashMap
import scala.ref.WeakReference
import scala.util.Random

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Filename

private object FileBasedBroadcastRepository {
  private val cache = HashMap[Filename, WeakReference[AnyRef]]()
  def getObjectFrom(filename: Filename): AnyRef = synchronized {
    cache.get(filename).flatMap(_.get).getOrElse {
      val value = filename.loadObjectKryo.asInstanceOf[AnyRef]
      cache(filename) = WeakReference(value)
      value
    }
  }
}

case class Broadcast[T](filename: Filename) {
  def get: T = FileBasedBroadcastRepository.getObjectFrom(filename).asInstanceOf[T]
}

case class RuntimeContext(sparkContext: spark.SparkContext,
                          broadcastDirectory: Filename,
                          // The number of cores available for computaitons.
                          numAvailableCores: Int,
                          // Total memory available for caching RDDs.
                          availableCacheMemoryGB: Double) {
  // This is set to 1 in tests to improve their performance on small data.
  private lazy val defaultPartitionsPerCore =
    System.getProperty("biggraph.default.partitions.per.core", "1").toInt
  lazy val defaultPartitioner: spark.Partitioner =
    new spark.HashPartitioner(numAvailableCores * defaultPartitionsPerCore)
  lazy val onePartitionPartitioner: spark.Partitioner =
    new spark.HashPartitioner(1)

  def broadcast[T](value: T): Broadcast[T] = {
    val filename = broadcastDirectory / Random.alphanumeric.take(10).mkString.toLowerCase
    log.info("Creating broadcast file at " + filename)
    filename.createFromObjectKryo(value)
    log.info("Broadcast file at " + filename + " created.")
    Broadcast[T](filename)
  }
}
