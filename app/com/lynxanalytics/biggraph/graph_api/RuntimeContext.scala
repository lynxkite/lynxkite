package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import scala.util.Random

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.graph_util.FileBasedObjectCache

case class Broadcast[T](filename: Filename) {
  def get: T = FileBasedObjectCache.get[T](filename)
}

case class RuntimeContext(sparkContext: spark.SparkContext,
                          broadcastDirectory: Filename,
                          // The number of cores available for computations.
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
