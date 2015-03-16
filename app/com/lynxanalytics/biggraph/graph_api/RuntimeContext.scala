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
                          // Memory per core that can be used for RDD work.
                          workMemoryPerCore: Long) {
  val bytesPerPartition = workMemoryPerCore / 2 // Make sure we fit 2 copies.
  val defaultPartitions = numAvailableCores
  // A suitable partitioner for N bytes.
  def partitionerForNBytes(n: Long): spark.Partitioner =
    new spark.HashPartitioner((n / bytesPerPartition).toInt max defaultPartitions)
  lazy val defaultPartitioner: spark.Partitioner =
    new spark.HashPartitioner(defaultPartitions)
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
