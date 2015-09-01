// Some handy extensions to the SparkContext interface.
package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import scala.util.Random

import com.lynxanalytics.biggraph.graph_api.io.EntityIO
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.FileBasedObjectCache

case class Broadcast[T](filename: HadoopFile) {
  def get: T = FileBasedObjectCache.get[T](filename)
}

case class RuntimeContext(sparkContext: spark.SparkContext,
                          broadcastDirectory: HadoopFile) {
  // A suitable partitioner for an RDD of N rows.
  def partitionerForNRows(n: Long): spark.Partitioner =
    new spark.HashPartitioner((n / EntityIO.verticesPerPartition).ceil.toInt max 1)
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
