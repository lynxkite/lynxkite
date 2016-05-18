// Some handy extensions to the SparkContext interface.
package com.lynxanalytics.biggraph.graph_api

import org.apache.spark

import com.lynxanalytics.biggraph.graph_util.HadoopFile

case class RuntimeContext(sparkContext: spark.SparkContext,
                          sqlContext: spark.sql.SQLContext,
                          ioContext: io.IOContext,
                          broadcastDirectory: HadoopFile,
                          dataManager: DataManager) {
  // A suitable partitioner for an RDD of N rows.
  def partitionerForNRows(n: Long): spark.Partitioner =
    new spark.HashPartitioner((n / io.EntityIO.verticesPerPartition).ceil.toInt max 1)
  lazy val onePartitionPartitioner: spark.Partitioner =
    new spark.HashPartitioner(1)
}
object RuntimeContext {
  // The optimal number of sample partitions is the number of tasks can be done in parallel.
  val numSamplePartitions =
    util.Properties.envOrElse("NUM_EXECUTORS", "1").toInt *
      util.Properties.envOrElse("NUM_CORES_PER_EXECUTOR", "1").toInt
}
