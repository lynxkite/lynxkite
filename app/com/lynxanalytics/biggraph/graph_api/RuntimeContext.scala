package com.lynxanalytics.biggraph.graph_api

import org.apache.spark

case class RuntimeContext(sparkContext: spark.SparkContext,
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
}
