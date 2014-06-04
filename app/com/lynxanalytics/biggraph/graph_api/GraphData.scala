package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd

import com.lynxanalytics.biggraph.graph_util.Filename

import attributes.DenseAttributes

/**
 * Generic representation of a graph as RDDs.
 *
 * Different implementations might have different performance tradeoffs.
 * Use GraphDataManager to get the data for a BigGraph.
 */
trait GraphData {
  val bigGraph: BigGraph

  def vertices: VertexRDD
  def edges: EdgeRDD
  def triplets: TripletRDD
}

/*
 * Interface for obtaining GraphData for a given BigGraph.
 */
abstract class GraphDataManager {
  def obtainData(bigGraph: BigGraph): GraphData

  // Saves the given BigGraph's data to disk.
  def saveDataToDisk(bigGraph: BigGraph)

  // Returns information about the current running enviroment.
  // Typically used by operations to optimize their execution.
  def runtimeContext: RuntimeContext

  def repositoryPath: Filename
}

object GraphDataManager {
  def apply(sparkContext: spark.SparkContext, repositoryPath: Filename): GraphDataManager =
    new GraphDataManagerImpl(sparkContext, repositoryPath)
}

case class RuntimeContext(sparkContext: spark.SparkContext,
                          // The number of cores available for computaitons.
                          numAvailableCores: Int,
                          // Total memory available for caching RDDs.
                          availableCacheMemoryGB: Double) {
  lazy val defaultPartitioner: spark.Partitioner =
    new spark.HashPartitioner(numAvailableCores * 3)
}

