package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.collection.mutable

private[graph_api] class GraphDataManagerImpl(sc: spark.SparkContext,
                                              repositoryPath: String)
    extends GraphDataManager {

  private val dataCache = mutable.Map[UUID, GraphData]()

  private def pathPrefix(graph: BigGraph) = "%s/%s".format(repositoryPath, graph.gUID)
  private def verticesPath(graph: BigGraph) = pathPrefix(graph) + ".vertices"
  private def edgesPath(graph: BigGraph) = pathPrefix(graph) + ".edges"

  private def hasGraph(graph: BigGraph): Boolean = {
    val verticesHadoopPath = new hadoop.fs.Path(
        verticesPath(graph) + "/_SUCCESS")
    val edgesHadoopPath = new hadoop.fs.Path(edgesPath(graph) + "/_SUCCESS")
    val fs = verticesHadoopPath.getFileSystem(new hadoop.conf.Configuration())
    return fs.exists(verticesHadoopPath) && fs.exists(edgesHadoopPath)
  }

  private def tryToLoadGraphData(graph: BigGraph): Option[GraphData] = {
    if (hasGraph(graph)) {
      val (vertices, edges) = GraphIO.loadFromObjectFile(sc, pathPrefix(graph))
      Some(new SimpleGraphData(graph, vertices, edges))
    } else {
      None
    }
  }

  def obtainData(bigGraph: BigGraph): GraphData = {
    val gUID = bigGraph.gUID
    this.synchronized {
      if (!dataCache.contains(gUID)) {
        dataCache(gUID) = tryToLoadGraphData(bigGraph).getOrElse(
            bigGraph.operation.execute(bigGraph, this))
      }
    }
    dataCache(gUID)
  }

  def saveDataToDisk(bigGraph: BigGraph) = {
    if (!hasGraph(bigGraph)) {
      GraphIO.saveAsObjectFile(obtainData(bigGraph), pathPrefix(bigGraph))
    }
  }

  // This is pretty sad, but I haven't find an automatic way to get the number of cores.
  private val numCoresPerExecutor = scala.util.Properties.envOrElse(
      "NUM_CORES_PER_EXECUTOR", "4").toInt
  private val bytesInGb = scala.math.pow(2, 30)
  def runtimeContext =
    RuntimeContext(
      sparkContext = sc,
      numAvailableCores = sc.getExecutorStorageStatus.size * numCoresPerExecutor,
      availableCacheMemoryGB = sc.getExecutorMemoryStatus.values.map(_._2).sum.toDouble / bytesInGb)
}
