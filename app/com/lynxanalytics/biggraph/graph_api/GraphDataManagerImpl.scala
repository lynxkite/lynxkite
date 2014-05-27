package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.collection.mutable

private[graph_api] class GraphDataManagerImpl(
  sc: spark.SparkContext,
  val repositoryPath: String)
    extends GraphDataManager {

  private val dataCache = mutable.Map[UUID, GraphData]()

  private def pathPrefix(bigGraph: BigGraph) = "%s/%s".format(repositoryPath, bigGraph.gUID)
  private def verticesPath(bigGraph: BigGraph) = GraphIO.verticesPath(pathPrefix(bigGraph))
  private def edgesPath(bigGraph: BigGraph) = GraphIO.edgesPath(pathPrefix(bigGraph))

  private def hasGraph(bigGraph: BigGraph): Boolean = {
    val verticesHadoopPath = new hadoop.fs.Path(
      verticesPath(bigGraph) + "/_SUCCESS")
    val edgesHadoopPath = new hadoop.fs.Path(edgesPath(bigGraph) + "/_SUCCESS")
    val fs = verticesHadoopPath.getFileSystem(new hadoop.conf.Configuration())
    return fs.exists(verticesHadoopPath) && fs.exists(edgesHadoopPath)
  }

  private def tryToLoadGraphData(bigGraph: BigGraph): Option[GraphData] = {
    if (hasGraph(bigGraph)) {
      val (vertices, edges) = GraphIO.loadFromObjectFile(sc, pathPrefix(bigGraph))
      Some(new SimpleGraphData(bigGraph, vertices, edges))
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
