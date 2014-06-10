package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_util.Filename

private[graph_api] class GraphDataManagerImpl(sc: spark.SparkContext,
                                              val repositoryPath: Filename)
    extends GraphDataManager {

  private val dataCache = mutable.Map[UUID, GraphData]()

  private def pathPrefix(bigGraph: BigGraph) = repositoryPath.addPathElement(bigGraph.gUID.toString)
  private def verticesPath(bigGraph: BigGraph) = GraphIO.verticesPath(pathPrefix(bigGraph))
  private def edgesPath(bigGraph: BigGraph) = GraphIO.edgesPath(pathPrefix(bigGraph))
  private def successPath(basePath: Filename): Filename = basePath.addPathElement("_SUCCESS")

  private def hasGraph(bigGraph: BigGraph): Boolean = {
    return successPath(verticesPath(bigGraph)).exists && successPath(edgesPath(bigGraph)).exists
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

  // Cannot set this automatically (SPARK-2095). Default to a low value for tests.
  private val numCoresPerExecutor = scala.util.Properties.envOrElse(
    "NUM_CORES_PER_EXECUTOR", "1").toInt
  private val bytesInGb = scala.math.pow(2, 30)
  def runtimeContext =
    RuntimeContext(
      sparkContext = sc,
      numAvailableCores = sc.getExecutorStorageStatus.size * numCoresPerExecutor,
      availableCacheMemoryGB = sc.getExecutorMemoryStatus.values.map(_._2).sum.toDouble / bytesInGb)
}
