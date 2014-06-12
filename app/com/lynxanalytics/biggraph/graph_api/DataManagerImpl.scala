package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd
import scala.collection.mutable

import com.lynxanalytics.biggraph.bigGraphLogger
import com.lynxanalytics.biggraph.graph_util.Filename

private[graph_api] class DataManagerImpl(sc: spark.SparkContext,
                                         val repositoryPath: Filename)
    extends DataManager {

  private val vertexSetCache = mutable.Map[UUID, VertexSetData]()
  private val edgeBundleCache = mutable.Map[UUID, EdgeBundleData]()
  private val vertexAttributeCache = mutable.Map[UUID, VertexAttributeData[_]]()
  private val edgeAttributeCache = mutable.Map[UUID, EdgeAttributeData[_]]()

  private def entityPath(entity: MetaGraphEntity) =
    repositoryPath.addPathElement(entity.gUID.toString)

  private def successPath(basePath: Filename): Filename = basePath.addPathElement("_SUCCESS")

  private def hasEntity(entity: MetaGraphEntity): Boolean = successPath(entityPath(entity)).exists

  private def tryToLoadVertexSetData(vertexSet: VertexSet): Option[VertexSetData] = {
    if (hasEntity(vertexSet)) {
      Some(new VertexSetData(
        vertexSet,
        entityPath(vertexSet).loadObjectFile[(ID, Unit)](sc)))
    } else {
      None
    }
  }

  def execute(instance: MetaGraphOperationInstance): DataSet = ???

  def get(vertexSet: VertexSet): VertexSetData = {
    val gUID = vertexSet.gUID
    this.synchronized {
      if (!vertexSetCache.contains(gUID)) {
        vertexSetCache(gUID) = tryToLoadVertexSetData(vertexSet).getOrElse(
          execute(vertexSet.source).vertexSets(vertexSet.name))
      }
    }
    vertexSetCache(gUID)
  }

  def get(edgeBundle: EdgeBundle): EdgeBundleData = ???

  def get[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T] = ???

  def get[T](edgeAttribute: EdgeAttribute[T]): EdgeAttributeData[T] = ???

  def get(entity: MetaGraphEntity): EntityData = {
    entity match {
      case vs: VertexSet => get(vs)
      case eb: EdgeBundle => get(eb)
      case va: VertexAttribute[_] => get(va)
      case ea: EdgeAttribute[_] => get(ea)
    }
  }

  def saveToDisk(entity: MetaGraphEntity): Unit = {
    if (!hasEntity(entity)) {
      bigGraphLogger.info(s"Saving entity %entity ...")
      entityPath(entity).saveAsObjectFile(get(entity).rdd)
      bigGraphLogger.info(s"Entity %entity saved.")
    } else {
      bigGraphLogger.info(s"Skip saving entity %entity as it's already saved.")
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
