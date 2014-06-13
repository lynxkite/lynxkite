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
    } else None
  }

  private def tryToLoadEdgeBundleData(edgeBundle: EdgeBundle): Option[EdgeBundleData] = {
    if (hasEntity(edgeBundle)) {
      Some(new EdgeBundleData(
        edgeBundle,
        entityPath(edgeBundle).loadObjectFile[(ID, Edge)](sc)))
    } else None
  }

  private def tryToLoadVertexAttributeData[T](
    vertexAttribute: VertexAttribute[T]): Option[VertexAttributeData[T]] = {
    if (hasEntity(vertexAttribute)) {
      Some(new VertexAttributeData[T](
        vertexAttribute,
        entityPath(vertexAttribute).loadObjectFile[(ID, T)](sc)))
    } else None
  }

  private def tryToLoadEdgeAttributeData[T](
    edgeAttribute: EdgeAttribute[T]): Option[EdgeAttributeData[T]] = {
    if (hasEntity(edgeAttribute)) {
      Some(new EdgeAttributeData[T](
        edgeAttribute,
        entityPath(edgeAttribute).loadObjectFile[(ID, T)](sc)))
    } else None
  }

  def execute(instance: MetaGraphOperationInstance): DataSet = {
    val inputs = instance.inputs
    val inputDatas = DataSet(
      inputs.vertexSets.mapValues(get(_)),
      inputs.edgeBundles.mapValues(get(_)),
      inputs.vertexAttributes.mapValues(get(_)),
      inputs.edgeAttributes.mapValues(get(_)))
    val outputBuilder = new DataSetBuilder(instance)
    instance.operation.execute(inputDatas, outputBuilder, runtimeContext)
    outputBuilder.toDataSet
  }

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

  def get(edgeBundle: EdgeBundle): EdgeBundleData = {
    val gUID = edgeBundle.gUID
    this.synchronized {
      if (!edgeBundleCache.contains(gUID)) {
        edgeBundleCache(gUID) = tryToLoadEdgeBundleData(edgeBundle).getOrElse(
          execute(edgeBundle.source).edgeBundles(edgeBundle.name))
      }
    }
    edgeBundleCache(gUID)
  }

  def get[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T] = {
    val gUID = vertexAttribute.gUID
    this.synchronized {
      if (!vertexAttributeCache.contains(gUID)) {
        vertexAttributeCache(gUID) = tryToLoadVertexAttributeData(vertexAttribute).getOrElse(
          execute(vertexAttribute.source).vertexAttributes(vertexAttribute.name))
      }
    }
    implicit val tagForT = vertexAttribute.typeTag
    vertexAttributeCache(gUID).runtimeSafeCast[T]
  }

  def get[T](edgeAttribute: EdgeAttribute[T]): EdgeAttributeData[T] = {
    val gUID = edgeAttribute.gUID
    this.synchronized {
      if (!edgeAttributeCache.contains(gUID)) {
        edgeAttributeCache(gUID) = tryToLoadEdgeAttributeData(edgeAttribute).getOrElse(
          execute(edgeAttribute.source).edgeAttributes(edgeAttribute.name))
      }
    }
    implicit val tagForT = edgeAttribute.typeTag
    edgeAttributeCache(gUID).runtimeSafeCast[T]
  }

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
      bigGraphLogger.info(s"Saving entity $entity ...")
      entityPath(entity).saveAsObjectFile(get(entity).rdd)
      bigGraphLogger.info(s"Entity $entity saved.")
    } else {
      bigGraphLogger.info(s"Skip saving entity $entity as it's already saved.")
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
