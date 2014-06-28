package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.bigGraphLogger
import com.lynxanalytics.biggraph.graph_util.Filename

class DataManager(sc: spark.SparkContext,
                  val repositoryPath: Filename) {
  private val vertexSetCache = mutable.Map[UUID, VertexSetData]()
  private val edgeBundleCache = mutable.Map[UUID, EdgeBundleData]()
  private val vertexAttributeCache = mutable.Map[UUID, VertexAttributeData[_]]()
  private val edgeAttributeCache = mutable.Map[UUID, EdgeAttributeData[_]]()
  private val scalarCache = mutable.Map[UUID, ScalarData[_]]()

  private def entityPath(entity: MetaGraphEntity) =
    repositoryPath / entity.gUID.toString

  private def successPath(basePath: Filename): Filename = basePath / "_SUCCESS"

  private def serializedScalarFileName(basePath: Filename): Filename = basePath / "serialized_data"

  private def hasEntityOnDisk(entity: MetaGraphEntity): Boolean =
    successPath(entityPath(entity)).exists

  private def hasEntity(entity: MetaGraphEntity): Boolean = {
    val gUID = entity.gUID
    entity match {
      case vs: VertexSet => vertexSetCache.contains(gUID)
      case eb: EdgeBundle => edgeBundleCache.contains(gUID)
      case va: VertexAttribute[_] => vertexAttributeCache.contains(gUID)
      case ea: EdgeAttribute[_] => edgeAttributeCache.contains(gUID)
      case sc: Scalar[_] => scalarCache.contains(gUID)
    }
  }

  private def load(vertexSet: VertexSet): Unit = {
    vertexSetCache(vertexSet.gUID) = new VertexSetData(
      vertexSet,
      entityPath(vertexSet).loadObjectFile[(ID, Unit)](sc)
        .partitionBy(runtimeContext.defaultPartitioner))
  }

  private def load(edgeBundle: EdgeBundle): Unit = {
    edgeBundleCache(edgeBundle.gUID) = new EdgeBundleData(
      edgeBundle,
      entityPath(edgeBundle).loadObjectFile[(ID, Edge)](sc)
        .partitionBy(runtimeContext.defaultPartitioner))
  }

  private def load[T](vertexAttribute: VertexAttribute[T]): Unit = {
    implicit val ct = vertexAttribute.classTag
    vertexAttributeCache(vertexAttribute.gUID) = new VertexAttributeData[T](
      vertexAttribute,
      entityPath(vertexAttribute).loadObjectFile[(ID, T)](sc)
        .partitionBy(runtimeContext.defaultPartitioner))
  }

  private def load[T](edgeAttribute: EdgeAttribute[T]): Unit = {
    implicit val ct = edgeAttribute.classTag
    edgeAttributeCache(edgeAttribute.gUID) = new EdgeAttributeData[T](
      edgeAttribute, entityPath(edgeAttribute).loadObjectFile[(ID, T)](sc)
        .partitionBy(runtimeContext.defaultPartitioner))
  }

  private def load[T](scalar: Scalar[T]): Unit = {
    val ois = new java.io.ObjectInputStream(serializedScalarFileName(entityPath(scalar)).open())
    val value = ois.readObject.asInstanceOf[T]
    ois.close()
    scalarCache(scalar.gUID) = new ScalarData[T](scalar, value)
  }

  private def load(entity: MetaGraphEntity): Unit = {
    entity match {
      case vs: VertexSet => load(vs)
      case eb: EdgeBundle => load(eb)
      case va: VertexAttribute[_] => load(va)
      case ea: EdgeAttribute[_] => load(ea)
      case sc: Scalar[_] => load(sc)
    }
  }

  private def execute(instance: MetaGraphOperationInstance): Unit = {
    val inputs = instance.inputs
    val inputDatas = DataSet(
      inputs.vertexSets.mapValues(get(_)),
      inputs.edgeBundles.mapValues(get(_)),
      inputs.vertexAttributes.mapValues(get(_)),
      inputs.edgeAttributes.mapValues(get(_)),
      inputs.scalars.mapValues(get(_)))
    val outputBuilder = new DataSetBuilder(instance)
    instance.operation.execute(inputDatas, outputBuilder, runtimeContext)
    val output = outputBuilder.toDataSet
    output.vertexSets.values.foreach(vs => vertexSetCache(vs.gUID) = vs)
    output.edgeBundles.values.foreach(eb => edgeBundleCache(eb.gUID) = eb)
    output.vertexAttributes.values.foreach(va => vertexAttributeCache(va.gUID) = va)
    output.edgeAttributes.values.foreach(ea => edgeAttributeCache(ea.gUID) = ea)
    output.scalars.values.foreach(sc => scalarCache(sc.gUID) = sc)
  }

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = {
    this.synchronized {
      if (!hasEntity(entity)) {
        if (hasEntityOnDisk(entity)) {
          load(entity)
        } else {
          execute(entity.source)
        }
      }
    }
  }

  def get(vertexSet: VertexSet): VertexSetData = {
    loadOrExecuteIfNecessary(vertexSet)
    vertexSetCache(vertexSet.gUID)
  }

  def get(edgeBundle: EdgeBundle): EdgeBundleData = {
    loadOrExecuteIfNecessary(edgeBundle)
    edgeBundleCache(edgeBundle.gUID)
  }

  def get[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T] = {
    loadOrExecuteIfNecessary(vertexAttribute)
    implicit val tagForT = vertexAttribute.typeTag
    vertexAttributeCache(vertexAttribute.gUID).runtimeSafeCast[T]
  }

  def get[T](edgeAttribute: EdgeAttribute[T]): EdgeAttributeData[T] = {
    loadOrExecuteIfNecessary(edgeAttribute)
    implicit val tagForT = edgeAttribute.typeTag
    edgeAttributeCache(edgeAttribute.gUID).runtimeSafeCast[T]
  }

  def get[T](scalar: Scalar[T]): ScalarData[T] = {
    loadOrExecuteIfNecessary(scalar)
    implicit val tagForT = scalar.typeTag
    scalarCache(scalar.gUID).runtimeSafeCast[T]
  }

  def get[T](entity: Attribute[T]): AttributeData[T] = {
    entity match {
      case va: VertexAttribute[_] => get(va)
      case ea: EdgeAttribute[_] => get(ea)
    }
  }

  def get(entity: MetaGraphEntity): EntityData = {
    entity match {
      case vs: VertexSet => get(vs)
      case eb: EdgeBundle => get(eb)
      case a: Attribute[_] => get(a)
      case sc: Scalar[_] => get(sc)
    }
  }

  def saveToDisk(entity: MetaGraphEntity): Unit = {
    this.synchronized {
      if (!hasEntityOnDisk(entity)) {
        bigGraphLogger.info(s"Saving entity $entity ...")
        val data = get(entity)
        data match {
          case rddData: EntityRDDData =>
            entityPath(entity).saveAsObjectFile(rddData.rdd)
          case scalarData: ScalarData[_] => {
            val targetDir = entityPath(entity)
            targetDir.mkdirs
            val oos = new java.io.ObjectOutputStream(serializedScalarFileName(targetDir).create())
            oos.writeObject(scalarData.value)
            oos.close()
            successPath(targetDir).createFromStrings("")
          }
        }
        bigGraphLogger.info(s"Entity $entity saved.")
      } else {
        bigGraphLogger.info(s"Skip saving entity $entity as it's already saved.")
      }
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
