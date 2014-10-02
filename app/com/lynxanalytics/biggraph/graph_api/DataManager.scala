package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable

import com.lynxanalytics.biggraph.bigGraphLogger
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.SortedRDD

class DataManager(sc: spark.SparkContext,
                  val repositoryPath: Filename) {
  private val vertexSetCache = mutable.Map[UUID, VertexSetData]()
  private val edgeBundleCache = mutable.Map[UUID, EdgeBundleData]()
  private val vertexAttributeCache = mutable.Map[UUID, VertexAttributeData[_]]()
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
      case sc: Scalar[_] => scalarCache.contains(gUID)
    }
  }

  private def load(vertexSet: VertexSet): Unit = {
    vertexSetCache(vertexSet.gUID) = new VertexSetData(
      vertexSet,
      SortedRDD.fromUnsorted(entityPath(vertexSet).loadObjectFile[(ID, Unit)](sc)
        .partitionBy(runtimeContext.defaultPartitioner)))
  }

  private def load(edgeBundle: EdgeBundle): Unit = {
    edgeBundleCache(edgeBundle.gUID) = new EdgeBundleData(
      edgeBundle,
      SortedRDD.fromUnsorted(entityPath(edgeBundle).loadObjectFile[(ID, Edge)](sc)
        .partitionBy(runtimeContext.defaultPartitioner)))
  }

  private def load[T](vertexAttribute: VertexAttribute[T]): Unit = {
    implicit val ct = vertexAttribute.classTag
    // We do our best to colocate partitions to corresponding vertex set partitions.
    val vsRDD = get(vertexAttribute.vertexSet).rdd.cache
    val rawRDD = SortedRDD.fromUnsorted(entityPath(vertexAttribute).loadObjectFile[(ID, T)](sc)
      .partitionBy(vsRDD.partitioner.get))
    vertexAttributeCache(vertexAttribute.gUID) = new VertexAttributeData[T](
      vertexAttribute,
      // This join does nothing except enforcing colocation.
      vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
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
      case sc: Scalar[_] => load(sc)
    }
  }

  private def execute(instance: MetaGraphOperationInstance): Unit = {
    val inputs = instance.inputs
    val inputDatas = DataSet(
      inputs.vertexSets.mapValues(get(_)),
      inputs.edgeBundles.mapValues(get(_)),
      inputs.vertexAttributes.mapValues(get(_)),
      inputs.scalars.mapValues(get(_)))
    if (instance.operation.isHeavy) {
      val outputs = instance.run(inputDatas, runtimeContext).values
      outputs.foreach(data => saveToDisk(data))
      outputs.foreach(data => load(data.entity))
    } else {
      instance.run(inputDatas, runtimeContext).foreach {
        case (uuid, data) =>
          data match {
            case vs: VertexSetData => vertexSetCache(vs.gUID) = vs
            case eb: EdgeBundleData => edgeBundleCache(eb.gUID) = eb
            case va: VertexAttributeData[_] => vertexAttributeCache(va.gUID) = va
            case sc: ScalarData[_] => scalarCache(sc.gUID) = sc
          }
      }
    }
  }

  def isCalculated(entity: MetaGraphEntity): Boolean = hasEntity(entity) || hasEntityOnDisk(entity)

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

  def get[T](scalar: Scalar[T]): ScalarData[T] = {
    loadOrExecuteIfNecessary(scalar)
    implicit val tagForT = scalar.typeTag
    scalarCache(scalar.gUID).runtimeSafeCast[T]
  }

  def get[T](entity: Attribute[T]): AttributeData[T] = {
    entity match {
      case va: VertexAttribute[_] => get(va)
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

  private def saveToDisk(data: EntityData): Unit = {
    this.synchronized {
      val entity = data.entity
      if (!hasEntityOnDisk(data.entity)) {
        bigGraphLogger.info(s"Saving entity $entity ...")
        bigGraphLogger.debug(s"Parallelism is set to ${runtimeContext.numAvailableCores}")
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
      numAvailableCores = ((sc.getExecutorStorageStatus.size - 1) max 1) * numCoresPerExecutor,
      availableCacheMemoryGB = sc.getExecutorMemoryStatus.values.map(_._2).sum.toDouble / bytesInGb)
}
