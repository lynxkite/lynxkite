package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable
import scala.concurrent._
import ExecutionContext.Implicits.global

import com.lynxanalytics.biggraph.bigGraphLogger
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.SortedRDD

class DataManager(sc: spark.SparkContext,
                  val repositoryPath: Filename) {
  private val entityCache = mutable.Map[UUID, Future[EntityData]]()

  private def entityPath(entity: MetaGraphEntity) =
    repositoryPath / entity.gUID.toString

  private def successPath(basePath: Filename): Filename = basePath / "_SUCCESS"

  private def serializedScalarFileName(basePath: Filename): Filename = basePath / "serialized_data"

  private def hasEntityOnDisk(entity: MetaGraphEntity): Boolean =
    successPath(entityPath(entity)).exists

  private def hasEntity(entity: MetaGraphEntity): Boolean = entityCache.contains(entity.gUID)

  private def load(vertexSet: VertexSet): Future[VertexSetData] = {
    future {
      blocking {
        new VertexSetData(
          vertexSet,
          SortedRDD.fromUnsorted(entityPath(vertexSet).loadObjectFile[(ID, Unit)](sc)
            .partitionBy(runtimeContext.defaultPartitioner)))
      }
    }
  }

  private def load(edgeBundle: EdgeBundle): Future[EdgeBundleData] = {
    future {
      blocking {
        new EdgeBundleData(
          edgeBundle,
          SortedRDD.fromUnsorted(entityPath(edgeBundle).loadObjectFile[(ID, Edge)](sc)
            .partitionBy(runtimeContext.defaultPartitioner)))
      }
    }
  }

  private def load[T](vertexAttribute: VertexAttribute[T]): Future[VertexAttributeData[T]] = {
    implicit val ct = vertexAttribute.classTag
    for {
      vs <- getFuture(vertexAttribute.vertexSet)
    } yield {
      // We do our best to colocate partitions to corresponding vertex set partitions.
      blocking {
        val vsRDD = vs.rdd.cache
        val rawRDD = SortedRDD.fromUnsorted(entityPath(vertexAttribute).loadObjectFile[(ID, T)](sc)
          .partitionBy(vsRDD.partitioner.get))
        new VertexAttributeData[T](
          vertexAttribute,
          // This join does nothing except enforcing colocation.
          vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
      }
    }
  }

  private def load[T](scalar: Scalar[T]): Future[ScalarData[T]] = {
    future {
      blocking {
        val ois = new java.io.ObjectInputStream(serializedScalarFileName(entityPath(scalar)).open())
        val value = ois.readObject.asInstanceOf[T]
        ois.close()
        new ScalarData[T](scalar, value)
      }
    }
  }

  private def load(entity: MetaGraphEntity): Future[EntityData] = {
    entity match {
      case vs: VertexSet => load(vs)
      case eb: EdgeBundle => load(eb)
      case va: VertexAttribute[_] => load(va)
      case sc: Scalar[_] => load(sc)
    }
  }

  private def set(entity: MetaGraphEntity, data: Future[EntityData]) = {
    entityCache(entity.gUID) = data
  }

  private def execute(instance: MetaGraphOperationInstance): Unit = {
    val inputs = instance.inputs
    val futureInputs = Future.sequence(
      inputs.all.toSeq.map {
        case (name, entity) =>
          getFuture(entity).map(data => (name, data))
      })
    val outputDataSetFuture = for {
      inputs <- futureInputs
    } yield {
      val inputDatas = DataSet(inputs.toMap)
      blocking {
        instance.run(inputDatas, runtimeContext)
      }
    }
    instance.outputs.all.foreach {
      case (name, entity) =>
        if (instance.operation.isHeavy) {
          set(
            entity,
            outputDataSetFuture
              .flatMap { outputDataSet =>
                val data = outputDataSet(entity.gUID)
                blocking {
                  saveToDisk(data)
                }
                load(entity)
              })
        } else {
          set(entity, outputDataSetFuture.map(_(entity.gUID)))
        }
    }
  }

  def isCalculated(entity: MetaGraphEntity): Boolean = hasEntity(entity) || hasEntityOnDisk(entity)

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = {
    this.synchronized {
      if (!hasEntity(entity)) {
        if (hasEntityOnDisk(entity)) {
          set(entity, load(entity))
        } else {
          execute(entity.source)
        }
      }
    }
  }

  def getFuture(vertexSet: VertexSet): Future[VertexSetData] = {
    loadOrExecuteIfNecessary(vertexSet)
    entityCache(vertexSet.gUID).map(_.asInstanceOf[VertexSetData])
  }

  def getFuture(edgeBundle: EdgeBundle): Future[EdgeBundleData] = {
    loadOrExecuteIfNecessary(edgeBundle)
    entityCache(edgeBundle.gUID).map(_.asInstanceOf[EdgeBundleData])
  }

  def getFuture[T](vertexAttribute: VertexAttribute[T]): Future[VertexAttributeData[T]] = {
    loadOrExecuteIfNecessary(vertexAttribute)
    implicit val tagForT = vertexAttribute.typeTag
    entityCache(vertexAttribute.gUID).map(_.asInstanceOf[VertexAttributeData[_]].runtimeSafeCast[T])
  }

  def getFuture[T](scalar: Scalar[T]): Future[ScalarData[T]] = {
    loadOrExecuteIfNecessary(scalar)
    implicit val tagForT = scalar.typeTag
    entityCache(scalar.gUID).map(_.asInstanceOf[ScalarData[_]].runtimeSafeCast[T])
  }

  def getFuture(entity: MetaGraphEntity): Future[EntityData] = {
    entity match {
      case vs: VertexSet => getFuture(vs)
      case eb: EdgeBundle => getFuture(eb)
      case va: VertexAttribute[_] => getFuture(va)
      case sc: Scalar[_] => getFuture(sc)
    }
  }

  def get(vertexSet: VertexSet): VertexSetData = {
    Await.result(getFuture(vertexSet), duration.Duration.Inf)
  }
  def get(edgeBundle: EdgeBundle): EdgeBundleData = {
    Await.result(getFuture(edgeBundle), duration.Duration.Inf)
  }
  def get[T](vertexAttribute: VertexAttribute[T]): VertexAttributeData[T] = {
    Await.result(getFuture(vertexAttribute), duration.Duration.Inf)
  }
  def get[T](scalar: Scalar[T]): ScalarData[T] = {
    Await.result(getFuture(scalar), duration.Duration.Inf)
  }
  def get(entity: MetaGraphEntity): EntityData = {
    Await.result(getFuture(entity), duration.Duration.Inf)
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
