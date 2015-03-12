package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.concurrent.TrieMap
import scala.concurrent._
import ExecutionContext.Implicits.global

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.Filename
import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD

class DataManager(sc: spark.SparkContext,
                  val repositoryPath: Filename) {
  private val instanceOutputCache = TrieMap[UUID, Future[Map[UUID, EntityData]]]()
  private val entityCache = TrieMap[UUID, Future[EntityData]]()

  // This can be switched to false to enter "demo mode" where no new calculations are allowed.
  var computationAllowed = true

  private def instancePath(instance: MetaGraphOperationInstance) =
    repositoryPath / "operations" / instance.gUID.toString

  private def entityPath(entity: MetaGraphEntity) =
    if (entity.isInstanceOf[Scalar[_]]) {
      repositoryPath / "scalars" / entity.gUID.toString
    } else {
      repositoryPath / "entities" / entity.gUID.toString
    }

  private def successPath(basePath: Filename): Filename = basePath / "_SUCCESS"

  private def serializedScalarFileName(basePath: Filename): Filename = basePath / "serialized_data"

  private def hasEntityOnDisk(entity: MetaGraphEntity): Boolean =
    (entity.source.operation.isHeavy || entity.isInstanceOf[Scalar[_]]) &&
      successPath(instancePath(entity.source)).exists &&
      successPath(entityPath(entity)).exists

  private def hasEntity(entity: MetaGraphEntity): Boolean = entityCache.contains(entity.gUID)

  // How much bigger the in-memory representation is, compared to the serialized file size.
  private lazy val kryoExplosion =
    System.getProperty("biggraph.kryo.explosion", "7").toInt
  private def load(vertexSet: VertexSet): Future[VertexSetData] = {
    future {
      val fn = entityPath(vertexSet)
      val bytes = (fn / "*").globLength
      // Repartition for optimal processing performance.
      val partitioner = runtimeContext.partitionerForNBytes(bytes * kryoExplosion)
      val rdd = fn.loadObjectFile[(ID, Unit)](sc).partitionBy(partitioner)
      new VertexSetData(vertexSet, SortedRDD.fromUnsorted(rdd))
    }
  }

  private def load(edgeBundle: EdgeBundle): Future[EdgeBundleData] = {
    getFuture(edgeBundle.idSet).map { idSet =>
      // We do our best to colocate partitions to corresponding vertex set partitions.
      val idsRDD = idSet.rdd.cache
      val rawRDD = entityPath(edgeBundle)
        .loadObjectFile[(ID, Edge)](sc)
        .toSortedRDD(idsRDD.partitioner.get)
      new EdgeBundleData(
        edgeBundle,
        idsRDD.sortedJoin(rawRDD).mapValues { case (_, edge) => edge })
    }
  }

  private def load[T](attribute: Attribute[T]): Future[AttributeData[T]] = {
    implicit val ct = attribute.classTag
    getFuture(attribute.vertexSet).map { vs =>
      // We do our best to colocate partitions to corresponding vertex set partitions.
      val vsRDD = vs.rdd.cache
      val rawRDD = SortedRDD.fromUnsorted(entityPath(attribute).loadObjectFile[(ID, T)](sc)
        .partitionBy(vsRDD.partitioner.get))
      new AttributeData[T](
        attribute,
        // This join does nothing except enforcing colocation.
        vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
    }
  }

  private def load[T](scalar: Scalar[T]): Future[ScalarData[T]] = {
    future {
      blocking {
        log.info(s"PERF Loading scalar $scalar of GUID ${scalar.gUID} from disk")
        val ois = new java.io.ObjectInputStream(serializedScalarFileName(entityPath(scalar)).open())
        val value = ois.readObject.asInstanceOf[T]
        ois.close()
        log.info(s"PERF Scalar of GUID ${scalar.gUID} loaded from disk")
        new ScalarData[T](scalar, value)
      }
    }
  }

  private def load(entity: MetaGraphEntity): Future[EntityData] = {
    log.info(s"PERF Found entity $entity of GUID ${entity.gUID} on disk")
    entity match {
      case vs: VertexSet => load(vs)
      case eb: EdgeBundle => load(eb)
      case va: Attribute[_] => load(va)
      case sc: Scalar[_] => load(sc)
    }
  }

  private def set(entity: MetaGraphEntity, data: Future[EntityData]) = synchronized {
    entityCache(entity.gUID) = data
    data.onFailure {
      case _ => synchronized { entityCache.remove(entity.gUID) }
    }
  }

  private def execute(instance: MetaGraphOperationInstance): Future[Map[UUID, EntityData]] = {
    val inputs = instance.inputs
    val futureInputs = Future.sequence(
      inputs.all.toSeq.map {
        case (name, entity) =>
          getFuture(entity).map(data => (name, data))
      })
    futureInputs.map { inputs =>
      val inputDatas = DataSet(inputs.toMap)
      instance.outputs.scalars.values
        .foreach(scalar => log.info(s"PERF Computing scalar $scalar of GUID ${scalar.gUID}"))
      val outputDatas = blocking {
        instance.run(inputDatas, runtimeContext)
      }
      blocking {
        if (instance.operation.isHeavy) {
          outputDatas.values.foreach { entityData =>
            saveToDisk(entityData)
          }
        } else {
          // We still save all scalars even for non-heavy operations.
          outputDatas.values.foreach { entityData =>
            if (entityData.isInstanceOf[ScalarData[_]]) saveToDisk(entityData)
          }
        }
        // Mark the operation as complete. Entities may not be loaded from incomplete operations.
        // The reason for this is that an operation may give different results if the number of
        // partitions is different. So for consistency, all outputs must be from the same run.
        successPath(instancePath(instance)).createFromStrings("")
      }
      instance.outputs.scalars.values
        .foreach(scalar => log.info(s"PERF Computed scalar of GUID ${scalar.gUID}"))
      outputDatas
    }
  }

  private def getInstanceFuture(
    instance: MetaGraphOperationInstance): Future[Map[UUID, EntityData]] = synchronized {

    val gUID = instance.gUID
    if (!instanceOutputCache.contains(gUID)) {
      instanceOutputCache(gUID) = execute(instance)
      instanceOutputCache(gUID).onFailure {
        case _ => synchronized { instanceOutputCache.remove(gUID) }
      }
    }
    instanceOutputCache(gUID)
  }

  def isCalculated(entity: MetaGraphEntity): Boolean = hasEntity(entity) || hasEntityOnDisk(entity)

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = synchronized {
    if (!hasEntity(entity)) {
      if (hasEntityOnDisk(entity)) {
        // If on disk already, we just load it.
        set(entity, load(entity))
      } else {
        assert(computationAllowed, "DEMO MODE, you cannot start new computations")
        // Otherwise we schedule execution of its operation.
        val instance = entity.source
        val instanceFuture = getInstanceFuture(instance)
        set(
          entity,
          // And the entity will have to wait until its full completion (including saves).
          if (instance.operation.isHeavy && !entity.isInstanceOf[Scalar[_]]) {
            instanceFuture.flatMap(_ => load(entity))
          } else {
            instanceFuture.map(_(entity.gUID))
          })
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

  def getFuture[T](attribute: Attribute[T]): Future[AttributeData[T]] = {
    loadOrExecuteIfNecessary(attribute)
    implicit val tagForT = attribute.typeTag
    entityCache(attribute.gUID).map(_.asInstanceOf[AttributeData[_]].runtimeSafeCast[T])
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
      case va: Attribute[_] => getFuture(va)
      case sc: Scalar[_] => getFuture(sc)
    }
  }

  def get(vertexSet: VertexSet): VertexSetData = {
    Await.result(getFuture(vertexSet), duration.Duration.Inf)
  }
  def get(edgeBundle: EdgeBundle): EdgeBundleData = {
    Await.result(getFuture(edgeBundle), duration.Duration.Inf)
  }
  def get[T](attribute: Attribute[T]): AttributeData[T] = {
    Await.result(getFuture(attribute), duration.Duration.Inf)
  }
  def get[T](scalar: Scalar[T]): ScalarData[T] = {
    Await.result(getFuture(scalar), duration.Duration.Inf)
  }
  def get(entity: MetaGraphEntity): EntityData = {
    Await.result(getFuture(entity), duration.Duration.Inf)
  }

  def cache(entity: MetaGraphEntity): Unit = {
    // We do not cache anything in demo mode.
    if (!computationAllowed) return
    val data = get(entity)
    data match {
      case rddData: EntityRDDData => rddData.rdd.cacheBackingArray()
      case _ => ()
    }
  }

  private def saveToDisk(data: EntityData): Unit = {
    val entity = data.entity
    val doesNotExist = !entityPath(entity).exists() || entityPath(entity).delete()
    assert(doesNotExist, s"Cannot delete directory of entity $entity")
    log.info(s"Saving entity $entity ...")
    data match {
      case rddData: EntityRDDData =>
        log.info(s"PERF Instantiating entity $entity of GUID ${entity.gUID} on disk")
        entityPath(entity).saveAsObjectFile(rddData.rdd)
        log.info(s"PERF Instantiated entity of GUID ${entity.gUID} on disk")
      case scalarData: ScalarData[_] => {
        log.info(s"PERF Writing scalar $entity of GUID ${entity.gUID} to disk")
        val targetDir = entityPath(entity)
        targetDir.mkdirs
        val oos = new java.io.ObjectOutputStream(serializedScalarFileName(targetDir).create())
        oos.writeObject(scalarData.value)
        oos.close()
        successPath(targetDir).createFromStrings("")
        log.info(s"PERF Written scalar of GUID ${entity.gUID} to disk")
      }
    }
    log.info(s"Entity $entity saved.")
  }

  private val numCoresPerExecutor = scala.util.Properties.envOrElse(
    "NUM_CORES_PER_EXECUTOR", "4").toInt
  def runtimeContext = {
    val numExecutors = (sc.getExecutorStorageStatus.size - 1) max 1
    val totalCores = numExecutors * numCoresPerExecutor
    val cacheMemory = sc.getExecutorMemoryStatus.values.map(_._1).sum
    val conf = sc.getConf
    // Unfortunately the defaults are hard-coded in Spark and not available.
    val cacheFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val shuffleFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val workFraction = 1.0 - cacheFraction - shuffleFraction
    val workMemory = workFraction * cacheMemory / cacheFraction
    RuntimeContext(
      sparkContext = sc,
      broadcastDirectory = repositoryPath / "broadcasts",
      numAvailableCores = totalCores,
      workMemoryPerCore = (workMemory / totalCores).toLong)
  }
}
