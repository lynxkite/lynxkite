// The DataManager can get the EntityDatas (RDDs/scalar values) for a MetaGraphEntity.
//
// It will either load the data from disk, or run the required operations
// (and then save the data) when RDDs are requested.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import org.apache.spark
import org.apache.spark.sql.SQLContext
import scala.collection.concurrent.TrieMap
import scala.concurrent._

import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.HadoopFile

case class DMParam(repoPath: HadoopFile, sparkContext: spark.SparkContext, executionContext: ExecutionContextExecutorService) {
}

abstract class EntityIOWrapper(val entity: MetaGraphEntity, dmParam: DMParam) {
  implicit val executionContext: ExecutionContextExecutorService = dmParam.executionContext
  val repositoryPath = dmParam.repoPath
  val sc = dmParam.sparkContext
  protected def legacyPath: HadoopFile
  protected def existsAtLegacy = (legacyPath / "_SUCCESS").exists
  def exists: Boolean

  def read(parent: Option[EntityData] = None): EntityData

  val possiblySavedEntities: Set[UUID] = {
    val scalars = (repositoryPath / "scalars" / "*").list
    val entities = (repositoryPath / "entities" / "*").list
    val newEntities = (repositoryPath / "new_entities" / "*").list
    (scalars ++ entities ++ newEntities).map(_.path.getName.asUUID).toSet
  }
}

class ScalarIOWrapper[T](entity: Scalar[T], dMParam: DMParam)
    extends EntityIOWrapper(entity, dMParam) {

  protected def legacyPath = repositoryPath / "scalars" / entity.gUID.toString
  def exists = existsAtLegacy
  private def serializedScalarFileName: HadoopFile = legacyPath / "serialized_data"
  override def read(parent: Option[EntityData] = None): ScalarData[T] = {
    val scalar = entity
    log.info(s"PERF Loading scalar $scalar from disk")
    val ois = new java.io.ObjectInputStream(serializedScalarFileName.open())
    val value = try ois.readObject.asInstanceOf[T] finally ois.close()
    log.info(s"PERF Loaded scalar $scalar from disk")
    new ScalarData[T](scalar, value)
  }

}

abstract class PartitionableDataIOWrapper[DT <: EntityRDDData](entity: MetaGraphEntity, dMParam: DMParam)
    extends EntityIOWrapper(entity, dMParam) {

  protected lazy val availablePartitions = collectAvailablePartitions

  private def collectAvailablePartitions = {
    val availablePartitions = scala.collection.mutable.Map[Int, HadoopFile]()

    if (existsAtLegacy) {
      availablePartitions(-1) = legacyPath
    }

    val subDirs = (newPath / ".*").list
    val number = "[123456789][0-9]*".r
    val subdirCandidates = subDirs.filter(x => number.pattern.matcher(x.path.getName).matches)

    for (v <- subdirCandidates) {
      val successFile = v / "_SUCCESS"
      if (successFile.exists) {
        val numParts = v.path.getName.toInt
        availablePartitions(numParts) = v
      }
    }
    availablePartitions
  }

  def selectPartitionNumber: Int

  def finalRead(path: HadoopFile, parent: Option[EntityData] = None): DT

  def repartitionTo(pn: Int): Unit = ???

  override def read(parent: Option[EntityData] = None): DT = {
    val pn = selectPartitionNumber
    if (!availablePartitions.contains(pn)) {
      repartitionTo(pn)
    }
    finalRead(availablePartitions(pn), parent)
  }

  def legacyPath = repositoryPath / "entities" / entity.gUID.toString
  def newPath = repositoryPath / "new_entities" / entity.gUID.toString
  def exists: Boolean = availablePartitions.nonEmpty

}

class VertexIOWrapper(entity: VertexSet, dMParam: DMParam)
    extends PartitionableDataIOWrapper[VertexSetData](entity, dMParam) {
  def selectPartitionNumber: Int = 8
  def finalRead(path: HadoopFile, parent: Option[EntityData]): VertexSetData = {
    val fn = availablePartitions(-1)
    val rdd = fn.loadEntityRDD[Unit](sc)
    new VertexSetData(entity, rdd)
  }
}

class EdgeBundleIOWrapper(entity: EdgeBundle, dMParam: DMParam)
    extends PartitionableDataIOWrapper[EdgeBundleData](entity, dMParam) {
  def selectPartitionNumber: Int = 8
  def edgeBundle = entity
  def finalRead(path: HadoopFile, parent: Option[EntityData]): EdgeBundleData = {
    // We do our best to colocate partitions to corresponding vertex set partitions.
    val p = parent.get
    val idsRDD = p.asInstanceOf[VertexSetData].rdd
    idsRDD.cacheBackingArray()
    val rawRDD = availablePartitions(-1).loadEntityRDD[Edge](sc, idsRDD.partitioner)
    new EdgeBundleData(
      edgeBundle,
      idsRDD.sortedJoin(rawRDD).mapValues { case (_, edge) => edge })
  }
}

class AttributeIOWrapper[T](entity: Attribute[T], dMParam: DMParam)
    extends PartitionableDataIOWrapper[AttributeData[T]](entity, dMParam) {
  def selectPartitionNumber: Int = 8
  def vertexSet = entity
  def finalRead(path: HadoopFile, parent: Option[EntityData]): AttributeData[T] = {
    // We do our best to colocate partitions to corresponding vertex set partitions.
    val p = parent.get
    val vsRDD = p.asInstanceOf[VertexSetData].rdd
    vsRDD.cacheBackingArray()
    implicit val ct = entity.classTag
    val rawRDD = availablePartitions(-1).loadEntityRDD[T](sc, vsRDD.partitioner)
    new AttributeData[T](
      vertexSet,
      // This join does nothing except enforcing colocation.
      vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
  }
}

object DataManager {
  val maxParallelSparkStages =
    scala.util.Properties.envOrElse("KITE_SPARK_PARALLELISM", "5").toInt
}
class DataManager(sc: spark.SparkContext,
                  val repositoryPath: HadoopFile) {
  // Limit parallelism to maxParallelSparkStages.
  implicit val executionContext =
    ExecutionContext.fromExecutorService(
      java.util.concurrent.Executors.newFixedThreadPool(
        DataManager.maxParallelSparkStages,
        new java.util.concurrent.ThreadFactory() {
          private var nextIndex = 1
          def newThread(r: Runnable) = synchronized {
            val t = new Thread(r)
            t.setDaemon(true)
            t.setName(s"DataManager-$nextIndex")
            nextIndex += 1
            t
          }
        }
      ))
  private val instanceOutputCache = TrieMap[UUID, Future[Map[UUID, EntityData]]]()
  private val entityCache = TrieMap[UUID, Future[EntityData]]()
  val sqlContext = new SQLContext(sc)

  // This can be switched to false to enter "demo mode" where no new calculations are allowed.
  var computationAllowed = true

  private def instancePath(instance: MetaGraphOperationInstance) =
    repositoryPath / "operations" / instance.gUID.toString

  def entityWrapperFactory(entity: MetaGraphEntity): EntityIOWrapper = {
    val param = DMParam(repositoryPath, sc, executionContext)
    entity match {
      case vs: VertexSet => new VertexIOWrapper(vs, param)
      case eb: EdgeBundle => new EdgeBundleIOWrapper(eb, param)
      case va: Attribute[_] => new AttributeIOWrapper(va, param)
      case sc: Scalar[_] => new ScalarIOWrapper(sc, param)
    }
  }

  // Things saved during previous runs. Checking for the _SUCCESS files is slow so we use the
  // list of directories instead. The results are thus somewhat optimistic.
  val possiblySavedInstances: Set[UUID] = {
    val instances = (repositoryPath / "operations" / "*").list
    instances.map(_.path.getName.asUUID).toSet
  }

  private def successPath(basePath: HadoopFile): HadoopFile = basePath / "_SUCCESS"

  private def serializedScalarFileName(basePath: HadoopFile): HadoopFile = basePath / "serialized_data"

  private def hasEntityOnDisk(wrapper: EntityIOWrapper): Boolean =
    (wrapper.entity.source.operation.isHeavy || wrapper.entity.isInstanceOf[Scalar[_]]) &&
      // Fast check for directory.
      possiblySavedInstances.contains(wrapper.entity.source.gUID) &&
      wrapper.possiblySavedEntities.contains(wrapper.entity.gUID) &&
      // Slow check for _SUCCESS file.
      successPath(instancePath(wrapper.entity.source)).exists &&
      wrapper.exists

  private def hasEntity(entity: MetaGraphEntity): Boolean = entityCache.contains(entity.gUID)

  private def load(wrapper: VertexIOWrapper): Future[VertexSetData] = {
    future {
      wrapper.read()
    }
  }

  private def load(wrapper: EdgeBundleIOWrapper): Future[EdgeBundleData] = {
    getFuture(wrapper.edgeBundle.idSet).map { idSet => wrapper.read(Option[EntityData](idSet))
      //      // We do our best to colocate partitions to corresponding vertex set partitions.
      //      val idsRDD = idSet.rdd
      //      idsRDD.cacheBackingArray()
      //      val rawRDD = entityPath(edgeBundle).loadEntityRDD[Edge](sc, idsRDD.partitioner)
      //      new EdgeBundleData(
      //        edgeBundle,
      //        idsRDD.sortedJoin(rawRDD).mapValues { case (_, edge) => edge })
    }
  }

  private def load[T](wrapper: AttributeIOWrapper[T]): Future[AttributeData[T]] = {
    implicit val ct = wrapper.vertexSet.classTag
    getFuture(wrapper.vertexSet).map { vs => wrapper.read(Option[EntityData](vs))
      //      // We do our best to colocate partitions to corresponding vertex set partitions.
      //      val vsRDD = vs.rdd
      //      vsRDD.cacheBackingArray()
      //      val rawRDD = entityPath(wrapper).loadEntityRDD[T](sc, vsRDD.partitioner)
      //      new AttributeData[T](
      //        wrapper,
      //        // This join does nothing except enforcing colocation.
      //        vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
    }
  }

  private def load[T](wrapper: ScalarIOWrapper[T]): Future[ScalarData[T]] = {
    future {
      blocking {
        wrapper.read()
      }
    }
  }

  private def load(entity: EntityIOWrapper): Future[EntityData] = {
    log.info(s"PERF Found entity $entity on disk")
    entity match {
      case vs: VertexIOWrapper => load(vs)
      case eb: EdgeBundleIOWrapper => load(eb)
      case va: AttributeIOWrapper[_] => load(va)
      case sc: ScalarIOWrapper[_] => load(sc)
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
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computing scalar $scalar")
      }
      val outputDatas = blocking {
        instance.run(inputDatas, runtimeContext)
      }
      validateOutput(instance, outputDatas)
      blocking {
        if (instance.operation.isHeavy) {
          saveOutputs(instance, outputDatas.values)
        } else {
          // We still save all scalars even for non-heavy operations.
          // This can happen asynchronously though.
          future {
            saveOutputs(instance, outputDatas.values.collect { case o: ScalarData[_] => o })
          }
        }
      }
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computed scalar $scalar")
      }
      outputDatas
    }
  }

  private def saveOutputs(instance: MetaGraphOperationInstance,
                          outputs: Iterable[EntityData]): Unit = {
    for (output <- outputs) {
      saveToDisk(output)
    }
    // Mark the operation as complete. Entities may not be loaded from incomplete operations.
    // The reason for this is that an operation may give different results if the number of
    // partitions is different. So for consistency, all outputs must be from the same run.
    successPath(instancePath(instance)).createFromStrings("")
  }

  private def validateOutput(instance: MetaGraphOperationInstance,
                             output: Map[UUID, EntityData]): Unit = {
    // Make sure attributes re-use the partitioners from their vertex sets.
    // An identity check is used to catch the case where the same number of partitions is used
    // accidentally (as is often the case in tests), but the code does not guarantee this.
    val attributes = output.values.collect { case x: AttributeData[_] => x }
    val edgeBundles = output.values.collect { case x: EdgeBundleData => x }
    val dataAndVs =
      attributes.map(x => x -> x.entity.vertexSet) ++
        edgeBundles.map(x => x -> x.entity.idSet)
    for ((entityd, vs) <- dataAndVs) {
      val entity = entityd.entity
      // The vertex set must either be loaded, or in the output.
      val vsd = output.get(vs.gUID) match {
        case Some(vsd) => vsd.asInstanceOf[VertexSetData]
        case None =>
          assert(entityCache.contains(vs.gUID), s"$vs, vertex set of $entity, not known")
          assert(entityCache(vs.gUID).value.nonEmpty, s"$vs, vertex set of $entity, not loaded")
          assert(entityCache(vs.gUID).value.get.isSuccess, s"$vs, vertex set of $entity, failed")
          entityCache(vs.gUID).value.get.get.asInstanceOf[VertexSetData]
      }
      assert(vsd.rdd.partitioner.get eq entityd.rdd.partitioner.get,
        s"The partitioner of $entity does not match the partitioner of $vs.")
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

  def isCalculated(entity: MetaGraphEntity): Boolean = {
    if (hasEntity(entity)) true
    else {
      val wrapper = entityWrapperFactory(entity)
      hasEntityOnDisk(wrapper)
    }
  }

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = synchronized {
    if (!hasEntity(entity)) {
      val wrapper = entityWrapperFactory(entity)
      if (hasEntityOnDisk(wrapper)) {
        // If on disk already, we just load it.
        set(entity, load(wrapper))
      } else {
        assert(computationAllowed, "DEMO MODE, you cannot start new computations")
        // Otherwise we schedule execution of its operation.
        val instance = entity.source
        val instanceFuture = getInstanceFuture(instance)
        for (output <- instance.outputs.all.values) {
          set(
            output,
            // And the entity will have to wait until its full completion (including saves).
            if (instance.operation.isHeavy && !output.isInstanceOf[Scalar[_]]) {
              instanceFuture.flatMap(_ => load(wrapper))
            } else {
              instanceFuture.map(_(output.gUID))
            })
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
        log.info(s"PERF Instantiating entity $entity on disk")
        val rdd = rddData.rdd
        entityPath(entity).saveEntityRDD(rdd)
        log.info(s"PERF Instantiated entity $entity on disk")
      case scalarData: ScalarData[_] => {
        log.info(s"PERF Writing scalar $entity to disk")
        val targetDir = entityPath(entity)
        targetDir.mkdirs
        val oos = new java.io.ObjectOutputStream(serializedScalarFileName(targetDir).create())
        oos.writeObject(scalarData.value)
        oos.close()
        successPath(targetDir).createFromStrings("")
        log.info(s"PERF Written scalar $entity to disk")
      }
    }
    log.info(s"Entity $entity saved.")
  }

  // No way to find cores per executor programmatically. SPARK-2095
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
    log.info("Creating runtime context")
    log.info("Work memory: " + workMemory)
    log.info("Total cores: " + totalCores)
    log.info("Cache memory: " + cacheMemory)
    log.info("Work fraction: " + workFraction)
    log.info("Cache fraction: " + cacheFraction)
    log.info("WM per core: " + (workMemory / totalCores).toLong)
    RuntimeContext(
      sparkContext = sc,
      broadcastDirectory = repositoryPath / "broadcasts",
      numExecutors = numExecutors,
      numAvailableCores = totalCores,
      workMemoryPerCore = (workMemory / totalCores).toLong)
  }
}
