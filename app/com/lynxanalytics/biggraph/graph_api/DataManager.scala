// The DataManager can get the EntityDatas (RDDs/scalar values) for a MetaGraphEntity.
//
// It will either load the data from disk, or run the required operations
// (and then save the data) when RDDs are requested.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import org.apache.spark
import org.apache.spark.sql.SQLContext
import scala.collection.concurrent.TrieMap
import scala.concurrent._

import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.spark_util.Implicits._

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

  private def entityPath(entity: MetaGraphEntity) = {
    if (entity.isInstanceOf[Scalar[_]]) {
      repositoryPath / "scalars" / entity.gUID.toString
    } else {
      repositoryPath / "entities" / entity.gUID.toString
    }
  }

  // Things saved during previous runs.
  val savedInstances: Set[UUID] = {
    val instances = (repositoryPath / "operations" / "*" / "_SUCCESS").list
      .filter(f => !(f.path.toString contains ".deleted"))
    instances.map(_.path.getParent.getName.asUUID).toSet
  }
  val savedEntities: Set[UUID] = {
    val scalars = (repositoryPath / "scalars" / "*" / "_SUCCESS").list
      .filter(f => !(f.path.toString contains ".deleted"))
    val entities = (repositoryPath / "entities" / "*" / "_SUCCESS").list
      .filter(f => !(f.path.toString contains ".deleted"))
    (scalars ++ entities).map(_.path.getParent.getName.asUUID).toSet
  }

  private def successPath(basePath: HadoopFile): HadoopFile = basePath / "_SUCCESS"

  private def serializedScalarFileName(basePath: HadoopFile): HadoopFile = basePath / "serialized_data"

  private def hasEntityOnDisk(entity: MetaGraphEntity): Boolean =
    (entity.source.operation.isHeavy || entity.isInstanceOf[Scalar[_]]) &&
      savedInstances.contains(entity.source.gUID) &&
      savedEntities.contains(entity.gUID)

  private def hasEntity(entity: MetaGraphEntity): Boolean = entityCache.contains(entity.gUID)

  private def load(vertexSet: VertexSet): Future[VertexSetData] = {
    future {
      val fn = entityPath(vertexSet)
      val rdd = fn.loadEntityRDD[Unit](sc)
      new VertexSetData(vertexSet, rdd)
    }
  }

  private def load(edgeBundle: EdgeBundle): Future[EdgeBundleData] = {
    getFuture(edgeBundle.idSet).map { idSet =>
      // We do our best to colocate partitions to corresponding vertex set partitions.
      val idsRDD = idSet.rdd.cache
      val rawRDD = entityPath(edgeBundle).loadEntityRDD[Edge](sc, idsRDD.partitioner)
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
      val rawRDD = entityPath(attribute).loadEntityRDD[T](sc, vsRDD.partitioner)
      new AttributeData[T](
        attribute,
        // This join does nothing except enforcing colocation.
        vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
    }
  }

  private def load[T](scalar: Scalar[T]): Future[ScalarData[T]] = {
    future {
      blocking {
        log.info(s"PERF Loading scalar $scalar from disk")
        val ois = new java.io.ObjectInputStream(serializedScalarFileName(entityPath(scalar)).open())
        val value = try ois.readObject.asInstanceOf[T] finally ois.close()
        log.info(s"PERF Loaded scalar $scalar from disk")
        new ScalarData[T](scalar, value)
      }
    }
  }

  private def load(entity: MetaGraphEntity): Future[EntityData] = {
    log.info(s"PERF Found entity $entity on disk")
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
        for (output <- instance.outputs.all.values) {
          set(
            output,
            // And the entity will have to wait until its full completion (including saves).
            if (instance.operation.isHeavy && !output.isInstanceOf[Scalar[_]]) {
              instanceFuture.flatMap(_ => load(output))
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
        entityPath(entity).saveEntityRDD(rddData.rdd)
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
