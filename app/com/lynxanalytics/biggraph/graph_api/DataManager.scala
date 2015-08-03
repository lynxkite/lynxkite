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

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.HadoopFile

object DataManager {
  val maxParallelSparkStages =
    scala.util.Properties.envOrElse("KITE_SPARK_PARALLELISM", "5").toInt
}
class DataManager(sc: spark.SparkContext,
                  val repositoryPath: HadoopFile,
                  val ephemeralPath: Option[HadoopFile] = None) {
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

  def entityIO(entity: MetaGraphEntity): io.EntityIO = {
    val param = io.DMParam(dataRoot, sc)
    entity match {
      case vs: VertexSet => new io.VertexIO(vs, param)
      case eb: EdgeBundle => new io.EdgeBundleIO(eb, param)
      case va: Attribute[_] => new io.AttributeIO(va, param)
      case sc: Scalar[_] => new io.ScalarIO(sc, param)
    }
  }

  private val dataRoot = {
    val mainRoot = new io.DataRoot(repositoryPath)
    ephemeralPath.map { ephemeralPath =>
      val ephemeralRoot = new io.DataRoot(ephemeralPath)
      new io.CombinedRoot(ephemeralRoot, mainRoot)
    }.getOrElse(mainRoot)
  }

  private def serializedScalarFileName(basePath: HadoopFile): HadoopFile = basePath / "serialized_data"

  private def hasEntityOnDisk(eio: io.EntityIO): Boolean = {
    (eio.entity.source.operation.isHeavy || eio.entity.isInstanceOf[Scalar[_]]) &&
      // Fast check for directory.
      (dataRoot / io.OperationsDir / eio.entity.source.gUID.toString).fastExists &&
      eio.fastExists &&
      // Slow check for _SUCCESS file.
      (dataRoot / io.OperationsDir / eio.entity.source.gUID.toString / io.Success).exists &&
      eio.exists
  }
  private def hasEntity(entity: MetaGraphEntity): Boolean = entityCache.contains(entity.gUID)

  private def load(entity: io.EntityIO): Future[EntityData] = {
    log.info(s"PERF Found entity $entity on disk")
    val vsOpt: Option[VertexSet] = entity.correspondingVertexSet
    val baseFuture = vsOpt.map(vs => getFuture(vs).map(x => Some(x))).getOrElse(future { None })
    baseFuture.map(bf => entity.read(bf))
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
    (dataRoot / io.OperationsDir / instance.gUID.toString / io.Success).forWriting.createFromStrings("")
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
      val eio = entityIO(entity)
      hasEntityOnDisk(eio)
    }
  }

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = synchronized {
    if (!hasEntity(entity)) {
      val eio = entityIO(entity)
      if (hasEntityOnDisk(eio)) {
        // If on disk already, we just load it.
        set(entity, load(eio))
      } else {
        assert(computationAllowed, "DEMO MODE, you cannot start new computations")
        // Otherwise we schedule execution of its operation.
        val instance = entity.source
        val instanceFuture = getInstanceFuture(instance)
        for (output <- instance.outputs.all.values) {
          val eio2 = entityIO(output)
          set(
            output,
            // And the entity will have to wait until its full completion (including saves).
            if (instance.operation.isHeavy && !output.isInstanceOf[Scalar[_]]) {
              instanceFuture.flatMap(_ => load(eio2))
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
    val eio = entityIO(entity)
    val entityPath = eio.legacyPath.forWriting
    val doesNotExist = !entityPath.exists || entityPath.delete()
    assert(doesNotExist, s"Cannot delete directory $entityPath of entity $entity")
    log.info(s"Saving entity $entity ...")
    eio.write(data)
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
    val broadcastDirectory = ephemeralPath.getOrElse(repositoryPath) / io.BroadcastsDir
    RuntimeContext(
      sparkContext = sc,
      broadcastDirectory = broadcastDirectory,
      numExecutors = numExecutors,
      numAvailableCores = totalCores,
      workMemoryPerCore = (workMemory / totalCores).toLong)
  }
}
