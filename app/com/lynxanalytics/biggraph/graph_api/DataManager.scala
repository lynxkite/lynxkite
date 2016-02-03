// The DataManager can get the EntityDatas (RDDs/scalar values) for a MetaGraphEntity.
//
// It will either load the data from disk, or run the required operations
// (and then save the data) when RDDs are requested.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import com.lynxanalytics.biggraph.graph_api.io.{ DataRoot, EntityIO }
import org.apache.spark
import org.apache.spark.sql.SQLContext
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.Duration

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api.{ SafeFuture => Future }

trait EntityProgressManager {
  case class ScalarComputationState[T](
    computeProgress: Double,
    value: Option[T],
    error: Option[Throwable])

  // Returns an indication of whether the entity has already been computed.
  // 0 means it is not computed.
  // 1 means it is computed.
  // Anything in between indicates that the computation is in progress.
  // -1.0 indicates that an error has occured during computation.
  // These constants need to be kept in sync with the ones in:
  // /web/app/script/util.js
  def computeProgress(entity: MetaGraphEntity): Double
  def getComputedScalarValue[T](entity: Scalar[T]): ScalarComputationState[T]
}

class DataManager(sc: spark.SparkContext,
                  val repositoryPath: HadoopFile,
                  val ephemeralPath: Option[HadoopFile] = None) extends EntityProgressManager {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext("DataManager",
      maxParallelism = util.Properties.envOrElse("KITE_SPARK_PARALLELISM", "5").toInt)
  private val instanceOutputCache = TrieMap[UUID, Future[Map[UUID, EntityData]]]()
  private val entityCache = TrieMap[UUID, Future[EntityData]]()
  // If computing an entity has failed, then it is not put into
  // entityCache, but we store the exception in failedEntityCache.
  // An entity should not be present in both of these at the same
  // time.
  private val failedEntityCache = TrieMap[UUID, Throwable]()
  private val sparkCachedEntities = mutable.Set[UUID]()
  val masterSQLContext = new SQLContext(sc)

  // This can be switched to false to enter "demo mode" where no new calculations are allowed.
  var computationAllowed = true

  def entityIO(entity: MetaGraphEntity): io.EntityIO = {
    val context = io.IOContext(dataRoot, sc)
    entity match {
      case vs: VertexSet => new io.VertexSetIO(vs, context)
      case eb: EdgeBundle => new io.EdgeBundleIO(eb, context)
      case va: Attribute[_] => new io.AttributeIO(va, context)
      case sc: Scalar[_] => new io.ScalarIO(sc, context)
    }
  }

  private val dataRoot: DataRoot = {
    val mainRoot = new io.SingleDataRoot(repositoryPath)
    ephemeralPath.map { ephemeralPath =>
      val ephemeralRoot = new io.SingleDataRoot(ephemeralPath)
      new io.CombinedRoot(ephemeralRoot, mainRoot)
    }.getOrElse(mainRoot)
  }

  val writablePath: HadoopFile = {
    ephemeralPath.getOrElse(repositoryPath)
  }

  private def hasEntityOnDisk(entity: MetaGraphEntity): Boolean = {
    val eio = entityIO(entity)
    // eio.mayHaveExisted is only necessary condition of exist on disk if we haven't calculated
    // the entity in this session, so we need this assertion.
    assert(!hasEntity(eio.entity), s"${eio}")
    (entity.source.operation.isHeavy || entity.isInstanceOf[Scalar[_]]) &&
      // Fast check for directory.
      eio.mayHaveExisted &&
      // Slow check for _SUCCESS file.
      eio.exists
  }
  private def hasEntity(entity: MetaGraphEntity): Boolean = entityCache.contains(entity.gUID)

  // For edge bundles and attributes we need to load the base vertex set first
  private def load(entity: MetaGraphEntity): Future[EntityData] = {
    val eio = entityIO(entity)
    log.info(s"PERF Found entity $entity on disk")
    val vsOpt: Option[VertexSet] = eio.correspondingVertexSet
    val baseFuture = vsOpt.map(vs => getFuture(vs).map(x => Some(x))).getOrElse(Future.successful(None))
    baseFuture.map(bf => eio.read(bf))
  }

  private def set(entity: MetaGraphEntity, data: Future[EntityData]) = synchronized {
    failedEntityCache.remove(entity.gUID)
    entityCache(entity.gUID) = data
    data.onFailure {
      case t: Throwable => synchronized {
        failedEntityCache(entity.gUID) = t
        entityCache.remove(entity.gUID)
      }
    }
  }

  // This is for asynchronous tasks. We store them in a WeakHashMap so that waitAllFutures can wait
  // for them, but the data structure does not grow indefinitely.
  private val loggedFutures = new java.util.WeakHashMap[Future[Unit], Unit]
  private def loggedFuture(func: => Unit): Unit = {
    val f = Future {
      try {
        func
      } catch {
        case t: Throwable => log.error("future failed:", t)
      }
    }
    loggedFutures.put(f, ())
  }

  private def execute(instance: MetaGraphOperationInstance): Future[Map[UUID, EntityData]] = {
    val inputs = instance.inputs
    val futureInputs = Future.sequence(
      inputs.all.toSeq.map {
        case (name, entity) =>
          getFuture(entity).map(data => (name, data))
      })
    futureInputs.map { inputs =>
      if (instance.operation.isHeavy) {
        log.info(s"PERF HEAVY Starting to compute heavy operation instance $instance")
      }
      val inputDatas = DataSet(inputs.toMap)
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computing scalar $scalar")
      }
      val outputDatas = concurrent.blocking {
        instance.run(inputDatas, runtimeContext)
      }
      validateOutput(instance, outputDatas)
      concurrent.blocking {
        if (instance.operation.isHeavy) {
          saveOutputs(instance, outputDatas.values)
        } else {
          // We still save all scalars even for non-heavy operations.
          // This can happen asynchronously though.
          loggedFuture {
            saveOutputs(instance, outputDatas.values.collect { case o: ScalarData[_] => o })
          }
        }
      }
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computed scalar $scalar")
      }
      if (instance.operation.isHeavy) {
        log.info(s"PERF HEAVY Finished computing heavy operation instance $instance")
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
    (EntityIO.operationPath(dataRoot, instance) / io.Success).forWriting.createFromStrings("")
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
      instanceOutputCache(gUID) = {
        val output = execute(instance)
        if (instance.operation.isHeavy) {
          // For heavy operations we want to avoid caching the RDDs. The RDD data will be reloaded
          // from disk anyway to break the lineage. These RDDs need to be GC'd to clean up the
          // shuffle files on the executors. See #2098.
          val nonRDD = output.map {
            _.filterNot { case (guid, entityData) => entityData.isInstanceOf[EntityRDDData[_]] }
          }
          // A GC is helpful here to avoid filling up the disk on the executors. See comment above.
          nonRDD.foreach { _ => System.gc() }
          nonRDD
        } else output
      }
      instanceOutputCache(gUID).onFailure {
        case _ => synchronized { instanceOutputCache.remove(gUID) }
      }
    }
    instanceOutputCache(gUID)
  }

  override def computeProgress(entity: MetaGraphEntity): Double = synchronized {
    val guid = entity.gUID
    // It would be great if we could be more granular, but for now we just return 0.5 if the
    // computation is running.
    if (entityCache.contains(guid) && !entityCache(guid).isCompleted) 0.5
    else if (hasEntity(entity) || hasEntityOnDisk(entity)) 1.0
    else if (failedEntityCache.contains(guid)) -1.0
    else 0.0
  }

  override def getComputedScalarValue[T](entity: Scalar[T]): ScalarComputationState[T] = synchronized {
    val progress = computeProgress(entity)
    ScalarComputationState(
      progress,
      if (progress == 1.0) Some(get(entity).value) else None,
      failedEntityCache.get(entity.gUID))
  }

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

  def waitAllFutures(): Unit = {
    Future.sequence(entityCache.values.toSeq).awaitReady(Duration.Inf)
    import collection.JavaConversions.mapAsScalaMap
    Future.sequence(loggedFutures.keys.toSeq).awaitReady(Duration.Inf)
  }

  def get(vertexSet: VertexSet): VertexSetData = {
    getFuture(vertexSet).awaitResult(Duration.Inf)
  }
  def get(edgeBundle: EdgeBundle): EdgeBundleData = {
    getFuture(edgeBundle).awaitResult(Duration.Inf)
  }
  def get[T](attribute: Attribute[T]): AttributeData[T] = {
    getFuture(attribute).awaitResult(Duration.Inf)
  }
  def get[T](scalar: Scalar[T]): ScalarData[T] = {
    getFuture(scalar).awaitResult(Duration.Inf)
  }
  def get(entity: MetaGraphEntity): EntityData = {
    getFuture(entity).awaitResult(Duration.Inf)
  }

  def cache(entity: MetaGraphEntity): Unit = {
    // We do not cache anything in demo mode.
    if (computationAllowed) {
      synchronized {
        if (!sparkCachedEntities.contains(entity.gUID)) {
          val dataFuture = getFuture(entity)
          entityCache(entity.gUID) = dataFuture.map {
            case rddData: EntityRDDData[_] => rddData.cached
            case data: EntityData => data
          }
          sparkCachedEntities.add(entity.gUID)
        }
      }
    }
  }

  private def saveToDisk(data: EntityData): Unit = {
    val entity = data.entity
    val eio = entityIO(entity)
    val doesNotExist = eio.delete()
    assert(doesNotExist, s"Cannot delete directory of entity $entity")
    log.info(s"Saving entity $entity ...")
    eio.write(data)
    log.info(s"Entity $entity saved.")
  }

  def runtimeContext = {
    val broadcastDirectory = ephemeralPath.getOrElse(repositoryPath) / io.BroadcastsDir
    RuntimeContext(
      sparkContext = sc,
      ioContext = io.IOContext(dataRoot, sc),
      broadcastDirectory = broadcastDirectory)
  }

  def newSQLContext(): SQLContext = {
    masterSQLContext.newSession()
  }
}
