// The DataManager can get the EntityDatas (RDDs/scalar values) for a MetaGraphEntity.
//
// It will either load the data from disk, or run the required operations
// (and then save the data) when RDDs are requested.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import com.google.common.collect.MapMaker
import org.apache.spark
import org.apache.spark.sql.SQLContext
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api.io.DataRoot
import com.lynxanalytics.biggraph.graph_api.io.EntityIO
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

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

class DataManager(val sparkSession: spark.sql.SparkSession,
                  val repositoryPath: HadoopFile,
                  val ephemeralPath: Option[HadoopFile] = None) extends EntityProgressManager {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext("DataManager",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_SPARK_PARALLELISM", "5").toInt)
  private var executingOperation =
    new ThreadLocal[Option[MetaGraphOperationInstance]] { override def initialValue() = None }
  private val instanceOutputCache = TrieMap[UUID, SafeFuture[Map[UUID, EntityData]]]()
  private val entityCache = TrieMap[UUID, SafeFuture[EntityData]]()
  private val sparkCachedEntities = mutable.Set[UUID]()
  lazy val masterSQLContext = sparkSession.sqlContext
  lazy val hiveConfigured = (getClass.getResource("/hive-site.xml") != null)

  // This can be switched to false to enter "demo mode" where no new calculations are allowed.
  var computationAllowed = true

  def entityIO(entity: MetaGraphEntity): io.EntityIO = {
    val context = io.IOContext(dataRoot, sparkSession.sparkContext)
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
    assert(!isEntityInProgressOrComputed(eio.entity), s"${eio} is new")
    (entity.source.operation.isHeavy || entity.isInstanceOf[Scalar[_]]) &&
      // Fast check for directory.
      eio.mayHaveExisted &&
      // Slow check for _SUCCESS file.
      eio.exists
  }
  private def isEntityInProgressOrComputed(
    entity: MetaGraphEntity): Boolean = {

    entityCache.contains(entity.gUID) &&
      (entityCache(entity.gUID).value match {
        case None => true // in progress
        case Some(Failure(_)) => false
        case Some(Success(_)) => true // computed
      })
  }

  // For edge bundles and attributes we need to load the base vertex set first
  private def load(entity: MetaGraphEntity): SafeFuture[EntityData] = {
    val eio = entityIO(entity)
    log.info(s"PERF Found entity $entity on disk")
    val vsOpt: Option[VertexSet] = eio.correspondingVertexSet
    val baseFuture = vsOpt.map(vs => getFuture(vs).map(x => Some(x))).getOrElse(SafeFuture.successful(None))
    baseFuture.map(bf => eio.read(bf))
  }

  private def set(entity: MetaGraphEntity, data: SafeFuture[EntityData]) = synchronized {
    entityCache(entity.gUID) = data
  }

  // This is for asynchronous tasks. We store them so that waitAllFutures can wait
  // for them, but remove them on completion so that the data structure does not grow indefinitely.
  private val loggedFutures = collection.mutable.Map[Object, SafeFuture[Unit]]()

  def loggedFuture(func: => Unit): Unit = loggedFutures.synchronized {
    val key = new Object
    val future = SafeFuture {
      try {
        func
      } catch {
        case t: Throwable => log.error("future failed:", t)
      } finally loggedFutures.synchronized {
        loggedFutures.remove(key)
      }
    }
    loggedFutures.put(key, future)
  }

  // Runs something on the DataManager threadpool.
  // Use this to run Spark operations from HTTP handlers. (SPARK-12964)
  def async[T](fn: => T): concurrent.Future[T] = {
    SafeFuture {
      fn
    }.future
  }

  // Asserts that this thread is not in the process of executing an operation.
  private def assertNotInOperation(msg: String): Unit = {
    for (op <- executingOperation.get) {
      throw new AssertionError(s"$op $msg")
    }
  }

  private def execute(instance: MetaGraphOperationInstance,
                      logger: OperationLogger): SafeFuture[Map[UUID, EntityData]] = {
    val inputs = instance.inputs
    val futureInputs = SafeFuture.sequence(
      inputs.all.toSeq.map {
        case (name, entity) =>
          getFuture(entity).map {
            data =>
              logger.addInput(name.toString, data)
              (name, data)
          }
      })
    futureInputs.map { inputs =>
      if (instance.operation.isHeavy) {
        log.info(s"PERF HEAVY Starting to compute heavy operation instance $instance")
        logger.startTimer()
      }
      val inputDatas = DataSet(inputs.toMap)
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computing scalar $scalar")
      }
      val outputDatas = concurrent.blocking {
        executingOperation.set(Some(instance))
        try instance.run(inputDatas, runtimeContext)
        finally executingOperation.set(None)
      }
      validateOutput(instance, outputDatas)
      concurrent.blocking {
        if (instance.operation.isHeavy) {
          saveOutputs(instance, outputDatas.values)
        } else if (!instance.operation.neverSerialize) {
          // We still save all scalars even for non-heavy operations,
          // unless they are explicitly say 'never serialize'.
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
        logger.stopTimer()
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
    instance: MetaGraphOperationInstance,
    logger: OperationLogger): SafeFuture[Map[UUID, EntityData]] = synchronized {

    val gUID = instance.gUID
    if (!instanceOutputCache.contains(gUID)) {
      instanceOutputCache(gUID) = {
        val output = execute(instance, logger)
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
    if (entityCache.contains(guid)) {
      entityCache(guid).value match {
        case None => 0.5
        case Some(Failure(_)) => -1.0
        case Some(Success(_)) => 1.0
      }
    } else if (hasEntityOnDisk(entity)) 1.0
    else 0.0
  }

  override def getComputedScalarValue[T](entity: Scalar[T]): ScalarComputationState[T] = synchronized {
    val progress = computeProgress(entity)
    ScalarComputationState(
      progress,
      if (progress == 1.0) Some(get(entity).value) else None,
      if (progress == -1.0) {
        entityCache(entity.gUID).value match {
          case Some(Failure(throwable)) => Some(throwable)
          case _ => None
        }
      } else None)
  }

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = synchronized {
    if (!isEntityInProgressOrComputed(entity)) {
      if (hasEntityOnDisk(entity)) {
        // If on disk already, we just load it.
        set(entity, load(entity))
      } else {
        assert(computationAllowed, "DEMO MODE, you cannot start new computations")
        assertNotInOperation(s"has triggered the computation of $entity. #5580")
        // Otherwise we schedule execution of its operation.
        val instance = entity.source

        val logger = new OperationLogger(instance, executionContext)
        val instanceFuture = getInstanceFuture(instance, logger)

        for (output <- instance.outputs.all.values) {
          set(
            output,
            // And the entity will have to wait until its full completion (including saves).
            if (instance.operation.isHeavy && !output.isInstanceOf[Scalar[_]]) {
              val futureEntityData = instanceFuture.flatMap(_ => load(output))
              logger.addOutput(futureEntityData)
              futureEntityData
            } else {
              instanceFuture.map(_(output.gUID))
            })
        }
        logger.logWhenReady()
      }
    }
  }

  def getFuture(vertexSet: VertexSet): SafeFuture[VertexSetData] = {
    loadOrExecuteIfNecessary(vertexSet)
    entityCache(vertexSet.gUID).map(_.asInstanceOf[VertexSetData])
  }

  def getFuture(edgeBundle: EdgeBundle): SafeFuture[EdgeBundleData] = {
    loadOrExecuteIfNecessary(edgeBundle)
    entityCache(edgeBundle.gUID).map(_.asInstanceOf[EdgeBundleData])
  }

  def getFuture[T](attribute: Attribute[T]): SafeFuture[AttributeData[T]] = {
    loadOrExecuteIfNecessary(attribute)
    implicit val tagForT = attribute.typeTag
    entityCache(attribute.gUID).map(_.asInstanceOf[AttributeData[_]].runtimeSafeCast[T])
  }

  def getFuture[T](scalar: Scalar[T]): SafeFuture[ScalarData[T]] = {
    loadOrExecuteIfNecessary(scalar)
    implicit val tagForT = scalar.typeTag
    entityCache(scalar.gUID).map(_.asInstanceOf[ScalarData[_]].runtimeSafeCast[T])
  }

  def getFuture(entity: MetaGraphEntity): SafeFuture[EntityData] = {
    entity match {
      case vs: VertexSet => getFuture(vs)
      case eb: EdgeBundle => getFuture(eb)
      case va: Attribute[_] => getFuture(va)
      case sc: Scalar[_] => getFuture(sc)
    }
  }

  def waitAllFutures(): Unit = {
    SafeFuture.sequence(entityCache.values.toSeq).awaitReady(Duration.Inf)
    val futures = loggedFutures.synchronized { loggedFutures.values.toList }
    SafeFuture.sequence(futures).awaitReady(Duration.Inf)
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

  private def coLocatedFuture[T: ClassTag](
    dataFuture: SafeFuture[EntityRDDData[T]],
    idSet: VertexSet): SafeFuture[(UniqueSortedRDD[Long, T], Option[Long])] = {

    dataFuture.zip(getFuture(idSet)).map {
      case (data, idSetData) =>
        (enforceCoLocationWithIdSet(data, idSetData), data.count)
    }
  }
  private def enforceCoLocationWithIdSet[T: ClassTag](
    rawEntityData: EntityRDDData[T],
    parent: VertexSetData): UniqueSortedRDD[Long, T] = {

    val vsRDD = parent.rdd.copyWithAncestorsCached
    // Enforcing colocation:
    val rawRDD = rawEntityData.rdd
    assert(
      vsRDD.partitions.size == rawRDD.partitions.size,
      s"$vsRDD and $rawRDD should have the same number of partitions, " +
        s"but ${vsRDD.partitions.size} != ${rawRDD.partitions.size}\n" +
        s"${vsRDD.toDebugString}\n${rawRDD.toDebugString}")
    import com.lynxanalytics.biggraph.spark_util.Implicits.PairRDDUtils
    vsRDD.zipPartitions(rawRDD, preservesPartitioning = true) {
      (it1, it2) => it2
    }.asUniqueSortedRDD
  }
  def cache(entity: MetaGraphEntity): Unit = {
    // We do not cache anything in demo mode.
    if (computationAllowed) {
      synchronized {
        if (!sparkCachedEntities.contains(entity.gUID)) {
          entityCache(entity.gUID) = entity match {
            case vs: VertexSet => getFuture(vs).map(_.cached)
            case eb: EdgeBundle =>
              coLocatedFuture(getFuture(eb), eb.idSet)
                .map { case (rdd, count) => new EdgeBundleData(eb, rdd, count) }
            case va: Attribute[_] =>
              coLocatedFuture(getFuture(va), va.vertexSet)(va.classTag)
                .map { case (rdd, count) => new AttributeData(va, rdd, count) }
            case sc: Scalar[_] => getFuture(sc)
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
      sparkContext = sparkSession.sparkContext,
      sqlContext = masterSQLContext,
      ioContext = io.IOContext(dataRoot, sparkSession.sparkContext),
      broadcastDirectory = broadcastDirectory,
      dataManager = this)
  }

  def newSQLContext(): SQLContext = {
    val sqlContext = masterSQLContext.newSession()
    registerUDFs(sqlContext)
    sqlContext
  }

  private def registerUDFs(sqlContext: SQLContext) = {
    sqlContext.udf.register(
      "hash",
      (string: String, salt: String) => graph_operations.HashVertexAttribute.hash(string, salt)
    )
  }
}

object DataManager {
  def sql(
    ctx: SQLContext,
    query: String,
    dfs: List[(String, spark.sql.DataFrame)]): spark.sql.DataFrame = {
    for ((name, df) <- dfs) {
      assert(df.sqlContext == ctx, "DataFrame from foreign SQLContext.")
      df.createOrReplaceTempView(s"`$name`")
    }
    log.info(s"Executing query: $query")
    ctx.sql(query)
  }
}
