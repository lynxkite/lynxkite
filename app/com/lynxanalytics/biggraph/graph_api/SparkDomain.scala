// The SparkDomain can run operations on Spark and give access to their outputs.
//
// It also manages loading the data from disk and saving the data when RDDs are requested.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

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
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.ControlledFutures
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

class SparkDomain(
    val sparkSession: spark.sql.SparkSession,
    val repositoryPath: HadoopFile,
    val ephemeralPath: Option[HadoopFile] = None) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SparkDomain",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_SPARK_PARALLELISM", "5").toInt)
  private val asyncJobs = new ControlledFutures()(executionContext)
  val hadoopCheckExecutionContext =
    ThreadUtil.limitedExecutionContext(
      "SparkDomain(Hadoop)",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_HADOOP_PARALLELISM", "5").toInt)
  private val hadoopAsyncJobs = new ControlledFutures()(hadoopCheckExecutionContext)
  private var executingOperation =
    new ThreadLocal[Option[MetaGraphOperationInstance]] { override def initialValue() = None }
  private val instanceOutputCache = TrieMap[UUID, SafeFuture[Map[UUID, EntityData]]]()
  private val entitiesOnDiskCache = TrieMap[UUID, SafeFuture[Boolean]]()
  private val entityCache = TrieMap[UUID, SafeFuture[EntityData]]()
  private val sparkCachedEntities = mutable.Set[UUID]()
  lazy val masterSQLContext = {
    val sqlContext = sparkSession.sqlContext
    UDF.register(sqlContext.udf)
    sqlContext
  }

  def entityIO(entity: MetaGraphEntity): io.EntityIO = {
    val context = io.IOContext(dataRoot, sparkSession)
    entity match {
      case vs: VertexSet => new io.VertexSetIO(vs, context)
      case eb: EdgeBundle => new io.EdgeBundleIO(eb, context)
      case eb: HybridBundle => new io.HybridBundleIO(eb, context)
      case va: Attribute[_] => new io.AttributeIO(va, context)
      case sc: Scalar[_] => new io.ScalarIO(sc, context)
      case tb: Table => new io.TableIO(tb, context)
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

  private def canLoadEntityFromDisk(entity: MetaGraphEntity): SafeFuture[Boolean] = {
    if (isEntityInProgressOrComputed(entity) || !entityIO(entity).mayHaveExisted) {
      SafeFuture.successful(false)
    } else entitiesOnDiskCache.synchronized {
      entitiesOnDiskCache.getOrElseUpdate(
        entity.gUID,
        hadoopAsyncJobs.register { entityIO(entity).exists })
    }
  }

  private def isEntityComputed(
    entity: MetaGraphEntity): Boolean = {
    entityCache.contains(entity.gUID) &&
      (entityCache(entity.gUID).value match {
        case None => false // in progress
        case Some(Failure(_)) => false
        case Some(Success(_)) => true // computed
      })
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
    val baseOptFuture: Option[SafeFuture[VertexSetData]] =
      eio.correspondingVertexSet.map(vs => getFuture(vs).map(_.asInstanceOf[VertexSetData]))
    val baseFutureOpt: SafeFuture[Option[VertexSetData]] =
      baseOptFuture.map(f => f.map(Some(_))).getOrElse(SafeFuture.successful(None))
    baseFutureOpt.map(bf => eio.read(bf))
  }

  private def set(entity: MetaGraphEntity, data: SafeFuture[EntityData]) = synchronized {
    entityCache(entity.gUID) = data
  }

  def clear() = synchronized {
    instanceOutputCache.clear()
    entityCache.clear()
    sparkCachedEntities.clear()
    entitiesOnDiskCache.clear()
    dataRoot.clear()
  }

  // Runs something on the SparkDomain threadpool.
  // Use this to run Spark operations from HTTP handlers. (SPARK-12964)
  def async[T](fn: => T): concurrent.Future[T] = SafeFuture(fn).future

  // Asserts that this thread is not in the process of executing an operation.
  private def assertNotInOperation(msg: => String): Unit = {
    for (op <- executingOperation.get) {
      throw new AssertionError(s"$op $msg")
    }
  }

  private def sparkOp[I <: InputSignatureProvider, O <: MetaDataSetProvider](
    instance: MetaGraphOperationInstance): SparkOperation[I, O] =
    sparkOp(instance.asInstanceOf[TypedOperationInstance[I, O]])

  private def sparkOp[I <: InputSignatureProvider, O <: MetaDataSetProvider](
    instance: TypedOperationInstance[I, O]): SparkOperation[I, O] = {
    instance.operation.asInstanceOf[SparkOperation[I, O]]
  }

  private def run[I <: InputSignatureProvider, O <: MetaDataSetProvider](
    instance: MetaGraphOperationInstance,
    inputDatas: DataSet): Map[UUID, EntityData] =
    run(instance.asInstanceOf[TypedOperationInstance[I, O]], inputDatas)

  private def run[I <: InputSignatureProvider, O <: MetaDataSetProvider](
    instance: TypedOperationInstance[I, O],
    inputDatas: DataSet): Map[UUID, EntityData] = {
    val outputBuilder = new OutputBuilder(instance)
    sparkOp(instance).execute(inputDatas, instance.result, outputBuilder, runtimeContext)
    outputBuilder.dataMap.toMap
  }

  private def execute(
    instance: MetaGraphOperationInstance,
    logger: OperationLogger): SafeFuture[Map[UUID, EntityData]] = {
    val inputs = instance.inputs.all.map {
      case (name, entity) =>
        name -> entityCache(entity.gUID).get
    }
    SafeFuture {
      if (sparkOp(instance).isHeavy) {
        log.info(s"PERF HEAVY Starting to compute heavy operation instance $instance")
        logger.startTimer()
      }
      val inputDatas = DataSet(inputs)
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computing scalar $scalar")
      }
      val outputDatas = concurrent.blocking {
        executingOperation.set(Some(instance))
        try run(instance, inputDatas)
        finally executingOperation.set(None)
      }
      validateOutput(instance, outputDatas)
      concurrent.blocking {
        if (sparkOp(instance).isHeavy) {
          saveOutputs(instance, outputDatas.values)
        } else if (!sparkOp(instance).neverSerialize) {
          // We still save all scalars even for non-heavy operations,
          // unless they are explicitly say 'never serialize'.
          // This can happen asynchronously though.
          asyncJobs.register {
            saveOutputs(instance, outputDatas.values.collect { case o: ScalarData[_] => o })
          }
        }
      }
      for (scalar <- instance.outputs.scalars.values) {
        log.info(s"PERF Computed scalar $scalar")
      }
      if (sparkOp(instance).isHeavy) {
        log.info(s"PERF HEAVY Finished computing heavy operation instance $instance")
        logger.stopTimer()
      }
      outputDatas
    }
  }

  private def saveOutputs(
    instance: MetaGraphOperationInstance,
    outputs: Iterable[EntityData]): Unit = {
    for (output <- outputs) {
      saveToDisk(output)
    }
    // Mark the operation as complete. Entities may not be loaded from incomplete operations.
    // The reason for this is that an operation may give different results if the number of
    // partitions is different. So for consistency, all outputs must be from the same run.
    (EntityIO.operationPath(dataRoot, instance) / io.Success).forWriting.createFromStrings("")
  }

  private def validateOutput(
    instance: MetaGraphOperationInstance,
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
      assert(
        vsd.rdd.partitioner.get eq entityd.rdd.partitioner.get,
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
        if (sparkOp(instance).isHeavy) {
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

  def getProgress(entity: MetaGraphEntity): Double = synchronized {
    val guid = entity.gUID
    // It would be great if we could be more granular, but for now we just return 0.5 if the
    // computation is running.
    if (entityCache.contains(guid)) {
      entityCache(guid).value match {
        case None => 0.5
        case Some(Failure(_)) => -1.0
        case Some(Success(_)) => 1.0
      }
    } else {
      canLoadEntityFromDisk(entity).value match {
        case Some(util.Success(true)) => 1.0
        case _ => 0.0
      }
    }
  }

  private def loadOrExecuteIfNecessary(entity: MetaGraphEntity): Unit = synchronized {
    if (!isEntityInProgressOrComputed(entity)) {
      if (canLoadEntityFromDisk(entity).awaitResult(Duration.Inf)) {
        // If on disk already, we just load it.
        set(entity, load(entity))
      } else {
        assertNotInOperation(s"has triggered the computation of $entity. #5580")
        // Otherwise we schedule execution of its operation.
        val instance = entity.source

        val logger = new OperationLogger(instance, executionContext)
        val instanceFuture = getInstanceFuture(instance, logger)

        for (output <- instance.outputs.all.values) {
          set(
            output,
            // And the entity will have to wait until its full completion (including saves).
            if (sparkOp(instance).isHeavy && !output.isInstanceOf[Scalar[_]]) {
              val futureEntityData = instanceFuture.flatMap(_ => load(output))
              logger.addOutput(futureEntityData)
              futureEntityData
            } else {
              instanceFuture.map(_(output.gUID))
            })
        }

        logger.logWhenReady(asyncJobs)

      }
    }
  }

  override def canCompute(e: MetaGraphEntity): Boolean = {
    e.source.operation.isInstanceOf[SparkOperation[_, _]]
  }
  override def compute(e: MetaGraphEntity): SafeFuture[Unit] = {
    loadOrExecuteIfNecessary(e)
    getFuture(e).map(_ => ())
  }
  override def has(entity: MetaGraphEntity): Boolean = synchronized {
    isEntityComputed(entity) || canLoadEntityFromDisk(entity).awaitResult(Duration.Inf)
  }

  def getFuture(e: MetaGraphEntity): SafeFuture[EntityData] = {
    entityCache(e.gUID)
  }

  def waitAllFutures(): Unit = {
    SafeFuture.sequence(entityCache.values.toSeq).awaitReady(Duration.Inf)
    asyncJobs.waitAllFutures()
    hadoopAsyncJobs.waitAllFutures()
  }

  // Convenience for awaiting something in this execution context.
  def await[T](f: SafeFuture[T]): T = f.awaitResult(Duration.Inf)

  override def get[T](scalar: Scalar[T]): SafeFuture[T] = {
    implicit val tt = scalar.typeTag
    entityCache(scalar.gUID).map(_.asInstanceOf[ScalarData[_]].runtimeSafeCast[T].value)
  }

  private def coLocatedFuture[T: ClassTag](
    dataFuture: SafeFuture[EntityRDDData[T]],
    idSet: VertexSet): SafeFuture[(UniqueSortedRDD[Long, T], Option[Long])] = {

    dataFuture.zip(getFuture(idSet).as[VertexSetData]).map {
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
    synchronized {
      if (!sparkCachedEntities.contains(entity.gUID)) {
        entityCache(entity.gUID) = entity match {
          case vs: VertexSet => getFuture(vs).as[VertexSetData].map(_.cached)
          case eb: EdgeBundle =>
            coLocatedFuture(getFuture(eb).as[EdgeBundleData], eb.idSet)
              .map { case (rdd, count) => new EdgeBundleData(eb, rdd, count) }
          case heb: HybridBundle => getFuture(heb).as[HybridBundleData].map(_.cached)
          case va: Attribute[_] =>
            def cached[T](va: Attribute[T]) =
              coLocatedFuture(getFuture(va).as[AttributeData[T]], va.vertexSet)(va.classTag)
                .map { case (rdd, count) => new AttributeData[T](va, rdd, count) }
            cached(va)
          case sc: Scalar[_] => getFuture(sc)
          case tb: Table => getFuture(tb)
        }
        sparkCachedEntities.add(entity.gUID)
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
      ioContext = io.IOContext(dataRoot, sparkSession),
      broadcastDirectory = broadcastDirectory,
      sparkDomain = this)
  }

  def newSQLContext(): SQLContext = {
    val sqlContext = masterSQLContext.newSession()
    UDF.register(sqlContext.udf)
    sqlContext
  }

  override def relocate(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = synchronized {
    source match {
      case source: ScalaDomain =>
        import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
        import com.lynxanalytics.biggraph.spark_util.Implicits._
        def parallelize[T: reflect.ClassTag](s: Seq[(ID, T)]): UniqueSortedRDD[ID, T] = {
          val rc = runtimeContext
          rc.sparkContext.parallelize(s).sortUnique(rc.partitionerForNRows(s.size))
        }
        val future = SafeFuture[EntityData] {
          e match {
            case e: VertexSet => new VertexSetData(
              e, parallelize(source.get(e).toSeq.map((_, ()))), count = Some(source.get(e).size))
            case e: EdgeBundle =>
              new EdgeBundleData(
                e, parallelize(source.get(e).toSeq), count = Some(source.get(e).size))
            case e: Attribute[_] => {
              def attr[T: reflect.ClassTag](e: Attribute[T]) = new AttributeData[T](
                e, parallelize(source.get(e).toSeq), count = Some(source.get(e).size))
              attr(e)(e.classTag)
            }
            case e: Scalar[_] => {
              def scalar[T](e: Scalar[T]) = new ScalarData[T](e, source.get(e).get)
              scalar(e)
            }
            case _ => throw new AssertionError(s"Cannot fetch $e from $source")
          }
        }
        set(e, future)
        future.map(_ => ())
    }
  }
}

object SparkDomain {
  // This has to be synchronized; see
  // https://app.asana.com/0/153258440689361/354006072569798
  // In a nutshell: we can now call this function from different threads with
  // the same SQLContext, and name collisions (same name - different DataFrame)
  // are thus now possible.
  def sql(
    ctx: SQLContext,
    query: String,
    dfs: List[(String, spark.sql.DataFrame)]): spark.sql.DataFrame = synchronized {
    for ((name, df) <- dfs) {
      assert(df.sqlContext == ctx, "DataFrame from foreign SQLContext.")
      df.createOrReplaceTempView(s"`$name`")
    }
    log.info(s"Executing query: $query")
    val df = ctx.sql(query)
    for ((name, _) <- dfs) {
      ctx.dropTempTable(name)
    }
    df
  }
  lazy val hiveConfigured = (getClass.getResource("/hive-site.xml") != null)
}

trait SparkOperation[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider] extends TypedMetaGraphOp[IS, OMDS] {
  // An operation is heavy if it is faster to load its results than it is to recalculate them.
  // Heavy operation outputs are written out and loaded back on completion.
  val isHeavy: Boolean = false
  val neverSerialize: Boolean = false
  assert(!isHeavy || !neverSerialize, "$this cannot be heavy and never serialize at the same time")
  // If a heavy operation hasCustomSaving, it can just write out some or all of its outputs
  // instead of putting them in the OutputBuilder in execute().
  val hasCustomSaving: Boolean = false
  assert(!hasCustomSaving || isHeavy, "$this cannot have custom saving if it is not heavy.")
  def execute(
    inputDatas: DataSet,
    outputMeta: OMDS,
    output: OutputBuilder,
    rc: RuntimeContext): Unit
}

sealed trait EntityData {
  val entity: MetaGraphEntity
  def gUID = entity.gUID
}
sealed trait EntityRDDData[T] extends EntityData {
  val rdd: spark.rdd.RDD[(ID, T)]
  val count: Option[Long]
  def cached: EntityRDDData[T]
  rdd.setName("RDD[%d]/%d of %s GUID[%s]".format(rdd.id, rdd.partitions.size, entity, gUID))
}
class VertexSetData(
    val entity: VertexSet,
    val rdd: VertexSetRDD,
    val count: Option[Long] = None) extends EntityRDDData[Unit] {
  val vertexSet = entity
  def cached = new VertexSetData(entity, rdd.copyWithAncestorsCached, count)
}

class EdgeBundleData(
    val entity: EdgeBundle,
    val rdd: EdgeBundleRDD,
    val count: Option[Long] = None) extends EntityRDDData[Edge] {
  val edgeBundle = entity
  def cached = new EdgeBundleData(entity, rdd.copyWithAncestorsCached, count)
}

class HybridBundleData(
    val entity: HybridBundle,
    val rdd: HybridBundleRDD,
    val count: Option[Long] = None) extends EntityRDDData[ID] {
  val hybridBundle = entity
  def cached = new HybridBundleData(entity, rdd.persist(spark.storage.StorageLevel.MEMORY_ONLY), count)
}

class AttributeData[T](
    val entity: Attribute[T],
    val rdd: AttributeRDD[T],
    val count: Option[Long] = None)
  extends EntityRDDData[T] with RuntimeSafeCastable[T, AttributeData] {
  val attribute = entity
  val typeTag = attribute.typeTag
  def cached = new AttributeData[T](entity, rdd.copyWithAncestorsCached, count)
}

class ScalarData[T](
    val entity: Scalar[T],
    val value: T)
  extends EntityData with RuntimeSafeCastable[T, ScalarData] {
  val scalar = entity
  val typeTag = scalar.typeTag
}

class TableData(
    val entity: Table,
    val df: spark.sql.DataFrame)
  extends EntityData {
  val table = entity
}

// A bundle of data types.
case class DataSet(
    vertexSets: Map[Symbol, VertexSetData] = Map(),
    edgeBundles: Map[Symbol, EdgeBundleData] = Map(),
    hybridBundles: Map[Symbol, HybridBundleData] = Map(),
    attributes: Map[Symbol, AttributeData[_]] = Map(),
    scalars: Map[Symbol, ScalarData[_]] = Map(),
    tables: Map[Symbol, TableData] = Map()) {
  def metaDataSet = MetaDataSet(
    vertexSets.mapValues(_.vertexSet),
    edgeBundles.mapValues(_.edgeBundle),
    hybridBundles.mapValues(_.hybridBundle),
    attributes.mapValues(_.attribute),
    scalars.mapValues(_.scalar),
    tables.mapValues(_.table))

  def all: Map[Symbol, EntityData] =
    vertexSets ++ edgeBundles ++ hybridBundles ++ attributes ++ scalars ++ tables
}

object DataSet {
  def apply(all: Map[Symbol, EntityData]): DataSet = {
    DataSet(
      vertexSets = all.collect { case (k, v: VertexSetData) => (k, v) },
      edgeBundles = all.collect { case (k, v: EdgeBundleData) => (k, v) },
      hybridBundles = all.collect { case (k, v: HybridBundleData) => (k, v) },
      attributes = all.collect { case (k, v: AttributeData[_]) => (k, v) }.toMap,
      scalars = all.collect { case (k, v: ScalarData[_]) => (k, v) }.toMap,
      tables = all.collect { case (k, v: TableData) => (k, v) })
  }
}

class OutputBuilder(val instance: MetaGraphOperationInstance) {
  val outputMeta: MetaDataSet = instance.outputs

  def addData(data: EntityData): Unit = {
    val gUID = data.gUID
    val entity = data.entity
    // Check that it's indeed a known output.
    assert(
      outputMeta.all(entity.name).gUID == entity.gUID,
      s"$entity is not an output of $instance")
    internalDataMap(gUID) = data
  }

  def apply(vertexSet: VertexSet, rdd: VertexSetRDD): Unit = {
    addData(new VertexSetData(vertexSet, rdd))
  }

  def apply(edgeBundle: EdgeBundle, rdd: EdgeBundleRDD): Unit = {
    addData(new EdgeBundleData(edgeBundle, rdd))
    if (edgeBundle.autogenerateIdSet) {
      addData(new VertexSetData(edgeBundle.idSet, rdd.mapValues(_ => ())))
    }
  }

  def apply(hybridBundle: HybridBundle, rdd: HybridBundleRDD): Unit = {
    addData(new HybridBundleData(hybridBundle, rdd))
  }

  def apply[T](attribute: Attribute[T], rdd: AttributeRDD[T]): Unit = {
    addData(new AttributeData(attribute, rdd))
  }

  def apply[T](scalar: Scalar[T], value: T): Unit = {
    addData(new ScalarData(scalar, value))
  }

  def apply(table: Table, df: spark.sql.DataFrame): Unit = {
    import com.lynxanalytics.biggraph.spark_util.SQLHelper
    SQLHelper.assertTableHasCorrectSchema(table, df.schema)
    addData(new TableData(table, df))
  }

  def dataMap() = {
    if (!instance.operation.asInstanceOf[SparkOperation[_, _]].hasCustomSaving) {
      val missing = outputMeta.all.values.filter(x => !internalDataMap.contains(x.gUID))
      assert(missing.isEmpty, s"Output data missing for: $missing")
    }
    internalDataMap
  }

  private val internalDataMap = mutable.Map[UUID, EntityData]()
}
