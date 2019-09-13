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
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

class SparkDomain(
    val sparkSession: spark.sql.SparkSession,
    val repositoryPath: HadoopFile,
    val ephemeralPath: Option[HadoopFile] = None) extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SparkDomain",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)
  private val entitiesOnDiskCache = TrieMap[UUID, Boolean]()
  private val entityCache = TrieMap[UUID, EntityData]()
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

  private def canLoadEntityFromDisk(entity: MetaGraphEntity): Boolean = {
    assert(!entityCache.contains(entity.gUID), s"We already have $entity.")
    entityIO(entity).mayHaveExisted &&
      entitiesOnDiskCache.getOrElseUpdate(entity.gUID, entityIO(entity).exists)
  }

  private def load(entity: MetaGraphEntity): EntityData = {
    val eio = entityIO(entity)
    log.info(s"PERF Found entity $entity on disk")
    // For edge bundles and attributes we need to load the base vertex set first
    val baseOpt = eio.correspondingVertexSet.map(vs => getData(vs).asInstanceOf[VertexSetData])
    val data = eio.read(baseOpt)
    set(entity, data)
    data
  }

  private def set(entity: MetaGraphEntity, data: EntityData) = synchronized {
    entityCache(entity.gUID) = data
  }

  def clear() = synchronized {
    entityCache.clear()
    sparkCachedEntities.clear()
    entitiesOnDiskCache.clear()
    dataRoot.clear()
  }

  private def asSparkOp[I <: InputSignatureProvider, O <: MetaDataSetProvider](
    instance: MetaGraphOperationInstance): SparkOperation[I, O] =
    asSparkOp(instance.asInstanceOf[TypedOperationInstance[I, O]])

  private def asSparkOp[I <: InputSignatureProvider, O <: MetaDataSetProvider](
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
    asSparkOp(instance).execute(inputDatas, instance.result, outputBuilder, runtimeContext)
    outputBuilder.dataMap.toMap
  }

  private def computeNow(instance: MetaGraphOperationInstance): Unit = {
    val logger = new OperationLogger(instance, executionContext)
    val inputs = instance.inputs.all.map {
      case (name, entity) => name -> getData(entity)
    }
    for ((name, data) <- inputs) logger.addInput(name.toString, data)
    val sparkOp = asSparkOp(instance)
    if (sparkOp.isHeavy) {
      log.info(s"PERF HEAVY Starting to compute heavy operation instance $instance")
      logger.startTimer()
    }
    val inputDatas = DataSet(inputs)
    for (scalar <- instance.outputs.scalars.values) {
      log.info(s"PERF Computing scalar $scalar")
    }

    // Keeping the original RDDs in this shorter scope allows the later GC to clean them up.
    {
      val output = run(instance, inputDatas)
      validateOutput(instance, output)
      // Reloading attributes needs us to have reloaded the vertex sets already. Hence the sort.
      val outputMeta = instance.outputs.all.values
      for (o <- outputMeta.toSeq.sortBy(o => if (o.isInstanceOf[VertexSet]) 1 else 2)) {
        val data = if (sparkOp.isHeavy) {
          if (!sparkOp.hasCustomSaving) saveToDisk(output(o.gUID))
          if (o.isInstanceOf[Scalar[_]]) {
            output(o.gUID) // No need to reload scalars, they are not lazy like RDDs.
          } else {
            val loaded = load(o)
            logger.addOutput(loaded)
            loaded
          }
        } else {
          // Serialize scalars even for non-heavy ops.
          if (o.isInstanceOf[Scalar[_]]) {
            saveToDisk(output(o.gUID))
          }
          output(o.gUID)
        }
        set(data.entity, data)
      }
    }
    markOperationComplete(instance)
    for (scalar <- instance.outputs.scalars.values) {
      log.info(s"PERF Computed scalar $scalar")
    }
    if (sparkOp.isHeavy) {
      // A GC is helpful here to avoid filling up the disk on the executors. (#2098)
      System.gc()
      log.info(s"PERF HEAVY Finished computing heavy operation instance $instance")
      logger.stopTimer()
    }
    logger.write()
  }

  // Mark the operation as complete. Entities may not be loaded from incomplete operations.
  // The reason for this is that an operation may give different results if the number of
  // partitions is different. So for consistency, all outputs must be from the same run.
  private def markOperationComplete(instance: MetaGraphOperationInstance): Unit = {
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
          entityCache(vs.gUID).asInstanceOf[VertexSetData]
      }
      assert(
        vsd.rdd.partitioner.get eq entityd.rdd.partitioner.get,
        s"The partitioner of $entity does not match the partitioner of $vs.")
    }
  }

  override def compute(instance: MetaGraphOperationInstance) = {
    SafeFuture[Unit](computeNow(instance))
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    instance.operation.isInstanceOf[SparkOperation[_, _]]
  }
  override def has(entity: MetaGraphEntity): Boolean = synchronized {
    entityCache.contains(entity.gUID) || canLoadEntityFromDisk(entity)
  }

  def getData(e: MetaGraphEntity): EntityData = {
    if (entityCache.contains(e.gUID)) entityCache(e.gUID)
    else if (canLoadEntityFromDisk(e)) load(e)
    else throw new AssertionError(s"Entity is not available in Spark domain: $e")
  }

  // Convenience for awaiting something in this execution context.
  def await[T](f: SafeFuture[T]): T = f.awaitResult(Duration.Inf)

  override def get[T](scalar: Scalar[T]) = {
    SafeFuture.successful(getData(scalar).asInstanceOf[ScalarData[T]].value)
  }

  private def enforceCoLocationWithIdSet[T: ClassTag](
    entity: MetaGraphEntity,
    idSet: VertexSet): (UniqueSortedRDD[Long, T], Option[Long]) = {
    val data = getData(entity).asInstanceOf[EntityRDDData[T]]
    val parent = getData(idSet).asInstanceOf[VertexSetData]
    val vsRDD = parent.rdd.copyWithAncestorsCached
    // Enforcing colocation:
    val rawRDD = data.rdd
    assert(
      vsRDD.partitions.size == rawRDD.partitions.size,
      s"$vsRDD and $rawRDD should have the same number of partitions, " +
        s"but ${vsRDD.partitions.size} != ${rawRDD.partitions.size}\n" +
        s"${vsRDD.toDebugString}\n${rawRDD.toDebugString}")
    import com.lynxanalytics.biggraph.spark_util.Implicits.PairRDDUtils
    (vsRDD.zipPartitions(rawRDD, preservesPartitioning = true) {
      (it1, it2) => it2
    }.asUniqueSortedRDD, data.count)
  }
  def cache(entity: MetaGraphEntity): Unit = {
    synchronized {
      if (!sparkCachedEntities.contains(entity.gUID)) {
        entityCache(entity.gUID) = entity match {
          case vs: VertexSet => getData(vs).asInstanceOf[VertexSetData].cached
          case eb: EdgeBundle =>
            val (rdd, count) = enforceCoLocationWithIdSet[Edge](eb, eb.idSet)
            new EdgeBundleData(eb, rdd, count)
          case heb: HybridBundle => getData(heb).asInstanceOf[HybridBundleData].cached
          case va: Attribute[_] =>
            def cached[T](va: Attribute[T]) = {
              val (rdd, count) = enforceCoLocationWithIdSet(va, va.vertexSet)(va.classTag)
              new AttributeData[T](va, rdd, count)
            }
            cached(va)
          case sc: Scalar[_] => getData(sc)
          case tb: Table => getData(tb)
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
    entitiesOnDiskCache(entity.gUID) = true
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
        val future: SafeFuture[EntityData] = e match {
          case e: VertexSet => SafeFuture(new VertexSetData(
            e, parallelize(source.get(e).toSeq.map((_, ()))), count = Some(source.get(e).size)))
          case e: EdgeBundle => SafeFuture {
            val seq = source.get(e).toSeq
            val vs = getData(e.idSet)
            val partitioner = vs.asInstanceOf[VertexSetData].rdd.partitioner.get
            val rdd = runtimeContext.sparkContext.parallelize(seq).sortUnique(partitioner)
            new EdgeBundleData(e, rdd, count = Some(seq.size))
          }
          case e: Attribute[_] =>
            def attr[T: reflect.ClassTag](e: Attribute[T]) = {
              val seq = source.get(e).toSeq
              val vs = getData(e.vertexSet)
              val partitioner = vs.asInstanceOf[VertexSetData].rdd.partitioner.get
              val rdd = runtimeContext.sparkContext.parallelize(seq).sortUnique(partitioner)
              new AttributeData[T](e, rdd, count = Some(seq.size))
            }
            SafeFuture(attr(e)(e.classTag))
          case e: Scalar[_] => source.get(e).map(new ScalarData(e, _))
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        }
        future.map { data =>
          saveToDisk(data)
        }
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
