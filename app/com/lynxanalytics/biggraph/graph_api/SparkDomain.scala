// The SparkDomain can run operations on Spark and give access to their outputs.
//
// It also manages loading the data from disk and saving the data when RDDs are requested.

package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

import org.apache.spark
import org.apache.spark.sql.{SQLContext, Row}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import reflect.runtime.universe.typeTag
import java.nio.file.Paths
import play.api.libs.json
import com.lynxanalytics.biggraph.{bigGraphLogger => log}
import com.lynxanalytics.biggraph.graph_api.io.DataRoot
import com.lynxanalytics.biggraph.graph_api.io.EntityIO
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD

class SparkDomain(
    val sparkSession: spark.sql.SparkSession,
    val repositoryPath: HadoopFile,
    val ephemeralPath: Option[HadoopFile] = None)
    extends Domain {
  implicit val executionContext =
    ThreadUtil.limitedExecutionContext(
      "SparkDomain",
      maxParallelism = LoggedEnvironment.envOrElse("KITE_PARALLELISM", "5").toInt)
  lazy val masterSQLContext = {
    val sqlContext = sparkSession.sqlContext
    UDF.register(sqlContext.udf)
    sqlContext
  }
  val isLocal = sparkSession.sparkContext.isLocal
  // Access to the following collections must always be synchronized on SparkDomain.
  // But do not hold the locks for long. Don't call potentially slow methods, like getData().
  private val entitiesOnDiskCache = collection.mutable.Map[UUID, Boolean]()
  private val entityCache = collection.mutable.Map[UUID, EntityData]()
  private val sparkCachedEntities = mutable.Set[UUID]()
  private val entityPromises = collection.mutable.Map[UUID, concurrent.Promise[Unit]]()

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

  private def canLoadEntityFromDisk(entity: MetaGraphEntity): Boolean = synchronized {
    assert(!entityCache.contains(entity.gUID), s"We already have $entity.")
    entitiesOnDiskCache.getOrElseUpdate(
      entity.gUID,
      entityIO(entity).mayHaveExisted && entityIO(entity).exists)
  }

  private def load(entity: MetaGraphEntity): EntityData = {
    assert(canLoadEntityFromDisk(entity), s"Entity is not available in Spark domain: $entity")
    val eio = entityIO(entity)
    log.info(s"PERF Found entity $entity on disk")
    // For edge bundles and attributes we need to load the base vertex set first
    val baseOpt = eio.correspondingVertexSet.map(vs => getData(vs).asInstanceOf[VertexSetData])
    eio.read(baseOpt)
  }

  private def set(entity: MetaGraphEntity, data: EntityData) = synchronized {
    assert(!entityPromises.contains(entity.gUID), s"Trying to set an entity that has a promise: $entity")
    assert(!entityCache.contains(entity.gUID), s"Trying to set an already loaded entity: $entity")
    entityCache(entity.gUID) = data
  }

  // Gets the EntityData from the entityCache or produces it with "fn".
  // Just like Map.getOrElseUpdate. The twist is that it guarantees that "fn" will not be
  // executed more than once if called from multiple threads.
  private def getOrElseUpdateData(entity: MetaGraphEntity, fn: => EntityData): EntityData = {
    // We need to hold the lock from checking entityCache to placing the promise in entityPromises.
    // That is the reason for the peculiar organization of this code.
    synchronized {
      entityCache.get(entity.gUID).toLeft {
        entityPromises.get(entity.gUID) match {
          case Some(p) => (p, false)
          case None =>
            val p = concurrent.Promise[Unit]()
            entityPromises(entity.gUID) = p
            (p, true)
        }
      }
    } match {
      case Left(ed) => ed
      case Right((promise, true)) =>
        // Why is the below code not simpler? (Like "val result = Try(fn)".)
        // 1. set() has to go before promise.complete() because the thread waiting on the promise
        //    expects to see the data in entityCache.
        // 2. set() needs the actual data. We can either access that outside of the Try as
        //    result.get or inside.
        // 3. Outside of Try and before promise.complete() we cannot use result.get because it may
        //    throw an exception and then promise.complete() would be never called.
        //    So set() must go inside the Try.
        // 4. We want to assert there is no current promise in set().
        //    So entityPromises -= entity.gUID must go before set().
        // 5. These two need to be in a synchronized block otherwise we could be caught with
        //    no promise and no entityCache.
        val result = util.Try {
          val data = fn
          synchronized {
            entityPromises -= entity.gUID
            set(entity, data)
          }
          data
        }
        // Makes the Await throw the same exception if an exception was thrown here.
        promise.complete(result.map(_ => ()))
        if (result.isFailure) synchronized { // Allow retries by removing the promise.
          entityPromises -= entity.gUID
        }
        result.get
      case Right((promise, false)) =>
        concurrent.Await.result(promise.future, concurrent.duration.Duration.Inf)
        synchronized { entityCache(entity.gUID) }
    }
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
    val inputs = instance.inputs.all.map {
      case (name, entity) => name -> getData(entity)
    }
    val sparkOp = asSparkOp(instance)
    if (sparkOp.isHeavy) {
      log.info(s"PERF HEAVY Starting to compute heavy operation instance $instance")
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
        val data =
          if (sparkOp.isHeavy) {
            if (sparkOp.hasCustomSaving) synchronized {
              // Just need to remember it's already saved.
              entitiesOnDiskCache(o.gUID) = true
            }
            else if (o.isInstanceOf[Scalar[_]]) {
              // Save asynchronously, but we can use the value immediately.
              SafeFuture.async(saveToDisk(output(o.gUID)))
              set(o, output(o.gUID))
            } else {
              // Save synchronously, and we will load it back when accessed.
              saveToDisk(output(o.gUID))
            }
          } else {
            if (o.isInstanceOf[Scalar[_]]) {
              SafeFuture.async(saveToDisk(output(o.gUID)))
            }
            set(o, output(o.gUID))
          }
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
    }
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
        case None => synchronized {
            assert(entityCache.contains(vs.gUID), s"$vs, vertex set of $entity, not known")
            entityCache(vs.gUID).asInstanceOf[VertexSetData]
          }
      }
      assert(
        vsd.rdd.partitioner.get eq entityd.rdd.partitioner.get,
        s"The partitioner of $entity does not match the partitioner of $vs.")
    }
  }

  override def compute(instance: MetaGraphOperationInstance) = {
    SafeFuture.async[Unit](computeNow(instance))
  }

  override def canCompute(instance: MetaGraphOperationInstance): Boolean = {
    instance.operation.isInstanceOf[SparkOperation[_, _]]
  }
  override def has(entity: MetaGraphEntity): Boolean = synchronized {
    entityCache.contains(entity.gUID) || canLoadEntityFromDisk(entity)
  }

  def getData(e: MetaGraphEntity): EntityData = {
    getOrElseUpdateData(e, load(e))
  }

  // Convenience for awaiting something in this execution context.
  def await[T](f: SafeFuture[T]): T = f.awaitResult(Duration.Inf)

  override def get[T](scalar: Scalar[T]) = {
    SafeFuture.async(getData(scalar).asInstanceOf[ScalarData[T]].value)
  }

  private def enforceCoLocationWithIdSet[T: ClassTag](
      entity: MetaGraphEntity,
      idSet: VertexSet): (UniqueSortedRDD[Long, T], Option[Long]) = synchronized {
    val data = entityCache(entity.gUID).asInstanceOf[EntityRDDData[T]]
    val parent = entityCache(idSet.gUID).asInstanceOf[VertexSetData]
    val vsRDD = parent.rdd.copyWithAncestorsCached
    // Enforcing colocation:
    val rawRDD = data.rdd
    assert(
      vsRDD.partitions.size == rawRDD.partitions.size,
      s"$vsRDD and $rawRDD should have the same number of partitions, " +
        s"but ${vsRDD.partitions.size} != ${rawRDD.partitions.size}\n" +
        s"${vsRDD.toDebugString}\n${rawRDD.toDebugString}",
    )
    import com.lynxanalytics.biggraph.spark_util.Implicits.PairRDDUtils
    (
      vsRDD.zipPartitions(rawRDD, preservesPartitioning = true) {
        (it1, it2) => it2
      }.asUniqueSortedRDD,
      data.count)
  }
  def cache(entity: MetaGraphEntity): Unit = synchronized {
    assert(entityCache.contains(entity.gUID), s"Cannot cache unloaded entity: $entity")
    if (!sparkCachedEntities.contains(entity.gUID)) {
      entityCache(entity.gUID) = entity match {
        case vs: VertexSet => entityCache(vs.gUID).asInstanceOf[VertexSetData].cached
        case eb: EdgeBundle =>
          val (rdd, count) = enforceCoLocationWithIdSet[Edge](eb, eb.idSet)
          new EdgeBundleData(eb, rdd, count)
        case heb: HybridBundle => entityCache(heb.gUID).asInstanceOf[HybridBundleData].cached
        case va: Attribute[_] =>
          def cached[T](va: Attribute[T]) = {
            val (rdd, count) = enforceCoLocationWithIdSet(va, va.vertexSet)(va.classTag)
            new AttributeData[T](va, rdd, count)
          }
          cached(va)
        case sc: Scalar[_] => entityCache(sc.gUID)
        case tb: Table => entityCache(tb.gUID)
      }
      sparkCachedEntities.add(entity.gUID)
    }
  }

  private def saveToDisk(data: EntityData): Unit = {
    val entity = data.entity
    val eio = entityIO(entity)
    val doesNotExist = eio.delete()
    assert(doesNotExist, s"Cannot delete directory of entity $entity")
    log.info(s"Saving entity $entity ...")
    eio.write(data)
    synchronized {
      entitiesOnDiskCache(entity.gUID) = true
    }
    log.info(s"Entity $entity saved.")
  }

  def runtimeContext = {
    val broadcastDirectory = ephemeralPath.getOrElse(repositoryPath) / io.BroadcastsDir
    RuntimeContext(
      sparkContext = sparkSession.sparkContext,
      sqlContext = masterSQLContext,
      ioContext = io.IOContext(dataRoot, sparkSession),
      broadcastDirectory = broadcastDirectory,
      sparkDomain = this,
    )
  }

  def newSQLContext(): SQLContext = {
    val sqlContext = masterSQLContext.newSession()
    UDF.register(sqlContext.udf)
    sqlContext
  }

  override def canRelocateFrom(source: Domain): Boolean = {
    source match {
      case source: ScalaDomain => true
      case source: UnorderedSphynxLocalDisk => isLocal
      case source: UnorderedSphynxSparkDisk => !isLocal
      case _ => false
    }
  }

  override def relocateFrom(e: MetaGraphEntity, source: Domain): SafeFuture[Unit] = {
    val future: SafeFuture[EntityData] = source match {
      case source: ScalaDomain =>
        import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
        import com.lynxanalytics.biggraph.spark_util.Implicits._
        def parallelize[T: reflect.ClassTag](s: Seq[(ID, T)]): UniqueSortedRDD[ID, T] = {
          val rc = runtimeContext
          rc.sparkContext.parallelize(s).sortUnique(rc.partitionerForNRows(s.size))
        }
        e match {
          case e: VertexSet => SafeFuture.async(new VertexSetData(
              e,
              parallelize(source.get(e).toSeq.map((_, ()))),
              count = Some(source.get(e).size)))
          case e: EdgeBundle => SafeFuture.async({
              val seq = source.get(e).toSeq
              val vs = getData(e.idSet)
              val partitioner = vs.asInstanceOf[VertexSetData].rdd.partitioner.get
              val rdd = runtimeContext.sparkContext.parallelize(seq).sortUnique(partitioner)
              new EdgeBundleData(e, rdd, count = Some(seq.size))
            })
          case e: Attribute[_] =>
            def attr[T: reflect.ClassTag](e: Attribute[T]) = {
              val seq = source.get(e).toSeq
              val vs = getData(e.vertexSet)
              val partitioner = vs.asInstanceOf[VertexSetData].rdd.partitioner.get
              val rdd = runtimeContext.sparkContext.parallelize(seq).sortUnique(partitioner)
              new AttributeData[T](e, rdd, count = Some(seq.size))
            }
            SafeFuture.async(attr(e)(e.classTag))
          case e: Scalar[_] => source.get(e).map(new ScalarData(e, _))
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        }
      case source: UnorderedSphynxDisk => {
        val srcPath = source.getGUIDPath(e)
        import com.lynxanalytics.biggraph.spark_util.UniqueSortedRDD
        import com.lynxanalytics.biggraph.spark_util.Implicits._
        e match {
          case e: VertexSet => SafeFuture.async({
              val rdd = sparkSession.read.parquet(srcPath).rdd
              val size = rdd.count()
              new VertexSetData(
                e,
                rdd.map(r => (r.getAs[Long]("id"), ()))
                  .sortUnique(runtimeContext.partitionerForNRows(size)),
                Some(size))
            })
          case e: EdgeBundle => SafeFuture.async({
              val rdd = sparkSession.read.parquet(srcPath).rdd
              val size = rdd.count()
              new EdgeBundleData(
                e,
                rdd.map(r =>
                  (r.getAs[Long]("id"), Edge(r.getAs[Long]("src"), r.getAs[Long]("dst"))))
                  .sortUnique(runtimeContext.partitionerForNRows(size)),
                Some(size))
            })
          case e: Attribute[_] if e.typeTag == typeTag[Vector[Double]] =>
            def attr(e: Attribute[Vector[Double]]) = {
              val vs = getData(e.vertexSet)
              val partitioner = vs.asInstanceOf[VertexSetData].rdd.partitioner.get
              val df = sparkSession.read.parquet(srcPath)
              val rdd = df.rdd
              val size = rdd.count()
              val valueIdx = df.schema.fieldIndex("value")
              val castRDD =
                rdd.map(r => (r.getAs[Long]("id"), r.getSeq(valueIdx).toVector.asInstanceOf[Vector[Double]]))
              new AttributeData[Vector[Double]](e, castRDD.sortUnique(partitioner), Some(size))
            }
            SafeFuture.async(attr(e.asInstanceOf[Attribute[Vector[Double]]]))
          case e: Attribute[_] =>
            def attr[T: reflect.ClassTag](e: Attribute[T]) = {
              val vs = getData(e.vertexSet)
              val partitioner = vs.asInstanceOf[VertexSetData].rdd.partitioner.get
              val rdd = sparkSession.read.parquet(srcPath).rdd
              val size = rdd.count()
              new AttributeData[T](
                e,
                rdd.map(r =>
                  (r.getAs[Long]("id"), r.getAs[T]("value")))
                  .sortUnique(partitioner),
                Some(size))
            }
            SafeFuture.async(attr(e)(e.classTag))
          case s: Scalar[_] =>
            SafeFuture.async({
              val format = TypeTagToFormat.typeTagToFormat(s.typeTag)
              val jsonString = source match {
                case source: UnorderedSphynxSparkDisk =>
                  (source.dataDir / s.gUID.toString / "serialized_data").readAsString()
                case source: UnorderedSphynxLocalDisk =>
                  val fname = s"${srcPath}/serialized_data"
                  Source.fromFile(fname, "utf-8").getLines.mkString
              }
              val value = format.reads(json.Json.parse(jsonString)).get
              new ScalarData(s, value)
            })
          case t: Table => SafeFuture.async(new TableData(t, sparkSession.read.parquet(srcPath)))
          case _ => throw new AssertionError(s"Cannot fetch $e from $source")
        }
      }
    }
    future.map { data =>
      saveToDisk(data)
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
    val count: Option[Long] = None)
    extends EntityRDDData[Unit] {
  val vertexSet = entity
  def cached = new VertexSetData(entity, rdd.copyWithAncestorsCached, count)
}

class EdgeBundleData(
    val entity: EdgeBundle,
    val rdd: EdgeBundleRDD,
    val count: Option[Long] = None)
    extends EntityRDDData[Edge] {
  val edgeBundle = entity
  def cached = new EdgeBundleData(entity, rdd.copyWithAncestorsCached, count)
}

class HybridBundleData(
    val entity: HybridBundle,
    val rdd: HybridBundleRDD,
    val count: Option[Long] = None)
    extends EntityRDDData[ID] {
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
    tables.mapValues(_.table),
  )

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
      tables = all.collect { case (k, v: TableData) => (k, v) },
    )
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
