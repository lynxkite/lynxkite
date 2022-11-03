// Classes for reading and writing EntityData to storage.

package com.lynxanalytics.biggraph.graph_api.io

import com.lynxanalytics.biggraph.graph_api.TypeTagToFormat
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.HashPartitioner
import play.api.libs.json
import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.{logger => log}
import com.lynxanalytics.biggraph.Environment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._

object IOContext {
  // Encompasses the Hadoop OutputFormat, Writer, and Committer in one object.
  private class TaskFile(
      tracker: String,
      stage: Int,
      taskType: hadoop.mapreduce.TaskType,
      task: Int,
      attempt: Int,
      file: HadoopFile,
      collection: TaskFileCollection) {
    import hadoop.mapreduce.lib.output.SequenceFileOutputFormat
    val fmt = new SequenceFileOutputFormat[hadoop.io.LongWritable, hadoop.io.BytesWritable]()
    val context = {
      val config = new hadoop.mapred.JobConf(file.hadoopConfiguration)
      // Set path for Hadoop 2.
      config.set(
        hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR,
        file.resolvedNameWithNoCredentials)
      config.setOutputKeyClass(classOf[hadoop.io.LongWritable])
      config.setOutputValueClass(classOf[hadoop.io.BytesWritable])
      val jobID = new hadoop.mapred.JobID(tracker, stage)
      val taskID = new hadoop.mapred.TaskID(jobID, taskType, task)
      val attemptID = new hadoop.mapred.TaskAttemptID(taskID, attempt)
      new hadoop.mapreduce.task.TaskAttemptContextImpl(config, attemptID)
    }
    val committer = fmt.getOutputCommitter(context)
    lazy val writer = {
      val w = fmt.getRecordWriter(context)
      collection.registerForClosing(this)
      w
    }
  }

  private class TaskFileCollection(
      tracker: String,
      stage: Int,
      taskType: hadoop.mapreduce.TaskType,
      task: Int,
      attempt: Int) {
    val toClose = new collection.mutable.ListBuffer[TaskFile]()
    def createTaskFile(path: HadoopFile) = {
      new TaskFile(tracker, stage, taskType, task, attempt, path, this)
    }
    def registerForClosing(file: TaskFile) = {
      toClose += file
    }
    // Closes any writers that were created.
    def closeWriters() = for (file <- toClose) file.writer.close(file.context)
  }
}

case class IOContext(dataRoot: DataRoot, sparkSession: spark.sql.SparkSession) {
  val sparkContext = sparkSession.sparkContext
  def partitionedPath(entity: MetaGraphEntity): HadoopFileLike =
    dataRoot / io.PartitionedDir / entity.gUID.toString

  def partitionedPath(entity: MetaGraphEntity, numPartitions: Int): HadoopFileLike =
    partitionedPath(entity) / numPartitions.toString
}

object EntityIO {
  // These "constants" are mutable for the sake of testing.
  var verticesPerPartition =
    Environment.envOrElse("KITE_VERTICES_PER_PARTITION", "200000").toInt
  var tolerance =
    Environment.envOrElse("KITE_VERTICES_PARTITION_TOLERANCE", "2.0").toDouble

  implicit val fEntityMetadata = json.Json.format[EntityMetadata]
  def operationPath(dataRoot: DataRoot, instance: MetaGraphOperationInstance) =
    dataRoot / io.OperationsDir / instance.gUID.toString

  // The ideal number of partitions for n rows.
  def desiredNumPartitions(n: Long): Int = {
    val p = Math.ceil(n.toDouble / verticesPerPartition).toInt
    // Always have at least 1 partition.
    p max 1
  }
}

case class EntityMetadata(lines: Long, serialization: Option[String]) {
  def write(path: HadoopFile) = {
    import EntityIO.fEntityMetadata
    val metaFile = path / io.Metadata
    assert(!metaFile.exists, s"Metafile $metaFile should not exist before we write it.")
    val metaFileCreated = path / io.MetadataCreate
    metaFileCreated.deleteIfExists()
    val j = json.Json.toJson(this)
    metaFileCreated.createFromStrings(json.Json.prettyPrint(j))
    metaFileCreated.renameTo(metaFile)
  }
}

abstract class EntityIO(val entity: MetaGraphEntity, context: IOContext) {
  def correspondingVertexSet: Option[VertexSet] = None
  def read(parent: Option[VertexSetData] = None): EntityData
  def write(data: EntityData): Unit
  def delete(): Boolean
  def exists: Boolean
  def mayHaveExisted: Boolean // May be outdated or incorrectly true.

  protected val dataRoot = context.dataRoot
  protected val sc = context.sparkContext
  protected def operationMayHaveExisted = EntityIO.operationPath(dataRoot, entity.source).mayHaveExisted
  protected def operationExists = (EntityIO.operationPath(dataRoot, entity.source) / io.Success).exists
}

class ScalarIO[T](entity: Scalar[T], context: IOContext)
    extends EntityIO(entity, context) {

  def read(parent: Option[VertexSetData]): ScalarData[T] = {
    assert(parent == None, s"Scalar read called with parent $parent")
    log.info(s"PERF Loading scalar $entity from disk")
    val jsonString = serializedScalarFileName.forReading.readAsString()
    val format = TypeTagToFormat.typeTagToFormat(entity.typeTag)
    val value = format.reads(json.Json.parse(jsonString)).get
    log.info(s"PERF Loaded scalar $entity from disk")
    new ScalarData[T](entity, value)
  }

  def write(data: EntityData): Unit = {
    val scalarData = data.asInstanceOf[ScalarData[T]]
    log.info(s"PERF Writing scalar $entity to disk")
    val format = TypeTagToFormat.typeTagToFormat(entity.typeTag)
    val output = format.writes(scalarData.value)
    val targetDir = path.forWriting
    targetDir.mkdirs
    serializedScalarFileName.forWriting.createFromStrings(json.Json.prettyPrint(output))
    successPath.forWriting.createFromStrings("")
    log.info(s"PERF Written scalar $entity to disk")
  }
  def delete() = path.forWriting.deleteIfExists()
  def exists = {
    val op = entity.source.operation
    if (op.isInstanceOf[SparkOperation[_, _]]) {
      operationExists && (path / Success).exists
    } else (path / Success).exists
  }
  def mayHaveExisted = {
    val op = entity.source.operation
    if (op.isInstanceOf[SparkOperation[_, _]]) {
      operationMayHaveExisted && path.mayHaveExisted
    } else path.mayHaveExisted
  }
  private def path = dataRoot / ScalarsDir / entity.gUID.toString
  private def serializedScalarFileName: HadoopFileLike = path / "serialized_data"
  private def successPath: HadoopFileLike = path / Success
}

class TableIO(entity: Table, context: IOContext) extends EntityIO(entity, context) {

  def read(parent: Option[VertexSetData]): TableData = {
    assert(parent == None, s"Table read called with parent $parent")
    log.info(s"PERF Loading table $entity from disk")
    val parquet = context.sparkSession.read.parquet(path.forReading.resolvedName)
    val df = columnsFromParquet(parquet)
    SQLHelper.assertTableHasCorrectSchema(entity, df.schema)
    log.info(s"PERF Loaded table $entity from disk")
    new TableData(entity, df)
  }

  def write(data: EntityData): Unit = {
    val tableData = data.asInstanceOf[TableData]
    val df = columnsToParquet(tableData.df)
    log.info(s"PERF Writing table $entity to disk")
    df.write.parquet(path.forWriting.resolvedName)
    log.info(s"PERF Written table $entity to disk")
  }

  // Renames columns to names that are allowed by Parquet.
  // Parquet disallows column names that contain any of " ,;{}()\n\t=".
  private def columnsToParquet(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (df, name) =>
      val safeName =
        if (name.startsWith("_") || name.matches(".*[ ,;{}()\n\t=].*"))
          "_" + java.net.URLEncoder.encode(name, "utf-8")
        else name
      df.withColumnRenamed(name, safeName)
    }
  }

  // Restores the original names.
  private def columnsFromParquet(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (df, name) =>
      val originalName =
        if (name.startsWith("_"))
          java.net.URLDecoder.decode(name.substring(1), "utf-8")
        else name
      df.withColumnRenamed(name, originalName)
    }
  }

  def delete() = path.forWriting.deleteIfExists()
  def exists = {
    val op = entity.source.operation
    if (op.isInstanceOf[SparkOperation[_, _]]) {
      operationExists && (path / Success).exists
    } else (path / Success).exists
  }
  def mayHaveExisted = {
    val op = entity.source.operation
    if (op.isInstanceOf[SparkOperation[_, _]]) {
      operationMayHaveExisted && path.mayHaveExisted
    } else path.mayHaveExisted
  }

  private def path = dataRoot / TablesDir / entity.gUID.toString
  private def successPath: HadoopFileLike = path / Success
}

object RatioSorter {
  def ratio(a: Int, desired: Int): Double = {
    val aa = a.toDouble
    if (aa > desired) aa / desired
    else desired.toDouble / aa
  }
}

case class RatioSorter(elements: Seq[Int], desired: Int) {
  assert(desired > 0, "RatioSorter only supports positive integers")
  assert(elements.filter(_ <= 0).isEmpty, "RatioSorter only supports positive integers")
  private val sorted: Seq[(Int, Double)] = {
    elements.map(a => (a, RatioSorter.ratio(a, desired))).sortBy(_._2)
  }

  val best: Option[Int] = sorted.map(_._1).headOption

  def getBestWithinTolerance(tolerance: Double): Option[Int] = {
    sorted.filter(_._2 < tolerance).map(_._1).headOption
  }
}

abstract class PartitionedDataIO[T, DT <: EntityRDDData[T]](
    entity: MetaGraphEntity,
    context: IOContext)
    extends EntityIO(entity, context) {

  // This class reflects the current state of the disk during the read operation
  case class EntityLocationSnapshot(availablePartitions: Map[Int, HadoopFile]) {
    val hasPartitionedDirs = availablePartitions.nonEmpty
    val metaPathExists = metaFile.forReading.exists
    val hasPartitionedData = hasPartitionedDirs && metaPathExists

    assert(hasPartitionedData, s"There seems to be no valid data in ${partitionedPath.forReading}")

    private lazy val metadata: EntityMetadata = {
      import EntityIO.fEntityMetadata
      val j = json.Json.parse(metaFile.forReading.readAsString)
      j.as[EntityMetadata]
    }

    val numVertices = metadata.lines

    def serialization = metadata.serialization.getOrElse("kryo")
  }

  def read(parent: Option[VertexSetData] = None): DT = {
    val entityLocation = EntityLocationSnapshot(computeAvailablePartitions)
    val pn = parent.map(_.rdd.partitions.size).getOrElse(selectPartitionNumber(entityLocation))
    val partitioner = parent.map(_.rdd.partitioner.get).getOrElse(new HashPartitioner(pn))

    val (file, serialization) =
      if (entityLocation.availablePartitions.contains(pn))
        (entityLocation.availablePartitions(pn), entityLocation.serialization)
      else
        repartitionTo(entityLocation, partitioner)

    val dataRead = finalRead(
      file,
      entityLocation.numVertices,
      serialization,
      partitioner,
      parent)
    assert(
      dataRead.rdd.partitions.size == pn,
      s"finalRead mismatch: ${dataRead.rdd.partitions.size} != $pn")
    dataRead
  }

  def write(data: EntityData): Unit = {
    assert(data.entity == entity, s"Tried to write $data through EntityIO for $entity.")
    log.info(s"PERF Instantiating entity $entity on disk")
    val partitions = castData(data).rdd.partitions.size
    val (lines, serialization) = write(data, targetDir(partitions))
    val metadata = EntityMetadata(lines, Some(serialization))
    metadata.write(partitionedPath.forWriting)
    log.info(s"PERF Instantiated entity $entity on disk")
  }

  // The subclasses know the specific type and can thus make a safer cast.
  def castData(data: EntityData): EntityRDDData[T]

  def write(data: EntityData, dir: HadoopFile) = dir.saveEntityRDD(castData(data).rdd, valueTypeTag)

  def valueTypeTag: TypeTag[T] // The TypeTag of the values we write out.
  def valueClassTag: reflect.ClassTag[T]

  def delete(): Boolean = {
    partitionedPath.forWriting.deleteIfExists()
  }

  def exists = {
    val vsExists = correspondingVertexSet.map(new VertexSetIO(_, context).exists).getOrElse(true)
    val op = entity.source.operation
    if (op.isInstanceOf[SparkOperation[_, _]]) {
      vsExists && operationExists && existsPartitioned
    } else vsExists && existsPartitioned
  }

  def mayHaveExisted = {
    val op = entity.source.operation
    if (op.isInstanceOf[SparkOperation[_, _]]) {
      operationMayHaveExisted && partitionedPath.mayHaveExisted
    } else partitionedPath.mayHaveExisted
  }

  private val partitionedPath = context.partitionedPath(entity)
  private val metaFile = partitionedPath / io.Metadata

  protected def targetDir(numPartitions: Int) =
    context.partitionedPath(entity, numPartitions).forWriting

  private def computeAvailablePartitions = {
    val subDirs = (partitionedPath / "*").list
    val number = "[1-9][0-9]*".r
    val numericSubdirs = subDirs.filter(x => number.pattern.matcher(x.path.getName).matches)
    val existingCandidates = numericSubdirs.filter(x => (x / Success).exists)
    val resultList = existingCandidates.map { x => (x.path.getName.toInt, x) }
    resultList.toMap
  }

  // This method performs the actual reading of the rdddata, from a path
  // The parent VertexSetData is given for EdgeBundleData and AttributeData[T] so that
  // the corresponding data will be co-located.
  // A partitioner is also passed, because we don't want to create another one for VertexSetData
  protected def finalRead(
      path: HadoopFile,
      count: Long,
      serialization: String,
      partitioner: spark.Partitioner,
      parent: Option[VertexSetData] = None): DT

  protected def bestPartitionedSource(
      entityLocation: EntityLocationSnapshot,
      desiredPartitionNumber: Int): (Int, HadoopFile) = {
    assert(
      entityLocation.availablePartitions.nonEmpty,
      s"There should be valid sub directories in $partitionedPath")
    val ratioSorter =
      RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desiredPartitionNumber)
    val pn = ratioSorter.best.get
    pn -> entityLocation.availablePartitions(pn)
  }

  // Copies and repartitions Hadoop file src to dst
  // and returns the number of lines written.
  protected def copyAndRepartition[U: TypeTag: reflect.ClassTag](
      src: HadoopFile,
      srcPartitions: Int,
      dst: HadoopFile,
      partitioner: spark.Partitioner): Long = {
    val typeName = EntitySerializer.forType(typeTag[U]).name
    val oldRDD = src.loadEntityRDD(sc, typeName, srcPartitions)
    val newRDD = oldRDD.sort(partitioner)
    dst.saveEntityRDD(newRDD, typeTag[U])._1
  }

  // Returns the file and the serialization format.
  protected def repartitionTo(
      entityLocation: EntityLocationSnapshot,
      partitioner: spark.Partitioner): (HadoopFile, String) = {
    val pn = partitioner.numPartitions
    val (sn, src) = bestPartitionedSource(entityLocation, pn)
    val dst = targetDir(pn)
    val lines = copyAndRepartition[T](src, sn, dst, partitioner)(valueTypeTag, valueClassTag)
    assert(
      entityLocation.numVertices == lines,
      s"Unexpected row count (${entityLocation.numVertices} != $lines) for $entity")
    (dst, entityLocation.serialization)
  }

  private def desiredPartitions(entityLocation: EntityLocationSnapshot) = {
    val vertices = entityLocation.numVertices
    EntityIO.desiredNumPartitions(vertices)
  }

  private def selectPartitionNumber(entityLocation: EntityLocationSnapshot): Int = {
    val desired = desiredPartitions(entityLocation)
    val ratioSorter = RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desired)
    ratioSorter.getBestWithinTolerance(EntityIO.tolerance).getOrElse(desired)
  }

  private def existsPartitioned = computeAvailablePartitions.nonEmpty && metaFile.exists
}

class VertexSetIO(entity: VertexSet, context: IOContext)
    extends PartitionedDataIO[Unit, VertexSetData](entity, context) {

  def finalRead(
      path: HadoopFile,
      count: Long,
      serialization: String,
      partitioner: spark.Partitioner,
      parent: Option[VertexSetData]): VertexSetData = {
    assert(parent == None, s"finalRead for $entity should not take a parent option")
    val rdd = path.loadEntityRDD[Unit](sc, serialization, partitioner.numPartitions)
    new VertexSetData(entity, rdd.asUniqueSortedRDD(partitioner), Some(count))
  }

  def castData(data: EntityData) = data.asInstanceOf[VertexSetData]

  def valueTypeTag = typeTag[Unit]
  def valueClassTag = reflect.classTag[Unit]
}

class EdgeBundleIO(entity: EdgeBundle, context: IOContext)
    extends PartitionedDataIO[Edge, EdgeBundleData](entity, context) {

  override def correspondingVertexSet = Some(entity.idSet)

  def finalRead(
      path: HadoopFile,
      count: Long,
      serialization: String,
      partitioner: spark.Partitioner,
      parent: Option[VertexSetData]): EdgeBundleData = {
    assert(
      partitioner eq parent.get.rdd.partitioner.get,
      s"Partitioner mismatch for $entity.")
    val rdd = path.loadEntityRDD[Edge](sc, serialization, partitioner.numPartitions)
    new EdgeBundleData(
      entity,
      rdd.asUniqueSortedRDD(partitioner),
      Some(count))
  }

  def castData(data: EntityData) = data.asInstanceOf[EdgeBundleData]

  def valueTypeTag = typeTag[Edge]
  def valueClassTag = reflect.classTag[Edge]
}

class HybridBundleIO(entity: HybridBundle, context: IOContext)
    extends PartitionedDataIO[ID, HybridBundleData](entity, context) {

  override def correspondingVertexSet = None

  def finalRead(
      path: HadoopFile,
      count: Long,
      serialization: String,
      partitioner: spark.Partitioner,
      parent: Option[VertexSetData]): HybridBundleData = {
    import scala.reflect._
    implicit val cv = classTag[ID]
    val idSerializer = EntitySerializer.forType(typeTag[ID]).name
    val longSerializer = EntitySerializer.forType(typeTag[Long]).name
    val smallKeysRDD = (path / "small_keys_rdd").loadEntityRDD[ID](
      sc,
      idSerializer,
      partitioner.numPartitions)
    val largeKeysSet = (path / "larges").loadEntityRDD[Long](sc, longSerializer, 1).collect.toSeq
    val largeKeysRDD =
      if (largeKeysSet.isEmpty) {
        None
      } else {
        Some((path / "large_keys_rdd").loadEntityRDD[ID](
          sc,
          idSerializer,
          partitioner.numPartitions))
      }
    new HybridBundleData(
      entity,
      HybridRDD(
        largeKeysRDD,
        smallKeysRDD.asSortedRDD(partitioner),
        largeKeysSet),
      Some(count))
  }

  override protected def repartitionTo(
      entityLocation: EntityLocationSnapshot,
      partitioner: spark.Partitioner): (HadoopFile, String) = {
    val pn = partitioner.numPartitions
    val (sn, src) = bestPartitionedSource(entityLocation, pn)
    val dst = targetDir(pn)
    var lines = 0L
    lines += copyAndRepartition[ID](src / "small_keys_rdd", sn, dst / "small_keys_rdd", partitioner)
    if ((src / "large_keys_rdd").exists()) {
      lines += copyAndRepartition[ID](
        src / "large_keys_rdd",
        sn,
        dst / "large_keys_rdd",
        partitioner)
    }
    copyAndRepartition[Long](src / "larges", 1, dst / "larges", new spark.HashPartitioner(1))
    assert(
      entityLocation.numVertices == lines,
      s"Unexpected row count (${entityLocation.numVertices} != $lines) for $entity")
    (dst, entityLocation.serialization)
  }

  override def write(data: EntityData, dir: HadoopFile): (Long, String) = {
    val hybridRDD = castData(data).rdd
    val linesSmallKeys = (dir / "small_keys_rdd")
      .saveEntityRDD(hybridRDD.smallKeysRDD, valueTypeTag)._1
    (dir / "larges")
      .saveEntityRDD(sc.parallelize(hybridRDD.larges, 1), typeTag[Long])
    val linesLargeKeys =
      if (hybridRDD.isSkewed) {
        (dir / "large_keys_rdd")
          .saveEntityRDD(hybridRDD.largeKeysRDD.get, valueTypeTag)._1
      } else { 0L }
    (dir / Success).createEmpty
    (linesSmallKeys + linesLargeKeys, "hybrid")
  }

  def castData(data: EntityData) = data.asInstanceOf[HybridBundleData]

  def valueTypeTag = typeTag[ID]
  def valueClassTag = reflect.classTag[ID]
}

class AttributeIO[T](entity: Attribute[T], context: IOContext)
    extends PartitionedDataIO[T, AttributeData[T]](entity, context) {
  override def correspondingVertexSet = Some(entity.vertexSet)

  def finalRead(
      path: HadoopFile,
      count: Long,
      serialization: String,
      partitioner: spark.Partitioner,
      parent: Option[VertexSetData]): AttributeData[T] = {
    assert(
      partitioner eq parent.get.rdd.partitioner.get,
      s"Partitioner mismatch for $entity.")
    implicit val ct = entity.classTag
    implicit val tt = entity.typeTag
    val rdd = path.loadEntityRDD[T](sc, serialization, partitioner.numPartitions)
    new AttributeData[T](
      entity,
      rdd.asUniqueSortedRDD(partitioner),
      Some(count))
  }

  def castData(data: EntityData) =
    data.asInstanceOf[AttributeData[_]].runtimeSafeCast(valueTypeTag)

  def valueTypeTag = entity.typeTag
  def valueClassTag = entity.classTag
}
