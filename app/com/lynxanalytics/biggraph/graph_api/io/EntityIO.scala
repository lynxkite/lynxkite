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
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util._

object IOContext {
  // Encompasses the Hadoop OutputFormat, Writer, and Committer in one object.
  private class TaskFile(
      tracker: String, stage: Int, taskType: hadoop.mapreduce.TaskType,
      task: Int, attempt: Int, file: HadoopFile,
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
      tracker: String, stage: Int, taskType: hadoop.mapreduce.TaskType, task: Int, attempt: Int) {
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

  // Writes multiple attributes and their vertex set to disk. The attributes are given in a
  // single RDD which will be iterated over only once.
  // It's the callers responsibility to make sure that the Seqs in data have elements of the right
  // type, corresponding to the given attributes. For wrong types, the behavior is unspecified,
  // it may or may not fail at write time.
  // Don't let the AnyType type parameter fool you, it has really no significance, you can basically
  // pass in an RDD of any kind of Seq you like. It's only needed because stupid RDDs are not
  // covariant, so taking AttributeRDD[Seq[_]] wouldn't be generic enough.
  def writeAttributes[AnyType](
    attributes: Seq[Attribute[_]],
    data: AttributeRDD[Seq[AnyType]]) = {

    val vs = attributes.head.vertexSet
    for (attr <- attributes) assert(attr.vertexSet == vs, s"$attr is not for $vs")

    // Delete output directories.
    val doesNotExist = new VertexSetIO(vs, this).delete()
    assert(doesNotExist, s"Cannot delete directory of $vs")
    for (attr <- attributes) {
      val doesNotExist = new AttributeIO(attr, this).delete()
      assert(doesNotExist, s"Cannot delete directory of $attr")
    }

    val outputEntities: Seq[MetaGraphEntity] = attributes :+ vs
    val paths = outputEntities.map(e => partitionedPath(e, data.partitions.size).forWriting)

    val trackerId = Timestamp.toString
    val rddId = data.id
    val vsCount = sparkContext.longAccumulator(s"Vertex count for ${vs.gUID}")
    val attrCounts = attributes.map {
      attr => sparkContext.longAccumulator(s"Attribute count for ${attr.gUID}")
    }
    val unitSerializer = EntitySerializer.forType[Unit]
    val serializers = attributes.map(EntitySerializer.forAttribute(_))
    // writeShard is the function that runs on the executors. It writes out one partition of the
    // RDD into one part-xxxx file per column, plus one for the vertex set.
    val writeShard = (task: spark.TaskContext, iterator: Iterator[(ID, Seq[Any])]) => {
      val collection = new IOContext.TaskFileCollection(
        trackerId, rddId, hadoop.mapreduce.TaskType.REDUCE, task.partitionId, task.attemptNumber)
      val files = paths.map(collection.createTaskFile(_))
      try {
        val verticesWriter = files.last.writer
        for (file <- files) {
          file.committer.setupTask(file.context)
          file.writer // Make sure a writer is created even if the partition is empty.
        }
        for ((id, cols) <- iterator) {
          vsCount.add(1)
          val key = new hadoop.io.LongWritable(id)
          val zipped = files.zip(serializers).zip(cols).zip(attrCounts)
          for ((((file, serializer), col), attrCount) <- zipped if col != null) {
            attrCount.add(1)
            val value = serializer.unsafeSerialize(col)
            file.writer.write(key, value)
          }
          verticesWriter.write(key, unitSerializer.serialize(()))
        }
      } finally collection.closeWriters()
      for (file <- files) file.committer.commitTask(file.context)
    }
    val collection = new IOContext.TaskFileCollection(
      trackerId, rddId, hadoop.mapreduce.TaskType.JOB_CLEANUP, 0, 0)
    val files = paths.map(collection.createTaskFile(_))
    for (file <- files) file.committer.setupJob(file.context)
    sparkContext.runJob(data, writeShard)
    for (file <- files) file.committer.commitJob(file.context)
    // Write metadata files.
    val vertexSetMeta = EntityMetadata(vsCount.value, Some(unitSerializer.name))
    vertexSetMeta.write(partitionedPath(vs).forWriting)
    for (((attr, serializer), count) <- attributes.zip(serializers).zip(attrCounts)) {
      EntityMetadata(count.value, Some(serializer.name)).write(partitionedPath(attr).forWriting)
    }
  }
}

object EntityIO {
  // These "constants" are mutable for the sake of testing.
  var verticesPerPartition =
    LoggedEnvironment.envOrElse("KITE_VERTICES_PER_PARTITION", "200000").toInt
  var tolerance =
    LoggedEnvironment.envOrElse("KITE_VERTICES_PARTITION_TOLERANCE", "2.0").toDouble

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
  def exists = operationExists && (path / Success).exists
  def mayHaveExisted = operationMayHaveExisted && path.mayHaveExisted

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
    assert(df.schema == entity.schema,
      s"Schema mismatch on read for $entity." +
        s"${df.schema.treeString} vs ${entity.schema.treeString}")
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
  def exists = operationExists && (path / Success).exists
  def mayHaveExisted = operationMayHaveExisted && path.mayHaveExisted

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

abstract class PartitionedDataIO[T, DT <: EntityRDDData[T]](entity: MetaGraphEntity,
                                                            context: IOContext)
    extends EntityIO(entity, context) {

  // This class reflects the current state of the disk during the read operation
  case class EntityLocationSnapshot(availablePartitions: Map[Int, HadoopFile]) {
    val hasPartitionedDirs = availablePartitions.nonEmpty
    val metaPathExists = metaFile.forReading.exists
    val hasPartitionedData = hasPartitionedDirs && metaPathExists

    val legacyPathExists = (legacyPath / io.Success).forReading.exists
    assert(hasPartitionedData || legacyPathExists,
      s"Legacy path ${legacyPath.forReading} does not exist," +
        s" and there seems to be no valid data in ${partitionedPath.forReading}")

    private lazy val metadata: EntityMetadata = {
      import EntityIO.fEntityMetadata
      val j = json.Json.parse(metaFile.forReading.readAsString)
      j.as[EntityMetadata]
    }

    val numVertices =
      if (hasPartitionedData) metadata.lines
      else legacyRDD.count

    def serialization = {
      assert(hasPartitionedData, s"EntitySerialization cannot be used with legacy data. ($entity)")
      metadata.serialization.getOrElse("kryo")
    }
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
      file, entityLocation.numVertices, serialization, partitioner, parent)
    assert(dataRead.rdd.partitions.size == pn,
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

  def write(data: EntityData, dir: HadoopFile): (Long, String)

  def valueTypeTag: TypeTag[T] // The TypeTag of the values we write out.

  def delete(): Boolean = {
    legacyPath.forWriting.deleteIfExists() && partitionedPath.forWriting.deleteIfExists()
  }

  def exists = operationExists && (existsPartitioned || existsAtLegacy)

  def mayHaveExisted = operationMayHaveExisted && (partitionedPath.mayHaveExisted || legacyPath.mayHaveExisted)

  private val partitionedPath = context.partitionedPath(entity)
  private val metaFile = partitionedPath / io.Metadata

  private def targetDir(numPartitions: Int) =
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
  protected def finalRead(path: HadoopFile,
                          count: Long,
                          serialization: String,
                          partitioner: spark.Partitioner,
                          parent: Option[VertexSetData] = None): DT

  protected def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, T]

  private def bestPartitionedSource(
    entityLocation: EntityLocationSnapshot,
    desiredPartitionNumber: Int) = {
    assert(entityLocation.availablePartitions.nonEmpty,
      s"There should be valid sub directories in $partitionedPath")
    val ratioSorter =
      RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desiredPartitionNumber)
    entityLocation.availablePartitions(ratioSorter.best.get)
  }

  // Returns the file and the serialization format.
  private def repartitionTo(entityLocation: EntityLocationSnapshot,
                            partitioner: spark.Partitioner): (HadoopFile, String) = {
    if (entityLocation.hasPartitionedData)
      repartitionFromPartitionedRDD(entityLocation, partitioner)
    else
      repartitionFromLegacyRDD(entityLocation, partitioner)
  }

  // Returns the file and the serialization format.
  private def repartitionFromPartitionedRDD(
    entityLocation: EntityLocationSnapshot,
    partitioner: spark.Partitioner): (HadoopFile, String) = {
    val pn = partitioner.numPartitions
    val from = bestPartitionedSource(entityLocation, pn)
    val oldRDD = from.loadEntityRawRDD(sc)
    val newRDD = oldRDD.sort(partitioner)
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRawRDD(newRDD)
    assert(entityLocation.numVertices == lines,
      s"Unexpected row count (${entityLocation.numVertices} != $lines) for $entity")
    (newFile, entityLocation.serialization)
  }

  // Returns the file and the serialization format.
  private def repartitionFromLegacyRDD(
    entityLocation: EntityLocationSnapshot,
    partitioner: spark.Partitioner): (HadoopFile, String) = {
    assert(entityLocation.legacyPathExists,
      s"There should be a valid legacy path at $legacyPath")
    val pn = partitioner.numPartitions
    val oldRDD = legacyRDD
    implicit val ct = RuntimeSafeCastable.classTagFromTypeTag(valueTypeTag)
    val newRDD = oldRDD.sort(partitioner)
    val newFile = targetDir(pn)
    val (lines, serialization) = newFile.saveEntityRDD[T](newRDD, valueTypeTag)
    assert(entityLocation.numVertices == lines,
      s"Unexpected row count (${entityLocation.numVertices} != $lines) for $entity")
    val metadata = EntityMetadata(lines, Some(serialization))
    metadata.write(partitionedPath.forWriting)
    (newFile, serialization)
  }

  private def legacyRDD = legacyLoadRDD(legacyPath.forReading)

  private def desiredPartitions(entityLocation: EntityLocationSnapshot) = {
    val vertices = entityLocation.numVertices
    EntityIO.desiredNumPartitions(vertices)
  }

  private def selectPartitionNumber(entityLocation: EntityLocationSnapshot): Int = {
    val desired = desiredPartitions(entityLocation)
    val ratioSorter = RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desired)
    ratioSorter.getBestWithinTolerance(EntityIO.tolerance).getOrElse(desired)
  }

  private def legacyPath = dataRoot / EntitiesDir / entity.gUID.toString
  private def existsAtLegacy = (legacyPath / Success).exists
  private def existsPartitioned = computeAvailablePartitions.nonEmpty && metaFile.exists
}

class VertexSetIO(entity: VertexSet, context: IOContext)
    extends PartitionedDataIO[Unit, VertexSetData](entity, context) {

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, Unit] = {
    path.loadLegacyEntityRDD[Unit](sc)
  }

  def finalRead(path: HadoopFile,
                count: Long,
                serialization: String,
                partitioner: spark.Partitioner,
                parent: Option[VertexSetData]): VertexSetData = {
    assert(parent == None, s"finalRead for $entity should not take a parent option")
    val rdd = path.loadEntityRDD[Unit](sc, serialization)
    new VertexSetData(entity, rdd.asUniqueSortedRDD(partitioner), Some(count))
  }

  def castData(data: EntityData) = data.asInstanceOf[VertexSetData]

  def write(data: EntityData, dir: HadoopFile) = dir.saveEntityRDD(castData(data).rdd, valueTypeTag)

  def valueTypeTag = typeTag[Unit]
}

class EdgeBundleIO(entity: EdgeBundle, context: IOContext)
    extends PartitionedDataIO[Edge, EdgeBundleData](entity, context) {

  override def correspondingVertexSet = Some(entity.idSet)

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, Edge] = {
    path.loadLegacyEntityRDD[Edge](sc)
  }

  def finalRead(path: HadoopFile,
                count: Long,
                serialization: String,
                partitioner: spark.Partitioner,
                parent: Option[VertexSetData]): EdgeBundleData = {
    assert(partitioner eq parent.get.rdd.partitioner.get,
      s"Partitioner mismatch for $entity.")
    val rdd = path.loadEntityRDD[Edge](sc, serialization)
    new EdgeBundleData(
      entity,
      rdd.asUniqueSortedRDD(partitioner),
      Some(count))
  }

  def castData(data: EntityData) = data.asInstanceOf[EdgeBundleData]

  def write(data: EntityData, dir: HadoopFile) = dir.saveEntityRDD(castData(data).rdd, valueTypeTag)

  def valueTypeTag = typeTag[Edge]
}

class HybridBundleIO(entity: HybridBundle, context: IOContext)
    extends PartitionedDataIO[ID, HybridBundleData](entity, context) {

  override def correspondingVertexSet = None

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, ID] = {
    throw new Exception("Unsupported")
  }

  def finalRead(path: HadoopFile,
                count: Long,
                serialization: String,
                partitioner: spark.Partitioner,
                parent: Option[VertexSetData]): HybridBundleData = {
    import scala.reflect._
    implicit val cv = classTag[ID]
    val idSerializer = EntitySerializer.forType(typeTag[ID]).name
    val longSerializer = EntitySerializer.forType(typeTag[Long]).name
    val smallKeysRDD = (path / "small_keys_rdd").loadEntityRDD[ID](sc, idSerializer)
    val largeKeysSet = (path / "larges").loadEntityRDD[Long](sc, longSerializer).collect.toSeq
    val largeKeysRDD = if (largeKeysSet.isEmpty) {
      None
    } else {
      Some((path / "large_keys_rdd").loadEntityRDD[ID](sc, idSerializer))
    }
    new HybridBundleData(
      entity,
      HybridRDD(largeKeysRDD,
        smallKeysRDD.sort(partitioner),
        largeKeysSet,
        partitioner),
      Some(count))
  }

  def write(data: EntityData, dir: HadoopFile): (Long, String) = {
    val hybridRDD = castData(data).rdd
    val linesSmallKeys = (dir / "small_keys_rdd")
      .saveEntityRDD(hybridRDD.smallKeysRDD, valueTypeTag)._1
    (dir / "larges")
      .saveEntityRDD(sc.parallelize(hybridRDD.larges, 1), typeTag[Long])
    val linesLargeKeys = if (hybridRDD.isSkewed) {
      (dir / "large_keys_rdd")
        .saveEntityRDD(hybridRDD.largeKeysRDD.get, valueTypeTag)._1
    } else { 0L }
    (dir / Success).create()
    (linesSmallKeys + linesLargeKeys, "hybrid")
  }

  def castData(data: EntityData) = data.asInstanceOf[HybridBundleData]

  def valueTypeTag = typeTag[ID]
}

class AttributeIO[T](entity: Attribute[T], context: IOContext)
    extends PartitionedDataIO[T, AttributeData[T]](entity, context) {
  override def correspondingVertexSet = Some(entity.vertexSet)

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, T] = {
    implicit val ct = entity.classTag
    path.loadLegacyEntityRDD[T](sc)
  }

  def finalRead(path: HadoopFile,
                count: Long,
                serialization: String,
                partitioner: spark.Partitioner,
                parent: Option[VertexSetData]): AttributeData[T] = {
    assert(partitioner eq parent.get.rdd.partitioner.get,
      s"Partitioner mismatch for $entity.")
    implicit val ct = entity.classTag
    implicit val tt = entity.typeTag
    val rdd = path.loadEntityRDD[T](sc, serialization)
    new AttributeData[T](
      entity,
      rdd.asUniqueSortedRDD(partitioner),
      Some(count))
  }

  def castData(data: EntityData) =
    data.asInstanceOf[AttributeData[_]].runtimeSafeCast(valueTypeTag)

  def write(data: EntityData, dir: HadoopFile) = dir.saveEntityRDD(castData(data).rdd, valueTypeTag)

  def valueTypeTag = entity.typeTag
}
