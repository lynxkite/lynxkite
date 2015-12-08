// Classes for reading and writing EntityData to storage.

package com.lynxanalytics.biggraph.graph_api.io

import org.apache.hadoop
import org.apache.spark
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import play.api.libs.json

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.{ HadoopFile, Timestamp }

object IOContext {
  // Encompasses the Hadoop OutputFormat, Writer, and Committer in one object.
  private class TaskFile(
      tracker: String, stage: Int, task: Int, attempt: Int, file: HadoopFile,
      collection: TaskFileCollection) {
    import hadoop.mapreduce.lib.output.SequenceFileOutputFormat
    val fmt = new SequenceFileOutputFormat[hadoop.io.LongWritable, hadoop.io.BytesWritable]()
    val context = {
      val config = new hadoop.mapred.JobConf(file.hadoopConfiguration)
      config.set(
        hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR,
        file.resolvedNameWithNoCredentials)
      config.setOutputKeyClass(classOf[hadoop.io.LongWritable])
      config.setOutputValueClass(classOf[hadoop.io.BytesWritable])
      spark.EntityIOHelper.createTaskAttemptContext(config, tracker, stage, task, attempt)
    }
    val committer = fmt.getOutputCommitter(context)
    lazy val writer = {
      val w = fmt.getRecordWriter(context)
      collection.registerForClosing(this)
      w
    }
  }

  private class TaskFileCollection(tracker: String, stage: Int, task: Int, attempt: Int) {
    val toClose = new collection.mutable.ListBuffer[TaskFile]()
    def createTaskFile(path: HadoopFile) = new TaskFile(tracker, stage, task, attempt, path, this)
    def registerForClosing(file: TaskFile) = {
      toClose += file
    }
    // Call this if you accessed any RecordWriters.
    def close() = for (file <- toClose) file.writer.close(file.context)
  }
}

case class IOContext(dataRoot: DataRoot, sparkContext: spark.SparkContext) {
  def partitionedPath(entity: MetaGraphEntity): HadoopFileLike =
    dataRoot / io.PartitionedDir / entity.gUID.toString

  def partitionedPath(entity: MetaGraphEntity, numPartitions: Int): HadoopFileLike =
    partitionedPath(entity) / numPartitions.toString

  // Writes multiple attributes and their vertex set to disk. The attributes are given in a
  // single RDD which will be iterated over only once.
  def writeAttributes(attributes: Seq[Attribute[String]], data: AttributeRDD[Seq[String]]) = {
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

    val trackerID = Timestamp.toString
    val rddID = data.id
    val count = sparkContext.accumulator[Long](0L, "Line count")
    val writeShard = (task: spark.TaskContext, iterator: Iterator[(ID, Seq[String])]) => {
      val collection = new IOContext.TaskFileCollection(
        trackerID, rddID, task.partitionId, task.attemptNumber)
      try {
        val files = paths.map(collection.createTaskFile(_))
        val verticesWriter = files.last.writer
        val unit = new hadoop.io.BytesWritable(RDDUtils.kryoSerialize(()))
        for (file <- files) file.committer.setupTask(file.context)
        for ((id, cols) <- iterator) {
          count += 1
          val key = new hadoop.io.LongWritable(id)
          for ((file, col) <- (files zip cols) if col != null) {
            val value = new hadoop.io.BytesWritable(RDDUtils.kryoSerialize(col))
            file.writer.write(key, value)
          }
          verticesWriter.write(key, unit)
        }
        for (file <- files) file.committer.commitTask(file.context)
      } finally collection.close()
    }
    val collection = new IOContext.TaskFileCollection(trackerID, rddID, 0, 0)
    try {
      val files = paths.map(collection.createTaskFile(_))
      for (file <- files) file.committer.setupJob(file.context)
      sparkContext.runJob(data, writeShard)
      for (file <- files) file.committer.commitJob(file.context)
      // Write metadata files.
      val meta = EntityMetadata(count.value)
      for (e <- outputEntities) meta.write(partitionedPath(e).forWriting)
    } finally collection.close()
  }
}

object EntityIO {
  // These "constants" are mutable for the sake of testing.
  var verticesPerPartition =
    scala.util.Properties.envOrElse(
      "KITE_VERTICES_PER_PARTITION",
      System.getProperty("biggraph.vertices.per.partition", "200000")).toInt
  var tolerance =
    System.getProperty("biggraph.vertices.partition.tolerance", "2.0").toDouble

  implicit val fEntityMetadata = json.Json.format[EntityMetadata]
  def operationPath(dataRoot: DataRoot, instance: MetaGraphOperationInstance) =
    dataRoot / io.OperationsDir / instance.gUID.toString
}

case class EntityMetadata(lines: Long) {
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
    val ois = new java.io.ObjectInputStream(serializedScalarFileName.forReading.open())
    val value = try ois.readObject.asInstanceOf[T] finally ois.close()
    log.info(s"PERF Loaded scalar $entity from disk")
    new ScalarData[T](entity, value)
  }

  def write(data: EntityData): Unit = {
    val scalarData = data.asInstanceOf[ScalarData[T]]
    log.info(s"PERF Writing scalar $entity to disk")
    val targetDir = path.forWriting
    targetDir.mkdirs
    val oos = new java.io.ObjectOutputStream(serializedScalarFileName.forWriting.create())
    try oos.writeObject(scalarData.value) finally oos.close()
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

case class RatioSorter(elements: Seq[Int], desired: Int) {
  assert(desired > 0, "RatioSorter only supports positive integers")
  assert(elements.filter(_ <= 0).isEmpty, "RatioSorter only supports positive integers")
  private val sorted: Seq[(Int, Double)] = {
    elements.map { a =>
      val aa = a.toDouble
      if (aa > desired) (a, aa / desired)
      else (a, desired.toDouble / aa)
    }
      .sortBy(_._2)
  }

  val best: Option[Int] = sorted.map(_._1).headOption

  def getBestWithinTolerance(tolerance: Double): Option[Int] = {
    sorted.filter(_._2 < tolerance).map(_._1).headOption
  }

}

abstract class PartitionedDataIO[DT <: EntityRDDData](entity: MetaGraphEntity,
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

    private def readMetadata: EntityMetadata = {
      import EntityIO.fEntityMetadata
      val j = json.Json.parse(metaFile.forReading.readAsString)
      j.as[EntityMetadata]
    }

    val numVertices =
      if (hasPartitionedData) readMetadata.lines
      else legacyRDD.count
  }

  def read(parent: Option[VertexSetData] = None): DT = {
    val entityLocation = EntityLocationSnapshot(computeAvailablePartitions)
    val pn = parent.map(_.rdd.partitions.size).getOrElse(selectPartitionNumber(entityLocation))
    val partitioner = parent.map(_.rdd.partitioner.get).getOrElse(new HashPartitioner(pn))

    val file =
      if (entityLocation.availablePartitions.contains(pn))
        entityLocation.availablePartitions(pn)
      else
        repartitionTo(entityLocation, partitioner)

    val dataRead = finalRead(file, entityLocation.numVertices, partitioner, parent)
    assert(dataRead.rdd.partitions.size == pn, s"finalRead mismatch: ${dataRead.rdd.partitions.size} != $pn")
    dataRead
  }

  def write(data: EntityData): Unit = {
    val rddData = data.asInstanceOf[EntityRDDData]
    log.info(s"PERF Instantiating entity $entity on disk")
    val rdd = rddData.rdd
    val partitions = rdd.partitions.size
    val lines = targetDir(partitions).saveEntityRDD(rdd)
    val metadata = EntityMetadata(lines)
    metadata.write(partitionedPath.forWriting)
    log.info(s"PERF Instantiated entity $entity on disk")
  }

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
                          partitioner: org.apache.spark.Partitioner,
                          parent: Option[VertexSetData] = None): DT

  protected def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, _]

  private def bestPartitionedSource(entityLocation: EntityLocationSnapshot, desiredPartitionNumber: Int) = {
    assert(entityLocation.availablePartitions.nonEmpty,
      s"there should be valid sub directories in $partitionedPath")
    val ratioSorter = RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desiredPartitionNumber)
    entityLocation.availablePartitions(ratioSorter.best.get)
  }

  private def repartitionTo(entityLocation: EntityLocationSnapshot,
                            partitioner: org.apache.spark.Partitioner): HadoopFile = {
    if (entityLocation.hasPartitionedData)
      repartitionFromPartitionedRDD(entityLocation, partitioner)
    else
      repartitionFromLegacyRDD(entityLocation, partitioner)
  }

  private def repartitionFromPartitionedRDD(entityLocation: EntityLocationSnapshot,
                                            partitioner: org.apache.spark.Partitioner): HadoopFile = {
    val pn = partitioner.numPartitions
    val from = bestPartitionedSource(entityLocation, pn)
    val oldRDD = from.loadEntityRawRDD(sc)
    val newRDD = oldRDD.sort(partitioner)
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRawRDD(newRDD)
    assert(entityLocation.numVertices == lines, s"${entityLocation.numVertices} != $lines")
    newFile
  }

  private def repartitionFromLegacyRDD(entityLocation: EntityLocationSnapshot,
                                       partitioner: org.apache.spark.Partitioner): HadoopFile = {
    assert(entityLocation.legacyPathExists,
      s"There should be a valid legacy path at $legacyPath")
    val pn = partitioner.numPartitions
    val oldRDD = legacyRDD
    val newRDD = oldRDD.sort(partitioner)
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRDD(newRDD)
    assert(entityLocation.numVertices == lines, s"${entityLocation.numVertices} != $lines")
    EntityMetadata(lines).write(partitionedPath.forWriting)
    newFile
  }

  private def legacyRDD = legacyLoadRDD(legacyPath.forReading)

  private def desiredPartitions(entityLocation: EntityLocationSnapshot) = {
    val vertices = entityLocation.numVertices
    val p = Math.ceil(vertices.toDouble / EntityIO.verticesPerPartition).toInt
    // Always have at least 1 partition.
    p max 1
  }

  private def selectPartitionNumber(entityLocation: EntityLocationSnapshot): Int = {
    val desired = desiredPartitions(entityLocation)
    val ratioSorter = RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desired)
    ratioSorter.getBestWithinTolerance(EntityIO.tolerance).getOrElse(desired)
  }

  private def legacyPath = dataRoot / EntitiesDir / entity.gUID.toString
  private def existsAtLegacy = (legacyPath / Success).exists
  private def existsPartitioned = computeAvailablePartitions.nonEmpty && metaFile.exists

  protected def enforceCoLocationWithParent[T](rawRDD: RDD[(Long, T)],
                                               parent: VertexSetData): RDD[(Long, T)] = {
    val vsRDD = parent.rdd
    vsRDD.cacheBackingArray()
    // Enforcing colocation:
    assert(vsRDD.partitions.size == rawRDD.partitions.size,
      s"$vsRDD and $rawRDD should have the same number of partitions, " +
        s"but ${vsRDD.partitions.size} != ${rawRDD.partitions.size}")
    vsRDD.zipPartitions(rawRDD, preservesPartitioning = true) {
      (it1, it2) => it2
    }
  }
}

class VertexSetIO(entity: VertexSet, context: IOContext)
    extends PartitionedDataIO[VertexSetData](entity, context) {

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, Unit] = {
    path.loadLegacyEntityRDD[Unit](sc)
  }

  def finalRead(path: HadoopFile,
                count: Long,
                partitioner: org.apache.spark.Partitioner,
                parent: Option[VertexSetData]): VertexSetData = {
    assert(parent == None, s"finalRead for $entity should not take a parent option")
    val rdd = path.loadEntityRDD[Unit](sc)
    new VertexSetData(entity, rdd.asUniqueSortedRDD(partitioner), Some(count))
  }
}

class EdgeBundleIO(entity: EdgeBundle, context: IOContext)
    extends PartitionedDataIO[EdgeBundleData](entity, context) {

  override def correspondingVertexSet = Some(entity.idSet)

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, Edge] = {
    path.loadLegacyEntityRDD[Edge](sc)
  }

  def finalRead(path: HadoopFile,
                count: Long,
                partitioner: org.apache.spark.Partitioner,
                parent: Option[VertexSetData]): EdgeBundleData = {
    assert(partitioner eq parent.get.rdd.partitioner.get)
    val rdd = path.loadEntityRDD[Edge](sc)
    val coLocated = enforceCoLocationWithParent(rdd, parent.get)
    new EdgeBundleData(
      entity,
      coLocated.asUniqueSortedRDD(partitioner),
      Some(count))
  }
}

class AttributeIO[T](entity: Attribute[T], context: IOContext)
    extends PartitionedDataIO[AttributeData[T]](entity, context) {
  override def correspondingVertexSet = Some(entity.vertexSet)

  def legacyLoadRDD(path: HadoopFile): SortedRDD[Long, T] = {
    implicit val ct = entity.classTag
    path.loadLegacyEntityRDD[T](sc)
  }

  def finalRead(path: HadoopFile,
                count: Long,
                partitioner: org.apache.spark.Partitioner,
                parent: Option[VertexSetData]): AttributeData[T] = {
    assert(partitioner eq parent.get.rdd.partitioner.get)
    implicit val ct = entity.classTag
    val rdd = path.loadEntityRDD[T](sc)
    val coLocated = enforceCoLocationWithParent(rdd, parent.get)
    new AttributeData[T](
      entity,
      coLocated.asUniqueSortedRDD(partitioner),
      Some(count))
  }
}
