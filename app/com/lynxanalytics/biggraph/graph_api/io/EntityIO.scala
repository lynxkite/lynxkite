// Classes for reading and writing EntityData to storage.

package com.lynxanalytics.biggraph.graph_api.io

import org.apache.spark
import org.apache.spark.HashPartitioner
import play.api.libs.json

import com.lynxanalytics.biggraph.spark_util.Implicits._
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile

case class IOContext(dataRoot: DataRoot, sparkContext: spark.SparkContext)

case class EntityMetadata(lines: Long)

object EntityIO {
  // These "constants" are mutable for the sake of testing.
  var verticesPerPartition =
    scala.util.Properties.envOrElse(
      "KITE_VERTICES_PER_PARTITION",
      System.getProperty("biggraph.vertices.per.partition", "1000000")).toInt
  var tolerance =
    System.getProperty("biggraph.vertices.partition.tolerance", "2.0").toDouble

  implicit val fEntityMetadata = json.Json.format[EntityMetadata]
  def operationPath(dataRoot: DataRoot, instance: MetaGraphOperationInstance) =
    dataRoot / io.OperationsDir / instance.gUID.toString
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

class ScalarIO[T](entity: Scalar[T], dMParam: IOContext)
    extends EntityIO(entity, dMParam) {

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
                                                      dMParam: IOContext)
    extends EntityIO(entity, dMParam) {

  // This class reflects the current state of the disk during the read operation
  case class EntityLocationSnapshot(availablePartitions: Map[Int, HadoopFile]) {
    val hasPartitionedDirs = availablePartitions.nonEmpty
    val metaPathExists = metaFile.forReading.exists
    val hasPartitionedData = hasPartitionedDirs && metaPathExists

    val legacyPathExists = (legacyPath / io.Success).forReading.exists
    assert(hasPartitionedData || legacyPathExists,
      s"Legacy path $legacyPath does not exist, and there seems to be no valid data in $partitionedPath")

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
    val file =
      if (entityLocation.availablePartitions.contains(pn)) entityLocation.availablePartitions(pn)
      else repartitionTo(entityLocation, pn)

    val dataRead = finalRead(file, parent)
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
    writeMetadata(metadata)
    log.info(s"PERF Instantiated entity $entity on disk")
  }

  def delete(): Boolean = {
    legacyPath.forWriting.deleteIfExists() && partitionedPath.forWriting.deleteIfExists()
  }

  def exists = operationExists && (existsPartitioned || existsAtLegacy)

  def mayHaveExisted = operationMayHaveExisted && (partitionedPath.mayHaveExisted || legacyPath.mayHaveExisted)

  private val partitionedPath = dataRoot / PartitionedDir / entity.gUID.toString
  private val metaFile = partitionedPath / io.Metadata
  private val metaFileCreated = partitionedPath / io.MetadataCreate

  private def targetDir(numPartitions: Int) = {
    val subdir = numPartitions.toString
    partitionedPath.forWriting / subdir
  }

  private def writeMetadata(metaData: EntityMetadata) = {
    import EntityIO.fEntityMetadata
    assert(!metaFile.forWriting.exists, s"Metafile $metaFile should not exist before we write it.")
    metaFileCreated.forWriting.deleteIfExists()
    val j = json.Json.toJson(metaData)
    metaFileCreated.forWriting.createFromStrings(json.Json.prettyPrint(j))
    metaFileCreated.forWriting.renameTo(metaFile.forWriting)
  }

  private def computeAvailablePartitions = {
    val subDirs = (partitionedPath / "*").list
    val number = "[1-9][0-9]*".r
    val numericSubdirs = subDirs.filter(x => number.pattern.matcher(x.path.getName).matches)
    val existingCandidates = numericSubdirs.filter(x => (x / Success).exists)
    val resultList = existingCandidates.map { x => (x.path.getName.toInt, x) }
    resultList.toMap
  }

  // This method performs the actual reading of the rdddata, from a path/
  // The parent VertexSetData is given for EdgeBundleData and AttributeData[T] so that
  // the corresponding data will be co-located.
  protected def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): DT

  protected def loadRDD(path: HadoopFile): SortedRDD[Long, _]

  private def bestPartitionedSource(entityLocation: EntityLocationSnapshot, desiredPartitionNumber: Int) = {
    assert(entityLocation.availablePartitions.nonEmpty,
      s"there should be valid sub directories in $partitionedPath")
    val ratioSorter = RatioSorter(entityLocation.availablePartitions.map(_._1).toSeq, desiredPartitionNumber)
    entityLocation.availablePartitions(ratioSorter.best.get)
  }

  private def repartitionTo(entityLocation: EntityLocationSnapshot, pn: Int): HadoopFile = {
    val rawRDD =
      if (entityLocation.hasPartitionedData) {
        val from = bestPartitionedSource(entityLocation, pn)
        loadRDD(from)
      } else {
        assert(entityLocation.legacyPathExists,
          s"There should be a valid legacy path at $legacyPath")
        legacyRDD
      }
    val newRDD = rawRDD.toSortedRDD(new HashPartitioner(pn))
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRDD(newRDD)
    assert(entityLocation.numVertices == lines, s"${entityLocation.numVertices} != $lines")
    if (!entityLocation.hasPartitionedData)
      writeMetadata(EntityMetadata(lines))
    newFile
  }

  private def legacyRDD = loadRDD(legacyPath.forReading)

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

  protected def joinedRDD[T](rawRDD: SortedRDD[Long, T], parent: VertexSetData) = {
    val vsRDD = parent.rdd
    vsRDD.cacheBackingArray()
    // This join does nothing except enforcing colocation.
    vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value }
  }

}

class VertexIO(entity: VertexSet, dMParam: IOContext)
    extends PartitionedDataIO[VertexSetData](entity, dMParam) {

  def loadRDD(path: HadoopFile): SortedRDD[Long, Unit] = {
    path.loadEntityRDD[Unit](sc)
  }

  def finalRead(path: HadoopFile, parent: Option[VertexSetData]): VertexSetData = {
    assert(parent == None, s"finalRead for $entity should not take a parent option")
    new VertexSetData(entity, loadRDD(path))
  }
}

class EdgeBundleIO(entity: EdgeBundle, dMParam: IOContext)
    extends PartitionedDataIO[EdgeBundleData](entity, dMParam) {

  override def correspondingVertexSet = Some(entity.idSet)
  def loadRDD(path: HadoopFile): SortedRDD[Long, Edge] = {
    path.loadEntityRDD[Edge](sc)
  }
  def finalRead(path: HadoopFile, parent: Option[VertexSetData]): EdgeBundleData = {
    // We do our best to colocate partitions to corresponding vertex set partitions.
    new EdgeBundleData(
      entity,
      joinedRDD(loadRDD(path), parent.get))
  }
}

class AttributeIO[T](entity: Attribute[T], dMParam: IOContext)
    extends PartitionedDataIO[AttributeData[T]](entity, dMParam) {
  override def correspondingVertexSet = Some(entity.vertexSet)

  def loadRDD(path: HadoopFile): SortedRDD[Long, T] = {
    implicit val ct = entity.classTag
    path.loadEntityRDD[T](sc)
  }
  def finalRead(path: HadoopFile, parent: Option[VertexSetData]): AttributeData[T] = {
    // We do our best to colocate partitions to corresponding vertex set partitions.
    new AttributeData[T](
      entity,
      joinedRDD(loadRDD(path), parent.get))
  }
}
