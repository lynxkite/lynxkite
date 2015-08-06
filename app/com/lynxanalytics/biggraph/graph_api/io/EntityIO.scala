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

abstract class EntityIO(val entity: MetaGraphEntity, dmParam: IOContext) {
  def correspondingVertexSet: Option[VertexSet] = None
  def read(parent: Option[VertexSetData] = None): EntityData
  def write(data: EntityData): Unit
  def delete(): Boolean
  def exists: Boolean
  def mayHaveExisted: Boolean // May be outdated or incorrectly true.

  protected val dataRoot = dmParam.dataRoot
  protected val sc = dmParam.sparkContext
  protected def operationMayHaveExisted = operationPath.mayHaveExisted
  protected def operationExists = (operationPath / io.Success).exists

  private def operationPath = dataRoot / io.OperationsDir / entity.source.gUID.toString
}

class ScalarIO[T](entity: Scalar[T], dMParam: IOContext)
    extends EntityIO(entity, dMParam) {

  def read(parent: Option[VertexSetData]): ScalarData[T] = {
    assert(parent == None)
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
    oos.writeObject(scalarData.value)
    oos.close()
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

case class RatioSorter[T](elements: Map[Int, T], desired: Int) {
  assert(desired != 0)
  val sorted = {
    def fun(a: Int) = {
      val aa = a.toDouble
      if (aa > desired) aa / desired
      else desired / aa
    }
    elements.map { case (p, h) => (p, h, fun(p)) }
      .toSeq.sortBy(_._3)
  }

  def getBest: Option[Int] = {
    sorted.map(_._1).headOption
  }
  def getBestWithinTolerance(tolerance: Double): Option[Int] = {
    sorted.filter(_._3 < tolerance).map(_._1).headOption
  }

}

abstract class PartitionableDataIO[DT <: EntityRDDData](entity: MetaGraphEntity,
                                                        dMParam: IOContext)
    extends EntityIO(entity, dMParam) {

  // This class reflects the current state of the disk during the read operation
  case class EntityLocation(availablePartitions: Map[Int, HadoopFile],
                            metaPath: HadoopFileLike,
                            legacyPath: HadoopFileLike) {
    val hasPartitionedDirs = availablePartitions.nonEmpty
    val metaPathExists = metaPath.forReading.exists
    val hasPartitionedData = hasPartitionedDirs && metaPathExists

    val legacyPathExists = (legacyPath / io.Success).forReading.exists
    assert(hasPartitionedData || legacyPathExists)
  }

  def read(parent: Option[VertexSetData] = None): DT = {

    val entityLocation = EntityLocation(computeAvailablePartitions, metaFile, legacyPath)
    val pn = parent.map(_.rdd.partitions.size).getOrElse(selectPartitionNumber(entityLocation))
    val file =
      if (entityLocation.availablePartitions.contains(pn)) entityLocation.availablePartitions(pn)
      else repartitionTo(entityLocation, pn)

    finalRead(file, parent)
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

  private implicit val fEntityMetadata = json.Json.format[EntityMetadata]

  private def targetDir(numPartitions: Int) = {
    val subdir = numPartitions.toString
    partitionedPath.forWriting / subdir
  }

  private def readMetadata: EntityMetadata = {
    val j = json.Json.parse(metaFile.forReading.readAsString)
    j.as[EntityMetadata]
  }

  private def writeMetadata(metaData: EntityMetadata) = {
    def doWrite = {
      val j = json.Json.toJson(metaData)
      metaFile.forWriting.createFromStrings(json.Json.prettyPrint(j))
    }
    if (metaFile.forWriting.exists) {
      val oldMetaData = readMetadata
      if (oldMetaData != metaData) doWrite
    } else doWrite
  }

  private def computeAvailablePartitions = {
    val subDirs = (partitionedPath / "*").list
    val number = "[1-9][0-9]*".r
    val numericSubdirs = subDirs.filter(x => number.pattern.matcher(x.path.getName).matches)
    val existingCandidates = numericSubdirs.filter(x => (x / Success).exists)
    val resultList = existingCandidates.map { x => (x.path.getName.toInt, x) }
    resultList.toMap
  }

  protected def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): DT

  protected def loadRDD(path: HadoopFile): SortedRDD[Long, _]

  private def bestPartitionedSource(entityLocation: EntityLocation, desiredPartitionNumber: Int) = {
    assert(entityLocation.availablePartitions.nonEmpty)
    val ratioSorter = RatioSorter(entityLocation.availablePartitions, desiredPartitionNumber)
    entityLocation.availablePartitions(ratioSorter.getBest.get)
  }

  private def repartitionTo(entityLocation: EntityLocation, pn: Int): HadoopFile = {
    val rawRDD =
      if (entityLocation.hasPartitionedData) {
        val from = bestPartitionedSource(entityLocation, pn)
        loadRDD(from)
      } else {
        assert(entityLocation.legacyPathExists)
        legacyRDD
      }
    val newRDD = rawRDD.toSortedRDD(new HashPartitioner(pn))
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRDD(newRDD)
    writeMetadata(EntityMetadata(lines))
    newFile
  }

  private def legacyRDD = loadRDD(legacyPath.forReading)

  private def numVertices(entityLocation: EntityLocation) =
    if (entityLocation.hasPartitionedData) readMetadata.lines
    else {
      assert(existsAtLegacy) // I dare not take this out yet
      legacyRDD.count
    }
  private def desiredPartitions(entityLocation: EntityLocation) = {
    val v = numVertices(entityLocation)
    val vertices = if (v > 0) v else 1
    Math.ceil(vertices.toDouble / System.getProperty("biggraph.vertices.per.partition", "1000000").toInt).toInt
  }

  private def selectPartitionNumber(entityLocation: EntityLocation): Int = {
    val desired = desiredPartitions(entityLocation)
    val ratioSorter = RatioSorter(entityLocation.availablePartitions, desired)
    val tolerance = System.getProperty("biggraph.vertices.partition.tolerance", "2.0").toDouble
    ratioSorter.getBestWithinTolerance(tolerance).getOrElse(desired)
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
    extends PartitionableDataIO[VertexSetData](entity, dMParam) {

  def loadRDD(path: HadoopFile): SortedRDD[Long, Unit] = {
    path.loadEntityRDD[Unit](sc)
  }

  def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): VertexSetData = {
    new VertexSetData(entity, loadRDD(path))
  }
}

class EdgeBundleIO(entity: EdgeBundle, dMParam: IOContext)
    extends PartitionableDataIO[EdgeBundleData](entity, dMParam) {

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
    extends PartitionableDataIO[AttributeData[T]](entity, dMParam) {
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
