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

case class DMParam(dataRoot: DataRoot, sparkContext: spark.SparkContext)

case class EntityMetadata(lines: Long)

abstract class EntityIO(val entity: MetaGraphEntity, dmParam: DMParam) {
  def correspondingVertexSet: Option[VertexSet] = None
  val dataRoot = dmParam.dataRoot
  val sc = dmParam.sparkContext
  def legacyPath: HadoopFileLike
  def existsAtLegacy = (legacyPath / Success).exists
  def exists: Boolean
  def fastExists: Boolean // May be outdated or incorrectly true.

  private def operationPath = dataRoot / io.OperationsDir / entity.source.gUID.toString
  protected def operationFastExists = operationPath.fastExists
  protected def operationExists = operationPath.exists

  def read(parent: Option[VertexSetData] = None): EntityData
  def write(data: EntityData): Unit
  def targetRootDir: HadoopFile
  def delete() = targetRootDir.delete()
}

class ScalarIO[T](entity: Scalar[T], dMParam: DMParam)
    extends EntityIO(entity, dMParam) {

  def legacyPath = dataRoot / ScalarsDir / entity.gUID.toString
  def targetRootDir = legacyPath.forWriting
  def exists = operationExists && existsAtLegacy
  def fastExists = operationFastExists && legacyPath.fastExists
  private def serializedScalarFileName: HadoopFileLike = legacyPath / "serialized_data"
  private def successPath: HadoopFileLike = legacyPath / Success

  override def read(parent: Option[VertexSetData] = None): ScalarData[T] = {
    val scalar = entity
    log.info(s"PERF Loading scalar $scalar from disk")
    val ois = new java.io.ObjectInputStream(serializedScalarFileName.forReading.open())
    val value = try ois.readObject.asInstanceOf[T] finally ois.close()
    log.info(s"PERF Loaded scalar $scalar from disk")
    new ScalarData[T](scalar, value)
  }
  override def write(data: EntityData): Unit = {
    val scalarData = data.asInstanceOf[ScalarData[T]]
    log.info(s"PERF Writing scalar $entity to disk")
    val targetDir = legacyPath.forWriting
    targetDir.mkdirs
    val oos = new java.io.ObjectOutputStream(serializedScalarFileName.forWriting.create())
    oos.writeObject(scalarData.value)
    oos.close()
    successPath.forWriting.createFromStrings("")
    log.info(s"PERF Written scalar $entity to disk")
  }
}

abstract class PartitionableDataIO[DT <: EntityRDDData](entity: MetaGraphEntity,
                                                        dMParam: DMParam)
    extends EntityIO(entity, dMParam) {

  protected lazy val availablePartitions = collectAvailablePartitions

  val rootDir = dataRoot / PartitionedDir / entity.gUID.toString
  def targetRootDir = rootDir.forWriting
  val metaFile = rootDir / io.Metadata

  implicit val fEntityMetadata = json.Json.format[EntityMetadata]

  def targetDir(numPartitions: Int) = {
    val subdir = numPartitions.toString
    targetRootDir / subdir
  }

  def readMetadata: EntityMetadata = {
    val j = json.Json.parse(metaFile.forReading.readAsString)
    j.as[EntityMetadata]
  }

  def writeMetadata(metaData: EntityMetadata) = {
    val j = json.Json.toJson(metaData)
    metaFile.forWriting.createFromStrings(json.Json.prettyPrint(j))
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

  private def collectAvailablePartitions = {
    val availablePartitions = scala.collection.mutable.Map[Int, HadoopFile]()

    if (existsAtLegacy) {
      availablePartitions(-1) = legacyPath.forReading
    }

    val subDirs = (newPath / "*").list
    val number = "[1-9][0-9]*".r
    val subdirCandidates = subDirs.filter(x => number.pattern.matcher(x.path.getName).matches)
    for (v <- subdirCandidates) {
      val successFile = v / Success
      if (successFile.exists) {
        val numParts = v.path.getName.toInt
        availablePartitions(numParts) = v
      }
    }
    availablePartitions
  }

  protected def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): DT

  protected def loadRDD(path: HadoopFile): SortedRDD[Long, _]

  private def closestSource(desiredPartitionNumber: Int) = {
    def distanceFromDesired(a: Int) = Math.abs(a - desiredPartitionNumber)
    availablePartitions.toSeq.map {
      case (n, f) => (f, distanceFromDesired(n))
    }.sortBy(_._2).map(_._1).head
  }

  def repartitionTo(pn: Int): Unit = {
    assert(availablePartitions.nonEmpty)
    val from = closestSource(pn)
    val rawRDD = loadRDD(from)
    val newRDD = rawRDD.toSortedRDD(new HashPartitioner(pn))
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRDD(newRDD)
    writeMetadata(EntityMetadata(lines))
    availablePartitions(pn) = newFile
  }

  def selectPartitionNumber: Int = {
    val numVertices = readMetadata.lines
    val verticesPerPartition = System.getProperty("biggraph.vertices.per.partition", "1000000").toInt
    val tolerance = System.getProperty("biggraph.vertices.partition.tolerance", "2.0").toDouble
    val desired = Math.ceil(numVertices.toDouble / verticesPerPartition.toDouble)

    def fun(a: Int) = {
      val aa = a.toDouble
      if (aa > desired) aa / desired
      else desired / aa
    }

    val bestCandidate =
      availablePartitions.map { case (p, h) => (p, h, fun(p)) }.toSeq.sortBy(_._3).headOption
    bestCandidate.filter(_._3 < tolerance).map(_._1).getOrElse(desired.toInt)
  }

  override def read(parent: Option[VertexSetData] = None): DT = {
    val pn = parent.map(_.rdd.partitions.size).getOrElse(selectPartitionNumber)
    if (!availablePartitions.contains(pn)) {
      repartitionTo(pn)
    }
    finalRead(availablePartitions(pn), parent)
  }

  def legacyPath = dataRoot / EntitiesDir / entity.gUID.toString
  def newPath = dataRoot / PartitionedDir / entity.gUID.toString
  def exists = operationExists && availablePartitions.nonEmpty && metaFile.exists
  def fastExists = operationFastExists && (newPath.fastExists || legacyPath.fastExists)

  def joinedRDD[T](rawRDD: SortedRDD[Long, T], parent: VertexSetData) = {
    val vsRDD = parent.rdd
    vsRDD.cacheBackingArray()
    // This join does nothing except enforcing colocation.
    vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value }
  }

}

class VertexIO(entity: VertexSet, dMParam: DMParam)
    extends PartitionableDataIO[VertexSetData](entity, dMParam) {

  def loadRDD(path: HadoopFile): SortedRDD[Long, Unit] = {
    path.loadEntityRDD[Unit](sc)
  }

  def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): VertexSetData = {
    new VertexSetData(entity, loadRDD(path))
  }
}

class EdgeBundleIO(entity: EdgeBundle, dMParam: DMParam)
    extends PartitionableDataIO[EdgeBundleData](entity, dMParam) {

  override def correspondingVertexSet = Some(entity.idSet)
  def edgeBundle = entity
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

class AttributeIO[T](entity: Attribute[T], dMParam: DMParam)
    extends PartitionableDataIO[AttributeData[T]](entity, dMParam) {
  override def correspondingVertexSet = Some(entity.vertexSet)
  def vertexSet = entity.vertexSet
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
