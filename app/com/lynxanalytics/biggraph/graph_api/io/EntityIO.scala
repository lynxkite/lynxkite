// Classes for reading and writing EntityData to storage.

package com.lynxanalytics.biggraph.graph_api.io

import java.util.UUID
import com.lynxanalytics.biggraph.spark_util.SortedRDD
import org.apache.spark
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SQLContext
import play.api.libs.json
import scala.collection.concurrent.TrieMap
import scala.concurrent._
import com.lynxanalytics.biggraph.spark_util.Implicits._

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile

case class DMParam(dataRoot: DataRootLike, sparkContext: spark.SparkContext)

case class EntityMetadata(lines: Int)

abstract class EntityIOWrapper(val entity: MetaGraphEntity, dmParam: DMParam) {
  val dataRoot = dmParam.dataRoot
  val sc = dmParam.sparkContext
  val newDirectoryName = "new_entities"
  def legacyPath: HadoopFileLike
  def existsAtLegacy = (legacyPath / "_SUCCESS").exists
  def exists: Boolean
  def fastExists: Boolean // May be outdated or incorrectly true.

  def read(parent: Option[VertexSetData] = None): EntityData
  def write(data: EntityData): Unit

  override def toString = s"exists: $exists legacy path: $legacyPath"
}

class ScalarIOWrapper[T](entity: Scalar[T], dMParam: DMParam)
    extends EntityIOWrapper(entity, dMParam) {

  def legacyPath = dataRoot / "scalars" / entity.gUID.toString
  def exists = existsAtLegacy
  def fastExists = legacyPath.fastExists
  private def serializedScalarFileName: HadoopFileLike = legacyPath / "serialized_data"
  private def successPath: HadoopFileLike = legacyPath / "_SUCCESS"

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

abstract class PartitionableDataIOWrapper[DT <: EntityRDDData](entity: MetaGraphEntity, dMParam: DMParam)
    extends EntityIOWrapper(entity, dMParam) {

  protected lazy val availablePartitions = collectAvailablePartitions

  override def toString = s"exists: $exists  legacy path: $legacyPath  available: $availablePartitions  targetdir: $targetRootDir"

  val rootDir = dataRoot / newDirectoryName / entity.gUID.toString
  val targetRootDir = rootDir.forWriting
  val metaFile = rootDir / "metadata"

  implicit val fEntityMetadata = json.Json.format[EntityMetadata]

  def targetDir(numPartitions: Int) = {
    val subdir = numPartitions.toString
    targetRootDir / subdir
  }

  def readMetadata: EntityMetadata = {
    // TODO: What happens if the file is not there?
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
    val number = "[123456789][0-9]*".r
    val subdirCandidates = subDirs.filter(x => number.pattern.matcher(x.path.getName).matches)
    val a = subdirCandidates.map(_.path.getName).toSet
    val b = subDirs.map(_.path.getName).toSet
    for (v <- subdirCandidates) {
      val successFile = v / "_SUCCESS"
      if (successFile.exists) {
        val numParts = v.path.getName.toInt
        availablePartitions(numParts) = v
      }
    }
    availablePartitions
  }

  def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): DT

  def repartitionTo(pn: Int): Unit = {
    assert(availablePartitions.nonEmpty)
    val from = availablePartitions.head._2
    val rawRDD = from.loadEntityRDD[Int](sc)
    val newRDD = rawRDD.toSortedRDD(new HashPartitioner(pn))
    val newFile = targetDir(pn)
    val lines = newFile.saveEntityRDD(newRDD)
    writeMetadata(EntityMetadata(lines))
    availablePartitions(pn) = newFile
  }

  def selectPartitionNumber(parent: Option[VertexSetData] = None): Int = {
    parent.get.rdd.partitions.size
  }

  override def read(parent: Option[VertexSetData] = None): DT = {
    val pn = selectPartitionNumber(parent)
    if (!availablePartitions.contains(pn)) {
      repartitionTo(pn)
    }
    finalRead(availablePartitions(pn), parent)
  }

  def legacyPath = dataRoot / "entities" / entity.gUID.toString
  def newPath = dataRoot / newDirectoryName / entity.gUID.toString
  def exists: Boolean = availablePartitions.nonEmpty
  def fastExists = legacyPath.fastExists || newPath.fastExists

}

class VertexIOWrapper(entity: VertexSet, dMParam: DMParam)
    extends PartitionableDataIOWrapper[VertexSetData](entity, dMParam) {

  override def selectPartitionNumber(parent: Option[VertexSetData] = None): Int = {
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

  def finalRead(path: HadoopFile, parent: Option[VertexSetData] = None): VertexSetData = {
    val fn = path
    val rdd = fn.loadEntityRDD[Unit](sc)
    new VertexSetData(entity, rdd)
  }
}

class EdgeBundleIOWrapper(entity: EdgeBundle, dMParam: DMParam)
    extends PartitionableDataIOWrapper[EdgeBundleData](entity, dMParam) {

  def edgeBundle = entity
  def finalRead(path: HadoopFile, parent: Option[VertexSetData]): EdgeBundleData = {
    // We do our best to colocate partitions to corresponding vertex set partitions.
    val idsRDD = parent.get.rdd
    idsRDD.cacheBackingArray()
    val rawRDD = path.loadEntityRDD[Edge](sc, idsRDD.partitioner)
    new EdgeBundleData(
      edgeBundle,
      idsRDD.sortedJoin(rawRDD).mapValues { case (_, edge) => edge })
  }
}

class AttributeIOWrapper[T](entity: Attribute[T], dMParam: DMParam)
    extends PartitionableDataIOWrapper[AttributeData[T]](entity, dMParam) {
  def vertexSet = entity.vertexSet
  def finalRead(path: HadoopFile, parent: Option[VertexSetData]): AttributeData[T] = {
    // We do our best to colocate partitions to corresponding vertex set partitions.
    val vsRDD = parent.get.rdd
    vsRDD.cacheBackingArray()
    implicit val ct = entity.classTag
    val rawRDD = path.loadEntityRDD[T](sc, vsRDD.partitioner)
    new AttributeData[T](
      entity,
      // This join does nothing except enforcing colocation.
      vsRDD.sortedJoin(rawRDD).mapValues { case (_, value) => value })
  }
}
