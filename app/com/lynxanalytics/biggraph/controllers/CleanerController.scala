// Utilities to mark and delete unused data files.

package com.lynxanalytics.biggraph.controllers

import java.util.UUID

import scala.collection.immutable.Set
import scala.collection.immutable.Map
import scala.collection.mutable.HashSet

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class DataFilesStats(
  fileNumber: Long,
  totalSize: Long)

case class DataFilesStatus(
  total: DataFilesStats,
  existsInMetaGraph: DataFilesStats,
  referredFromProject: DataFilesStats,
  transitivelyReferredFromProject: DataFilesStats)

case class MarkDeletedRequest(method: String)

case class DataToKeep(operations: Set[String], entities: Set[String], scalars: Set[String])

case class AllFiles(
  entities: Map[String, Long],
  operations: Map[String, Long],
  scalars: Map[String, Long])

class CleanerController(environment: BigGraphEnvironment) {
  def getCleaner(user: serving.User, req: serving.Empty): DataFilesStatus = {
    assert(user.isAdmin, "Only administrator users can use the cleaner.")
    val files = getAllFiles()
    val allFiles = files.entities ++ files.operations ++ files.scalars

    DataFilesStatus(
      DataFilesStats(allFiles.size, allFiles.map(_._2).sum),
      getDataFilesStats(
        existInMetaGraph(),
        files),
      DataFilesStats(0, 0),
      DataFilesStats(0, 0))
  }

  private def getAllFiles(): AllFiles = {
    AllFiles(
      getAllFilesInDir("entities"),
      getAllFilesInDir("operations"),
      getAllFilesInDir(DataManager.scalarDir))
  }

  private def getAllFilesInDir(dir: String): Map[String, Long] = {
    val hadoopFileDir = environment.dataManager.repositoryPath / dir
    hadoopFileDir.listStatus.filter {
      subDir => !(subDir.getPath().toString contains ".deleted")
    }.map { subDir =>
      {
        val baseName = subDir.getPath().getName()
        baseName -> (hadoopFileDir / baseName).getContentSummary.getSpaceConsumed()
      }
    }.toMap
  }

  private def existInMetaGraph(): DataToKeep = {
    allEntities(environment.metaGraphManager.getEntities().values)
  }

  private def allEntities(baseEntities: Iterable[MetaGraphEntity]): DataToKeep = {
    val operations = new HashSet[String]
    val scalars = new HashSet[String]
    val entities = new HashSet[String]
    for (baseEntity <- baseEntities) {
      val operation = baseEntity.source
      operations += operation.gUID.toString
      entities ++= operation.outputs.all.values.map { e => e.gUID.toString }
      scalars ++= operation.outputs.scalars.values.map { e => e.gUID.toString }
    }
    DataToKeep(operations.toSet, entities.toSet, scalars.toSet)
  }

  private def getDataFilesStats(
    dataToKeep: DataToKeep,
    files: AllFiles): DataFilesStats = {
    val entityFiles = files.entities -- dataToKeep.entities
    val operationFiles = files.operations -- dataToKeep.operations
    val scalarFiles = files.scalars -- dataToKeep.scalars
    val allFiles = entityFiles ++ operationFiles ++ scalarFiles
    DataFilesStats(allFiles.size, allFiles.map(_._2).sum)
  }

  def markFilesDeleted(user: serving.User, req: MarkDeletedRequest): Unit = synchronized {
    assert(user.isAdmin, "Only administrators can delete orphan files.")
    log.info(s"Attempting to mark unused files deleted using '${req.method}'.")
    val files = getAllFiles()
    val dataToKeep = req.method match {
      case "existInMetaGraph" => existInMetaGraph()
    }
    markDeleted(DataManager.entityDir, files.entities.keys.toSet -- dataToKeep.entities)
    markDeleted(DataManager.operationDir, files.operations.keys.toSet -- dataToKeep.operations)
    markDeleted(DataManager.scalarDir, files.scalars.keys.toSet -- dataToKeep.scalars)
  }

  private def markDeleted(dir: String, files: Set[String]): Unit = {
    val hadoopFileDir = environment.dataManager.repositoryPath / dir
    for (file <- files) {
      (hadoopFileDir / file).renameTo(hadoopFileDir / (file + ".deleted"))
    }
    log.info(s"${files.size} files marked deleted in ${hadoopFileDir.path}.")
  }
}
