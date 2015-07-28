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
  id: String = "",
  name: String = "",
  desc: String = "",
  fileNumber: Long,
  totalSize: Long)

case class DataFilesStatus(
  total: DataFilesStats,
  methods: List[DataFilesStats])

case class Method(
  id: String,
  name: String,
  desc: String,
  filesToKeep: () => Set[String])

case class MarkDeletedRequest(method: String)

case class AllFiles(
  entities: Map[String, Long],
  operations: Map[String, Long],
  scalars: Map[String, Long])

class CleanerController(environment: BigGraphEnvironment) {
  private val methods = List(
    Method(
      "notExistsInMetaGraph",
      "Files which do not exist in the MetaGraph",
      """Truly orphan files. These are created e.g. when the kite meta directory
      is deleted. Deleting these should not have any side effects.""",
      existInMetaGraph))

  def getDataFilesStatus(user: serving.User, req: serving.Empty): DataFilesStatus = {
    assert(user.isAdmin, "Only administrator users can use the cleaner.")
    log.info(s"${user.email} requesting data files status.")
    val files = getAllFiles()
    val allFiles = files.entities ++ files.operations ++ files.scalars

    DataFilesStatus(
      DataFilesStats(
        fileNumber = allFiles.size,
        totalSize = allFiles.map(_._2).sum),
      methods.map { m =>
        getDataFilesStats(m.id, m.name, m.desc, m.filesToKeep(), files)
      })
  }

  private def getAllFiles(): AllFiles = {
    AllFiles(
      getAllFilesInDir(DataManager.entityDir),
      getAllFilesInDir(DataManager.operationDir),
      getAllFilesInDir(DataManager.scalarDir))
  }

  // Return all files and dirs and their respecitve sizes in bytes in a
  // certain directory - not recursively, except for those files or dirs
  // marked as deleted.
  private def getAllFilesInDir(dir: String): Map[String, Long] = {
    val hadoopFileDir = environment.dataManager.repositoryPath / dir
    hadoopFileDir.listStatus.filterNot {
      subDir => subDir.getPath().toString contains DataManager.deletedSfx
    }.map { subDir =>
      val baseName = subDir.getPath().getName()
      baseName -> (hadoopFileDir / baseName).getContentSummary.getSpaceConsumed
    }.toMap
  }

  private def existInMetaGraph(): Set[String] = {
    allEntities(environment.metaGraphManager.getEntities().values)
  }

  // Returns the set of ID strings of all the entities and scalars created by
  // the parent operations of the baseEntities, plus the ID strings of the
  // operations themselves.
  private def allEntities(baseEntities: Iterable[MetaGraphEntity]): Set[String] = {
    val filesToKeep = new HashSet[String]
    for (baseEntity <- baseEntities) {
      val operation = baseEntity.source
      filesToKeep += operation.gUID.toString
      filesToKeep ++= operation.outputs.all.values.map { e => e.gUID.toString }
    }
    filesToKeep.toSet
  }

  private def getDataFilesStats(
    id: String,
    name: String,
    desc: String,
    filesToKeep: Set[String],
    files: AllFiles): DataFilesStats = {
    val allFiles = (files.entities ++ files.operations ++ files.scalars) -- filesToKeep
    DataFilesStats(id, name, desc, allFiles.size, allFiles.map(_._2).sum)
  }

  def markFilesDeleted(user: serving.User, req: MarkDeletedRequest): Unit = synchronized {
    assert(user.isAdmin, "Only administrators can delete orphan files.")
    assert(methods.map { m => m.id } contains req.method,
      s"Unkown orphan file deletion method: ${req.method}")
    log.info(s"${user.email} attempting to mark orphan files deleted using '${req.method}'.")
    val files = getAllFiles()
    val filesToKeep = methods.find(m => m.id == req.method).get.filesToKeep()
    markDeleted(DataManager.entityDir, files.entities.keys.toSet -- filesToKeep)
    markDeleted(DataManager.operationDir, files.operations.keys.toSet -- filesToKeep)
    markDeleted(DataManager.scalarDir, files.scalars.keys.toSet -- filesToKeep)
  }

  private def markDeleted(dir: String, files: Set[String]): Unit = {
    val hadoopFileDir = environment.dataManager.repositoryPath / dir
    for (file <- files) {
      (hadoopFileDir / file).renameTo(hadoopFileDir / (file + DataManager.deletedSfx))
    }
    log.info(s"${files.size} files marked deleted in ${hadoopFileDir.path}.")
  }
}
