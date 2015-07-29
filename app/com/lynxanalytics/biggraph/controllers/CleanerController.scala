// Utilities to mark and delete unused data files.

package com.lynxanalytics.biggraph.controllers

import java.util.UUID

import scala.collection.immutable.Set
import scala.collection.immutable.Map
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class DataFilesStats(
  id: String = "",
  name: String = "",
  desc: String = "",
  fileCount: Long,
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
      "notMetaGraphContents",
      "Files which do not exist in the MetaGraph",
      """Truly orphan files. These are created e.g. when the kite meta directory
      is deleted. Deleting these should not have any side effects.""",
      metaGraphContents))

  def getDataFilesStatus(user: serving.User, req: serving.Empty): DataFilesStatus = {
    assert(user.isAdmin, "Only administrator users can use the cleaner.")
    val files = getAllFiles()
    val allFiles = files.entities ++ files.operations ++ files.scalars

    DataFilesStatus(
      DataFilesStats(
        fileCount = allFiles.size,
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

  // Return all files and dirs and their respective sizes in bytes in a
  // certain directory. Directories marked as deleted are not included.
  private def getAllFilesInDir(dir: String): Map[String, Long] = {
    val hadoopFileDir = environment.dataManager.repositoryPath / dir
    hadoopFileDir.listStatus.filterNot {
      subDir => subDir.getPath().toString contains DataManager.deletedSfx
    }.map { subDir =>
      val baseName = subDir.getPath().getName()
      baseName -> (hadoopFileDir / baseName).getContentSummary.getSpaceConsumed
    }.toMap
  }

  private def metaGraphContents(): Set[String] = {
    allFilesFromSourceOperation(environment.metaGraphManager.getEntities().values)
  }

  // Returns the set of ID strings of all the entities and scalars created by
  // the parent operations of the baseEntities, plus the ID strings of the
  // operations themselves. Note that these ID strings are the base names of
  // the corresponding data directories.
  private def allFilesFromSourceOperation(
    baseEntities: Iterable[MetaGraphEntity]): Set[String] = {
    val files = new HashSet[String]
    val operations = new HashMap[String, MetaGraphOperationInstance]
    for (baseEntity <- baseEntities) {
      val operation = baseEntity.source
      operations += (operation.gUID.toString -> operation)
    }
    for (operation <- operations.values) {
      files += operation.gUID.toString
      files ++= operation.outputs.all.values.map { e => e.gUID.toString }
    }
    files.toSet
  }

  private def getDataFilesStats(
    id: String,
    name: String,
    desc: String,
    filesToKeep: Set[String],
    allFiles: AllFiles): DataFilesStats = {
    val filesToDelete = (allFiles.entities ++ allFiles.operations ++ allFiles.scalars) -- filesToKeep
    DataFilesStats(id, name, desc, filesToDelete.size, filesToDelete.map(_._2).sum)
  }

  def markFilesDeleted(user: serving.User, req: MarkDeletedRequest): Unit = synchronized {
    assert(user.isAdmin, "Only administrators can mark files deleted.")
    assert(methods.map { m => m.id } contains req.method,
      s"Unkown orphan file marking method: ${req.method}")
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

  def deleteMarkedFiles(user: serving.User, req: serving.Empty): Unit = synchronized {
    assert(user.isAdmin, "Only administrators can delete marked files.")
    log.info(s"${user.email} attempting to delete marked files.")
    deleteMarkedFilesInDir(DataManager.entityDir)
    deleteMarkedFilesInDir(DataManager.operationDir)
    deleteMarkedFilesInDir(DataManager.scalarDir)
  }

  private def deleteMarkedFilesInDir(dir: String): Unit = {
    val hadoopFileDir = environment.dataManager.repositoryPath / dir
    hadoopFileDir.listStatus.filter {
      subDir => subDir.getPath().toString contains DataManager.deletedSfx
    }.map { subDir =>
      (hadoopFileDir / subDir.getPath().getName()).delete()
    }
  }
}
