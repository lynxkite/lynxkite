// Utilities to mark and delete unused data files.

package com.lynxanalytics.biggraph.controllers

import java.util.UUID

import scala.collection.immutable.Set
import scala.collection.immutable.Map
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class DataFilesStats(
  id: String = "",
  name: String = "",
  desc: String = "",
  fileCount: Long,
  totalSize: Long)

case class DataFilesStatus(
  freeSpace: Long,
  total: DataFilesStats,
  trash: DataFilesStats,
  methods: List[DataFilesStats])

case class CleanerMethod(
  id: String,
  name: String,
  desc: String,
  filesToKeep: () => Set[String])

case class MoveToTrashRequest(method: String)

case class AllFiles(
    partitioned: Map[String, Long],
    entities: Map[String, Long],
    operations: Map[String, Long],
    scalars: Map[String, Long]) {

  lazy val all = partitioned ++ entities ++ operations ++ scalars
}

class CleanerController(metaGraphManager: MetaGraphManager, dataManager: DataManager) {
  implicit val manager = metaGraphManager

  private val methods = List(
    CleanerMethod(
      "notMetaGraphContents",
      "Entities which do not exist in the meta-graph",
      "Truly orphan entities. Cached entities can get orphaned e.g. when the kite meta directory" +
        " is deleted or during a Kite version upgrade. Deleting these should not have any side" +
        " effects.",
      metaGraphContents),
    CleanerMethod(
      "notReferredFromProjectTransitively",
      "Entities not associated with any project",
      "We consider an entity associated with a project if it's either directly referred to from" +
        " the project or it is used as an input to calculate another associated entity",
      transitivelyReferredFromProject),
    CleanerMethod(
      "notReferredFromProject",
      "Entities not referenced by any project",
      "All entities except those that are vertex sets, edge bundles, attributes or scalars of" +
        " a current project or segmentation",
      referredFromProject))

  def getDataFilesStatus(user: serving.User, req: serving.Empty): DataFilesStatus = {
    assert(user.isAdmin, "Only administrator users can use the cleaner.")
    val files = getAllFiles(trash = false)
    val trashFiles = getAllFiles(trash = true)

    DataFilesStatus(
      HadoopFile.defaultFs.getStatus().getRemaining(),
      DataFilesStats(
        fileCount = files.all.size,
        totalSize = files.all.map(_._2).sum),
      DataFilesStats(
        fileCount = trashFiles.all.size,
        totalSize = trashFiles.all.map(_._2).sum),
      methods.map { m =>
        getDataFilesStats(m.id, m.name, m.desc, m.filesToKeep(), files)
      })
  }

  private def getAllFiles(trash: Boolean): AllFiles = {
    AllFiles(
      getAllFilesInDir(io.PartitionedDir, trash),
      getAllFilesInDir(io.EntitiesDir, trash),
      getAllFilesInDir(io.OperationsDir, trash),
      getAllFilesInDir(io.ScalarsDir, trash))
  }

  // Return all files and dirs and their respective sizes in bytes in a
  // certain directory. Directories in trash are included iff the trash param is true.
  private def getAllFilesInDir(dir: String, trash: Boolean): Map[String, Long] = {
    val hadoopFileDir = dataManager.writablePath / dir
    if (!hadoopFileDir.exists) {
      Map[String, Long]()
    } else {
      hadoopFileDir.listStatus.filter {
        subDir => (subDir.getPath().toString contains io.DeletedSfx) == trash
      }.map { subDir =>
        val baseName = subDir.getPath().getName()
        baseName -> (hadoopFileDir / baseName).getContentSummary.getSpaceConsumed
      }.toMap
    }
  }

  private def metaGraphContents(): Set[String] = {
    allFilesFromSourceOperation(metaGraphManager.getOperationInstances())
  }

  private def referredFromProject(): Set[String] = {
    val operations = operationsFromAllProjects()
    allFilesFromSourceOperation(operations)
  }

  private def transitivelyReferredFromProject(): Set[String] = {
    var operations = operationsFromAllProjects()
    val toExpand = new Queue[UUID] ++ operations.keys
    val ops = manager.getOperationInstances()
    while (!toExpand.isEmpty) {
      val op = ops(toExpand.dequeue)
      for (input <- op.inputs.all.values) {
        val dependentOp = input.source
        val dependentID = dependentOp.gUID
        if (!(operations isDefinedAt dependentID)) {
          operations += (dependentID -> dependentOp)
          toExpand.enqueue(dependentID)
        }
      }
    }
    allFilesFromSourceOperation(operations.toMap)
  }

  private def operationsFromAllProjects()(
    implicit manager: MetaGraphManager): Map[UUID, MetaGraphOperationInstance] = {
    val operations = new HashMap[UUID, MetaGraphOperationInstance]
    for (project <- Operation.allObjects(serving.User.fake)) {
      operations ++= operationsFromProject(project.viewer)
    }
    operations.toMap
  }

  // Returns the operations mapped by their ID strings which created
  // the vertices, edges, attributes and scalars of this project.
  private def operationsFromProject(
    project: ProjectViewer): Map[UUID, MetaGraphOperationInstance] = {
    val operations = new HashMap[UUID, MetaGraphOperationInstance]
    if (project.vertexSet != null) {
      operations += operationWithID(project.vertexSet.source)
    }
    if (project.edgeBundle != null) {
      operations += operationWithID(project.edgeBundle.source)
    }
    operations ++= project.scalars.map {
      case (_, s) => operationWithID(s.source)
    }
    operations ++= project.vertexAttributes.map {
      case (_, a) => operationWithID(a.source)
    }
    operations ++= project.edgeAttributes.map {
      case (_, a) => operationWithID(a.source)
    }
    for (segmentation <- project.segmentationMap.values) {
      operations ++= operationsFromProject(segmentation)
    }
    operations.toMap
  }

  private def operationWithID(
    operation: MetaGraphOperationInstance): (UUID, MetaGraphOperationInstance) = {
    (operation.gUID, operation)
  }

  // Returns the set of ID strings of all the entities and scalars created by
  // the operations, plus the ID strings of the operations themselves.
  // Note that these ID strings are the base names of the corresponding
  // data directories.
  private def allFilesFromSourceOperation(
    operations: Map[UUID, MetaGraphOperationInstance]): Set[String] = {
    val files = new HashSet[String]
    for ((id, operation) <- operations) {
      files += id.toString
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
    val filesToDelete = allFiles.all -- filesToKeep
    DataFilesStats(id, name, desc, filesToDelete.size, filesToDelete.map(_._2).sum)
  }

  def moveAllToCleanerTrash(user: serving.User): Unit = {
    for (method <- methods) {
      val req = MoveToTrashRequest(method.id)
      moveToCleanerTrash(user, req)
    }
  }

  def moveToCleanerTrash(user: serving.User, req: MoveToTrashRequest): Unit = synchronized {
    assert(user.isAdmin, "Only administrators can move data files to trash.")
    assert(methods.map { m => m.id } contains req.method,
      s"Unknown data file trashing method: ${req.method}")
    log.info(s"${user.email} attempting to move data files to trash using '${req.method}'.")
    val files = getAllFiles(trash = false)
    val filesToKeep = methods.find(m => m.id == req.method).get.filesToKeep()
    moveToTrash(io.PartitionedDir, files.partitioned.keys.toSet -- filesToKeep)
    moveToTrash(io.EntitiesDir, files.entities.keys.toSet -- filesToKeep)
    moveToTrash(io.OperationsDir, files.operations.keys.toSet -- filesToKeep)
    moveToTrash(io.ScalarsDir, files.scalars.keys.toSet -- filesToKeep)
  }

  private def moveToTrash(dir: String, files: Set[String]): Unit = {
    val hadoopFileDir = dataManager.writablePath / dir
    if (hadoopFileDir.exists()) {
      for (file <- files) {
        (hadoopFileDir / file).renameTo(hadoopFileDir / (file + io.DeletedSfx))
      }
      log.info(s"${files.size} files moved to trash in ${hadoopFileDir.path}.")
    }
  }

  def emptyCleanerTrash(user: serving.User, req: serving.Empty): Unit = synchronized {
    assert(user.isAdmin, "Only administrators can delete trash files.")
    log.info(s"${user.email} attempting to delete trash files.")
    deleteTrashFilesInDir(io.PartitionedDir)
    deleteTrashFilesInDir(io.EntitiesDir)
    deleteTrashFilesInDir(io.OperationsDir)
    deleteTrashFilesInDir(io.ScalarsDir)
  }

  private def deleteTrashFilesInDir(dir: String): Unit = {
    val hadoopFileDir = dataManager.writablePath / dir
    if (hadoopFileDir.exists()) {
      hadoopFileDir.listStatus.filter {
        subDir => subDir.getPath().toString contains io.DeletedSfx
      }.map { subDir =>
        (hadoopFileDir / subDir.getPath().getName()).delete()
      }
      log.info(s"Emptied the cleaner trash in ${hadoopFileDir.path}.")
    }
  }
}
