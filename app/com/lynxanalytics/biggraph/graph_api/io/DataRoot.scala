// The DataRoot is a directory where the DataManager stores data files.

package com.lynxanalytics.biggraph.graph_api.io

import java.util.UUID
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_util.HadoopFile

trait DataRootLike {
  def instancePath(i: MetaGraphOperationInstance): HadoopFile
  def entityPath(e: MetaGraphEntity): HadoopFile
  def fastHasInstance(i: MetaGraphOperationInstance): Boolean // May be incorrectly true.
  def fastHasEntity(e: MetaGraphEntity): Boolean // May be incorrectly true.
  def hasInstance(i: MetaGraphOperationInstance): Boolean
  def hasEntity(e: MetaGraphEntity): Boolean
}

class DataRoot(repositoryPath: HadoopFile) extends DataRootLike {
  def instancePath(instance: MetaGraphOperationInstance) =
    repositoryPath / OperationsDir / instance.gUID.toString

  def entityPath(entity: MetaGraphEntity) = {
    if (entity.isInstanceOf[Scalar[_]]) {
      repositoryPath / ScalarsDir / entity.gUID.toString
    } else {
      repositoryPath / EntitiesDir / entity.gUID.toString
    }
  }

  // Things saved during previous runs. Checking for the _SUCCESS files is slow so we use the
  // list of directories instead. The results are thus somewhat optimistic.
  private def contents(dir: String): Set[UUID] = {
    (repositoryPath / dir / "*").list
      .map(_.path.getName)
      .filterNot(_.endsWith(DeletedSfx))
      .map(_.asUUID).toSet
  }
  private val possiblySavedInstances: Set[UUID] = contents(OperationsDir)
  private val possiblySavedEntities: Set[UUID] = contents(ScalarsDir) ++ contents(EntitiesDir)

  def fastHasInstance(i: MetaGraphOperationInstance) =
    possiblySavedInstances.contains(i.gUID)
  def fastHasEntity(e: MetaGraphEntity) =
    possiblySavedEntities.contains(e.gUID)
  def hasInstance(i: MetaGraphOperationInstance) =
    fastHasInstance(i) && (instancePath(i) / Success).exists
  def hasEntity(e: MetaGraphEntity) =
    fastHasEntity(e) && (entityPath(e) / Success).exists
}

class CombinedRoot(a: DataRoot, b: DataRoot) extends DataRootLike {
  def instancePath(i: MetaGraphOperationInstance) =
    if (a.hasInstance(i) || !b.hasInstance(i)) a.instancePath(i) else b.instancePath(i)
  def entityPath(e: MetaGraphEntity) =
    if (a.hasEntity(e) || !b.hasEntity(e)) a.entityPath(e) else b.entityPath(e)
  def fastHasInstance(i: MetaGraphOperationInstance) = a.fastHasInstance(i) || b.fastHasInstance(i)
  def fastHasEntity(e: MetaGraphEntity) = a.fastHasEntity(e) || b.fastHasEntity(e)
  def hasInstance(i: MetaGraphOperationInstance) = a.hasInstance(i) || b.hasInstance(i)
  def hasEntity(e: MetaGraphEntity) = a.hasEntity(e) || b.hasEntity(e)
}

