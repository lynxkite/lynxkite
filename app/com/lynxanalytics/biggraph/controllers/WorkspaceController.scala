// Methods for manipulating workspaces.
package com.lynxanalytics.biggraph.controllers

import scala.collection.mutable.HashMap
import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.frontend_operations.Operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class GetWorkspaceRequest(name: String)
case class BoxOutputInfo(boxOutput: BoxOutput, stateID: String, success: FEStatus, kind: String)
case class GetWorkspaceResponse(
  workspace: Workspace,
  outputs: List[BoxOutputInfo],
  summaries: Map[String, String],
  canUndo: Boolean,
  canRedo: Boolean)
case class SetWorkspaceRequest(name: String, workspace: Workspace)
case class GetOperationMetaRequest(workspace: String, box: String)
case class Progress(computed: Int, inProgress: Int, notYetStarted: Int, failed: Int)
case class GetProgressRequest(stateIDs: List[String])
case class GetProgressResponse(progress: Map[String, Option[Progress]])
case class GetProjectOutputRequest(id: String, path: String)
case class GetTableOutputRequest(id: String, sampleRows: Int)
case class TableColumn(name: String, dataType: String)
case class GetTableOutputResponse(header: List[TableColumn], data: List[List[DynamicValue]])
case class CreateWorkspaceRequest(name: String, privacy: String)
case class BoxCatalogResponse(boxes: List[BoxMetadata])
case class CreateSnapshotRequest(name: String, id: String)

class WorkspaceController(env: SparkFreeEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val entityProgressManager: EntityProgressManager = env.entityProgressManager

  val ops = new Operations(env)

  private def assertNameNotExists(name: String) = {
    assert(!DirectoryEntry.fromName(name).exists, s"Entry '$name' already exists.")
  }

  def createWorkspace(
    user: serving.User, request: CreateWorkspaceRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val w = entry.asNewWorkspaceFrame()
    w.setupACL(request.privacy, user)
  }

  private def getWorkspaceFrame(
    user: serving.User, name: String): WorkspaceFrame = metaManager.synchronized {
    val f = DirectoryEntry.fromName(name)
    assert(f.exists, s"Project ${name} does not exist.")
    f.assertReadAllowedFrom(user)
    f match {
      case f: WorkspaceFrame => f
      case _ => throw new AssertionError(s"${name} is not a workspace.")
    }
  }

  def getWorkspace(
    user: serving.User, request: GetWorkspaceRequest): GetWorkspaceResponse = {
    val frame = getWorkspaceFrame(user, request.name)
    val workspace = frame.workspace
    try {
      val context = workspace.context(user, ops, Map())
      val states = context.allStates
      val statesWithId = states.mapValues((_, Timestamp.toString)).view.force
      calculatedStates.synchronized {
        for ((_, (boxOutputState, id)) <- statesWithId) {
          calculatedStates(id) = boxOutputState
        }
      }
      val stateInfo = statesWithId.toList.map {
        case (boxOutput, (boxOutputState, stateID)) =>
          BoxOutputInfo(boxOutput, stateID, boxOutputState.success, boxOutputState.kind)
      }
      val summaries = workspace.boxes.map(
        box => box.id -> (
          try { context.getOperationForStates(box, states).summary }
          catch { case e: AssertionError => box.operationID }
        )
      ).toMap
      GetWorkspaceResponse(
        workspace, stateInfo, summaries,
        canUndo = frame.currentState.previousCheckpoint.nonEmpty,
        canRedo = frame.nextCheckpoint.nonEmpty)
    } catch {
      case t: Throwable =>
        log.error(s"Could not execute ${request.name}", t)
        // We can still return the "cold" data that is available without execution.
        // This makes it at least possible to press Undo.
        GetWorkspaceResponse(
          workspace, List(), Map(),
          canUndo = frame.currentState.previousCheckpoint.nonEmpty,
          canRedo = frame.nextCheckpoint.nonEmpty)
    }
  }

  // This is for storing the calculated BoxOutputState objects, so the same states can be referenced later.
  val calculatedStates = new HashMap[String, BoxOutputState]()

  def getOutput(user: serving.User, stateID: String): BoxOutputState = {
    calculatedStates.synchronized {
      calculatedStates.get(stateID)
    } match {
      case None => throw new AssertionError(s"BoxOutputState state identified by $stateID not found")
      case Some(state: BoxOutputState) => state
    }
  }

  def getProjectOutput(
    user: serving.User, request: GetProjectOutputRequest): FEProject = {
    val state = getOutput(user, request.id)
    val pathSeq = SubProject.splitPipedPath(request.path).filter(_ != "")
    val viewer = state.project.viewer.offspringViewer(pathSeq)
    viewer.toFE(request.path)
  }

  def getProgress(user: serving.User, request: GetProgressRequest): GetProgressResponse = {
    val states = request.stateIDs.map(stateID => stateID -> getOutput(user, stateID)).toMap
    val progress = states.map {
      case (stateID, state) =>
        if (state.success.enabled) {
          state.kind match {
            case BoxOutputKind.Project => stateID -> Some(state.project.viewer.getProgress)
            case BoxOutputKind.Table =>
              val progress = entityProgressManager.computeProgress(state.table)
              stateID -> Some(List(progress))
            case _ => throw new AssertionError(s"Unknown kind ${state.kind}")
          }
        } else {
          stateID -> None
        }
    }.mapValues(option => option.map(
      progressList => Progress(
        computed = progressList.count(_ == 1.0),
        inProgress = progressList.count(x => x < 1.0 && x > 0.0),
        notYetStarted = progressList.count(_ == 0.0),
        failed = progressList.count(_ < 0.0)
      ))
    ).view.force
    GetProgressResponse(progress)
  }

  def createSnapshot(
    user: serving.User, request: CreateSnapshotRequest): Unit = {
    val calculatedState = calculatedStates.synchronized {
      calculatedStates(request.id)
    }
    val snapshot = new SnapshotFrame(SymbolPath.parse(request.name))
    snapshot.initialize(calculatedState)
  }

  def setWorkspace(
    user: serving.User, request: SetWorkspaceRequest): Unit = metaManager.synchronized {
    val f = getWorkspaceFrame(user, request.name)
    f.assertWriteAllowedFrom(user)
    val ws = request.workspace
    val repaired = ws.context(user, ops, Map()).repairedWorkspace
    val cp = repaired.checkpoint(previous = f.checkpoint)
    f.setCheckpoint(cp)
  }

  def undoWorkspace(
    user: serving.User, request: GetWorkspaceRequest): Unit = metaManager.synchronized {
    val f = getWorkspaceFrame(user, request.name)
    f.assertWriteAllowedFrom(user)
    f.undo()
  }

  def redoWorkspace(
    user: serving.User, request: GetWorkspaceRequest): Unit = metaManager.synchronized {
    val f = getWorkspaceFrame(user, request.name)
    f.assertWriteAllowedFrom(user)
    f.redo()
  }

  def boxCatalog(user: serving.User, request: serving.Empty): BoxCatalogResponse = {
    BoxCatalogResponse(ops.operationIds(user).toList.map(ops.getBoxMetadata(_)))
  }

  def getOperationMeta(user: serving.User, request: GetOperationMetaRequest): FEOperationMeta = {
    val ws = getWorkspaceFrame(user, request.workspace).workspace
    val op = ws.context(user, ops, Map()).getOperation(request.box)
    op.toFE
  }
}
