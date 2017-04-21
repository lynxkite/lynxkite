// Methods for manipulating workspaces.
package com.lynxanalytics.biggraph.controllers

import scala.collection.mutable.HashMap
import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.frontend_operations.Operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving

case class GetWorkspaceRequest(name: String)
case class BoxOutputInfo(boxOutput: BoxOutput, stateID: String, success: FEStatus, kind: String)
case class GetWorkspaceResponse(workspace: Workspace, outputs: List[BoxOutputInfo], summaries: Map[String, String])
case class SetWorkspaceRequest(name: String, workspace: Workspace)
case class GetOperationMetaRequest(workspace: String, box: String)
case class Progress(computed: Int, inProgress: Int, notYetStarted: Int, failed: Int)
case class GetProgressRequest(stateIDs: List[String])
case class GetProgressResponse(progress: Map[String, Option[Progress]])
case class GetProjectOutputRequest(id: String, path: String)
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

  private def getWorkspaceByName(
    user: serving.User, name: String): Workspace = metaManager.synchronized {
    val f = DirectoryEntry.fromName(name)
    assert(f.exists, s"Project ${name} does not exist.")
    f.assertReadAllowedFrom(user)
    f match {
      case f: WorkspaceFrame => f.workspace
      case _ => throw new AssertionError(s"${name} is not a workspace.")
    }
  }

  def getWorkspace(
    user: serving.User, request: GetWorkspaceRequest): GetWorkspaceResponse = {
    val workspace = getWorkspaceByName(user, request.name)
    val states = workspace.context(user, ops, Map()).allStates
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
    val boxInfo = stateInfo.groupBy(_.boxOutput.boxID).map {
      case (id, infos) => (id, infos.map(_.success.enabled).foldLeft(true) { (a, b) => a && b })
    }
    val summaries = workspace.boxes.map(
      box => box.id -> (
        if (boxInfo.getOrElse(box.id, false)) ops.opForBox(user, box, Map(), Map()).summary
        else box.operationID
      )
    ).toMap
    GetWorkspaceResponse(workspace, stateInfo, summaries)
  }

  // This is for storing the calculated BoxOutputState objects, so the same states can be referenced later.
  val calculatedStates = new HashMap[String, BoxOutputState]()

  private def getOutput(user: serving.User, stateID: String): BoxOutputState = {
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
    state.kind match {
      case BoxOutputKind.Project =>
        val pathSeq = SubProject.splitPipedPath(request.path).filter(_ != "")
        val viewer = state.project.viewer.offspringViewer(pathSeq)
        viewer.toFE(request.path)
    }
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
    val f = DirectoryEntry.fromName(request.name)
    assert(f.exists, s"Project ${request.name} does not exist.")
    f.assertWriteAllowedFrom(user)
    f match {
      case f: WorkspaceFrame =>
        val cp = request.workspace.checkpoint(previous = f.checkpoint)
        f.setCheckpoint(cp)
      case _ => throw new AssertionError(s"${request.name} is not a workspace.")
    }
  }

  def boxCatalog(user: serving.User, request: serving.Empty): BoxCatalogResponse = {
    BoxCatalogResponse(ops.operationIds.toList.map(ops.getBoxMetadata(_)))
  }

  def getOperationMeta(user: serving.User, request: GetOperationMetaRequest): FEOperationMeta = {
    val ws = getWorkspaceByName(user, request.workspace)
    val op = ws.context(user, ops, Map()).getOperation(request.box)
    op.toFE
  }
}
