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

case class WorkspaceName(name: String)
case class WorkspaceReference(
  top: String, // The name of the top-level workspace.
  customBoxStack: List[String] = List()) // The ID of the custom boxes we have "dived" into.
case class BoxOutputInfo(boxOutput: BoxOutput, stateId: String, success: FEStatus, kind: String)
case class GetWorkspaceResponse(
  name: String,
  workspace: Workspace,
  outputs: List[BoxOutputInfo],
  summaries: Map[String, String],
  canUndo: Boolean,
  canRedo: Boolean)
case class SetWorkspaceRequest(name: String, workspace: Workspace)
case class GetOperationMetaRequest(workspace: WorkspaceReference, box: String)
case class Progress(computed: Int, inProgress: Int, notYetStarted: Int, failed: Int)
case class GetProgressRequest(stateIds: List[String])
case class GetProgressResponse(progress: Map[String, Option[Progress]])
case class GetProjectOutputRequest(id: String, path: String)
case class GetTableOutputRequest(id: String, sampleRows: Int)
case class TableColumn(name: String, dataType: String)
case class GetTableOutputResponse(header: List[TableColumn], data: List[List[DynamicValue]])
case class GetPlotOutputRequest(id: String)
case class GetPlotOutputResponse(json: FEScalar)
case class GetVisualizationOutputRequest(id: String)
case class CreateWorkspaceRequest(name: String, privacy: String)
case class BoxCatalogResponse(boxes: List[BoxMetadata])
case class CreateSnapshotRequest(name: String, id: String)
case class GetExportResultRequest(stateId: String)
case class GetExportResultResponse(parameters: Map[String, String], result: FEScalar)

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
    assert(f.exists, s"Entry ${name} does not exist.")
    f.assertReadAllowedFrom(user)
    f match {
      case f: WorkspaceFrame => f
      case _ => throw new AssertionError(s"${name} is not a workspace.")
    }
  }

  case class ResolvedWorkspaceReference(user: serving.User, ref: WorkspaceReference) {
    val topWorkspace = getWorkspaceFrame(user, ref.top).workspace
    val (ws, params, name) = ref.customBoxStack.foldLeft((topWorkspace, Map[String, String](), ref.top)) {
      case ((ws, params, _), boxId) =>
        val ctx = ws.context(user, ops, params)
        val op = ctx.getOperation(boxId).asInstanceOf[CustomBoxOperation]
        val cws = op.connectedWorkspace
        (cws, op.getParams, op.context.box.operationId)
    }
    lazy val frame = getWorkspaceFrame(user, name)
    lazy val context = ws.context(user, ops, params)
  }

  def getWorkspace(
    user: serving.User, request: WorkspaceReference): GetWorkspaceResponse = {
    val res = ResolvedWorkspaceReference(user, request)
    val (stateInfo, summaries) = try {
      val context = res.context
      val states = context.allStates
      val statesWithId = states.mapValues((_, Timestamp.toString)).view.force
      calculatedStates.synchronized {
        for ((_, (boxOutputState, id)) <- statesWithId) {
          calculatedStates(id) = boxOutputState
        }
      }
      val stateInfo = statesWithId.toList.map {
        case (boxOutput, (boxOutputState, stateId)) =>
          BoxOutputInfo(boxOutput, stateId, boxOutputState.success, boxOutputState.kind)
      }
      val summaries = res.ws.boxes.map(
        box => box.id -> (
          try { context.getOperationForStates(box, states).summary }
          catch { case e: AssertionError => box.operationId }
        )
      ).toMap
      (stateInfo, summaries)
    } catch {
      case t: Throwable =>
        log.error(s"Could not execute $request", t)
        // We can still return the "cold" data that is available without execution.
        // This makes it at least possible to press Undo.
        (List[BoxOutputInfo](), Map[String, String]())
    }
    GetWorkspaceResponse(
      res.name, res.ws, stateInfo, summaries,
      canUndo = res.frame.currentState.previousCheckpoint.nonEmpty,
      canRedo = res.frame.nextCheckpoint.nonEmpty)
  }

  // This is for storing the calculated BoxOutputState objects, so the same states can be referenced later.
  val calculatedStates = new HashMap[String, BoxOutputState]()

  def getOutput(user: serving.User, stateId: String): BoxOutputState = {
    calculatedStates.synchronized {
      calculatedStates.get(stateId)
    } match {
      case None => throw new AssertionError(s"BoxOutputState state identified by $stateId not found")
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

  def getPlotOutput(
    user: serving.User, request: GetPlotOutputRequest): GetPlotOutputResponse = {
    val state = getOutput(user, request.id)
    val scalar = state.plot
    val fescalar = ProjectViewer.feScalar(scalar, "result", "", Map())
    GetPlotOutputResponse(fescalar)
  }

  def getVisualizationOutput(
    user: serving.User, request: GetVisualizationOutputRequest): VisualizationState = {
    val state = getOutput(user, request.id)
    state.visualization
  }

  def getExportResultOutput(
    user: serving.User, request: GetExportResultRequest): GetExportResultResponse = {
    val state = getOutput(user, request.stateId)
    state.kind match {
      case BoxOutputKind.ExportResult =>
        val scalar = state.exportResult
        val feScalar = ProjectViewer.feScalar(scalar, "result", "", Map())
        val parameters = (state.state.get \ "parameters").as[Map[String, String]]
        GetExportResultResponse(parameters, feScalar)
    }
  }

  def getProgress(user: serving.User, request: GetProgressRequest): GetProgressResponse = {
    val states = request.stateIds.map(stateId => stateId -> getOutput(user, stateId)).toMap
    val progress = states.map {
      case (stateId, state) =>
        if (state.success.enabled) {
          state.kind match {
            case BoxOutputKind.Project => stateId -> Some(state.project.viewer.getProgress)
            case BoxOutputKind.Table =>
              val progress = entityProgressManager.computeProgress(state.table)
              stateId -> Some(List(progress))
            case BoxOutputKind.Plot =>
              val progress = entityProgressManager.computeProgress(state.plot)
              stateId -> Some(List(progress))
            case BoxOutputKind.ExportResult =>
              val progress = entityProgressManager.computeProgress(state.exportResult)
              stateId -> Some(List(progress))
            case BoxOutputKind.Visualization =>
              stateId -> Some(List(1.0))
            case _ => throw new AssertionError(s"Unknown kind ${state.kind}")
          }
        } else {
          stateId -> None
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
    new DirectoryEntry(SymbolPath.parse(request.name)).asNewSnapshotFrame(calculatedState)
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
    user: serving.User, request: WorkspaceName): Unit = metaManager.synchronized {
    val f = getWorkspaceFrame(user, request.name)
    f.assertWriteAllowedFrom(user)
    f.undo()
  }

  def redoWorkspace(
    user: serving.User, request: WorkspaceName): Unit = metaManager.synchronized {
    val f = getWorkspaceFrame(user, request.name)
    f.assertWriteAllowedFrom(user)
    f.redo()
  }

  def boxCatalog(user: serving.User, request: serving.Empty): BoxCatalogResponse = {
    BoxCatalogResponse(ops.operationIds(user).toList.map(ops.getBoxMetadata(_)))
  }

  def getOperationMeta(user: serving.User, request: GetOperationMetaRequest): FEOperationMeta = {
    val ctx = ResolvedWorkspaceReference(user, request.workspace).context
    val op = ctx.getOperation(request.box)
    op.toFE
  }
}
