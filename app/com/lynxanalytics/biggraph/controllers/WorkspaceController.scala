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
case class SetWorkspaceRequest(reference: WorkspaceReference, workspace: Workspace)
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
case class CreateWorkspaceRequest(name: String)
case class BoxCatalogResponse(boxes: List[BoxMetadata], categories: List[FEOperationCategory])
case class CreateSnapshotRequest(name: String, id: String)
case class GetExportResultRequest(stateId: String)
case class GetExportResultResponse(parameters: Map[String, String], result: FEScalar)
case class RunWorkspaceRequest(workspace: Workspace, parameters: Map[String, String])
case class RunWorkspaceResponse(outputs: List[BoxOutputInfo], summaries: Map[String, String])

// An instrument is like a box. But we do not want to place it and save it in the workspace.
// It always has 1 input and 1 output, so the connections do not need to be expressed either.
case class Instrument(
  operationId: String,
  parameters: Map[String, String],
  parametricParameters: Map[String, String])
case class InstrumentState(
  stateId: String,
  kind: String,
  error: String)
case class GetInstrumentedStateRequest(
  workspace: WorkspaceReference,
  inputStateId: String, // State at the start of the instrument chain.
  instruments: List[Instrument]) // Instrument chain.
case class GetInstrumentedStateResponse(
  metas: List[FEOperationMeta], // Metadata for each instrument.
  states: List[InstrumentState]) // Initial state followed by the output state of each instrument.

class WorkspaceController(env: SparkFreeEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val entityProgressManager: EntityProgressManager = env.entityProgressManager

  val ops = new Operations(env)
  BuiltIns.createBuiltIns(env.metaGraphManager)

  private def assertNameNotExists(name: String) = {
    assert(!DirectoryEntry.fromName(name).exists, s"Entry '$name' already exists.")
  }

  def createWorkspace(
    user: serving.User, request: CreateWorkspaceRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val w = entry.asNewWorkspaceFrame()
    w.setupACL("public-write", user)
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
  }

  def getWorkspace(
    user: serving.User, request: WorkspaceReference): GetWorkspaceResponse = {
    val ref = ResolvedWorkspaceReference(user, request)
    val run = try runWorkspace(user, RunWorkspaceRequest(ref.ws, ref.params)) catch {
      case t: Throwable =>
        log.error(s"Could not execute $request", t)
        // We can still return the "cold" data that is available without execution.
        // This makes it at least possible to press Undo.
        RunWorkspaceResponse(List(), Map())
    }
    GetWorkspaceResponse(
      ref.name, ref.ws, run.outputs, run.summaries,
      canUndo = ref.frame.currentState.previousCheckpoint.nonEmpty,
      canRedo = ref.frame.nextCheckpoint.nonEmpty)
  }

  def runWorkspace(
    user: serving.User, request: RunWorkspaceRequest): RunWorkspaceResponse = {
    val context = request.workspace.context(user, ops, request.parameters)
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
    def crop(s: String): String = {
      val maxLength = 50
      if (s.length > maxLength) { s.substring(0, maxLength - 3) + "..." } else { s }
    }
    val summaries = request.workspace.boxes.map(
      box => box.id -> crop(
        try { context.getOperationForStates(box, states).summary }
        catch {
          case t: Throwable =>
            log.error(s"Error while generating summary for $box in $request.", t)
            box.operationId
        }
      )
    ).toMap
    RunWorkspaceResponse(stateInfo, summaries)
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
    val project =
      if (state.isProject) state.project
      else if (state.isVisualization) state.visualization.project
      else if (state.isError) throw new AssertionError(state.success.disabledReason)
      else throw new AssertionError(s"Not a project: $state")
    val viewer = project.viewer.offspringViewer(pathSeq)
    viewer.toFE(request.path)
  }

  def getPlotOutput(
    user: serving.User, request: GetPlotOutputRequest): GetPlotOutputResponse = {
    val state = getOutput(user, request.id)
    val scalar = state.plot
    val fescalar = ProjectViewer.feScalar(scalar, "result", "", Map())
    GetPlotOutputResponse(fescalar)
  }

  import UIStatusSerialization.fTwoSidedUIStatus

  def getVisualizationOutput(
    user: serving.User, request: GetVisualizationOutputRequest): TwoSidedUIStatus = {
    val state = getOutput(user, request.id)
    state.visualization.uiStatus
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
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertWriteAllowedFrom(user)
    val calculatedState = calculatedStates.synchronized {
      calculatedStates(request.id)
    }
    entry.asNewSnapshotFrame(calculatedState)
  }

  def setWorkspace(
    user: serving.User, request: SetWorkspaceRequest): GetWorkspaceResponse = metaManager.synchronized {
    val f = getWorkspaceFrame(user, ResolvedWorkspaceReference(user, request.reference).name)
    f.assertWriteAllowedFrom(user)
    val ws = request.workspace
    val repaired = ws.context(user, ops, Map()).repairedWorkspace
    val cp = repaired.checkpoint(previous = f.checkpoint)
    f.setCheckpoint(cp)
    getWorkspace(user, request.reference)
  }

  def undoWorkspace(
    user: serving.User, request: WorkspaceReference): GetWorkspaceResponse = metaManager.synchronized {
    val f = getWorkspaceFrame(user, ResolvedWorkspaceReference(user, request).name)
    f.assertWriteAllowedFrom(user)
    f.undo()
    getWorkspace(user, request)
  }

  def redoWorkspace(
    user: serving.User, request: WorkspaceReference): GetWorkspaceResponse = metaManager.synchronized {
    val f = getWorkspaceFrame(user, ResolvedWorkspaceReference(user, request).name)
    f.assertWriteAllowedFrom(user)
    f.redo()
    getWorkspace(user, request)
  }

  def boxCatalog(user: serving.User, request: serving.Empty): BoxCatalogResponse = {
    BoxCatalogResponse(
      ops.operationIds(user).toList.map(ops.getBoxMetadata(_)),
      ops.getCategories(user))
  }

  def getOperationMeta(user: serving.User, request: GetOperationMetaRequest): FEOperationMeta = {
    getOperation(user, request).toFE
  }

  def getOperationInputTables(user: serving.User, request: GetOperationMetaRequest): Map[String, ProtoTable] = {
    import Operation.Implicits._
    getOperation(user, request).getInputTables()
  }

  private def getOperation(user: serving.User, request: GetOperationMetaRequest): Operation = {
    val ref = ResolvedWorkspaceReference(user, request.workspace)
    val ctx = ref.ws.context(user, ops, ref.params)
    ctx.getOperation(request.box)
  }

  // "Safe" because if an instrument fails to execute it only returns the results up to there.
  @annotation.tailrec
  private def safeInstrumentStatesAndMetas(
    ctx: WorkspaceExecutionContext,
    instruments: List[Instrument],
    states: List[BoxOutputState],
    opMetas: List[FEOperationMeta]): (List[BoxOutputState], List[FEOperationMeta]) = {
    val next = if (instruments.isEmpty) {
      None
    } else try {
      val instr = instruments.head
      val state = states.last
      val meta = ops.getBoxMetadata(instr.operationId)
      assert(
        meta.inputs.size == 1,
        s"${instr.operationId} has ${meta.inputs.size} inputs instead of 1.")
      assert(
        meta.outputs.size == 1,
        s"${instr.operationId} has ${meta.outputs.size} outputs instead of 1.")
      val box = Box(
        id = "",
        operationId = instr.operationId,
        parameters = instr.parameters,
        x = 0,
        y = 0,
        // It does not matter where the inputs come from. Using "null" for BoxOutput.
        inputs = Map(meta.inputs.head -> null),
        parametricParameters = instr.parametricParameters)
      val op = box.getOperation(ctx, Map(meta.inputs.head -> state))
      val newState = box.orErrors(meta) { op.getOutputs }(box.output(meta.outputs.head))
      Some((newState, op.toFE))
    } catch {
      case t: Throwable =>
        log.error(s"Could not execute instrument ${instruments.head}.", t)
        None
    }
    next match {
      case Some((newState, newMeta)) =>
        safeInstrumentStatesAndMetas(ctx, instruments.tail, states :+ newState, opMetas :+ newMeta)
      case None => (states, opMetas)
    }
  }

  def getInstrumentedState(
    user: serving.User, request: GetInstrumentedStateRequest): GetInstrumentedStateResponse = {
    val ref = ResolvedWorkspaceReference(user, request.workspace)
    val ctx = ref.ws.context(user, ops, ref.params)
    val inputState = getOutput(user, request.inputStateId)
    var (states, opMetas) = safeInstrumentStatesAndMetas(
      ctx, request.instruments, List(inputState), List[FEOperationMeta]())
    val instrumentStates = calculatedStates.synchronized {
      states.map { state =>
        val id = Timestamp.toString
        calculatedStates(id) = state
        InstrumentState(id, state.kind, state.success.disabledReason)
      }
    }
    GetInstrumentedStateResponse(opMetas, instrumentStates)
  }
}
