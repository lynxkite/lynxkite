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
import MetaGraphManager.StringAsUUID

case class WorkspaceReference(
    top: String, // The name of the top-level workspace.
    customBoxStack: List[String] = List()) // The ID of the custom boxes we have "dived" into.
case class BoxOutputInfo(boxOutput: BoxOutput, stateId: String, success: FEStatus, kind: String)
case class GetWorkspaceResponse(
    name: String,
    workspace: Workspace,
    outputs: List[BoxOutputInfo],
    summaries: Map[String, String],
    progress: Map[String, List[Double]],
    canUndo: Boolean,
    canRedo: Boolean)
case class SetWorkspaceRequest(reference: WorkspaceReference, workspace: Workspace)
case class GetOperationMetaRequest(workspace: WorkspaceReference, box: String)
case class GetProjectOutputRequest(id: String, path: String)
case class GetTableOutputRequest(id: String, sampleRows: Int)
case class TableColumn(name: String, dataType: String)
case class GetTableOutputResponse(header: List[TableColumn], data: List[List[DynamicValue]])
case class GetPlotOutputRequest(id: String)
case class GetPlotOutputResponse(json: FEScalar)
case class GetVisualizationOutputRequest(id: String)
case class CreateWorkspaceRequest(name: String)
case class BoxCatalogRequest(ref: WorkspaceReference)
case class BoxCatalogResponse(boxes: List[BoxMetadata], categories: List[FEOperationCategory])
case class CreateSnapshotRequest(name: String, id: String)
case class GetExportResultRequest(stateId: String)
case class GetExportResultResponse(parameters: Map[String, String], result: FEScalar)
case class RunWorkspaceRequest(workspace: Workspace, parameters: Map[String, String])
case class RunWorkspaceResponse(
    outputs: List[BoxOutputInfo],
    summaries: Map[String, String],
    progress: Map[String, List[Double]])
case class ImportBoxRequest(box: Box, ref: Option[WorkspaceReference])
case class OpenWizardRequest(name: String) // We want to open this.
case class OpenWizardResponse(name: String) // Open this instead.

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
    workspace: Option[WorkspaceReference],
    inputStateId: String, // State at the start of the instrument chain.
    instruments: List[Instrument]) // Instrument chain. (N instruments.)
case class GetInstrumentedStateResponse(
    metas: List[FEOperationMeta], // Metadata for each instrument. (N metadatas.)
    states: List[InstrumentState]) // Initial state + the output of each instrument. (N+1 states.)

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
    assert(!user.wizardOnly, s"User ${user.email} is restricted to using wizards.")
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val w = entry.asNewWorkspaceFrame()
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
        try {
          val op = ctx.getOperation(boxId).asInstanceOf[CustomBoxOperation]
          val cws = op.connectedWorkspace
          (cws, op.getParams, op.context.box.operationId)
        } catch {
          case t: Throwable =>
            log.error(s"Could not run box $boxId for $ref", t)
            // We can still show the insides without connecting the inputs.
            val box = ws.findBox(boxId)
            val cws =
              DirectoryEntry.fromName(box.operationId).asInstanceOf[WorkspaceFrame].workspace
            (cws, box.parameters, box.operationId)
        }
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
        RunWorkspaceResponse(List(), Map(), Map())
    }
    GetWorkspaceResponse(
      ref.name, ref.ws, run.outputs, run.summaries, run.progress,
      canUndo = ref.frame.currentState.previousCheckpoint.nonEmpty,
      canRedo = ref.frame.nextCheckpoint.nonEmpty)
  }

  def openWizard(
    user: serving.User, request: OpenWizardRequest): OpenWizardResponse = {
    val frame = getWorkspaceFrame(user, request.name)
    val ws = frame.workspace
    assert(ws.isWizard, s"${request.name} is not a wizard")
    // In-progress wizards can be opened normally. Otherwise we create an in-progress copy first.
    if (ws.inProgress) OpenWizardResponse(request.name)
    else metaManager.synchronized {
      val newName = s"${user.home}/In progress wizards/${frame.path.last.name}/${Timestamp.human}"
      assertNameNotExists(newName)
      val newFrame = DirectoryEntry.fromName(newName)
      newFrame.assertParentWriteAllowedFrom(user)
      val newWorkspace = Workspace(ws.boxes.map(box =>
        if (box.id == "anchor") box.copy(parameters = box.parameters + ("in_progress" -> "yes"))
        else box))
      newFrame.asNewWorkspaceFrame().setCheckpoint(newWorkspace.checkpoint())
      OpenWizardResponse(newName)
    }
  }

  def runWorkspace(
    user: serving.User, request: RunWorkspaceRequest): RunWorkspaceResponse = {
    val context = request.workspace.context(user, ops, request.parameters)
    val states = context.allStatesOrdered
    calculatedStates.synchronized {
      for ((_, boxOutputState) <- states) {
        calculatedStates(boxOutputState.gUID) = boxOutputState
      }
    }
    val stateInfo = states.toList.map {
      case (boxOutput, boxOutputState) =>
        BoxOutputInfo(
          boxOutput, boxOutputState.gUID.toString, boxOutputState.success, boxOutputState.kind)
    }
    def crop(s: String): String = {
      val maxLength = 50
      if (s.length > maxLength) { s.substring(0, maxLength - 3) + "..." } else { s }
    }
    val summaries = request.workspace.boxes.map(
      box => box.id -> crop(
        try { context.getOperationForStates(box, context.allStates).summary }
        catch {
          case t: Throwable =>
            log.error(s"Error while generating summary for $box in $request.", t)
            box.operationId
        })).toMap
    val progress = getProgress(user, states.map(_._2.gUID.toString))
    RunWorkspaceResponse(stateInfo, summaries, progress)
  }

  // This is for storing the calculated BoxOutputState objects, so the same states can be referenced later.
  val calculatedStates = new HashMap[java.util.UUID, BoxOutputState]()

  def getOutput(user: serving.User, stateId: String): BoxOutputState = {
    calculatedStates.synchronized {
      calculatedStates.get(stateId.asUUID)
    } match {
      case None =>
        val msg = s"Unknown BoxOutputState: $stateId"
        BoxOutputState("error", None, FEStatus(false, disabledReason = msg))
      case Some(state: BoxOutputState) => state
    }
  }

  def getProjectOutput(
    user: serving.User, request: GetProjectOutputRequest): FEProject = {
    val state = getOutput(user, request.id)
    val pathSeq = SubProject.splitPipedPath(request.path).filter(_ != "")
    val project =
      if (state.isVisualization) state.visualization.project
      else state.project
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

  private val edgeVisualizationOptions = Set("edge label", "edge color", "width")
  private def visualizedEntitiesForSide(
    state: VisualizationState, side: UIStatus): List[MetaGraphEntity] = {
    side.projectPath match {
      case None => List()
      case Some(p) =>
        val segs = p.split("\\.", -1).filter(_.nonEmpty)
        val viewer = state.project.viewer.offspringViewer(segs)
        side.attributeTitles.map {
          case (visuType, attrName) =>
            if (edgeVisualizationOptions.contains(visuType)) viewer.edgeAttributes(attrName)
            else viewer.vertexAttributes(attrName)
        }.toList ++ Option(viewer.edgeBundle) :+ viewer.vertexSet
    }
  }

  def getProgress(user: serving.User, stateIdsOrdered: Seq[String]): Map[String, List[Double]] = {
    val states = stateIdsOrdered.map(stateId => stateId -> getOutput(user, stateId))
    val seen = collection.mutable.Set[java.util.UUID]()
    states.map {
      case (stateId, state) => try {
        state.success.check()
        val entities: List[MetaGraphEntity] = state.kind match {
          case BoxOutputKind.Project => state.project.viewer.allEntities
          case BoxOutputKind.Table => List(state.table)
          case BoxOutputKind.Plot => List(state.plot)
          case BoxOutputKind.ExportResult => List(state.exportResult)
          case BoxOutputKind.Visualization =>
            visualizedEntitiesForSide(state.visualization, state.visualization.uiStatus.left) ++
              visualizedEntitiesForSide(state.visualization, state.visualization.uiStatus.right)
          case _ => throw new AssertionError(s"Unknown kind ${state.kind}")
        }
        val progress: List[Double] = entities.filter(e => !seen.contains(e.gUID)).map(
          e => entityProgressManager.computeProgress(e))
        seen ++= entities.map(_.gUID)
        stateId -> progress
      } catch {
        case t: Throwable =>
          log.error(s"Error computing progress for $stateId", t)
          stateId -> List(-1.0)
      }
    }.toMap
  }

  def createSnapshot(
    user: serving.User, request: CreateSnapshotRequest): Unit = {
    assert(!user.wizardOnly, s"User ${user.email} is restricted to using wizards.")
    def calculatedState() = calculatedStates.synchronized {
      calculatedStates(request.id.asUUID)
    }
    createSnapshotFromState(user, request.name, calculatedState)
  }

  def createSnapshotFromState(user: serving.User, path: String, state: () => BoxOutputState): Unit = {
    val entry = DirectoryEntry.fromName(path)
    entry.assertWriteAllowedFrom(user)
    entry.asNewSnapshotFrame(state())
  }

  def setWorkspace(
    user: serving.User, request: SetWorkspaceRequest): Unit = metaManager.synchronized {
    val f = getWorkspaceFrame(user, ResolvedWorkspaceReference(user, request.reference).name)
    f.assertWriteAllowedFrom(user)
    if (user.wizardOnly) assertWizardEditAllowed(user, f.workspace, request.workspace)
    val cp = request.workspace.checkpoint(previous = f.checkpoint)
    f.setCheckpoint(cp)
  }

  private def assertWizardEditAllowed(user: serving.User, ws0: Workspace, ws1: Workspace): Unit = {
    // Wizard-only users need to be able to modify in-progress wizards through setWorkspace.
    // But we make sure the modification is limited to the exposed parts.
    val msg = s"User ${user.email} is restricted to using wizards."
    assert(ws1.copy(boxes = ws0.boxes) == ws0, msg) // Only boxes have changed.
    val stepsJson = ws0.anchor.parameters.getOrElse("steps", "[]")
    import play.api.libs.json
    val steps = json.Json.parse(stepsJson).as[List[json.JsObject]]
    val editableBoxes = steps
      .filter(j => (j \ "popup").as[String] == "parameters")
      .map(j => (j \ "box").as[String]).toSet
    assert(ws1.boxes.length == ws0.boxes.length, msg)
    for (i <- ws1.boxes.indices) {
      val b0 = ws0.boxes(i)
      val b1 = ws1.boxes(i)
      assert(b1.id == b0.id, msg)
      if (editableBoxes.contains(b1.id)) {
        // Only simple parameters have changed.
        assert(b1.copy(parameters = b0.parameters) == b0, msg)
      } else {
        assert(b1 == b0, msg)
      }
    }
  }

  def setAndGetWorkspace(
    user: serving.User, request: SetWorkspaceRequest): GetWorkspaceResponse = metaManager.synchronized {
    setWorkspace(user, request)
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

  private def operationIsWorkspace(user: serving.User, opId: String): Boolean = {
    val entry = DirectoryEntry.fromName(opId)
    entry.exists && entry.isWorkspace && entry.readAllowedFrom(user)
  }

  def boxCatalog(user: serving.User, request: BoxCatalogRequest): BoxCatalogResponse = {
    // We need the custom boxes of the current workspace, if the call comes
    // from the UI.
    val customBoxOperationIds = if (!request.ref.top.isEmpty()) {
      val ref = ResolvedWorkspaceReference(
        user,
        WorkspaceReference(request.ref.top, request.ref.customBoxStack))
      ref.ws.boxes.map(_.operationId).filter(operationIsWorkspace(user, _))
    } else {
      List()
    }
    BoxCatalogResponse(
      ops.operationsRelevantToWorkspace(
        user, request.ref.top, customBoxOperationIds).toList.map(ops.getBoxMetadata(_)),
      ops.getCategories(user))
  }

  def getOperationMeta(user: serving.User, request: GetOperationMetaRequest): FEOperationMeta = {
    getOperation(user, request).toFE
  }

  def getOperationInputTables(user: serving.User, request: GetOperationMetaRequest): Map[String, ProtoTable] = {
    import Operation.Implicits._
    getOperation(user, request).getInputTables()
  }

  def getOperation(user: serving.User, request: GetOperationMetaRequest): Operation = {
    val ref = ResolvedWorkspaceReference(user, request.workspace)
    val ctx = ref.ws.context(user, ops, ref.params)
    ctx.getOperation(request.box)
  }

  @annotation.tailrec
  private def instrumentStatesAndMetas(
    ctx: WorkspaceExecutionContext,
    instruments: List[Instrument],
    states: List[BoxOutputState],
    opMetas: List[FEOperationMeta]): (List[BoxOutputState], List[FEOperationMeta]) = {
    val next = if (instruments.isEmpty) {
      None
    } else {
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
    }
    next match {
      case Some((newState, newMeta)) if newState.isError =>
        // Pretend there are no more instruments. This allows the error state to be seen.
        (states :+ newState, opMetas :+ newMeta)
      case Some((newState, newMeta)) =>
        instrumentStatesAndMetas(ctx, instruments.tail, states :+ newState, opMetas :+ newMeta)
      case None => (states, opMetas)
    }
  }

  def getInstrumentedState(
    user: serving.User, request: GetInstrumentedStateRequest): GetInstrumentedStateResponse = {
    assert(!user.wizardOnly, s"User ${user.email} is restricted to using wizards.")
    val ctx = request.workspace match {
      case Some(ws) =>
        val ref = ResolvedWorkspaceReference(user, ws)
        ref.ws.context(user, ops, ref.params)
      case None => // We can still execute the instrument we just won't have ws parameters.
        Workspace.from().context(user, ops, Map())
    }
    val inputState = getOutput(user, request.inputStateId)
    var (states, opMetas) = instrumentStatesAndMetas(
      ctx, request.instruments, List(inputState), List[FEOperationMeta]())
    val instrumentStates = calculatedStates.synchronized {
      states.map { state =>
        calculatedStates(state.gUID) = state
        InstrumentState(state.gUID.toString, state.kind, state.success.disabledReason)
      }
    }
    GetInstrumentedStateResponse(opMetas, instrumentStates)
  }
}
