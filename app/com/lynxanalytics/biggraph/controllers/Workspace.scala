// Things that go inside a "boxes & arrows" workspace.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import scala.annotation.tailrec

case class Workspace(
    boxes: List[Box]) {
  val boxMap = boxes.map(b => b.id -> b).toMap
  assert(boxMap.size == boxes.size, {
    val dups = boxes.groupBy(_.id).filter(_._2.size > 1).keys
    s"Duplicate box name: ${dups.mkString(", ")}"
  })

  def findBox(id: String): Box = {
    assert(boxMap.contains(id), s"Cannot find box $id")
    boxMap(id)
  }

  private def parametersMeta: List[FEOperationParameterMeta] = {
    val anchor = boxMap.getOrElse("anchor", Workspace.anchorBox)
    val parametersParamValue =
      anchor.parameters.getOrElse("parameters", OperationParams.ParametersParam.defaultValue)
    OperationParams.ParametersParam.parse(parametersParamValue)
  }

  private def defaultParameters: Map[String, String] =
    parametersMeta.map(p => p.id -> p.defaultValue).toMap

  private def fillDefaults(ctx: WorkspaceExecutionContext) =
    ctx.copy(workspaceParameters = defaultParameters ++ ctx.workspaceParameters)

  // Changes the workspace to enforce some invariants.
  def repaired(ops: OperationRepository): Workspace = {
    repairAnchor
    // TODO: #5883 after #5834.
  }

  private def repairAnchor: Workspace = {
    val anchors = boxes.filter(_.operationID == "Anchor")
    anchors match {
      case List(box) =>
        assert(box.id == "anchor", "The anchor box must have the 'anchor' ID.")
        this
      case Nil => Workspace(Workspace.anchorBox +: boxes)
      case _ => throw new AssertionError(s"${anchors.size} anchors found.")
    }
  }

  def checkpoint(previous: String = null)(implicit manager: graph_api.MetaGraphManager): String = {
    manager.checkpointRepo.checkpointState(
      RootProjectState.emptyState.copy(checkpoint = None, workspace = Some(this)),
      previous).checkpoint.get
  }

  def allStates(ctx: WorkspaceExecutionContext): Map[BoxOutput, BoxOutputState] = {
    val dependencies = discoverDependencies
    val statesWithoutCircularDependency = dependencies.topologicalOrder
      .foldLeft(Map[BoxOutput, BoxOutputState]()) {
        (states, box) =>
          val newOutputStates = outputStatesOfBox(ctx, box, states)
          newOutputStates ++ states
      }
    val statesWithCircularDependency = dependencies.withCircularDependency.flatMap { box =>
      val meta = ctx.ops.getBoxMetadata(box.operationID)
      meta.outputs.map { o =>
        o.ofBox(box) ->
          BoxOutputState(
            o.kind,
            None,
            FEStatus.disabled("Can not compute state due to circular dependencies.")
          )
      }
    }.toMap
    statesWithoutCircularDependency ++ statesWithCircularDependency
  }

  private def outputStatesOfBox(ctx: WorkspaceExecutionContext, box: Box,
                                inputStates: Map[BoxOutput, BoxOutputState]): Map[BoxOutput, BoxOutputState] = {
    val meta = ctx.ops.getBoxMetadata(box.operationID)

    def allOutputsWithError(msg: String): Map[BoxOutput, BoxOutputState] = {
      meta.outputs.map {
        o => o.ofBox(box) -> BoxOutputState(o.kind, None, FEStatus.disabled(msg))
      }.toMap
    }

    val unconnectedInputs = meta.inputs.filterNot(conn => box.inputs.contains(conn))
    if (unconnectedInputs.nonEmpty) {
      val list = unconnectedInputs.mkString(", ")
      allOutputsWithError(s"Input $list is not connected.")
    } else {
      val inputs = box.inputs.map { case (id, output) => id -> inputStates(output) }
      val inputErrors = inputs.filter(_._2.isError)
      if (inputErrors.nonEmpty) {
        val list = inputErrors.keys.mkString(", ")
        allOutputsWithError(s"Input $list has an error.")
      } else {
        val outputStates = try {
          box.execute(fillDefaults(ctx), inputs)
        } catch {
          case ex: Throwable =>
            log.error(s"Failed to execute $box:", ex)
            val msg = ex match {
              case ae: AssertionError => ae.getMessage
              case _ => ex.toString
            }
            allOutputsWithError(msg)
        }
        outputStates
      }
    }
  }

  case class Dependencies(topologicalOrder: List[Box], withCircularDependency: List[Box])

  // Tries to determine a topological order among boxes. All boxes with a circular dependency and
  // ones that depend on another with a circular dependency are returned unordered.
  private def discoverDependencies: Dependencies = {
    val outEdges: Map[Box, Set[Box]] = {
      val edges = boxes.flatMap(dst => dst.inputs.map(input => findBox(input._2.boxID) -> dst))
      edges.groupBy(_._1).mapValues(_.map(_._2).toSet)
    }

    // Determines the topological order by selecting a node without in-edges, removing the node and
    // its connections and calling itself on the remaining graph.
    @tailrec
    def discover(reversedTopologicalOrder: List[Box],
                 remainingBoxInDegrees: List[(Box, Int)]): Dependencies =
      if (remainingBoxInDegrees.isEmpty) {
        Dependencies(reversedTopologicalOrder.reverse, List())
      } else {
        val (nextBox, lowestDegree) = remainingBoxInDegrees.minBy(_._2)
        if (lowestDegree > 0) {
          Dependencies(
            topologicalOrder = reversedTopologicalOrder.reverse,
            withCircularDependency = remainingBoxInDegrees.map(_._1)
          )
        } else {
          val dependants = outEdges.getOrElse(nextBox, Set())
          val updatedInDegrees = remainingBoxInDegrees.withFilter(_._1 != nextBox)
            .map {
              case (box, degree) => (box, if (dependants.contains(box)) degree - 1 else degree)
            }.map(identity)
          discover(nextBox :: reversedTopologicalOrder, updatedInDegrees)
        }
      }

    val inDegrees: List[(Box, Int)] = boxes.map(box => box -> box.inputs.size)
    discover(List(), inDegrees)
  }

  def getOperation(
    ctx: WorkspaceExecutionContext, boxID: String): Operation = {
    val box = findBox(boxID)
    val meta = ctx.ops.getBoxMetadata(box.operationID)
    for (i <- meta.inputs) {
      assert(box.inputs.contains(i), s"Input $i is not connected.")
    }
    val states = allStates(ctx)
    val inputs = box.inputs.map { case (id, output) => id -> states(output) }
    assert(!inputs.exists(_._2.isError), {
      val errors = inputs.filter(_._2.isError).map(_._1).mkString(", ")
      s"Input $errors has an error."
    })
    box.getOperation(fillDefaults(ctx), inputs)
  }
}

// Things that are outside of the workspace itself, but are relevant to executing it.
case class WorkspaceExecutionContext(
  user: serving.User,
  ops: OperationRepository,
  workspaceParameters: Map[String, String])

object Workspace {
  val anchorBox = Box("anchor", "Anchor", Map(), 0, 0, Map())
  val empty = Workspace(List(anchorBox))
}

case class Box(
    id: String,
    operationID: String,
    parameters: Map[String, String],
    x: Double,
    y: Double,
    inputs: Map[String, BoxOutput]) {

  def output(id: String) = BoxOutput(this.id, id)

  def getOperation(
    ctx: WorkspaceExecutionContext,
    inputStates: Map[String, BoxOutputState]): Operation = {
    assert(
      inputs.keys == inputStates.keys,
      s"Input mismatch: $inputStates does not match $inputs")
    ctx.ops.opForBox(ctx.user, this, inputStates, ctx.workspaceParameters)
  }

  def execute(
    ctx: WorkspaceExecutionContext,
    inputStates: Map[String, BoxOutputState]): Map[BoxOutput, BoxOutputState] = {
    val op = getOperation(ctx, inputStates)
    val outputStates = op.getOutputs
    outputStates
  }
}

case class TypedConnection(
    id: String,
    kind: String) {
  BoxOutputKind.assertKind(kind)
  def ofBox(box: Box) = box.output(id)
}

case class BoxOutput(
  boxID: String,
  id: String)

case class BoxMetadata(
  categoryID: String,
  operationID: String,
  inputs: List[String],
  outputs: List[TypedConnection])

object BoxOutputKind {
  val Project = "project"
  val Table = "table"
  val validKinds = Set(Project, Table)
  def assertKind(kind: String): Unit =
    assert(validKinds.contains(kind), s"Unknown connection type: $kind")
}

object BoxOutputState {
  // Cannot call these "apply" due to the JSON formatter macros.
  def from(project: ProjectEditor): BoxOutputState = {
    import CheckpointRepository._ // For JSON formatters.
    BoxOutputState(BoxOutputKind.Project, Some(json.Json.toJson(project.rootState.state)))
  }

  def from(table: graph_api.Table): BoxOutputState = {
    BoxOutputState(BoxOutputKind.Table, Some(json.Json.obj("guid" -> table.gUID)))
  }
}

case class BoxOutputState(
    kind: String,
    state: Option[json.JsValue],
    success: FEStatus = FEStatus.enabled) {
  BoxOutputKind.assertKind(kind)
  assert(success.enabled ^ (state.isEmpty || state.get == null),
    "State should be present iff computation was successful")

  def isError = !success.enabled
  def isProject = kind == BoxOutputKind.Project
  def isTable = kind == BoxOutputKind.Table

  def project(implicit m: graph_api.MetaGraphManager): RootProjectEditor = {
    assert(isProject, s"Tried to access '$kind' as 'project'.")
    assert(success.enabled, success.disabledReason)
    import CheckpointRepository.fCommonProjectState
    val p = state.get.as[CommonProjectState]
    val rps = RootProjectState.emptyState.copy(state = p)
    new RootProjectEditor(rps)
  }

  def table(implicit manager: graph_api.MetaGraphManager): graph_api.Table = {
    assert(isTable, s"Tried to access '$kind' as 'table'.")
    assert(success.enabled, success.disabledReason)
    import graph_api.MetaGraphManager.StringAsUUID
    manager.table((state.get \ "guid").as[String].asUUID)
  }
}

object WorkspaceJsonFormatters {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fFEStatus
  implicit val fBoxOutput = json.Json.format[BoxOutput]
  implicit val fTypedConnection = json.Json.format[TypedConnection]
  implicit val fBoxOutputState = json.Json.format[BoxOutputState]
  implicit val fBox = json.Json.format[Box]
  implicit val fBoxMetadata = json.Json.format[BoxMetadata]
  implicit val fWorkspace = json.Json.format[Workspace]
}
