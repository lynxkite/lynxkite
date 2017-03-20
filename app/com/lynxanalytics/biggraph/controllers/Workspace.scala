// Things that go inside a "boxes & arrows" workspace.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json
import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class Workspace(
    boxes: List[Box]) {
  val boxMap = boxes.map(b => b.id -> b).toMap
  assert(boxMap.size == boxes.size, {
    val dups = boxes.map(_.id).groupBy(identity).collect { case (id, ids) if ids.size > 1 => id }
    s"Duplicate box name: ${dups.mkString(", ")}"
  })

  def findBox(id: String): Box = {
    assert(boxMap.contains(id), s"Cannot find box $id")
    boxMap(id)
  }

  def checkpoint(previous: String = null)(implicit manager: graph_api.MetaGraphManager): String = {
    manager.checkpointRepo.checkpointState(
      RootProjectState.emptyState.copy(checkpoint = None, workspace = Some(this)),
      previous).checkpoint.get
  }

  def state(
    user: serving.User, ops: OperationRepository, connection: BoxOutput): BoxOutputState = {
    calculate(user, ops, connection, Map())(connection)
  }

  // Calculates an output. Returns every state that had been calculated as a side-effect.
  def calculate(
    user: serving.User, ops: OperationRepository, connection: BoxOutput,
    states: Map[BoxOutput, BoxOutputState]): Map[BoxOutput, BoxOutputState] = {
    if (states.contains(connection)) states else {
      val box = findBox(connection.boxID)
      val meta = ops.getBoxMetadata(box.operationID)

      def errorOutputs(msg: String): Map[BoxOutput, BoxOutputState] = {
        meta.outputs.map {
          o => o.ofBox(box) -> BoxOutputState(o.kind, null, FEStatus.disabled(msg))
        }.toMap
      }

      val unconnecteds = meta.inputs.filterNot(conn => box.inputs.contains(conn.id))
      if (unconnecteds.nonEmpty) {
        val list = unconnecteds.map(_.id).mkString(", ")
        states ++ errorOutputs(s"Input $list is not connected.")
      } else {
        val updatedStates = box.inputs.values.foldLeft(states) {
          (states, output) => calculate(user, ops, output, states)
        }
        val inputs = box.inputs.map { case (id, output) => id -> updatedStates(output) }
        val errors = inputs.filter(_._2.isError)
        if (errors.nonEmpty) {
          val list = errors.map(_._1).mkString(", ")
          updatedStates ++ errorOutputs(s"Input $list has an error.")
        } else {
          val outputStates = try {
            box.execute(user, inputs, ops)
          } catch {
            case ex: Throwable =>
              log.error(s"Failed to execute $box:", ex)
              val msg = ex match {
                case ae: AssertionError => ae.getMessage
                case _ => ex.toString
              }
              errorOutputs(msg)
          }
          updatedStates ++ outputStates
        }
      }
    }
  }

  // Calculates the progress of an output and all other outputs that had been calculated as a
  // side-effect.
  def progress(
    user: serving.User, ops: OperationRepository,
    connection: BoxOutput)(implicit entityProgressManager: graph_api.EntityProgressManager,
                           manager: graph_api.MetaGraphManager): List[ProgressInfo] = {
    val states = calculate(user, ops, connection, Map())
    states.toList.map((calculatedStateProgress _).tupled)
  }

  private def calculatedStateProgress(
    boxOutput: BoxOutput,
    state: BoxOutputState)(implicit entityProgressManager: graph_api.EntityProgressManager,
                           manager: graph_api.MetaGraphManager): ProgressInfo =
    state.kind match {
      case BoxOutputKind.Project => projectProgress(boxOutput, state)
    }

  private def projectProgress(
    boxOutput: BoxOutput,
    state: BoxOutputState)(implicit entityProgressManager: graph_api.EntityProgressManager,
                           manager: graph_api.MetaGraphManager): ProgressInfo = {
    assert(state.kind == BoxOutputKind.Project,
      s"Can't compute projectProgress for kind ${state.kind}")

    def commonProjectStateProgress(state: CommonProjectState): List[Double] = {
      val allEntities = state.vertexSetGUID.map(manager.vertexSet).toList ++
        state.edgeBundleGUID.map(manager.edgeBundle).toList ++
        state.scalarGUIDs.values.map(manager.scalar) ++
        state.vertexAttributeGUIDs.values.map(manager.attribute) ++
        state.edgeAttributeGUIDs.values.map(manager.attribute)

      val segmentationProgress = state.segmentations.values.flatMap(segmentationStateProgress)
      allEntities.map(entityProgressManager.computeProgress) ++ segmentationProgress
    }

    def segmentationStateProgress(state: SegmentationState): List[Double] = {
      val segmentationProgress = commonProjectStateProgress(state.state)
      val belongsToProgress = state.belongsToGUID.map(belongsToGUID => {
        val belongsTo = manager.edgeBundle(belongsToGUID)
        entityProgressManager.computeProgress(belongsTo)
      }).toList
      belongsToProgress ++ segmentationProgress
    }

    val progress = if (state.success.enabled) {
      val progressList = commonProjectStateProgress(state.project.rootState.state)
      Map(
        "computed" -> progressList.count(_ == 1.0),
        "inProgress" -> progressList.count(x => x < 1.0 && x > 0.0),
        "notYetStarted" -> progressList.count(_ == 0.0),
        "failed" -> progressList.count(_ < 0.0)
      )
    } else {
      null
    }
    ProgressInfo(boxOutput, progress, state.success)
  }

  def getOperation(
    user: serving.User, ops: OperationRepository, boxID: String): Operation = {
    val box = findBox(boxID)
    val meta = ops.getBoxMetadata(box.operationID)
    for (i <- meta.inputs) {
      assert(box.inputs.contains(i.id), s"Input ${i.id} is not connected.")
    }
    val states = box.inputs.values.foldLeft(Map[BoxOutput, BoxOutputState]()) {
      (states, output) => calculate(user, ops, output, states)
    }
    val inputs = box.inputs.map { case (id, output) => id -> states(output) }
    assert(!inputs.exists(_._2.isError), {
      val errors = inputs.filter(_._2.isError).map(_._1).mkString(", ")
      s"Input $errors has an error."
    })
    box.getOperation(user, inputs, ops)
  }
}

object Workspace {
  val empty = Workspace(List())
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
    user: serving.User,
    inputStates: Map[String, BoxOutputState],
    ops: OperationRepository): Operation = {
    assert(
      inputs.keys == inputStates.keys,
      s"Input mismatch: $inputStates does not match $inputs")
    ops.opForBox(user, this, inputStates)
  }

  def execute(
    user: serving.User,
    inputStates: Map[String, BoxOutputState],
    ops: OperationRepository): Map[BoxOutput, BoxOutputState] = {
    val op = getOperation(user, inputStates, ops)
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
  inputs: List[TypedConnection],
  outputs: List[TypedConnection])

object BoxOutputKind {
  val Project = "project"
  val validKinds = Set(Project) // More kinds to come.
  def assertKind(kind: String): Unit =
    assert(validKinds.contains(kind), s"Unknown connection type: $kind")
}

case class BoxOutputState(
    kind: String,
    state: json.JsValue,
    success: FEStatus = FEStatus.enabled) {
  BoxOutputKind.assertKind(kind)
  def isError = !success.enabled
  def isProject = kind == BoxOutputKind.Project
  def project(implicit m: graph_api.MetaGraphManager): RootProjectEditor = {
    assert(isProject, s"Tried to access '$kind' as 'project'.")
    assert(success.enabled, success.disabledReason)
    import CheckpointRepository.fCommonProjectState
    val p = state.as[CommonProjectState]
    val rps = RootProjectState.emptyState.copy(state = p)
    new RootProjectEditor(rps)
  }
}

case class ProgressInfo(boxOutput: BoxOutput, progress: Map[String, Int], success: FEStatus)

object WorkspaceJsonFormatters {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fFEStatus
  implicit val fBoxOutput = json.Json.format[BoxOutput]
  implicit val fTypedConnection = json.Json.format[TypedConnection]
  implicit val fBoxOutputState = json.Json.format[BoxOutputState]
  implicit val fBox = json.Json.format[Box]
  implicit val fBoxMetadata = json.Json.format[BoxMetadata]
  implicit val fWorkspace = json.Json.format[Workspace]
  implicit val fProgressInfo = json.Json.format[ProgressInfo]
}
