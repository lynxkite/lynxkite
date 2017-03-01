// Things that go inside a "boxes & arrows" workspace.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json
import com.lynxanalytics.biggraph._

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
    user: serving.User, ops: OperationRepository, connection: BoxConnection): BoxOutputState = {
    calculate(user, ops, connection, Map())(connection)
  }

  // Calculates an output. Returns every state that had been calculated as a side-effect.
  def calculate(
    user: serving.User, ops: OperationRepository, connection: BoxConnection,
    states: Map[BoxConnection, BoxOutputState]): Map[BoxConnection, BoxOutputState] = {
    if (states.contains(connection)) states else {
      val box = findBox(connection.box)
      val unconnecteds = box.inputs.filter(_.connectedTo.isEmpty)
      if (unconnecteds.nonEmpty) {
        val list = unconnecteds.map(_.id).mkString(", ")
        states ++ box.errorOutputs(s"Input $list is not connected.")
      } else {
        val updatedStates = box.inputs.foldLeft(states) {
          (states, lc) => calculate(user, ops, lc.connectedTo.get, states)
        }
        val inputs = box.inputs.map(lc => lc.id -> updatedStates(lc.connectedTo.get)).toMap
        val errors = inputs.filter(_._2.isError)
        if (errors.nonEmpty) {
          val list = errors.map(_._1).mkString(", ")
          updatedStates ++ box.errorOutputs(s"Input $list has an error.")
        } else {
          val outputStates = try {
            box.execute(user, inputs, ops)
          } catch {
            case ex: Throwable =>
              val msg = ex match {
                case ex: AssertionError => ex.getMessage
                case _ => ex.toString
              }
              box.errorOutputs(msg)
          }
          updatedStates ++ outputStates
        }
      }
    }
  }
}

object Workspace {
  val empty = Workspace(List())
}

case class Box(
    id: String,
    category: String,
    operation: String,
    parameters: Map[String, String],
    x: Double,
    y: Double,
    inputs: List[LocalBoxConnection],
    outputs: List[LocalBoxConnection]) {

  def input(id: String) = inputs.find(_.id == id).get.ofBox(this)

  def output(id: String) = outputs.find(_.id == id).get.ofBox(this)

  def execute(
    user: serving.User,
    inputStates: Map[String, BoxOutputState],
    ops: OperationRepository): Map[BoxConnection, BoxOutputState] = {
    assert(
      inputs.size == inputStates.size &&
        inputs.forall(i => inputStates.get(i.id).map(_.kind == i.kind).getOrElse(false)),
      s"Input mismatch: $inputStates does not match $inputs")
    val op = ops.opForBox(ops.context(user, inputStates), this)
    val outputStates = op.getOutputs(parameters)
    assert(
      outputs.size == outputStates.size &&
        outputs.forall(o => outputStates.get(o.ofBox(this)).map(_.kind == o.kind).getOrElse(false)),
      s"Output mismatch: $outputStates does not match $outputs")
    outputStates
  }

  def connect(input: String, output: BoxConnection): Box = {
    this.copy(inputs = inputs.map {
      i => if (i.id != input) i else i.copy(connectedTo = Some(output))
    })
  }

  def errorOutputs(msg: String): Map[BoxConnection, BoxOutputState] = {
    outputs.map(_.ofBox(this)).map(c => c -> BoxOutputState.error(c, msg)).toMap
  }
}

case class LocalBoxConnection(
    id: String,
    kind: String,
    connectedTo: Option[BoxConnection] = None) {
  BoxOutputState.assertKind(kind)
  for (c <- connectedTo) {
    assert(kind == c.kind, s"$id is of type $kind, and cannot connect to $connectedTo")
  }
  def ofBox(box: Box) = BoxConnection(box.id, id, kind)
}

// BoxConnection is ambiguous in that it does not specify whether the connection is an input or
// output. But inputs and outputs live such different lives that this is okay.
case class BoxConnection(
    box: String,
    id: String,
    kind: String) {
  BoxOutputState.assertKind(kind)
}

case class BoxMetadata(
    category: String,
    operation: String,
    inputs: List[LocalBoxConnection],
    outputs: List[LocalBoxConnection]) {
  def toBox(id: String, parameters: Map[String, String], x: Double, y: Double) =
    Box(id, category, operation, parameters, x, y, inputs, outputs)
}

object BoxOutputState {
  val ProjectKind = "project"
  val validKinds = Set(ProjectKind) // More kinds to come.
  def assertKind(kind: String): Unit =
    assert(validKinds.contains(kind), s"Unknown connection type: $kind")
  def error(c: BoxConnection, message: String) =
    BoxOutputState(c.box, c.id, c.kind, null, FEStatus.disabled(message))
}

case class BoxOutputState(
    box: String,
    output: String,
    kind: String,
    state: json.JsValue,
    success: FEStatus = FEStatus.enabled) {
  BoxOutputState.assertKind(kind)
  def isError = !success.enabled
  def isProject = kind == BoxOutputState.ProjectKind
  def project(implicit m: graph_api.MetaGraphManager): RootProjectEditor = {
    assert(isProject, s"$box=>$output is not a project but a $kind.")
    assert(success.enabled, success.disabledReason)
    import CheckpointRepository.fCommonProjectState
    val p = state.as[CommonProjectState]
    val rps = RootProjectState.emptyState.copy(state = p)
    new RootProjectEditor(rps)
  }
  def connection = BoxConnection(box, output, kind)
}

object WorkspaceJsonFormatters {
  import com.lynxanalytics.biggraph.serving.FrontendJson.fFEStatus
  implicit val fBoxConnection = json.Json.format[BoxConnection]
  implicit val fLocalBoxConnection = json.Json.format[LocalBoxConnection]
  implicit val fBoxOutputState = json.Json.format[BoxOutputState]
  implicit val fBox = json.Json.format[Box]
  implicit val fBoxMetadata = json.Json.format[BoxMetadata]
  implicit val fWorkspace = json.Json.format[Workspace]
}
