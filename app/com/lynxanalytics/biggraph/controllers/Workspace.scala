// Things that go inside a "boxes & arrows" workspace.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json
import com.lynxanalytics.biggraph._

case class Workspace(
    boxes: List[Box],
    arrows: List[Arrow],
    states: List[BoxOutputState]) {

  def findBox(id: String): Option[Box] = boxes.find(_.id == id)

  def addBox(box: Box): Workspace = {
    assert(findBox(box.id).isEmpty, s"Workspace already contains a box named ${box.id}")
    this.copy(boxes = boxes :+ box)
  }

  def autoName(box: Box): Box = {
    val baseID = box.id
    val uniqueID = Stream.from(1).map(baseID + _).filter(findBox(_).isEmpty).head
    box.copy(id = uniqueID)
  }

  def addArrows(arrows: TraversableOnce[Arrow]) = {
    // TODO: Asserts.
    this.copy(arrows = this.arrows ++ arrows)
  }

  def checkpoint(previous: String = null)(implicit manager: graph_api.MetaGraphManager): String = {
    manager.checkpointRepo.checkpointState(
      RootProjectState.emptyState.copy(workspace = Some(this)),
      previous).checkpoint.get
  }

  // Fetches the other ends of the inputs.
  def inputs(box: Box): List[Option[BoxConnection]] = {
    box.inputs.map(_.ofBox(box)).map(arrowDstToSrc.get(_))
  }

  lazy val arrowDstToSrc = arrows.map(a => a.dst -> a.src).toMap
  lazy val stateMap: Map[BoxConnection, BoxOutputState] =
    states.map(s => s.connection -> s).toMap

  def outputStates(box: Box): List[Option[BoxOutputState]] = {
    box.outputs.map(lc => stateMap.get(lc.ofBox(box)))
  }

  def fillStates(user: serving.User, ops: OperationRepository): Workspace = {
    @annotation.tailrec
    def computeMissing(
      boxes: List[Box], states: Map[BoxConnection, BoxOutputState]): List[BoxOutputState] = {
      val (ready, rest) = boxes.partition(box => inputs(box).forall(o => states.contains(o.get)))
      if (ready.isEmpty) states.values.toList
      else {
        val newStates = states ++ ready.flatMap { box =>
          val inputs = box.inputs.map(lc => lc.id -> states(arrowDstToSrc(lc.ofBox(box)))).toMap
          try {
            box.execute(user, inputs, ops).values.map(s => s.connection -> s)
          } catch {
            case ex: Throwable =>
              val msg = ex match {
                case ex: AssertionError => ex.getMessage
                case _ => ex.toString
              }
              box.outputs.map(_.ofBox(box)).map(c => c -> BoxOutputState.error(c, msg))
          }
        }
        computeMissing(rest, newStates)
      }
    }
    val viable = boxes
      .filterNot(outputStates(_).forall(_.nonEmpty)) // Does not have state yet.
      .filter(inputs(_).forall(_.nonEmpty)) // All inputs are connected.
    this.copy(states = computeMissing(viable, stateMap))
  }
}

object Workspace {
  val empty = Workspace(List(), List(), List())
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
    ops: OperationRepository): Map[String, BoxOutputState] = {
    assert(
      inputs.size == inputStates.size &&
        inputs.forall(i => inputStates.get(i.id).map(_.kind == i.kind).getOrElse(false)),
      s"Input mismatch: $inputStates does not match $inputs")
    val op = ops.opForBox(ops.context(user, inputStates), this)
    val outputStates = op.getOutputs(parameters)
    assert(
      outputs.size == outputStates.size &&
        outputs.forall(o => outputStates.get(o.id).map(_.kind == o.kind).getOrElse(false)),
      s"Output mismatch: $outputStates does not match $outputs")
    outputStates
  }
}

case class LocalBoxConnection(
    id: String,
    kind: String) {
  BoxOutputState.assertKind(kind)
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
  def toBox(parameters: Map[String, String], x: Double, y: Double) =
    Box(operation, category, operation, parameters, x, y, inputs, outputs)
}

case class Arrow(
  src: BoxConnection,
  dst: BoxConnection)

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
  import com.lynxanalytics.biggraph.serving.FrontendJson._
  implicit val fLocalBoxConnection = json.Json.format[LocalBoxConnection]
  implicit val fBoxConnection = json.Json.format[BoxConnection]
  implicit val fBoxOutputState = json.Json.format[BoxOutputState]
  implicit val fArrow = json.Json.format[Arrow]
  implicit val fBox = json.Json.format[Box]
  implicit val fBoxMetadata = json.Json.format[BoxMetadata]
  implicit val fWorkspace = json.Json.format[Workspace]
}
