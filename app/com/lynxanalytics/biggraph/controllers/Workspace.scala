// Things that go inside a "boxes & arrows" workspace.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json
import com.lynxanalytics.biggraph.graph_api._

case class Workspace(
    boxes: List[Box],
    arrows: List[Arrow],
    states: List[BoxOutputState]) {
  def findBox(id: String): Option[Box] = boxes.find(_.id == id)

  def addBox(box: Box): Workspace = {
    val baseID = box.id
    val uniqueID = Stream.from(1).map(baseID + _).filter(findBox(_).isEmpty).head
    this.copy(boxes = boxes :+ box.copy(id = uniqueID))
  }

  def addArrows(arrows: TraversableOnce[Arrow]) = this.copy(arrows = this.arrows ++ arrows)

  def checkpoint(previous: String = null)(implicit manager: MetaGraphManager): String = {
    manager.checkpointRepo.checkpointState(
      RootProjectState.emptyState.copy(workspace = Some(this)),
      previous).checkpoint.get
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
    outputs: List[LocalBoxConnection],
    status: FEStatus) {
  def input(id: String) = inputs.find(_.id == id).get.ofBox(this)
  def output(id: String) = outputs.find(_.id == id).get.ofBox(this)
}

case class LocalBoxConnection(
    id: String,
    kind: String) {
  BoxOutputState.assertKind(kind)
  def ofBox(box: Box) = BoxConnection(box.id, id, kind)
}

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
    Box(operation, category, operation, parameters, x, y, inputs, outputs,
      FEStatus.assert(inputs.isEmpty, "Disconnected."))
}

case class Arrow(
  src: BoxConnection,
  dst: BoxConnection)

object BoxOutputState {
  val ProjectKind = "project"
  val validKinds = Set(ProjectKind) // More kinds to come.
  def assertKind(kind: String): Unit =
    assert(validKinds.contains(kind), s"Unknown connection type: $kind")
}

case class BoxOutputState(
    box: String,
    output: String,
    kind: String,
    state: json.JsObject) {
  BoxOutputState.assertKind(kind)
  def project(implicit m: MetaGraphManager): RootProjectEditor = {
    assert(kind == BoxOutputState.ProjectKind, s"$box=>$output is not a project but a $kind.")
    import CheckpointRepository.fCommonProjectState
    val p = state.as[CommonProjectState]
    val rps = RootProjectState.emptyState.copy(state = p)
    new RootProjectEditor(rps)
  }
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
