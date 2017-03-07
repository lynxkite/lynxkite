// The base classes and mechanisms for frontend operations.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import scala.collection.mutable
import scala.reflect.runtime.universe._

case class FEOperationMeta(
  id: String,
  title: String,
  parameters: List[FEOperationParameterMeta],
  visibleScalars: List[FEScalar],
  category: String = "",
  status: FEStatus = FEStatus.enabled,
  color: Option[String] = None)

object FEOperationParameterMeta {
  val validKinds = Seq(
    "default", // A simple textbox.
    "choice", // A drop down box.
    "file", // Simple textbox with file upload button.
    "tag-list", // A variation of "multipleChoice" with a more concise, horizontal design.
    "code", // JavaScript code
    "model", // A special kind to set model parameters.
    "table", // A table.
    "segmentation") // One of the segmentations of the current project.
}

case class FEOperationParameterMeta(
    id: String,
    title: String,
    kind: String, // Special rendering on the UI.
    defaultValue: String,
    options: List[FEOption],
    multipleChoice: Boolean,
    mandatory: Boolean, // If false, this parameter can be omitted from the request.
    payload: Option[json.JsValue]) { // A custom JSON serialized value to transfer to the UI

  require(
    kind.isEmpty || FEOperationParameterMeta.validKinds.contains(kind),
    s"'$kind' is not a valid parameter type")
  if (kind == "tag-list") require(multipleChoice, "multipleChoice is required for tag-list")
}

case class FEOperationSpec(
  id: String,
  parameters: Map[String, String])

case class OperationCategory(
  title: String, icon: String, color: String, ops: List[FEOperationMeta])

abstract class OperationParameterMeta {
  val id: String
  val title: String
  val kind: String
  val defaultValue: String
  val options: List[FEOption]
  val multipleChoice: Boolean
  val mandatory: Boolean
  val payload: Option[json.JsValue] = None

  // Asserts that the value is valid, otherwise throws an AssertionException.
  def validate(value: String): Unit
  def toFE = FEOperationParameterMeta(
    id, title, kind, defaultValue, options, multipleChoice, mandatory, payload)
}

// An Operation is the computation that a Box represents in a workspace.
// They are registered in an OperationRegistry with a factory function that takes an
// Operation.Context parameter. Operations are short-lived and created for a specific input.
trait Operation {
  def enabled: FEStatus
  def summary: String
  def getOutputs: Map[BoxOutput, BoxOutputState]
  def toFE: FEOperationMeta
}
object Operation {
  case class Category(
      title: String,
      color: String, // A color class from web/app/styles/operation-toolbox.css.
      visible: Boolean = true,
      icon: String = "", // Glyphicon name, or empty for first letter of title.
      sortKey: String = null, // Categories are ordered by this. The title is used by default.
      deprecated: Boolean = false) extends Ordered[Category] {
    private val safeSortKey = Option(sortKey).getOrElse(title)
    def compare(that: Category) = this.safeSortKey compare that.safeSortKey
    def toFE(ops: List[FEOperationMeta]): OperationCategory =
      OperationCategory(title, icon, color, ops)
  }

  type Factory = Context => Operation
  case class Context(
    user: serving.User,
    box: Box,
    meta: BoxMetadata,
    inputs: Map[String, BoxOutputState],
    manager: MetaGraphManager)
}

// OperationRegistry is a simple trait for a class that wants to declare a set of operations.
trait OperationRegistry {
  // The registry maps operation IDs to their constructors.
  val operations = mutable.Map[String, (BoxMetadata, Operation.Factory)]()
  def registerOp(
    id: String,
    category: Operation.Category,
    inputs: List[TypedConnection],
    outputs: List[TypedConnection],
    factory: Operation.Factory): Unit = {
    // TODO: Register category somewhere.
    assert(!operations.contains(id), s"$id is already registered.")
    operations(id) = BoxMetadata(category.title, id, inputs, outputs) -> factory
  }
}

// OperationRepository holds a registry of all operations.
abstract class OperationRepository(env: SparkFreeEnvironment) {
  // The registry maps operation IDs to their constructors.
  protected val operations: Map[String, (BoxMetadata, Operation.Factory)]

  def getBoxMetadata(id: String) = operations(id)._1

  def operationIds = operations.keys.toSeq

  def opForBox(user: serving.User, box: Box, inputs: Map[String, BoxOutputState]) = {
    val (meta, factory) = operations(box.operationID)
    val context = Operation.Context(user, box, meta, inputs, env.metaGraphManager)
    factory(context)
  }
}

// A "ProjectOperation" is an operation that has 1 project-typed output. It includes a lot of
// utility methods for supporting this.
abstract class ProjectOperation(context: Operation.Context) extends Operation {
  assert(
    context.meta.outputs == List(TypedConnection("project", "project")),
    s"A ProjectOperation must output a project. $context")
  implicit val manager = context.manager
  protected val project: ProjectEditor
  protected val user = context.user
  protected val id = context.meta.operationID
  protected val title = id
  protected val params = context.box.parameters
  protected def parameters: List[OperationParameterMeta]
  protected def allParameters = parameters // Allows adding common parameters in subclasses.
  protected def visibleScalars: List[FEScalar] = List()
  def summary = title

  protected def apply(): Unit
  protected def help = "<help-popup href=\"" + id + "\"></help-popup>" // Add to notes for help link.

  protected def validateParameters(values: Map[String, String]): Unit = {
    val paramIds = allParameters.map { param => param.id }.toSet
    val extraIds = values.keySet &~ paramIds
    assert(extraIds.size == 0,
      s"""Extra parameters found: ${extraIds.mkString(", ")} is not in ${paramIds.mkString(", ")}""")
    val mandatoryParamIds =
      allParameters.filter(_.mandatory).map { param => param.id }.toSet
    val missingIds = mandatoryParamIds &~ values.keySet
    assert(missingIds.size == 0, s"""Missing parameters: ${missingIds.mkString(", ")}""")
    for (param <- allParameters) {
      if (values.contains(param.id)) {
        param.validate(values(param.id))
      }
    }
  }

  // Updates the vertex_count_delta/edge_count_delta scalars after an operation finished.
  private def updateDeltas(editor: ProjectEditor, original: ProjectViewer): Unit = {
    updateDelta(editor, original, "vertex_count")
    updateDelta(editor, original, "edge_count")
    for (seg <- editor.segmentationNames) {
      if (original.state.segmentations.contains(seg)) {
        updateDeltas(editor.existingSegmentation(seg), original.segmentation(seg))
      }
    }
  }
  private def updateDelta(editor: ProjectEditor, original: ProjectViewer, name: String): Unit = {
    val before = original.scalars.get(name).map(_.runtimeSafeCast[Long])
    val after = editor.scalars.get(name).map(_.runtimeSafeCast[Long])
    val delta =
      if (before.isEmpty || after.isEmpty || before == after) null
      else graph_operations.ScalarLongDifference.run(after.get, before.get)
    editor.scalars.set(s"!${name}_delta", delta)
  }

  def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    validateParameters(params)
    val before = project.viewer
    apply()
    updateDeltas(project, before)
    import CheckpointRepository._ // For JSON formatters.
    val output = BoxOutputState(
      "project", json.Json.toJson(project.rootState.state).as[json.JsObject])
    Map(context.meta.outputs(0).ofBox(context.box) -> output)
  }

  def toFE: FEOperationMeta = FEOperationMeta(
    id,
    title,
    allParameters.map { param => param.toFE },
    visibleScalars,
    context.meta.categoryID,
    enabled)
  protected def scalars[T: TypeTag] =
    FEOption.list(project.scalarNames[T].toList)
  protected def vertexAttributes[T: TypeTag] =
    FEOption.list(project.vertexAttributeNames[T].toList)
  protected def parentVertexAttributes[T: TypeTag] = {
    FEOption.list(project.asSegmentation.parent.vertexAttributeNames[T].toList)
  }
  protected def edgeAttributes[T: TypeTag] =
    FEOption.list(project.edgeAttributeNames[T].toList)
  protected def segmentations =
    FEOption.list(project.segmentationNames.toList)
  protected def hasVertexSet = FEStatus.assert(project.vertexSet != null, "No vertices.")
  protected def hasNoVertexSet = FEStatus.assert(project.vertexSet == null, "Vertices already exist.")
  protected def hasEdgeBundle = FEStatus.assert(project.edgeBundle != null, "No edges.")
  protected def hasNoEdgeBundle = FEStatus.assert(project.edgeBundle == null, "Edges already exist.")
  protected def isNotSegmentation = FEStatus.assert(!project.isSegmentation,
    "This operation is not available with segmentations.")
  protected def isSegmentation = FEStatus.assert(project.isSegmentation,
    "This operation is only available for segmentations.")

  // TODO: Operations using these must be rewritten with multiple inputs as part of #5724.
  protected def accessibleTableOptions: List[FEOption] = ???
  protected def readableProjectCheckpoints: List[FEOption] = ???
}

// A "ProjectTransformation" takes 1 input project and produces 1 output project.
abstract class ProjectTransformation(context: Operation.Context) extends ProjectOperation(context) {
  assert(
    context.meta.inputs == List(TypedConnection("project", "project")),
    s"A ProjectTransformation must input a single project. $context")
  protected lazy val project = {
    val segPath = SubProject.splitPipedPath(params.getOrElse("apply_to", ""))
    assert(segPath.head == "", s"'apply_to' path must start with separator: $params")
    context.inputs("project").project.offspringEditor(segPath.tail)
  }
  protected def segmentationsRecursively(
    editor: ProjectEditor, prefix: String = ""): Seq[String] = {
    prefix +: editor.segmentationNames.flatMap { seg =>
      segmentationsRecursively(editor.segmentation(seg), prefix + "|" + seg)
    }
  }
  protected override def allParameters = {
    val params = parameters
    // "apply_to" is used to pick the base project or segmentation to apply the operation to.
    assert(
      params.find(_.id == "apply_to").isEmpty, s"$id: 'apply_to' is a reserved parameter name.")
    val segmentations = FEOption.list(segmentationsRecursively(project.rootEditor).toList)
    val applyTo = OperationParams.SegmentationParam(
      "apply_to", "Apply to", segmentations, mandatory = false)
    applyTo :: params
  }
}

// A "ProjectCreation" creates 1 output project from nothingness (no inputs).
abstract class ProjectCreation(context: Operation.Context) extends ProjectOperation(context) {
  assert(context.meta.inputs == List(), s"A ProjectCreation must have no inputs. $context")
  protected lazy val project = new RootProjectEditor(RootProjectState.emptyState)
}
