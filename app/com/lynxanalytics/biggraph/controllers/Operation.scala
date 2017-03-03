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
  description: String = "",
  color: Option[String] = None)

object FEOperationParameterMeta {
  val validKinds = Seq(
    "default", // A simple textbox.
    "choice", // A drop down box.
    "file", // Simple textbox with file upload button.
    "tag-list", // A variation of "multipleChoice" with a more concise, horizontal design.
    "code", // JavaScript code
    "model", // A special kind to set model parameters.
    "table") // A table.

  val choiceKinds = Set("choice", "tag-list", "table")
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
    title: String, icon: String, color: String, ops: List[FEOperationMeta]) {
  def containsOperation(op: Operation): Boolean = ops.find(_.id == op.id).nonEmpty
}

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

abstract class Operation(context: Operation.Context) {
  assert(
    context.meta.outputs == List(TypedConnection("project", "project")),
    s"A ProjectOperation must output a project. $context")
  implicit val manager = context.manager
  lazy val project =
    if (context.inputs == Map()) new RootProjectEditor(RootProjectState.emptyState)
    else context.inputs("project").project
  val user = context.user
  def id = Operation.titleToID(context.meta.operationID)
  def title = context.box.operationID // Override this to change the display title while keeping the original ID.
  val description = "" // Override if description is dynamically generated.
  def parameters: List[OperationParameterMeta]
  def visibleScalars: List[FEScalar] = List()
  def enabled: FEStatus
  // A summary of the operation, to be displayed on the UI.
  def summary(params: Map[String, String]): String = title

  protected def apply(params: Map[String, String]): Unit
  protected def help = "<help-popup href=\"" + id + "\"></help-popup>" // Add to notes for help link.

  def validateParameters(values: Map[String, String]): Unit = {
    val paramIds = parameters.map { param => param.id }.toSet
    val extraIds = values.keySet &~ paramIds
    assert(extraIds.size == 0,
      s"""Extra parameters found: ${extraIds.mkString(", ")} is not in ${paramIds.mkString(", ")}""")
    val mandatoryParamIds =
      parameters.filter(_.mandatory).map { param => param.id }.toSet
    val missingIds = mandatoryParamIds &~ values.keySet
    assert(missingIds.size == 0, s"""Missing parameters: ${missingIds.mkString(", ")}""")
    for (param <- parameters) {
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

  def getOutputs(params: Map[String, String]): Map[BoxOutput, BoxOutputState] = {
    // This is a project-specific implementation. This should go in a subclass once we have other
    // (non-project) operations.
    validateParameters(params)
    val before = project.viewer
    apply(params)
    updateDeltas(project, before)
    import CheckpointRepository._ // For JSON formatters.
    val output = BoxOutputState("project", json.Json.toJson(project.state).as[json.JsObject])
    Map(context.meta.outputs(0).ofBox(context.box) -> output)
  }

  def toFE: FEOperationMeta = FEOperationMeta(
    id,
    title,
    parameters.map { param => param.toFE },
    visibleScalars,
    context.meta.categoryID,
    enabled,
    description)
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
  // All projects that the user has read access to.
  protected def readableProjectCheckpoints(implicit manager: MetaGraphManager): List[FEOption] = {
    Operation.allObjects(user)
      .filter(_.isProject)
      .filter(_.checkpoint.nonEmpty)
      .map(_.asProjectFrame)
      .map(project => FEOption.titledCheckpoint(project.checkpoint, project.name))
      .toList
  }

  // All tables that the user has read access to.
  private def readableGlobalTableOptions(implicit manager: MetaGraphManager): List[FEOption] = {
    Operation.allObjects(user)
      .filter(_.checkpoint.nonEmpty)
      .flatMap {
        projectOrTable =>
          projectOrTable.viewer.allAbsoluteTablePaths
            .map(_.toGlobal(projectOrTable.checkpoint, projectOrTable.name).toFE)
            // If it is an imported table which is not in a project then it looks nicer from the user's point
            // of view if there is no |vertices suffix in the title of the FEOpt (what is shown to the user)
            .map { FEOpt =>
              if (projectOrTable.isTable) FEOpt.copy(title = FEOpt.title.replace("|vertices", "")) else FEOpt
            }
      }.toList.sortBy(_.title)
  }

  protected def accessibleTableOptions(implicit manager: MetaGraphManager): List[FEOption] = {
    val viewer = project.viewer
    val localPaths = viewer.allRelativeTablePaths
    val localAbsolutePaths = localPaths.map(_.toAbsolute(viewer.offspringPath)).toSet
    val absolutePaths =
      viewer.rootViewer.allAbsoluteTablePaths.filter(!localAbsolutePaths.contains(_))
    (localPaths ++ absolutePaths).toList.map(_.toFE) ++ readableGlobalTableOptions
  }
}
object Operation {
  def titleToID(title: String) = title
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

  def allObjects(user: serving.User)(implicit manager: MetaGraphManager): Seq[ObjectFrame] = {
    val objects = DirectoryEntry.rootDirectory.listObjectsRecursively
    val readable = objects.filter(_.readAllowedFrom(user))
    // Do not list internal project names (starting with "!").
    readable.filterNot(_.name.startsWith("!"))
  }
}

abstract class OperationRepository(env: SparkFreeEnvironment) {
  implicit lazy val manager = env.metaGraphManager

  // The registry maps operation IDs to their constructors.
  private val operations = mutable.Map[String, (BoxMetadata, Operation.Factory)]()
  def register(
    title: String,
    category: Operation.Category,
    factory: Operation.Factory,
    inputs: List[TypedConnection] = List(TypedConnection("project", "project")),
    outputs: List[TypedConnection] = List(TypedConnection("project", "project"))): Unit = {
    val id = Operation.titleToID(title)
    assert(!operations.contains(id), s"$id is already registered.")
    operations(id) = BoxMetadata(category.title, title, inputs, outputs) -> factory
  }

  def getBoxMetadata(id: String) = operations(id)._1

  def operationIds = operations.keys.toSeq

  def opForBox(user: serving.User, box: Box, inputs: Map[String, BoxOutputState]) = {
    val (meta, factory) = operations(box.operationID)
    val context = Operation.Context(user, box, meta, inputs, manager)
    factory(context)
  }
}
