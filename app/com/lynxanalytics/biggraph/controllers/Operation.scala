// The base classes and mechanisms for frontend operations.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment

import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_api._
import play.api.libs.json
import org.apache.spark

import scala.collection.mutable
import scala.reflect.runtime.universe._

case class FEOperationMeta(
  id: String,
  htmlId: String,
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
    "code", // An editor with a language option for highlights.
    "model", // A special kind to set model parameters.
    "imported-table", // A table importing button.
    "parameters", // A whole section defining the parameters of an operation.
    "segmentation") // One of the segmentations of the current project.
}

case class FEOperationParameterMeta(
    id: String,
    title: String,
    kind: String, // Special rendering on the UI.
    defaultValue: String,
    options: List[FEOption],
    multipleChoice: Boolean,
    payload: Option[json.JsValue]) { // A custom JSON serialized value to transfer to the UI

  require(
    kind.isEmpty || FEOperationParameterMeta.validKinds.contains(kind),
    s"'$kind' is not a valid parameter type")
  if (kind == "tag-list") require(multipleChoice, "multipleChoice is required for tag-list")
}

case class CustomOperationParameterMeta(
    id: String,
    kind: String,
    defaultValue: String) {
  assert(
    CustomOperationParameterMeta.validKinds.contains(kind),
    s"'$kind' is not a valid parameter type.")
}
object CustomOperationParameterMeta {
  val validKinds = List(
    "text",
    "boolean",
    "code",
    "vertexattribute",
    "edgeattribute",
    "segmentation",
    "scalar",
    "column")
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
  val payload: Option[json.JsValue] = None

  // Asserts that the value is valid, otherwise throws an AssertionException.
  def validate(value: String): Unit
  def toFE = FEOperationParameterMeta(
    id, title, kind, defaultValue, options, multipleChoice, payload)
}

// An Operation is the computation that a Box represents in a workspace.
// They are registered in an OperationRegistry with a factory function that takes an
// Operation.Context parameter. Operations are short-lived and created for a specific input.
trait Operation {
  protected val context: Operation.Context
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
    ops: OperationRepository,
    box: Box,
    meta: BoxMetadata,
    inputs: Map[String, BoxOutputState],
    workspaceParameters: Map[String, String],
    manager: MetaGraphManager)

  // Turns an operation name into a valid HTML identifier.
  def htmlID(name: String) = name.toLowerCase.replaceAll("\\W+", "-").replaceFirst("-+$", "")

  // Adds a bunch of utility methods to projects that make it easier to write operations.
  object Implicits {
    implicit class OperationProject(project: ProjectEditor) {
      def scalarList[T: TypeTag] =
        FEOption.list(project.scalarNames[T].toList)
      def vertexAttrList[T: TypeTag] =
        FEOption.list(project.vertexAttributeNames[T].toList)
      def parentVertexAttrList[T: TypeTag] = {
        FEOption.list(project.asSegmentation.parent.vertexAttributeNames[T].toList)
      }
      def edgeAttrList[T: TypeTag] =
        FEOption.list(project.edgeAttributeNames[T].toList)
      def segmentationList =
        FEOption.list(project.segmentationNames.toList)
      def hasVertexSet = FEStatus.assert(project.vertexSet != null, "No vertices.")
      def hasEdgeBundle = FEStatus.assert(project.edgeBundle != null, "No edges.")
      def hasSegmentation = FEStatus.assert(project.segmentations.nonEmpty, "No segmentations.")
      def assertNotSegmentation = FEStatus.assert(!project.isSegmentation,
        "This operation is not available for segmentations.")
      def assertSegmentation = FEStatus.assert(project.isSegmentation,
        "This operation is only available for segmentations.")

      protected def segmentationsRecursively(
        editor: ProjectEditor, prefix: String = ""): Seq[String] = {
        prefix +: editor.segmentationNames.flatMap { seg =>
          segmentationsRecursively(editor.segmentation(seg), prefix + "|" + seg)
        }
      }
      def segmentationsRecursively: List[FEOption] =
        List(FEOption("", "Main project")) ++
          FEOption.list(
            segmentationsRecursively(project.rootEditor)
              .toList
              .filter(_ != ""))
    }
    implicit class OperationTable(table: Table) {
      def columnList = FEOption.list(table.schema.map(_.name).toList)
    }
  }
}
import Operation.Implicits._

// OperationRegistry is a simple trait for a class that wants to declare a set of operations.
trait OperationRegistry {
  // The registry maps operation IDs to their constructors.
  val operations = mutable.Map[String, (BoxMetadata, Operation.Factory)]()
  def registerOp(
    id: String,
    category: Operation.Category,
    inputs: List[String],
    outputs: List[TypedConnection],
    factory: Operation.Factory): Unit = {
    // TODO: Register category somewhere.
    assert(!operations.contains(id), s"$id is already registered.")
    operations(id) = BoxMetadata(category.title, id, inputs, outputs) -> factory
  }
}

// OperationRepository holds a registry of all operations.
abstract class OperationRepository(env: SparkFreeEnvironment) {
  implicit val metaGraphManager = env.metaGraphManager
  // The registry maps operation IDs to their constructors.
  // "Atomic" operations (as opposed to custom boxes) are simply in a Map.
  protected val atomicOperations: Map[String, (BoxMetadata, Operation.Factory)]

  private def getBox(id: String): (BoxMetadata, Operation.Factory) = {
    if (atomicOperations.contains(id)) {
      atomicOperations(id)
    } else {
      val frame = DirectoryEntry.fromName(id).asInstanceOf[WorkspaceFrame]
      val ws = frame.workspace
      (ws.getBoxMetadata(frame.path.toString), new CustomBoxOperation(ws, _))
    }
  }

  def getBoxMetadata(id: String) = getBox(id)._1

  def atomicOperationIds = atomicOperations.keys.toSeq.sorted

  def operationIds(user: serving.User) = {
    val customBoxes = DirectoryEntry.fromName("").asDirectory
      .listObjectsRecursively
      .filter(_.readAllowedFrom(user))
      .collect { case wsf: WorkspaceFrame => wsf }
      .map(_.path.toString).toSet
    val atomicBoxes = atomicOperations.keySet
    (atomicBoxes ++ customBoxes).toSeq.sorted
  }

  def opForBox(
    user: serving.User, box: Box, inputs: Map[String, BoxOutputState],
    workspaceParameters: Map[String, String]) = {
    val (meta, factory) = getBox(box.operationID)
    val context =
      Operation.Context(user, this, box, meta, inputs, workspaceParameters, env.metaGraphManager)
    factory(context)
  }
}

// A base class with some conveniences for working with projects and tables.
trait BasicOperation extends Operation {
  implicit val manager = context.manager
  protected val user = context.user
  protected val id = context.meta.operationID
  protected val title = id
  // Parameters without default values:
  protected val parametricValues = context.box.parametricParameters.map {
    case (name, value) =>
      val result = com.lynxanalytics.sandbox.ScalaScript.run(
        "s\"\"\"" + value + "\"\"\"",
        context.workspaceParameters)
      name -> result
  }
  protected val paramValues = context.box.parameters ++ parametricValues
  // Parameters with default values:
  protected def params =
    parameters
      .map {
        paramMeta => (paramMeta.id, paramMeta.defaultValue)
      }
      .toMap ++ paramValues
  protected def parameters: List[OperationParameterMeta]
  protected def visibleScalars: List[FEScalar] = List()
  def summary = title

  protected def apply(): Unit
  protected def help = // Add to notes for help link.
    "<help-popup href=\"" + Operation.htmlID(id) + "\"></help-popup>"

  protected def validateParameters(values: Map[String, String]): Unit = {
    val paramIds = allParameters.map { param => param.id }.toSet
    val extraIds = values.keySet &~ paramIds
    assert(extraIds.size == 0,
      s"""Extra parameters found: ${extraIds.mkString(", ")} is not in ${paramIds.mkString(", ")}""")
    for (param <- allParameters) {
      if (values.contains(param.id)) {
        param.validate(values(param.id))
      }
    }
  }

  // Updates the vertex_count_delta/edge_count_delta scalars after an operation finished.
  protected def updateDeltas(editor: ProjectEditor, original: ProjectViewer): Unit = {
    updateDelta(editor, original, "vertex_count")
    updateDelta(editor, original, "edge_count")
    for (seg <- editor.segmentationNames) {
      if (original.state.segmentations.contains(seg)) {
        updateDeltas(editor.existingSegmentation(seg), original.segmentation(seg))
      }
    }
  }
  protected def updateDelta(editor: ProjectEditor, original: ProjectViewer, name: String): Unit = {
    val before = original.scalars.get(name).map(_.runtimeSafeCast[Long])
    val after = editor.scalars.get(name).map(_.runtimeSafeCast[Long])
    val delta =
      if (before.isEmpty || after.isEmpty || before == after) null
      else graph_operations.ScalarLongDifference.run(after.get, before.get)
    editor.scalars.set(s"!${name}_delta", delta)
  }

  def toFE: FEOperationMeta = FEOperationMeta(
    id,
    Operation.htmlID(id),
    allParameters.map { param => param.toFE },
    visibleScalars,
    context.meta.categoryID,
    enabled)

  protected def projectInput(input: String): ProjectEditor = {
    val segPath = SubProject.splitPipedPath(paramValues.getOrElse("apply_to_" + input, ""))
    assert(segPath.head == "", s"'apply_to_$input' path must start with separator: $paramValues")
    context.inputs(input).project.offspringEditor(segPath.tail)
  }

  protected def tableInput(input: String): Table = {
    context.inputs(input).table
  }

  protected def tableLikeInput(input: String) = new TableLikeInput(input)

  class TableLikeInput(name: String) {
    val input = context.inputs(name)
    def asProject = {
      input.kind match {
        case BoxOutputKind.Project =>
          projectInput(name)
        case BoxOutputKind.Table =>
          import graph_util.Scripting._
          val t = tableInput(name).toAttributes
          val project = new RootProjectEditor(RootProjectState.emptyState)
          project.vertexSet = t.ids
          project.vertexAttributes = t.columns.mapValues(_.entity)
          project
      }
    }

    def asTable = {
      input.kind match {
        case BoxOutputKind.Project =>
          val p = projectInput(name)
          ??? // TODO: Do it with AttributesToTable.
        case BoxOutputKind.Table =>
          tableInput(name)
      }
    }
  }

  protected def columnList(table: Table): List[FEOption] = {
    table.schema.fieldNames.toList.map(n => FEOption(n, n))
  }

  protected def reservedParameter(reserved: String): Unit = {
    assert(
      parameters.find(_.id == reserved).isEmpty, s"$id: '$reserved' is a reserved parameter name.")
  }

  protected def allParameters: List[OperationParameterMeta] = {
    // "apply_to_*" is used to pick the base project or segmentation to apply the operation to.
    // An "apply_to_*" parameter is added for each project input.
    context.inputs.filter(_._2.kind == BoxOutputKind.Project).keys.toList.sorted.map { input =>
      val param = "apply_to_" + input
      reservedParameter(param)
      OperationParams.SegmentationParam(
        param, s"Apply to ($input)",
        context.inputs(input).project.segmentationsRecursively)
    } ++ parameters
  }

  protected def splitParam(param: String): Seq[String] = {
    val p = params(param)
    if (p.isEmpty) Seq()
    else p.split(",", -1).map(_.trim)
  }
}

// A ProjectOutputOperation is an operation that has 1 project-typed output.
abstract class ProjectOutputOperation(
    protected val context: Operation.Context) extends BasicOperation {
  assert(
    context.meta.outputs == List(TypedConnection("project", BoxOutputKind.Project)),
    s"A ProjectOperation must output a project. $context")
  protected lazy val project: ProjectEditor = new RootProjectEditor(RootProjectState.emptyState)

  protected def makeOutput(project: ProjectEditor): Map[BoxOutput, BoxOutputState] = {
    Map(context.meta.outputs(0).ofBox(context.box) -> BoxOutputState.from(project))
  }

  override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    validateParameters(params)
    apply()
    makeOutput(project)
  }
}

// A "ProjectTransformation" takes 1 input project and produces 1 output project.
abstract class ProjectTransformation(
    context: Operation.Context) extends ProjectOutputOperation(context) {
  assert(
    context.meta.inputs == List("project"),
    s"A ProjectTransformation must input a single project. $context")
  override lazy val project = projectInput("project")
  override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    validateParameters(params)
    val before = project.rootEditor.viewer
    apply()
    updateDeltas(project.rootEditor, before)
    makeOutput(project)
  }
}

// A MinimalOperation defines simple defaults for everything.
abstract class MinimalOperation(
    protected val context: Operation.Context) extends Operation {
  protected def parameters: List[OperationParameterMeta] = List()
  protected val id = context.meta.operationID
  val title = id
  def summary = title
  def toFE: FEOperationMeta = FEOperationMeta(
    id,
    Operation.htmlID(id),
    parameters.map { param => param.toFE },
    List(),
    context.meta.categoryID,
    enabled)
  def getOutputs() = ???
  def enabled = FEStatus.enabled
}

// A DecoratorOperation is an operation that has no input or output and is outside of the
// Metagraph.
abstract class DecoratorOperation(context: Operation.Context) extends MinimalOperation(context) {
  assert(
    context.meta.inputs == List(),
    s"A DecoratorOperation must not have an input. $context")
  assert(
    context.meta.outputs == List(),
    s"A DecoratorOperation must not have an output. $context")
}

abstract class TableOutputOperation(
    protected val context: Operation.Context) extends BasicOperation {
  assert(
    context.meta.outputs == List(TypedConnection("table", BoxOutputKind.Table)),
    s"A TableOutputOperation must output a table. $context")

  protected def makeOutput(t: Table): Map[BoxOutput, BoxOutputState] = {
    Map(context.meta.outputs(0).ofBox(context.box) -> BoxOutputState.from(t))
  }
}

abstract class ImportOperation(context: Operation.Context) extends TableOutputOperation(context) {
  import MetaGraphManager.StringAsUUID
  protected def tableFromParam(name: String): Table = manager.table(params(name).asUUID)

  override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    validateParameters(params)
    assert(params("imported_table").nonEmpty, "You have to import the data first.")
    makeOutput(tableFromParam("imported_table"))
  }

  def enabled = FEStatus.enabled // Useful default.

  override def apply(): Unit = ???

  // Called by /ajax/importBox to create the table that is passed in "imported_table".
  def getDataFrame(context: spark.sql.SQLContext): spark.sql.DataFrame = {
    val importedColumns = splitParam("imported_columns")
    val limit = params("limit")
    val query = params("sql")
    val raw = getRawDataFrame(context)
    val partial = if (importedColumns.isEmpty) raw else {
      val columns = importedColumns.map(spark.sql.functions.column(_))
      raw.select(columns: _*)
    }
    val limited = if (limit.isEmpty) partial else partial.limit(limit.toInt)
    val queried = if (query.isEmpty) limited else {
      DataManager.sql(context, query, List("this" -> limited))
    }
    queried
  }

  def getRawDataFrame(context: spark.sql.SQLContext): spark.sql.DataFrame
}

class CustomBoxOperation(
    workspace: Workspace, val context: Operation.Context) extends BasicOperation {
  lazy val parameters = {
    val custom = workspace.parametersMeta
    val tables = context.inputs.values.collect { case i if i.isTable => i.table }
    val projects = context.inputs.values.collect { case i if i.isProject => i.project }
    custom.map { p =>
      val id = p.id
      val dv = p.defaultValue
      import OperationParams._
      p.kind match {
        case "text" => Param(id, id, dv)
        case "boolean" => Choice(id, id, FEOption.bools)
        case "code" => Code(id, id, "plain_text", dv)
        case "vertexattribute" => Choice(id, id, projects.flatMap(_.vertexAttrList).toList)
        case "edgeattribute" => Choice(id, id, projects.flatMap(_.edgeAttrList).toList)
        case "segmentation" => Choice(id, id, projects.flatMap(_.segmentationList).toList)
        case "scalar" => Choice(id, id, projects.flatMap(_.scalarList).toList)
        case "column" => Choice(id, id, tables.flatMap(_.columnList).toList)
      }
    }.toList
  }

  def apply: Unit = ???
  def enabled = FEStatus.enabled
  override def allParameters = parameters

  // Returns a version of the internal workspace in which the input boxes are patched to output the
  // inputs connected to the custom box.
  def connectedWorkspace = {
    workspace.copy(boxes = workspace.boxes.map { box =>
      if (box.operationID == "Input box" && box.parameters.contains("name")) {
        new Box(
          box.id, box.operationID, box.parameters, box.x, box.y, box.inputs,
          box.parametricParameters) {
          override def execute(
            ctx: WorkspaceExecutionContext,
            inputStates: Map[String, BoxOutputState]): Map[BoxOutput, BoxOutputState] = {
            Map(this.output("input") -> context.inputs(this.parameters("name")))
          }
        }
      } else box
    })
  }

  def getOutputs = {
    val ws = connectedWorkspace
    val states = ws.context(context.user, context.ops, params).allStates
    val byOutput = ws.boxes.flatMap { box =>
      if (box.operationID == "Output box" && box.parameters.contains("name"))
        Some(box.parameters("name") -> states(box.inputs("output")))
      else None
    }.toMap
    context.meta.outputs.map(output => output.ofBox(context.box) -> byOutput(output.id)).toMap
  }
}
