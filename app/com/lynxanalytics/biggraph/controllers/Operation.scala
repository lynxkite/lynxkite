// The base classes and mechanisms for frontend operations.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.serving.DownloadFileRequest
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
  color: Option[String] = None,
  description: Option[String] = None)

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
    "segmentation", // One of the segmentations of the current project.
    "visualization", // Describes a two-sided visualization UI state.
    "staleness-check") // Used for checking staleness of parameters.
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

case class FEOperationCategory(
  title: String, icon: String, color: String, browseByDir: Boolean)

abstract class OperationParameterMeta {
  val id: String
  val title: String
  val kind: String
  val defaultValue: String
  val options: List[FEOption]
  val multipleChoice: Boolean
  def payload: Option[json.JsValue] = None

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
  def summary: String
  def getOutputs: Map[BoxOutput, BoxOutputState]
  def toFE: FEOperationMeta
}
object Operation {
  case class Category(
    title: String,
    color: String, // A color class from web/app/styles/operation-toolbox.scss.
    visible: Boolean = true,
    icon: String = "", // Icon class name, or empty for first letter of title.
    index: Int, // Categories are listed in this order on the UI.
    // Browse operations in this category using the dir structure. If true, the UI will display the
    // operations in a tree structure using the '/' character in the operation id as path separator.
    browseByDir: Boolean = false)
      extends Ordered[Category] {
    def compare(that: Category) = this.index compare that.index
    def toFE: FEOperationCategory =
      FEOperationCategory(title, addClass(icon), color, browseByDir)
    // Add main CSS class. E.g. "fa-superpowers" => "fa fa-superpowers".
    private def addClass(cls: String): String = {
      val parts = cls.split("-", 2)
      if (parts.length == 1) cls
      else s"${parts.head} $cls"
    }
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
  def htmlId(name: String) = name.toLowerCase.replaceAll("\\W+", "-").replaceFirst("-+$", "")

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
    implicit class OperationInputTables(operation: Operation) {
      // Returns all tables output by all inputs of this operation.
      def getInputTables()(implicit metaManager: MetaGraphManager): Map[String, ProtoTable] = {
        val inputs = operation.context.inputs
        // TODO: Clean this hack up once we have a standard way of handling N-input boxes.
        val bindInputName = inputs.size > 1 // Whether to bind input names to avoid collisions.
        inputs.flatMap {
          case (inputName, state) if state.isTable => Seq(inputName -> ProtoTable(state.table))
          case (inputName, state) if state.isProject => state.project.viewer.getProtoTables.map {
            case (tableName, proto) =>
              val prefix = if (bindInputName) s"$inputName|" else ""
              s"$prefix$tableName" -> proto
          }
        }
      }
    }
  }
}
import Operation.Implicits._

// OperationRegistry is a simple trait for a class that wants to declare a set of operations.
trait OperationRegistry {
  // The registry maps operation IDs to their constructors.
  val operations = mutable.Map[String, (BoxMetadata, Operation.Factory)]()
  val categories = mutable.Map[String, Operation.Category]()

  // Default icon for operations.
  val defaultIcon = "black_medium_square"

  def registerOp(
    id: String,
    icon: String,
    category: Operation.Category,
    inputs: List[String],
    outputs: List[String],
    factory: Operation.Factory): Unit = {
    assert(!operations.contains(id), s"$id is already registered.")
    assert(
      !categories.contains(category.title) || categories(category.title) == category,
      s"Re-registered category with different value: ${category.title}")
    categories(category.title) = category
    operations(id) = BoxMetadata(
      category.title,
      s"/images/icons/$icon.png",
      category.color,
      id,
      inputs,
      outputs,
      htmlId = Some(Operation.htmlId(id))) -> factory
  }
}

// OperationRepository holds a registry of all operations.
abstract class OperationRepository(env: SparkFreeEnvironment) {
  implicit val metaGraphManager = env.metaGraphManager
  // The registry maps operation IDs to their constructors.
  // "Atomic" operations (as opposed to custom boxes) are simply in a Map.
  protected val atomicOperations: Map[String, (BoxMetadata, Operation.Factory)]
  protected val atomicCategories: Map[String, Operation.Category]

  private def getBox(id: String): (BoxMetadata, Operation.Factory) = {
    if (atomicOperations.contains(id)) {
      atomicOperations(id)
    } else {
      val frame = DirectoryEntry.fromName(id) match {
        case f: WorkspaceFrame => f
        case _ => throw new AssertionError(s"Unknown operation: $id")
      }
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

  private val customBoxesCategory = Operation.Category(
    Workspace.customBoxesCategory,
    "yellow",
    icon = "fa-superpowers",
    index = 999,
    browseByDir = true)

  def getCategories(user: serving.User): List[FEOperationCategory] = {
    (atomicCategories.values.toList :+ customBoxesCategory)
      .filter(_.visible)
      .sorted
      .map(_.toFE)
  }

  def opForBox(
    user: serving.User, box: Box, inputs: Map[String, BoxOutputState],
    workspaceParameters: Map[String, String]) = {
    val (meta, factory) = getBox(box.operationId)
    val context =
      Operation.Context(user, this, box, meta, inputs, workspaceParameters, env.metaGraphManager)
    factory(context)
  }
}

// Defines simple defaults for everything.
abstract class SimpleOperation(protected val context: Operation.Context) extends Operation {
  protected val params = new ParameterHolder(context)
  protected val id = context.meta.operationId
  val title = id
  def summary = title
  def toFE: FEOperationMeta = FEOperationMeta(
    id,
    Operation.htmlId(id),
    params.toFE,
    List(),
    context.meta.categoryId,
    FEStatus.enabled)
  def getOutputs(): Map[BoxOutput, BoxOutputState] = ???
}

// Adds a lot of conveniences for working with projects and tables.
abstract class SmartOperation(context: Operation.Context) extends SimpleOperation(context) {
  implicit val manager = context.manager
  protected val user = context.user
  protected def enabled: FEStatus
  protected def visibleScalars: List[FEScalar] = List()
  protected def apply(): Unit
  protected def safeEnabled: FEStatus =
    util.Try(enabled).recover { case exc => FEStatus.disabled(exc.getMessage) }.get
  protected def help = // Add to notes for help link.
    "<help-popup href=\"" + Operation.htmlId(id) + "\"></help-popup>"

  override protected val params = {
    val params = new ParameterHolder(context)
    // "apply_to_*" is used to pick the base project or segmentation to apply the operation to.
    // An "apply_to_*" parameter is added for each project input.
    val projects = context.meta.inputs.filter(i => context.inputs(i).kind == BoxOutputKind.Project)
    for (input <- projects) {
      val param = "apply_to_" + input
      params += OperationParams.SegmentationParam(
        param, s"Apply to ($input)",
        context.inputs(input).project.segmentationsRecursively)
    }
    params
  }

  // Updates the vertex_count_delta/edge_count_delta scalars after an operation finished.
  protected def updateDeltas(editor: ProjectEditor, original: ProjectViewer): Unit = {
    updateDelta(editor, original, "!vertex_count")
    updateDelta(editor, original, "!edge_count")
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
    editor.scalars.set(s"${name}_delta", delta)
  }

  override def toFE: FEOperationMeta = super.toFE.copy(
    visibleScalars = visibleScalars,
    status = safeEnabled,
    description = context.meta.description)

  protected def projectInput(input: String): ProjectEditor = {
    val param = params("apply_to_" + input)
    val segPath = SubProject.splitPipedPath(param)
    assert(segPath.head == "", s"'apply_to_$input' path must start with separator: $param")
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
          graph_operations.AttributesToTable.run(p.vertexAttributes)
        case BoxOutputKind.Table =>
          tableInput(name)
      }
    }
  }

  protected def columnList(table: Table): List[FEOption] = {
    table.schema.fieldNames.toList.map(n => FEOption(n, n))
  }

  protected def splitParam(param: String): Seq[String] = {
    val p = params(param)
    if (p.isEmpty) Seq()
    else p.split(",", -1).map(_.trim)
  }
}

// A ProjectOutputOperation is an operation that has 1 project-typed output.
abstract class ProjectOutputOperation(context: Operation.Context) extends SmartOperation(context) {
  assert(
    context.meta.outputs == List("project"),
    s"A ProjectOperation must output a project. $context")
  protected lazy val project: ProjectEditor = new RootProjectEditor(RootProjectState.emptyState)

  protected def makeOutput(project: ProjectEditor): Map[BoxOutput, BoxOutputState] = {
    Map(context.box.output(context.meta.outputs(0)) -> BoxOutputState.from(project))
  }

  override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    params.validate()
    assert(enabled.enabled, enabled.disabledReason)
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
    params.validate()
    val before = project.rootEditor.viewer
    assert(enabled.enabled, enabled.disabledReason)
    apply()
    updateDeltas(project.rootEditor, before)
    makeOutput(project)
  }
}

// A DecoratorOperation is an operation that has no input or output and is outside of the
// Metagraph.
abstract class DecoratorOperation(context: Operation.Context) extends SimpleOperation(context) {
  assert(
    context.meta.inputs == List(),
    s"A DecoratorOperation must not have an input. $context")
  assert(
    context.meta.outputs == List(),
    s"A DecoratorOperation must not have an output. $context")
}

abstract class TableOutputOperation(context: Operation.Context) extends SmartOperation(context) {
  assert(
    context.meta.outputs == List("table"),
    s"A TableOutputOperation must output a table. $context")

  protected def makeOutput(t: Table): Map[BoxOutput, BoxOutputState] = {
    Map(context.box.output(context.meta.outputs(0)) -> BoxOutputState.from(t))
  }

  override def apply(): Unit = ???
}

abstract class ImportOperation(context: Operation.Context) extends TableOutputOperation(context) {
  import MetaGraphManager.StringAsUUID
  protected def tableFromGuid(guid: String): Table = manager.table(guid.asUUID)

  // The set of those parameters that affect the resulting table of the import operation.
  // The last_settings parameter is only used to check if the settings are stale. The
  // imported_table is generated from the other parameters and is populated in the frontend so
  // it is easier to also exclude it.
  private def currentSettings = params.toMap - "last_settings" - "imported_table"

  // When the /ajax/importBox is called then the response contains the guid of the resulting table
  // and also this string describing the settings at the moment of the import. On the frontend the
  // table-kind directive gets this response and uses these two strings to populate the
  // "imported_table" and "last_settings" parameters respectively.
  def settingsString(): String = {
    val realParamsJson = json.Json.toJson(currentSettings)
    json.Json.prettyPrint(realParamsJson)
  }

  private def getLastSettings = {
    val lastSettingsString = params.toMap.getOrElse("last_settings", "")
    if (lastSettingsString == "") { Map() }
    else {
      json.Json.parse(lastSettingsString).as[Map[String, String]]
    }
  }

  protected def areSettingsStale(): Boolean = {
    val lastSettings = getLastSettings
    // For not needing to provide the last_settings parameter for testing we are also allowing it to
    // be empty. This doesn't cause problem in practice since in the getOutputs method we first
    // assert if the "imported_table" is not empty. If the "last_settings" parameter is empty then
    // there was no import yet so the first assert on the "imported_table" already fails.
    lastSettings.isEmpty && lastSettings != currentSettings
  }

  override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    params.validate()
    assert(params("imported_table").nonEmpty, "You have to import the data first.")
    assert(!areSettingsStale, "Import settings are stale. " +
      "Please click on the import button to apply the changed settings.")
    makeOutput(tableFromGuid(params("imported_table")))
  }

  def enabled = FEStatus.enabled // Useful default.

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

// An ExportOperation takes a Table as input and returns an ExportResult as output.
abstract class ExportOperation(context: Operation.Context) extends SmartOperation(context) {
  assert(
    context.meta.inputs == List("table"),
    s"An ExportOperation must input a single table. $context")
  assert(
    context.meta.outputs == List("exportResult"),
    s"An ExportOperation must output an ExportResult. $context"
  )

  protected lazy val table = tableInput("table")

  def apply() = ???
  def exportResult: Scalar[String]
  val format: String

  def getParamsToDisplay() = params.toMap + ("format" -> format)

  protected def makeOutput(exportResult: Scalar[String]): Map[BoxOutput, BoxOutputState] = {
    val paramsToDisplay = getParamsToDisplay()
    Map(context.box.output(
      context.meta.outputs(0)) -> BoxOutputState.from(exportResult, paramsToDisplay))
  }

  override def getOutputs() = {
    params.validate()
    makeOutput(exportResult)
  }

  def enabled = FEStatus.enabled
}

abstract class ExportOperationToFile(context: Operation.Context)
    extends ExportOperation(context) {

  override def getOutputs(): Map[BoxOutput, BoxOutputState] = {
    assertWriteAllowed(params("path"))
    super.getOutputs()
  }

  protected def generatePathIfNeeded(path: String): String = {
    if (path == "<auto>") {
      val inputGuid = table.gUID.toString
      val paramsWithInput = params.toMap ++ Map("input" -> inputGuid)
      "DATA$/exports/" + paramsWithInput.hashCode.toString + "." + format
    } else
      path
  }

  private def assertWriteAllowed(path: String) = {
    val genPath = generatePathIfNeeded(path)
    val file = HadoopFile(genPath)
    file.assertWriteAllowedFrom(context.user)
  }

  override def getParamsToDisplay() = params.toMap +
    ("format" -> format, "path" -> generatePathIfNeeded(params("path")))
}

class CustomBoxOperation(
    workspace: Workspace, override val context: Operation.Context) extends SmartOperation(context) {
  override val params = new ParameterHolder(context) // No automatically generated parameters.
  params ++= {
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
    }
  }

  def getParams = params.toMap

  def apply: Unit = ???
  def enabled = FEStatus.enabled

  // Returns a version of the internal workspace in which the input boxes are patched to output the
  // inputs connected to the custom box.
  def connectedWorkspace = {
    workspace.copy(boxes = workspace.boxes.map { box =>
      if (box.operationId == "Input" && box.parameters.contains("name")) {
        new Box(
          box.id, box.operationId, box.parameters, box.x, box.y, box.inputs,
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

  override def getOutputs = {
    val ws = connectedWorkspace
    val states = ws.context(context.user, context.ops, params.toMap).allStates
    val byOutput = ws.boxes.flatMap { box =>
      if (box.operationId == "Output" && box.parameters.contains("name"))
        Some(box.parameters("name") -> states(box.inputs("output")))
      else None
    }.toMap
    context.meta.outputs.map(output => context.box.output(output) -> byOutput(output)).toMap
  }
}
