// BigGraphController includes the request handlers that operate on projects at the metagraph level.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.frontend_operations.{ OperationParams, Operations }
import com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.serving.User
import play.api.libs.json

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.runtime.universe._

case class FEStatus(enabled: Boolean, disabledReason: String = "") {
  def ||(other: => FEStatus) = if (enabled) this else other
  def &&(other: => FEStatus) = if (enabled) other else this
}
object FEStatus {
  val enabled = FEStatus(true)
  def disabled(disabledReason: String) = FEStatus(false, disabledReason)
  def assert(condition: Boolean, disabledReason: => String) =
    if (condition) enabled else disabled(disabledReason)
}

// Something with a display name and an internal ID.
case class FEOption(
  id: String,
  title: String)
object FEOption {
  def regular(optionTitleAndId: String): FEOption = FEOption(optionTitleAndId, optionTitleAndId)
  def special(specialID: String): FEOption = specialOpt(specialID).get
  val TitledCheckpointRE = raw"!checkpoint\(([0-9]*),([^|]*)\)(|\|.*)".r
  private def specialOpt(specialID: String): Option[FEOption] = {
    Option(specialID match {
      case "!unset" => ""
      case "!no weight" => "no weight"
      case "!unit distances" => "unit distances"
      case "!internal id (default)" => "internal id (default)"
      case TitledCheckpointRE(cp, title, suffix) =>
        val time = {
          val df = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm z")
          df.format(new java.util.Date(cp.toLong))
        }
        s"$title$suffix ($time)"
      case _ => null
    }).map(FEOption(specialID, _))
  }
  def titledCheckpoint(cp: String, title: String, suffix: String = ""): FEOption =
    special(s"!checkpoint($cp,$title)$suffix")
  def unpackTitledCheckpoint(id: String): (String, String, String) =
    unpackTitledCheckpoint(id, s"$id does not look like a project checkpoint identifier")
  def unpackTitledCheckpoint(id: String, customError: String): (String, String, String) =
    maybeUnpackTitledCheckpoint(id).getOrElse(throw new AssertionError(customError))
  def maybeUnpackTitledCheckpoint(id: String): Option[(String, String, String)] =
    id match {
      case TitledCheckpointRE(cp, title, suffix) => Some((cp, title, suffix))
      case _ => None
    }

  def fromID(id: String) = specialOpt(id).getOrElse(FEOption.regular(id))
  def list(lst: String*): List[FEOption] = list(lst.toList)
  def list(lst: List[String]): List[FEOption] = lst.map(id => FEOption(id, id))
  val bools = list("true", "false")
  val noyes = list("no", "yes")
  val unset = special("!unset")
  val noWeight = special("!no weight")
  val unitDistances = special("!unit distances")
  val internalId = special("!internal id (default)")
  val jsDataTypes = FEOption.list("double", "string", "vector of doubles", "vector of strings")
}

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

case class FEAttribute(
  id: String,
  title: String,
  typeName: String,
  note: String,
  metadata: Map[String, String],
  canBucket: Boolean,
  canFilter: Boolean,
  isNumeric: Boolean,
  isInternal: Boolean,
  computeProgress: Double)

case class FEScalar(
  id: String,
  title: String,
  typeName: String,
  note: String,
  metadata: Map[String, String],
  isNumeric: Boolean,
  isInternal: Boolean,
  computeProgress: Double,
  errorMessage: Option[String],
  computedValue: Option[graph_operations.DynamicValue])

case class FEProjectListElement(
    name: String,
    objectType: String,
    notes: String = "",
    vertexCount: Option[FEScalar] = None, // Whether the project has vertices defined.
    edgeCount: Option[FEScalar] = None, // Whether the project has edges defined.
    error: Option[String] = None, // If set the project could not be opened.
    // The contents of this depend on the element, e.g. table uses it to
    // store import configuration
    details: Option[json.JsObject] = None) {

  assert(objectType == "table" || objectType == "project" || objectType == "view",
    s"Unrecognized objectType: $objectType")
}

case class FEProject(
  name: String,
  undoOp: String = "", // Name of last operation. Empty if there is nothing to undo.
  redoOp: String = "", // Name of next operation. Empty if there is nothing to redo.
  readACL: String = "",
  writeACL: String = "",
  vertexSet: String = "",
  edgeBundle: String = "",
  notes: String = "",
  scalars: List[FEScalar] = List(),
  vertexAttributes: List[FEAttribute] = List(),
  edgeAttributes: List[FEAttribute] = List(),
  segmentations: List[FESegmentation] = List(),
  opCategories: List[OperationCategory] = List())

case class FESegmentation(
  name: String,
  fullName: String,
  // The connecting edge bundle's GUID.
  belongsTo: String,
  // A Vector[ID] vertex attribute, that contains for each vertex
  // the vector of ids of segments the vertex belongs to.
  equivalentAttribute: FEAttribute)
case class ProjectRequest(name: String)
case class ProjectListRequest(path: String)
case class ProjectSearchRequest(
  basePath: String, // We only search for projects/directories contained (recursively) in this.
  query: String,
  includeNotes: Boolean)
case class ProjectList(
  path: String,
  readACL: String,
  writeACL: String,
  directories: List[String],
  objects: List[FEProjectListElement])
case class OperationCategory(
    title: String, icon: String, color: String, ops: List[FEOperationMeta]) {
  def containsOperation(op: Operation): Boolean = ops.find(_.id == op.id).nonEmpty
}
case class CreateProjectRequest(name: String, notes: String, privacy: String)
case class CreateDirectoryRequest(name: String, privacy: String)
case class DiscardEntryRequest(name: String)

// A request for the execution of a FE operation on a specific project. The project might be
// a non-root project, that is a segmentation (or segmentation of segmentation, etc) of a root
// project. In this case, the project parameter has the format:
// rootProjectName|childSegmentationName|grandChildSegmentationName|...
case class ProjectOperationRequest(project: String, op: FEOperationSpec)
// Represents an operation executed on a subproject. It is only meaningful in the context of
// a root project. path specifies the offspring project in question, e.g. it could be sg like:
// Seq(childSegmentationName, grandChildSegmentationName, ...)
case class SubProjectOperation(path: Seq[String], op: FEOperationSpec)

case class ProjectAttributeFilter(attributeName: String, valueSpec: String)
case class ProjectFilterRequest(
  project: String,
  vertexFilters: List[ProjectAttributeFilter],
  edgeFilters: List[ProjectAttributeFilter])
case class ForkEntryRequest(from: String, to: String)
case class RenameEntryRequest(from: String, to: String, overwrite: Boolean)
case class UndoProjectRequest(project: String)
case class RedoProjectRequest(project: String)
case class ACLSettingsRequest(project: String, readACL: String, writeACL: String)

case class HistoryRequest(project: String)
case class AlternateHistory(
  startingPoint: String, // The checkpoint where to start to apply requests below.
  requests: List[SubProjectOperation])
case class SaveHistoryRequest(
  oldProject: String, // Old project is used to copy ProjectFrame level metadata.
  newProject: String,
  history: AlternateHistory)
case class ProjectHistory(steps: List[ProjectHistoryStep])
case class ProjectHistoryStep(
  request: SubProjectOperation,
  status: FEStatus,
  segmentationsBefore: List[FESegmentation],
  segmentationsAfter: List[FESegmentation],
  opMeta: Option[FEOperationMeta],
  checkpoint: Option[String])

case class OpCategories(categories: List[OperationCategory])

class BigGraphController(val env: SparkFreeEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val entityProgressManager: EntityProgressManager = env.entityProgressManager

  val ops = new Operations(env)

  def projectList(user: serving.User, request: ProjectListRequest): ProjectList = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.path)
    entry.assertReadAllowedFrom(user)
    val dir = entry.asDirectory
    val entries = dir.list
    val (dirs, objects) = entries.partition(_.isDirectory)
    val visibleDirs = dirs.filter(_.readAllowedFrom(user)).map(_.asDirectory)
    val visibleObjectFrames = objects.filter(_.readAllowedFrom(user)).map(_.asObjectFrame)
    ProjectList(
      request.path,
      dir.readACL,
      dir.writeACL,
      visibleDirs.map(_.path.toString).toList,
      visibleObjectFrames.map(_.toListElementFE).toList)
  }

  def projectSearch(user: serving.User, request: ProjectSearchRequest): ProjectList = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.basePath)
    entry.assertReadAllowedFrom(user)
    val dir = entry.asDirectory
    val terms = request.query.split(" ").map(_.toLowerCase)
    val dirs = dir
      .listDirectoriesRecursively
      .filter(_.readAllowedFrom(user))
      .filter { dir =>
        val baseName = dir.path.last.name
        terms.forall(term => baseName.toLowerCase.contains(term))
      }
    val objects = dir
      .listObjectsRecursively
      .filter(_.readAllowedFrom(user))
      .filter { project =>
        val baseName = project.path.last.name
        val notes = project.viewer.state.notes
        terms.forall {
          term =>
            baseName.toLowerCase.contains(term) ||
              (request.includeNotes && notes.toLowerCase.contains(term))
        }
      }

    ProjectList(
      request.basePath,
      dir.readACL,
      dir.writeACL,
      dirs.map(_.path.toString).toList,
      objects.map(_.toListElementFE).toList)
  }

  private def assertNameNotExists(name: String) = {
    assert(!DirectoryEntry.fromName(name).exists, s"Entry '$name' already exists.")
  }

  def createDirectory(user: serving.User, request: CreateDirectoryRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val dir = entry.asNewDirectory()
    dir.setupACL(request.privacy, user)
  }

  def createWorkspace(
    user: serving.User, request: CreateWorkspaceRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val w = entry.asNewWorkspaceFrame()
    w.setupACL(request.privacy, user)
  }

  def discardEntry(
    user: serving.User, request: DiscardEntryRequest): Unit = metaManager.synchronized {

    val p = DirectoryEntry.fromName(request.name)
    p.assertParentWriteAllowedFrom(user)
    p.remove()
  }

  def renameEntry(
    user: serving.User, request: RenameEntryRequest): Unit = metaManager.synchronized {
    if (!request.overwrite) {
      assertNameNotExists(request.to)
    }
    val pFrom = DirectoryEntry.fromName(request.from)
    pFrom.assertParentWriteAllowedFrom(user)
    val pTo = DirectoryEntry.fromName(request.to)
    pTo.assertParentWriteAllowedFrom(user)
    pFrom.copy(pTo)
    pFrom.remove()
  }

  def discardAll(user: serving.User, request: serving.Empty): Unit = metaManager.synchronized {
    assert(user.isAdmin, "Only admins can delete all objects and directories")
    DirectoryEntry.rootDirectory.remove()
  }

  def projectOp(user: serving.User, request: ProjectOperationRequest): Unit = metaManager.synchronized {
    ??? // TODO: Build the workflow.
  }

  def filterProject(user: serving.User, request: ProjectFilterRequest): Unit = metaManager.synchronized {
    val vertexParams = request.vertexFilters.map {
      f => s"filterva-${f.attributeName}" -> f.valueSpec
    }
    val edgeParams = request.edgeFilters.map {
      f => s"filterea-${f.attributeName}" -> f.valueSpec
    }
    projectOp(user, ProjectOperationRequest(
      project = request.project,
      op = FEOperationSpec(
        id = "Filter-by-attributes",
        parameters = (vertexParams ++ edgeParams).toMap)))
  }

  def forkEntry(user: serving.User, request: ForkEntryRequest): Unit = metaManager.synchronized {
    val pFrom = DirectoryEntry.fromName(request.from)
    pFrom.assertReadAllowedFrom(user)
    assertNameNotExists(request.to)
    val pTo = DirectoryEntry.fromName(request.to)
    pTo.assertParentWriteAllowedFrom(user)
    pFrom.copy(pTo)
    if (!pTo.writeAllowedFrom(user)) {
      pTo.writeACL += "," + user.email
    }
  }

  def changeACLSettings(user: serving.User, request: ACLSettingsRequest): Unit = metaManager.synchronized {
    val p = DirectoryEntry.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    // To avoid accidents, a user cannot remove themselves from the write ACL.
    assert(user.isAdmin || p.aclContains(request.writeACL, user),
      s"You cannot forfeit your write access to project $p.")
    // Checking that we only give access to users with read access to all parent directories
    val gotReadAccess = request.readACL.replace(" ", "").split(",").toSet
    val gotWriteAccess = request.writeACL.replace(" ", "").split(",").toSet
    val union = gotReadAccess union gotWriteAccess
    val notAllowed =
      if (p.parent.isEmpty) Set()
      else union.map { email => User(email, false) }.filter(!p.parent.get.readAllowedFrom(_))

    assert(notAllowed.isEmpty,
      s"The following users don't have read access to all of the parent folders: ${notAllowed.mkString(", ")}")

    p.readACL = request.readACL
    p.writeACL = request.writeACL
  }

  def getWorkspace(
    user: serving.User, request: GetWorkspaceRequest): Workspace = metaManager.synchronized {
    val f = DirectoryEntry.fromName(request.name)
    assert(f.exists, s"Project ${request.name} does not exist.")
    f.assertReadAllowedFrom(user)
    f match {
      case f: WorkspaceFrame => f.workspace
      case _ => throw new AssertionError(s"${request.name} is not a workspace.")
    }
  }
}

case class GetWorkspaceRequest(name: String)
case class CreateWorkspaceRequest(name: String, privacy: String)

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

abstract class Operation(metadata: BoxMetadata, context: Operation.Context) {
  implicit val manager = context.manager
  lazy val project = metadata.inputs match {
    case Seq() => new RootProjectEditor(RootProjectState.emptyState)
    case Seq(LocalBoxConnection("project", "project")) => context.inputs("project").project
  }
  val user = context.user
  def id = Operation.titleToID(metadata.operation)
  def title = metadata.operation // Override this to change the display title while keeping the original ID.
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

  def validateAndApply(params: Map[String, String]): Unit = {
    validateParameters(params)
    apply(params)
    project.setLastOperationDesc(summary(params))
    project.setLastOperationRequest(SubProjectOperation(Seq(), FEOperationSpec(id, params)))
  }

  def toFE: FEOperationMeta = FEOperationMeta(
    id,
    title,
    parameters.map { param => param.toFE },
    visibleScalars,
    metadata.category,
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
  def titleToID(title: String) = title.replace(" ", "-")
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

  type Factory = (BoxMetadata, Context) => Operation
  case class Context(
    user: serving.User,
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
    inputs: List[LocalBoxConnection] = List(LocalBoxConnection("project", "project")),
    outputs: List[LocalBoxConnection] = List(LocalBoxConnection("project", "project"))): Unit = {
    val id = Operation.titleToID(title)
    assert(!operations.contains(id), s"$id is already registered.")
    operations(id) = BoxMetadata(category.title, title, inputs, outputs) -> factory
  }

  def getBoxMetadata(id: String) = operations(id)._1

  /*
  def categories(context: Operation.Context, includeDeprecated: Boolean): List[OperationCategory] = {
    val allOps = opsForContext(context)
    val cats = allOps.groupBy(_.category).toList
    cats.filter { x => x._1.visible && (includeDeprecated || x._1.deprecated == false) }
      .sortBy(_._1).map {
        case (cat, ops) =>
          val feOps = ops.map(_.toFE).sortBy(_.title).toList
          cat.toFE(feOps)
      }
  }
  */

  def operationIds = operations.keys.toSeq

  def opById(context: Operation.Context, id: String): Operation = {
    val (metadata, factory) = operations(id)
    factory(metadata, context)
  }

  // Applies the operation specified by op in the given context and returns the
  // applied operation.
  def appliedOp(context: Operation.Context, opSpec: FEOperationSpec): Operation = {
    val op = opById(context, opSpec.id)
    op.validateAndApply(opSpec.parameters)
    op
  }

  /*
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

  def applyAndCheckpoint(context: Operation.Context, opSpec: FEOperationSpec): RootProjectState = {
    val editor = appliedOp(context, opSpec).project
    //updateDeltas(editor.rootEditor, original = context.project.rootViewer)
  }

  def apply(
    user: serving.User,
    subProject: SubProject,
    op: FEOperationSpec): Unit = manager.tagBatch {

    val context = Operation.Context(user, Map())
    subProject.frame.setCheckpoint(applyAndCheckpoint(context, op).checkpoint.get)
  }
  */
}
