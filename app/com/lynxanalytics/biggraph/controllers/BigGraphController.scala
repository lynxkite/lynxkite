// BigGraphController includes the request handlers that operate on projects at the metagraph level.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.groovy
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.frontend_operations.{ Operations, OperationParams }

import java.util.regex.Pattern
import play.api.libs.json
import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.matching.Regex

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
case class UIValue(
  id: String,
  title: String)
object UIValue {
  def fromEntity(e: MetaGraphEntity): UIValue = UIValue(e.gUID.toString, e.toStringStruct.toString)
  def list(list: List[String]) = list.map(id => UIValue(id, id))
}

case class UIValues(values: List[UIValue])

case class FEOperationMeta(
  id: String,
  title: String,
  parameters: List[FEOperationParameterMeta],
  category: String = "",
  status: FEStatus = FEStatus.enabled,
  description: String = "")

case class FEOperationParameterMeta(
    id: String,
    title: String,
    kind: String, // Special rendering on the UI.
    defaultValue: String,
    options: List[UIValue],
    multipleChoice: Boolean) {

  val validKinds = Seq(
    "default", // A simple textbox.
    "choice", // A drop down box.
    "file", // Simple textbox with file upload button.
    "tag-list") // A variation of "multipleChoice" with a more concise, horizontal design.
  require(kind.isEmpty || validKinds.contains(kind), s"'$kind' is not a valid parameter type")
  if (kind == "tag-list") require(multipleChoice, "multipleChoice is required for tag-list")
}

case class FEOperationSpec(
  id: String,
  parameters: Map[String, String])

abstract class FEOperation {
  val id: String = getClass.getName
  val title: String
  val category: String
  val parameters: List[FEOperationParameterMeta]
  def apply(params: Map[String, String]): Unit
}

case class FEAttribute(
  id: String,
  title: String,
  typeName: String,
  canBucket: Boolean,
  canFilter: Boolean,
  isNumeric: Boolean,
  isInternal: Boolean)

case class FEProjectListElement(
  name: String,
  notes: String = "",
  vertexCount: Option[FEAttribute] = None, // Whether the project has vertices defined.
  edgeCount: Option[FEAttribute] = None, // Whether the project has edges defined.
  error: Option[String] = None) // If set the project could not be opened.

case class FEProject(
  name: String,
  undoOp: String = "", // Name of last operation. Empty if there is nothing to undo.
  redoOp: String = "", // Name of next operation. Empty if there is nothing to redo.
  readACL: String = "",
  writeACL: String = "",
  vertexSet: String = "",
  edgeBundle: String = "",
  notes: String = "",
  scalars: List[FEAttribute] = List(),
  vertexAttributes: List[FEAttribute] = List(),
  edgeAttributes: List[FEAttribute] = List(),
  segmentations: List[FESegmentation] = List(),
  opCategories: List[OperationCategory] = List())

case class FESegmentation(
  name: String,
  fullName: String,
  // The connecting edge bundle.
  belongsTo: UIValue,
  // A Vector[ID] vertex attribute, that contains for each vertex
  // the vector of ids of segments the vertex belongs to.
  equivalentAttribute: UIValue)
case class ProjectRequest(name: String)
case class ProjectListRequest(path: String)
case class ProjectList(path: String, directories: List[String], projects: List[FEProjectListElement])
case class OperationCategory(
    title: String, icon: String, color: String, ops: List[FEOperationMeta]) {
  def containsOperation(op: Operation): Boolean = ops.find(_.id == op.id).nonEmpty
}
case class CreateProjectRequest(name: String, notes: String, privacy: String)
case class CreateDirectoryRequest(name: String)
case class DiscardDirectoryRequest(name: String)

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
case class ForkDirectoryRequest(from: String, to: String)
case class RenameDirectoryRequest(from: String, to: String)
case class UndoProjectRequest(project: String)
case class RedoProjectRequest(project: String)
case class ProjectSettingsRequest(project: String, readACL: String, writeACL: String)

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
  opCategoriesBefore: List[OperationCategory],
  checkpoint: Option[String])

case class SaveWorkflowRequest(
  workflowName: String,
  stepsAsGroovy: String,
  description: String)

case class SavedWorkflow(
    stepsAsGroovy: String,
    author: String,
    description: String) {
  @transient lazy val prettyJson: String = SavedWorkflow.asPrettyJson(this)
}
object SavedWorkflow {
  implicit val fSavedWorkflow = json.Json.format[SavedWorkflow]
  def asPrettyJson(wf: SavedWorkflow): String = json.Json.prettyPrint(json.Json.toJson(wf))
  def fromJson(js: String): SavedWorkflow = json.Json.parse(js).as[SavedWorkflow]
}

object BigGraphController {
  val workflowsRoot: SymbolPath = SymbolPath("workflows")

  val dirtyOperationError = "Dirty operations are not allowed in history"
}
class BigGraphController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager

  val ops = new Operations(env)

  def projectList(user: serving.User, request: ProjectListRequest): ProjectList = metaManager.synchronized {
    val dir = ProjectDirectory.fromName(request.path)
    dir.assertReadAllowedFrom(user)
    val (dirs, projects) = dir.listDirectoriesAndProjects
    val visibleDirs = dirs.filter(_.readAllowedFrom(user))
    val visible = projects.filter(_.readAllowedFrom(user))
    ProjectList(
      request.path,
      visibleDirs.map(_.path.last.name).toList,
      visible.map(_.toListElementFE).toList)
  }

  def project(user: serving.User, request: ProjectRequest): FEProject = metaManager.synchronized {
    val p = SubProject.parsePath(request.name)
    p.frame.assertReadAllowedFrom(user)
    val context = Operation.Context(user, p.viewer)
    val categories = ops.categories(context)
    // Utility operations are made available through dedicated UI elements.
    // Let's hide them from the project operation toolbox to avoid confusion.
    val nonUtilities = categories.filter(_.icon != "wrench")
    p.toFE.copy(opCategories = nonUtilities)
  }

  private def assertNameNotExists(name: String) = {
    assert(!ProjectDirectory.fromName(name).exists, s"Project $name already exists.")
  }

  def createProject(user: serving.User, request: CreateProjectRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val p = ProjectFrame.fromName(request.name)
    request.privacy match {
      case "private" =>
        p.writeACL = user.email
        p.readACL = user.email
      case "public-read" =>
        p.writeACL = user.email
        p.readACL = "*"
      case "public-write" =>
        p.writeACL = "*"
        p.readACL = "*"
    }
    p.initialize
    if (request.notes != "") {
      ops.apply(user, p.subproject, Operations.addNotesOperation(request.notes))
    }
  }

  def createDirectory(user: serving.User, request: CreateDirectoryRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val d = ProjectDirectory.fromName(request.name)
    d.readACL = "*"
    d.writeACL = "*"
  }

  def discardDirectory(user: serving.User, request: DiscardDirectoryRequest): Unit = metaManager.synchronized {
    val p = ProjectDirectory.fromName(request.name)
    p.assertWriteAllowedFrom(user)
    p.remove()
  }

  def renameDirectory(user: serving.User, request: RenameDirectoryRequest): Unit = metaManager.synchronized {
    val p = ProjectDirectory.fromName(request.from)
    p.assertWriteAllowedFrom(user)
    assertNameNotExists(request.to)
    p.copy(ProjectDirectory.fromName(request.to))
    p.remove()
  }

  def discardAll(user: serving.User, request: serving.Empty): Unit = metaManager.synchronized {
    assert(user.isAdmin, "Only admins can delete all projects and directories")
    ProjectDirectory.rootDirectory.remove()
  }

  def projectOp(user: serving.User, request: ProjectOperationRequest): Unit = metaManager.synchronized {
    val p = SubProject.parsePath(request.project)
    p.frame.assertWriteAllowedFrom(user)
    ops.apply(user, p, request.op)
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

  def forkDirectory(user: serving.User, request: ForkDirectoryRequest): Unit = metaManager.synchronized {
    val p1 = ProjectDirectory.fromName(request.from)
    p1.assertReadAllowedFrom(user)
    assertNameNotExists(request.to)
    val p2 = ProjectDirectory.fromName(request.to)
    p1.copy(p2)
    if (!p2.writeAllowedFrom(user)) {
      p2.writeACL += "," + user.email
    }
  }

  def undoProject(user: serving.User, request: UndoProjectRequest): Unit = metaManager.synchronized {
    val p = ProjectFrame.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    p.undo()
  }

  def redoProject(user: serving.User, request: RedoProjectRequest): Unit = metaManager.synchronized {
    val p = ProjectFrame.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    p.redo()
  }

  def changeProjectSettings(user: serving.User, request: ProjectSettingsRequest): Unit = metaManager.synchronized {
    val p = ProjectFrame.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    // To avoid accidents, a user cannot remove themselves from the write ACL.
    assert(user.isAdmin || p.aclContains(request.writeACL, user),
      s"You cannot forfeit your write access to project $p.")
    p.readACL = request.readACL
    p.writeACL = request.writeACL
  }

  def getHistory(user: serving.User, request: HistoryRequest): ProjectHistory = {
    val checkpoint = metaManager.synchronized {
      val p = ProjectFrame.fromName(request.project)
      p.assertReadAllowedFrom(user)
      p.checkpoint
    }
    validateHistory(user, AlternateHistory(checkpoint, List()))
  }

  // Returns the history of a state appended with a given history suffix.
  @tailrec
  private def stateHistory(
    user: serving.User,
    lastState: RootProjectState,
    historyAfter: List[ProjectHistoryStep] = List()): List[ProjectHistoryStep] = {
    val prevCheckpointOpt = lastState.previousCheckpoint
    if (prevCheckpointOpt.isEmpty) historyAfter
    else {
      val prevState = metaManager.checkpointRepo.readCheckpoint(prevCheckpointOpt.get)
      val step =
        historyStep(
          user, prevState, lastState.lastOperationRequest.get, Some(lastState))._2
      stateHistory(user, prevState, step :: historyAfter)
    }
  }

  // Simulates an operation sequence starting from a given state. Returns the list of
  // states reached by the operations and the corresponding ProjectHistorySteps.
  private def extendedHistory(
    user: serving.User,
    start: RootProjectState,
    operations: List[SubProjectOperation]): Stream[(RootProjectState, ProjectHistoryStep)] = {
    if (operations.isEmpty) Stream()
    else {
      val (nextState, step) = historyStep(user, start, operations.head, None)
      (nextState, step) #:: extendedHistory(user, nextState, operations.tail)
    }
  }

  // Tries to execute the requested operation on the project.
  // Returns the ProjectHistoryStep to be displayed in the history and the state reached by
  // the operation.
  // Won't ever execute dirty operation, for those it sets a special error status.
  private def historyStep(
    user: serving.User,
    startState: RootProjectState,
    request: SubProjectOperation,
    nextStateOpt: Option[RootProjectState]): (RootProjectState, ProjectHistoryStep) = {

    val startStateRootViewer = new RootProjectViewer(startState)
    val context = Operation.Context(user, startStateRootViewer.offspringViewer(request.path))
    val opCategoriesBefore = ops.categories(context)
    val segmentationsBefore = startStateRootViewer.toFE("dummy").segmentations
    val op = ops.opById(context, request.op.id)
    // If it's a deprecated workflow operation, display it in a special category.
    val opCategoriesBeforeWithOp =
      if (opCategoriesBefore.find(_.containsOperation(op)).isEmpty &&
        op.isInstanceOf[WorkflowOperation]) {
        val deprCat = WorkflowOperation.deprecatedCategory
        val deprCatFE = deprCat.toFE(List(op.toFE.copy(category = deprCat.title)))
        opCategoriesBefore :+ deprCatFE
      } else {
        opCategoriesBefore
      }

    val status =
      if (op.enabled.enabled && !op.dirty) {
        try {
          op.validateAndApply(request.op.parameters)
          FEStatus.enabled
        } catch {
          case t: Throwable =>
            FEStatus.disabled(t.getMessage)
        }
      } else if (op.dirty) {
        FEStatus.disabled(BigGraphController.dirtyOperationError)
      } else {
        op.enabled
      }

    // For the next state, we take it if it's given, otherwise take the end result of the
    // operation if it succeeded or we fall back to the state before the operation.
    val nextState = nextStateOpt.getOrElse(
      if (status.enabled) op.project.rootState
      else startState)
    val nextStateRootViewer = new RootProjectViewer(nextState)
    val segmentationsAfter = nextStateRootViewer.toFE("dummy").segmentations
    (nextState,
      ProjectHistoryStep(
        request,
        status,
        segmentationsBefore,
        segmentationsAfter,
        opCategoriesBeforeWithOp,
        nextState.checkpoint))
  }

  def validateHistory(user: serving.User, request: AlternateHistory): ProjectHistory = {
    val startingState = metaManager.checkpointRepo.readCheckpoint(request.startingPoint)
    val checkpointHistory = stateHistory(user, startingState)
    val historyExtension = extendedHistory(user, startingState, request.requests)
    val cleanCheckpointHistory =
      checkpointHistory
        .filter(step => step.status.disabledReason != BigGraphController.dirtyOperationError)
    ProjectHistory(
      cleanCheckpointHistory ++ historyExtension.map(_._2))
  }

  def saveHistory(user: serving.User, request: SaveHistoryRequest): Unit = {
    // See first that we can deal with this history.
    val startingPoint = request.history.startingPoint
    val historyExtension = extendedHistory(
      user,
      metaManager.checkpointRepo.readCheckpoint(startingPoint),
      request.history.requests)
    assert(historyExtension.map(_._2).forall(_.status.enabled), "Trying to save invalid history")

    var finalCheckpoint = startingPoint
    for (state <- historyExtension.map(_._1)) {
      finalCheckpoint =
        metaManager.checkpointRepo.checkpointState(state, finalCheckpoint).checkpoint.get
    }

    metaManager.tagBatch {
      // Create/check target project.
      ProjectFrame.validateName(request.newProject, "Project name")
      val p = ProjectFrame.fromName(request.newProject)
      if (request.newProject != request.oldProject) {
        // Saving under a new name.
        assertNameNotExists(request.newProject)
        // Copying old ProjectFrame level data.
        ProjectFrame.fromName(request.oldProject).copy(p)
        // But adding user as writer if necessary.
        if (!p.writeAllowedFrom(user)) {
          p.writeACL += "," + user.email
        }
      } else {
        p.assertWriteAllowedFrom(user)
      }
      // Set the new history.
      p.setCheckpoint(finalCheckpoint)
    }
  }

  def saveWorkflow(user: serving.User, request: SaveWorkflowRequest): Unit = metaManager.synchronized {
    val dateString =
      (new java.text.SimpleDateFormat("dd-MM-yyyy hh:mm")).format(new java.util.Date())
    val description =
      s"<p>User defined workflow saved by ${user.email} at $dateString<p>${request.description}"

    val savedWorkflow = SavedWorkflow(
      request.stepsAsGroovy,
      user.email,
      description)
    ProjectFrame.validateName(request.workflowName, "Workflow name")
    val tagName = BigGraphController.workflowsRoot / request.workflowName / Timestamp.toString
    metaManager.setTag(tagName, savedWorkflow.prettyJson)
  }
}

abstract class OperationParameterMeta {
  val id: String
  val title: String
  val kind: String
  val defaultValue: String
  val options: List[UIValue]
  val multipleChoice: Boolean
  val mandatory: Boolean

  // Asserts that the value is valid, otherwise throws an AssertionException.
  def validate(value: String): Unit
  def toFE = FEOperationParameterMeta(id, title, kind, defaultValue, options, multipleChoice)
}

abstract class Operation(originalTitle: String, context: Operation.Context, val category: Operation.Category) {
  val project = context.project.editor
  val user = context.user
  def id = Operation.titleToID(originalTitle)
  def title = originalTitle // Override this to change the display title while keeping the original ID.
  val description = "" // Override if description is dynamically generated.
  def parameters: List[OperationParameterMeta]
  def enabled: FEStatus
  // A summary of the operation, to be displayed on the UI.
  def summary(params: Map[String, String]): String = title

  protected def apply(params: Map[String, String]): Unit

  def validateParameters(values: Map[String, String]): Unit = {
    val paramIds = parameters.map { param => param.id }.toSet
    val extraIds = values.keySet &~ paramIds
    assert(extraIds.size == 0, s"""Extra parameters found: ${extraIds.mkString(", ")}""")
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

  // "Dirty" operations have side-effects, such as writing files. (See #1564.)
  val dirty = false
  def toFE: FEOperationMeta =
    FEOperationMeta(id, title, parameters.map { param => param.toFE }, category.title, enabled, description)
  protected def scalars[T: TypeTag] =
    UIValue.list(project.scalarNames[T].toList)
  protected def vertexAttributes[T: TypeTag] =
    UIValue.list(project.vertexAttributeNames[T].toList)
  protected def edgeAttributes[T: TypeTag] =
    UIValue.list(project.edgeAttributeNames[T].toList)
  protected def segmentations =
    UIValue.list(project.segmentationNames.toList)
  protected def hasVertexSet = FEStatus.assert(project.vertexSet != null, "No vertices.")
  protected def hasNoVertexSet = FEStatus.assert(project.vertexSet == null, "Vertices already exist.")
  protected def hasEdgeBundle = FEStatus.assert(project.edgeBundle != null, "No edges.")
  protected def hasNoEdgeBundle = FEStatus.assert(project.edgeBundle == null, "Edges already exist.")
  protected def isNotSegmentation = FEStatus.assert(!project.isSegmentation,
    "This operation is not available with segmentations.")
  protected def isSegmentation = FEStatus.assert(project.isSegmentation,
    "This operation is only available for segmentations.")
  // All projects that the user has read access to.
  protected def readableProjects(implicit manager: MetaGraphManager): List[UIValue] = {
    UIValue.list(Operation.allProjects(user)
      .map(_.projectName)
      .toList)
  }
}
object Operation {
  def titleToID(title: String) = title.replace(" ", "-")
  case class Category(
      title: String,
      color: String, // A color class from web/app/styles/operation-toolbox.css.
      visible: Boolean = true,
      icon: String = "", // Glyphicon name, or empty for first letter of title.
      sortKey: String = null // Categories are ordered by this. The title is used by default.
      ) extends Ordered[Category] {
    private val safeSortKey = Option(sortKey).getOrElse(title)
    def compare(that: Category) = this.safeSortKey compare that.safeSortKey
    def toFE(ops: List[FEOperationMeta]): OperationCategory =
      OperationCategory(title, icon, color, ops)
  }

  case class Context(user: serving.User, project: ProjectViewer)

  def allProjects(user: serving.User)(implicit manager: MetaGraphManager): Seq[ProjectFrame] = {
    val root = new SymbolPath(Nil)
    val projects = new ProjectDirectory(root).listProjectsRecursively
    val readable = projects.filter(_.readAllowedFrom(user))
    // Do not list internal project names (starting with "!").
    readable.filterNot(_.projectName.startsWith("!"))
  }
}

object WorkflowOperation {
  private val WorkflowParameterRegex = "params\\['([-A-Za-z0-9_ ]+)'\\]".r
  private def findParameterReferences(source: String): Set[String] = {
    WorkflowParameterRegex
      .findAllMatchIn(source)
      .map(_.group(1))
      .toSet
  }
  val category = Operation.Category("User Defined Workflows", "pink")
  val deprecatedCategory = Operation.Category("Deprecated User Defined Workflows", "red")

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]
  implicit val rSubProjectOperation = json.Json.reads[SubProjectOperation]
}
case class WorkflowOperation(
  fullName: SymbolPath,
  workflow: SavedWorkflow,
  context: Operation.Context,
  operationRepository: OperationRepository)
    extends Operation(
      fullName.drop(1).head.name,
      context,
      WorkflowOperation.category) {

  override val id = fullName.toString

  override val description = workflow.description

  val parameterReferences = WorkflowOperation.findParameterReferences(workflow.stepsAsGroovy)

  def parameters =
    parameterReferences
      .toList
      .sorted
      .map(paramName => OperationParams.Param(paramName, paramName))

  def enabled = FEStatus.enabled
  def apply(params: Map[String, String]): Unit = {
    val ctx = groovy.GroovyContext(context.user, operationRepository)
    ctx.withUntrustedShell(
      "params" -> scala.collection.JavaConversions.mapAsJavaMap(params),
      "project" -> new groovy.GroovyWorkflowProject(ctx, project, Seq())) {
        shell => shell.evaluate(workflow.stepsAsGroovy, title)
      }
  }
}

abstract class OperationRepository(env: BigGraphEnvironment) {
  implicit lazy val manager = env.metaGraphManager

  // The registry maps operation IDs to their constructors.
  private val operations = mutable.Map[String, Operation.Context => Operation]()
  def register(title: String, factory: (String, Operation.Context) => Operation): Unit = {
    val id = Operation.titleToID(title)
    assert(!operations.contains(id), s"$id is already registered.")
    operations(id) = factory(title, _)
  }

  private def opsForContext(context: Operation.Context): Seq[Operation] = {
    operations.values.toSeq.map(_(context))
  }

  private def workflowOpFromTag(fullName: SymbolPath, context: Operation.Context) =
    WorkflowOperation(fullName, SavedWorkflow.fromJson(manager.getTag(fullName)), context, this)

  private def workflowOperations(context: Operation.Context): Seq[Operation] =
    if (manager.tagExists(BigGraphController.workflowsRoot)) {
      manager
        .lsTag(BigGraphController.workflowsRoot)
        .map(perNamePrefix => manager.lsTag(perNamePrefix).sorted.last)
        .map(fullName => workflowOpFromTag(fullName, context))
    } else {
      Seq()
    }

  def categories(context: Operation.Context): List[OperationCategory] = {
    val allOps = opsForContext(context) ++ workflowOperations(context)
    val cats = allOps.groupBy(_.category).toList
    cats.filter(_._1.visible).sortBy(_._1).map {
      case (cat, ops) =>
        val feOps = ops.map(_.toFE).sortBy(_.title).toList
        cat.toFE(feOps)
    }
  }

  def operationIds = operations.keys.toSeq

  def opById(context: Operation.Context, id: String): Operation = {
    if (id.startsWith(BigGraphController.workflowsRoot.toString + "/")) {
      // Oho, a workflow operation!
      workflowOpFromTag(SymbolPath.parse(id), context)
    } else {
      assert(operations.contains(id), s"Cannot find operation: ${id}")
      operations(id)(context)
    }
  }

  // Applies the operation specified by op in the given context and returns the
  // applied operation.
  def appliedOp(context: Operation.Context, opSpec: FEOperationSpec): Operation = {
    val op = opById(context, opSpec.id)
    op.validateAndApply(opSpec.parameters)
    op
  }

  def applyAndCheckpoint(context: Operation.Context, opSpec: FEOperationSpec): RootProjectState = {
    val opResult = appliedOp(context, opSpec).project.rootState
    manager.checkpointRepo.checkpointState(opResult, context.project.rootState.checkpoint.get)
  }

  def apply(
    user: serving.User,
    subProject: SubProject,
    op: FEOperationSpec): Unit = manager.tagBatch {

    val context = Operation.Context(user, subProject.viewer)
    subProject.frame.setCheckpoint(applyAndCheckpoint(context, op).checkpoint.get)
  }
}
