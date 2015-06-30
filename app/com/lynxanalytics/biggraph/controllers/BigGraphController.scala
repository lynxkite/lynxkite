// BigGraphController includes the request handlers that operate on projects at the metagraph level.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving

import java.util.regex.Pattern
import play.api.libs.json
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
  vertexCount: Option[FEAttribute], // Whether the project has vertices defined.
  edgeCount: Option[FEAttribute]) // Whether the project has edges defined.

case class FEProject(
  name: String,
  error: String = "", // If this is non-empty the project is broken and cannot be opened.
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
case class Splash(version: String, projects: List[FEProjectListElement])
case class OperationCategory(
    title: String, icon: String, color: String, ops: List[FEOperationMeta]) {
  def containsOperation(op: Operation): Boolean = ops.find(_.id == op.id).nonEmpty
}
case class CreateProjectRequest(name: String, notes: String, privacy: String)
case class DiscardProjectRequest(name: String)
case class ProjectOperationRequest(project: String, op: FEOperationSpec)
case class ProjectAttributeFilter(attributeName: String, valueSpec: String)
case class ProjectFilterRequest(
  project: String,
  vertexFilters: List[ProjectAttributeFilter],
  edgeFilters: List[ProjectAttributeFilter])
case class ForkProjectRequest(from: String, to: String)
case class RenameProjectRequest(from: String, to: String)
case class UndoProjectRequest(project: String)
case class RedoProjectRequest(project: String)
case class ProjectSettingsRequest(project: String, readACL: String, writeACL: String)

case class HistoryRequest(project: String)
case class AlternateHistory(
  project: String,
  skips: Int, // Number of unmodified operations.
  requests: List[ProjectOperationRequest])
case class SaveHistoryRequest(
  newProject: String,
  history: AlternateHistory)
case class ProjectHistory(
    project: String,
    steps: List[ProjectHistoryStep]) {
  def valid = steps.forall(step => step.hasCheckpoint || step.status.enabled)
}
case class ProjectHistoryStep(
  request: ProjectOperationRequest,
  status: FEStatus,
  segmentationsBefore: List[FESegmentation],
  segmentationsAfter: List[FESegmentation],
  opCategoriesBefore: List[OperationCategory],
  hasCheckpoint: Boolean)

case class SaveWorkflowRequest(
  workflowName: String,
  // This may contain parameter references in the format ${param-name}. There is a special
  // reference, ${!project} which is automatically replaced with the project name the workflow
  // should run on. After parameter substitution we parse the string as a JSON form of
  // List[ProjectOperationRequest] and then we try to apply these operations in sequence.
  stepsAsJSON: String,
  description: String)

case class SavedWorkflow(
    stepsAsJSON: String,
    author: String,
    description: String) {
  @transient lazy val prettyJson: String = SavedWorkflow.asPrettyJson(this)
}
object SavedWorkflow {
  implicit val wUIValue = json.Json.writes[UIValue]
  implicit val wFEOperationParameterMeta = json.Json.writes[FEOperationParameterMeta]
  implicit val wSavedWorkflow = json.Json.writes[SavedWorkflow]
  implicit val rUIValue = json.Json.reads[UIValue]
  implicit val rFEOperationParameterMeta = json.Json.reads[FEOperationParameterMeta]
  implicit val rSavedWorkflow = json.Json.reads[SavedWorkflow]
  def asPrettyJson(wf: SavedWorkflow): String = json.Json.prettyPrint(json.Json.toJson(wf))
  def fromJson(js: String): SavedWorkflow = json.Json.parse(js).as[SavedWorkflow]
}

object BigGraphController {
  val workflowsRoot: SymbolPath = SymbolPath("workflows")
}
class BigGraphController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager

  lazy val version = try {
    scala.io.Source.fromFile(util.Properties.userDir + "/version").mkString
  } catch {
    case e: java.io.IOException => ""
  }

  val ops = new Operations(env)

  def splash(user: serving.User, request: serving.Empty): Splash = metaManager.synchronized {
    val projects = Operation.projects.filter(_.readAllowedFrom(user)).map(_.toListElementFE)
    return Splash(version, projects.toList)
  }

  def project(user: serving.User, request: ProjectRequest): FEProject = metaManager.synchronized {
    val p = Project.fromPath(request.name)
    p.assertReadAllowedFrom(user)
    val categories = ops.categories(user, p)
    // Utility operations are made available through dedicated UI elements.
    // Let's hide them from the project operation toolbox to avoid confusion.
    val nonUtilities = categories.filter(_.icon != "wrench")
    p.toFE.copy(opCategories = nonUtilities)
  }

  def createProject(user: serving.User, request: CreateProjectRequest): Unit = metaManager.synchronized {
    Project.validateName(request.name, "Project name")
    val p = Project.fromName(request.name)
    assert(!Operation.projects.contains(p), s"Project ${request.name} already exists.")
    p.notes = request.notes
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
    p.checkpointAfter("") // Initial checkpoint.
  }

  def discardProject(user: serving.User, request: DiscardProjectRequest): Unit = metaManager.synchronized {
    val p = Project.fromName(request.name)
    p.assertWriteAllowedFrom(user)
    p.remove()
  }

  def renameProject(user: serving.User, request: RenameProjectRequest): Unit = metaManager.synchronized {
    Project.validateName(request.to, "Project name")
    val p = Project.fromName(request.from)
    p.assertWriteAllowedFrom(user)
    p.copy(Project.fromName(request.to))
    p.remove()
  }

  def projectOp(user: serving.User, request: ProjectOperationRequest): Unit = metaManager.synchronized {
    val p = Project.fromPath(request.project)
    p.assertWriteAllowedFrom(user)
    ops.apply(user, request)
  }

  def filterProject(user: serving.User, request: ProjectFilterRequest): Unit = metaManager.synchronized {
    // Historical bridge.
    val c = Operation.Context(user, Project.fromPath(request.project))
    val op = ops.opById(c, "Filter-by-attributes")
    val emptyParams = op.parameters.map(_.id -> "")
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
        parameters = (emptyParams ++ vertexParams ++ edgeParams).toMap)))
  }

  def forkProject(user: serving.User, request: ForkProjectRequest): Unit = metaManager.synchronized {
    Project.validateName(request.to, "Project name")
    val p1 = Project.fromName(request.from)
    val p2 = Project.fromName(request.to)
    p1.assertReadAllowedFrom(user)
    assert(!Operation.projects.contains(p2), s"Project $p2 already exists.")
    p1.copy(p2)
    if (!p2.writeAllowedFrom(user)) {
      p2.writeACL += "," + user.email
    }
  }

  def undoProject(user: serving.User, request: UndoProjectRequest): Unit = metaManager.synchronized {
    val p = Project.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    p.undo()
  }

  def redoProject(user: serving.User, request: RedoProjectRequest): Unit = metaManager.synchronized {
    val p = Project.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    p.redo()
  }

  def changeProjectSettings(user: serving.User, request: ProjectSettingsRequest): Unit = metaManager.synchronized {
    val p = Project.fromName(request.project)
    p.assertWriteAllowedFrom(user)
    // To avoid accidents, a user cannot remove themselves from the write ACL.
    assert(user.isAdmin || p.aclContains(request.writeACL, user),
      s"You cannot forfeit your write access to project $p.")
    p.readACL = request.readACL
    p.writeACL = request.writeACL
  }

  def getHistory(user: serving.User, request: HistoryRequest): ProjectHistory = metaManager.synchronized {
    val p = Project.fromName(request.project)
    p.assertReadAllowedFrom(user)
    assert(p.checkpointCount > 0, s"No history for $p. Try the parent project.")
    validateHistory(user, AlternateHistory(request.project, p.checkpointCount, List()))
  }

  // Expand each checkpoint of a project to a separate project, run the code, then clean up.
  private def withCheckpoints[T](p: Project)(code: Seq[Project] => T): T = metaManager.tagBatch {
    val tmpDir = s"!tmp-$Timestamp"
    val ps = (0 until p.checkpointCount).map { i =>
      val tmp = Project.fromName(s"$tmpDir-$i")
      assert(!Operation.projects.contains(tmp), s"Project $tmp already exists.")
      p.copyCheckpoint(i, tmp)
      tmp
    }
    try {
      code(ps)
    } finally {
      for (p <- ps) {
        p.remove
      }
    }
  }

  // Returns the evaluated alternate history, and optionally
  // copies the resulting state into a new project.
  private def alternateHistory(
    user: serving.User,
    request: AlternateHistory,
    copyTo: Option[Project]): ProjectHistory = metaManager.synchronized {
    val p = Project.fromName(request.project)
    p.assertReadAllowedFrom(user)
    withCheckpoints(p) { checkpoints =>
      val beforeAfter = checkpoints.zip(checkpoints.tail)
      // Operate on a lazy "view" because historyStep() is slow.
      val skippedStepsAndStates = beforeAfter.view.flatMap {
        case (before, after) =>
          val request = after.lastOperationRequest
          val stepOpt = request.flatMap(historyStep(user, before, _, nextCheckpoint = Some(after)))
          stepOpt.map(_ -> after)
      }.take(request.skips).toIndexedSeq
      val (skippedSteps, skippedStates) = skippedStepsAndStates.unzip
      // The state to be mutated by the modified steps.
      // Starts from the state after the last skipped step
      // or (if there are none) from the first checkpoint.
      val state = skippedStates.lastOption.getOrElse(checkpoints.head)
      val modifiedSteps = request.requests.flatMap(historyStep(user, state, _))
      val steps = skippedSteps ++ modifiedSteps
      val history = ProjectHistory(p.projectName, steps.toList)
      if (copyTo.nonEmpty) {
        assert(history.valid, s"Tried to copy invalid history for project $p.")
        state.copy(copyTo.get)
      }
      history
    }
  }

  // Tries to execute the requested operation on the project.
  // Returns the ProjectHistoryStep to be displayed in the history, or None if it should be hidden.
  private def historyStep(
    user: serving.User,
    state: Project,
    request: ProjectOperationRequest,
    nextCheckpoint: Option[Project] = None): Option[ProjectHistoryStep] = {
    val op = ops.operationOnSubproject(state, request, user)
    val recipient = op.project

    val segmentationsBefore = state.toFE.segmentations
    val opCategoriesBefore = ops.categories(user, recipient)
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
    def newStep(status: FEStatus) = {
      val segmentationsAfter = nextCheckpoint.getOrElse(state).toFE.segmentations
      Some(ProjectHistoryStep(
        request, status, segmentationsBefore, segmentationsAfter, opCategoriesBeforeWithOp,
        hasCheckpoint = nextCheckpoint.nonEmpty))
    }
    if (op.enabled.enabled && !op.dirty) {
      try {
        recipient.checkpoint(op.summary(request.op.parameters), request) {
          op.validateAndApply(request.op.parameters)
        }
        newStep(FEStatus.enabled)
      } catch {
        case t: Throwable =>
          newStep(FEStatus.disabled(t.getMessage))
      }
    } else if (op.dirty) {
      None // Dirty operations are hidden from the history.
    } else {
      newStep(op.enabled)
    }
  }

  def validateHistory(user: serving.User, request: AlternateHistory): ProjectHistory = metaManager.synchronized {
    alternateHistory(user, request, None)
  }

  def saveHistory(user: serving.User, request: SaveHistoryRequest): Unit = metaManager.synchronized {
    Project.validateName(request.newProject, "Project name")
    val p = Project.fromName(request.newProject)
    if (request.newProject != request.history.project) {
      // Saving under a new name.
      assert(!Operation.projects.contains(p), s"Project $p already exists.")
      alternateHistory(user, request.history, Some(p))
      if (!p.writeAllowedFrom(user)) {
        p.writeACL += "," + user.email
      }
    } else {
      // Overwriting the original project.
      p.assertWriteAllowedFrom(user)
      alternateHistory(user, request.history, Some(p))
    }
  }

  def saveWorkflow(user: serving.User, request: SaveWorkflowRequest): Unit = metaManager.synchronized {
    val dateString =
      (new java.text.SimpleDateFormat("dd-MM-yyyy hh:mm")).format(new java.util.Date())
    val description =
      s"<p>User defined workflow saved by ${user.email} at $dateString<p>${request.description}"

    val savedWorkflow = SavedWorkflow(
      request.stepsAsJSON,
      user.email,
      description)
    Project.validateName(request.workflowName, "Workflow name")
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
  val project = context.project
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
  // All projects that the user has read access to.
  protected def readableProjects(implicit manager: MetaGraphManager): List[UIValue] = {
    UIValue.list(Operation.projects
      .filter(_.readAllowedFrom(user))
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

  case class Context(user: serving.User, project: Project)

  def projects(implicit manager: MetaGraphManager): Seq[Project] = {
    val dirs = {
      if (manager.tagExists(SymbolPath("projects")))
        manager.lsTag(SymbolPath("projects"))
      else
        Nil
    }
    // Do not list internal project names (starting with "!").
    dirs.map(p => Project.fromName(p.path.last.name)).filterNot(_.projectName.startsWith("!"))
  }
}

object WorkflowOperation {
  private val WorkflowParameterRegex = "\\$\\{([-A-Za-z0-9_ ]+)\\}".r
  private def workflowConcreteParameterRegex(parameterName: String): Regex = {
    ("\\$\\{" + Pattern.quote(parameterName) + "\\}").r
  }
  private def findParameterReferences(source: String): Set[String] = {
    WorkflowParameterRegex
      .findAllMatchIn(source)
      .map(_.group(1))
      .toSet
  }
  val category = Operation.Category("User Defined Workflows", "pink")
  val deprecatedCategory = Operation.Category("Deprecated User Defined Workflows", "red")

  implicit val rFEOperationSpec = json.Json.reads[FEOperationSpec]
  implicit val rProjectOperationRequest = json.Json.reads[ProjectOperationRequest]
  private def stepsFromJSON(stepsAsJSON: String): List[ProjectOperationRequest] = {
    json.Json.parse(stepsAsJSON).as[List[ProjectOperationRequest]]
  }
  def substituteUserParameters(jsonTemplate: String, parameters: Map[String, String]): String = {
    var completeJSON = jsonTemplate

    for ((paramName, paramValue) <- parameters) {
      completeJSON = workflowConcreteParameterRegex(paramName)
        .replaceAllIn(completeJSON, Regex.quoteReplacement(paramValue))
    }

    completeJSON
  }

  private def substituteJSONParameters(
    jsonTemplate: String, parameters: Map[String, String], context: Operation.Context): String = {

    var withUserParams = substituteUserParameters(jsonTemplate, parameters)

    workflowConcreteParameterRegex("!project")
      .replaceAllIn(withUserParams, Regex.quoteReplacement(context.project.projectName))
  }
  def workflowSteps(
    jsonTemplate: String,
    parameters: Map[String, String],
    context: Operation.Context): List[ProjectOperationRequest] = {

    stepsFromJSON(substituteJSONParameters(jsonTemplate, parameters, context))
  }
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

  val parameterReferences = WorkflowOperation.findParameterReferences(workflow.stepsAsJSON)

  val (systemReferences, customReferences) = parameterReferences.partition(_.startsWith("!"))

  private val unknownSystemReferences = systemReferences &~ Set("!project")
  assert(
    unknownSystemReferences.isEmpty,
    "Unknown system parameter(s): " + unknownSystemReferences)

  def parameters =
    customReferences
      .toList
      .sorted
      .map(paramName => OperationParams.Param(paramName, paramName))

  def enabled = FEStatus.enabled
  def apply(params: Map[String, String]): Unit = {
    var stepsAsJSON = workflow.stepsAsJSON
    val steps = WorkflowOperation.workflowSteps(stepsAsJSON, params, context)
    for (step <- steps) {
      val op = operationRepository.operationOnSubproject(context.project, step, context.user)
      op.validateAndApply(step.op.parameters)
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

  def categories(user: serving.User, project: Project): List[OperationCategory] = {
    val context = Operation.Context(user, project)
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

  def operationOnSubproject(
    mainProject: Project,
    request: ProjectOperationRequest,
    user: serving.User): Operation = {
    val subProjectPath = SymbolPath.parse(request.project)
    val mainProjectSymbol = Symbol(mainProject.projectName)
    val relativePath = subProjectPath.tail
    val fullPath = mainProjectSymbol :: relativePath.toList
    val recipient = Project(SymbolPath.fromIterable(fullPath))
    val ctx = Operation.Context(user, recipient)
    opById(ctx, request.op.id)
  }

  def apply(
    user: serving.User, req: ProjectOperationRequest): Unit = manager.tagBatch {
    val p = Project.fromPath(req.project)
    val context = Operation.Context(user, p)
    val op = opById(context, req.op.id)
    p.checkpoint(op.summary(req.op.parameters), req) {
      op.validateAndApply(req.op.parameters)
    }
  }
}
