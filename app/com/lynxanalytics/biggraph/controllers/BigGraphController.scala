// BigGraphController includes the request handlers that operate on projects at the metagraph level.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.groovy
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
  isWorkflow: Boolean = false,
  workflowAuthor: String = "",
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

case class SaveWorkflowRequest(
  workflowName: String,
  stepsAsGroovy: String,
  description: String)

case class WorkflowRequest(
  id: String)

case class WorkflowResponse(
  name: String,
  description: String,
  code: String)
object WorkflowResponse {
  implicit val fWorkflowResponse = json.Json.format[WorkflowResponse]
  def fromJson(js: String): WorkflowResponse = json.Json.parse(js).as[WorkflowResponse]
}

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
}
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

  def project(user: serving.User, request: ProjectRequest): FEProject = metaManager.synchronized {
    val p = SubProject.parsePath(request.name)
    assert(p.frame.exists, s"Project ${request.name} does not exist.")
    p.frame.assertReadAllowedFrom(user)
    val context = Operation.Context(user, p.viewer)
    val categories = ops.categories(context, includeDeprecated = false)
    // Utility operations are made available through dedicated UI elements.
    // Let's hide them from the project operation toolbox to avoid confusion.
    val nonUtilities = categories.filter(_.icon != "wrench")
    p.toFE.copy(opCategories = nonUtilities)
  }

  private def assertNameNotExists(name: String) = {
    assert(!DirectoryEntry.fromName(name).exists, s"Entry '$name' already exists.")
  }

  def createProject(user: serving.User, request: CreateProjectRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val p = entry.asNewProjectFrame()
    p.setupACL(request.privacy, user)
    if (request.notes != "") {
      ops.apply(user, p.subproject, Operations.addNotesOperation(request.notes))
    }
  }

  def createDirectory(user: serving.User, request: CreateDirectoryRequest): Unit = metaManager.synchronized {
    assertNameNotExists(request.name)
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    val dir = entry.asNewDirectory()
    dir.setupACL(request.privacy, user)
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
    if (metaManager.tagExists(BigGraphController.workflowsRoot)) {
      metaManager.rmTag(BigGraphController.workflowsRoot)
    }
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

  def undoProject(user: serving.User, request: UndoProjectRequest): Unit = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.project)
    entry.assertWriteAllowedFrom(user)
    entry.asProjectFrame.undo()
  }

  def redoProject(user: serving.User, request: RedoProjectRequest): Unit = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.project)
    entry.assertWriteAllowedFrom(user)
    entry.asProjectFrame.redo()
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

  def getHistory(user: serving.User, request: HistoryRequest): ProjectHistory = {
    val checkpoint = metaManager.synchronized {
      val entry = DirectoryEntry.fromName(request.project)
      entry.assertReadAllowedFrom(user)
      entry.asProjectFrame.checkpoint
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

  def getOpCategories(user: serving.User,
                      history: AlternateHistory): OpCategories = {
    val rootState = metaManager.checkpointRepo.readCheckpoint(history.startingPoint)

    val currentState = if (history.requests.nonEmpty) {
      extendedHistory(user, rootState, history.requests).last._1
    } else {
      rootState
    }

    val viewer = new RootProjectViewer(currentState)
    val op = currentState.lastOperationRequest.getOrElse(
      history.requests.headOption.getOrElse(
        SubProjectOperation(Seq(), FEOperationSpec("No-operation", Map()))))
    val context = Operation.Context(user, viewer.offspringViewer(op.path))
    OpCategories(ops.categories(context, includeDeprecated = true))
  }

  // Tries to execute the requested operation on the project.
  // Returns the ProjectHistoryStep to be displayed in the history and the state reached by
  // the operation.
  private def historyStep(
    user: serving.User,
    startState: RootProjectState,
    request: SubProjectOperation,
    nextStateOpt: Option[RootProjectState]): (RootProjectState, ProjectHistoryStep) = {

    val startStateRootViewer = new RootProjectViewer(startState)

    val segmentationsBefore = startStateRootViewer.allOffspringFESegmentations("dummy")
    val context = Operation.Context(user, startStateRootViewer.offspringViewer(request.path))
    val op = ops.opById(context, request.op.id)

    val opMeta: Option[FEOperationMeta] = Some(op.toFE.copy(color = Some(op.category.color)))

    // Remove parameters from the request that no longer exist.
    val restrictedRequest = {
      val validParameterIds = op.parameters.map(_.id).toSet
      val restrictedParameters = request.op.parameters.filterKeys(validParameterIds.contains(_))
      request.copy(op = request.op.copy(parameters = restrictedParameters))
    }

    // Try to apply the operation and get the success state.
    val status = {
      if (op.enabled.enabled) {
        try {
          op.validateAndApply(restrictedRequest.op.parameters)
          FEStatus.enabled
        } catch {
          case t: Throwable =>
            FEStatus.disabled(t.getMessage)
        }
      } else {
        op.enabled
      }
    }

    // For the next state, we take it if it's given, otherwise take the end result of the
    // operation if it succeeded or we fall back to the state before the operation.
    val nextState = nextStateOpt.getOrElse(
      if (status.enabled) op.project.rootState
      else startState)
    val nextStateRootViewer = new RootProjectViewer(nextState)
    val segmentationsAfter = nextStateRootViewer.allOffspringFESegmentations("dummy")
    val checkPointOpt = nextStateOpt.flatMap(_.checkpoint)
    (nextState,
      ProjectHistoryStep(
        restrictedRequest,
        status,
        segmentationsBefore,
        segmentationsAfter,
        opMeta,
        checkPointOpt
      ))
  }

  def validateHistory(user: serving.User, request: AlternateHistory): ProjectHistory = {
    val startingState = metaManager.checkpointRepo.readCheckpoint(request.startingPoint)
    val checkpointHistory = stateHistory(user, startingState)
    val historyExtension = extendedHistory(user, startingState, request.requests)
    ProjectHistory(
      checkpointHistory ++ historyExtension.map(_._2))
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
      val entry = DirectoryEntry.fromName(request.newProject)
      val project = if (request.newProject != request.oldProject) {
        // Saving under a new name.
        assertNameNotExists(request.newProject)
        // But adding user as writer if necessary.
        if (!entry.writeAllowedFrom(user)) {
          entry.writeACL += "," + user.email
        }
        // Copying old ProjectFrame level data.
        ProjectFrame.fromName(request.oldProject).copy(entry)
      } else {
        entry.assertWriteAllowedFrom(user)
        entry.asProjectFrame
      }
      // Now we have a project in the tag tree. Set the new history.
      project.setCheckpoint(finalCheckpoint)
    }
  }

  def saveWorkflow(user: serving.User, request: SaveWorkflowRequest): Unit = metaManager.synchronized {
    // Check for syntax errors with an unbound Groovy shell.
    try {
      groovy.GroovyContext(null, null).withUntrustedShell() {
        shell => shell.parse(request.stepsAsGroovy, request.workflowName)
      }
    } catch {
      case cfe: org.codehaus.groovy.control.CompilationFailedException =>
        throw new AssertionError(cfe.getMessage) // Looks better on the frontend.
      case t: Throwable => // It can throw Errors, that silently kill the request.
        throw new AssertionError(t.getMessage)
    }
    val savedWorkflow = SavedWorkflow(
      request.stepsAsGroovy,
      user.email,
      request.description)
    ProjectFrame.validateName(request.workflowName, "Workflow name")
    val tagName = BigGraphController.workflowsRoot / request.workflowName / Timestamp.toString
    metaManager.setTag(tagName, savedWorkflow.prettyJson)
  }

  def workflow(user: serving.User, request: WorkflowRequest): WorkflowResponse = metaManager.synchronized {
    val id = SymbolPath.parse(request.id)
    val workflow = env.metaGraphManager.workflow(id)
    WorkflowResponse(
      request.id.split("/")(1), // extract name from id
      workflow.description,
      workflow.stepsAsGroovy)
  }

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

abstract class Operation(originalTitle: String, context: Operation.Context, val category: Operation.Category) {
  val project = context.project.editor
  val user = context.user
  def id = Operation.titleToID(originalTitle)
  def title = originalTitle // Override this to change the display title while keeping the original ID.
  val description = "" // Override if description is dynamically generated.
  def parameters: List[OperationParameterMeta]
  def visibleScalars: List[FEScalar] = List()
  def enabled: FEStatus
  def isWorkflow: Boolean = false
  def workflowAuthor: String = ""
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
    category.title,
    enabled,
    description,
    isWorkflow,
    workflowAuthor)
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

  case class Context(user: serving.User, project: ProjectViewer)

  def allObjects(user: serving.User)(implicit manager: MetaGraphManager): Seq[ObjectFrame] = {
    val objects = DirectoryEntry.rootDirectory.listObjectsRecursively
    val readable = objects.filter(_.readAllowedFrom(user))
    // Do not list internal project names (starting with "!").
    readable.filterNot(_.name.startsWith("!"))
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

  override val workflowAuthor = workflow.author

  val parameterReferences = WorkflowOperation.findParameterReferences(workflow.stepsAsGroovy)

  def parameters =
    parameterReferences
      .toList
      .sorted
      .map(paramName => OperationParams.Param(paramName, paramName))

  def enabled = FEStatus.enabled

  override def isWorkflow = true

  def apply(params: Map[String, String]): Unit = {
    val ctx = groovy.GroovyContext(context.user, operationRepository)
    ctx.withUntrustedShell(
      "params" -> scala.collection.JavaConversions.mapAsJavaMap(params),
      "project" -> new groovy.GroovyWorkflowProject(ctx, project, Seq())) {
        shell => shell.evaluate(workflow.stepsAsGroovy, title)
      }
  }
}

abstract class OperationRepository(env: SparkFreeEnvironment) {
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
    WorkflowOperation(fullName, manager.workflow(fullName), context, this)

  def newestWorkflow(name: String) =
    manager.lsTag(BigGraphController.workflowsRoot / name).sorted.last

  private def workflowOperations(context: Operation.Context): Seq[Operation] =
    if (manager.tagExists(BigGraphController.workflowsRoot)) {
      manager
        .lsTag(BigGraphController.workflowsRoot)
        .map(perNamePrefix => manager.lsTag(perNamePrefix).sorted.last)
        .map(fullName => workflowOpFromTag(fullName, context))
    } else {
      Seq()
    }

  def categories(context: Operation.Context, includeDeprecated: Boolean): List[OperationCategory] = {
    val allOps = opsForContext(context) ++ workflowOperations(context)
    val cats = allOps.groupBy(_.category).toList
    cats.filter { x => x._1.visible && (includeDeprecated || x._1.deprecated == false) }
      .sortBy(_._1).map {
        case (cat, ops) =>
          val feOps = ops.map(_.toFE).sortBy(_.title).toList
          cat.toFE(feOps)
      }
  }

  def operationIds = operations.keys.toSeq

  private def maybeOpById(context: Operation.Context, id: String): Option[Operation] = {
    if (id.startsWith(BigGraphController.workflowsRoot.toString + "/")) {
      // Oho, a workflow operation!
      Some(workflowOpFromTag(SymbolPath.parse(id), context))
    } else {
      operations.get(id).map(_(context))
    }
  }

  def opById(context: Operation.Context, id: String): Operation = {
    maybeOpById(context, id).getOrElse {
      new Operation(id, context, Operation.Category("Removed operations", "red")) {
        def parameters = List()
        def enabled = FEStatus.disabled(s"$id no longer exists")
        def apply(params: Map[String, String]) =
          throw new AssertionError(s"Cannot find operation: $id")
      }
    }
  }

  // Applies the operation specified by op in the given context and returns the
  // applied operation.
  def appliedOp(context: Operation.Context, opSpec: FEOperationSpec): Operation = {
    val op = opById(context, opSpec.id)
    op.validateAndApply(opSpec.parameters)
    op
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

  def applyAndCheckpoint(context: Operation.Context, opSpec: FEOperationSpec): RootProjectState = {
    val editor = appliedOp(context, opSpec).project
    updateDeltas(editor.rootEditor, original = context.project.rootViewer)
    manager.checkpointRepo.checkpointState(editor.rootState, context.project.rootCheckpoint)
  }

  def apply(
    user: serving.User,
    subProject: SubProject,
    op: FEOperationSpec): Unit = manager.tagBatch {

    val context = Operation.Context(user, subProject.viewer)
    subProject.frame.setCheckpoint(applyAndCheckpoint(context, op).checkpoint.get)
  }
}
