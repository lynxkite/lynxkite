// BigGraphController includes the request handlers that operate on projects at the metagraph level.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import scala.collection.mutable

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
  val boolsDefaultFalse = list("false", "true")
  val noyes = list("no", "yes")
  val unset = special("!unset")
  val noWeight = special("!no weight")
  val unitDistances = special("!unit distances")
  val internalId = special("!internal id (default)")
  val jsDataTypes = FEOption.list("double", "string", "vector of doubles", "vector of strings")
}

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

  assert(
    objectType == "table" || objectType == "project" ||
      objectType == "view" || objectType == "workspace",
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
case class ACLSettingsRequest(project: String, readACL: String, writeACL: String)

class BigGraphController(val env: SparkFreeEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val entityProgressManager: EntityProgressManager = env.entityProgressManager

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
      else union.map(email => serving.User(email, false)).filter(!p.parent.get.readAllowedFrom(_))

    assert(notAllowed.isEmpty,
      s"The following users don't have read access to all of the parent folders: ${notAllowed.mkString(", ")}")

    p.readACL = request.readACL
    p.writeACL = request.writeACL
  }
}
