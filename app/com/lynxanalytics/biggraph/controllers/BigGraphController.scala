// BigGraphController includes the request handlers that operate on projects at the metagraph level.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.serving
import com.lynxanalytics.biggraph.graph_operations

import play.api.libs.json

import scala.collection.mutable

case class FEStatus(
    enabled: Boolean,
    disabledReason: String = "",
    exception: Option[Throwable] = None) {
  if (enabled) assert(exception.isEmpty, toString)
  def ||(other: => FEStatus) = if (enabled) this else other
  def &&(other: => FEStatus) = if (enabled) other else this
  def check(): Unit = exception match {
    case Some(exception) => throw new AssertionError(disabledReason, exception)
    case None => assert(enabled, disabledReason)
  }
}
object FEStatus {
  val enabled = FEStatus(true)
  def disabled(disabledReason: String) = FEStatus(false, disabledReason)
  def from(t: Throwable) = FEStatus(
    false,
    t match {
      case ae: AssertionError => ae.getMessage
      case _ => t.toString
    },
    Some(t))
  def assert(condition: Boolean, disabledReason: => String) =
    if (condition) enabled else disabled(disabledReason)
  implicit val format = new json.Format[FEStatus] {
    // Lose the exception reference in serialization.
    def reads(j: json.JsValue): json.JsResult[FEStatus] =
      json.JsSuccess(FEStatus((j \ "enabled").as[Boolean], (j \ "disabledReason").as[String]))
    def writes(s: FEStatus): json.JsValue =
      json.Json.obj("enabled" -> s.enabled, "disabledReason" -> s.disabledReason)
  }
}

// Something with a display name and an internal ID.
case class FEOption(
    id: String,
    title: String)
object FEOption {
  def regular(optionTitleAndId: String): FEOption = FEOption(optionTitleAndId, optionTitleAndId)
  def special(specialId: String): FEOption = specialOpt(specialId).get
  private def specialOpt(specialId: String): Option[FEOption] = {
    Option(specialId match {
      case "!unset" => ""
      case "!no weight" => "no weight"
      case "!unit distances" => "unit distances"
      case "!internal id (default)" => "internal id (default)"
      case _ => null
    }).map(FEOption(specialId, _))
  }
  def fromId(id: String) = specialOpt(id).getOrElse(FEOption.regular(id))
  def list(lst: String*): List[FEOption] = list(lst.toList)
  def list(lst: List[String]): List[FEOption] = lst.map(id => FEOption(id, id))
  val bools = list("true", "false")
  val boolsDefaultFalse = list("false", "true")
  val noyes = list("no", "yes")
  val yesno = list("yes", "no")
  val saveMode = list("error if exists", "overwrite", "append", "ignore")
  val unset = special("!unset")
  val noWeight = special("!no weight")
  val unitDistances = special("!unit distances")
  val internalId = special("!internal id (default)")
  val jsDataTypes = FEOption.list("Double", "String", "Vector of Doubles", "Vector of Strings")
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

case class FEEntryListElement(
    name: String,
    objectType: String,
    icon: String,
    notes: String = "",
    error: Option[String] = None)

case class FEProject(
    name: String,
    undoOp: String = "", // Name of last operation. Empty if there is nothing to undo.
    redoOp: String = "", // Name of next operation. Empty if there is nothing to redo.
    readACL: String = "",
    writeACL: String = "",
    vertexSet: String = "",
    edgeBundle: String = "",
    notes: String = "",
    graphAttributes: List[FEScalar] = List(),
    vertexAttributes: List[FEAttribute] = List(),
    edgeAttributes: List[FEAttribute] = List(),
    segmentations: List[FESegmentation] = List())

case class FESegmentation(
    name: String,
    fullName: String,
    // The connecting edge bundle's GUID.
    belongsTo: String,
    // A Vector[ID] vertex attribute, that contains for each vertex
    // the vector of ids of segments the vertex belongs to.
    equivalentAttribute: FEAttribute)
case class EntryListRequest(path: String)
case class EntrySearchRequest(
    basePath: String, // We only search for entries contained (recursively) in this.
    query: String,
    includeNotes: Boolean)
case class EntryList(
    path: String,
    readACL: String,
    writeACL: String,
    canWrite: Boolean,
    directories: List[String],
    objects: List[FEEntryListElement])
case class CreateDirectoryRequest(name: String, privacy: String)
case class DiscardEntryRequest(name: String)

case class ProjectAttributeFilter(attributeName: String, valueSpec: String)
case class ProjectFilterRequest(
    project: String,
    vertexFilters: List[ProjectAttributeFilter],
    edgeFilters: List[ProjectAttributeFilter])
case class ForkEntryRequest(from: String, to: String)
case class RenameEntryRequest(from: String, to: String, overwrite: Boolean)
case class ACLSettingsRequest(project: String, readACL: String, writeACL: String)

object BigGraphController {
  def entryList(user: serving.User, dir: Directory): (Iterable[DirectoryEntry], Iterable[ObjectFrame]) = {
    val entries = dir.list
    val (dirs, objects) = entries.partition(_.isDirectory)
    val visibleDirs = dirs.filter(_.readAllowedFrom(user)).map(_.asDirectory)
    val visibleObjectFrames = objects.filter(_.readAllowedFrom(user)).map(_.asObjectFrame)
    (visibleDirs, visibleObjectFrames)
  }

  def entrySearch(
      user: serving.User,
      dir: Directory,
      query: String,
      includeNotes: Boolean): (Iterable[DirectoryEntry], Iterable[ObjectFrame]) = {
    val terms = query.split(" ").map(_.toLowerCase)
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
      .filter { entry =>
        val baseName = entry.path.last.name
        val notes = "" // TODO: Maybe look at workspace description.
        terms.forall {
          term =>
            baseName.toLowerCase.contains(term) ||
            (includeNotes && notes.toLowerCase.contains(term))
        }
      }
    (dirs, objects)
  }
}

class BigGraphController(val env: SparkFreeEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val entityProgressManager: EntityProgressManager = env.entityProgressManager

  def entryList(user: serving.User, request: EntryListRequest): EntryList = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.path)
    entry.assertReadAllowedFrom(user)
    val dir = entry.asDirectory
    val (visibleDirs, visibleObjectFrames) = BigGraphController.entryList(user, dir)
    EntryList(
      request.path,
      dir.readACL,
      dir.writeACL,
      dir.writeAllowedFrom(user),
      visibleDirs.map(_.path.toString).toList,
      visibleObjectFrames.map(_.toListElementFE).toList,
    )
  }

  def entrySearch(user: serving.User, request: EntrySearchRequest): EntryList = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.basePath)
    entry.assertReadAllowedFrom(user)
    val dir = entry.asDirectory
    val (dirs, objects) = BigGraphController.entrySearch(user, dir, request.query, request.includeNotes)
    EntryList(
      request.basePath,
      dir.readACL,
      dir.writeACL,
      dir.writeAllowedFrom(user),
      dirs.map(_.path.toString).toList,
      objects.map(_.toListElementFE).toList)
  }

  private def assertNameNotExists(name: String) = {
    assert(!DirectoryEntry.fromName(name).exists, s"Entry '$name' already exists.")
  }

  def createDirectory(user: serving.User, request: CreateDirectoryRequest): Unit = metaManager.synchronized {
    val entry = DirectoryEntry.fromName(request.name)
    entry.assertParentWriteAllowedFrom(user)
    assertNameNotExists(request.name)
    val dir = entry.asNewDirectory()
    dir.setupACL(request.privacy, user)
  }

  def discardEntry(
      user: serving.User,
      request: DiscardEntryRequest): Unit = metaManager.synchronized {

    val p = DirectoryEntry.fromName(request.name)
    p.assertParentWriteAllowedFrom(user)
    p.remove()
  }

  def renameEntry(
      user: serving.User,
      request: RenameEntryRequest): Unit = metaManager.synchronized {
    val pFrom = DirectoryEntry.fromName(request.from)
    pFrom.assertParentWriteAllowedFrom(user)
    val pTo = DirectoryEntry.fromName(request.to)
    pTo.assertParentWriteAllowedFrom(user)
    if (!request.overwrite) {
      assertNameNotExists(request.to)
    }
    pFrom.copy(pTo)
    pFrom.remove()
  }

  def discardAll(user: serving.User, request: serving.Empty): Unit = metaManager.synchronized {
    assert(user.isAdmin, "Only admins can delete all objects and directories")
    DirectoryEntry.rootDirectory.remove()
    DirectoryEntry.rootDirectory.asNewDirectory()
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
    assert(
      user.isAdmin || p.aclContains(request.writeACL, user),
      s"You cannot forfeit your write access to project $p.")
    // Checking that we only give access to users with read access to all parent directories
    val gotReadAccess = request.readACL.replace(" ", "").split(",").toSet
    val gotWriteAccess = request.writeACL.replace(" ", "").split(",").toSet
    val union = gotReadAccess union gotWriteAccess
    val notAllowed =
      if (p.parent.isEmpty) Set()
      else union
        .map(email => serving.User(email, false, false))
        .filter(!p.parent.get.readAllowedFrom(_))

    assert(
      notAllowed.isEmpty,
      s"The following users don't have read access to all of the parent folders: ${notAllowed.mkString(", ")}")

    p.readACL = request.readACL
    p.writeACL = request.writeACL
  }
}
