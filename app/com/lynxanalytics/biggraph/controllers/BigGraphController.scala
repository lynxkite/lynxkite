package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.{ Failure, Success, Try }

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

case class VertexSetRequest(id: String)

// Something with a display name and an internal ID.
case class UIValue(
  id: String,
  title: String)
object UIValue {
  def fromEntity(e: MetaGraphEntity): UIValue = UIValue(e.gUID.toString, e.toString)
  def list(list: List[String]) = list.map(id => UIValue(id, id))
}

case class UIValues(values: List[UIValue])

case class FEOperationMeta(
  id: String,
  title: String,
  parameters: List[FEOperationParameterMeta],
  status: FEStatus = FEStatus.enabled,
  description: String = "")

case class FEOperationMetas(ops: List[FEOperationMeta])

case class FEOperationParameterMeta(
    id: String,
    title: String,
    kind: String = "scalar", // vertex-set, edge-bundle, ...
    defaultValue: String = "",
    options: List[UIValue] = List(),
    multipleChoice: Boolean = false) {

  val validKinds = Seq(
    "scalar", "vertex-set", "edge-bundle", "vertex-attribute", "edge-attribute",
    "multi-vertex-attribute", "multi-edge-attribute", "file")
  require(validKinds.contains(kind), s"'$kind' is not a valid parameter type")
}

case class FEEdgeBundle(
  id: String,
  title: String,
  source: UIValue,
  destination: UIValue,
  attributes: List[UIValue])

case class FEVertexSet(
  id: String,
  title: String,
  inEdges: List[FEEdgeBundle],
  outEdges: List[FEEdgeBundle],
  localEdges: List[FEEdgeBundle],
  attributes: List[UIValue],
  ops: List[FEOperationMeta])

case class FEOperationSpec(
  id: String,
  parameters: Map[String, String])

abstract class FEOperation {
  val id: String = getClass.getName
  val title: String
  val category: String
  val parameters: List[FEOperationParameterMeta]
  lazy val starting = parameters.forall(_.kind == "scalar")
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
case class Splash(version: String, projects: List[FEProject])
case class OperationCategory(title: String, icon: String, color: String, ops: List[FEOperationMeta])
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
  skips: Int, // Number of checkpoints to skip.
  requests: List[ProjectOperationRequest])
case class SaveHistoryRequest(
  project: String, // New project name.
  history: AlternateHistory)
case class ProjectHistory(
    project: String,
    skips: Int, // Number of checkpoints skipped.
    steps: List[ProjectHistoryStep]) {
  def valid = steps.forall(_.status.enabled)
}
case class ProjectHistoryStep(
  op: FEOperationMeta,
  request: ProjectOperationRequest,
  status: FEStatus)

// An ordered bundle of metadata types.
case class MetaDataSeq(vertexSets: List[VertexSet] = List(),
                       edgeBundles: List[EdgeBundle] = List(),
                       vertexAttributes: List[Attribute[_]] = List(),
                       edgeAttributes: List[Attribute[_]] = List())

class FEOperationRepository(env: BigGraphEnvironment) {
  implicit val manager = env.metaGraphManager

  def registerOperation(op: FEOperation): Unit = {
    assert(!operations.contains(op.id), s"Already registered: ${op.id}")
    operations(op.id) = op
  }

  def getStartingOperationMetas: Seq[FEOperationMeta] = {
    toSimpleMetas(operations.values.toSeq.filter(_.starting))
  }

  private def toSimpleMetas(ops: Seq[FEOperation]): Seq[FEOperationMeta] = {
    ops.map {
      op => FEOperationMeta(op.id, op.title, op.parameters)
    }
  }

  // Get non-starting operations, based on a current view.
  def getApplicableOperationMetas(vs: VertexSet): Seq[FEOperationMeta] =
    getApplicableOperationMetas(optionsFor(vs))

  def optionsFor(vs: VertexSet): MetaDataSeq = {
    val in = manager.incomingBundles(vs).toSet
    val out = manager.outgoingBundles(vs).toSet
    val neighbors = in.map(_.srcVertexSet) ++ out.map(_.dstVertexSet) - vs
    val strangers = manager.allVertexSets - vs
    // List every vertex set if there are no neighbors.
    val vertexSets = if (neighbors.nonEmpty) vs +: neighbors.toList else vs +: strangers.toList
    val edgeBundles = (in ++ out).toList
    val vertexAttributes = vertexSets.flatMap(manager.attributes(_))
    val edgeAttributes = edgeBundles.flatMap(manager.attributes(_))
    return MetaDataSeq(
      vertexSets.filter(manager.isVisible(_)),
      edgeBundles.filter(manager.isVisible(_)),
      vertexAttributes.filter(manager.isVisible(_)),
      edgeAttributes.filter(manager.isVisible(_)))
  }

  def getApplicableOperationMetas(options: MetaDataSeq): Seq[FEOperationMeta] = {
    val vertexSets = options.vertexSets.map(UIValue.fromEntity(_))
    val edgeBundles = options.edgeBundles.map(UIValue.fromEntity(_))
    val vertexAttributes = options.vertexAttributes.map(UIValue.fromEntity(_))
    val edgeAttributes = options.edgeAttributes.map(UIValue.fromEntity(_))
    operations.values.toSeq.filterNot(_.starting).flatMap { op =>
      val params: List[FEOperationParameterMeta] = op.parameters.flatMap {
        case p if p.kind == "vertex-set" => vertexSets.headOption.map(
          first => p.copy(options = vertexSets, defaultValue = first.id))
        case p if p.kind == "edge-bundle" => edgeBundles.headOption.map(
          first => p.copy(options = edgeBundles, defaultValue = first.id))
        case p if p.kind == "vertex-attribute" => vertexAttributes.headOption.map(
          first => p.copy(options = vertexAttributes, defaultValue = first.id))
        case p if p.kind == "edge-attribute" => edgeAttributes.headOption.map(
          first => p.copy(options = edgeAttributes, defaultValue = first.id))
        case p if p.kind == "multi-vertex-attribute" => Some(p.copy(options = vertexAttributes))
        case p if p.kind == "multi-edge-attribute" => Some(p.copy(options = edgeAttributes))
        case p => Some(p)
      }
      if (params.length == op.parameters.length) {
        // There is a valid option for every parameter, so this is a legitimate operation.
        Some(FEOperationMeta(op.id, op.title, params))
      } else {
        None
      }
    }
  }

  def applyOp(spec: FEOperationSpec): Unit =
    operations(spec.id).apply(spec.parameters)

  private val operations = mutable.Map[String, FEOperation]()
}

/**
 * Logic for processing requests
 */

class BigGraphController(val env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  val operations = new FEOperations(env)

  private def toFE(vs: VertexSet): FEVertexSet = {
    val in = metaManager.incomingBundles(vs).toSet.filter(metaManager.isVisible(_))
    val out = metaManager.outgoingBundles(vs).toSet.filter(metaManager.isVisible(_))
    val local = in & out
    val visibleAttributes = metaManager.attributes(vs).filter(metaManager.isVisible(_))

    FEVertexSet(
      id = vs.gUID.toString,
      title = vs.toString,
      inEdges = (in -- local).toList.map(toFE(_)),
      outEdges = (out -- local).toList.map(toFE(_)),
      localEdges = local.toList.map(toFE(_)),
      attributes = visibleAttributes.map(UIValue.fromEntity(_)).toList,
      ops = operations.getApplicableOperationMetas(vs).sortBy(_.title).toList)
  }

  private def toFE(eb: EdgeBundle): FEEdgeBundle = {
    val visibleAttributes = metaManager.attributes(eb).filter(metaManager.isVisible(_))
    FEEdgeBundle(
      id = eb.gUID.toString,
      title = eb.toString,
      source = UIValue.fromEntity(eb.srcVertexSet),
      destination = UIValue.fromEntity(eb.dstVertexSet),
      attributes = visibleAttributes.map(UIValue.fromEntity(_)).toList)
  }

  def vertexSet(user: serving.User, request: VertexSetRequest): FEVertexSet = {
    toFE(metaManager.vertexSet(request.id.asUUID))
  }

  def applyOp(user: serving.User, request: FEOperationSpec): Unit =
    operations.applyOp(request)

  def startingOperations(user: serving.User, request: serving.Empty): FEOperationMetas =
    FEOperationMetas(operations.getStartingOperationMetas.sortBy(_.title).toList)

  def startingVertexSets(user: serving.User, request: serving.Empty): UIValues =
    UIValues(metaManager.allVertexSets
      .filter(_.source.inputs.all.isEmpty)
      .filter(metaManager.isVisible(_))
      .map(UIValue.fromEntity(_)).toList)

  // Project view stuff below.

  lazy val version = try {
    scala.io.Source.fromFile(util.Properties.userDir + "/version").mkString
  } catch {
    case e: java.io.IOException => ""
  }

  val ops = new Operations(env)

  def splash(user: serving.User, request: serving.Empty): Splash = metaManager.synchronized {
    val projects = Operation.projects.filter(_.readAllowedFrom(user)).map(_.toFE)
    return Splash(version, projects.toList)
  }

  def project(user: serving.User, request: ProjectRequest): FEProject = metaManager.synchronized {
    val p = Project(request.name)
    p.assertReadAllowedFrom(user)
    return p.toFE.copy(opCategories = ops.categories(user, p))
  }

  def createProject(user: serving.User, request: CreateProjectRequest): Unit = metaManager.synchronized {
    val p = Project(request.name)
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
    val p = Project(request.name)
    p.assertWriteAllowedFrom(user)
    p.remove()
  }

  def renameProject(user: serving.User, request: RenameProjectRequest): Unit = metaManager.synchronized {
    val p = Project(request.from)
    p.assertWriteAllowedFrom(user)
    p.copy(Project(request.to))
    p.remove()
  }

  def projectOp(user: serving.User, request: ProjectOperationRequest): Unit = metaManager.synchronized {
    val p = Project(request.project)
    p.assertWriteAllowedFrom(user)
    ops.apply(user, request)
  }

  def filterProject(user: serving.User, request: ProjectFilterRequest): Unit = metaManager.synchronized {
    val project = Project(request.project)
    project.assertWriteAllowedFrom(user)
    val vertexSet = project.vertexSet
    assert(vertexSet != null, s"No vertex set for $project.")
    assert(request.vertexFilters.nonEmpty || request.edgeFilters.nonEmpty,
      "No filters specified.")
    val vertexFilters = request.vertexFilters.map { f =>
      val attr = project.vertexAttributes(f.attributeName)
      FEVertexAttributeFilter(attr.gUID.toString, f.valueSpec)
    }
    val edgeFilters = request.edgeFilters.map { f =>
      val attr = project.edgeAttributes(f.attributeName)
      FEVertexAttributeFilter(attr.gUID.toString, f.valueSpec)
    }
    val vertexEmbedding = FEFilters.embedFilteredVertices(vertexSet, vertexFilters, heavy = true)
    val filterStrings = (request.vertexFilters ++ request.edgeFilters).map {
      f => s"${f.attributeName} ${f.valueSpec}"
    }
    project.checkpoint("Filter " + filterStrings.mkString(", ")) {
      project.pullBack(vertexEmbedding)
      if (edgeFilters.nonEmpty) {
        val edgeEmbedding =
          FEFilters.embedFilteredVertices(project.edgeBundle.idSet, edgeFilters, heavy = true)
        project.pullBackEdges(edgeEmbedding)
      }
    }
  }

  def forkProject(user: serving.User, request: ForkProjectRequest): Unit = metaManager.synchronized {
    val p1 = Project(request.from)
    val p2 = Project(request.to)
    p1.assertReadAllowedFrom(user)
    assert(!Operation.projects.contains(p2), s"Project $p2 already exists.")
    p1.copy(p2)
    if (!p2.writeAllowedFrom(user)) {
      p2.writeACL += "," + user.email
    }
  }

  def undoProject(user: serving.User, request: UndoProjectRequest): Unit = metaManager.synchronized {
    val p = Project(request.project)
    p.assertWriteAllowedFrom(user)
    p.undo()
  }

  def redoProject(user: serving.User, request: RedoProjectRequest): Unit = metaManager.synchronized {
    val p = Project(request.project)
    p.assertWriteAllowedFrom(user)
    p.redo()
  }

  def changeProjectSettings(user: serving.User, request: ProjectSettingsRequest): Unit = metaManager.synchronized {
    val p = Project(request.project)
    p.assertWriteAllowedFrom(user)
    // To avoid accidents, a user cannot remove themselves from the write ACL.
    assert(p.aclContains(request.writeACL, user),
      s"You cannot forfeit your write access to project $p.")
    p.readACL = request.readACL
    p.writeACL = request.writeACL
  }

  def getHistory(user: serving.User, request: HistoryRequest): ProjectHistory = metaManager.synchronized {
    val p = Project(request.project)
    p.assertReadAllowedFrom(user)
    withCheckpoints(p) { ps =>
      // Find the lowest number of skips to get a valid history.
      (0 to p.checkpointCount - 1).view.map { skips =>
        val remaining = ps.drop(skips + 1)
        if (remaining.forall(_.lastOperationRequest.nonEmpty)) {
          val requests = remaining.map(_.lastOperationRequest.get)
          val h = validateHistory(user, AlternateHistory(request.project, skips, requests.toList))
          if (h.valid) Some(h) else None
        } else None
      }.find(_.nonEmpty).get.get
    }
  }

  // Expand each checkpoint of a project to a separate project, run the code, then clean up.
  private def withCheckpoints[T](p: Project)(code: Seq[Project] => T): T = metaManager.synchronized {
    val tmpDir = s"!tmp-$Timestamp"
    val ps = (1 to p.checkpointCount).map { i =>
      val tmp = Project(s"$tmpDir/$i")
      assert(!Operation.projects.contains(tmp), s"Project $tmp already exists.")
      p.copyCheckpoint(i, tmp)
      tmp
    }
    try {
      code(ps)
    } finally {
      Project(tmpDir).remove
    }
  }

  // Returns the evaluated alternate history, and optionally copies the resulting state into a new project.
  private def alternateHistory(user: serving.User, request: AlternateHistory, copyTo: Option[Project]): ProjectHistory = metaManager.synchronized {
    val p = Project(request.project)
    p.assertReadAllowedFrom(user)
    withCheckpoints(p) { ps =>
      val state = ps(request.skips)
      val steps = request.requests.foldLeft(List[ProjectHistoryStep]()) { (steps, request) =>
        // The request may refer to a segmentation. Figure out the recipient project.
        val relativeProject = new SymbolPath(SymbolPath.fromString(request.project).tail)
        val recipient = Project(state.projectName + "/" + relativeProject)
        val ctx = Operation.Context(user, recipient)
        val op = ops.opById(ctx, request.op.id)
        if (op.enabled.enabled) {
          try {
            recipient.checkpoint(op.toString, request) {
              op.apply(request.op.parameters)
            }
            steps :+ ProjectHistoryStep(op.toFE, request, FEStatus.enabled)
          } catch {
            case t: Throwable =>
              steps :+ ProjectHistoryStep(op.toFE, request, FEStatus.disabled(t.getMessage))
          }
        } else {
          steps :+ ProjectHistoryStep(op.toFE, request, op.enabled)
        }
      }
      val history = ProjectHistory(p.projectName, request.skips, steps)
      if (copyTo.nonEmpty) {
        assert(history.valid, s"Tried to copy invalid history for project $p.")
        state.copy(copyTo.get)
      }
      history
    }
  }

  def validateHistory(user: serving.User, request: AlternateHistory): ProjectHistory = metaManager.synchronized {
    alternateHistory(user, request, None)
  }

  def saveHistory(user: serving.User, request: SaveHistoryRequest): Unit = metaManager.synchronized {
    val p2 = Project(request.project)
    assert(!Operation.projects.contains(p2), s"Project $p2 already exists.")
    alternateHistory(user, request.history, Some(p2))
    if (!p2.writeAllowedFrom(user)) {
      p2.writeACL += "," + user.email
    }
  }
}

abstract class Operation(context: Operation.Context, val category: Operation.Category) {
  val project = context.project
  val user = context.user
  def id = title.replace(" ", "-")
  def title: String
  def description: String
  def parameters: List[FEOperationParameterMeta]
  def enabled: FEStatus
  def apply(params: Map[String, String]): Unit
  def toFE: FEOperationMeta = FEOperationMeta(id, title, parameters, enabled, description)
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
  // All projects that the user has read access to.
  protected def readableProjects(implicit manager: MetaGraphManager): List[UIValue] = {
    UIValue.list(Operation.projects
      .filter(_.readAllowedFrom(user))
      .map(_.projectName)
      .toList)
  }
}
object Operation {
  case class Category(title: String, color: String, visible: Boolean = true) {
    val icon = title.take(1) // The "icon" in the operation toolbox.
  }

  case class Context(user: serving.User, project: Project)

  def projects(implicit manager: MetaGraphManager): Seq[Project] = {
    val dirs = if (manager.tagExists("projects")) manager.lsTag("projects") else Nil
    dirs.map(p => Project(p.path.last.name))
  }
}

abstract class OperationRepository(env: BigGraphEnvironment) {
  implicit val manager = env.metaGraphManager

  private val operations = mutable.Buffer[Operation.Context => Operation]()
  def register(factory: Operation.Context => Operation): Unit = operations += factory
  private def forContext(context: Operation.Context) = operations.map(_(context))

  def categories(user: serving.User, project: Project): List[OperationCategory] = {
    val context = Operation.Context(user, project)
    val cats = forContext(context).groupBy(_.category).toList
    cats.filter(_._1.visible).sortBy(_._1.title).map {
      case (cat, ops) =>
        val feOps = ops.map(_.toFE).sortBy(_.title).toList
        OperationCategory(cat.title, cat.icon, cat.color, feOps)
    }
  }

  def opById(context: Operation.Context, id: String): Operation = {
    // TODO: Do this without instantiating all operations.
    val ops = forContext(context).filter(_.id == id)
    assert(ops.nonEmpty, s"Cannot find operation: ${id}")
    assert(ops.size == 1, s"Operation not unique: ${id}")
    ops.head
  }

  def apply(user: serving.User, req: ProjectOperationRequest): Unit = manager.synchronized {
    val p = Project(req.project)
    val context = Operation.Context(user, p)
    val op = opById(context, req.op.id)
    p.checkpoint(op.title, req) {
      op.apply(req.op.parameters)
    }
  }
}
