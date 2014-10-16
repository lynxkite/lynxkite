package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
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
  isNumeric: Boolean)

case class FEProject(
  name: String,
  undoOp: String, // Name of last operation. Empty if there is nothing to undo.
  redoOp: String, // Name of next operation. Empty if there is nothing to redo.
  vertexSet: String,
  edgeBundle: String,
  notes: String,
  scalars: List[FEAttribute],
  vertexAttributes: List[FEAttribute],
  edgeAttributes: List[FEAttribute],
  segmentations: List[FESegmentation],
  opCategories: List[OperationCategory])

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
case class CreateProjectRequest(name: String, notes: String)
case class DiscardProjectRequest(name: String)
case class ProjectOperationRequest(project: String, op: FEOperationSpec)
case class ProjectFilterRequest(project: String, filters: List[FEVertexAttributeFilter])
case class ForkProjectRequest(from: String, to: String)
case class UndoProjectRequest(project: String)
case class RedoProjectRequest(project: String)

// An ordered bundle of metadata types.
case class MetaDataSeq(vertexSets: List[VertexSet] = List(),
                       edgeBundles: List[EdgeBundle] = List(),
                       vertexAttributes: List[VertexAttribute[_]] = List(),
                       edgeAttributes: List[VertexAttribute[_]] = List())

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

  def vertexSet(request: VertexSetRequest): FEVertexSet = {
    toFE(metaManager.vertexSet(request.id.asUUID))
  }

  def applyOp(request: FEOperationSpec): Unit =
    operations.applyOp(request)

  def startingOperations(request: serving.Empty): FEOperationMetas =
    FEOperationMetas(operations.getStartingOperationMetas.sortBy(_.title).toList)

  def startingVertexSets(request: serving.Empty): UIValues =
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

  def splash(request: serving.Empty): Splash = {
    val projects = ops.projects.flatMap { p =>
      Try(p.toFE) match {
        case Success(fe) =>
          Some(fe)
        case Failure(ex) =>
          log.error(s"Problem with project $p:", ex)
          None
      }
    }
    return Splash(version, projects.toList)
  }

  def project(request: ProjectRequest): FEProject = {
    val p = Project(request.name)
    return p.toFE.copy(opCategories = ops.categories(p))
  }

  def createProject(request: CreateProjectRequest): Unit = {
    val p = Project(request.name)
    p.notes = request.notes
    p.checkpointAfter("") // Initial checkpoint.
  }

  def discardProject(request: DiscardProjectRequest): Unit = {
    Project(request.name).remove()
  }

  def projectOp(request: ProjectOperationRequest): Unit = ops.apply(request)

  def filterProject(request: ProjectFilterRequest): Unit = {
    val project = Project(request.project)
    val vertexSet = project.vertexSet
    assert(vertexSet != null)
    assert(request.filters.nonEmpty)
    val embedding = FEFilters.embedFilteredVertices(vertexSet, request.filters)
    project.checkpoint("Filter") {
      project.pullBackWithInjection(embedding)
    }
  }

  def forkProject(request: ForkProjectRequest): Unit = {
    Project(request.from).copy(Project(request.to))
  }

  def undoProject(request: UndoProjectRequest): Unit = {
    Project(request.project).undo()
  }

  def redoProject(request: RedoProjectRequest): Unit = {
    Project(request.project).redo()
  }
}

abstract class Operation(val project: Project, val category: Operation.Category) {
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
}
object Operation {
  case class Category(title: String, color: String, visible: Boolean = true) {
    val icon = title.take(1) // The "icon" in the operation toolbox.
  }
}

abstract class OperationRepository(env: BigGraphEnvironment) {
  implicit val manager = env.metaGraphManager

  def projects: Seq[Project] = {
    val dirs = if (manager.tagExists("projects")) manager.lsTag("projects") else Nil
    dirs.map(p => Project(p.path.last.name))
  }

  private val operations = mutable.Buffer[Project => Operation]()
  def register(factory: Project => Operation): Unit = operations += factory
  private def forProject(project: Project) = operations.map(_(project))

  def categories(project: Project): List[OperationCategory] = {
    val cats = forProject(project).groupBy(_.category).toList
    cats.filter(_._1.visible).sortBy(_._1.title).map {
      case (cat, ops) =>
        val feOps = ops.map(_.toFE).sortBy(_.title).toList
        OperationCategory(cat.title, cat.icon, cat.color, feOps)
    }
  }

  def uIProjects: List[UIValue] = UIValue.list(projects.map(_.projectName).toList)

  def apply(req: ProjectOperationRequest): Unit = manager.synchronized {
    val p = Project(req.project)
    val ops = forProject(p).filter(_.id == req.op.id)
    assert(ops.nonEmpty, s"Cannot find operation: ${req.op.id}")
    assert(ops.size == 1, s"Operation not unique: ${req.op.id}")
    p.checkpoint(ops.head.title) {
      ops.head.apply(req.op.parameters)
    }
  }
}
