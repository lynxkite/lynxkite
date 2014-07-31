package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.{ Failure, Success, Try }

case class FEStatus(success: Boolean, failureReason: String = "")
object FEStatus {
  val success = FEStatus(true)
  def failure(failureReason: String) = FEStatus(false, failureReason)
}

case class VertexSetRequest(id: String)

// Something with a display name and an internal ID.
case class UIValue(
  id: String,
  title: String)
object UIValue {
  def fromEntity(e: MetaGraphEntity): UIValue = UIValue(e.gUID.toString, e.toString)
  def seq(list: String*) = list.map(id => UIValue(id, id))
}

case class FEOperationMeta(
  id: String,
  title: String,
  parameters: Seq[FEOperationParameterMeta],
  enabled: FEStatus = FEStatus.success)

case class FEOperationParameterMeta(
    id: String,
    title: String,
    kind: String = "scalar", // vertex-set, edge-bundle, ...
    defaultValue: String = "",
    options: Seq[UIValue] = Seq()) {

  val validKinds = Seq(
    "scalar", "vertex-set", "edge-bundle", "vertex-attribute", "edge-attribute",
    "multi-vertex-attribute", "multi-edge-attribute")
  require(validKinds.contains(kind), s"'$kind' is not a valid parameter type")
}

case class FEEdgeBundle(
  id: String,
  title: String,
  source: UIValue,
  destination: UIValue,
  attributes: Seq[UIValue])

case class FEVertexSet(
  id: String,
  title: String,
  inEdges: Seq[FEEdgeBundle],
  outEdges: Seq[FEEdgeBundle],
  localEdges: Seq[FEEdgeBundle],
  attributes: Seq[UIValue],
  ops: Seq[FEOperationMeta])

case class FEOperationSpec(
  id: String,
  parameters: Map[String, String])

abstract class FEOperation {
  val id: String = getClass.getName
  val title: String
  val category: String
  val parameters: Seq[FEOperationParameterMeta]
  lazy val starting = parameters.forall(_.kind == "scalar")
  def apply(params: Map[String, String]): FEStatus
}

case class FEProject(
  id: String,
  vertexCount: Long,
  edgeCount: Long,
  notes: String,
  vertexAttributes: Seq[UIValue],
  edgeAttributes: Seq[UIValue],
  segmentations: Seq[UIValue],
  opCategories: Seq[OperationCategory])

class Project(val id: String)(implicit manager: MetaGraphManager) {
  val path: SymbolPath = s"projects/$id"
  def toFE(implicit dm: DataManager): FEProject = {
    val notes = manager.scalarOf[String](path / "notes").value
    FEProject(id, 0, 0, notes, Seq(), Seq(), Seq(), Seq())
  }
  def vertexSet = ifExists(path / "vertexSet") { manager.vertexSet(_) }
  def vertexSet_=(e: VertexSet) = {
    if (e != vertexSet) {
      // TODO: "Induce" the edges and attributes to the new vertex set.
      edgeBundle = null
      vertexAttributes = Map()
    }
    set(path / "vertexSet", e)
  }

  def edgeBundle = ifExists(path / "edgeBundle") { manager.edgeBundle(_) }
  def edgeBundle_=(e: EdgeBundle) = {
    if (e != edgeBundle) {
      // TODO: "Induce" the attributes to the new edge bundle.
      edgeAttributes = Map()
    }
    set(path / "edgeBundle", e)
  }

  def vertexAttributes = new Holder[VertexAttribute[_]]("vertexAttributes")
  def vertexAttributes_=(attrs: Map[String, VertexAttribute[_]]) = {
    ifExists(path / "vertexAttributes") { manager.rmTag(_) }
    for ((name, attr) <- attrs) {
      manager.setTag(path / "vertexAttributes" / name, attr)
    }
  }
  def vertexAttributeNames[T: TypeTag] = vertexAttributes.collect {
    case (name, attr) if attr.is[T] => name
  }
  def edgeAttributes = new Holder[EdgeAttribute[_]]("edgeAttributes")
  def edgeAttributes_=(attrs: Map[String, EdgeAttribute[_]]) = {
    ifExists(path / "edgeAttributes") { manager.rmTag(_) }
    for ((name, attr) <- attrs) {
      manager.setTag(path / "edgeAttributes" / name, attr)
    }
  }
  def edgeAttributeNames[T: TypeTag] = edgeAttributes.collect {
    case (name, attr) if attr.is[T] => name
  }
  def segmentations = new Holder[VertexSet]("edgeAttributes")
  def segmentations_=(attrs: Map[String, VertexSet]) = {
    ifExists(path / "segmentations") { manager.rmTag(_) }
    for ((name, attr) <- attrs) {
      manager.setTag(path / "segmentations" / name, attr)
    }
  }
  def segmentationNames = segmentations.map { case (name, attr) => name }

  private def ifExists[T](tag: SymbolPath)(fn: SymbolPath => T): T =
    if (manager.tagExists(tag)) { fn(tag) } else { null }
  private def set(tag: SymbolPath, entity: MetaGraphEntity): Unit =
    if (entity == null) manager.rmTag(tag) else manager.setTag(tag, entity)
  private def ls(dir: String) = if (manager.tagExists(path / dir)) manager.lsTag(path / dir) else Nil

  class Holder[T <: MetaGraphEntity](dir: String) extends Iterable[(String, T)] {
    def update(name: String, entity: T) =
      manager.setTag(path / dir / name, entity)
    def apply(name: String) =
      manager.entity(path / dir / name).asInstanceOf[T]
    def iterator =
      ls(dir).map(_.last.name).map(p => p -> apply(p)).iterator
  }
}

object Project {
  def apply(id: String)(implicit metaManager: MetaGraphManager): Project = new Project(id)
}

case class ProjectRequest(id: String)
case class Splash(projects: Seq[FEProject])
case class OperationCategory(title: String, ops: Seq[FEOperationMeta])
case class CreateProjectRequest(id: String, notes: String)
case class ProjectOperationRequest(project: String, op: FEOperationSpec)

// An ordered bundle of metadata types.
case class MetaDataSeq(vertexSets: Seq[VertexSet] = Seq(),
                       edgeBundles: Seq[EdgeBundle] = Seq(),
                       vertexAttributes: Seq[VertexAttribute[_]] = Seq(),
                       edgeAttributes: Seq[EdgeAttribute[_]] = Seq())

class FEOperationRepository(env: BigGraphEnvironment) {
  implicit val manager = env.metaGraphManager
  implicit val dataManager = env.dataManager

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
    val vertexSets = if (neighbors.nonEmpty) vs +: neighbors.toSeq else vs +: strangers.toSeq
    val edgeBundles = (in ++ out).toSeq
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
      val params: Seq[FEOperationParameterMeta] = op.parameters.flatMap {
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

  def applyOp(spec: FEOperationSpec): FEStatus =
    operations(spec.id).apply(spec.parameters)

  private val operations = mutable.Map[String, FEOperation]()
}

/**
 * Logic for processing requests
 */

class BigGraphController(env: BigGraphEnvironment) {
  implicit val metaManager = env.metaGraphManager
  implicit val dataManager = env.dataManager
  val operations = new FEOperations(env)

  private def toFE(vs: VertexSet): FEVertexSet = {
    val in = metaManager.incomingBundles(vs).toSet.filter(metaManager.isVisible(_))
    val out = metaManager.outgoingBundles(vs).toSet.filter(metaManager.isVisible(_))
    val local = in & out

    FEVertexSet(
      id = vs.gUID.toString,
      title = vs.toString,
      inEdges = (in -- local).toSeq.map(toFE(_)),
      outEdges = (out -- local).toSeq.map(toFE(_)),
      localEdges = local.toSeq.map(toFE(_)),
      attributes = metaManager.attributes(vs).filter(metaManager.isVisible(_)).map(UIValue.fromEntity(_)),
      ops = operations.getApplicableOperationMetas(vs).sortBy(_.title))
  }

  private def toFE(eb: EdgeBundle): FEEdgeBundle = {
    FEEdgeBundle(
      id = eb.gUID.toString,
      title = eb.toString,
      source = UIValue.fromEntity(eb.srcVertexSet),
      destination = UIValue.fromEntity(eb.dstVertexSet),
      attributes = metaManager.attributes(eb).filter(metaManager.isVisible(_)).map(UIValue.fromEntity(_)))
  }

  def vertexSet(request: VertexSetRequest): FEVertexSet = {
    toFE(metaManager.vertexSet(request.id.asUUID))
  }

  def applyOp(request: FEOperationSpec): FEStatus =
    operations.applyOp(request)

  def startingOperations(request: serving.Empty): Seq[FEOperationMeta] =
    operations.getStartingOperationMetas.sortBy(_.title)

  def startingVertexSets(request: serving.Empty): Seq[UIValue] =
    metaManager.allVertexSets
      .filter(_.source.inputs.all.isEmpty)
      .filter(metaManager.isVisible(_))
      .map(UIValue.fromEntity(_)).toSeq

  // Project view stuff below.

  val ops = new Operations(env)

  def splash(request: serving.Empty): Splash = {
    val dirs = if (metaManager.tagExists("projects")) metaManager.lsTag("projects") else Nil
    val projects = dirs.map(p => Project(p.path.last.name).toFE)
    return Splash(projects = projects)
  }

  def project(request: ProjectRequest): FEProject = {
    val p = Project(request.id)
    return p.toFE.copy(opCategories = ops.categories(p))
  }

  def createProject(request: CreateProjectRequest): serving.Empty = {
    val notes = graph_operations.CreateStringScalar(request.notes)().result.created
    metaManager.setTag(s"projects/${request.id}/notes", notes)
    return serving.Empty()
  }
}

abstract class Operation(val project: Project, val category: String) {
  def id = title.replace(" ", "-")
  def title: String
  def parameters: Seq[FEOperationParameterMeta]
  def enabled: FEStatus
  def apply(params: Map[String, String]): FEStatus
  def toFE: FEOperationMeta = FEOperationMeta(id, title, parameters, enabled)
  protected def vertexAttributes[T: TypeTag] =
    project.vertexAttributeNames[T].map(name => UIValue(name, name)).toSeq
  protected def edgeAttributes[T: TypeTag] =
    project.edgeAttributeNames[T].map(name => UIValue(name, name)).toSeq
  protected def segmentations =
    project.segmentationNames.map(name => UIValue(name, name)).toSeq
}

abstract class OperationRepository(env: BigGraphEnvironment) {
  implicit val manager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  private val operations = mutable.Buffer[Project => Operation]()
  def register(factory: Project => Operation): Unit = operations += factory
  private def forProject(project: Project) = operations.map(_(project))

  def categories(project: Project): Seq[OperationCategory] = {
    forProject(project).groupBy(_.category).map {
      case (cat, ops) => OperationCategory(cat, ops.map(_.toFE))
    }.toSeq
  }

  def apply(req: ProjectOperationRequest): FEStatus = {
    val p = Project(req.project)
    val ops = forProject(p).filter(_.id == req.op.id)
    assert(ops.size == 1, s"Operation not unique: ${req.op.id}")
    ops.head.apply(req.op.parameters)
  }
}
