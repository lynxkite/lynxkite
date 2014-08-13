package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.MetaGraphManager.StringAsUUID
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.{ Failure, Success, Try }

case class FEStatus(success: Boolean, failureReason: String = "") {
  def ||(other: FEStatus) = if (success) this else other
  def &&(other: FEStatus) = if (success) other else this
}
object FEStatus {
  val success = FEStatus(true)
  def failure(failureReason: String) = FEStatus(false, failureReason)
  def assert(condition: Boolean, failureReason: String) =
    if (condition) success else failure(failureReason)
}

case class VertexSetRequest(id: String)

// Something with a display name and an internal ID.
case class UIValue(
  id: String,
  title: String)
object UIValue {
  def fromEntity(e: MetaGraphEntity): UIValue = UIValue(e.gUID.toString, e.toString)
  def seq(list: Seq[String]) = list.map(id => UIValue(id, id))
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
    options: Seq[UIValue] = Seq(),
    multipleChoice: Boolean = false) {

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
  name: String,
  vertexSet: String,
  edgeBundle: String,
  vertexCount: Long,
  edgeCount: Long,
  notes: String,
  vertexAttributes: Seq[UIValue],
  edgeAttributes: Seq[UIValue],
  segmentations: Seq[UIValue],
  opCategories: Seq[OperationCategory])

class Project(val projectName: String)(implicit manager: MetaGraphManager) {
  val path: SymbolPath = s"projects/$projectName"
  def toFE(implicit dm: DataManager): FEProject = {
    val notes = manager.scalarOf[String](path / "notes").value
    val vs = Option(vertexSet).map(_.gUID.toString).getOrElse("")
    val eb = Option(edgeBundle).map(_.gUID.toString).getOrElse("")
    // For now, counts are calculated here. TODO: Make them respond to soft filters.
    val vsCount = if (vertexSet == null) 0 else {
      val op = graph_operations.CountVertices()
      op(op.vertices, vertexSet).result.count.value
    }
    val esCount = if (edgeBundle == null) 0 else {
      val op = graph_operations.CountEdges()
      op(op.edges, edgeBundle).result.count.value
    }
    FEProject(
      projectName, vs, eb,
      vsCount, esCount, notes,
      vertexAttributes.map { case (name, attr) => UIValue(attr.gUID.toString, name) }.toSeq,
      edgeAttributes.map { case (name, attr) => UIValue(attr.gUID.toString, name) }.toSeq,
      segmentations.map { case (name, seg) => UIValue(seg.gUID.toString, name) }.toSeq,
      opCategories = Seq())
  }

  def vertexSet = existing(path / "vertexSet").map(manager.vertexSet(_)).getOrElse(null)
  def vertexSet_=(e: VertexSet) = {
    if (e != vertexSet) {
      // TODO: "Induce" the edges and attributes to the new vertex set.
      edgeBundle = null
      vertexAttributes = Map()
    }
    set(path / "vertexSet", e)
  }
  def pullBackWithInjection(injection: EdgeBundle): Unit = {
    assert(injection.properties.compliesWith(EdgeBundleProperties.injection))
    assert(injection.dstVertexSet.gUID == vertexSet.gUID)
    val origVS = vertexSet
    val origVAttrs = vertexAttributes.toIndexedSeq
    val origEB = edgeBundle
    val origEAttrs = edgeAttributes.toIndexedSeq
    vertexSet = injection.srcVertexSet
    origVAttrs.foreach {
      case (name, attr) =>
        vertexAttributes(name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(attr, injection)
    }

    val iop = graph_operations.InducedEdgeBundle()
    edgeBundle = iop(iop.srcInjection, injection)(iop.dstInjection, injection)(iop.edges, origEB)
      .result.induced

    origEAttrs.foreach {
      case (name, attr) =>
        edgeAttributes(name) =
          graph_operations.PulledOverEdgeAttribute.pullAttributeTo(attr, edgeBundle)
    }
  }

  def edgeBundle = existing(path / "edgeBundle").map(manager.edgeBundle(_)).getOrElse(null)
  def edgeBundle_=(e: EdgeBundle) = {
    if (e != edgeBundle) {
      assert(e == null || vertexSet != null, s"No vertex set for project $projectName")
      assert(e == null || e.srcVertexSet == vertexSet, s"Edge bundle does not match vertex set for project $projectName")
      assert(e == null || e.dstVertexSet == vertexSet, s"Edge bundle does not match vertex set for project $projectName")
      // TODO: "Induce" the attributes to the new edge bundle.
      edgeAttributes = Map()
    }
    set(path / "edgeBundle", e)
  }

  def vertexAttributes = new VertexAttributeHolder
  def vertexAttributes_=(attrs: Map[String, VertexAttribute[_]]) = {
    existing(path / "vertexAttributes").foreach(manager.rmTag(_))
    assert(attrs.isEmpty || vertexSet != null, s"No vertex set for project $projectName")
    for ((name, attr) <- attrs) {
      assert(attr.vertexSet == vertexSet, s"Vertex attribute $name does not match vertex set for project $projectName")
      manager.setTag(path / "vertexAttributes" / name, attr)
    }
  }
  def vertexAttributeNames[T: TypeTag] = vertexAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def edgeAttributes = new EdgeAttributeHolder
  def edgeAttributes_=(attrs: Map[String, EdgeAttribute[_]]) = {
    existing(path / "edgeAttributes").foreach(manager.rmTag(_))
    assert(attrs.isEmpty || edgeBundle != null, s"No edge bundle for project $projectName")
    for ((name, attr) <- attrs) {
      assert(attr.edgeBundle == edgeBundle, s"Edge attribute $name does not match edge bundle for project $projectName")
      manager.setTag(path / "edgeAttributes" / name, attr)
    }
  }
  def edgeAttributeNames[T: TypeTag] = edgeAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def segmentations = new SegmentationHolder
  def segmentations_=(segs: Map[String, VertexSet]) = {
    existing(path / "segmentations").foreach(manager.rmTag(_))
    assert(segs.isEmpty || vertexSet != null, s"No vertex set for project $projectName")
    for ((name, seg) <- segs) {
      // TODO: Assert that this is a segmentation for vertexSet.
      manager.setTag(path / "segmentations" / name, seg)
    }
  }
  def segmentationNames = segmentations.map { case (name, attr) => name }.toSeq

  private def existing(tag: SymbolPath): Option[SymbolPath] =
    if (manager.tagExists(tag)) Some(tag) else None
  private def set(tag: SymbolPath, entity: MetaGraphEntity): Unit = {
    if (entity == null) {
      existing(tag).foreach(manager.rmTag(_))
    } else {
      manager.setTag(tag, entity)
    }
  }
  private def ls(dir: String) = if (manager.tagExists(path / dir)) manager.lsTag(path / dir) else Nil

  abstract class Holder[T <: MetaGraphEntity](dir: String) extends Iterable[(String, T)] {
    def validate(name: String, entity: T): Unit
    def update(name: String, entity: T) = {
      validate(name, entity)
      manager.setTag(path / dir / name, entity)
    }
    def apply(name: String) =
      manager.entity(path / dir / name).asInstanceOf[T]
    def iterator =
      ls(dir).map(_.last.name).map(p => p -> apply(p)).iterator
  }
  class VertexAttributeHolder extends Holder[VertexAttribute[_]]("vertexAttributes") {
    def validate(name: String, attr: VertexAttribute[_]) =
      assert(attr.vertexSet == vertexSet, s"Vertex attribute $name does not match vertex set for project $projectName")
  }
  class EdgeAttributeHolder extends Holder[EdgeAttribute[_]]("edgeAttributes") {
    def validate(name: String, attr: EdgeAttribute[_]) =
      assert(attr.edgeBundle == edgeBundle, s"Edge attribute $name does not match edge bundle for project $projectName")
  }
  class SegmentationHolder extends Holder[VertexSet]("segmentations") {
    def validate(name: String, seg: VertexSet) = () // TODO: Validate segmentation.
  }
}

object Project {
  def apply(projectName: String)(implicit metaManager: MetaGraphManager): Project = new Project(projectName)
}

case class ProjectRequest(name: String)
case class Splash(projects: Seq[FEProject])
case class OperationCategory(title: String, ops: Seq[FEOperationMeta])
case class CreateProjectRequest(name: String, notes: String)
case class ProjectOperationRequest(project: String, op: FEOperationSpec)
case class ProjectFilterRequest(project: String, filters: Seq[FEVertexAttributeFilter])

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
    val p = Project(request.name)
    return p.toFE.copy(opCategories = ops.categories(p))
  }

  def createProject(request: CreateProjectRequest): serving.Empty = {
    val notes = graph_operations.CreateStringScalar(request.notes)().result.created
    metaManager.setTag(s"projects/${request.name}/notes", notes)
    return serving.Empty()
  }

  def projectOp(request: ProjectOperationRequest): FEStatus = ops.apply(request)

  def filterProject(request: ProjectFilterRequest): FEStatus = {
    val project = Project(request.project)
    val vertexSet = project.vertexSet
    assert(vertexSet != null)
    assert(request.filters.nonEmpty)
    val embedding = FEFilters.embedFilteredVertices(vertexSet, request.filters)
    project.pullBackWithInjection(embedding)
    FEStatus.success
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
    UIValue.seq(project.vertexAttributeNames[T])
  protected def edgeAttributes[T: TypeTag] =
    UIValue.seq(project.edgeAttributeNames[T])
  protected def segmentations =
    UIValue.seq(project.segmentationNames)
  protected def hasVertexSet = if (project.vertexSet == null) FEStatus.failure("No vertices.") else FEStatus.success
  protected def hasNoVertexSet = if (project.vertexSet != null) FEStatus.failure("Vertices already exist.") else FEStatus.success
  protected def hasEdgeBundle = if (project.edgeBundle == null) FEStatus.failure("No edges.") else FEStatus.success
  protected def hasNoEdgeBundle = if (project.edgeBundle != null) FEStatus.failure("Edges already exist.") else FEStatus.success
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
    }.toSeq.sortBy(_.title)
  }

  def apply(req: ProjectOperationRequest): FEStatus = {
    val p = Project(req.project)
    val ops = forProject(p).filter(_.id == req.op.id)
    assert(ops.size == 1, s"Operation not unique: ${req.op.id}")
    ops.head.apply(req.op.parameters)
  }
}
