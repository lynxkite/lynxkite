// Projects are the top-level entities on the UI.
//
// A project has a vertex set, an edge bundle, and any number of attributes,
// scalars and segmentations. It represents data stored in the tag system.
// The Project instances are short-lived, they are just a rich interface for
// querying and manipulating the tags.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.serving.User

import java.io.File
import java.util.UUID
import org.apache.commons.io.FileUtils
import play.api.libs.json
import play.api.libs.json.Json
import scala.collection
import scala.util.{ Failure, Success, Try }
import scala.reflect.runtime.universe._

case class CommonProjectState(
  vertexSetGUID: Option[UUID],
  vertexAttributeGUIDs: Map[String, UUID],
  edgeBundleGUID: Option[UUID],
  edgeAttributeGUIDs: Map[String, UUID],
  scalarGUIDs: Map[String, UUID],
  segmentations: Map[String, SegmentationState],
  notes: String)
object CommonProjectState {
  def emptyState = CommonProjectState(None, Map(), None, Map(), Map(), Map(), "")
}

case class RootProjectState(
    state: CommonProjectState,
    checkpoint: Option[String],
    previousCheckpoint: Option[String],
    lastOperationDesc: String,
    lastOperationRequest: Option[SubProjectOperation]) {
}

object RootProjectState {
  def emptyState = RootProjectState(CommonProjectState.emptyState, Some(""), None, "", None)
}

case class SegmentationState(
  state: CommonProjectState,
  belongsToGUID: Option[UUID])
object SegmentationState {
  def emptyState = SegmentationState(CommonProjectState.emptyState, None)
}

sealed trait ProjectViewer {
  val state: CommonProjectState
  implicit val manager: MetaGraphManager

  def vertexSet: VertexSet =
    state.vertexSetGUID.map(manager.vertexSet(_)).getOrElse(null)
  def vertexAttributes: Map[String, Attribute[_]] =
    state.vertexAttributeGUIDs.mapValues(manager.attribute(_))
  def edgeBundle: EdgeBundle =
    state.edgeBundleGUID.map(manager.edgeBundle(_)).getOrElse(null)
  def edgeAttributes: Map[String, Attribute[_]] =
    state.edgeAttributeGUIDs.mapValues(manager.attribute(_))
  def scalars: Map[String, Scalar[_]] =
    state.scalarGUIDs.mapValues(manager.scalar(_))

  def segmentationViewers: Map[String, SegmentationViewer] =
    state.segmentations
      .map { case (name, state) => name -> new SegmentationViewer(this, name) }

  def offspringViewer(path: Seq[String]): ProjectViewer =
    if (path.isEmpty) this
    else segmentationViewers(path.head).offspringViewer(path.tail)

  def editor: ProjectEditor

  def isSegmentation = isInstanceOf[SegmentationViewer]

  def asSegmentation = asInstanceOf[SegmentationViewer]

  // Methods for convertion to FE objects.
  private def feScalar(name: String): Option[FEAttribute] = {
    if (scalars.contains(name)) {
      Some(ProjectViewer.feAttr(scalars(name), name))
    } else {
      None
    }
  }

  def toListElementFE(projectName: String): FEProjectListElement = {
    FEProjectListElement(
      projectName,
      state.notes,
      feScalar("vertex_count"),
      feScalar("edge_count"))
  }

  def toFE(projectName: String): FEProject = {
    Try(unsafeToFE(projectName)) match {
      case Success(fe) => fe
      case Failure(ex) => FEProject(
        name = projectName,
        error = ex.getMessage
      )
    }
  }

  protected def getFEMembers: Option[FEAttribute] = None

  // May raise an exception.
  private def unsafeToFE(projectName: String): FEProject = {
    val vs = Option(vertexSet).map(_.gUID.toString).getOrElse("")
    val eb = Option(edgeBundle).map(_.gUID.toString).getOrElse("")
    def feList(things: Iterable[(String, TypedEntity[_])]) = {
      things.map { case (name, e) => ProjectViewer.feAttr(e, name) }.toList
    }

    FEProject(
      name = projectName,
      vertexSet = vs,
      edgeBundle = eb,
      notes = state.notes,
      scalars = feList(scalars),
      vertexAttributes = feList(vertexAttributes) ++ getFEMembers,
      edgeAttributes = feList(edgeAttributes),
      segmentations = segmentationViewers
        .map { case (name, segm) => segm.toFESegmentation(projectName) }
        .toList,
      // To be set by the ProjectFrame for root projects.
      undoOp = "",
      redoOp = "",
      readACL = "",
      writeACL = "")
  }
}
object ProjectViewer {
  def feAttr[T](e: TypedEntity[T], name: String, isInternal: Boolean = false) = {
    val canBucket = Seq(typeOf[Double], typeOf[String]).exists(e.typeTag.tpe <:< _)
    val canFilter = Seq(typeOf[Double], typeOf[String], typeOf[Long], typeOf[Vector[Any]])
      .exists(e.typeTag.tpe <:< _)
    val isNumeric = Seq(typeOf[Double]).exists(e.typeTag.tpe <:< _)
    FEAttribute(
      e.gUID.toString,
      name,
      e.typeTag.tpe.toString.replace("com.lynxanalytics.biggraph.graph_api.", ""),
      canBucket,
      canFilter,
      isNumeric,
      isInternal)
  }
}

class RootProjectViewer(val rootState: RootProjectState)(implicit val manager: MetaGraphManager)
    extends ProjectViewer {
  val state = rootState.state
  def editor: RootProjectEditor = new RootProjectEditor(rootState)
}

class SegmentationViewer(val parent: ProjectViewer, val segmentationName: String)
    extends ProjectViewer {

  implicit val manager = parent.manager
  val segmentationState: SegmentationState = parent.state.segmentations(segmentationName)
  val state = segmentationState.state

  def editor: SegmentationEditor = parent.editor.segmentation(segmentationName)

  def belongsTo: EdgeBundle =
    segmentationState.belongsToGUID.map(manager.edgeBundle(_)).getOrElse(null)

  def belongsToAttribute: Attribute[Vector[ID]] = {
    val segmentationIds = graph_operations.IdAsAttribute.run(vertexSet)
    val reversedBelongsTo = graph_operations.ReverseEdges.run(belongsTo)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(aop.connection, reversedBelongsTo)(aop.attr, segmentationIds).result.attr
  }

  def membersAttribute: Attribute[Vector[ID]] = {
    val parentIds = graph_operations.IdAsAttribute.run(parent.vertexSet)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(aop.connection, belongsTo)(aop.attr, parentIds).result.attr
  }

  override protected def getFEMembers: Option[FEAttribute] =
    Some(ProjectViewer.feAttr(membersAttribute, "#members", isInternal = true))

  def equivalentUIAttribute = {
    val bta = Option(belongsToAttribute).map(_.gUID.toString).getOrElse("")
    UIValue(id = bta, title = s"segmentation[$segmentationName]")
  }

  def toFESegmentation(parentName: String): FESegmentation = {
    val bt = Option(belongsTo).map(UIValue.fromEntity(_)).getOrElse(null)
    FESegmentation(
      segmentationName,
      parentName + ProjectFrame.separator + segmentationName,
      bt,
      equivalentUIAttribute)
  }
}

object ProjectStateRepository {
  implicit val fFEOperationSpec = Json.format[FEOperationSpec]
  implicit val fSubProjectOperation = Json.format[SubProjectOperation]

  // We need to define this manually because of the cyclic reference.
  implicit val fSegmentationState = new json.Format[SegmentationState] {
    def reads(j: json.JsValue): json.JsResult[SegmentationState] = {
      json.JsSuccess(SegmentationState(
        jsonToCommonProjectState(j \ "state"),
        (j \ "belongsToGUID").as[Option[UUID]]))
    }
    def writes(o: SegmentationState): json.JsValue =
      Json.obj(
        "state" -> commonProjectStateToJSon(o.state),
        "belongsToGUID" -> o.belongsToGUID)
  }
  implicit val fCommonProjectState = Json.format[CommonProjectState]
  implicit val fRootProjectState = Json.format[RootProjectState]

  private def commonProjectStateToJSon(state: CommonProjectState): json.JsValue = Json.toJson(state)
  private def jsonToCommonProjectState(j: json.JsValue): CommonProjectState =
    j.as[CommonProjectState]

  val startingState = RootProjectState.emptyState
}
class ProjectStateRepository(val baseDir: String) {
  import ProjectStateRepository.fRootProjectState

  val baseDirFile = new File(baseDir)
  baseDirFile.mkdirs

  def checkpointFileName(checkpoint: String): File =
    new File(baseDirFile, s"save-$checkpoint")

  def checkpoint(state: RootProjectState): String = {
    state.checkpoint.getOrElse({
      val checkpoint = Timestamp.toString
      val dumpFile = new File(baseDirFile, s"dump-$checkpoint")
      val finalFile = checkpointFileName(checkpoint)
      FileUtils.writeStringToFile(
        dumpFile,
        Json.prettyPrint(Json.toJson(state)),
        "utf8")
      dumpFile.renameTo(finalFile)
      checkpoint
    })
  }

  def readCheckpoint(checkpoint: String): RootProjectState = {
    if (checkpoint == "") {
      ProjectStateRepository.startingState
    } else {
      Json.parse(FileUtils.readFileToString(checkpointFileName(checkpoint), "utf8"))
        .as[RootProjectState].copy(checkpoint = Some(checkpoint))
    }
  }
}

// This class is used to provide a convenient interface for entity map-like project data as
// scalars and attributes. The instantiator needs to know how to do the actual updates on the
// underlying state.
abstract class StateMapHolder[T <: MetaGraphEntity] extends collection.Map[String, T] {
  protected def getMap: Map[String, T]
  protected def updateMap(newMap: Map[String, UUID]): Unit
  def validate(name: String, entity: T): Unit

  def updateEntityMap(newMap: Map[String, T]): Unit =
    updateMap(newMap.mapValues(_.gUID))
  // Skip name validation. Special-name entities can be set through this method.
  def set(name: String, entity: T) = {
    if (entity == null) {
      updateEntityMap(getMap - name)
    } else {
      validate(name, entity)
      updateEntityMap(getMap + (name -> entity))
    }
  }

  def update(name: String, entity: T) = {
    ProjectFrame.validateName(name)
    set(name, entity)
  }

  // Implementing the map interface
  def get(key: String) = getMap.get(key)
  def iterator = getMap.iterator
  def +[T1 >: T](kv: (String, T1)) = getMap + kv
  def -(key: String) = getMap - key
}

sealed trait ProjectEditor {
  implicit val manager: MetaGraphManager
  def state: CommonProjectState
  def state_=(newState: CommonProjectState): Unit

  def viewer: ProjectViewer

  def rootState: RootProjectState

  def vertexSet = viewer.vertexSet
  def vertexSet_=(e: VertexSet): Unit = {
    updateVertexSet(e, killSegmentations = true)
  }
  protected def updateVertexSet(e: VertexSet, killSegmentations: Boolean): Unit = {
    if (e != vertexSet) {
      edgeBundle = null
      vertexAttributes = Map()
      if (killSegmentations) state = state.copy(segmentations = Map())
      if (e != null) {
        state = state.copy(vertexSetGUID = Some(e.gUID))
        scalars("vertex_count") = graph_operations.Count.run(e)
      } else {
        state = state.copy(vertexSetGUID = None)
        scalars("vertex_count") = null
      }
    }
  }
  def setVertexSet(e: VertexSet, idAttr: String): Unit = {
    vertexSet = e
    vertexAttributes(idAttr) = graph_operations.IdAsAttribute.run(e)
  }

  def edgeBundle = viewer.edgeBundle
  def edgeBundle_=(e: EdgeBundle) = {
    if (e != edgeBundle) {
      assert(e == null || vertexSet != null, s"No vertex set")
      assert(e == null || e.srcVertexSet == vertexSet, s"Edge bundle does not match vertex set")
      assert(e == null || e.dstVertexSet == vertexSet, s"Edge bundle does not match vertex set")
      edgeAttributes = Map()
      state = state.copy(edgeBundleGUID = Some(e.gUID))
      scalars("edge_count") = graph_operations.Count.run(e)
    } else {
      state = state.copy(edgeBundleGUID = None)
      scalars("edge_count") = null
    }
  }

  def vertexAttributes =
    new StateMapHolder[Attribute[_]] {
      protected def getMap = viewer.vertexAttributes
      protected def updateMap(newMap: Map[String, UUID]) =
        state = state.copy(vertexAttributeGUIDs = newMap)
      def validate(name: String, attr: Attribute[_]): Unit = {
        assert(
          attr.vertexSet == viewer.vertexSet,
          s"Vertex attribute $name does not match vertex set")
      }
    }
  def vertexAttributes_=(attrs: Map[String, Attribute[_]]) =
    vertexAttributes.updateEntityMap(attrs)
  def vertexAttributeNames[T: TypeTag] = vertexAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def edgeAttributes =
    new StateMapHolder[Attribute[_]] {
      protected def getMap = viewer.edgeAttributes
      protected def updateMap(newMap: Map[String, UUID]) =
        state = state.copy(edgeAttributeGUIDs = newMap)
      def validate(name: String, attr: Attribute[_]): Unit = {
        assert(
          attr.vertexSet == viewer.edgeBundle.idSet,
          s"Edge attribute $name does not match edge bundle")
      }
    }
  def edgeAttributes_=(attrs: Map[String, Attribute[_]]) =
    edgeAttributes.updateEntityMap(attrs)
  def edgeAttributeNames[T: TypeTag] = edgeAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def scalars =
    new StateMapHolder[Scalar[_]] {
      protected def getMap = viewer.scalars
      protected def updateMap(newMap: Map[String, UUID]) =
        state = state.copy(scalarGUIDs = newMap)
      def validate(name: String, scalar: Scalar[_]): Unit = {}
    }
  def scalars_=(newScalars: Map[String, Scalar[_]]) =
    scalars.updateEntityMap(newScalars)
  def scalarNames[T: TypeTag] = scalars.collect {
    case (name, scalar) if typeOf[T] =:= typeOf[Nothing] || scalar.is[T] => name
  }.toSeq

  def segmentations = segmentationNames.map(segmentation(_))
  def segmentation(name: String) = new SegmentationEditor(this, name)
  def segmentationNames = state.segmentations.keys.toSeq
  def deleteSegmentation(name: String) =
    state = state.copy(segmentations = state.segmentations - name)

  def offspringEditor(path: Seq[String]): ProjectEditor =
    if (path.isEmpty) this
    else segmentation(path.head).offspringEditor(path.tail)

  def isSegmentation = isInstanceOf[SegmentationEditor]
  def asSegmentation = asInstanceOf[SegmentationEditor]

  def notes = state.notes
  def notes_=(n: String) = state = state.copy(notes = n)

  def pullBackEdges(injection: EdgeBundle): Unit = {
    val op = graph_operations.PulledOverEdges()
    val newEB = op(op.originalEB, edgeBundle)(op.injection, injection).result.pulledEB
    pullBackEdges(edgeBundle, edgeAttributes.toIndexedSeq, newEB, injection)
  }
  def pullBackEdges(
    origEdgeBundle: EdgeBundle,
    origEAttrs: Seq[(String, Attribute[_])],
    newEdgeBundle: EdgeBundle,
    pullBundle: EdgeBundle): Unit = {

    assert(pullBundle.properties.compliesWith(EdgeBundleProperties.partialFunction),
      s"Not a partial function: $pullBundle")
    assert(pullBundle.srcVertexSet.gUID == newEdgeBundle.idSet.gUID,
      s"Wrong source: $pullBundle")
    assert(pullBundle.dstVertexSet.gUID == origEdgeBundle.idSet.gUID,
      s"Wrong destination: $pullBundle")

    edgeBundle = newEdgeBundle

    for ((name, attr) <- origEAttrs) {
      edgeAttributes(name) =
        graph_operations.PulledOverVertexAttribute.pullAttributeVia(attr, pullBundle)
    }
  }

  def pullBack(pullBundle: EdgeBundle): Unit = {
    assert(pullBundle.properties.compliesWith(EdgeBundleProperties.partialFunction),
      s"Not a partial function: $pullBundle")
    assert(pullBundle.dstVertexSet.gUID == vertexSet.gUID,
      s"Wrong destination: $pullBundle")
    val origVS = vertexSet
    val origVAttrs = vertexAttributes.toIndexedSeq
    val origEB = edgeBundle
    val origEAttrs = edgeAttributes.toIndexedSeq
    val origBelongsTo: Option[EdgeBundle] =
      if (isSegmentation) Some(asSegmentation.belongsTo) else None

    updateVertexSet(pullBundle.srcVertexSet, killSegmentations = false)
    for ((name, attr) <- origVAttrs) {
      vertexAttributes(name) =
        graph_operations.PulledOverVertexAttribute.pullAttributeVia(attr, pullBundle)
    }

    if (origEB != null) {
      val iop = graph_operations.InducedEdgeBundle()
      val induction = iop(
        iop.srcMapping, graph_operations.ReverseEdges.run(pullBundle))(
          iop.dstMapping, graph_operations.ReverseEdges.run(pullBundle))(
            iop.edges, origEB).result
      pullBackEdges(origEB, origEAttrs, induction.induced, induction.embedding)
    }

    for (seg <- segmentations) {
      val op = graph_operations.InducedEdgeBundle(induceDst = false)
      seg.belongsTo = op(
        op.srcMapping, graph_operations.ReverseEdges.run(pullBundle))(
          op.edges, seg.belongsTo).result.induced
    }

    if (isSegmentation) {
      val seg = asSegmentation
      val op = graph_operations.InducedEdgeBundle(induceSrc = false)
      seg.belongsTo = op(
        op.dstMapping, graph_operations.ReverseEdges.run(pullBundle))(
          op.edges, origBelongsTo.get).result.induced
    }
  }

  def setLastOperationDesc(n: String): Unit
  def setLastOperationRequest(n: SubProjectOperation): Unit
}

class RootProjectEditor(
    initialState: RootProjectState)(
        implicit val manager: MetaGraphManager) extends ProjectEditor {
  var rootState: RootProjectState = initialState.copy(checkpoint = None)

  def state = rootState.state
  def state_=(newState: CommonProjectState): Unit = {
    rootState = rootState.copy(state = newState)
  }

  def viewer = new RootProjectViewer(rootState)

  def lastOperationDesc = rootState.lastOperationDesc
  def lastOperationDesc_=(n: String) =
    rootState = rootState.copy(lastOperationDesc = n)
  def setLastOperationDesc(n: String) = lastOperationDesc = n

  def lastOperationRequest = rootState.lastOperationRequest
  def lastOperationRequest_=(n: SubProjectOperation) =
    rootState = rootState.copy(lastOperationRequest = Option(n))
  def setLastOperationRequest(n: SubProjectOperation) = lastOperationRequest = n
}

class SegmentationEditor(
    val parent: ProjectEditor,
    val segmentationName: String) extends ProjectEditor {

  implicit val manager = parent.manager

  {
    val oldSegs = parent.state.segmentations
    if (!oldSegs.contains(segmentationName)) {
      parent.state = parent.state.copy(
        segmentations = oldSegs + (segmentationName -> SegmentationState.emptyState))
    }
  }

  def segmentationState = parent.state.segmentations(segmentationName)
  def segmentationState_=(newState: SegmentationState): Unit = {
    val pState = parent.state
    parent.state = pState.copy(
      segmentations = pState.segmentations + (segmentationName -> newState))
  }
  def state = segmentationState.state
  def state_=(newState: CommonProjectState): Unit = {
    segmentationState = segmentationState.copy(state = newState)
  }

  def viewer = parent.viewer.segmentationViewers(segmentationName)

  def rootState = parent.rootState

  def belongsTo = viewer.belongsTo
  def belongsTo_=(e: EdgeBundle): Unit = {
    segmentationState = segmentationState.copy(belongsToGUID = Some(e.gUID))
  }

  override protected def updateVertexSet(e: VertexSet, killSegmentations: Boolean): Unit = {
    if (e != vertexSet) {
      super.updateVertexSet(e, killSegmentations)
      val op = graph_operations.EmptyEdgeBundle()
      belongsTo = op(op.src, parent.vertexSet)(op.dst, e).result.eb
    }
  }

  def setLastOperationDesc(n: String) =
    parent.setLastOperationDesc(s"$n on $segmentationName")

  def setLastOperationRequest(n: SubProjectOperation) =
    parent.setLastOperationRequest(n.copy(path = segmentationName +: n.path))
}

class ProjectFrame(val projectPath: SymbolPath)(
    implicit manager: MetaGraphManager) {
  val projectName = projectPath.toString
  override def toString = projectName
  override def equals(p: Any) =
    p.isInstanceOf[ProjectFrame] && projectName == p.asInstanceOf[ProjectFrame].projectName
  override def hashCode = projectName.hashCode

  assert(projectName.nonEmpty, "Invalid project name: <empty string>")
  assert(!projectName.contains(ProjectFrame.separator), s"Invalid project name: $projectName")
  val rootDir: SymbolPath = SymbolPath("projects") / projectPath

  // Current checkpoint of the project
  def checkpoint: String = get(rootDir / "checkpoint")
  private def checkpoint_=(x: String): Unit = set(rootDir / "checkpoint", x)

  // The farthest checkpoint available in the current redo sequence
  private def farthestCheckpoint: String = get(rootDir / "farthestCheckpoint")
  private def farthestCheckpoint_=(x: String): Unit = set(rootDir / "farthestCheckpoint", x)

  // The next checkpoint in the current redo sequence if a redo is available
  private def nextCheckpoint: Option[String] = get(rootDir / "nextCheckpoint") match {
    case "" => None
    case x => Some(x)
  }
  private def nextCheckpoint_=(x: Option[String]): Unit =
    set(rootDir / "nextCheckpoint", x.getOrElse(""))

  /*private def checkpoints: Seq[String] = get(rootDir / "checkpoints") match {
    case "" => Seq()
    case x => x.split(ProjectFrame.quotedSeparator, -1)
  }

  private def checkpoints_=(cs: Seq[String]): Unit =
    set(rootDir / "checkpoints", cs.mkString(ProjectFrame.separator))
  private def checkpointIndex = get(rootDir / "checkpointIndex") match {
    case "" => 0
    case x => x.toInt
  }
  private def checkpointIndex_=(x: Int): Unit =
    set(rootDir / "checkpointIndex", x.toString)
  def checkpointCount = if (checkpoints.nonEmpty) checkpointIndex + 1 else 0*/

  def exists = manager.tagExists(rootDir)

  def undo(): Unit = manager.synchronized {
    nextCheckpoint = Some(checkpoint)
    val state = currentState
    checkpoint = currentState.previousCheckpoint.get
  }
  def redo(): Unit = manager.synchronized {
    val next = nextCheckpoint.get
    checkpoint = next
    lazy val reverseCheckpoints: Stream[String] = farthestCheckpoint #:: reverseCheckpoints.map {
      next => checkpointState(next).previousCheckpoint.get
    }
    nextCheckpoint = reverseCheckpoints.takeWhile(_ != next).lastOption
  }

  def dodo(newState: RootProjectState): Unit = manager.synchronized {
    setCheckpoint(manager.stateRepo.checkpoint(
      newState.copy(previousCheckpoint = Some(checkpoint))))
  }

  def setCheckpoint(cp: String): Unit = manager.synchronized {
    checkpoint = cp
    farthestCheckpoint = cp
    nextCheckpoint = None
  }

  def initialize: Unit = manager.synchronized {
    checkpoint = ""
    nextCheckpoint = None
    farthestCheckpoint = ""
  }

  def checkpointState(checkpoint: String): RootProjectState =
    manager.stateRepo.readCheckpoint(checkpoint)

  def currentState: RootProjectState = checkpointState(checkpoint)

  def nextState: Option[RootProjectState] = nextCheckpoint.map(checkpointState(_))

  def viewer = new RootProjectViewer(currentState)

  def toListElementFE = viewer.toListElementFE(projectName)

  private def existing(tag: SymbolPath): Option[SymbolPath] =
    if (manager.tagExists(tag)) Some(tag) else None
  private def set(tag: SymbolPath, content: String): Unit = manager.setTag(tag, content)
  private def get(tag: SymbolPath): String = manager.synchronized {
    existing(tag).map(manager.getTag(_)).get
  }

  def readACL: String = get(rootDir / "readACL")
  def readACL_=(x: String): Unit = set(rootDir / "readACL", x)

  def writeACL: String = get(rootDir / "writeACL")
  def writeACL_=(x: String): Unit = set(rootDir / "writeACL", x)

  def assertReadAllowedFrom(user: User): Unit = {
    assert(readAllowedFrom(user), s"User $user does not have read access to project $projectName.")
  }
  def assertWriteAllowedFrom(user: User): Unit = {
    assert(writeAllowedFrom(user), s"User $user does not have write access to project $projectName.")
  }
  def readAllowedFrom(user: User): Boolean = {
    // Write access also implies read access.
    user.isAdmin || writeAllowedFrom(user) || aclContains(readACL, user)
  }
  def writeAllowedFrom(user: User): Boolean = {
    user.isAdmin || aclContains(writeACL, user)
  }

  def aclContains(acl: String, user: User): Boolean = {
    // The ACL is a comma-separated list of email addresses with '*' used as a wildcard.
    // We translate this to a regex for checking.
    val regex = acl.replace(" ", "").replace(".", "\\.").replace(",", "|").replace("*", ".*")
    user.email.matches(regex)
  }

  def remove(): Unit = manager.synchronized {
    existing(rootDir).foreach(manager.rmTag(_))
    log.info(s"A project has been discarded: $rootDir")
  }

  private def cp(from: SymbolPath, to: SymbolPath) = manager.synchronized {
    existing(to).foreach(manager.rmTag(_))
    manager.cpTag(from, to)
  }
  def copy(to: ProjectFrame): Unit = cp(rootDir, to.rootDir)
}

object ProjectFrame {
  val x: Stream[Int] = 0 #:: x.map(_ + 1)

  val separator = "|"
  val quotedSeparator = java.util.regex.Pattern.quote(ProjectFrame.separator)

  def fromName(rootProjectName: String)(implicit metaManager: MetaGraphManager): ProjectFrame = {
    validateName(rootProjectName)
    new ProjectFrame(SymbolPath(rootProjectName))
  }

  def validateName(name: String, what: String = "Name"): Unit = {
    assert(name.nonEmpty, s"$what cannot be empty.")
    assert(!name.startsWith("!"), s"$what cannot start with '!'.")
    assert(!name.contains(separator), s"$what cannot contain '$separator'.")
    assert(!name.contains("/"), s"$what cannot contain '/'.")
  }
}

case class SubProject(val frame: ProjectFrame, val path: Seq[String]) {
  def viewer = frame.viewer.offspringViewer(path)
  def fullName = (frame.projectName +: path).mkString(ProjectFrame.separator)
  def toFE: FEProject = {
    val raw = viewer.toFE(fullName)
    if (path.isEmpty) {
      raw.copy(
        undoOp = frame.currentState.lastOperationDesc,
        redoOp = frame.nextState.map(_.lastOperationDesc).getOrElse(""),
        readACL = frame.readACL,
        writeACL = frame.writeACL)
    } else raw
  }
}
object SubProject {
  def parsePath(projectName: String)(implicit metaManager: MetaGraphManager): SubProject = {
    val nameElements = projectName.split(ProjectFrame.quotedSeparator, -1)
    new SubProject(ProjectFrame.fromName(nameElements.head), nameElements.tail)
  }
}
