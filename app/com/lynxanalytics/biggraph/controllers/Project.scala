// Projects are the top-level entities on the UI.
//
// A project has a vertex set, an edge bundle, and any number of attributes,
// scalars and segmentations. A segmentation itself can also be looked at as a project.
// It has its own vertex set, edge bundle, attributes, scalars and is connected to the
// base project via belongsTo edge bundle.
//
// The complete state of a root project - a project which is not a segmentation of a base project -
// is completely represented by the immutable RootProjectState class.
//
// We can optionally store a RootProjectState as a checkpoint. Checkpoints are globally unique
// (within a Kite instance) identifiers that can be used to recall a project state. Checkpointed
// project states also encapsulate the full operation history of the state as they have references
// to their parent state checkpoint and the operation that was used to get from the parent to the
// child. Thus checkpoints form a directed tree.
//
// Project frames represent named project states. Basically each project on the UI is
// represented via a project frame. A project frame is uniquely identified by a project name
// and contains informations like the checkpoint representing the current state of the project and
// high level state independent meta information like access control. Project frames are persisted
// using tags. ProjectFrame instances are short-lived, they are just a rich interface for
// querying and manipulating the underlying tag tree.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.model
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Timestamp
import com.lynxanalytics.biggraph.graph_util.SoftHashMap
import com.lynxanalytics.biggraph.serving.{ AccessControl, User, Utils }
import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import play.api.libs.json
import play.api.libs.json.JsObject
import play.api.libs.json.Json

import scala.reflect.runtime.universe._

sealed abstract class ElementKind(kindName: String) {
  def /(name: String): String = {
    s"$kindName/$name"
  }
}
object VertexAttributeKind extends ElementKind("vertex attribute")
object EdgeAttributeKind extends ElementKind("edge attribute")
object ScalarKind extends ElementKind("scalar")
object SegmentationKind extends ElementKind("segmentation")

// Keys for elementMetadata.
object MetadataNames {
  val Icon = "icon"
}

// Captures the part of the state that is common for segmentations and root projects.
case class CommonProjectState(
  vertexSetGUID: Option[UUID],
  vertexAttributeGUIDs: Map[String, UUID],
  edgeBundleGUID: Option[UUID],
  edgeAttributeGUIDs: Map[String, UUID],
  scalarGUIDs: Map[String, UUID],
  segmentations: Map[String, SegmentationState],
  notes: String,
  elementNotes: Option[Map[String, String]], // Option for compatibility.
  elementMetadata: Option[Map[String, Map[String, String]]]) // Option for compatibility.
object CommonProjectState {
  val emptyState = CommonProjectState(
    None, Map(), None, Map(), Map(), Map(), "", Some(Map()), Some(Map()))
}

// Complete state of a root project.
case class RootProjectState(
    state: CommonProjectState,
    checkpoint: Option[String],
    previousCheckpoint: Option[String],
    lastOperationDesc: String,
    // This will be set exactly when previousCheckpoint is set.
    lastOperationRequest: Option[SubProjectOperation],
    viewRecipe: Option[JsObject],
    workspace: Option[Workspace]) {
}
object RootProjectState {
  val emptyState = RootProjectState(
    CommonProjectState.emptyState, Some(""), None, "", None, None, None)
}

// Complete state of segmentation.
case class SegmentationState(
  state: CommonProjectState,
  belongsToGUID: Option[UUID])
object SegmentationState {
  val emptyState = SegmentationState(CommonProjectState.emptyState, None)
}

// Rich interface for looking at project states.
sealed trait ProjectViewer {
  val rootState: RootProjectState
  val state: CommonProjectState
  implicit val manager: MetaGraphManager

  def rootViewer: RootProjectViewer

  lazy val vertexSet: VertexSet =
    state.vertexSetGUID.map(manager.vertexSet(_)).getOrElse(null)
  lazy val vertexAttributes: Map[String, Attribute[_]] =
    state.vertexAttributeGUIDs.mapValues(manager.attribute(_))
  def vertexAttributeNames[T: TypeTag] = vertexAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq.sorted
  lazy val edgeBundle: EdgeBundle =
    state.edgeBundleGUID.map(manager.edgeBundle(_)).getOrElse(null)
  lazy val edgeAttributes: Map[String, Attribute[_]] =
    state.edgeAttributeGUIDs.mapValues(manager.attribute(_))
  def edgeAttributeNames[T: TypeTag] = edgeAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq.sorted
  lazy val scalars: Map[String, Scalar[_]] =
    state.scalarGUIDs.mapValues(manager.scalar(_))
  def scalarNames[T: TypeTag] = scalars.collect {
    case (name, scalar) if typeOf[T] =:= typeOf[Nothing] || scalar.is[T] => name
  }.toSeq.sorted
  def models: Map[String, model.ModelMeta] = {
    import model.Implicits._
    scalars.collect {
      case (k, v) if v.is[model.Model] => k -> v.runtimeSafeCast[model.Model].modelMeta
    }
  }

  def getProgress()(implicit entityProgressManager: EntityProgressManager): List[Double] = {
    def commonProjectStateProgress(state: CommonProjectState): List[Double] = {
      val allEntities = state.vertexSetGUID.map(manager.vertexSet).toList ++
        state.edgeBundleGUID.map(manager.edgeBundle).toList ++
        state.scalarGUIDs.values.map(manager.scalar) ++
        state.vertexAttributeGUIDs.values.map(manager.attribute) ++
        state.edgeAttributeGUIDs.values.map(manager.attribute)

      val segmentationProgress = state.segmentations.values.flatMap(segmentationStateProgress)
      allEntities.map(entityProgressManager.computeProgress) ++ segmentationProgress
    }

    def segmentationStateProgress(state: SegmentationState): List[Double] = {
      val segmentationProgress = commonProjectStateProgress(state.state)
      val belongsToProgress = state.belongsToGUID.map(belongsToGUID => {
        val belongsTo = manager.edgeBundle(belongsToGUID)
        entityProgressManager.computeProgress(belongsTo)
      }).toList
      belongsToProgress ++ segmentationProgress
    }

    commonProjectStateProgress(state)
  }

  lazy val segmentationMap: Map[String, SegmentationViewer] =
    state.segmentations
      .map { case (name, state) => name -> new SegmentationViewer(this, name) }
  def segmentation(name: String) = segmentationMap(name)

  def getVertexAttributeNote(name: String) = getElementNote(VertexAttributeKind, name)
  def getEdgeAttributeNote(name: String) = getElementNote(EdgeAttributeKind, name)
  def getScalarNote(name: String) = getElementNote(ScalarKind, name)
  def getElementNote(kind: ElementKind, name: String) =
    state.elementNotes.getOrElse(Map()).getOrElse(kind / name, "")
  def getElementMetadata(kind: ElementKind, name: String): Map[String, String] =
    state.elementMetadata.getOrElse(Map()).getOrElse(kind / name, Map())

  def offspringPath: Seq[String]
  def offspringViewer(path: Seq[String]): ProjectViewer =
    if (path.isEmpty) this
    else segmentation(path.head).offspringViewer(path.tail)

  def editor: ProjectEditor
  def rootCheckpoint: String = rootState.checkpoint.get

  val isSegmentation: Boolean
  def asSegmentation: SegmentationViewer

  // Methods for conversion to FE objects.
  private def feScalar(name: String)(implicit epm: EntityProgressManager): Option[FEScalar] = {
    if (scalars.contains(name)) {
      Some(ProjectViewer.feScalar(
        scalars(name), name, getScalarNote(name), getElementMetadata(ScalarKind, name)))
    } else {
      None
    }
  }

  def toListElementFE(name: String, objectType: String, details: Option[json.JsObject])(
    implicit epm: EntityProgressManager): FEEntryListElement = {
    FEEntryListElement(
      name = name,
      objectType = objectType,
      icon = objectType,
      notes = state.notes,
      vertexCount = feScalar("!vertex_count"),
      edgeCount = feScalar("!edge_count"),
      details = details)
  }

  // Returns the FE attribute representing the seq of members for
  // each segment in a segmentation. None in root projects.
  protected def getFEMembers()(implicit epm: EntityProgressManager): Option[FEAttribute]

  def sortedSegmentations: List[SegmentationViewer] =
    segmentationMap.toList.sortBy(_._1).map(_._2)

  def toFE(projectName: String)(implicit epm: EntityProgressManager): FEProject = {
    val vs = Option(vertexSet).map(_.gUID.toString).getOrElse("")
    val eb = Option(edgeBundle).map(_.gUID.toString).getOrElse("")

    def feAttributeList(
      attributes: Iterable[(String, Attribute[_])],
      kind: ElementKind): List[FEAttribute] = {
      attributes.toSeq.sortBy(_._1).map {
        case (name, attr) => ProjectViewer.feAttribute(
          attr, name, getElementNote(kind, name), getElementMetadata(kind, name))
      }.toList
    }

    def feScalarList(scalars: Iterable[(String, Scalar[_])]): List[FEScalar] = {
      scalars.toSeq.sortBy(_._1).map {
        case (name, scalar) => ProjectViewer.feScalar(
          scalar, name, getScalarNote(name), getElementMetadata(ScalarKind, name))
      }.toList
    }

    FEProject(
      name = projectName,
      vertexSet = vs,
      edgeBundle = eb,
      notes = state.notes,
      scalars = feScalarList(scalars),
      vertexAttributes = feAttributeList(vertexAttributes, VertexAttributeKind) ++ getFEMembers,
      edgeAttributes = feAttributeList(edgeAttributes, EdgeAttributeKind),
      segmentations = sortedSegmentations.map(_.toFESegmentation(projectName)),
      // To be set by the ProjectFrame for root projects.
      undoOp = "",
      redoOp = "",
      readACL = "",
      writeACL = "")
  }

  def allOffspringFESegmentations(
    rootName: String, rootRelativePath: String = "")(
      implicit epm: EntityProgressManager): List[FESegmentation] = {
    sortedSegmentations.flatMap { segmentation =>
      segmentation.toFESegmentation(rootName, rootRelativePath) +:
        segmentation.allOffspringFESegmentations(
          rootName, rootRelativePath + segmentation.segmentationName + ProjectFrame.separator)
    }
  }

  protected def maybeProtoTable(
    maybe: Any, tableName: String): Option[(String, ProtoTable)] = {
    maybe match {
      case null => None
      case _ => Some(tableName -> getSingleLocalProtoTable(tableName))
    }
  }

  def getLocalProtoTables: Iterable[(String, ProtoTable)] = {
    import ProjectViewer._
    maybeProtoTable(vertexSet, VertexTableName) ++
      maybeProtoTable(edgeBundle, EdgeAttributeTableName) ++
      maybeProtoTable(edgeBundle, EdgeTableName)
  }

  def getProtoTables: Iterable[(String, ProtoTable)] = {
    val childProtoTables = sortedSegmentations.flatMap { segmentation =>
      segmentation.getLocalProtoTables.map {
        case (name, table) => segmentation.segmentationName + "." + name -> table
      }
    }
    getLocalProtoTables ++ childProtoTables
  }

  def getSingleProtoTable(tablePath: String): ProtoTable = {
    val splittedPath = tablePath.split('.')
    val (segPath, tableName) = (splittedPath.dropRight(1), splittedPath.last)
    val segViewer = offspringViewer(segPath)
    segViewer.getSingleLocalProtoTable(tableName)
  }

  protected def getSingleLocalProtoTable(tableName: String): ProtoTable = {
    import ProjectViewer._
    val protoTable = tableName match {
      case VertexTableName => ProtoTable(vertexAttributes.toSeq.sortBy(_._1))
      case EdgeTableName => {
        import graph_operations.VertexToEdgeAttribute._
        val edgeAttrs = edgeAttributes.map {
          case (name, attr) => s"edge_$name" -> attr
        }
        val srcAttrs = vertexAttributes.map {
          case (name, attr) => s"src_$name" -> srcAttribute(attr, edgeBundle)
        }
        val dstAttrs = vertexAttributes.map {
          case (name, attr) => s"dst_$name" -> dstAttribute(attr, edgeBundle)
        }
        ProtoTable((edgeAttrs ++ srcAttrs ++ dstAttrs).toSeq.sortBy(_._1))
      }
      case EdgeAttributeTableName => ProtoTable(edgeAttributes.toSeq.sortBy(_._1))
      case BelongsToTableName =>
        throw new AssertionError("Only segmentations have a BelongsTo table")
      case _ => {
        val correctTableNames = List(VertexTableName, EdgeTableName, EdgeAttributeTableName,
          BelongsToTableName).mkString(", ")
        throw new AssertionError("Not recognized table name. Correct table names: " +
          s"$correctTableNames")
      }
    }
    protoTable
  }
}

object ProjectViewer {
  val VertexTableName = "vertices"
  val EdgeTableName = "edges"
  val EdgeAttributeTableName = "edge_attributes"
  val BelongsToTableName = "belongs_to"

  def feTypeName[T](typeTag: TypeTag[T]): String = {
    typeTag.tpe.toString
      .replace("com.lynxanalytics.biggraph.graph_api.", "")
      .replace("com.lynxanalytics.biggraph.model.", "")
  }

  def feTypeName[T](e: TypedEntity[T]): String =
    feTypeName(e.typeTag)

  private def feIsNumeric[T](e: TypedEntity[T]): Boolean =
    Seq(typeOf[Double]).exists(e.typeTag.tpe <:< _)

  def feAttribute[T](
    e: Attribute[T],
    name: String,
    note: String,
    metadata: Map[String, String],
    isInternal: Boolean = false)(implicit epm: EntityProgressManager): FEAttribute = {
    val canBucket = Seq(typeOf[Double], typeOf[String]).exists(e.typeTag.tpe <:< _)
    val canFilter = Seq(typeOf[Double], typeOf[String], typeOf[Long], typeOf[Vector[Any]], typeOf[(Double, Double)])
      .exists(e.typeTag.tpe <:< _)
    FEAttribute(
      e.gUID.toString,
      name,
      feTypeName(e),
      note,
      metadata,
      canBucket,
      canFilter,
      feIsNumeric(e),
      isInternal,
      epm.computeProgress(e))
  }

  def feScalar[T](
    e: Scalar[T],
    name: String,
    note: String,
    metadata: Map[String, String],
    isInternal: Boolean = false)(implicit epm: EntityProgressManager): FEScalar = {
    implicit val tt = e.typeTag
    val state = epm.getComputedScalarValue(e)
    FEScalar(
      e.gUID.toString,
      name,
      feTypeName(e),
      note,
      metadata,
      feIsNumeric(e),
      isInternal,
      state.computeProgress,
      state.error.map(Utils.formatThrowable(_)),
      state.value.map(graph_operations.DynamicValue.convert(_))
    )
  }
}

// Specialized ProjectViewer for RootProjectStates.
class RootProjectViewer(val rootState: RootProjectState)(implicit val manager: MetaGraphManager)
    extends ProjectViewer {
  val state = rootState.state
  def rootViewer = this
  def editor: RootProjectEditor = new RootProjectEditor(rootState)

  val isSegmentation = false
  def asSegmentation: SegmentationViewer = ???
  def offspringPath: Seq[String] = Nil

  protected def getFEMembers()(implicit epm: EntityProgressManager): Option[FEAttribute] = None

  def viewRecipe = rootState.viewRecipe.map(TypedJson.read[ViewRecipe])
}

// Specialized ProjectViewer for SegmentationStates.
class SegmentationViewer(val parent: ProjectViewer, val segmentationName: String)
    extends ProjectViewer {

  implicit val manager = parent.manager
  val rootState = parent.rootState
  val segmentationState: SegmentationState = parent.state.segmentations(segmentationName)
  val state = segmentationState.state

  def rootViewer = parent.rootViewer

  override val isSegmentation = true
  override val asSegmentation = this
  def offspringPath: Seq[String] = parent.offspringPath :+ segmentationName

  def editor: SegmentationEditor = parent.editor.segmentation(segmentationName)

  lazy val belongsTo: EdgeBundle =
    segmentationState.belongsToGUID.map(manager.edgeBundle(_)).getOrElse(null)

  lazy val belongsToAttribute: Attribute[Vector[ID]] = {
    val segmentationIds = graph_operations.IdAsAttribute.run(vertexSet)
    val reversedBelongsTo = graph_operations.ReverseEdges.run(belongsTo)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(aop.connection, reversedBelongsTo)(aop.attr, segmentationIds).result.attr
  }

  lazy val membersAttribute: Attribute[Vector[ID]] = {
    val parentIds = graph_operations.IdAsAttribute.run(parent.vertexSet)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(aop.connection, belongsTo)(aop.attr, parentIds).result.attr
  }

  override protected def getFEMembers()(implicit epm: EntityProgressManager): Option[FEAttribute] =
    Some(ProjectViewer.feAttribute(
      membersAttribute, "#members", note = "", metadata = Map(), isInternal = true))

  val equivalentUIAttributeTitle = s"segmentation[$segmentationName]"

  def equivalentUIAttribute()(implicit epm: EntityProgressManager): FEAttribute =
    ProjectViewer.feAttribute(
      belongsToAttribute, equivalentUIAttributeTitle, note = "", metadata = Map())

  def toFESegmentation(
    rootName: String,
    rootRelativePath: String = "")(implicit epm: EntityProgressManager): FESegmentation = {
    val bt =
      if (belongsTo == null) null
      else belongsTo.gUID.toString
    FESegmentation(
      rootRelativePath + segmentationName,
      rootName + ProjectFrame.separator + rootRelativePath + segmentationName,
      bt,
      equivalentUIAttribute)
  }

  override def getLocalProtoTables: Iterable[(String, ProtoTable)] = {
    import ProjectViewer._
    maybeProtoTable(belongsTo, BelongsToTableName) ++ super.getLocalProtoTables
  }

  override protected def getSingleLocalProtoTable(tableName: String): ProtoTable = {
    import ProjectViewer._
    val protoTable = tableName match {
      case BelongsToTableName => {
        import graph_operations.VertexToEdgeAttribute._
        val baseAttrs = parent.vertexAttributes.map {
          case (name, attr) => s"base_$name" -> srcAttribute(attr, belongsTo)
        }
        val segAttrs = vertexAttributes.map {
          case (name, attr) => s"segment_$name" -> dstAttribute(attr, belongsTo)
        }
        ProtoTable((baseAttrs ++ segAttrs).toSeq.sortBy(_._1))
      }
      case _ => super.getSingleLocalProtoTable(tableName)
    }
    protoTable
  }
}

// The CheckpointRepository's job is to persist project states to checkpoints.
// There is one special checkpoint, "", which is the root of the checkpoint tree.
// It corresponds to an empty project state with no parent state.
object CheckpointRepository {
  private val checkpointFilePrefix = "save-"

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
  import WorkspaceJsonFormatters._
  implicit val fCommonProjectState = Json.format[CommonProjectState]
  implicit val fRootProjectState = Json.format[RootProjectState]

  private def commonProjectStateToJSon(state: CommonProjectState): json.JsValue = Json.toJson(state)
  private def jsonToCommonProjectState(j: json.JsValue): CommonProjectState =
    j.as[CommonProjectState]

  val startingState = RootProjectState.emptyState
}
class CheckpointRepository(val baseDir: String) {
  import CheckpointRepository.fRootProjectState
  import CheckpointRepository.checkpointFilePrefix

  val baseDirFile = new File(baseDir)
  baseDirFile.mkdirs

  def checkpointFileName(checkpoint: String): File =
    new File(baseDirFile, s"${checkpointFilePrefix}${checkpoint}")

  def allCheckpoints: Map[String, RootProjectState] =
    baseDirFile
      .list
      .filter(_.startsWith(checkpointFilePrefix))
      .map(fileName => fileName.drop(checkpointFilePrefix.length))
      .map(cp => cp -> readCheckpoint(cp))
      .toMap

  def saveCheckpointedState(checkpoint: String, state: RootProjectState): Unit = {
    val dumpFile = new File(baseDirFile, s"dump-$checkpoint")
    val finalFile = checkpointFileName(checkpoint)
    FileUtils.writeStringToFile(
      dumpFile,
      Json.prettyPrint(Json.toJson(state)),
      "utf8")
    dumpFile.renameTo(finalFile)
  }

  def checkpointState(state: RootProjectState, prevCheckpoint: String): RootProjectState = {
    if (state.checkpoint.nonEmpty) {
      // Already checkpointed.
      state
    } else {
      val withPrev = state.copy(previousCheckpoint = Some(prevCheckpoint))
      val checkpoint = Timestamp.toString
      saveCheckpointedState(checkpoint, withPrev)
      withPrev.copy(checkpoint = Some(checkpoint))
    }
  }

  val cache = new SoftHashMap[String, RootProjectState]()
  def readCheckpoint(checkpoint: String): RootProjectState = {
    if (checkpoint == "") {
      CheckpointRepository.startingState
    } else synchronized {
      cache.getOrElseUpdate(checkpoint,
        Json.parse(FileUtils.readFileToString(checkpointFileName(checkpoint), "utf8"))
          .as[RootProjectState].copy(checkpoint = Some(checkpoint)))
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

// A mutable wrapper around a CommonProjectState. A ProjectEditor can be editing the state
// of a particular segmentation or a root project. The actual state is always stored and modified
// on the root level. E.g. if you take a RootProjectEditor, get a particular SegmentationEditor
// from it and then do some modifications using the SegmentationEditor then the state of the
// original RootProjectEditor will change as well.
sealed trait ProjectEditor {
  implicit val manager: MetaGraphManager
  def state: CommonProjectState
  def state_=(newState: CommonProjectState): Unit

  def viewer: ProjectViewer

  def rootState: RootProjectState
  def rootCheckpoint: String = rootState.checkpoint.get

  def vertexSet = viewer.vertexSet
  def vertexSet_=(e: VertexSet): Unit = {
    updateVertexSet(e)
  }
  protected def updateVertexSet(e: VertexSet): Unit = {
    if (e != vertexSet) {
      edgeBundle = null
      vertexAttributes = Map()
      state = state.copy(segmentations = Map())
      if (e != null) {
        state = state.copy(vertexSetGUID = Some(e.gUID))
        scalars.set("!vertex_count", graph_operations.Count.run(e))
      } else {
        state = state.copy(vertexSetGUID = None)
        scalars.set("!vertex_count", null)
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
    }
    if (e != null) {
      state = state.copy(edgeBundleGUID = Some(e.gUID))
      scalars.set("!edge_count", graph_operations.Count.run(e))
    } else {
      state = state.copy(edgeBundleGUID = None)
      scalars.set("!edge_count", null)
    }
  }

  def newVertexAttribute(name: String, attr: Attribute[_], note: String = null) = {
    vertexAttributes(name) = attr
    setElementNote(VertexAttributeKind, name, note)
  }
  def deleteVertexAttribute(name: String) = {
    vertexAttributes(name) = null
    setElementNote(VertexAttributeKind, name, null)
  }

  def newEdgeAttribute(name: String, attr: Attribute[_], note: String = null) = {
    edgeAttributes(name) = attr
    setElementNote(EdgeAttributeKind, name, note)
  }
  def deleteEdgeAttribute(name: String) = {
    edgeAttributes(name) = null
    setElementNote(EdgeAttributeKind, name, null)
  }

  def newScalar(name: String, scalar: Scalar[_], note: String = null) = {
    scalars(name) = scalar
    setElementNote(ScalarKind, name, note)
  }
  def deleteScalar(name: String) = {
    scalars(name) = null
    setElementNote(ScalarKind, name, null)
  }

  def setElementNote(kind: ElementKind, name: String, note: String) = {
    val notes = state.elementNotes.getOrElse(Map())
    if (note == null) {
      state = state.copy(elementNotes = Some(notes - kind / name))
    } else {
      state = state.copy(elementNotes = Some(notes + (kind / name -> note)))
    }
  }

  def setElementMetadata(kind: ElementKind, name: String, key: String, value: String) = {
    val allMeta = state.elementMetadata.getOrElse(Map())
    val kindName = kind / name
    val meta = allMeta.getOrElse(kindName, Map())
    val newMeta =
      if (value == null) meta - key else meta + (key -> value)
    val newAllMeta =
      if (newMeta.isEmpty) allMeta - kindName else allMeta + (kindName -> newMeta)
    state = state.copy(elementMetadata = Some(newAllMeta))
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
  def vertexAttributeNames[T: TypeTag] = viewer.vertexAttributeNames[T]

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
  def edgeAttributeNames[T: TypeTag] = viewer.edgeAttributeNames[T]

  def scalars =
    new StateMapHolder[Scalar[_]] {
      protected def getMap = viewer.scalars
      protected def updateMap(newMap: Map[String, UUID]) =
        state = state.copy(scalarGUIDs = newMap)
      def validate(name: String, scalar: Scalar[_]): Unit = {}
    }
  def scalars_=(newScalars: Map[String, Scalar[_]]) =
    scalars.updateEntityMap(newScalars)
  def scalarNames[T: TypeTag] = viewer.scalarNames[T]

  def segmentations = segmentationNames.map(segmentation(_))
  def segmentation(name: String) = new SegmentationEditor(this, name)
  def existingSegmentation(name: String) = {
    assert(segmentationNames.contains(name), s"Segmentation $name does not exist.")
    segmentation(name)
  }
  def segmentationNames = state.segmentations.keys.toSeq.sorted
  def deleteSegmentation(name: String) = {
    state = state.copy(segmentations = state.segmentations - name)
    setElementNote(SegmentationKind, name, null)
  }

  def offspringEditor(path: Seq[String]): ProjectEditor =
    if (path.isEmpty) this
    else segmentation(path.head).offspringEditor(path.tail)

  def rootEditor: RootProjectEditor

  val isSegmentation: Boolean
  def asSegmentation: SegmentationEditor

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
    val origVAttrs = vertexAttributes.toIndexedSeq
    val origEB = edgeBundle
    val origEAttrs = edgeAttributes.toIndexedSeq
    val origBelongsTo: Option[EdgeBundle] =
      if (isSegmentation) Some(asSegmentation.belongsTo) else None
    val origSegmentations = state.segmentations
    updateVertexSet(pullBundle.srcVertexSet)

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

    for ((segName, segState) <- origSegmentations) {
      val seg = segmentation(segName)
      seg.segmentationState = segState
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

// Specialized project editor for a RootProjectState.
class RootProjectEditor(
    initialState: RootProjectState)(
        implicit val manager: MetaGraphManager) extends ProjectEditor {
  var rootState: RootProjectState = initialState.copy(checkpoint = None)

  def state = rootState.state
  def state_=(newState: CommonProjectState): Unit = {
    rootState = rootState.copy(state = newState)
  }

  def viewer = new RootProjectViewer(rootState)

  def rootEditor: RootProjectEditor = this

  def checkpoint = rootState.checkpoint
  def checkpoint_=(n: Option[String]) =
    rootState = rootState.copy(checkpoint = n)

  def lastOperationDesc = rootState.lastOperationDesc
  def lastOperationDesc_=(n: String) =
    rootState = rootState.copy(lastOperationDesc = n)
  def setLastOperationDesc(n: String) = lastOperationDesc = n

  def lastOperationRequest = rootState.lastOperationRequest
  def lastOperationRequest_=(n: SubProjectOperation) =
    rootState = rootState.copy(lastOperationRequest = Option(n))
  def setLastOperationRequest(n: SubProjectOperation) = lastOperationRequest = n

  val isSegmentation = false
  def asSegmentation: SegmentationEditor = ???

  def viewRecipe_=[T <: ViewRecipe: json.Writes](r: T) = {
    val js = TypedJson.createFromWriter(r).as[json.JsObject]
    rootState = rootState.copy(viewRecipe = Some(js))
  }
  def viewRecipe = viewer.viewRecipe
}

// Specialized editor for a SegmentationState.
class SegmentationEditor(
    val parent: ProjectEditor,
    val segmentationName: String) extends ProjectEditor {

  implicit val manager = parent.manager

  {
    // If this segmentation editor is called with new segmentationName then we
    // create the corresponding SegmentationState in the project state's segmentations field.
    val oldSegs = parent.state.segmentations
    if (!oldSegs.contains(segmentationName)) {
      ProjectFrame.validateName(segmentationName)
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

  def viewer = parent.viewer.segmentation(segmentationName)

  def rootState = parent.rootState

  def rootEditor = parent.rootEditor

  override val isSegmentation = true
  override val asSegmentation: SegmentationEditor = this

  def belongsTo = viewer.belongsTo
  def belongsTo_=(e: EdgeBundle): Unit = {
    segmentationState = segmentationState.copy(belongsToGUID = Some(e.gUID))
    scalars.set("!coverage", graph_operations.Coverage.run(e).srcCoverage)
    scalars.set("!nonEmpty", graph_operations.Coverage.run(e).dstCoverage)
    scalars.set("!belongsToEdges", graph_operations.Count.run(e))
  }

  override protected def updateVertexSet(e: VertexSet): Unit = {
    if (e != vertexSet) {
      super.updateVertexSet(e)
      val op = graph_operations.EmptyEdgeBundle()
      belongsTo = op(op.src, parent.vertexSet)(op.dst, e).result.eb
    }
  }

  def setLastOperationDesc(n: String) =
    parent.setLastOperationDesc(s"$n on $segmentationName")

  def setLastOperationRequest(n: SubProjectOperation) =
    parent.setLastOperationRequest(n.copy(path = segmentationName +: n.path))
}

// Represents a mutable, named project. It can be seen as a modifiable pointer into the
// checkpoint tree with some additional metadata. ProjectFrame's data is persisted using tags.
class ProjectFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends ObjectFrame(path) {
  // The farthest checkpoint available in the current redo sequence
  private def farthestCheckpoint: String = get(rootDir / "farthestCheckpoint")
  private def farthestCheckpoint_=(x: String): Unit = set(rootDir / "farthestCheckpoint", x)

  // The next checkpoint in the current redo sequence if a redo is available
  def nextCheckpoint: Option[String] = get(rootDir / "nextCheckpoint") match {
    case "" => None
    case x => Some(x)
  }
  private def nextCheckpoint_=(x: Option[String]): Unit =
    set(rootDir / "nextCheckpoint", x.getOrElse(""))

  def undo(): Unit = manager.synchronized {
    nextCheckpoint = Some(checkpoint)
    checkpoint = currentState.previousCheckpoint.get
  }
  def redo(): Unit = manager.synchronized {
    val next = nextCheckpoint.get
    checkpoint = next
    val reverseCheckpoints = Stream.iterate(farthestCheckpoint) {
      next => getCheckpointState(next).previousCheckpoint.get
    }
    nextCheckpoint = reverseCheckpoints.takeWhile(_ != next).lastOption
  }

  def setCheckpoint(cp: String): Unit = manager.synchronized {
    checkpoint = cp
    farthestCheckpoint = cp
    nextCheckpoint = None
  }

  // Initializes a new project. One needs to call this to make the initial preparations
  // for a project in the tag tree.
  def initialize(): Unit = manager.synchronized {
    checkpoint = ""
    nextCheckpoint = None
    farthestCheckpoint = ""
  }

  def nextState: Option[RootProjectState] = nextCheckpoint.map(getCheckpointState(_))

  def subproject = SubProject(this, Seq())

  override def copy(to: DirectoryEntry): ProjectFrame = super.copy(to).asProjectFrame
}
object ProjectFrame {
  val separator = "."
  val quotedSeparator = java.util.regex.Pattern.quote(ProjectFrame.separator)
  val reservedWords = Set(
    "readACL", "writeACL",
    "checkpoint", "nextCheckpoint",
    "farthestCheckpoint",
    "objectType", "details")
  // Do not add to the set above, begin internal names with '!'.

  def validateName(name: String, what: String = "Name",
                   allowSlash: Boolean = false,
                   allowEmpty: Boolean = false): Unit = {
    assert(allowEmpty || name.nonEmpty, s"$what cannot be empty.")
    assert(!name.startsWith("!"), s"$what ($name) cannot start with '!'.")
    assert(!name.contains(separator), s"$what ($name) cannot contain '$separator'.")
    assert(allowSlash || !name.contains("/"), s"$what ($name) cannot contain '/'.")
    val path = SymbolPath.parse(name)
    if (path.nonEmpty) {
      val name = path.last.name
      assert(!reservedWords.contains(name), s"$name is a reserved word")
    }
  }

  def fromName(name: String)(implicit manager: MetaGraphManager) =
    DirectoryEntry.fromName(name).asProjectFrame
}

// Represents a named but not necessarily root project. A SubProject is identified by a ProjectFrame
// representing the named root project and a sequence of segmentation names which show how one
// should climb down the project tree.
// When referring to SubProjects via a single string, we use the format:
//   RootProjectName.Seg1Name.Seg2Name.Seg3...
case class SubProject(val frame: ProjectFrame, val path: Seq[String]) {
  def viewer = frame.viewer.offspringViewer(path)
  def fullName = (frame.name +: path).mkString(ProjectFrame.separator)
  def toFE()(implicit epm: EntityProgressManager): FEProject = {
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
  def splitPipedPath(pipedPath: String) = pipedPath.split(ProjectFrame.quotedSeparator, -1)
  def parsePath(projectName: String)(implicit metaManager: MetaGraphManager): SubProject = {
    val nameElements = splitPipedPath(projectName)
    new SubProject(ProjectFrame.fromName(nameElements.head), nameElements.tail)
  }
}

class ViewFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends ObjectFrame(path) {
  def initializeFromConfig[T <: ViewRecipe: json.Writes](
    recipe: T, notes: String): Unit = manager.synchronized {
    initializeFromCheckpoint(ViewRecipe.saveAsCheckpoint(recipe, notes))
    details = TypedJson.createFromWriter(recipe).as[json.JsObject]
  }

  def initializeFromCheckpoint(cp: String): Unit = manager.synchronized {
    set(rootDir / "objectType", "view")
    checkpoint = cp
  }

  override def isDirectory: Boolean = false
  def getRecipe: ViewRecipe = viewer.viewRecipe.get
}

class SnapshotFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends ObjectFrame(path) {

  def initialize(state: BoxOutputState) = {
    set(rootDir / "objectType", "snapshot")
    import WorkspaceJsonFormatters.fBoxOutputState
    details = json.Json.toJson(state).as[JsObject]
  }

  override def toListElementFE()(implicit epm: EntityProgressManager) = {
    try {
      FEEntryListElement(
        name = name,
        objectType = objectType,
        icon = getState().kind,
        details = details)
    } catch {
      case ex: Throwable =>
        log.warn(s"Could not list $name:", ex)
        FEEntryListElement(
          name = name,
          objectType = objectType,
          icon = getState().kind,
          error = Some(ex.getMessage)
        )
    }
  }

  def getState(): BoxOutputState = {
    import WorkspaceJsonFormatters.fBoxOutputState
    details.get.as[BoxOutputState]
  }

  override def isDirectory: Boolean = false
}

class WorkspaceFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends ProjectFrame(path) {
  override def initialize(): Unit = manager.synchronized {
    super.initialize()
    set(rootDir / "objectType", "workspace")
  }
  override def subproject = ???
  def workspace: Workspace = viewer.rootState.workspace.getOrElse(Workspace.from())
}

abstract class ObjectFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends DirectoryEntry(path) {
  val name = path.toString
  assert(!name.contains(ProjectFrame.separator), s"Invalid project name: $name")

  // Current checkpoint of the project
  def checkpoint: String = {
    assert(exists, s"$this does not exist.")
    get(rootDir / "checkpoint")
  }
  protected def checkpoint_=(x: String): Unit = set(rootDir / "checkpoint", x)

  protected def getCheckpointState(checkpoint: String): RootProjectState =
    manager.checkpointRepo.readCheckpoint(checkpoint)

  def currentState: RootProjectState = getCheckpointState(checkpoint)

  def details: Option[json.JsObject] = {
    val path = rootDir / "details"
    existing(path).map(get).map(s => json.Json.parse(s).as[json.JsObject])
  }

  protected def details_=(x: json.JsObject): Unit = set(rootDir / "details", json.Json.stringify(x))

  def viewer = new RootProjectViewer(currentState)

  def objectType: String = get(rootDir / "objectType", "project")

  def toListElementFE()(implicit epm: EntityProgressManager) = {
    try {
      viewer.toListElementFE(name, objectType, details)
    } catch {
      case ex: Throwable =>
        log.warn(s"Could not list $name:", ex)
        FEEntryListElement(
          name = name,
          objectType = objectType,
          icon = objectType,
          error = Some(ex.getMessage)
        )
    }
  }

  override def copy(to: DirectoryEntry): ObjectFrame = super.copy(to).asObjectFrame
}

class Directory(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends DirectoryEntry(path) {
  // Returns the list of all directories contained in this directory.
  def listDirectoriesRecursively: Seq[Directory] = {
    val dirs = list.filter(_.isDirectory).map(_.asDirectory)
    dirs ++ dirs.flatMap(_.listDirectoriesRecursively)
  }

  // Returns the list of all projects contained in this directory.
  def listObjectsRecursively: Seq[ObjectFrame] = {
    val (dirs, objects) = list.partition(_.isDirectory)
    objects.map(_.asObjectFrame) ++ dirs.map(_.asDirectory).flatMap(_.listObjectsRecursively)
  }

  // Lists directories and projects inside this directory.
  def list: Seq[DirectoryEntry] = {
    val rooted = DirectoryEntry.root / path
    if (manager.tagExists(rooted)) {
      val tags = manager.lsTag(rooted).filter(manager.tagIsDir(_))
      val unrooted = tags.map(path => new SymbolPath(path.drop(DirectoryEntry.root.size)))
      unrooted.map(DirectoryEntry.fromPath(_))
    } else Nil
  }

  override def copy(to: DirectoryEntry): Directory = super.copy(to).asDirectory
}
object Directory {
  def fromName(name: String)(implicit manager: MetaGraphManager) =
    DirectoryEntry.fromName(name).asDirectory
}

// May be a directory a project frame or a table.
class DirectoryEntry(val path: SymbolPath)(
    implicit manager: MetaGraphManager) extends AccessControl {

  override def toString = path.toString
  override def equals(p: Any) =
    p.isInstanceOf[DirectoryEntry] && toString == p.asInstanceOf[DirectoryEntry].toString
  override def hashCode = toString.hashCode
  val rootDir: SymbolPath = DirectoryEntry.root / path

  def exists = manager.tagExists(rootDir)

  protected def existing(tag: SymbolPath): Option[SymbolPath] =
    if (manager.tagExists(tag)) Some(tag) else None
  protected def set(tag: SymbolPath, content: String): Unit = manager.setTag(tag, content)
  protected def get(tag: SymbolPath): String = manager.synchronized {
    existing(tag).map(manager.getTag(_)).get
  }
  protected def get(tag: SymbolPath, default: String): String = manager.synchronized {
    existing(tag).map(manager.getTag(_)).getOrElse(default)
  }

  def readACL: String = get(rootDir / "readACL", "*")
  def readACL_=(x: String): Unit = set(rootDir / "readACL", x)

  def writeACL: String = get(rootDir / "writeACL", "*")
  def writeACL_=(x: String): Unit = set(rootDir / "writeACL", x)

  // Some simple ACL definitions for object creation.
  def setupACL(privacy: String, user: User): Unit = {
    privacy match {
      case "private" =>
        writeACL = user.email
        readACL = user.email
      case "public-read" =>
        writeACL = user.email
        readACL = "*"
      case "public-write" =>
        writeACL = "*"
        readACL = "*"
    }
  }

  def assertParentWriteAllowedFrom(user: User): Unit = {
    if (!parent.isEmpty) {
      parent.get.assertWriteAllowedFrom(user)
    }
  }
  def readAllowedFrom(user: User): Boolean = {
    user.isAdmin || (localReadAllowedFrom(user) && transitiveReadAllowedFrom(user, parent))
  }
  def writeAllowedFrom(user: User): Boolean = {
    user.isAdmin || (localWriteAllowedFrom(user) && transitiveReadAllowedFrom(user, parent))
  }

  protected def transitiveReadAllowedFrom(user: User, p: Option[Directory]): Boolean = {
    p.isEmpty || (p.get.localReadAllowedFrom(user) && transitiveReadAllowedFrom(user, p.get.parent))
  }
  protected def localReadAllowedFrom(user: User): Boolean = {
    // Write access also implies read access.
    localWriteAllowedFrom(user) || aclContains(readACL, user)
  }
  protected def localWriteAllowedFrom(user: User): Boolean = {
    aclContains(writeACL, user)
  }

  def remove(): Unit = manager.synchronized {
    existing(rootDir).foreach(manager.rmTag(_))
    log.info(s"An entry has been discarded: $rootDir")
  }

  private def cp(from: SymbolPath, to: SymbolPath) = manager.synchronized {
    existing(to).foreach(manager.rmTag(_))
    manager.cpTag(from, to)
  }
  def copy(to: DirectoryEntry): DirectoryEntry = {
    cp(rootDir, to.rootDir)
    // We "reread" the path, as now it may have a more specific type.
    DirectoryEntry.fromPath(to.path)
  }

  def parent = {
    if (path.size > 1) Some(new Directory(path.init))
    else if (path.size == 1) Some(DirectoryEntry.rootDirectory)
    else None
  }
  def parents: Iterable[Directory] = parent ++ parent.toSeq.flatMap(_.parents)

  def hasCheckpoint = manager.tagExists(rootDir / "checkpoint")
  def hasObjectType = manager.tagExists(rootDir / "objectType")
  def isTable = get(rootDir / "objectType", "") == "table"
  def isProject = hasCheckpoint && !isView && !isWorkspace
  def isDirectory = exists && !hasCheckpoint && !hasObjectType
  def isView = get(rootDir / "objectType", "") == "view"
  def isWorkspace = get(rootDir / "objectType", "") == "workspace"
  def isSnapshot = get(rootDir / "objectType", "") == "snapshot"
  def getObjectType = get(rootDir / "objectType", "")

  def asProjectFrame: ProjectFrame = {
    assert(isInstanceOf[ProjectFrame], s"Entry '$path' is not a project.")
    asInstanceOf[ProjectFrame]
  }
  def asNewProjectFrame(): ProjectFrame = {
    assert(!exists, s"Entry '$path' already exists.")
    val res = new ProjectFrame(path)
    res.initialize()
    res
  }
  def asNewProjectFrame(checkpoint: String): ProjectFrame = {
    val res = asNewProjectFrame()
    res.setCheckpoint(checkpoint)
    res
  }

  def asWorkspaceFrame(): WorkspaceFrame = {
    assert(isInstanceOf[WorkspaceFrame], s"Entry '$path' is not a workspace.")
    asInstanceOf[WorkspaceFrame]
  }
  def asNewWorkspaceFrame(): WorkspaceFrame = {
    assert(!exists, s"Entry '$path' already exists.")
    val res = new WorkspaceFrame(path)
    res.initialize()
    res
  }
  def asNewWorkspaceFrame(checkpoint: String): WorkspaceFrame = {
    val res = asNewWorkspaceFrame()
    res.setCheckpoint(checkpoint)
    res
  }

  def asObjectFrame: ObjectFrame = {
    assert(isInstanceOf[ObjectFrame], s"Entry '$path' is not an object")
    asInstanceOf[ObjectFrame]
  }

  def asDirectory: Directory = {
    assert(isInstanceOf[Directory], s"Entry '$path' is not a directory")
    asInstanceOf[Directory]
  }
  def asNewDirectory(): Directory = {
    assert(!exists, s"Entry '$path' already exists")
    val res = new Directory(path)
    res.readACL = "*"
    res.writeACL = ""
    res
  }

  def asViewFrame(): ViewFrame = {
    assert(isInstanceOf[ViewFrame], s"Entry '$path' is not a view")
    asInstanceOf[ViewFrame]
  }

  def asNewViewFrame[T <: ViewRecipe: json.Writes](recipe: T, notes: String): ViewFrame = {
    assert(!exists, s"Entry '$path' already exists")
    val res = new ViewFrame(path)
    res.initializeFromConfig(recipe, notes)
    res
  }
  def asNewViewFrame(checkpoint: String): ViewFrame = {
    val res = new ViewFrame(path)
    res.initializeFromCheckpoint(checkpoint)
    res
  }

  def asNewSnapshotFrame(calculatedState: BoxOutputState): SnapshotFrame = {
    assert(!exists, s"Entry '$path' already exists")
    val snapshot = new SnapshotFrame(path)
    snapshot.initialize(calculatedState)
    snapshot
  }
  def asSnapshotFrame: SnapshotFrame = {
    assert(isInstanceOf[SnapshotFrame], s"Entry '$path' is not a snapshot.")
    asInstanceOf[SnapshotFrame]
  }
}

object DirectoryEntry {
  val root = SymbolPath("projects")

  def rootDirectory(implicit metaManager: MetaGraphManager) = new Directory(SymbolPath())

  def fromName(path: String)(implicit metaManager: MetaGraphManager): DirectoryEntry = {
    ProjectFrame.validateName(path, "Name", allowSlash = true, allowEmpty = true)
    fromPath(SymbolPath.parse(path))
  }

  def fromPath(path: SymbolPath)(implicit metaManager: MetaGraphManager): DirectoryEntry = {
    val entry = new DirectoryEntry(path)
    val nonDirParent = entry.parents.find(e => e.exists && !e.isDirectory)
    assert(
      nonDirParent.isEmpty,
      s"Invalid path: $path. Parent ${nonDirParent.get} is not a directory.")
    if (entry.exists) {
      if (entry.isProject) {
        new ProjectFrame(entry.path)
      } else if (entry.isView) {
        new ViewFrame(entry.path)
      } else if (entry.isWorkspace) {
        new WorkspaceFrame(entry.path)
      } else if (entry.isSnapshot) {
        new SnapshotFrame(entry.path)
      } else {
        new Directory(entry.path)
      }
    } else {
      entry
    }
  }
}
