// Projects are a complex type of box output state. They hold graphs and segmentations.
// This file also contains the classes for the LynxKite directory structure.
// (Directories came later than projects. But since LynxKite 2.0 projects are no longer stored.
// TODO: Split into separate files for clarity.)

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
    elementMetadata: Map[String, Map[String, String]]) {
  def mapGuids(change: UUID => UUID): CommonProjectState = {
    CommonProjectState(
      vertexSetGUID.map(change),
      vertexAttributeGUIDs.mapValues(change),
      edgeBundleGUID.map(change),
      edgeAttributeGUIDs.mapValues(change),
      scalarGUIDs.mapValues(change),
      segmentations.mapValues(_.mapGuids(change)),
      notes,
      elementMetadata)
  }
}
object CommonProjectState {
  val emptyState = CommonProjectState(
    None, Map(), None, Map(), Map(), Map(), "", Map())
}

// This gets written into a checkpoint.
case class CheckpointObject(
  workspace: Option[Workspace] = None,
  snapshot: Option[BoxOutputState] = None,
  checkpoint: Option[String] = None,
  previousCheckpoint: Option[String] = None)

// Complete state of segmentation.
case class SegmentationState(
    state: CommonProjectState,
    belongsToGUID: Option[UUID]) {
  def mapGuids(change: UUID => UUID): SegmentationState = {
    SegmentationState(
      state.mapGuids(change),
      belongsToGUID.map(change))
  }
}
object SegmentationState {
  val emptyState = SegmentationState(CommonProjectState.emptyState, None)
}

// Rich interface for looking at project states.
sealed trait ProjectViewer {
  val rootState: CommonProjectState
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
    getElementMetadata(kind, name).getOrElse("note", "")
  def getElementMetadata(kind: ElementKind, name: String): Map[String, String] =
    state.elementMetadata.getOrElse(kind / name, Map())

  def offspringPath: Seq[String]
  def offspringViewer(path: Seq[String]): ProjectViewer =
    if (path.isEmpty) this
    else segmentation(path.head).offspringViewer(path.tail)

  def editor: ProjectEditor

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
      // TODO: Remove these.
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
          rootName, rootRelativePath + segmentation.segmentationName + SubProject.separator)
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
      maybeProtoTable(edgeBundle, EdgeTableName) ++
      maybeProtoTable(1, ScalarTableName)
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
      case VertexTableName => ProtoTable(vertexSet, vertexAttributes.toSeq.sortBy(_._1))
      case ScalarTableName => ProtoTable.scalar(scalars.toSeq.sortBy(_._1))

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
        ProtoTable(edgeBundle.idSet, (edgeAttrs ++ srcAttrs ++ dstAttrs).toSeq.sortBy(_._1))
      }
      case EdgeAttributeTableName => ProtoTable(edgeBundle.idSet, edgeAttributes.toSeq.sortBy(_._1))
      case BelongsToTableName =>
        throw new AssertionError("Only segmentations have a BelongsTo table")
      case _ => {
        val correctTableNames = List(ScalarTableName, VertexTableName, EdgeTableName, EdgeAttributeTableName,
          BelongsToTableName).mkString(", ")
        throw new AssertionError("Not recognized table name. Correct table names: " +
          s"$correctTableNames")
      }
    }
    protoTable
  }
}

object ProjectViewer {
  val ScalarTableName = "scalars"
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

// ProjectViewer for the root state.
class RootProjectViewer(val rootState: CommonProjectState)(implicit val manager: MetaGraphManager)
    extends ProjectViewer {
  val state = rootState
  def rootViewer = this
  def editor: RootProjectEditor = new RootProjectEditor(state)

  val isSegmentation = false
  def asSegmentation: SegmentationViewer = ???
  def offspringPath: Seq[String] = Nil

  protected def getFEMembers()(implicit epm: EntityProgressManager): Option[FEAttribute] = None
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
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(aop.connectionBySrc, graph_operations.HybridEdgeBundle.byDst(belongsTo))(
      aop.attr, segmentationIds).result.attr
  }

  lazy val membersAttribute: Attribute[Vector[ID]] = {
    val parentIds = graph_operations.IdAsAttribute.run(parent.vertexSet)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(
      aop.connectionBySrc, graph_operations.HybridEdgeBundle.bySrc(belongsTo))(
        aop.attr, parentIds).result.attr
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
      rootName + SubProject.separator + rootRelativePath + segmentationName,
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
        ProtoTable(belongsTo.idSet, (baseAttrs ++ segAttrs).toSeq.sortBy(_._1))
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
  implicit val fCheckpointObject = Json.format[CheckpointObject]
  implicit val fCommonProjectState = Json.format[CommonProjectState]

  private def commonProjectStateToJSon(state: CommonProjectState): json.JsValue = Json.toJson(state)
  private def jsonToCommonProjectState(j: json.JsValue): CommonProjectState =
    j.as[CommonProjectState]
}
class CheckpointRepository(val baseDir: String) {
  import CheckpointRepository.fCheckpointObject
  import CheckpointRepository.checkpointFilePrefix

  val baseDirFile = new File(baseDir)
  baseDirFile.mkdirs

  def checkpointFileName(checkpoint: String): File =
    new File(baseDirFile, s"${checkpointFilePrefix}${checkpoint}")

  def allCheckpoints: Map[String, CheckpointObject] =
    baseDirFile
      .list
      .filter(_.startsWith(checkpointFilePrefix))
      .map(fileName => fileName.drop(checkpointFilePrefix.length))
      .map(cp => cp -> readCheckpoint(cp))
      .toMap

  def saveCheckpointedState(checkpoint: String, state: CheckpointObject): Unit = {
    val dumpFile = new File(baseDirFile, s"dump-$checkpoint")
    val finalFile = checkpointFileName(checkpoint)
    FileUtils.writeStringToFile(
      dumpFile,
      Json.prettyPrint(Json.toJson(state)),
      "utf8")
    dumpFile.renameTo(finalFile)
  }

  def checkpointState(state: CheckpointObject, prevCheckpoint: String): CheckpointObject = {
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

  val cache = new SoftHashMap[String, CheckpointObject]()
  def readCheckpoint(checkpoint: String): CheckpointObject = {
    if (checkpoint == "") {
      CheckpointObject()
    } else synchronized {
      cache.getOrElseUpdate(checkpoint,
        Json.parse(FileUtils.readFileToString(checkpointFileName(checkpoint), "utf8"))
          .as[CheckpointObject].copy(checkpoint = Some(checkpoint)))
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
    SubProject.validateName(name)
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

  def rootState: CommonProjectState

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
    setElementMetadata(kind, name, "note", note)
  }

  def setElementMetadata(kind: ElementKind, name: String, key: String, value: String) = {
    val allMeta = state.elementMetadata
    val kindName = kind / name
    val meta = allMeta.getOrElse(kindName, Map())
    val newMeta =
      if (value == null) meta - key else meta + (key -> value)
    val newAllMeta =
      if (newMeta.isEmpty) allMeta - kindName else allMeta + (kindName -> newMeta)
    state = state.copy(elementMetadata = newAllMeta)
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
}

// Editor that holds the state.
class RootProjectEditor(
    initialState: CommonProjectState)(
        implicit val manager: MetaGraphManager) extends ProjectEditor {
  var rootState = initialState

  def state = rootState
  def state_=(newState: CommonProjectState): Unit = {
    rootState = newState
  }

  def viewer = new RootProjectViewer(state)

  def rootEditor: RootProjectEditor = this

  val isSegmentation = false
  def asSegmentation: SegmentationEditor = ???
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
      SubProject.validateName(segmentationName)
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
}

// Represents a mutable, named workspace. It can be seen as a modifiable pointer into the
// checkpoint tree with some additional metadata. WorkspaceFrame's data is persisted using tags.
class WorkspaceFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends ObjectFrame(path) {
  // The farthest checkpoint available in the current redo sequence
  private def farthestCheckpoint: String = get(rootDir / "!farthestCheckpoint")
  private def farthestCheckpoint_=(x: String): Unit = set(rootDir / "!farthestCheckpoint", x)

  // The next checkpoint in the current redo sequence if a redo is available
  def nextCheckpoint: Option[String] = get(rootDir / "!nextCheckpoint") match {
    case "" => None
    case x => Some(x)
  }
  private def nextCheckpoint_=(x: Option[String]): Unit =
    set(rootDir / "!nextCheckpoint", x.getOrElse(""))

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
    set(rootDir / "!objectType", "workspace")
  }

  def nextState: Option[CheckpointObject] = nextCheckpoint.map(getCheckpointState(_))

  override def copy(to: DirectoryEntry): WorkspaceFrame = super.copy(to).asWorkspaceFrame

  def workspace: Workspace = currentState.workspace.getOrElse(Workspace.from())
}
object WorkspaceFrame {

  def fromName(name: String)(implicit manager: MetaGraphManager) =
    DirectoryEntry.fromName(name).asWorkspaceFrame
}

object SubProject {
  val separator = "."
  val quotedSeparator = java.util.regex.Pattern.quote(separator)
  def splitPipedPath(pipedPath: String) = pipedPath.split(quotedSeparator, -1)

  def validateName(name: String, what: String = "Name") = {
    assert(name.nonEmpty, s"$what cannot be empty.")
    assert(!name.startsWith("!"), s"$what ($name) cannot start with '!'.")
    assert(!name.contains(separator), s"$what ($name) cannot contain '$separator'.")
  }
}

class SnapshotFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends ObjectFrame(path) {

  def initialize(state: BoxOutputState) = {
    set(rootDir / "!objectType", "snapshot")
    checkpoint = manager.checkpointRepo.checkpointState(
      CheckpointObject(snapshot = Some(state)), prevCheckpoint = "").checkpoint.get
  }

  override def toListElementFE()(implicit epm: EntityProgressManager) = {
    try {
      FEEntryListElement(
        name = name,
        objectType = objectType,
        icon = getState().kind)
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

  def getState(): BoxOutputState = currentState.snapshot.get
}

abstract class ObjectFrame(path: SymbolPath)(
    implicit manager: MetaGraphManager) extends DirectoryEntry(path) {
  val name = path.toString
  assert(!name.contains(SubProject.separator), s"Invalid project name: $name")

  // Current checkpoint of the project
  def checkpoint: String = {
    assert(exists, s"$this does not exist.")
    get(rootDir / "!checkpoint")
  }
  protected def checkpoint_=(x: String): Unit = set(rootDir / "!checkpoint", x)

  protected def getCheckpointState(checkpoint: String): CheckpointObject =
    manager.checkpointRepo.readCheckpoint(checkpoint)

  def currentState: CheckpointObject = getCheckpointState(checkpoint)

  def toListElementFE()(implicit epm: EntityProgressManager) = {
    FEEntryListElement(
      name = name,
      objectType = objectType,
      icon = objectType)
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

  def readACL: String = get(rootDir / "!readACL", "*")
  def readACL_=(x: String): Unit = set(rootDir / "!readACL", x)

  def writeACL: String = get(rootDir / "!writeACL", "*")
  def writeACL_=(x: String): Unit = set(rootDir / "!writeACL", x)

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

  def hasObjectType = manager.tagExists(rootDir / "!objectType")
  def isDirectory = exists && !hasObjectType
  def isWorkspace = objectType == "workspace"
  def isSnapshot = objectType == "snapshot"
  def objectType = get(rootDir / "!objectType", "")

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

  def validateName(name: String, what: String = "Name") = {
    val path = SymbolPath.parse(name)
    for (e <- path) {
      SubProject.validateName(e.name, what)
    }
  }

  def fromName(path: String)(implicit metaManager: MetaGraphManager): DirectoryEntry = {
    validateName(path)
    fromPath(SymbolPath.parse(path))
  }

  def fromPath(path: SymbolPath)(implicit metaManager: MetaGraphManager): DirectoryEntry = {
    val entry = new DirectoryEntry(path)
    val nonDirParent = entry.parents.find(e => e.exists && !e.isDirectory)
    assert(
      nonDirParent.isEmpty,
      s"Invalid path: $path. Parent ${nonDirParent.get} is not a directory.")
    if (entry.exists) {
      if (entry.isWorkspace) {
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
