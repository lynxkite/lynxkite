package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import scala.reflect.runtime.universe._

class Project(val projectName: String)(implicit manager: MetaGraphManager) {
  val separator = "|"
  assert(!projectName.contains(separator), s"Invalid project name: $projectName")
  val path: SymbolPath = s"projects/$projectName"
  def toFE: FEProject = {
    assert(manager.tagExists(path / "notes"), s"No such project: $projectName")
    val vs = Option(vertexSet).map(_.gUID.toString).getOrElse("")
    val eb = Option(edgeBundle).map(_.gUID.toString).getOrElse("")
    def feAttr[T](e: TypedEntity[T], name: String) = {
      val canBucket = Seq(typeOf[Double], typeOf[String]).exists(e.typeTag.tpe <:< _)
      val canFilter = Seq(typeOf[Double], typeOf[String], typeOf[Long], typeOf[Vector[_]])
        .exists(e.typeTag.tpe <:< _)
      val isNumeric = Seq(typeOf[Double]).exists(e.typeTag.tpe <:< _)
      FEAttribute(e.gUID.toString, name, e.typeTag.tpe.toString, canBucket, canFilter, isNumeric)
    }
    FEProject(
      projectName, lastOperation, nextOperation, vs, eb, notes,
      scalars.map { case (name, scalar) => feAttr(scalar, name) }.toSeq,
      vertexAttributes.map { case (name, attr) => feAttr(attr, name) }.toSeq,
      edgeAttributes.map { case (name, attr) => feAttr(attr, name) }.toSeq,
      segmentations.map(_.toFE),
      opCategories = Seq())
  }

  private def checkpoints: Seq[String] = get("checkpoints") match {
    case "" => Seq()
    case x => x.split(java.util.regex.Pattern.quote(separator), -1)
  }
  private def checkpoints_=(cs: Seq[String]): Unit = set("checkpoints", cs.mkString(separator))
  private def checkpointIndex = get("checkpointIndex") match {
    case "" => 0
    case x => x.toInt
  }
  private def checkpointIndex_=(x: Int): Unit = set("checkpointIndex", x.toString)

  private def lastOperation = get("lastOperation")
  private def lastOperation_=(x: String): Unit = set("lastOperation", x)
  private def nextOperation = manager.synchronized {
    val i = checkpointIndex + 1
    if (checkpoints.size <= i) ""
    else manager.getTag(s"${checkpoints(i)}/lastOperation")
  }

  def checkpointAfter(op: String): Unit = manager.synchronized {
    lastOperation = op
    val nextIndex = if (checkpoints.nonEmpty) checkpointIndex + 1 else 0
    val timestamp = Timestamp.toString
    val checkpoint = s"checkpoints/$path/$timestamp"
    checkpoints = checkpoints.take(nextIndex) :+ checkpoint
    checkpointIndex = nextIndex
    cp(path, checkpoint)
  }
  def undo(): Unit = manager.synchronized {
    // checkpoints and checkpointIndex are not restored, but copied over from the current state.
    val c = checkpoints
    val i = checkpointIndex
    assert(i > 0, s"Already at checkpoint $i.")
    cp(c(i - 1), path)
    checkpointIndex = i - 1
    checkpoints = c
  }
  def redo(): Unit = manager.synchronized {
    // checkpoints and checkpointIndex are not restored, but copied over from the current state.
    val c = checkpoints
    val i = checkpointIndex
    assert(i < c.size - 1, s"Already at checkpoint $i of ${c.size}.")
    cp(c(i + 1), path)
    checkpointIndex = i + 1
    checkpoints = c
  }
  def reloadCurrentCheckpoint(): Unit = manager.synchronized {
    // checkpoints and checkpointIndex are not restored, but copied over from the current state.
    val c = checkpoints
    val i = checkpointIndex
    assert(c.nonEmpty, "No checkpoints.")
    cp(c(i), path)
    checkpointIndex = i
    checkpoints = c
  }

  def isSegmentation = manager.synchronized {
    val grandFather = path.parent.parent
    grandFather.nonEmpty && (grandFather.name == 'segmentations)
  }
  def asSegmentation = manager.synchronized {
    assert(isSegmentation, s"$projectName is not a segmentation")
    // If our parent is a top-level project, path is like:
    //   project/parentName/segmentations/segmentationName/project
    val parentName = new SymbolPath(path.drop(1).dropRight(3))
    val segmentationName = path.dropRight(1).last.name
    Segmentation(parentName.toString, segmentationName)
  }

  def notes = get("notes")
  def notes_=(n: String) = set("notes", n)

  def vertexSet = manager.synchronized {
    existing(path / "vertexSet")
      .flatMap(vsPath => Project.doOrNone(
        manager.vertexSet(vsPath), s"Couldn't resolve vertex set of project $projectName"))
      .getOrElse(null)
  }
  def vertexSet_=(e: VertexSet): Unit = {
    updateVertexSet(e, killSegmentations = true)
  }

  def setVertexSet(e: VertexSet, idAttr: String): Unit = manager.synchronized {
    vertexSet = e
    vertexAttributes(idAttr) = graph_operations.IdAsAttribute.run(e)
  }

  private def updateVertexSet(e: VertexSet, killSegmentations: Boolean) = manager.synchronized {
    if (e != vertexSet) {
      // TODO: "Induce" the edges and attributes to the new vertex set.
      edgeBundle = null
      vertexAttributes = Map()
      if (killSegmentations) segmentations.foreach(_.remove())
    }
    set("vertexSet", e)
    if (e != null) {
      val op = graph_operations.CountVertices()
      scalars("vertex_count") = op(op.vertices, e).result.count
    } else {
      scalars("vertex_count") = null
    }
  }

  def pullBackWithInjection(injection: EdgeBundle): Unit = manager.synchronized {
    assert(injection.properties.compliesWith(EdgeBundleProperties.injection))
    assert(injection.dstVertexSet.gUID == vertexSet.gUID)
    val origVS = vertexSet
    val origVAttrs = vertexAttributes.toIndexedSeq
    val origEB = edgeBundle
    val origEAttrs = edgeAttributes.toIndexedSeq

    updateVertexSet(injection.srcVertexSet, killSegmentations = false)
    origVAttrs.foreach {
      case (name, attr) =>
        vertexAttributes(name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(attr, injection)
    }

    var edgeInduction: graph_operations.InducedEdgeBundle.Output = null
    if (origEB != null) {
      val iop = graph_operations.InducedEdgeBundle()
      edgeInduction = iop(
        iop.srcMapping, graph_operations.ReverseEdges.run(injection))(
          iop.dstMapping, graph_operations.ReverseEdges.run(injection))(
            iop.edges, origEB).result
      edgeBundle = edgeInduction.induced
    }

    origEAttrs.foreach {
      case (name, attr) =>
        edgeAttributes(name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(attr, edgeInduction.embedding)
    }

    segmentations.foreach { seg =>
      val op = graph_operations.InducedEdgeBundle(induceDst = false)
      seg.belongsTo = op(
        op.srcMapping, graph_operations.ReverseEdges.run(injection))(
          op.edges, seg.belongsTo).result.induced
    }

    if (isSegmentation) {
      val seg = asSegmentation
      val op = graph_operations.InducedEdgeBundle(induceSrc = false)
      seg.belongsTo = op(
        op.dstMapping, graph_operations.ReverseEdges.run(injection))(
          op.edges, seg.belongsTo).result.induced
    }
  }

  def edgeBundle = manager.synchronized {
    existing(path / "edgeBundle")
      .flatMap(ebPath => Project.doOrNone(
        manager.edgeBundle(ebPath), s"Couldn't resolve edge bundle of project $projectName"))
      .getOrElse(null)
  }
  def edgeBundle_=(e: EdgeBundle) = manager.synchronized {
    if (e != edgeBundle) {
      assert(e == null || vertexSet != null, s"No vertex set for project $projectName")
      assert(e == null || e.srcVertexSet == vertexSet, s"Edge bundle does not match vertex set for project $projectName")
      assert(e == null || e.dstVertexSet == vertexSet, s"Edge bundle does not match vertex set for project $projectName")
      // TODO: "Induce" the attributes to the new edge bundle.
      edgeAttributes = Map()
    }
    set("edgeBundle", e)
    if (e != null) {
      val op = graph_operations.CountEdges()
      scalars("edge_count") = op(op.edges, e).result.count
    } else {
      scalars("edge_count") = null
    }
  }

  def scalars = new ScalarHolder
  def scalars_=(scalars: Map[String, Scalar[_]]) = manager.synchronized {
    existing(path / "scalars").foreach(manager.rmTag(_))
    for ((name, scalar) <- scalars) {
      manager.setTag(path / "scalars" / name, scalar)
    }
  }
  def scalarNames[T: TypeTag] = scalars.collect {
    case (name, scalar) if typeOf[T] =:= typeOf[Nothing] || scalar.is[T] => name
  }.toSeq

  def vertexAttributes = new VertexAttributeHolder
  def vertexAttributes_=(attrs: Map[String, VertexAttribute[_]]) = manager.synchronized {
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
  def edgeAttributes_=(attrs: Map[String, VertexAttribute[_]]) = manager.synchronized {
    existing(path / "edgeAttributes").foreach(manager.rmTag(_))
    assert(attrs.isEmpty || edgeBundle != null, s"No edge bundle for project $projectName")
    for ((name, attr) <- attrs) {
      assert(attr.vertexSet == edgeBundle.asVertexSet, s"Edge attribute $name does not match edge bundle for project $projectName")
      manager.setTag(path / "edgeAttributes" / name, attr)
    }
  }
  def edgeAttributeNames[T: TypeTag] = edgeAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def segmentations = segmentationNames.map(segmentation(_))
  def segmentation(name: String) = Segmentation(projectName, name)
  def segmentationNames = ls("segmentations").map(_.last.name)

  def copy(to: Project): Unit = cp(path, to.path)
  def remove(): Unit = manager.synchronized {
    manager.rmTag(path)
    log.info(s"A project has been discarded: $path")
  }

  private def cp(from: SymbolPath, to: SymbolPath) = manager.synchronized {
    existing(to).foreach(manager.rmTag(_))
    manager.cpTag(from, to)
  }

  private def existing(tag: SymbolPath): Option[SymbolPath] =
    if (manager.tagExists(tag)) Some(tag) else None
  private def set(tag: String, entity: MetaGraphEntity): Unit = manager.synchronized {
    if (entity == null) {
      existing(path / tag).foreach(manager.rmTag(_))
    } else {
      manager.setTag(path / tag, entity)
    }
  }
  private def set(tag: String, content: String): Unit = manager.setTag(path / tag, content)
  private def get(tag: String): String = manager.synchronized {
    existing(path / tag).map(manager.getTag(_)).getOrElse("")
  }
  private def ls(dir: String) = manager.synchronized {
    if (manager.tagExists(path / dir)) manager.lsTag(path / dir) else Nil
  }

  abstract class Holder[T <: MetaGraphEntity](dir: String) extends Iterable[(String, T)] {
    def validate(name: String, entity: T): Unit
    def update(name: String, entity: T) = manager.synchronized {
      if (entity == null) {
        existing(path / dir / name).foreach(manager.rmTag(_))
      } else {
        validate(name, entity)
        manager.setTag(path / dir / name, entity)
      }
    }
    def apply(name: String): T =
      manager.entity(path / dir / name).asInstanceOf[T]

    def iterator = manager.synchronized {
      ls(dir)
        .flatMap { path =>
          val name = path.last.name
          Project.doOrNone(apply(name), s"Couldn't resolve $path").map(name -> _)
        }
        .iterator
    }

    def contains(x: String) = iterator.exists(_._1 == x)
  }
  class ScalarHolder extends Holder[Scalar[_]]("scalars") {
    def validate(name: String, scalar: Scalar[_]) = {}
  }
  class VertexAttributeHolder extends Holder[VertexAttribute[_]]("vertexAttributes") {
    def validate(name: String, attr: VertexAttribute[_]) =
      assert(attr.vertexSet == vertexSet, s"Vertex attribute $name does not match vertex set for project $projectName")
  }
  class EdgeAttributeHolder extends Holder[VertexAttribute[_]]("edgeAttributes") {
    def validate(name: String, attr: VertexAttribute[_]) =
      assert(attr.vertexSet == edgeBundle.asVertexSet, s"Edge attribute $name does not match edge bundle for project $projectName")
  }
}

object Project {
  def apply(projectName: String)(implicit metaManager: MetaGraphManager): Project = new Project(projectName)

  def doOrNone[T](op: => T, onErrorLog: String): Option[T] =
    try {
      Some(op)
    } catch {
      case e: Exception => {
        log.error(onErrorLog, e)
        None
      }
    }
}

case class Segmentation(parentName: String, name: String)(implicit manager: MetaGraphManager) {
  def parent = Project(parentName)
  val path: SymbolPath = s"projects/$parentName/segmentations/$name"
  def toFE =
    FESegmentation(
      name,
      project.projectName,
      UIValue.fromEntity(belongsTo),
      UIValue(id = belongsToAttribute.gUID.toString, title = "segmentation[%s]".format(name)))
  def belongsTo = manager.edgeBundle(path / "belongsTo")
  def belongsTo_=(eb: EdgeBundle) = manager.synchronized {
    assert(eb.dstVertexSet == project.vertexSet, s"Incorrect 'belongsTo' relationship for $name")
    manager.setTag(path / "belongsTo", eb)
  }
  def belongsToAttribute: VertexAttribute[Vector[ID]] = {
    val segmentationIds = graph_operations.IdAsAttribute.run(project.vertexSet)
    val reversedBelongsTo = graph_operations.ReverseEdges.run(belongsTo)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    aop(aop.connection, reversedBelongsTo)(aop.attr, segmentationIds).result.attr
  }
  def project = Project(s"$parentName/segmentations/$name/project")

  def rename(newName: String) = manager.synchronized {
    val to = new SymbolPath(path.init) / newName
    manager.cpTag(path, to)
    manager.rmTag(path)
  }
  def remove(): Unit = manager.synchronized {
    manager.rmTag(path)
  }
}
