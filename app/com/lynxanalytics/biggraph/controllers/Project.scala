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
import play.api.libs.json.Json
import scala.util.{ Failure, Success, Try }
import scala.reflect.runtime.universe._

class Project(val projectName: String)(implicit manager: MetaGraphManager) {
  override def toString = projectName
  override def equals(p: Any) =
    p.isInstanceOf[Project] && projectName == p.asInstanceOf[Project].projectName
  override def hashCode = projectName.hashCode

  assert(projectName.nonEmpty, s"Invalid project name: <empty string>")
  assert(!projectName.contains(Project.separator), s"Invalid project name: $projectName")
  val rootDir: SymbolPath = s"projects/$projectName"
  // Part of the state that needs to be checkpointed.
  val checkpointedDir: SymbolPath = rootDir / "checkpointed"
  def toFE: FEProject = {
    Try(unsafeToFE) match {
      case Success(fe) => fe
      case Failure(ex) => FEProject(
        name = projectName,
        error = ex.getMessage
      )
    }
  }

  // May raise an exception.
  private def unsafeToFE: FEProject = {
    assert(manager.tagExists(checkpointedDir / "notes"), s"No such project: $projectName")
    val vs = Option(vertexSet).map(_.gUID.toString).getOrElse("")
    val eb = Option(edgeBundle).map(_.gUID.toString).getOrElse("")
    def feAttr[T](e: TypedEntity[T], name: String, isInternal: Boolean = false) = {
      val canBucket = Seq(typeOf[Double], typeOf[String]).exists(e.typeTag.tpe <:< _)
      val canFilter = Seq(typeOf[Double], typeOf[String], typeOf[Long], typeOf[Vector[Any]])
        .exists(e.typeTag.tpe <:< _)
      val isNumeric = Seq(typeOf[Double]).exists(e.typeTag.tpe <:< _)
      FEAttribute(e.gUID.toString, name, e.typeTag.tpe.toString, canBucket, canFilter, isNumeric, isInternal)
    }
    def feList(things: Iterable[(String, TypedEntity[_])]) = {
      things.map { case (name, e) => feAttr(e, name) }.toList
    }
    val members = if (isSegmentation) {
      Some(feAttr(asSegmentation.membersAttribute, "#members", isInternal = true))
    } else {
      None
    }

    FEProject(
      name = projectName,
      undoOp = lastOperation,
      redoOp = nextOperation,
      readACL = readACL,
      writeACL = writeACL,
      vertexSet = vs,
      edgeBundle = eb,
      notes = notes,
      scalars = feList(scalars),
      vertexAttributes = feList(vertexAttributes) ++ members,
      edgeAttributes = feList(edgeAttributes),
      segmentations = segmentations.map(_.toFE).toList)
  }

  private def checkpoints: Seq[String] = get(rootDir / "checkpoints") match {
    case "" => Seq()
    case x => x.split(java.util.regex.Pattern.quote(Project.separator), -1)
  }
  private def checkpoints_=(cs: Seq[String]): Unit =
    set(rootDir / "checkpoints", cs.mkString(Project.separator))
  private def checkpointIndex = get(rootDir / "checkpointIndex") match {
    case "" => 0
    case x => x.toInt
  }
  private def checkpointIndex_=(x: Int): Unit =
    set(rootDir / "checkpointIndex", x.toString)
  def checkpointCount = if (checkpoints.nonEmpty) checkpointIndex + 1 else 0
  def checkpointDir(i: Int): SymbolPath = {
    // We used to have absolute checkpoint paths. (Until LynxKite 1.2.0.)
    // To provide backward compatibility these are transformed into relative paths.
    // TODO: Eventually remove this code.
    val path = checkpoints(i)
    val relative = if (path.startsWith("projects/")) path.split("/", -1).last else path
    rootDir / "checkpoint" / relative
  }

  def copyCheckpoint(i: Int, destination: Project): Unit = manager.synchronized {
    assert(0 <= i && i < checkpointCount, s"Requested checkpoint $i out of $checkpointCount.")
    copy(destination)
    while (destination.checkpointCount > i + 1) {
      destination.undo
    }
  }

  def lastOperation = get(checkpointedDir / "lastOperation")
  private def lastOperation_=(x: String): Unit = set(checkpointedDir / "lastOperation", x)
  private def nextOperation = manager.synchronized {
    val i = checkpointIndex + 1
    if (checkpoints.size <= i) ""
    else get(checkpointDir(i) / "lastOperation")
  }

  def checkpointAfter(op: String, req: ProjectOperationRequest = null): Unit = manager.synchronized {
    if (isSegmentation) {
      val name = asSegmentation.name
      asSegmentation.parent.checkpointAfter(s"$op on $name", req)
    } else {
      lastOperation = op
      lastOperationRequest = Option(req)
      val nextIndex = checkpointCount
      val timestamp = Timestamp.toString
      val checkpoint = rootDir / "checkpoint" / timestamp
      checkpoints = checkpoints.take(nextIndex) :+ timestamp
      checkpointIndex = nextIndex
      cp(checkpointedDir, checkpoint)
    }
  }

  def checkpoint(title: String, req: ProjectOperationRequest = null)(op: => Unit): Unit = {
    Try(op) match {
      case Success(_) =>
        // Save changes.
        checkpointAfter(title, req)
      case Failure(e) =>
        // Discard potentially corrupt changes.
        reloadCurrentCheckpoint()
        throw e;
    }
  }

  def undo(): Unit = manager.synchronized {
    assert(checkpointIndex > 0, s"Already at checkpoint $checkpointIndex.")
    checkpointIndex -= 1
    cp(checkpointDir(checkpointIndex), checkpointedDir)
  }
  def redo(): Unit = manager.synchronized {
    assert(checkpointIndex < checkpoints.size - 1,
      s"Already at checkpoint $checkpointIndex of ${checkpoints.size}.")
    checkpointIndex += 1
    cp(checkpointDir(checkpointIndex), checkpointedDir)
  }
  def reloadCurrentCheckpoint(): Unit = manager.synchronized {
    if (isSegmentation) {
      val name = asSegmentation.name
      asSegmentation.parent.reloadCurrentCheckpoint()
    } else {
      assert(checkpointIndex < checkpoints.size, s"No checkpoint $checkpointIndex.")
      cp(checkpointDir(checkpointIndex), checkpointedDir)
    }
  }

  def discardCheckpoints(): Unit = manager.synchronized {
    existing(rootDir / "checkpoints").foreach(manager.rmTag(_))
    existing(rootDir / "checkpointIndex").foreach(manager.rmTag(_))
    existing(checkpointedDir / "lastOperation").foreach(manager.rmTag(_))
    existing(checkpointedDir / "lastOperationRequest").foreach(manager.rmTag(_))
  }

  def readACL: String = {
    if (isSegmentation) asSegmentation.parent.readACL
    else get(rootDir / "readACL")
  }
  def readACL_=(x: String): Unit = {
    assert(!isSegmentation,
      s"Access control has to be set on the parent project. $this is a segmentation.")
    set(rootDir / "readACL", x)
  }
  def writeACL: String = {
    if (isSegmentation) asSegmentation.parent.writeACL
    else get(rootDir / "writeACL")
  }
  def writeACL_=(x: String): Unit = {
    assert(!isSegmentation,
      s"Access control has to be set on the parent project. $this is a segmentation.")
    set(rootDir / "writeACL", x)
  }

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

  def isSegmentation = manager.synchronized {
    val grandFather = rootDir.parent.parent
    grandFather.nonEmpty && (grandFather.name == 'segmentations)
  }
  def asSegmentation = manager.synchronized {
    assert(isSegmentation, s"$projectName is not a segmentation")
    // If our parent is a top-level project, rootDir is like:
    //   project/parentName/checkpointed/segmentations/segmentationName/project
    val parentName = new SymbolPath(rootDir.drop(1).dropRight(4))
    val segmentationName = rootDir.dropRight(1).last.name
    Segmentation(parentName.toString, segmentationName)
  }

  def notes = get(checkpointedDir / "notes")
  def notes_=(n: String) = set(checkpointedDir / "notes", n)

  implicit val fFEOperationSpec = Json.format[FEOperationSpec]
  implicit val fProjectOperationRequest = Json.format[ProjectOperationRequest]
  def lastOperationRequest = manager.synchronized {
    existing(checkpointedDir / "lastOperationRequest").map {
      tag => Json.parse(get(tag)).as[ProjectOperationRequest]
    }
  }
  def lastOperationRequest_=(spec: Option[ProjectOperationRequest]) = manager.synchronized {
    spec match {
      case Some(spec) =>
        val json = Json.prettyPrint(Json.toJson(spec))
        set(checkpointedDir / "lastOperationRequest", json)
      case None =>
        existing(checkpointedDir / "lastOperationRequest").foreach(manager.rmTag(_))
    }
  }

  def vertexSet = manager.synchronized {
    existing(checkpointedDir / "vertexSet")
      .flatMap(vsPath =>
        Project.withErrorLogging(s"Couldn't resolve vertex set of project $this") {
          manager.vertexSet(vsPath)
        })
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
      edgeBundle = null
      vertexAttributes = Map()
      if (killSegmentations) segmentations.foreach(_.remove())
      set(checkpointedDir / "vertexSet", e)
      if (e != null) {
        scalars("vertex_count") = graph_operations.Count.run(e)
      } else {
        scalars("vertex_count") = null
      }
      // This must be the last thing we do otherwise
      // we run into assertions when we update belongsTo
      if (isSegmentation) {
        val seg = asSegmentation
        val op = graph_operations.EmptyEdgeBundle()
        seg.belongsTo = op(op.src, seg.parent.vertexSet)(op.dst, e).result.eb
      }
    }
  }

  def pullBackEdges(injection: EdgeBundle): Unit = manager.synchronized {
    val op = graph_operations.PulledOverEdges()
    val newEB = op(op.originalEB, edgeBundle)(op.injection, injection).result.pulledEB
    pullBackEdges(edgeBundle, edgeAttributes.toIndexedSeq, newEB, injection)
  }
  def pullBackEdges(
    origEdgeBundle: EdgeBundle,
    origEAttrs: Seq[(String, Attribute[_])],
    newEdgeBundle: EdgeBundle,
    pullBundle: EdgeBundle): Unit = manager.synchronized {

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

  def pullBack(pullBundle: EdgeBundle): Unit = manager.synchronized {
    assert(pullBundle.properties.compliesWith(EdgeBundleProperties.partialFunction),
      s"Not a partial function: $pullBundle")
    assert(pullBundle.dstVertexSet.gUID == vertexSet.gUID,
      s"Wrong destination: $pullBundle")
    val origVS = vertexSet
    val origVAttrs = vertexAttributes.toIndexedSeq
    val origEB = edgeBundle
    val origEAttrs = edgeAttributes.toIndexedSeq
    val origBelongsTo: Option[EdgeBundle] =
      if (isSegmentation) Some(asSegmentation.belongsTo) else Some(null)

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
          op.edges, origBelongsTo.getOrElse(null)).result.induced
    }
  }

  def edgeBundle = manager.synchronized {
    existing(checkpointedDir / "edgeBundle")
      .flatMap(ebPath =>
        Project.withErrorLogging(s"Couldn't resolve edge bundle of project $this") {
          manager.edgeBundle(ebPath)
        })
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
    set(checkpointedDir / "edgeBundle", e)
    if (e != null) {
      scalars("edge_count") = graph_operations.Count.run(e)
    } else {
      scalars("edge_count") = null
    }
  }

  def scalars = new ScalarHolder
  def scalars_=(scalars: Map[String, Scalar[_]]) = manager.synchronized {
    existing(checkpointedDir / "scalars").foreach(manager.rmTag(_))
    for ((name, scalar) <- scalars) {
      manager.setTag(checkpointedDir / "scalars" / name, scalar)
    }
  }
  def scalarNames[T: TypeTag] = scalars.collect {
    case (name, scalar) if typeOf[T] =:= typeOf[Nothing] || scalar.is[T] => name
  }.toSeq

  def vertexAttributes = new VertexAttributeHolder
  def vertexAttributes_=(attrs: Map[String, Attribute[_]]) = manager.synchronized {
    existing(checkpointedDir / "vertexAttributes").foreach(manager.rmTag(_))
    assert(attrs.isEmpty || vertexSet != null, s"No vertex set for project $projectName")
    for ((name, attr) <- attrs) {
      assert(attr.vertexSet == vertexSet, s"Vertex attribute $name does not match vertex set for project $projectName")
      manager.setTag(checkpointedDir / "vertexAttributes" / name, attr)
    }
  }
  def vertexAttributeNames[T: TypeTag] = vertexAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def edgeAttributes = new EdgeAttributeHolder
  def edgeAttributes_=(attrs: Map[String, Attribute[_]]) = manager.synchronized {
    existing(checkpointedDir / "edgeAttributes").foreach(manager.rmTag(_))
    assert(attrs.isEmpty || edgeBundle != null, s"No edge bundle for project $projectName")
    for ((name, attr) <- attrs) {
      assert(attr.vertexSet == edgeBundle.idSet, s"Edge attribute $name does not match edge bundle for project $projectName")
      manager.setTag(checkpointedDir / "edgeAttributes" / name, attr)
    }
  }
  def edgeAttributeNames[T: TypeTag] = edgeAttributes.collect {
    case (name, attr) if typeOf[T] =:= typeOf[Nothing] || attr.is[T] => name
  }.toSeq

  def segmentations = segmentationNames.map(segmentation(_))
  def segmentation(name: String) = Segmentation(projectName, name)
  def segmentationNames = ls(checkpointedDir / "segmentations").map(_.last.name)

  def copy(to: Project): Unit = cp(rootDir, to.rootDir)
  def remove(): Unit = manager.synchronized {
    existing(rootDir).foreach(manager.rmTag(_))
    log.info(s"A project has been discarded: $rootDir")
  }

  private def cp(from: SymbolPath, to: SymbolPath) = manager.synchronized {
    existing(to).foreach(manager.rmTag(_))
    manager.cpTag(from, to)
  }

  private def existing(tag: SymbolPath): Option[SymbolPath] =
    if (manager.tagExists(tag)) Some(tag) else None
  private def set(tag: SymbolPath, entity: MetaGraphEntity): Unit = manager.synchronized {
    if (entity == null) {
      existing(tag).foreach(manager.rmTag(_))
    } else {
      manager.setTag(tag, entity)
    }
  }
  private def set(tag: SymbolPath, content: String): Unit = manager.setTag(tag, content)
  private def get(tag: SymbolPath): String = manager.synchronized {
    existing(tag).map(manager.getTag(_)).getOrElse("")
  }
  private def ls(dir: SymbolPath) = manager.synchronized {
    existing(dir).map(manager.lsTag(_)).getOrElse(Nil)
  }

  def debugPrint() = manager.debugPrintTag(rootDir)

  // Makes assertions on a user-provided attribute/segmentation/scalar name.
  private def validateName(name: String): Unit = {
    assert(name.nonEmpty, "Name cannot be empty.")
    assert(!name.startsWith("!"), "Name cannot start with '!'.")
    assert(!name.contains(Project.separator), s"Name cannot contain '${Project.separator}'.")
    assert(!name.contains("/"), "Name cannot contain '/'.")
  }

  abstract class Holder[T <: MetaGraphEntity](dir: SymbolPath) extends Iterable[(String, T)] {
    def validate(name: String, entity: T): Unit

    def update(name: String, entity: T) = {
      validateName(name)
      set(name, entity)
    }

    // Skip name validation. Special-name entities can be set through this method.
    def set(name: String, entity: T) = manager.synchronized {
      if (entity == null) {
        existing(dir / name).foreach(manager.rmTag(_))
      } else {
        validate(name, entity)
        manager.setTag(dir / name, entity)
      }
    }
    def apply(name: String): T = {
      assert(manager.tagExists(dir / name), s"$name does not exist in $dir")
      manager.entity(dir / name).asInstanceOf[T]
    }

    def iterator = manager.synchronized {
      ls(dir)
        .flatMap { path =>
          val name = path.last.name
          Project.withErrorLogging(s"Couldn't resolve $path") { apply(name) }
            .map(name -> _)
        }
        .iterator
    }

    def contains(x: String) = iterator.exists(_._1 == x)
  }
  class ScalarHolder extends Holder[Scalar[_]](checkpointedDir / "scalars") {
    def validate(name: String, scalar: Scalar[_]) = {}
  }
  class VertexAttributeHolder extends Holder[Attribute[_]](checkpointedDir / "vertexAttributes") {
    def validate(name: String, attr: Attribute[_]) =
      assert(attr.vertexSet == vertexSet, s"Vertex attribute $name does not match vertex set for project $projectName")
  }
  class EdgeAttributeHolder extends Holder[Attribute[_]](checkpointedDir / "edgeAttributes") {
    def validate(name: String, attr: Attribute[_]) =
      assert(attr.vertexSet == edgeBundle.idSet, s"Edge attribute $name does not match edge bundle for project $projectName")
  }
}

object Project {
  val separator = "|"

  def apply(projectName: String)(implicit metaManager: MetaGraphManager): Project = new Project(projectName)

  def withErrorLogging[T](message: String)(op: => T): Option[T] = {
    try {
      Some(op)
    } catch {
      case e: Throwable => {
        log.error(message, e)
        None
      }
    }
  }

  // Makes assertions on a user-provided project name.
  def validateName(name: String): Unit = {
    assert(name.nonEmpty, "Project name cannot be empty.")
    assert(!name.startsWith("!"), "Project name cannot start with '!'.")
    assert(!name.contains(separator), s"Project name cannot contain '$separator'.")
    assert(!name.contains("/"), "Project name cannot contain '/'.")
  }
}

case class Segmentation(parentName: String, name: String)(implicit manager: MetaGraphManager) {
  def parent = Project(parentName)
  val path: SymbolPath = s"projects/$parentName/checkpointed/segmentations/$name"
  def project = Project(s"$parentName/checkpointed/segmentations/$name/project")

  def equivalentUIAttribute = {
    val bta = Option(belongsToAttribute).map(_.gUID.toString).getOrElse("")
    UIValue(id = bta, title = s"segmentation[$name]")
  }

  def toFE = {
    val bt = Option(belongsTo).map(UIValue.fromEntity(_)).getOrElse(null)
    FESegmentation(
      name,
      project.projectName,
      bt,
      equivalentUIAttribute)
  }
  def belongsTo = {
    Project.withErrorLogging(s"Cannot get 'belongsTo' for $this") {
      manager.edgeBundle(path / "belongsTo")
    }.getOrElse(null)
  }
  def belongsTo_=(eb: EdgeBundle) = manager.synchronized {
    assert(eb.dstVertexSet == project.vertexSet, s"Incorrect 'belongsTo' relationship for $name")
    manager.setTag(path / "belongsTo", eb)
    project.scalars.set("!coverage", graph_operations.Coverage.run(eb).srcCoverage)
    project.scalars.set("!nonEmpty", graph_operations.Coverage.run(eb).dstCoverage)
    project.scalars.set("!belongsToEdges", graph_operations.Count.run(eb))
  }
  def belongsToAttribute: Attribute[Vector[ID]] = {
    val segmentationIds = graph_operations.IdAsAttribute.run(project.vertexSet)
    val reversedBelongsTo = graph_operations.ReverseEdges.run(belongsTo)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    Project.withErrorLogging(s"Cannot get 'belongsToAttribute' for $this") {
      aop(aop.connection, reversedBelongsTo)(aop.attr, segmentationIds).result.attr: Attribute[Vector[ID]]
    }.getOrElse(null)
  }
  // In case the project is a segmentation
  // a Vector[ID] vertex attribute, that contains for each vertex
  // the vector of parent ids the segment contains.
  def membersAttribute: Attribute[Vector[ID]] = {
    val parentIds = graph_operations.IdAsAttribute.run(parent.vertexSet)
    val aop = graph_operations.AggregateByEdgeBundle(graph_operations.Aggregator.AsVector[ID]())
    Project.withErrorLogging(s"Cannot get 'membersAttribute' for $this") {
      aop(aop.connection, belongsTo)(aop.attr, parentIds).result.attr: Attribute[Vector[ID]]
    }.getOrElse(null)
  }

  def rename(newName: String) = manager.synchronized {
    val to = new SymbolPath(path.init) / newName
    manager.cpTag(path, to)
    manager.rmTag(path)
  }
  def remove(): Unit = manager.synchronized {
    manager.rmTag(path)
  }
}
