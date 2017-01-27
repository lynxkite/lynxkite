// Projects are the top-level entities on the UI.
//
// A project has a vertex set, an edge bundle, and any number of attributes,
// scalars and segmentations. It represents data stored in the tag system.
// The Project instances are short-lived, they are just a rich interface for
// querying and manipulating the tags.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Timestamp

import java.util.UUID
import play.api.libs.json.Json

class ObsoleteProject(val projectPath: SymbolPath)(implicit val tagRoot: TagRoot) {
  val projectName = projectPath.toString
  override def toString = projectName
  override def equals(p: Any) =
    p.isInstanceOf[ObsoleteProject] && projectName == p.asInstanceOf[ObsoleteProject].projectName
  override def hashCode = projectName.hashCode

  assert(projectName.nonEmpty, "Invalid project name: <empty string>")
  assert(!projectName.contains(ObsoleteProject.separator), s"Invalid project name: $projectName")
  val rootDir: SymbolPath = SymbolPath("projects") / projectPath
  // Part of the state that needs to be checkpointed.
  val checkpointedDir: SymbolPath = rootDir / "checkpointed"

  private def checkpoints: Seq[String] = get(rootDir / "checkpoints") match {
    case "" => Seq()
    case x => x.split(java.util.regex.Pattern.quote(ObsoleteProject.separator), -1)
  }
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

  def copyCheckpoint(i: Int, destination: ObsoleteProject): Unit = tagRoot.synchronized {
    assert(0 <= i && i < checkpointCount, s"Requested checkpoint $i out of $checkpointCount.")
    copy(destination)
    while (destination.checkpointCount > i + 1) {
      destination.undo
    }
  }

  def lastOperation = get(checkpointedDir / "lastOperation")

  def undo(): Unit = tagRoot.synchronized {
    assert(checkpointIndex > 0, s"Already at checkpoint $checkpointIndex.")
    checkpointIndex -= 1
    cp(checkpointDir(checkpointIndex), checkpointedDir)
  }

  def readACL: String = {
    if (isSegmentation) asSegmentation.parent.readACL
    else get(rootDir / "readACL")
  }
  def writeACL: String = {
    if (isSegmentation) asSegmentation.parent.writeACL
    else get(rootDir / "writeACL")
  }

  def isSegmentation = tagRoot.synchronized {
    val grandFather = rootDir.parent.parent
    grandFather.nonEmpty && (grandFather.name == 'segmentations)
  }
  def asSegmentation = tagRoot.synchronized {
    assert(isSegmentation, s"$projectName is not a segmentation")
    // If our parent is a top-level project, rootDir is like:
    //   project/parentName/checkpointed/segmentations/segmentationName/project
    val parentName = new SymbolPath(rootDir.drop(1).dropRight(4))
    val segmentationName = rootDir.dropRight(1).last.name
    ObsoleteSegmentation(parentName, segmentationName)
  }

  def notes = get(checkpointedDir / "notes")

  implicit val fFEOperationSpec = Json.format[FEOperationSpec]
  implicit val fProjectOperationRequest = Json.format[ProjectOperationRequest]
  def lastOperationRequest = tagRoot.synchronized {
    existing(checkpointedDir / "lastOperationRequest").map {
      tag => Json.parse(get(tag)).as[ProjectOperationRequest]
    }
  }
  def lastOperationRequest_=(spec: ProjectOperationRequest) = tagRoot.synchronized {
    val json = Json.prettyPrint(Json.toJson(spec))
    set(checkpointedDir / "lastOperationRequest", json)
  }

  def vertexSet = tagRoot.synchronized {
    existing(checkpointedDir / "vertexSet")
      .flatMap(vsPath =>
        ObsoleteProject.withErrorLogging(s"Couldn't resolve vertex set of project $this") {
          tagRoot.gUID(vsPath)
        })
      .getOrElse(null)
  }
  def edgeBundle = tagRoot.synchronized {
    existing(checkpointedDir / "edgeBundle")
      .flatMap(ebPath =>
        ObsoleteProject.withErrorLogging(s"Couldn't resolve edge bundle of project $this") {
          tagRoot.gUID(ebPath)
        })
      .getOrElse(null)
  }

  def scalars = new ScalarHolder
  def vertexAttributes = new VertexAttributeHolder
  def edgeAttributes = new EdgeAttributeHolder
  def segmentations = segmentationNames.map(segmentation(_))
  def segmentation(name: String) = ObsoleteSegmentation(projectPath, name)
  def segmentationNames = ls(checkpointedDir / "segmentations").map(_.last.name)

  def copy(to: ObsoleteProject): Unit = cp(rootDir, to.rootDir)

  private def cp(from: SymbolPath, to: SymbolPath) = tagRoot.synchronized {
    existing(to).foreach(tagRoot.rm(_))
    tagRoot.cp(from, to)
  }

  def remove(): Unit = tagRoot.synchronized {
    existing(rootDir).foreach(tagRoot.rm(_))
  }

  private def existing(tag: SymbolPath): Option[SymbolPath] =
    if (tagRoot.exists(tag)) Some(tag) else None
  private def set(tag: SymbolPath, content: String): Unit = tagRoot.setTag(tag, content)
  private def get(tag: SymbolPath): String = tagRoot.synchronized {
    existing(tag).map(x => (tagRoot / x).content).getOrElse("")
  }
  private def ls(dir: SymbolPath) = tagRoot.synchronized {
    existing(dir).map(x => (tagRoot / x).ls).getOrElse(Nil).map(_.fullName)
  }

  abstract class Holder(dir: SymbolPath) extends Iterable[(String, UUID)] {
    def apply(name: String): UUID = {
      assert(tagRoot.exists(dir / name), s"$name does not exist in $dir")
      tagRoot.gUID(dir / name)
    }

    def iterator = tagRoot.synchronized {
      ls(dir)
        .flatMap { path =>
          val name = path.last.name
          ObsoleteProject.withErrorLogging(s"Couldn't resolve $path") { apply(name) }
            .map(name -> _)
        }
        .iterator
    }

    def contains(x: String) = iterator.exists(_._1 == x)
  }
  class ScalarHolder extends Holder(checkpointedDir / "scalars")
  class VertexAttributeHolder extends Holder(checkpointedDir / "vertexAttributes")
  class EdgeAttributeHolder extends Holder(checkpointedDir / "edgeAttributes")
}

object ObsoleteProject {
  val separator = "|"

  def apply(projectPath: SymbolPath)(implicit tagRoot: TagRoot): ObsoleteProject =
    new ObsoleteProject(projectPath)

  def fromPath(stringPath: String)(implicit tagRoot: TagRoot): ObsoleteProject =
    new ObsoleteProject(SymbolPath.parse(stringPath))

  def fromName(name: String)(implicit tagRoot: TagRoot): ObsoleteProject =
    new ObsoleteProject(SymbolPath(name))

  def withErrorLogging[T](message: String)(op: => T): Option[T] = {
    try {
      Some(op)
    } catch {
      case e: Throwable =>
        log.error(message, e)
        None
    }
  }

  private def projects(tagRoot: TagRoot): Seq[ObsoleteProject] = {
    val dirs = {
      val projectsRoot = SymbolPath("projects")
      if (tagRoot.exists(projectsRoot))
        (tagRoot / projectsRoot).ls.map(_.fullName)
      else
        Nil
    }
    // Do not list internal project names (starting with "!").
    dirs
      .map(p => ObsoleteProject.fromName(p.path.last.name)(tagRoot))
      .filterNot(_.projectName.startsWith("!"))
  }

  private def getProjectState(project: ObsoleteProject): CommonProjectState = {
    CommonProjectState(
      vertexSetGUID = Option(project.vertexSet),
      vertexAttributeGUIDs = project.vertexAttributes.toMap,
      edgeBundleGUID = Option(project.edgeBundle),
      edgeAttributeGUIDs = project.edgeAttributes.toMap,
      scalarGUIDs = project.scalars.toMap,
      segmentations =
        project.segmentationNames.map(
          name => name -> getSegmentationState(project.segmentation(name))).toMap,
      notes = project.notes,
      elementNotes = Some(Map()),
      elementMetadata = Some(Map()))
  }

  private def getSegmentationState(seg: ObsoleteSegmentation): SegmentationState = {
    SegmentationState(
      state = getProjectState(seg.project),
      belongsToGUID = Option(seg.belongsTo))
  }

  private def getSegmentationPath(oldFullName: String): Seq[String] = {
    getSegmentationPath(SymbolPath.parse(oldFullName).toList)
  }

  private def getSegmentationPath(oldFullPath: List[scala.Symbol]): List[String] = {
    oldFullPath match {
      case List(name) => List()
      case rootName :: 'checkpointed :: 'segmentations :: segName :: rest =>
        segName.name :: getSegmentationPath(rest)
    }
  }

  private def getRootState(
    project: ObsoleteProject): RootProjectState = {

    RootProjectState(
      state = getProjectState(project),
      checkpoint = None,
      previousCheckpoint = None,
      lastOperationDesc = project.lastOperation,
      lastOperationRequest = Some(
        project.lastOperationRequest
          .map {
            projectOperationRequest =>
              SubProjectOperation(
                getSegmentationPath(projectOperationRequest.project),
                projectOperationRequest.op)
          }
          .getOrElse(SubProjectOperation(Seq(), FEOperationSpec("No-operation", Map())))),
      viewRecipe = None)

  }
  private def oldCheckpoints(p: ObsoleteProject): Seq[ObsoleteProject] = {
    val tmpDir = s"!tmp-$Timestamp"
    (0 until p.checkpointCount).flatMap { i =>
      val tmp = ObsoleteProject.fromName(s"$tmpDir-$i")(p.tagRoot)
      try {
        p.copyCheckpoint(i, tmp)
        Some(tmp)
      } catch {
        case t: Throwable =>
          log.error(s"Could not migrate checkpoint $i of project $p.", t)
          None
      }
    }
  }

  private def lastNewCheckpoint(
    oldCheckpoints: Seq[ObsoleteProject], repo: CheckpointRepository): String = {
    oldCheckpoints.headOption.foreach { firstCheckpoint =>
      if (firstCheckpoint.lastOperationRequest.isEmpty) {
        firstCheckpoint.lastOperationRequest =
          ProjectOperationRequest(
            project = "fake",
            op = FEOperationSpec("Change-project-notes", Map("notes" -> firstCheckpoint.notes)))
      }
    }
    oldCheckpoints.foldLeft("") {
      case (previousCheckpoint, project) =>
        val state = getRootState(project)
        repo.checkpointState(state, previousCheckpoint).checkpoint.get
    }
  }

  private def migrateOneProject(source: ObsoleteProject, targetManager: MetaGraphManager): Unit = {
    val oldCps = oldCheckpoints(source)
    val lastCp = lastNewCheckpoint(oldCps, targetManager.checkpointRepo)
    oldCps.foreach(_.remove())
    val frame = DirectoryEntry.fromName(source.projectName)(targetManager).asNewProjectFrame
    frame.setCheckpoint(lastCp)
    (0 until (source.checkpointCount - source.checkpointIndex - 1)).foreach {
      i => frame.undo()
    }
    frame.readACL = source.readACL
    frame.writeACL = source.writeACL
  }

  def migrateV1ToV2(
    v1TagRoot: TagRoot, targetManager: MetaGraphManager): Unit = targetManager.tagBatch {
    v1TagRoot.writesCanBeIgnored {
      projects(v1TagRoot).foreach { project =>
        migrateOneProject(project, targetManager)
      }
    }
  }
}

case class ObsoleteSegmentation(parentPath: SymbolPath, name: String)(
    implicit tagRoot: TagRoot) {
  def parent = ObsoleteProject(parentPath)
  val parentName = parent.projectName
  val path = SymbolPath("projects") / parentPath / "checkpointed" / "segmentations" / name
  def project = ObsoleteProject(parentPath / "checkpointed" / "segmentations" / name / "project")

  def belongsTo = {
    ObsoleteProject.withErrorLogging(s"Cannot get 'belongsTo' for $this") {
      tagRoot.gUID(path / "belongsTo")
    }.getOrElse(null)
  }
}

