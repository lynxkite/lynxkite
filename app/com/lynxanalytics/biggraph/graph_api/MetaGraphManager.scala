package com.lynxanalytics.biggraph.graph_api

import org.apache.commons.io.FileUtils
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.UUID
import play.api.libs.json
import play.api.libs.json.Json
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_util.{ Filename, Timestamp }

object MetaGraphManager {
  implicit class StringAsUUID(s: String) {
    def asUUID: UUID = UUID.fromString(s)
  }
}
class MetaGraphManager {
  def apply[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: (Symbol, MetaGraphEntity)*): TypedOperationInstance[IS, OMDS] = {

    apply(operation, MetaDataSet.applyWithSignature(operation.inputSig, inputs: _*))
  }

  def apply[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: MetaDataSet = MetaDataSet(),
    transient: Boolean = false): TypedOperationInstance[IS, OMDS] = synchronized {

    val operationInstance = TypedOperationInstance(this, operation, inputs)
    val gUID = operationInstance.gUID
    if (!operationInstances.contains(gUID)) {
      if (!transient) {
        saveInstanceToDisk(operationInstance)
      }
      internalApply(operationInstance)
    }
    operationInstances(gUID).asInstanceOf[TypedOperationInstance[IS, OMDS]]
  }

  // Override these to store the data.
  protected def saveInstanceToDisk(inst: MetaGraphOperationInstance): Unit = {}
  protected def saveTags(): Unit = {}
  protected def saveVisibles(): Unit = {}

  // Marks a set of entities for frontend visibility.
  def show[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: (Symbol, MetaGraphEntity)*): TypedOperationInstance[IS, OMDS] = {

    val inst = apply(operation, inputs: _*)
    show(inst.outputs)
    return inst
  }
  def show(mds: MetaDataSet): Unit = show(mds.all.values.toSeq)
  def show(entities: Seq[MetaGraphEntity]): Unit = {
    visibles ++= entities.map(_.gUID)
    saveVisibles()
  }
  def isVisible(entity: MetaGraphEntity): Boolean = visibles.contains(entity.gUID)

  def allVertexSets: Set[VertexSet] = entities.values.collect { case e: VertexSet => e }.toSet

  def vertexSet(gUID: UUID): VertexSet = entity(gUID).asInstanceOf[VertexSet]
  def edgeBundle(gUID: UUID): EdgeBundle = entity(gUID).asInstanceOf[EdgeBundle]
  def vertexAttribute(gUID: UUID): Attribute[_] =
    entity(gUID).asInstanceOf[Attribute[_]]
  def vertexAttributeOf[T: TypeTag](gUID: UUID): Attribute[T] =
    vertexAttribute(gUID).runtimeSafeCast[T]
  def scalar(gUID: UUID): Scalar[_] =
    entity(gUID).asInstanceOf[Scalar[_]]
  def scalarOf[T: TypeTag](gUID: UUID): Scalar[T] =
    scalar(gUID).runtimeSafeCast[T]
  def entity(gUID: UUID): MetaGraphEntity = entities(gUID)

  def incomingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    incomingBundlesMap(vertexSet.gUID)
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    outgoingBundlesMap(vertexSet.gUID)
  def attributes(vertexSet: VertexSet): Seq[Attribute[_]] =
    vertexAttributesMap(vertexSet.gUID)
  def attributes(edgeBundle: EdgeBundle): Seq[Attribute[_]] =
    vertexAttributesMap(edgeBundle.asVertexSet.gUID)

  def dependentOperations(entity: MetaGraphEntity): Seq[MetaGraphOperationInstance] =
    dependentOperationsMap.getOrElse(entity.gUID, Seq())

  def setTag(tag: SymbolPath, entity: MetaGraphEntity): Unit = synchronized {
    setTag(tag, entity.gUID.toString)
    show(Seq(entity))
  }
  def setTag(tag: SymbolPath, content: String): Unit = synchronized {
    tagRoot.setTag(tag, content)
    saveTags()
  }
  def getTag(tag: SymbolPath): String = synchronized {
    (tagRoot / tag).content
  }

  def rmTag(tag: SymbolPath): Unit = synchronized {
    (tagRoot / tag).rm
    saveTags()
  }

  def cpTag(from: SymbolPath, to: SymbolPath): Unit = synchronized {
    tagRoot.cp(from, to)
    saveTags()
  }

  def debugPrintTag(tag: SymbolPath): Unit = synchronized {
    println((tagRoot / tag).lsRec())
  }

  def tagExists(tag: SymbolPath): Boolean = synchronized {
    tagRoot.exists(tag)
  }

  def lsTag(tag: SymbolPath): Seq[SymbolPath] = synchronized {
    (tagRoot / tag).ls.map(_.fullName)
  }

  def vertexSet(tag: SymbolPath): VertexSet = synchronized {
    vertexSet((tagRoot / tag).gUID)
  }
  def edgeBundle(tag: SymbolPath): EdgeBundle = synchronized {
    edgeBundle((tagRoot / tag).gUID)
  }
  def vertexAttribute(tag: SymbolPath): Attribute[_] = synchronized {
    vertexAttribute((tagRoot / tag).gUID)
  }
  def scalar(tag: SymbolPath): Scalar[_] = synchronized {
    scalar((tagRoot / tag).gUID)
  }
  def vertexAttributeOf[T: TypeTag](tag: SymbolPath): Attribute[T] = synchronized {
    vertexAttributeOf[T]((tagRoot / tag).gUID)
  }
  def scalarOf[T: TypeTag](tag: SymbolPath): Scalar[T] = synchronized {
    scalarOf[T]((tagRoot / tag).gUID)
  }
  def entity(tag: SymbolPath): MetaGraphEntity = synchronized {
    entity((tagRoot / tag).gUID)
  }

  private val operationInstances = mutable.Map[UUID, MetaGraphOperationInstance]()

  protected val entities = mutable.Map[UUID, MetaGraphEntity]()
  protected val visibles = mutable.Set[UUID]()

  // All tagRoot access must be synchronized on this MetaGraphManager object.
  // This allows users of MetaGraphManager to safely conduct transactions over
  // multiple tags.
  protected val tagRoot = TagRoot()

  private val outgoingBundlesMap =
    mutable.Map[UUID, List[EdgeBundle]]().withDefaultValue(List())
  private val incomingBundlesMap =
    mutable.Map[UUID, List[EdgeBundle]]().withDefaultValue(List())
  private val vertexAttributesMap =
    mutable.Map[UUID, List[Attribute[_]]]().withDefaultValue(List())

  private val dependentOperationsMap =
    mutable.Map[UUID, List[MetaGraphOperationInstance]]().withDefaultValue(List())

  protected def internalApply(operationInstance: MetaGraphOperationInstance): Unit = {
    operationInstances(operationInstance.gUID) = operationInstance
    operationInstance.outputs.all.values.foreach { entity =>
      val gUID = entity.gUID
      assert(
        !entities.contains(gUID),
        "Fatal conflict %s <=> %s".format(entity, entities(gUID)))
      entities(gUID) = entity
    }
    operationInstance.outputs.edgeBundles.values.foreach { eb =>
      outgoingBundlesMap(eb.srcVertexSet.gUID) ::= eb
      incomingBundlesMap(eb.dstVertexSet.gUID) ::= eb
    }
    operationInstance.outputs.vertexAttributes.values.foreach { va =>
      vertexAttributesMap(va.vertexSet.gUID) ::= va
    }
    operationInstance.inputs.all.values.foreach { entity =>
      dependentOperationsMap(entity.gUID) ::= operationInstance
    }
  }
}

private class PersistentMetaGraphManager(val repositoryPath: String) extends MetaGraphManager {

  override def saveInstanceToDisk(inst: MetaGraphOperationInstance) =
    saveInstanceToDisk(inst, repositoryPath)

  protected def saveInstanceToDisk(inst: MetaGraphOperationInstance, repo: String): Unit = {
    log.info(s"Saving $inst to disk.")
    val time = Timestamp.toString
    val dir = Filename(repo) / "operations"
    val dumpFile = dir / s"dump-$time"
    val finalFile = dir / s"save-$time"
    dumpFile.createFromStrings(serializeOperation(inst))
    // Validate the saved operation by trying to reload it.
    val i = loadInstanceFromDisk(new File(dumpFile.toString))
    assert(inst == i, s"Operation reloaded after serialization was not identical: $inst")
    dumpFile.renameTo(finalFile)
  }

  protected def loadInstanceFromDisk(file: File): MetaGraphOperationInstance = {
    log.info(s"Loading operation from ${file.getName}")
    try {
      val data = scala.io.Source.fromFile(file).mkString
      deserializeOperation(data)
    } catch {
      case e: Throwable => throw new Exception(s"Error loading operation from ${file.getName}", e)
    }
  }

  override def saveVisibles(): Unit = saveVisibles(repositoryPath)

  protected def saveVisibles(repo: String): Unit = {
    val dumpFile = new File(s"$repo/dump-visibles")
    val stream = new ObjectOutputStream(new FileOutputStream(dumpFile))
    stream.writeObject(visibles)
    stream.close()
    dumpFile.renameTo(new File(s"$repo/visibles"))
  }

  override def saveTags(): Unit = saveTags(repositoryPath)

  protected def saveTags(repo: String): Unit = synchronized {
    val dumpFile = new File(s"$repo/dump-tags")
    FileUtils.writeStringToFile(dumpFile, tagRoot.saveToString, "utf8")
    dumpFile.renameTo(new File(s"$repo/tags"))
  }

  protected def loadAndApplyInstanceFromDisk(file: File): Unit = {
    val instance = loadInstanceFromDisk(file)
    internalApply(instance)
  }

  protected def initializeFromDisk(): Unit = {
    val repo = new File(repositoryPath)
    if (!repo.exists) repo.mkdirs
    val oprepo = new File(repo, "operations")
    if (!oprepo.exists) oprepo.mkdirs
    val operationFiles = oprepo.listFiles.filter(_.getName.startsWith("save-")).sortBy(_.getName)
    for (file <- operationFiles) {
      loadAndApplyInstanceFromDisk(file)
    }
    visibles.clear
    val visiblesFile = new File(repo, "visibles")
    if (visiblesFile.exists) {
      log.info(s"Loading visible set.")
      try {
        val stream = new ObjectInputStream(new FileInputStream(visiblesFile))
        visibles ++= stream.readObject().asInstanceOf[mutable.Set[UUID]]
        stream.close()
      } catch {
        case e: Throwable => log.error("Error loading visible set:", e)
      }
    }
    val tagsFile = new File(repo, "tags")
    if (tagsFile.exists) {
      log.info(s"Loading tags.")
      try {
        synchronized {
          tagRoot.loadFromString(FileUtils.readFileToString(tagsFile, "utf8"))
        }
      } catch {
        case e: Throwable => log.error("Error loading tags set:", e)
      }
    } else {
      synchronized {
        tagRoot.clear()
      }
    }
  }

  protected def serializeOperation(inst: MetaGraphOperationInstance): String = {
    val j = Json.obj(
      "operation" -> inst.operation.toTypedJson,
      "inputs" -> inst.inputs.toJson,
      "outputs" -> inst.outputs.toJson)
    try {
      Json.prettyPrint(j)
    } catch {
      // Put details of "inst" in the exception.
      case e: Throwable => throw new Exception(s"Error while serializing $inst:", e)
    }
  }

  protected def deserializeOperation(input: String): MetaGraphOperationInstance = {
    val j = Json.parse(input)
    deserializeOperation(j)
  }

  protected def deserializeOperation(j: json.JsValue): MetaGraphOperationInstance = {
    val op = TypedJson.read[TypedMetaGraphOp.Type](j \ "operation")
    val inputs = (j \ "inputs").as[Map[String, String]].map {
      case (name, guid) => Symbol(name) -> UUID.fromString(guid)
    }
    TypedOperationInstance(
      this,
      op,
      MetaDataSet(
        op.inputSig.vertexSets
          .map(n => n -> vertexSet(inputs(n))).toMap,
        op.inputSig.edgeBundles.keys
          .map(n => n -> edgeBundle(inputs(n))).toMap,
        op.inputSig.vertexAttributes.keys
          .map(n => n -> vertexAttribute(inputs(n))).toMap,
        op.inputSig.scalars
          .map(n => n -> scalar(inputs(n))).toMap))
  }
}

// Used for loading repository data from the current version.
private class ValidatingMetaGraphManager(repo: String) extends PersistentMetaGraphManager(repo) {
  initializeFromDisk()

  override def deserializeOperation(j: json.JsValue): MetaGraphOperationInstance = {
    val inst = super.deserializeOperation(j)
    // Verify outputs.
    val outputs = j \ "outputs"
    assert(outputs == inst.outputs.toJson,
      s"Output mismatch in operation read from $j." +
        s" Expected: $outputs, found: ${inst.outputs.toJson}")
    inst
  }
}

// Used for loading reposity data from an old version and writing it out as the new version.
private class MigrationalMetaGraphManager(
    src: String, // Directory to read from.
    dst: String, // Directory to write to.
    srcVersion: JsonMigration.VersionMap, // Source version map.
    migration: JsonMigration // JsonMigration for the current version.
    ) extends PersistentMetaGraphManager(src) {
  // A mapping for entity GUIDs (from old to new) that have changed in the new version.
  val guidMapping = collection.mutable.Map[UUID, UUID]()
  initializeFromDisk()

  override def entity(gUID: UUID): MetaGraphEntity =
    // Look up missing entities by their new GUIDs.
    entities.getOrElse(gUID, entity(guidMapping(gUID)))

  override def loadAndApplyInstanceFromDisk(file: File) = {
    // Save operations as they are loaded.
    val inst = loadInstanceFromDisk(file)
    saveInstanceToDisk(inst, dst)
    super.internalApply(inst)
  }

  override def initializeFromDisk(): Unit = {
    // The superclass call loads everything and writes out the updated operations.
    super.initializeFromDisk()
    // Update GUIDs for "visibles".
    visibles.map(v => guidMapping.getOrElse(v, v))
    saveVisibles(dst)
    // Update GUIDs in tags.
    val mapping = guidMapping.map { case (k, v) => k.toString -> v.toString }
    for (t <- tagRoot.allTags) {
      for (newGuid <- mapping.get(t.content)) {
        val p = t.parent
        t.rm
        p.addTag(t.name, newGuid)
      }
    }
    saveTags(dst)
  }

  // Replaces a field in a JsObject.
  private def replaceJson(j: json.JsObject, Field: String, newValue: json.JsValue): json.JsObject = {
    new json.JsObject(j.fields.map {
      case (Field, _) => Field -> newValue
      case x => x
    })
  }

  override def deserializeOperation(j: json.JsValue): MetaGraphOperationInstance = {
    // Call upgraders.
    val op = (j \ "operation").as[json.JsObject]
    val cls = (op \ "class").as[String]
    val v1 = srcVersion(cls)
    val v2 = migration.version(cls)
    val newData = (v1 until v2).foldLeft((op \ "data").as[json.JsObject]) {
      (j, v) => migration.upgraders(cls -> v)(j)
    }
    val newOp = replaceJson(op, "data", newData)
    val newJson = replaceJson(j.as[json.JsObject], "operation", newOp)
    // Deserialize the upgraded JSON.
    val inst = super.deserializeOperation(newJson)
    // Add outputs to the GUID mapping.
    for ((name, guid) <- (j \ "outputs").as[Map[String, String]]) {
      guidMapping(UUID.fromString(guid)) = inst.outputs.all(Symbol(name)).gUID
    }
    inst
  }
}

object VersioningMetaGraphManager {
  // Load repository as current version.
  def apply(rootPath: String): MetaGraphManager =
    apply(rootPath, JsonMigration.current)

  // Load repository as a custom version. This is for testing only.
  def apply(rootPath: String, mig: JsonMigration): MetaGraphManager = {
    val current = findCurrentRepository(new File(rootPath), mig).toString
    new ValidatingMetaGraphManager(current)
  }

  // Returns the path to the repo belonging to the current version.
  // If the newest repo belongs to an older version, it performs migration.
  // If the newest repo belongs to a newer version, an exception is raised.
  private def findCurrentRepository(repo: File, current: JsonMigration): File = {
    val dirs = repo.listFiles
    import JsonMigration.versionOrdering
    import JsonMigration.versionOrdering.mkOrderingOps
    case class DV(dir: File, version: JsonMigration.VersionMap)
    val versions =
      dirs
        .flatMap(dir => readVersion(dir).map(v => DV(dir, v)))
        .sortBy(_.version).reverse
    if (versions.isEmpty) {
      val currentDir = new File(repo, "1")
      writeVersion(currentDir, current.version)
      currentDir
    } else {
      val newest = versions.head
      if (newest.version == current.version) newest.dir
      else {
        val supported = versions.find(_.version <= current.version)
        assert(newest.version < current.version,
          supported match {
            case Some(supported) =>
              s"The repository data in ${newest.dir} is newer than the current version." +
                s" The first supported version is in ${supported.dir}."
            case None =>
              s"All repository data in $repo has a newer version than the current version."
          })
        val last = newest.dir.getName.toInt
        val currentDir = new File(repo, (last + 1).toString)
        log.warn(s"Migrating from ${newest.dir} to $currentDir.")
        new MigrationalMetaGraphManager(newest.dir.toString, currentDir.toString, newest.version, current)
        writeVersion(currentDir, current.version)
        currentDir
      }
    }
  }

  def readVersion(dir: File) = {
    val versionFile = new File(dir, "version")
    if (versionFile.exists) {
      val data = scala.io.Source.fromFile(versionFile).mkString
      val j = Json.parse(data)
      val versions = j.as[JsonMigration.VersionMap].withDefaultValue(0)
      Some(versions)
    } else None
  }

  def writeVersion(dir: File, version: JsonMigration.VersionMap): Unit = {
    dir.mkdirs
    val versionFile = new File(dir, "version")
    val data = json.JsObject(version.mapValues(json.JsNumber(_)).toSeq)
    val fn = Filename(versionFile.toString)
    fn.createFromStrings(json.Json.prettyPrint(data))
  }
}
