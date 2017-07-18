// The MetaGraphManager allows querying and manipulating the metagraph and the tags.
// All tag access is synchronized on the MetaGraphManager.
// The MetaGraphManager is also responsible for the persistence of the metagraph.
package com.lynxanalytics.biggraph.graph_api

import org.apache.commons.io.FileUtils
import java.io.File
import java.util.UUID

import play.api.libs.json
import play.api.libs.json.Json

import scala.collection.immutable
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers.CheckpointRepository
import com.lynxanalytics.biggraph.controllers.DirectoryEntry
import com.lynxanalytics.biggraph.controllers.Workspace
import com.lynxanalytics.biggraph.graph_util.Timestamp

class MetaGraphManager(val repositoryPath: String) {
  val checkpointRepo = MetaGraphManager.getCheckpointRepo(repositoryPath)
  val repositoryRoot = new File(repositoryPath).getParent()

  def apply[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: (Symbol, MetaGraphEntity)*): TypedOperationInstance[IS, OMDS] = {

    apply(operation, MetaDataSet(inputs.toMap))
  }

  def apply[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    operation: TypedMetaGraphOp[IS, OMDS],
    inputs: MetaDataSet = MetaDataSet()): TypedOperationInstance[IS, OMDS] = synchronized {

    val operationInstance = TypedOperationInstance(this, operation, inputs)
    val gUID = operationInstance.gUID
    if (!operationInstances.contains(gUID)) {
      saveInstanceToDisk(operationInstance)
      internalApply(operationInstance)
    }
    operationInstances(gUID).asInstanceOf[TypedOperationInstance[IS, OMDS]]
  }

  // Applies an operation instance from its JSON form.
  def applyJson(j: json.JsValue): MetaGraphOperationInstance = {
    val inst = deserializeOperation(j)
    saveInstanceToDisk(inst)
    internalApply(inst)
    inst
  }

  def vertexSet(gUID: UUID): VertexSet = entities(gUID).asInstanceOf[VertexSet]
  def edgeBundle(gUID: UUID): EdgeBundle = entities(gUID).asInstanceOf[EdgeBundle]
  def attribute(gUID: UUID): Attribute[_] =
    entities(gUID).asInstanceOf[Attribute[_]]
  def attributeOf[T: TypeTag](gUID: UUID): Attribute[T] =
    attribute(gUID).runtimeSafeCast[T]
  def scalar(gUID: UUID): Scalar[_] =
    entities(gUID).asInstanceOf[Scalar[_]]
  def scalarOf[T: TypeTag](gUID: UUID): Scalar[T] =
    scalar(gUID).runtimeSafeCast[T]
  def table(gUID: UUID): Table =
    entities(gUID).asInstanceOf[Table]
  def entity(gUID: UUID): MetaGraphEntity = entities(gUID)

  def incomingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    incomingBundlesMap(vertexSet.gUID)
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    outgoingBundlesMap(vertexSet.gUID)
  def attributes(vertexSet: VertexSet): Seq[Attribute[_]] =
    attributesMap(vertexSet.gUID)
  def attributes(edgeBundle: EdgeBundle): Seq[Attribute[_]] =
    attributesMap(edgeBundle.idSet.gUID)

  def dependentOperations(entity: MetaGraphEntity): Seq[MetaGraphOperationInstance] =
    dependentOperationsMap.getOrElse(entity.gUID, Seq())

  def setTag(tag: SymbolPath, entity: MetaGraphEntity): Unit = synchronized {
    setTag(tag, entity.gUID.toString)
  }

  def setTag(tag: SymbolPath, content: String): Unit = synchronized {
    tagRoot.setTag(tag, content)
  }

  def setTags(tags: Map[SymbolPath, String]): Unit = synchronized {
    tagRoot.setTags(tags)
  }

  def getTag(tag: SymbolPath): String = synchronized {
    (tagRoot / tag).content
  }

  def rmTag(tag: SymbolPath): Unit = synchronized {
    (tagRoot / tag).rm
  }

  def cpTag(from: SymbolPath, to: SymbolPath): Unit = synchronized {
    tagRoot.cp(from, to)
  }

  def debugPrintTag(tag: SymbolPath): Unit = synchronized {
    println((tagRoot / tag).lsRec())
  }

  def tagExists(tag: SymbolPath): Boolean = synchronized {
    tagRoot.exists(tag)
  }

  def tagIsDir(tag: SymbolPath): Boolean = synchronized {
    (tagRoot / tag).isDir
  }

  def lsTag(tag: SymbolPath): Seq[SymbolPath] = synchronized {
    (tagRoot / tag).ls.map(_.fullName)
  }

  // Defers tag writes until after "fn". Improves performance for large tag transactions.
  def tagBatch[T](fn: => T) = synchronized {
    tagRoot.batch { fn }
  }

  private val operationInstances = mutable.Map[UUID, MetaGraphOperationInstance]()

  def getOperationInstances(): immutable.Map[UUID, MetaGraphOperationInstance] = {
    operationInstances.toMap
  }

  private val entities = mutable.Map[UUID, MetaGraphEntity]()

  // All tagRoot access must be synchronized on this MetaGraphManager object.
  // This allows users of MetaGraphManager to safely conduct transactions over
  // multiple tags.
  private val tagRoot = {
    log.info("Reading tags...")
    val res = TagRoot(repositoryPath)
    log.info("Tags read.")
    res
  }

  private val outgoingBundlesMap =
    mutable.Map[UUID, List[EdgeBundle]]().withDefaultValue(List())
  private val incomingBundlesMap =
    mutable.Map[UUID, List[EdgeBundle]]().withDefaultValue(List())
  private val attributesMap =
    mutable.Map[UUID, List[Attribute[_]]]().withDefaultValue(List())

  private val dependentOperationsMap =
    mutable.Map[UUID, List[MetaGraphOperationInstance]]().withDefaultValue(List())

  initializeFromDisk()

  private def internalApply(operationInstance: MetaGraphOperationInstance): Unit = {
    operationInstances(operationInstance.gUID) = operationInstance
    for (entity <- operationInstance.outputs.all.values) {
      val gUID = entity.gUID
      assert(
        !entities.contains(gUID),
        "Fatal conflict %s <=> %s".format(entity, entities(gUID)))
      entities(gUID) = entity
    }
    for (eb <- operationInstance.outputs.edgeBundles.values) {
      outgoingBundlesMap(eb.srcVertexSet.gUID) ::= eb
      incomingBundlesMap(eb.dstVertexSet.gUID) ::= eb
    }
    for (va <- operationInstance.outputs.attributes.values) {
      attributesMap(va.vertexSet.gUID) ::= va
    }
    for (entity <- operationInstance.inputs.all.values) {
      dependentOperationsMap(entity.gUID) ::= operationInstance
    }
  }

  private def saveInstanceToDisk(inst: MetaGraphOperationInstance): Unit = {
    log.info(s"Saving operation $inst to disk.")
    val j = serializeOperation(inst)
    // Validate the serialized operation by trying to reload it.
    val i = deserializeOperation(j)
    assert(inst == i,
      s"Operation reloaded after serialization was not identical: $inst vs $i\n\n$j")
    try {
      saveOperation(j)
    } catch {
      case e: Throwable => throw new Exception(s"Failed to write $inst.", e)
    }
  }

  private def saveOperation(j: json.JsValue): String = saveJson("operations", j)

  private def saveCheckpoint(j: json.JsValue): String = saveJson("checkpoints", j)

  private def saveJson(folder: String, j: json.JsValue): String = {
    val time = Timestamp.toString
    val repo = new File(repositoryPath, folder)
    val dumpFile = new File(repo, s"dump-$time")
    val finalFile = new File(repo, s"save-$time")
    FileUtils.writeStringToFile(dumpFile, Json.prettyPrint(j), "utf8")
    dumpFile.renameTo(finalFile)
    time
  }

  private def initializeFromDisk(): Unit = synchronized {
    log.info("Loading meta graph from disk...")
    for ((file, j) <- MetaGraphManager.loadOperations(repositoryPath)) {
      try {
        val inst = deserializeOperation(j)
        // Verify outputs.
        val expected = (j \ "outputs").as[Map[String, String]]
        val found = inst.outputs.asStringMap
        assert(
          expected == found,
          s"Output mismatch on $inst. JSON file says: $expected" +
            s" Recreated operation reports: $found")
        internalApply(inst)
      } catch {
        case e: Throwable => throw new Exception(s"Failed to load $file.", e)
      }
    }
    log.info("Meta graph loaded from disk.")
  }

  def serializeOperation(inst: MetaGraphOperationInstance): json.JsObject = {
    try {
      Json.obj(
        "operation" -> inst.operation.toTypedJson,
        "inputs" -> inst.inputs.toJson,
        "guid" -> inst.gUID,
        "outputs" -> inst.outputs.toJson)
    } catch {
      // Put details of "inst" in the exception.
      case e: Throwable => throw new Exception(s"Error while serializing $inst:", e)
    }
  }

  private def deserializeOperation(j: json.JsValue): MetaGraphOperationInstance = {
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
        op.inputSig.edgeBundles
          .map(n => n -> edgeBundle(inputs(n))).toMap,
        op.inputSig.attributes
          .map(n => n -> attribute(inputs(n))).toMap,
        op.inputSig.scalars
          .map(n => n -> scalar(inputs(n))).toMap,
        op.inputSig.tables
          .map(n => n -> table(inputs(n))).toMap))
  }
}
object MetaGraphManager {
  implicit class StringAsUUID(s: String) {
    def asUUID: UUID = UUID.fromString(s)
  }

  // Read operations as file -> JSON from a repo.
  def loadOperations(repo: String): Iterator[(File, json.JsValue)] = {
    val opdir = new File(repo, "operations")
    if (!opdir.exists) opdir.mkdirs
    val files = opdir.listFiles.filter(_.getName.startsWith("save-")).sortBy(_.getName)
    files.iterator.map { f =>
      f -> Json.parse(FileUtils.readFileToString(f, "utf8"))
    }
  }

  def getCheckpointRepo(repositoryPath: String): CheckpointRepository =
    new CheckpointRepository(repositoryPath + "/checkpoints")
}

object BuiltIns {
  def createBuiltIns(implicit manager: MetaGraphManager) = {
    val builtInsDir = DirectoryEntry.fromName("built-ins")
    if (!builtInsDir.exists) {
      builtInsDir.asNewDirectory()
      builtInsDir.readACL = "*"
      builtInsDir.writeACL = ""
    }
    log.info("Loading built-ins from disk...")
    val builtInsLocalDir = getBuiltInsLocalDirectory()
    import com.lynxanalytics.biggraph.controllers.WorkspaceJsonFormatters._
    for ((file, json) <- loadBuiltIns(builtInsLocalDir)) {
      try {
        val entry = DirectoryEntry.fromName("built-ins/" + file)
        // Overwrite existing built-ins with the ones from the disk.
        if (entry.exists) {
          entry.remove()
        }
        val frame = entry.asNewWorkspaceFrame()
        val cp = json.as[Workspace].checkpoint()
        frame.setCheckpoint(cp)
      } catch {
        case e: Throwable => throw new Exception(s"Failed to create built-in for file $file.", e)
      }
    }
    log.info("Built-ins loaded from disk.")
  }

  private def getBuiltInsLocalDirectory(): String = {
    val stageDir = scala.util.Properties.envOrNone("KITE_STAGE_DIR")
    // In the backend-tests we don't have the KITE_STAGE_DIR environment variable set.
    stageDir.getOrElse("stage")
  }

  private def loadBuiltIns(repo: String): Iterable[(String, json.JsValue)] = {
    val opdir = new File(repo, "built-ins")
    if (opdir.exists) {
      val files = opdir.listFiles.sortBy(_.getName)
      files.map { f =>
        try {
          f.getName() -> Json.parse(FileUtils.readFileToString(f, "utf8"))
        } catch {
          case e: Throwable => throw new Exception(s"Failed to load built-in file $f.", e)
        }
      }
    } else Iterable()
  }
}
