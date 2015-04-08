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

class MetaGraphManager(val repositoryPath: String) {
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
    show(Seq(entity))
  }

  def setTag(tag: SymbolPath, content: String): Unit = synchronized {
    tagRoot.setTag(tag, content)
    saveTags()
  }

  def setTags(tags: Map[SymbolPath, String]): Unit = synchronized {
    for ((tag, content) <- tags) {
      tagRoot.setTag(tag, content)
    }
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

  // At the moment tag transactions are an optimization. The changes will not be rolled back
  // on failure. It's just a way of batching writes to improve performance.
  private var tagTransactionDepth = 0
  def tagTransaction[T](fn: => T) = synchronized {
    tagTransactionDepth += 1
    try {
      fn
    } finally {
      tagTransactionDepth -= 1
      saveTags()
    }
  }

  def vertexSet(tag: SymbolPath): VertexSet = synchronized {
    vertexSet((tagRoot / tag).gUID)
  }
  def edgeBundle(tag: SymbolPath): EdgeBundle = synchronized {
    edgeBundle((tagRoot / tag).gUID)
  }
  def attribute(tag: SymbolPath): Attribute[_] = synchronized {
    attribute((tagRoot / tag).gUID)
  }
  def scalar(tag: SymbolPath): Scalar[_] = synchronized {
    scalar((tagRoot / tag).gUID)
  }
  def attributeOf[T: TypeTag](tag: SymbolPath): Attribute[T] = synchronized {
    attributeOf[T]((tagRoot / tag).gUID)
  }
  def scalarOf[T: TypeTag](tag: SymbolPath): Scalar[T] = synchronized {
    scalarOf[T]((tagRoot / tag).gUID)
  }
  def entity(tag: SymbolPath): MetaGraphEntity = synchronized {
    entity((tagRoot / tag).gUID)
  }

  private val operationInstances = mutable.Map[UUID, MetaGraphOperationInstance]()

  private val entities = mutable.Map[UUID, MetaGraphEntity]()
  private val visibles = mutable.Set[UUID]()

  // All tagRoot access must be synchronized on this MetaGraphManager object.
  // This allows users of MetaGraphManager to safely conduct transactions over
  // multiple tags.
  private val tagRoot = TagRoot()

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
    log.info(s"Saving $inst to disk.")
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

  private def saveOperation(j: json.JsValue): Unit = {
    val time = Timestamp.toString
    val repo = new File(repositoryPath, "operations")
    val dumpFile = new File(repo, s"dump-$time")
    val finalFile = new File(repo, s"save-$time")
    FileUtils.writeStringToFile(dumpFile, Json.prettyPrint(j), "utf8")
    dumpFile.renameTo(finalFile)
  }

  private def saveVisibles(): Unit = {
    val dumpFile = new File(repositoryPath, "dump-visibles")
    val j = Json.toJson(visibles.map(_.toString))
    FileUtils.writeStringToFile(dumpFile, Json.prettyPrint(j), "utf8")
    dumpFile.renameTo(new File(repositoryPath, "visibles"))
  }

  private def saveTags(): Unit = synchronized {
    // Writes are deferred during transactions.
    if (tagTransactionDepth == 0) {
      val dumpFile = new File(repositoryPath, "dump-tags")
      val j = json.JsObject(tagRoot.allTags.map {
        tag => tag.fullName.toString -> json.JsString(tag.content)
      }.toSeq)
      FileUtils.writeStringToFile(dumpFile, Json.prettyPrint(j), "utf8")
      dumpFile.renameTo(new File(repositoryPath, "tags"))
    }
  }

  private def initializeFromDisk(): Unit = synchronized {
    for ((file, j) <- MetaGraphManager.loadOperations(repositoryPath)) {
      try {
        val inst = deserializeOperation(j)
        // Verify outputs.
        val expected = (j \ "outputs").as[Map[String, String]]
        val found = inst.outputs.toJson.as[Map[String, String]]
        for ((k, v) <- expected) {
          assert(v == expected.getOrElse(k, v),
            s"Output mismatch on $k in $inst." +
              s" Expected: $v, found: ${expected(k)}")
        }
        internalApply(inst)
      } catch {
        case e: Throwable => throw new Exception(s"Failed to load $file.", e)
      }
    }

    visibles.clear
    visibles ++= MetaGraphManager.loadVisibles(repositoryPath)

    setTags(MetaGraphManager.loadTags(repositoryPath))
  }

  def serializeOperation(inst: MetaGraphOperationInstance): json.JsObject = {
    try {
      Json.obj(
        "operation" -> inst.operation.toTypedJson,
        "inputs" -> inst.inputs.toJson,
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
          .map(n => n -> scalar(inputs(n))).toMap))
  }
}
object MetaGraphManager {
  implicit class StringAsUUID(s: String) {
    def asUUID: UUID = UUID.fromString(s)
  }

  // Read operations as file -> JSON from a repo.
  def loadOperations(repo: String): Seq[(File, json.JsValue)] = {
    val opdir = new File(repo, "operations")
    if (!opdir.exists) opdir.mkdirs
    val files = opdir.listFiles.filter(_.getName.startsWith("save-")).sortBy(_.getName)
    files.map { f =>
      f -> Json.parse(FileUtils.readFileToString(f, "utf8"))
    }
  }

  def loadVisibles(repo: String): Set[UUID] = {
    val visiblesFile = new File(repo, "visibles")
    if (visiblesFile.exists) {
      val j = Json.parse(FileUtils.readFileToString(visiblesFile, "utf8"))
      j.as[Seq[String]].map(UUID.fromString(_)).toSet
    } else Set()
  }

  def loadTags(repo: String): Map[SymbolPath, String] = {
    val tagsFile = new File(repo, "tags")
    if (tagsFile.exists) {
      val j = Json.parse(FileUtils.readFileToString(tagsFile, "utf8"))
      j.as[Map[String, String]].map { case (k, v) => SymbolPath.fromString(k) -> v }
    } else {
      Map()
    }
  }
}
