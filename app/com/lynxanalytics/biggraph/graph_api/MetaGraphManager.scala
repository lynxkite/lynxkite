package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.UUID
import scala.collection.mutable

import com.lynxanalytics.biggraph.bigGraphLogger

class MetaGraphManager(val repositoryPath: String) {
  def apply(operation: MetaGraphOperation, inputs: MetaDataSet): MetaGraphOperationInstance = {
    val operationInstance = MetaGraphOperationInstance(operation, inputs)
    val gUID = operationInstance.gUID
    if (!operationInstances.contains(gUID)) {
      internalApply(operationInstance)
      saveInstanceToDisk(operationInstance)
    }
    operationInstances(gUID)
  }

  def vertexSet(gUID: UUID): VertexSet = entities(gUID).asInstanceOf[VertexSet]
  def edgeBundle(gUID: UUID): EdgeBundle = entities(gUID).asInstanceOf[EdgeBundle]
  def vertexAttribute(gUID: UUID): VertexAttribute[_] =
    entities(gUID).asInstanceOf[VertexAttribute[_]]
  def edgeAttribute(gUID: UUID): EdgeAttribute[_] =
    entities(gUID).asInstanceOf[EdgeAttribute[_]]
  def entity(gUID: UUID): MetaGraphEntity = entities(gUID)

  def incomingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    incomingBundlesMap(vertexSet.gUID)
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    outgoingBundlesMap(vertexSet.gUID)
  def attributes(vertexSet: VertexSet): Seq[VertexAttribute[_]] =
    vertexAttributesMap(vertexSet.gUID)
  def attributes(edgeBundle: EdgeBundle): Seq[EdgeAttribute[_]] =
    edgeAttributesMap(edgeBundle.gUID)

  def dependentOperations(entity: MetaGraphEntity): Seq[MetaGraphOperationInstance] =
    dependentOperationsMap.getOrElse(entity.gUID, Seq())

  private val operationInstances = mutable.Map[UUID, MetaGraphOperationInstance]()

  private val entities = mutable.Map[UUID, MetaGraphEntity]()

  private val outgoingBundlesMap =
    mutable.Map[UUID, List[EdgeBundle]]().withDefaultValue(List())
  private val incomingBundlesMap =
    mutable.Map[UUID, List[EdgeBundle]]().withDefaultValue(List())
  private val vertexAttributesMap =
    mutable.Map[UUID, List[VertexAttribute[_]]]().withDefaultValue(List())
  private val edgeAttributesMap =
    mutable.Map[UUID, List[EdgeAttribute[_]]]().withDefaultValue(List())

  private val dependentOperationsMap =
    mutable.Map[UUID, List[MetaGraphOperationInstance]]().withDefaultValue(List())

  initializeFromDisk()

  def internalApply(operationInstance: MetaGraphOperationInstance): Unit = {
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
    operationInstance.outputs.edgeAttributes.values.foreach { ea =>
      edgeAttributesMap(ea.edgeBundle.gUID) ::= ea
    }
    operationInstance.inputs.all.values.foreach { entity =>
      dependentOperationsMap(entity.gUID) ::= operationInstance
    }
  }

  private def saveInstanceToDisk(inst: MetaGraphOperationInstance): Unit = {
    val time = scala.compat.Platform.currentTime
    val dumpFile = new File("%s/dump-%13d".format(repositoryPath, time))
    val finalFile = new File("%s/save-%13d".format(repositoryPath, time))
    val stream = new ObjectOutputStream(new FileOutputStream(dumpFile))
    stream.writeObject(SerializedOperation(inst))
    stream.close()
    dumpFile.renameTo(finalFile)
  }

  private def initializeFromDisk(): Unit = {
    val repo = new File(repositoryPath)
    val operationFileNames = repo.list.filter(_.startsWith("save-")).sorted
    operationFileNames.foreach { fileName =>
      try {
        val file = new File(repo, fileName)
        val stream = new ObjectInputStream(new FileInputStream(file))
        val instance = stream.readObject().asInstanceOf[SerializedOperation].toInstance(this)
        internalApply(instance)
      } catch {
        // TODO(xandrew): Be more selective here...
        case e: Exception =>
          bigGraphLogger.error(s"Error loading operation from file: $fileName", e)
      }
    }
  }
}

private case class SerializedOperation(operation: MetaGraphOperation,
                                       inputs: Map[Symbol, UUID]) extends Serializable {
  def toInstance(manager: MetaGraphManager): MetaGraphOperationInstance = {
    MetaGraphOperationInstance(
      operation,
      MetaDataSet(
        operation.inputVertexSets.map(n => n -> manager.vertexSet(inputs(n))).toMap,
        operation.inputEdgeBundles.keys.map(n => n -> manager.edgeBundle(inputs(n))).toMap,
        operation.inputVertexAttributes.keys
          .map(n => n -> manager.vertexAttribute(inputs(n))).toMap,
        operation.inputEdgeAttributes.keys
          .map(n => n -> manager.edgeAttribute(inputs(n))).toMap))
  }
}
private object SerializedOperation {
  def apply(inst: MetaGraphOperationInstance): SerializedOperation = {
    SerializedOperation(
      inst.operation,
      inst.inputs.all.map { case (name, entity) => name -> entity.gUID })
  }
}
