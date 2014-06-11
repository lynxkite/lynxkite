package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.UUID
import scala.collection.mutable

import com.lynxanalytics.biggraph.bigGraphLogger

class MetaGraphManagerImpl(val repositoryPath: String) extends MetaGraphManager {
  def apply(operationInstance: MetaGraphOperationInstance): Unit = {
    internalApply(operationInstance)
    saveInstanceToDisk(operationInstance)
  }

  def vertexSet(gUID: UUID): VertexSet = entities(gUID).asInstanceOf[VertexSet]
  def edgeBundle(gUID: UUID): EdgeBundle = entities(gUID).asInstanceOf[EdgeBundle]
  def vertexAttribute(gUID: UUID): VertexAttribute[_] =
    entities(gUID).asInstanceOf[VertexAttribute[_]]
  def edgeAttribute(gUID: UUID): EdgeAttribute[_] =
    entities(gUID).asInstanceOf[EdgeAttribute[_]]

  def incomingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    incomingBundlesMap.getOrElse(vertexSet.gUID, Seq())
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    outgoingBundlesMap.getOrElse(vertexSet.gUID, Seq())
  def attributes(vertexSet: VertexSet): Seq[VertexAttribute[_]] =
    vertexAttributesMap.getOrElse(vertexSet.gUID, Seq())
  def attributes(edgeBundle: EdgeBundle): Seq[EdgeAttribute[_]] =
    edgeAttributesMap.getOrElse(edgeBundle.gUID, Seq())

  def dependentOperations(component: MetaGraphEntity): Seq[MetaGraphOperationInstance] =
    dependentOperationsMap.getOrElse(component.gUID, Seq())

  initializeFromDisk()

  private val entities = mutable.Map[UUID, MetaGraphEntity]()

  private val outgoingBundlesMap = mutable.Map[UUID, mutable.Buffer[EdgeBundle]]()
  private val incomingBundlesMap = mutable.Map[UUID, mutable.Buffer[EdgeBundle]]()
  private val vertexAttributesMap = mutable.Map[UUID, mutable.Buffer[VertexAttribute[_]]]()
  private val edgeAttributesMap = mutable.Map[UUID, mutable.Buffer[EdgeAttribute[_]]]()

  private val dependentOperationsMap =
    mutable.Map[UUID, mutable.Buffer[MetaGraphOperationInstance]]()

  def internalApply(operationInstance: MetaGraphOperationInstance): Unit = {
    operationInstance.outputs.all.values.foreach { entity =>
      val gUID = entity.gUID
      assert(
        entities.contains(gUID),
        "Fatal conflict %s <=> %s".format(entity, entities(gUID)))
      entities(gUID) = entity
    }
    operationInstance.outputs.edgeBundles.values.foreach { eb =>
      outgoingBundlesMap.getOrElseUpdate(eb.srcVertexSet.gUID, mutable.Buffer()) += eb
      incomingBundlesMap.getOrElseUpdate(eb.dstVertexSet.gUID, mutable.Buffer()) += eb
    }
    operationInstance.outputs.vertexAttributes.values.foreach { va =>
      vertexAttributesMap.getOrElseUpdate(va.vertexSet.gUID, mutable.Buffer()) += va
    }
    operationInstance.outputs.edgeAttributes.values.foreach { ea =>
      edgeAttributesMap.getOrElseUpdate(ea.edgeBundle.gUID, mutable.Buffer()) += ea
    }
    operationInstance.inputs.all.values.foreach { entity =>
      dependentOperationsMap.getOrElseUpdate(entity.gUID, mutable.Buffer()) += operationInstance
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
          bigGraphLogger.error(
            "Error loading operation from file: %s\n Exception: %s\n Stack: %s",
            fileName,
            e,
            e.getStackTrace.toList)
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
    SerializedOperation(inst.operation,
      inst.inputs.all.mapValues(_.gUID))
  }
}
