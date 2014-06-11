package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.UUID
import scala.collection.mutable

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

  def incommingBundles(vertexSet: VertexSet): Seq[EdgeBundle] =
    incommingBundlesMap.getOrElse(vertexSet.gUID, Seq())
  def outgoingBundles(vertexSet: VertexSet): Seq[EdgeBundle]
    outgoingBundlesMap.getOrElse(vertexSet.gUID, Seq())
  def attributes(vertexSet: VertexSet): Seq[VertexAttribute[_]]
    vertexAttribute.getOrElse(vertexSet.gUID, Seq())
  def attributes(edgeBundle: EdgeBundle): Seq[EdgeAttribute[_]]
    edgeAttribute.getOrElse(vertexSet.gUID, Seq())

  def dependentOperations(component: MetaGraphComponent): Seq[MetaGraphOperationInstance] =
    dependentOperationsMap.getOrElse(component.gUID, Seq())

  initializeFromDisk()

  private val entities = mutable.Map[UUID, MetaGraphEntity]()

  private val outgoingBundlesMap = mutable.Map[UUID, mutable.Buffer[EdgeBundle]]
  private val incommingBundlesMap = mutable.Map[UUID, mutable.Buffer[EdgeBundle]]
  private val vertexAttributes = mutable.Map[UUID, mutable.Buffer[VertexAttribute[_]]]
  private val edgeAttributes = mutable.Map[UUID, mutable.Buffer[EdgeAttribute[_]]]

  private val dependentOperationsMap =
    mutable.Map[UUID, mutable.Buffer[MetaGraphOperationInstance]]()

  def internalApply(operationInstance: MetaGraphOperationInstance): Unit = ???

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
    val operations = repo.list.filter(_.startsWith("save-")).sorted
    snapshots.foreach{ fileName =>
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

private class SerializedOperation(operation: MetaGraphOperation,
                                  inputs: Map[String, UUID]) extends Serializable = {
  def toInstance(manager: MetaGraphManager): MetaGraphOperationInstance = ???
}
object SerializedOperation {
  def apply(inst: MetaGraphOperationInstance): SerializedOperation = ???
}
