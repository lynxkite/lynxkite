package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.UUID
import scala.collection.mutable

class BigGraphManagerImpl(repositoryPath: String) extends BigGraphManager {
  private val bigGraphs = mutable.Map[UUID, BigGraph]()
  private val derivatives = mutable.Map[UUID, mutable.Buffer[BigGraph]]()

  loadFromDisk()

  def deriveGraph(sources: Seq[BigGraph],
                  operation: GraphOperation): BigGraph = {
    val newGraph = new BigGraph(sources, operation)
    val gUID = newGraph.gUID
    if (!bigGraphs.contains(gUID)) {
      bigGraphs(gUID) = newGraph
      updateDeriatives(newGraph)
      saveToDisk
    }
    return bigGraphs(gUID)
  }

  // Returns the BigGraph corresponding to a given GUID.
  def graphForGUID(gUID: UUID): Option[BigGraph] = {
    bigGraphs.get(gUID)
  }

  // Returns all graphs in the meta graph known to this manager that has the given
  // graph as one of its sources.
  def knownDirectDerivatives(graph: BigGraph): Seq[BigGraph] = {
    derivatives(graph.gUID)
  }


  private def updateDeriatives(graph: BigGraph): Unit = {
    graph.sources.foreach(
      source => derivatives.getOrElseUpdate(source.gUID, mutable.Buffer()) += graph)
  }

  private def saveToDisk(): Unit = {
    val time = scala.compat.Platform.currentTime
    val dumpFile = new File("%s/dump-%13d".format(repositoryPath, time))
    val finalFile = new File("%s/save-%13d".format(repositoryPath, time))
    val stream = new ObjectOutputStream(new FileOutputStream(dumpFile))
    stream.writeObject(bigGraphs)
    stream.close()
    dumpFile.renameTo(finalFile)
  }

  private def loadFromDisk(): Unit = {
    bigGraphs.clear
    val repo = new File(repositoryPath)
    val snapshots = repo.list.filter(_.startsWith("save-")).sorted
    if (!snapshots.isEmpty) {
      val file = new File(repo, snapshots.last)
      val stream = new ObjectInputStream(new FileInputStream(file))
      bigGraphs ++= stream.readObject().asInstanceOf[mutable.Map[UUID, BigGraph]]
      bigGraphs.values.foreach(updateDeriatives)
    }
  }
}
