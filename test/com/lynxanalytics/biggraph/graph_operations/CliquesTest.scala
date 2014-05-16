package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.graphx.VertexId
import org.scalatest.FunSuite
import scala.util.Random

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class CliquesTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  private def allConnected(
    set: Seq[VertexId], v: VertexId, edges: Set[(VertexId, VertexId)]): Boolean = {
    set.forall(u => edges.contains((u,v)) && edges.contains((v,u)))
  }

  private def childMaxCliquesOf(currentSet: Seq[VertexId],
                                vertices: Seq[VertexId],
                                edges: Set[(VertexId, VertexId)]): Seq[Seq[VertexId]] = {
    var reportCurrent = true
    val csi = currentSet.iterator.buffered
    val resultsFromChildren = vertices.flatMap(
        v => {
          if (csi.hasNext && csi.head == v) { // We already have this vertex
            csi.next
            Seq()
          } else {
            if (allConnected(currentSet, v, edges)) {
              reportCurrent = false
              if (currentSet.lastOption.forall(_ < v)) { // currentSet + {v} is a child set
                childMaxCliquesOf(currentSet :+ v, vertices, edges)
              } else {
                Seq()
              }
            } else {
              Seq()
            }
          }
        })
    if (reportCurrent) {
      resultsFromChildren :+ currentSet
    } else {
      resultsFromChildren
    }
  }

  private def localNaiveCliques(data: GraphData, minCliqueSize: Int): Seq[Seq[VertexId]] = {
    val vertices = data.vertices.collect.toSeq.map(_._1).sorted
    val edges = data.edges.collect.map(edge => (edge.srcId, edge.dstId)).toSet
    childMaxCliquesOf(Seq(), vertices, edges).filter(_.size >= minCliqueSize)
  }

  test("Check for a few random graphs") {
    val graphManager = cleanGraphManager("checkrandomgraphcliques")
    val dataManager = cleanDataManager("checkrandomgraphcliques")
    val rnd = new Random(0)
    for (i <- (0 until 20)) {
      val graph = graphManager.deriveGraph(Seq(), SimpleRandomGraph(rnd.nextInt(20),
                                                                    rnd.nextInt(),
                                                                    rnd.nextFloat()))
      val graphData = dataManager.obtainData(graph)
      val minCliqueSize = rnd.nextInt(5) + 1
      val cliquesBG = graphManager.deriveGraph(Seq(graph), FindMaxCliques("cliques", minCliqueSize))
      val idx = cliquesBG.vertexAttributes.readIndex[Array[VertexId]]("cliques")
      val cliquesFound = dataManager.obtainData(cliquesBG).vertices.collect
        .map {
          case (id, attr) => attr(idx).toList.toString
        }.sorted
      val cliquesExpected = localNaiveCliques(graphData, minCliqueSize)
        .map(_.toList.toString).sorted
      assert(cliquesFound.toList == cliquesExpected.toList)
    }
  }
}
