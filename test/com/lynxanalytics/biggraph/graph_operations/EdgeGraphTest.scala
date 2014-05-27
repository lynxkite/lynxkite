package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class EdgeGraphTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("We can get edge graph for simple graph") {
    val graphManager = cleanGraphManager("simpleedgegraph")
    val dataManager = cleanDataManager("simpleedgegraph")
    val origGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val edgeGraph = graphManager.deriveGraph(Seq(origGraph), new EdgeGraph)
    val edgeGraphData = dataManager.obtainData(edgeGraph)
    assert(TestUtils.RDDToSortedString(edgeGraphData.vertices) ==
      "(0,Adam loves Eve)\n" +
      "(1,Eve loves Adam)\n" +
      "(2,Bob envies Adam)\n" +
      "(3,Bob loves Eve)")
    assert(TestUtils.RDDToSortedString(edgeGraphData.edges) ==
      "Edge(0,1,Eve,18.2)\n" +
      "Edge(1,0,Adam,20.3)\n" +
      "Edge(2,0,Adam,20.3)\n" +
      "Edge(3,1,Eve,18.2)")
  }
}

