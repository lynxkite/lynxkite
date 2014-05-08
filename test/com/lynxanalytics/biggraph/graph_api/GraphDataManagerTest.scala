package com.lynxanalytics.biggraph.graph_api

import java.io.File
import org.apache.spark
import org.apache.spark.graphx
import org.apache.spark.rdd
import org.scalatest.FunSuite

class GraphDataManagerTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("We can obtain a simple new graph") {
    val graphManager = cleanGraphManager("createone")
    val dataManager = cleanDataManager("createone")
    val myGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val myData = dataManager.obtainData(myGraph)
    assert(TestUtils.RDDToSortedString(myData.vertices) ==
             "(0,Adam)\n" +
             "(1,Eve)\n" +
             "(2,Bob)")
    assert(TestUtils.RDDToSortedString(myData.edges) ==
             "Edge(0,1,Adam loves Eve)\n" +
             "Edge(1,0,Eve loves Adam)\n" +
             "Edge(2,0,Bob envies Adam)\n" +
             "Edge(2,1,Bob loves Eve)")
    assert(TestUtils.RDDToSortedString(myData.triplets) ==
             "((0,Adam),(1,Eve),Adam loves Eve)\n" +
             "((1,Eve),(0,Adam),Eve loves Adam)\n" +
             "((2,Bob),(0,Adam),Bob envies Adam)\n" +
             "((2,Bob),(1,Eve),Bob loves Eve)")
  }
}

