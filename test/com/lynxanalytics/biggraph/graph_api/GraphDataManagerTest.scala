package com.lynxanalytics.biggraph.graph_api

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils

class GraphDataManagerTest extends FunSuite with TestBigGraphManager with TestGraphDataManager {
  test("We can obtain a simple new graph") {
    val graphManager = cleanGraphManager("createone")
    val dataManager = cleanDataManager("createone")
    val myGraph = graphManager.deriveGraph(Seq(), new InstantiateSimpleGraph)
    val myData = dataManager.obtainData(myGraph)
    assert(TestUtils.RDDToSortedString(myData.vertices) ==
             "(0,Adam,20.3)\n" +
             "(1,Eve,18.2)\n" +
             "(2,Bob,50.3)")
    assert(TestUtils.RDDToSortedString(myData.edges) ==
             "Edge(0,1,Adam loves Eve)\n" +
             "Edge(1,0,Eve loves Adam)\n" +
             "Edge(2,0,Bob envies Adam)\n" +
             "Edge(2,1,Bob loves Eve)")
    assert(TestUtils.RDDToSortedString(myData.triplets) ==
             "((0,Adam,20.3),(1,Eve,18.2),Adam loves Eve)\n" +
             "((1,Eve,18.2),(0,Adam,20.3),Eve loves Adam)\n" +
             "((2,Bob,50.3),(0,Adam,20.3),Bob envies Adam)\n" +
             "((2,Bob,50.3),(1,Eve,18.2),Bob loves Eve)")
  }

  test("We can reload a graph from memory without recomputing it") {
    val graphManager = cleanGraphManager("memorycaching")
    val dataManager = cleanDataManager("memorycaching")
    val operation = new InstantiateSimpleGraph
    val myGraph = graphManager.deriveGraph(Seq(), operation)
    val myData = dataManager.obtainData(myGraph)
    val myData2 = dataManager.obtainData(myGraph)
    assert(myData eq myData2)
    assert(operation.executionCounter == 1)
  }

  test("We can reload a graph from disk without recomputing it") {
    val graphManager = cleanGraphManager("diskcaching")
    val dataManager1 = cleanDataManager("diskcaching")
    val dataManager2 = GraphDataManager(sparkContext, dataManager1.repositoryPath)
    val operation = new InstantiateSimpleGraph
    val myGraph = graphManager.deriveGraph(Seq(), operation)
    val myData = dataManager1.obtainData(myGraph)
    dataManager1.saveDataToDisk(myGraph)
    val myData2 = dataManager2.obtainData(myGraph)
    assert(myData ne myData2)
    assert(TestUtils.RDDToSortedString(myData.triplets) ==
             TestUtils.RDDToSortedString(myData2.triplets))
    assert(operation.executionCounter == 1)
  }
}
