package com.lynxanalytics.biggraph.graph_api

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils

class DataManagerTest extends FunSuite with TestMetaGraphManager with TestDataManager {
  test("We can obtain a simple new graph") {
    val metaManager = cleanMetaGraphManager("createone")
    val dataManager = cleanDataManager("createone")
    val instance = metaManager.apply(CreateExampleGraphOperation(), MetaDataSet())

    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.vertexSets('vertices)).rdd) ==
      "(0,())\n" +
      "(1,())\n" +
      "(2,())")
    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.vertexAttributes('name)).rdd) ==
      "(0,Adam)\n" +
      "(1,Eve)\n" +
      "(2,Bob)")
    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.vertexAttributes('age)).rdd) ==
      "(0,20.3)\n" +
      "(1,18.2)\n" +
      "(2,50.3)")

    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.edgeBundles('edges)).rdd) ==
      "(0,Edge(0,1))\n" +
      "(1,Edge(1,0))\n" +
      "(2,Edge(2,0))\n" +
      "(3,Edge(2,1))")
    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.edgeAttributes('comment)).rdd) ==
      "(0,Adam loves Eve)\n" +
      "(1,Eve loves Adam)\n" +
      "(2,Bob envies Adam)\n" +
      "(3,Bob loves Eve)")
  }
  /*
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
  }*/
}
