package com.lynxanalytics.biggraph.graph_api

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils

class DataManagerTest extends FunSuite with TestMetaGraphManager with TestDataManager {
  test("We can obtain a simple new graph") {
    val metaManager = cleanMetaManager
    val dataManager = cleanDataManager
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

  test("We can reload a graph from disk without recomputing it") {
    val metaManager = cleanMetaManager
    val dataManager1 = cleanDataManager
    val dataManager2 = new DataManager(sparkContext, dataManager1.repositoryPath)
    val operation = CreateExampleGraphOperation()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.vertexAttributes('name).runtimeSafeCast[String]
    val data1: VertexAttributeData[String] = dataManager1.get(names)
    dataManager1.saveToDisk(names)
    val data2 = dataManager2.get(names)
    assert(data1 ne data2)
    assert(TestUtils.RDDToSortedString(data1.rdd) ==
      TestUtils.RDDToSortedString(data2.rdd))
    assert(operation.executionCounter == 1)
  }
}
