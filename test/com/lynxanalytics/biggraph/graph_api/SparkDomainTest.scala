package com.lynxanalytics.biggraph.graph_api

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.TestUtils.computeProgress
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.ExampleGraph
import com.lynxanalytics.biggraph.graph_util.HadoopFile

class SparkDomainTest extends FunSuite with TestMetaGraphManager with TestDataManager {
  /*
  test("We can reload a graph from disk without recomputing it") {
    val metaManager = cleanMetaManager
    val dataManager1 = cleanDataManager
    val operation = ExampleGraph()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.attributes('name).runtimeSafeCast[String]
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    val data1: AttributeData[String] = dataManager1.get(names)
    val scalarData1: ScalarData[String] = dataManager1.get(greeting)
    val dataManager2 = new DataManager(sparkSession, dataManager1.repositoryPath)
    val data2 = dataManager2.get(names)
    val scalarData2 = dataManager2.get(greeting)
    assert(data1 ne data2)
    assert(TestUtils.RDDToSortedString(data1.rdd) ==
      TestUtils.RDDToSortedString(data2.rdd))
    assert(scalarData1 ne scalarData2)
    assert(scalarData1.value == scalarData2.value)
    assert(operation.executionCounter == 1)
  }

  test("Ephemeral repo can read main repo") {
    val metaManager = cleanMetaManager
    val dataManager1 = cleanDataManager
    val operation = ExampleGraph()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.attributes('name).runtimeSafeCast[String]
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    val data1: AttributeData[String] = dataManager1.get(names)
    val scalarData1: ScalarData[String] = dataManager1.get(greeting)
    val dataManager2 = {
      val tmpDM = cleanDataManager
      new DataManager(
        sparkSession, dataManager1.repositoryPath,
        ephemeralPath = Some(tmpDM.repositoryPath))
    }
    assert(computeProgress(dataManager2, names) == 1.0)
    assert(computeProgress(dataManager2, greeting) == 1.0)
  }

  test("Ephemeral repo writes to ephemeral directory") {
    val metaManager = cleanMetaManager
    val dataManager1 = {
      val dm1 = cleanDataManager
      val dm2 = cleanDataManager
      new DataManager(
        sparkSession, dm1.repositoryPath,
        ephemeralPath = Some(dm2.repositoryPath))
    }
    val operation = ExampleGraph()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.attributes('name).runtimeSafeCast[String]
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    val data1: AttributeData[String] = dataManager1.get(names)
    val scalarData1: ScalarData[String] = dataManager1.get(greeting)
    val dataManagerMain = new DataManager(sparkSession, dataManager1.repositoryPath)
    assert(computeProgress(dataManagerMain, names) == 0.0)
    assert(computeProgress(dataManagerMain, greeting) == 0.0)
    val dataManagerEphemeral = new DataManager(sparkSession, dataManager1.ephemeralPath.get)
    assert(computeProgress(dataManagerEphemeral, names) == 1.0)
    assert(computeProgress(dataManagerEphemeral, greeting) == 1.0)
  }*/

  /*
  // TODO: Adapt to boxes.
  case class TestTable(idSet: VertexSet, columns: Map[String, Attribute[_]])
    extends controllers.Table

  test("operation chaining does not exhaust thread pool (#5580)") {
    implicit val metaManager = cleanMetaManager
    implicit val dataManager = cleanDataManager
    import Scripting._
    var df = dataManager.sparkSession.range(5).toDF("x")
    for (i <- 1 to 6) {
      val g = graph_operations.ImportDataFrame(df).result
      df = TestTable(g.ids, g.columns.mapValues(_.entity)).toDF(dataManager.masterSQLContext)
    }
    assert(df.count == 5)
  }
  */
}
