package com.lynxanalytics.lynxkite.graph_api

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.TestUtils
import com.lynxanalytics.lynxkite.TestUtils.computeProgress
import com.lynxanalytics.lynxkite.controllers
import com.lynxanalytics.lynxkite.graph_operations.EnhancedExampleGraph
import com.lynxanalytics.lynxkite.graph_util.HadoopFile

class SparkDomainTest extends AnyFunSuite with TestMetaGraphManager with TestDataManager {
  def newDataManager(sd: SparkDomain) = new DataManager(Seq(new ScalaDomain, sd))

  test("We can reload a graph from disk without recomputing it") {
    val metaManager = cleanMetaManager
    val sparkDomain1 = cleanSparkDomain
    val dataManager1 = newDataManager(sparkDomain1)
    val operation = EnhancedExampleGraph()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.attributes('name).runtimeSafeCast[String]
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    val data1 = GraphTestUtils.get(names)(metaManager, dataManager1)
    val scalarData1 = dataManager1.get(greeting)
    val sparkDomain2 = new SparkDomain(sparkSession, sparkDomain1.repositoryPath)
    val dataManager2 = newDataManager(sparkDomain2)
    val data2 = GraphTestUtils.get(names)(metaManager, dataManager2)
    val scalarData2 = dataManager2.get(greeting)
    assert(data1 ne data2)
    assert(data1 == data2)
    assert(scalarData1 ne scalarData2)
    assert(scalarData1 == scalarData2)
    assert(operation.executionCounter == 1)
  }

  test("Ephemeral repo can read main repo") {
    val metaManager = cleanMetaManager
    val sparkDomain1 = cleanSparkDomain
    val dataManager1 = newDataManager(sparkDomain1)
    val operation = EnhancedExampleGraph()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.attributes('name).runtimeSafeCast[String]
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    dataManager1.get(greeting)
    val sparkDomain2 = {
      val tmpSD = cleanSparkDomain
      new SparkDomain(
        sparkSession,
        sparkDomain1.repositoryPath,
        ephemeralPath = Some(tmpSD.repositoryPath))
    }
    assert(sparkDomain2.has(names))
    assert(sparkDomain2.has(greeting))
  }

  test("Ephemeral repo writes to ephemeral directory") {
    val metaManager = cleanMetaManager
    val sparkDomain1 = {
      val sd1 = cleanSparkDomain
      val sd2 = cleanSparkDomain
      new SparkDomain(
        sparkSession,
        sd1.repositoryPath,
        ephemeralPath = Some(sd2.repositoryPath))
    }
    val operation = EnhancedExampleGraph()
    val instance = metaManager.apply(operation)
    val names = instance.outputs.attributes('name).runtimeSafeCast[String]
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    sparkDomain1.await(sparkDomain1.compute(names.source))
    val sparkDomainMain = new SparkDomain(sparkSession, sparkDomain1.repositoryPath)
    assert(!sparkDomainMain.has(names))
    assert(!sparkDomainMain.has(greeting))
    val sparkDomainEphemeral = new SparkDomain(sparkSession, sparkDomain1.ephemeralPath.get)
    assert(sparkDomainEphemeral.has(names))
    assert(sparkDomainEphemeral.has(greeting))
  }

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
