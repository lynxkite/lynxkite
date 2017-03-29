package com.lynxanalytics.biggraph.graph_api

import org.apache.spark
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.{ EnhancedExampleGraph, ExampleGraph }
import com.lynxanalytics.biggraph.graph_util.HadoopFile

class DataManagerTest extends FunSuite with TestMetaGraphManager with TestDataManager {
  test("We can obtain a simple new graph") {
    val metaManager = cleanMetaManager
    val dataManager = cleanDataManager
    val instance = metaManager.apply(ExampleGraph(), MetaDataSet())

    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.vertexSets('vertices)).rdd) ==
      "(0,())\n" +
      "(1,())\n" +
      "(2,())\n" +
      "(3,())")
    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.attributes('name)).rdd) ==
      "(0,Adam)\n" +
      "(1,Eve)\n" +
      "(2,Bob)\n" +
      "(3,Isolated Joe)")
    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.attributes('age)).rdd) ==
      "(0,20.3)\n" +
      "(1,18.2)\n" +
      "(2,50.3)\n" +
      "(3,2.0)")

    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.edgeBundles('edges)).rdd) ==
      "(0,Edge(0,1))\n" +
      "(1,Edge(1,0))\n" +
      "(2,Edge(2,0))\n" +
      "(3,Edge(2,1))")
    assert(TestUtils.RDDToSortedString(
      dataManager.get(instance.outputs.attributes('comment)).rdd) ==
      "(0,Adam loves Eve)\n" +
      "(1,Eve loves Adam)\n" +
      "(2,Bob envies Adam)\n" +
      "(3,Bob loves Eve)")
    assert(dataManager.get(instance.outputs.scalars('greeting)).value == "Hello world! 😀 ")
  }

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

  test("We can compute a graph whose meta was loaded from disk") {
    val mmDir = cleanMetaManagerDir
    val metaManager = MetaRepositoryManager(mmDir)
    val dataManager = cleanDataManager
    val operation = ExampleGraph()
    val instance = metaManager.apply(operation)
    val ageGUID = instance.outputs.attributes('age).gUID
    val reloadedMetaManager = MetaRepositoryManager(mmDir)
    val reloadedAge = reloadedMetaManager.attribute(ageGUID).runtimeSafeCast[Double]
    assert(TestUtils.RDDToSortedString(dataManager.get(reloadedAge).rdd) ==
      "(0,20.3)\n" +
      "(1,18.2)\n" +
      "(2,50.3)\n" +
      "(3,2.0)")
  }

  test("Failed operation can be retried") {
    implicit val metaManager = cleanMetaManager
    val dataManager = cleanDataManager
    import Scripting._

    val testfile = HadoopFile(myTempDirPrefix) / "test.csv"
    // Create the file so the schema can be read from it.
    testfile.createFromStrings("a,b\n1,2\n")
    val imported = TestGraph.loadCSV(testfile.resolvedName)(metaManager, dataManager)

    // Delete file, so that the actual computation fails.
    testfile.delete()
    // The file does not exist, so the import fails.
    val e = intercept[Exception] {
      dataManager.get(imported.ids)
    }
    assert(-1.0 == dataManager.computeProgress(imported.ids))
    // Create the file.
    testfile.createFromStrings("a,b\n3,4\n")
    // The result can be accessed now.
    assert(dataManager.get(imported.columns("a").entity).rdd.values.collect.toSeq == Seq("3"))
    // The compute progress of ids is also updated.
    dataManager.get(imported.ids)
    assert(1.0 == dataManager.computeProgress(imported.ids))
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
    assert(dataManager2.computeProgress(names) == 1.0)
    assert(dataManager2.computeProgress(greeting) == 1.0)
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
    assert(dataManagerMain.computeProgress(names) == 0.0)
    assert(dataManagerMain.computeProgress(greeting) == 0.0)
    val dataManagerEphemeral = new DataManager(sparkSession, dataManager1.ephemeralPath.get)
    assert(dataManagerEphemeral.computeProgress(names) == 1.0)
    assert(dataManagerEphemeral.computeProgress(greeting) == 1.0)
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

  ignore("waitAllFutures waits for futures") {
    val metaManager = cleanMetaManager
    val dataManager = cleanDataManager

    val state = new java.util.concurrent.atomic.AtomicReference[Integer](0)
    dataManager.loggedFuture {
      Thread.sleep(1000L * 15)
      state.set(1)
    }
    val instance = metaManager.apply(ExampleGraph(), MetaDataSet())
    val greeting = instance.outputs.scalars('greeting).runtimeSafeCast[String]
    dataManager.get(greeting)

    dataManager.waitAllFutures()
    assert(state.get() == 1)
  }

  test("an operation triggering computation results in exception (#5580)") {
    implicit val metaManager = cleanMetaManager
    implicit val dataManager = cleanDataManager
    import Scripting._
    val g = ExampleGraph().result
    val vs = {
      val op = OpTriggeringTestOperation()
      op.sideChannelVS = g.vertices
      op.result.vs
    }
    val exc = intercept[Exception] {
      println(vs.rdd.collect)
    }
    assert(java.util.regex.Pattern.matches(
      ".*OpTriggeringTestOperation.* triggered the computation of .*vertices of .*ExampleGraph.*",
      exc.getCause.getMessage))
  }
}

object OpTriggeringTestOperation extends OpFromJson {
  def fromJson(j: JsValue) = OpTriggeringTestOperation()
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
  }
}
case class OpTriggeringTestOperation()
    extends TypedMetaGraphOp[graph_operations.NoInput, OpTriggeringTestOperation.Output] {
  var sideChannelVS: VertexSet = null
  @transient override lazy val inputs = new graph_operations.NoInput
  def outputMeta(instance: MetaGraphOperationInstance) =
    new OpTriggeringTestOperation.Output()(instance)
  def execute(inputDatas: DataSet,
              o: OpTriggeringTestOperation.Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    println(rc.dataManager.get(sideChannelVS).rdd.collect)
  }
}
