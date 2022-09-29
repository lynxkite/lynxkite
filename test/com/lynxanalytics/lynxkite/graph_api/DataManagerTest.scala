package com.lynxanalytics.lynxkite.graph_api

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.TestUtils
import com.lynxanalytics.lynxkite.TestUtils.computeProgress
import com.lynxanalytics.lynxkite.controllers
import com.lynxanalytics.lynxkite.graph_operations
import com.lynxanalytics.lynxkite.graph_operations.ExampleGraph
import com.lynxanalytics.lynxkite.graph_util.HadoopFile

class DataManagerTest extends AnyFunSuite with TestMetaGraphManager with TestDataManager {

  test("We can obtain a simple new graph") {
    implicit val metaManager = cleanMetaManager
    implicit val dataManager = cleanDataManager
    val instance = metaManager.apply(ExampleGraph(), MetaDataSet())

    assert(GraphTestUtils.get(instance.outputs.vertexSets('vertices)) == Set(0, 1, 2, 3))
    assert(GraphTestUtils.get(instance.outputs.attributes('name)) ==
      Map(0 -> "Adam", 1 -> "Eve", 2 -> "Bob", 3 -> "Isolated Joe"))
    assert(GraphTestUtils.get(instance.outputs.attributes('age)) ==
      Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
    assert(GraphTestUtils.get(instance.outputs.edgeBundles('edges)) ==
      Map(0 -> Edge(0, 1), 1 -> Edge(1, 0), 2 -> Edge(2, 0), 3 -> Edge(2, 1)))
    assert(GraphTestUtils.get(instance.outputs.attributes('comment)) ==
      Map(0 -> "Adam loves Eve", 1 -> "Eve loves Adam", 2 -> "Bob envies Adam", 3 -> "Bob loves Eve"))
    assert(dataManager.get(instance.outputs.scalars('greeting)) == "Hello world! 😀 ")
  }

  test("We can compute a graph whose meta was loaded from disk") {
    val mmDir = cleanMetaManagerDir
    val metaManager = MetaRepositoryManager(mmDir)
    implicit val dataManager = cleanDataManager
    val operation = ExampleGraph()
    val instance = metaManager.apply(operation)
    val ageGUID = instance.outputs.attributes('age).gUID
    implicit val reloadedMetaManager = MetaRepositoryManager(mmDir)
    val reloadedAge = reloadedMetaManager.attribute(ageGUID).runtimeSafeCast[Double]
    assert(GraphTestUtils.get(reloadedAge) == Map(0 -> 20.3, 1 -> 18.2, 2 -> 50.3, 3 -> 2.0))
  }

  test("Failed operation can be retried") {
    implicit val metaManager = cleanMetaManager
    val sparkDomain = cleanSparkDomain
    implicit val dataManager = new DataManager(Seq(new ScalaDomain, sparkDomain))
    import Scripting._

    val testfile = HadoopFile(myTempDirPrefix) / "test.csv"
    // Create the file so the schema can be read from it.
    testfile.createFromStrings("a,b\n1,2\n")
    val imported = TestGraph.loadCSV(testfile.resolvedName)(metaManager, sparkDomain)

    // Delete file, so that the actual computation fails.
    testfile.delete()
    // The file does not exist, so the import fails.
    val e = intercept[Exception] {
      GraphTestUtils.get(imported.ids)
    }
    assert(-1.0 == computeProgress(dataManager, imported.ids))
    // Create the file.
    testfile.createFromStrings("a,b\n3,4\n")
    // The result can be accessed now.
    assert(GraphTestUtils.get(imported.columns("a").entity).values.toSeq == Seq("3"))
    // The compute progress of ids is also updated.
    GraphTestUtils.get(imported.ids)
    assert(1.0 == computeProgress(dataManager, imported.ids))
  }
}
