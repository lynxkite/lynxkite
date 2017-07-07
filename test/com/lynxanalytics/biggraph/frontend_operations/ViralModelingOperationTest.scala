package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ViralModelingOperationTest extends OperationsTestBase {
  def remapIds[T](attr: Attribute[T], origIds: Attribute[String]) =
    attr.rdd.sortedJoin(origIds.rdd).map { case (id, (num, origId)) => origId -> num }

  test("Viral modeling segment logic") {
    val project = importCSV("viral-vertices-1.csv")
      .box("Use table as vertices")
      .box("Use table as edges", Map(
        "attr" -> "id",
        "src" -> "src",
        "dst" -> "dst"), Seq(importCSV("viral-edges-1.csv")))
      .box("Find maximal cliques", Map(
        "name" -> "cliques",
        "bothdir" -> "false",
        "min" -> "3"))
      .box("Convert vertex attribute to Double", Map(
        "attr" -> "num"))
      .box("Predict attribute by viral modeling", Map(
        "prefix" -> "viral",
        "target" -> "num",
        "test_set_ratio" -> "0",
        "max_deviation" -> "0.75",
        "seed" -> "0",
        "iterations" -> "1",
        "min_num_defined" -> "1",
        "min_ratio_defined" -> "0.5",
        "apply_to_project" -> "|cliques"))
      .project
    val viral = project.vertexAttributes("viral_num_after_iteration_1").runtimeSafeCast[Double]
    val stringId = project.vertexAttributes("id").runtimeSafeCast[String]
    assert(remapIds(viral, stringId).collect.toMap == Map(
      "0" -> 0.5,
      "1" -> 0.0,
      "2" -> 1.0,
      "3" -> 2.0,
      "4" -> 0.0,
      "7" -> 3.0))
    assert(project.scalars("viral num coverage initial").value == 5)
    assert(project.scalars("viral num coverage after iteration 1").value == 6)
  }
}
