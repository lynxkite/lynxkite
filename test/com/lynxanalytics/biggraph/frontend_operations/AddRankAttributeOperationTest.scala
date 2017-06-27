package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AddRankAttributeOperationTest extends OperationsTestBase {
  test("Rank attribute for string ascending") {
    val project = box("Create example graph")
      .box("Add rank attribute",
        Map("rankattr" -> "ranking", "keyattr" -> "name", "order" -> "ascending"))
      .project
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 2.0, 2 -> 1.0, 3 -> 3.0))
  }
  test("Rank attribute for string descending") {
    val project = box("Create example graph")
      .box("Add rank attribute",
        Map("rankattr" -> "ranking", "keyattr" -> "name", "order" -> "descending"))
      .project
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 3.0, 1 -> 1.0, 2 -> 2.0, 3 -> 0.0))
  }
  test("Rank attribute for string undefined") {
    val project = box("Create example graph")
      .box("Convert vertex attribute to String", Map("attr" -> "income"))
      .box("Add rank attribute",
        Map("rankattr" -> "ranking", "keyattr" -> "income", "order" -> "ascending"))
      .project
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 0.0, 2 -> 1.0))
  }
  test("Rank attribute for double ascending") {
    val project = box("Create example graph")
      .box("Add rank attribute",
        Map("rankattr" -> "ranking", "keyattr" -> "age", "order" -> "ascending"))
      .project
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 2.0, 1 -> 1.0, 2 -> 3.0, 3 -> 0.0))
  }
  test("Rank attribute for double descending") {
    val project = box("Create example graph")
      .box("Add rank attribute",
        Map("rankattr" -> "ranking", "keyattr" -> "age", "order" -> "descending"))
      .project
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 1.0, 1 -> 2.0, 2 -> 0.0, 3 -> 3.0))
  }
}
