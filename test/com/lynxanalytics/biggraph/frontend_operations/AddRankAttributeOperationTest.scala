package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AddRankAttributeOperationTest extends OperationsTestBase {
  test("Rank attribute for string ascending") {
    run("Example Graph")
    run("Add rank attribute",
      Map("rankattr" -> "ranking", "keyattr" -> "name", "order" -> "ascending"))
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 2.0, 2 -> 1.0, 3 -> 3.0))
  }
  test("Rank attribute for string descending") {
    run("Example Graph")
    run("Add rank attribute",
      Map("rankattr" -> "ranking", "keyattr" -> "name", "order" -> "descending"))
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 3.0, 1 -> 1.0, 2 -> 2.0, 3 -> 0.0))
  }
  test("Rank attribute for string undefined") {
    run("Example Graph")
    run("Vertex attribute to string", Map("attr" -> "income"))
    run("Add rank attribute",
      Map("rankattr" -> "ranking", "keyattr" -> "income", "order" -> "ascending"))
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 0.0, 2 -> 1.0))
  }
  test("Rank attribute for double ascending") {
    run("Example Graph")
    run("Add rank attribute",
      Map("rankattr" -> "ranking", "keyattr" -> "age", "order" -> "ascending"))
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 2.0, 1 -> 1.0, 2 -> 3.0, 3 -> 0.0))
  }
  test("Rank attribute for double descending") {
    run("Example Graph")
    run("Add rank attribute",
      Map("rankattr" -> "ranking", "keyattr" -> "age", "order" -> "descending"))
    val attr = project.vertexAttributes("ranking").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 1.0, 1 -> 2.0, 2 -> 0.0, 3 -> 3.0))
  }
}
