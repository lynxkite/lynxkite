package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class FillEdgeAttributeWithConstantDefaultValueOperationTest extends OperationsTestBase {
  test("Fill edge attribute with constant default value") {
    run("Create example graph")
    run("Derive edge attribute",
      Map("type" -> "double", "output" -> "income_edge", "expr" -> "src$income"))
    run("Fill edge attribute with constant default value",
      Map("attr" -> "income_edge", "def" -> "-1.0"))
    val filledIncome = project.edgeAttributes("income_edge").runtimeSafeCast[Double]
    assert(filledIncome.rdd.values.collect.toSeq.sorted == Seq(-1.0, 1000.0, 2000.0, 2000.0))
  }
}

