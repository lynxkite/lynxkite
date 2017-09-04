package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class FillVertexAttributesWithConstantDefaultValuesOperationTest extends OperationsTestBase {
  test("Fill vertex attributes with constant default values") {
    val project = box("Create example graph")
      .box("Derive vertex attribute",
        Map("output" -> "increased_income", "expr" -> "income + 500"))
      .box("Fill vertex attributes with constant default values",
        Map("fill_income" -> "-1.0", "fill_increased_income" -> "1.0")).project
    val filledIncome = project.vertexAttributes("income").runtimeSafeCast[Double]
    assert(filledIncome.rdd.values.collect.toSeq.sorted == Seq(-1.0, -1.0, 1000.0, 2000.0))
    val filledIncreasedIncome = project.vertexAttributes("increased_income").runtimeSafeCast[Double]
    assert(filledIncreasedIncome.rdd.values.collect.toSeq.sorted == Seq(1.0, 1.0, 1500.0, 2500.0))
  }
}
