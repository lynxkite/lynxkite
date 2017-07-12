package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class FillWithConstantDefaultValueOperationTest extends OperationsTestBase {
  test("Fill with constant default value") {
    val project = box("Create example graph")
      .box("Fill vertex attributes with constant default values",
        Map("fill_income" -> "-1.0")).project
    val filledIncome = project.vertexAttributes("income").runtimeSafeCast[Double]
    assert(filledIncome.rdd.values.collect.toSeq.sorted == Seq(-1.0, -1.0, 1000.0, 2000.0))
  }
}
