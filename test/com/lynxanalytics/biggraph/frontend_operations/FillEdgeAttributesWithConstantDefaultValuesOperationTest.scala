package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class FillEdgeAttributesWithConstantDefaultValuesOperationTest extends OperationsTestBase {
  test("Fill edge attributes with constant default values") {
    val project = box("Create example graph")
      .box("Derive edge attribute",
        Map("type" -> "Double", "output" -> "src_income_edge", "expr" -> "src$income"))
      .box("Derive edge attribute",
        Map("type" -> "Double", "output" -> "dst_income_edge", "expr" -> "dst$income"))
      .box("Fill edge attributes with constant default values",
        Map("fill_src_income_edge" -> "-1.0", "fill_dst_income_edge" -> "1.0")).project
    val filledSrcIncome = project.edgeAttributes("src_income_edge").runtimeSafeCast[Double]
    assert(filledSrcIncome.rdd.values.collect.toSeq.sorted == Seq(-1.0, 1000.0, 2000.0, 2000.0))
    val filledDstIncome = project.edgeAttributes("dst_income_edge").runtimeSafeCast[Double]
    assert(filledDstIncome.rdd.values.collect.toSeq.sorted == Seq(1.0, 1.0, 1000.0, 1000.0))
  }
}
