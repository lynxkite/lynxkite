package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SplitEdgesOperationTest extends OperationsTestBase {
  test("Split edges") {
    val project = box("Create example graph")
      .box("Split edges", Map("rep" -> "weight", "idx" -> "index"))
      .project
    val weight = project.edgeAttributes("weight").runtimeSafeCast[Double]
    assert(weight.rdd.values.collect.toSeq.sorted == Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4))
    val index = project.edgeAttributes("index").runtimeSafeCast[Long]
    assert(index.rdd.values.collect.toSeq.sorted == Seq(0, 0, 0, 0, 1, 1, 1, 2, 2, 3))
  }
}

