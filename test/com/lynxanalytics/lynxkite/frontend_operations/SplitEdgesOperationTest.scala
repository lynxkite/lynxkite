package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class SplitEdgesOperationTest extends OperationsTestBase {
  test("Split edges") {
    val project = box("Create example graph")
      .box("Split edges", Map("rep" -> "weight", "idx" -> "index"))
      .project
    val weight = project.edgeAttributes("weight").runtimeSafeCast[Double]
    assert(weight.rdd.values.collect.toSeq.sorted == Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4))
    val index = project.edgeAttributes("index").runtimeSafeCast[Double]
    assert(index.rdd.values.collect.toSeq.sorted == Seq(0, 0, 0, 0, 1, 1, 1, 2, 2, 3))
  }
}
