package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SplitEdgesOperationTest extends OperationsTestBase {
  test("Split edges") {
    run("Example Graph")
    run("Split edges", Map("rep" -> "weight", "idx" -> "index"))
    val merged = project.edgeAttributes("weight").runtimeSafeCast[Double]
    assert(merged.rdd.values.collect.toSeq.sorted == Seq(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 4L))
  }
}

