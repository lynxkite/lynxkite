package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeTwoEdgeAttributesOperationTest extends OperationsTestBase {
  test("Merge two edge attributes") {
    val project = box("Create example graph")
      .box("Derive edge attribute",
        Map("type" -> "Double", "output" -> "income_edge", "expr" -> "src$income"))
      .box("Merge two edge attributes",
        Map("name" -> "merged", "attr1" -> "income_edge", "attr2" -> "weight"))
      .project
    val merged = project.edgeAttributes("merged").runtimeSafeCast[Double]
    assert(merged.rdd.values.collect.toSeq.sorted == Seq(2.0, 1000.0, 2000.0, 2000.0))
  }
}

