package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeTwoEdgeAttributesOperationTest extends OperationsTestBase {
  test("Merge two edge attributes") {
    run("Create example graph")
    run("Derive edge attribute",
      Map("type" -> "double", "output" -> "income_edge", "expr" -> "src$income"))
    run("Merge two edge attributes",
      Map("name" -> "merged", "attr1" -> "income_edge", "attr2" -> "weight"))
    val merged = project.edgeAttributes("merged").runtimeSafeCast[Double]
    assert(merged.rdd.values.collect.toSeq.sorted == Seq(2.0, 1000.0, 2000.0, 2000.0))
  }
}

