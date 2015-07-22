package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateEdgeAttributeOperationTest extends OperationsTestBase {
  test("Aggregate edge attribute") {
    run("Example Graph")
    run("Aggregate edge attribute globally",
      Map("prefix" -> "", "aggregate-weight" -> "sum", "aggregate-comment" -> ""))
    assert(project.scalars("weight_sum").value == 10.0)
  }
}
