package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateEdgeAttributeOperationTest extends OperationsTestBase {
  test("Aggregate edge attribute") {
    val project = box("Create enhanced example graph")
      .box("Aggregate edge attribute globally",
        Map("prefix" -> "", "aggregate_weight" -> "sum", "aggregate_comment" -> ""))
      .project
    assert(project.scalars("weight_sum").value == 171.0)
  }
}
