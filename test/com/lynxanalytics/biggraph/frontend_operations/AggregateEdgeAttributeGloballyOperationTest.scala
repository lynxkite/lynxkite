package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateEdgeAttributeOperationTest extends OperationsTestBase {
  test("Aggregate edge attribute") {
    val project = box("Create enhanced example graph")
      .box(
        "Aggregate edge attribute globally",
        Map("prefix" -> "", "aggregate_weight" -> "sum", "aggregate_comment" -> "count_distinct"))
      .project
    assert(project.scalars("weight_sum").value == 171.0)
    assert(project.scalars("comment_count_distinct").value == 19.0)
  }
}

class WeightedAggregateEdgeAttributeOperationTest extends OperationsTestBase {
  test("Weighted aggregate edge attribute") {
    val project = box("Create enhanced example graph")
      .box(
        "Weighted aggregate edge attribute globally",
        Map(
          "prefix" -> "",
          "weight" -> "weight",
          "aggregate_weight" -> "weighted_sum",
          "aggregate_comment" -> "by_max_weight"),
      )
      .project
    assert(project.scalars("weight_weighted_sum_by_weight").value == 2109.0)
    assert(project.scalars("comment_by_max_weight_by_weight").value == "The fish calls Wanda")
  }
}
