package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._

class AggregateVertexAttributeOperationTest extends OperationsTestBase {
  test("Aggregate vertex attribute globally") {
    val project = box("Create enhanced example graph")
      .box(
        "Aggregate vertex attribute globally",
        Map("prefix" -> "", "aggregate_age" -> "average", "aggregate_name" -> "count_distinct"))
      .project
    assert(project.scalars("age_average").value == 21.8625)
    assert(project.scalars("name_count_distinct").value == 8)
  }
}

class WeightedAggregateVertexAttributeOperationTest extends OperationsTestBase {
  test("Weighted aggregate vertex attribute") {
    val project = box("Create enhanced example graph")
      .box(
        "Weighted aggregate vertex attribute globally",
        Map(
          "prefix" -> "",
          "weight" -> "age",
          "aggregate_age" -> "weighted_sum",
          "aggregate_gender" -> "by_max_weight"),
      )
      .project
    assert(project.scalars("age_weighted_sum_by_age").value == 5808.63)
    assert(project.scalars("gender_by_max_weight_by_age").value == "Male")
  }
}
