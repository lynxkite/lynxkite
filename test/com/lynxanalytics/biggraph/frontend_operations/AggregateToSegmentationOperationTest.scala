package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateToSegmentationOperationTest extends OperationsTestBase {

  test("Aggregate to segmentation") {
    val seg = box("Create example graph")
      .box("Find connected components", Map("name" -> "cc", "directions" -> "require both directions"))
      .box("Aggregate to segmentation",
        Map(
          "apply_to_project" -> "|cc",
          "aggregate_age" -> "average", "aggregate_name" -> "count", "aggregate_gender" -> "majority_100",
          "aggregate_id" -> "", "aggregate_location" -> "", "aggregate_income" -> ""))
      .project.segmentation("cc")
    val age = seg.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(19.25, 50.3, 2.0))
    val count = seg.vertexAttributes("name_count").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(2.0, 1.0, 1.0))
    val gender = seg.vertexAttributes("gender_majority_100").runtimeSafeCast[String]
    assert(gender.rdd.collect.toMap.values.toSeq.sorted == Seq("", "Male", "Male"))
  }

}
