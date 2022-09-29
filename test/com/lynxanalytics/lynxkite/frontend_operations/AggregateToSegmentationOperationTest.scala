package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class AggregateToSegmentationOperationTest extends OperationsTestBase {

  test("Aggregate to segmentation") {
    val seg = box("Create example graph")
      .box("Find connected components", Map("name" -> "cc", "directions" -> "require both directions"))
      .box(
        "Aggregate to segmentation",
        Map(
          "apply_to_graph" -> ".cc",
          "aggregate_age" -> "average",
          "aggregate_name" -> "count",
          "aggregate_gender" -> "majority_100",
          "aggregate_id" -> "",
          "aggregate_location" -> "",
          "aggregate_income" -> "",
        ),
      )
      .project.segmentation("cc")
    val age = seg.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(19.25, 50.3, 2.0))
    val count = seg.vertexAttributes("name_count").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(2.0, 1.0, 1.0))
    val gender = seg.vertexAttributes("gender_majority_100").runtimeSafeCast[String]
    assert(gender.rdd.collect.toMap.values.toSeq.sorted == Seq("", "Male", "Male"))
  }

}

class WeightedAggregateToSegmentationOperationTest extends OperationsTestBase {
  test("Weighted aggregate to segmentation") {
    def agg[T](attribute: String, aggregator: String, weight: String): Map[Long, T] = {
      val seg = box("Create example graph")
        .box("Find connected components", Map("name" -> "cc", "directions" -> "require both directions"))
        .box(
          "Weighted aggregate to segmentation",
          Map(
            "weight" -> weight,
            "apply_to_graph" -> ".cc",
            ("aggregate_" + attribute) -> aggregator))
        .project.segmentation("cc")
      get(seg.vertexAttributes(attribute + "_" + aggregator + "_by_" + weight)).asInstanceOf[Map[Long, T]]
    }
    assert(agg[Double]("age", "weighted_average", "age").mapValues(_.round) == Map(0 -> 19, 2 -> 50, 3 -> 2))
    assert(agg[Double]("age", "weighted_sum", "age").mapValues(_.round) == Map(0 -> 743, 2 -> 2530, 3 -> 4))
    assert(agg[Double]("gender", "by_max_weight", "age") == Map(0 -> "Male", 2 -> "Male", 3 -> "Male"))
    assert(agg[Double]("name", "by_min_weight", "age") == Map(0 -> "Eve", 2 -> "Bob", 3 -> "Isolated Joe"))
  }
}
