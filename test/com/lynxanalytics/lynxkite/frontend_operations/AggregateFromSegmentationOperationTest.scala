package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class AggregateFromSegmentationOperationTest extends OperationsTestBase {

  test("Aggregate from segmentation") {
    val project = box("Create example graph")
      .box("Find connected components", Map("name" -> "cc", "directions" -> "require both directions"))
      .box(
        "Aggregate from segmentation",
        Map(
          "apply_to_graph" -> ".cc",
          "aggregate_id" -> "set",
          "aggregate_size" -> "first",
        ),
      )
      .project
    val size = project.vertexAttributes("cc_size_first").runtimeSafeCast[Double]
    assert(size.rdd.collect.toSeq == Seq(0 -> 2.0, 1 -> 2.0, 2 -> 1.0, 3 -> 1.0))
    val id = project.vertexAttributes("cc_id_set").runtimeSafeCast[Set[String]]
    assert(id.rdd.collect.toSeq == Seq(0 -> Set("0"), 1 -> Set("0"), 2 -> Set("2"), 3 -> Set("3")))
  }
}

class WeightedAggregateFromSegmentationOperationTest extends OperationsTestBase {

  test("Weighted aggregate from segmentation") {
    val project = box("Create example graph")
      .box("Find connected components", Map("name" -> "cc", "directions" -> "require both directions"))
      .box(
        "Weighted aggregate from segmentation",
        Map(
          "apply_to_graph" -> ".cc",
          "weight" -> "size",
          "aggregate_id" -> "by_max_weight",
          "aggregate_size" -> "by_min_weight",
        ),
      )
      .project
    val id_by_max_weight_by_size = project.vertexAttributes("cc_id_by_max_weight_by_size").runtimeSafeCast[String]
    assert(id_by_max_weight_by_size.rdd.collect.toSet == Set(0 -> "0", 1 -> "0", 2 -> "2", 3 -> "3"))
    val size_by_min_weight_by_size = project.vertexAttributes("cc_size_by_min_weight_by_size").runtimeSafeCast[Double]
    assert(size_by_min_weight_by_size.rdd.collect.toSeq == Seq(0 -> 2.0, 1 -> 2.0, 2 -> 1.0, 3 -> 1.0))
  }
}
