package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class AggregateEdgeAttributeToVerticesOperationTest extends OperationsTestBase {
  test("Aggregate edge attribute to vertices, all directions") {
    val project = box("Create example graph")
      .box(
        "Aggregate edge attribute to vertices",
        Map(
          "prefix" -> "incoming",
          "direction" -> "incoming edges",
          "aggregate_weight" -> "sum",
          "aggregate_comment" -> ""))
      .box(
        "Aggregate edge attribute to vertices",
        Map(
          "prefix" -> "outgoing",
          "direction" -> "outgoing edges",
          "aggregate_weight" -> "sum",
          "aggregate_comment" -> ""))
      .box(
        "Aggregate edge attribute to vertices",
        Map(
          "prefix" -> "all",
          "direction" -> "all edges",
          "aggregate_weight" -> "sum",
          "aggregate_comment" -> ""))
      .project
    def value(direction: String) = {
      val attr = project.vertexAttributes(s"${direction}_weight_sum").runtimeSafeCast[Double]
      attr.rdd.collect.toSeq.sorted
    }
    assert(value("incoming") == Seq(0L -> 5.0, 1L -> 5.0))
    assert(value("outgoing") == Seq(0L -> 1.0, 1L -> 2.0, 2L -> 7.0))
    assert(value("all") == Seq(0L -> 6.0, 1L -> 7.0, 2L -> 7.0))
  }
}

class WeightedAggregateEdgeAttributeToVerticesOperationTest extends OperationsTestBase {
  test("Weighted aggregate edge attribute to vertices, all directions") {
    val project = box("Create example graph")
      .box(
        "Weighted aggregate edge attribute to vertices",
        Map(
          "weight" -> "weight",
          "prefix" -> "incoming",
          "direction" -> "incoming edges",
          "aggregate_weight" -> "weighted_sum",
          "aggregate_comment" -> ""),
      )
      .box(
        "Weighted aggregate edge attribute to vertices",
        Map(
          "weight" -> "weight",
          "prefix" -> "outgoing",
          "direction" -> "outgoing edges",
          "aggregate_weight" -> "weighted_sum",
          "aggregate_comment" -> ""),
      )
      .box(
        "Weighted aggregate edge attribute to vertices",
        Map(
          "weight" -> "weight",
          "prefix" -> "all",
          "direction" -> "all edges",
          "aggregate_weight" -> "weighted_sum",
          "aggregate_comment" -> ""),
      )
      .project
    def value(direction: String) = {
      val attr = project.vertexAttributes(s"${direction}_weight_weighted_sum_by_weight").runtimeSafeCast[Double]
      attr.rdd.collect.toSeq.sorted
    }
    assert(value("incoming") == Seq(0L -> 13.0, 1L -> 17.0))
    assert(value("outgoing") == Seq(0L -> 1.0, 1L -> 4.0, 2L -> 25.0))
    assert(value("all") == Seq(0L -> 14.0, 1L -> 21.0, 2L -> 25.0))
  }
}
