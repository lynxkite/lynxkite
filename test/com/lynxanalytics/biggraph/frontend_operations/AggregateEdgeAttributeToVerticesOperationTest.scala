package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class AggregateEdgeAttributeToVerticesOperationTest extends OperationsTestBase {
  test("Aggregate edge attribute to vertices, all directions") {
    val a = box("Create example graph")
      .box("Aggregate edge attribute to vertices", Map(
        "prefix" -> "incoming",
        "direction" -> "incoming edges",
        "aggregate_weight" -> "sum",
        "aggregate_comment" -> ""))
      .box("Aggregate edge attribute to vertices", Map(
        "prefix" -> "outgoing",
        "direction" -> "outgoing edges",
        "aggregate_weight" -> "sum",
        "aggregate_comment" -> ""))
      .box("Aggregate edge attribute to vertices", Map(
        "prefix" -> "all",
        "direction" -> "all edges",
        "aggregate_weight" -> "sum",
        "aggregate_comment" -> ""))
    val project = a.project
    def value(direction: String) = {
      val attr = project.vertexAttributes(s"${direction}_weight_sum").runtimeSafeCast[Double]
      attr.rdd.collect.toSeq.sorted
    }
    assert(value("incoming") == Seq(0L -> 5.0, 1L -> 5.0))
    assert(value("outgoing") == Seq(0L -> 1.0, 1L -> 2.0, 2L -> 7.0))
    assert(value("all") == Seq(0L -> 6.0, 1L -> 7.0, 2L -> 7.0))
  }
}

