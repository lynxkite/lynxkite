package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class SegmentByVectorAttributeOperationTest extends OperationsTestBase {
  test("Segment by vector attribute - vector of doubles") {
    val project = box("Create example graph")
      .box(
        "Derive vertex attribute",
        Map("output" -> "vector", "expr" -> "Vector(1.0, 2.0)"))
      .box(
        "Segment by Vector attribute",
        Map(
          "name" -> "segment",
          "attr" -> "vector")).project
    val seg = project.segmentation("segment")
    assert(seg.vertexAttributes("vector").runtimeSafeCast[Double].rdd.values.collect.toSet ==
      Set(1, 2))
  }

  test("Segment by vector attribute - vector of strings") {
    val project = box("Create example graph")
      .box(
        "Derive vertex attribute",
        Map("output" -> "vector", "expr" -> "Vector(name)"))
      .box(
        "Segment by Vector attribute",
        Map(
          "name" -> "segment",
          "attr" -> "vector")).project
    val seg = project.segmentation("segment")
    assert(seg.vertexAttributes("vector").runtimeSafeCast[String].rdd.values.collect.toSet ==
      Set("Adam", "Eve", "Bob", "Isolated Joe"))
  }
}
