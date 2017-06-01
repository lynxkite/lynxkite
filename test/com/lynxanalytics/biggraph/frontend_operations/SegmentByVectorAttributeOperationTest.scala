package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentByVectorAttributeOperationTest extends OperationsTestBase {
  test("Segment by vector attribute - vector of doubles") {
    val project = box("Example Graph")
      .box("Derived vertex attribute",
        Map("type" -> "vector of doubles", "output" -> "vector", "expr" -> "[1, 2]"))
      .box("Segment by Vector attribute", Map(
        "name" -> "segment",
        "attr" -> "vector")).project
    val seg = project.segmentation("segment")
    assert(seg.vertexAttributes("vector").runtimeSafeCast[Double].rdd.values.collect.toSet ==
      Set(1, 2))
  }

  test("Segment by vector attribute - vector of strings") {
    val project = box("Example Graph")
      .box("Derived vertex attribute",
        Map("type" -> "vector of strings", "output" -> "vector", "expr" -> "[name]"))
      .box("Segment by Vector attribute", Map(
        "name" -> "segment",
        "attr" -> "vector")).project
    val seg = project.segmentation("segment")
    assert(seg.vertexAttributes("vector").runtimeSafeCast[String].rdd.values.collect.toSet ==
      Set("Adam", "Eve", "Bob", "Isolated Joe"))
  }
}
