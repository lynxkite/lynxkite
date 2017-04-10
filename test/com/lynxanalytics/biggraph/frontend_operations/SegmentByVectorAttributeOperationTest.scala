package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentByVectorAttributeOperationTest extends OperationsTestBase {
  test("Segment by vector attribute - vector of doubles") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("type" -> "vector of doubles", "output" -> "vector", "expr" -> "[1, 2]"))

    run("Segment by vector attribute", Map(
      "name" -> "segment",
      "attr" -> "vector"))
    val seg = project.segmentation("segment")
    assert(
      seg.vertexAttributes("label").runtimeSafeCast[Double].rdd.values.collect.toSet == Set(1, 2))
  }

  test("Segment by vector attribute - vector of strings") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("type" -> "vector of doubles", "output" -> "vector", "expr" -> "[name]"))

    run("Segment by vector attribute", Map(
      "name" -> "segment",
      "attr" -> "vector"))
    val seg = project.segmentation("segment")
    assert(seg.vertexAttributes("label").runtimeSafeCast[Double].rdd.values.collect.toSet ==
      Set("Adam", "Eve", "Bob", "Isolated Joe"))
  }
}
