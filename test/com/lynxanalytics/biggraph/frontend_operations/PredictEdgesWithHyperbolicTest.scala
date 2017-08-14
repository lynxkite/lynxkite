package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class PredictEdgesWithHyperbolicTest extends OperationsTestBase {
  test("Predict edges with hyperbolic positions") {
    val project = box("Create example graph")
      .box("Predict edges with hyperbolic positions", Map(
        "size" -> "4",
        "radial" -> "age",
        "angular" -> "age"))
      .project

    assert(project.edgeAttributes("hyperbolic_edge_probability")
      .runtimeSafeCast[Double].rdd.values.collect.toSeq(0) > 0)
    assert(project.edgeAttributes("comment").rdd.values.collect.toSeq(0) == "Bob loves Eve")
  }
}
