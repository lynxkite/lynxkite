package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveScalarTest extends OperationsTestBase {
  test("Derive scalar") {
    val project = box("Create example graph")
      .box("Derive scalar",
        Map("output" -> "output", "expr" -> "20.0 + greeting.length")).project
    val sc = project.scalars("output").runtimeSafeCast[Double]
    assert(sc.value == 36)
  }
}
