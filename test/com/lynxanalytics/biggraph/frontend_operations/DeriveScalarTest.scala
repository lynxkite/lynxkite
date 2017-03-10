package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DeriveScalarTest extends OperationsTestBase {
  test("Derive scalar") {
    run("Create example graph")
    run("Derive scalar",
      Map("type" -> "double", "output" -> "output", "expr" -> "20 + greeting.length"))
    val sc = project.scalars("output").runtimeSafeCast[Double]
    assert(sc.value == 36)
  }
}
