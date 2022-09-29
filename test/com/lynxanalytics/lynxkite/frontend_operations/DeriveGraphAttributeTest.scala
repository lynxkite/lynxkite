package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._

class DeriveGraphAttributeTest extends OperationsTestBase {
  test("Derive graph attribute") {
    val project = box("Create example graph")
      .box(
        "Derive graph attribute",
        Map("output" -> "output", "expr" -> "20.0 + greeting.length")).project
    val sc = project.scalars("output").runtimeSafeCast[Double]
    assert(sc.value == 36)
  }
}
