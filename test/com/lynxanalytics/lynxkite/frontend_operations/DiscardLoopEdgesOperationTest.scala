package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class DiscardLoopEdgesOperationTest extends OperationsTestBase {
  test("Discard loop edges") {
    val imported = importCSV("loop-edges.csv")
      .box(
        "Use table as graph",
        Map(
          "src" -> "src",
          "dst" -> "dst"))
    val discarded = imported.box("Discard loop edges")
    def colors(box: TestBox) =
      box.project.edgeAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors(imported) == Seq("blue", "green", "red"))
    assert(colors(discarded) == Seq("blue", "green")) // "red" was the loop edge.
  }
}
