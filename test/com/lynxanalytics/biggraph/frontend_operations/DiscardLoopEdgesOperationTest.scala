package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DiscardLoopEdgesOperationTest extends OperationsTestBase {
  test("Compiles and fails") {
    assert(false)
  }
  /*
  test("Discard loop edges") {
    run("Import vertices and edges from a single table", Map(
      "table" -> importCSV("OPERATIONSTEST$/loop-edges.csv"),
      "src" -> "src",
      "dst" -> "dst"))
    def colors =
      project.edgeAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    run("Discard loop edges")
    assert(colors == Seq("blue", "green")) // "red" was the loop edge.
  }
  */
}
