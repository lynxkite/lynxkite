package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DiscardLoopEdgesOperationTest extends OperationsTestBase {
  test("Discard loop edges") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/loop-edges.csv",
      "header" -> "src,dst,color",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "allow-corrupt-lines" -> "no",
      "filter" -> ""))
    def colors =
      project.edgeAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    run("Discard loop edges")
    assert(colors == Seq("blue", "green")) // "red" was the loop edge.
  }
}
