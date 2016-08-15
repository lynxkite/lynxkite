package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class TriadicClosureTest extends OperationsTestBase {
  test("Triadic closure") {

    run("triadic closure on small graph", Map(
      "table" -> importCSV("OPERATIONSTEST$/triadic-test.csv"),
      "src" -> "src",
      "dst" -> "dst"))

    assert(colors == Seq("blue", "green", "red"))


  }
}