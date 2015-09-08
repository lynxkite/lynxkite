package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class ConvertVerticesIntoEdgesOperationTest extends OperationsTestBase {
  test("Convert vertices into edges") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/loop-edges.csv",
      "header" -> "src,dst,color",
      "delimiter" -> ",",
      "id-attr" -> "id",
      "omitted" -> "",
      "allow-corrupt-lines" -> "yes",
      "filter" -> ""))
    var colors =
      project.vertexAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    run("Convert vertices into edges", Map("src" -> "src", "dst" -> "dst"))
    colors =
      project.edgeAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    val stringIDs =
      project.vertexAttributes("stringID").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(stringIDs == Seq("0", "1", "2"))
  }

}
