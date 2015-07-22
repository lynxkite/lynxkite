package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ConvertVerticesIntoEdgesOperationTest extends OperationsTestBase {
  test("Convert vertices into edges") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/loop-edges.csv",
      "header" -> "src,dst,color",
      "delimiter" -> ",",
      "id-attr" -> "id",
      "omitted" -> "",
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
