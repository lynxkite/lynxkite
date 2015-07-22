package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ViralModelingOperationTest extends OperationsTestBase {
  test("Viral modeling segment logic") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/viral-vertices-1.csv",
      "header" -> "id,num",
      "delimiter" -> ",",
      "id-attr" -> "internalID",
      "omitted" -> "",
      "filter" -> ""))
    run("Import edges for existing vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/viral-edges-1.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst",
      "omitted" -> "",
      "filter" -> ""))
    run("Maximal cliques", Map(
      "name" -> "cliques",
      "bothdir" -> "false",
      "min" -> "3"))
    run("Vertex attribute to double", Map(
      "attr" -> "num"))

    run("Viral modeling", Map(
      "prefix" -> "viral",
      "target" -> "num",
      "test_set_ratio" -> "0",
      "max_deviation" -> "0.75",
      "seed" -> "0",
      "iterations" -> "1",
      "min_num_defined" -> "1",
      "min_ratio_defined" -> "0.5"), on = project.segmentation("cliques").project)
    val viral = project.vertexAttributes("viral_num_after_iteration_1").runtimeSafeCast[Double]
    val stringID = project.vertexAttributes("id").runtimeSafeCast[String]
    assert(remapIDs(viral, stringID).collect.toMap == Map(
      "0" -> 0.5,
      "1" -> 0.0,
      "2" -> 1.0,
      "3" -> 2.0,
      "4" -> 0.0,
      "7" -> 3.0))
    assert(project.scalars("viral num coverage initial").value == 5)
    assert(project.scalars("viral num coverage after iteration 1").value == 6)
  }
}
