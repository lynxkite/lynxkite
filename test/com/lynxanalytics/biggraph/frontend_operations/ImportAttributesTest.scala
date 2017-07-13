package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_operations

class ImportAttributesTest extends OperationsTestBase {
  test("Use table as vertex attributes") {
    val project = box("Create example graph")
      .box("Use table as vertex attributes", Map(
        "id_attr" -> "name",
        "id_column" -> "name",
        "prefix" -> "imported"), Seq(importCSV("import-vertex-attributes.csv")))
      .project
    assert(project.vertexAttributes("imported_favorite_color").rdd.collect.toSeq.sortBy(_._1) ==
      Seq(0 -> "green", 1 -> "yellow", 2 -> "red",
        3 -> "the color of television, tuned to a dead channel"))
  }

  test("Use table as edge attributes") {
    val project = box("Create example graph")
      .box("Use table as edge attributes", Map(
        "id_attr" -> "comment",
        "id_column" -> "comment",
        "prefix" -> "imported"), Seq(importCSV("import-edge-attributes.csv")))
      .project
    assert(project.edgeAttributes("imported_score").rdd.collect.toSeq.sortBy(_._1) ==
      Seq(0 -> "1", 1 -> "1", 2 -> "-1", 3 -> "-1"))
  }
}
