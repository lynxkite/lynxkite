package com.lynxanalytics.biggraph.frontend_operations

//import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FilterWithSQLTest extends OperationsTestBase {
  test("filter vertices") {
    val p = box("Create example graph")
      .box("Filter with SQL", Map("vertex_filter" -> "age < 25"))
      .output("output").project
    assert(get(p.vertexAttributes("name")) == Map(0 -> "Adam", 1 -> "Eve", 3 -> "Isolated Joe"))
    assert(get(p.edgeAttributes("comment")) == Map(0 -> "Adam loves Eve", 1 -> "Eve loves Adam"))
  }

  test("filter edges") {
    val p = box("Create example graph")
      .box("Filter with SQL", Map("edge_filter" -> "comment like '%envies%'"))
      .output("output").project
    assert(get(p.vertexAttributes("name")).size == 4)
    assert(get(p.edgeAttributes("comment")) == Map(2 -> "Bob envies Adam"))
  }

  test("filter vertices and edges") {
    val p = box("Create example graph")
      .box(
        "Filter with SQL",
        Map(
          "vertex_filter" -> "age < income",
          "edge_filter" -> "comment like '%envies%'"))
      .output("output").project
    assert(get(p.vertexAttributes("name")) == Map(0 -> "Adam", 2 -> "Bob"))
    assert(get(p.edgeAttributes("comment")) == Map(2 -> "Bob envies Adam"))
  }

  test("filter table") {
    val t = box("Create example graph")
      .box("SQL1")
      .box("Filter with SQL", Map("filter" -> "age < 25"))
      .output("output").table
    assert(t.df.select("name").collect.map(_.getString(0)).toSet == Set("Adam", "Eve", "Isolated Joe"))
  }
}
