package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SQLControllerTest extends BigGraphControllerTestBase {
  val sqlController = new SQLController(this)

  test("sql on vertices") {
    run("Example Graph")
    val result = sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec(project = projectName, sql = "select name from `!vertices` where age < 40"),
      maxRows = 10))
    assert(result.header == List("name"))
    assert(result.data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }
}
