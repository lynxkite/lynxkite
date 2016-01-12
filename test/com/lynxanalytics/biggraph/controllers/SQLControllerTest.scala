package com.lynxanalytics.biggraph.controllers

import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.graph_api._

class SQLControllerTest extends BigGraphControllerTest {
  val sqlController = new SQLController(this)

  test("sql on vertices") {
    run("Example Graph")
    val result = sqlController.runQuery(user, SQLRequest(project = projectName,
      sql = "select name from `!vertices` where age < 40"))
    assert(result.header == Array("name"))
    assert(result.data == Array(Seq("Adam"), Seq("Eve"), Seq("Isolated Joe")))
  }
}
