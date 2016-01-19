package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util

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

  test("sql export") {
    run("Example Graph")
    val result = sqlController.exportSQLQuery(user, SQLExportRequest(
      DataFrameSpec(project = projectName, sql = "select name from `!vertices` where age < 40"),
      format = "csv", path = "<download>", options = Map()))
    val output = graph_util.HadoopFile(result.download.get).loadTextFile(sparkContext)
    assert(output.collect.sorted.mkString(", ") == "Adam, Eve, Isolated Joe")
  }

  test("import from CSV") {
    val res = getClass.getResource("/graph_operations/ImportGraphTest").toString
    graph_util.PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", res)
    val csvFiles = "IMPORTGRAPHTEST$/testgraph/vertex-data/part*"
    val cpResponse = sqlController.importCSV(
      user, CSVImportRequest(csvFiles, List("vertexId", "name", "age"), ",", "FAILFAST"))
    val tableCheckpoint = s"!checkpoint(${cpResponse.checkpoint},)"

    run(
      "Import vertices from table",
      Map(
        "table" -> s"${tableCheckpoint}|!vertices",
        "id-attr" -> "new_id"))
    assert(vattr[String]("vertexId") == Seq("0", "1", "2"))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[String]("age") == Seq("18.2", "20.3", "50.3"))
  }
}
