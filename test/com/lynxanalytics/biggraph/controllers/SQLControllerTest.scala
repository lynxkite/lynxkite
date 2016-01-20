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

  test("import from SQLite") {
    val url = s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    val connection = java.sql.DriverManager.getConnection(url)
    val statement = connection.createStatement()
    statement.executeUpdate("""
    DROP TABLE IF EXISTS subscribers;
    CREATE TABLE subscribers
      (n TEXT, id INTEGER, name TEXT, gender TEXT, "race condition" TEXT, level DOUBLE PRECISION);
    INSERT INTO subscribers VALUES
      ('A', 1, 'Daniel', 'Male', 'Halfling', 10.0),
      ('B', 2, 'Beata', 'Female', 'Dwarf', 20.0),
      ('C', 3, 'Felix', 'Male', 'Gnome', NULL),
      (NULL, 4, NULL, NULL, NULL, NULL);
    """)
    connection.close()

    val cpResponse = sqlController.importJDBC(
      user,
      JDBCImportRequest(
        url, "subscribers", "id", List("n", "id", "name", "race condition", "level")))
    val tableCheckpoint = s"!checkpoint(${cpResponse.checkpoint},)"

    run(
      "Import vertices from table",
      Map(
        "table" -> s"${tableCheckpoint}|!vertices",
        "id-attr" -> "new_id"))
    assert(vattr[String]("n") == Seq("A", "B", "C"))
    assert(vattr[Long]("id") == Seq(1, 2, 3, 4))
    assert(vattr[String]("name") == Seq("Beata", "Daniel", "Felix"))
    assert(vattr[String]("race condition") == Seq("Dwarf", "Gnome", "Halfling"))
    assert(vattr[Double]("level") == Seq(10.0, 20.0))
    assert(subProject.viewer.vertexAttributes.keySet ==
      Set("new_id", "n", "id", "name", "race condition", "level"))
  }
}
