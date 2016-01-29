package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util

class SQLControllerTest extends BigGraphControllerTestBase {
  val sqlController = new SQLController(this)

  def await[T](f: concurrent.Future[T]): T =
    concurrent.Await.result(f, concurrent.duration.Duration.Inf)

  test("sql on vertices") {
    run("Example Graph")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec(project = projectName, sql = "select name from `!vertices` where age < 40"),
      maxRows = 10)))
    assert(result.header == List("name"))
    assert(result.data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("sql export to csv") {
    run("Example Graph")
    val result = await(sqlController.exportSQLQueryToCSV(user, SQLExportToCSVRequest(
      DataFrameSpec(project = projectName, sql = "select name, age from `!vertices` where age < 40"),
      path = "<download>",
      delimiter = ";",
      quote = "\"",
      header = true)))
    val output = graph_util.HadoopFile(result.download.get).loadTextFile(sparkContext)
    assert(output.collect.sorted.mkString(", ") ==
      "Adam;20.3, Eve;18.2, Isolated Joe;2.0, name;age")
  }

  test("sql export to database") {
    val url = s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Example Graph")
    val result = await(sqlController.exportSQLQueryToJdbc(user, SQLExportToJdbcRequest(
      DataFrameSpec(project = projectName, sql = "select name, age from `!vertices` where age < 40"),
      jdbcUrl = url,
      table = "export_test",
      mode = "error")))
    val connection = java.sql.DriverManager.getConnection(url)
    val statement = connection.createStatement()
    val results = {
      val rs = statement.executeQuery("select * from export_test;")
      new Iterator[String] {
        def hasNext = rs.next
        def next = s"${rs.getString(1)};${rs.getDouble(2)}"
      }.toIndexedSeq
    }
    connection.close()
    assert(results.sorted == Seq("Adam;20.3", "Eve;18.2", "Isolated Joe;2.0"))
  }

  test("import from CSV") {
    val res = getClass.getResource("/graph_operations/ImportGraphTest").toString
    graph_util.PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", res)
    val csvFiles = "IMPORTGRAPHTEST$/testgraph/vertex-data/part*"
    val response = await(sqlController.importCSV(
      user,
      CSVImportRequest(
        table = "csv-import-test",
        privacy = "public-read",
        files = csvFiles,
        columnNames = List("vertexId", "name", "age"),
        delimiter = ",",
        mode = "FAILFAST")))
    val tablePath = response.id

    run(
      "Import vertices from table",
      Map(
        "table" -> tablePath,
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

    val response = await(sqlController.importJdbc(
      user,
      JdbcImportRequest(
        table = "jdbc-import-test",
        privacy = "public-read",
        jdbcUrl = url,
        jdbcTable = "subscribers",
        keyColumn = "id",
        columnsToImport = List("n", "id", "name", "race condition", "level"))))
    val tablePath = response.id

    run(
      "Import vertices from table",
      Map(
        "table" -> tablePath,
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
