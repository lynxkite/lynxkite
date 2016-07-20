package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util

class SQLControllerTest extends BigGraphControllerTestBase {
  val sqlController = new SQLController(this)
  val resourceDir = getClass.getResource("/graph_operations/ImportGraphTest").toString
  graph_util.PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", resourceDir)

  def await[T](f: concurrent.Future[T]): T =
    concurrent.Await.result(f, concurrent.duration.Duration.Inf)

  test("global sql on vertices") {
    val globalProjectframe = DirectoryEntry.fromName("Test_Dir/Test_Project").asNewProjectFrame()
    run("Example Graph", on = "Test_Dir/Test_Project")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "Test_Dir",
        sql = "select name from `Test_Project|vertices` where age < 40"),
      maxRows = 10)))
    assert(result.header == List("name"))
    assert(result.data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql on vertices with attribute name quoted with backticks") {
    val globalProjectframe = DirectoryEntry.fromName("Test_Dir/Test_Project").asNewProjectFrame()
    run("Example Graph", on = "Test_Dir/Test_Project")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "Test_Dir",
        sql = "select `name` from `Test_Project|vertices` where age < 40"),
      maxRows = 10)))
    assert(result.header == List("name"))
    assert(result.data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("sql on vertices") {
    run("Example Graph")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.local(project = projectName, sql = "select name from vertices where age < 40"),
      maxRows = 10)))
    assert(result.header == List("name"))
    assert(result.data == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("sql with empty results") {
    run("Example Graph")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.local(project = projectName, sql = "select id from vertices where id = 11"),
      maxRows = 10)))
    assert(result.header == List("id"))
    assert(result.data == List())
  }

  test("sql file reading is disabled") {
    val file = getClass.getResource("/controllers/noread.csv").toString
    intercept[Throwable] {
      await(sqlController.runSQLQuery(user, SQLQueryRequest(
        DataFrameSpec.local(
          project = projectName,
          sql = s"select * from csv.`$file`"),
        maxRows = 10)))
    }
  }

  test("sql export to csv") {
    run("Example Graph")
    val result = await(sqlController.exportSQLQueryToCSV(user, SQLExportToCSVRequest(
      DataFrameSpec.local(
        project = projectName,
        sql = "select name, age from vertices where age < 40"),
      path = "<download>",
      delimiter = ";",
      quote = "\"",
      header = true)))
    val output = graph_util.HadoopFile(result.download.get.path).loadTextFile(sparkContext)
    val header = "name;age"
    assert(output.collect.filter(_ != header).sorted.mkString(", ") ==
      "Adam;20.3, Eve;18.2, Isolated Joe;2.0")
  }

  test("sql export to database") {
    val url = s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Example Graph")
    val result = await(sqlController.exportSQLQueryToJdbc(user, SQLExportToJdbcRequest(
      DataFrameSpec.local(
        project = projectName,
        sql = "select name, age from vertices where age < 40 order by name"),
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

  def importCSV(file: String, columns: List[String], infer: Boolean): Unit = {
    val csvFiles = "IMPORTGRAPHTEST$/" + file + "/part*"
    val response = sqlController.importCSV(
      user,
      CSVImportRequest(
        table = "csv-import-test",
        privacy = "public-read",
        files = csvFiles,
        columnNames = columns,
        delimiter = ",",
        mode = "FAILFAST",
        infer = infer,
        columnsToImport = List()))
    val tablePath = response.id
    run(
      "Import vertices",
      Map(
        "table" -> tablePath,
        "id-attr" -> "new_id"))
  }

  test("import from CSV without header") {
    importCSV("testgraph/vertex-data", List("vertexId", "name", "age"), infer = false)
    assert(vattr[String]("vertexId") == Seq("0", "1", "2"))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[String]("age") == Seq("18.2", "20.3", "50.3"))
  }

  test("import from CSV with header") {
    importCSV("with-header", List(), infer = false)
    assert(vattr[String]("vertexId") == Seq("0", "1", "2"))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[String]("age") == Seq("18.2", "20.3", "50.3"))
  }

  test("import from CSV with type inference") {
    importCSV("with-header", List(), infer = true)
    assert(vattr[Int]("vertexId") == Seq(0, 1, 2))
    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[Double]("age") == Seq(18.2, 20.3, 50.3))
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
      ('D', 4, 'Oliver', 'Male', 'Troll', NULL),
      (NULL, 5, NULL, NULL, NULL, NULL);
    """)
    connection.close()

    val response = sqlController.importJdbc(
      user,
      JdbcImportRequest(
        table = "jdbc-import-test",
        privacy = "public-read",
        jdbcUrl = url,
        jdbcTable = "subscribers",
        keyColumn = "id",
        columnsToImport = List("n", "id", "name", "race condition", "level")))
    val tablePath = response.id

    run(
      "Import vertices",
      Map(
        "table" -> tablePath,
        "id-attr" -> "new_id"))
    assert(vattr[String]("n") == Seq("A", "B", "C", "D"))
    assert(vattr[Long]("id") == Seq(1, 2, 3, 4))
    assert(vattr[String]("name") == Seq("Beata", "Daniel", "Felix", "Oliver"))
    assert(vattr[String]("race condition") == Seq("Dwarf", "Gnome", "Halfling", "Troll"))
    assert(vattr[Double]("level") == Seq(10.0, 20.0))
    assert(subProject.viewer.vertexAttributes.keySet ==
      Set("new_id", "n", "id", "name", "race condition", "level"))
  }

  test("sql export to parquet + import back (including tuple columns)") {
    val exportPath = "IMPORTGRAPHTEST$/example.parquet"
    graph_util.HadoopFile(exportPath).deleteIfExists

    run("Example Graph")
    val result = await(
      sqlController.exportSQLQueryToParquet(
        user,
        SQLExportToParquetRequest(
          DataFrameSpec.local(
            project = projectName,
            sql = "select name, age, location from vertices"),
          path = exportPath)))
    val response = sqlController.importParquet(
      user,
      ParquetImportRequest(
        table = "csv-import-test",
        privacy = "public-read",
        files = exportPath + "/part*",
        columnsToImport = List("name", "location")))
    val tablePath = response.id
    run(
      "Import vertices",
      Map(
        "table" -> tablePath,
        "id-attr" -> "new_id"))

    assert(vattr[String]("name") == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(vattr[(Double, Double)]("location") == Seq(
      (-33.8674869, 151.2069902),
      (1.352083, 103.819836),
      (40.71448, -74.00598),
      (47.5269674, 19.0323968)))
    graph_util.HadoopFile(exportPath).delete
  }

}
