package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.controllers._

class ImportBoxTest extends OperationsTestBase {
  graph_util.PrefixRepository.registerPrefix(
    "IMPORTGRAPHTEST$",
    getClass.getResource("/graph_operations/ImportGraphTest").toString)

  def importCSVFile(
    file: String,
    columns: List[String],
    infer: Boolean,
    limit: Option[Int] = None): TestBox = {
    importBox("Import CSV", Map(
      "filename" -> ("IMPORTGRAPHTEST$/" + file + "/part*"),
      "columns" -> columns.mkString(","),
      "infer" -> (if (infer) "yes" else "no"),
      "limit" -> limit.map(_.toString).getOrElse("")))
  }

  def csvToProject(
    file: String,
    columns: List[String],
    infer: Boolean,
    limit: Option[Int] = None): ProjectEditor = {
    importCSVFile(file, columns, infer, limit)
      .box("Use table as vertices")
      .project
  }

  def vattr[T: reflect.runtime.universe.TypeTag: Ordering](project: ProjectEditor, name: String) =
    project.vertexAttributes(name).runtimeSafeCast[T].rdd.collect.toMap.values.toSeq.sorted

  test("import from CSV without header") {
    val p = csvToProject("testgraph/vertex-data", List("vertexId", "name", "age"), infer = false)
    assert(vattr[String](p, "vertexId") == Seq("0", "1", "2"))
    assert(vattr[String](p, "name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[String](p, "age") == Seq("18.2", "20.3", "50.3"))
  }

  test("import from CSV without header with limit") {
    val p0 = csvToProject(
      "testgraph/vertex-data", List("vertexId", "name", "age"), infer = false, limit = Some(0))
    assert(vattr[String](p0, "vertexId").isEmpty)
    assert(vattr[String](p0, "name").isEmpty)
    assert(vattr[String](p0, "age").isEmpty)

    val p1 = csvToProject(
      "testgraph/vertex-data", List("vertexId", "name", "age"), infer = false, limit = Some(1))
    assert(vattr[String](p1, "vertexId").length == 1)
    assert(vattr[String](p1, "name").length == 1)
    assert(vattr[String](p1, "age").length == 1)

    val p2 = csvToProject(
      "testgraph/vertex-data", List("vertexId", "name", "age"), infer = false, limit = Some(2))
    assert(vattr[String](p2, "vertexId").length == 2)
    assert(vattr[String](p2, "name").length == 2)
    assert(vattr[String](p2, "age").length == 2)

    val p3 = csvToProject(
      "testgraph/vertex-data", List("vertexId", "name", "age"), infer = false, limit = Some(3))
    assert(vattr[String](p3, "vertexId").length == 3)
    assert(vattr[String](p3, "name").length == 3)
    assert(vattr[String](p3, "age").length == 3)
  }

  test("import from CSV with header") {
    val p = csvToProject("with-header", List(), infer = false)
    assert(vattr[String](p, "vertexId") == Seq("0", "1", "2"))
    assert(vattr[String](p, "name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[String](p, "age") == Seq("18.2", "20.3", "50.3"))
  }

  test("import from CSV with type inference") {
    val p = csvToProject("with-header", List(), infer = true)
    assert(vattr[Int](p, "vertexId") == Seq(0, 1, 2))
    assert(vattr[String](p, "name") == Seq("Adam", "Bob", "Eve"))
    assert(vattr[Double](p, "age") == Seq(18.2, 20.3, 50.3))
  }

  test("stale settings warning") {
    val impBox = importCSVFile(
      "testgraph/vertex-data",
      List("vertexId", "name", "age"),
      infer = false)
    val impBoxWithStaleSettings = impBox.changeParameterSettings(Map("infer" -> "yes"))
    val feStatus = impBoxWithStaleSettings.output("table").success
    assert(!feStatus.enabled, "Stale settings should result an error in the output")

    val errorMessage = feStatus.disabledReason
    val errorMessageShouldBe =
      "assertion failed: The following import settings are stale: infer (no). Please click on " +
        "the import button to apply the changed settings or reset the changed settings to " +
        "their original values."
    assert(errorMessage == errorMessageShouldBe,
      "Stale settings error should list the stale parameters and their original value.")
  }

  val sqliteURL = s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"

  def createSqliteSubscribers() = {
    val connection = graph_util.JDBCUtil.getConnection(sqliteURL)
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
  }

  def checkSqliteSubscribers(box: TestBox) = {
    val p = box.box("Use table as vertices", Map("id_attr" -> "new_id")).project
    assert(vattr[String](p, "n") == Seq("A", "B", "C", "D"))
    assert(vattr[Long](p, "id") == Seq(1, 2, 3, 4, 5))
    assert(vattr[String](p, "name") == Seq("Beata", "Daniel", "Felix", "Oliver"))
    assert(vattr[String](p, "race condition") == Seq("Dwarf", "Gnome", "Halfling", "Troll"))
    assert(vattr[Double](p, "level") == Seq(10.0, 20.0))
    assert(p.vertexAttributes.keySet == Set("new_id", "n", "id", "name", "race condition", "level"))
  }

  test("import from SQLite (no partitioning)") {
    createSqliteSubscribers()
    checkSqliteSubscribers(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "subscribers",
      "imported_columns" -> List("n", "id", "name", "race condition", "level").mkString(","))))
  }

  test("import from SQLite (INTEGER partitioning)") {
    createSqliteSubscribers()
    checkSqliteSubscribers(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "subscribers",
      "key_column" -> "id",
      "imported_columns" -> List("n", "id", "name", "race condition", "level").mkString(","))))
  }

  test("import from SQLite (DOUBLE partitioning)") {
    createSqliteSubscribers()
    checkSqliteSubscribers(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "subscribers",
      "key_column" -> "level",
      "imported_columns" -> List("n", "id", "name", "race condition", "level").mkString(","))))
  }

  test("import from SQLite (TEXT partitioning)") {
    createSqliteSubscribers()
    checkSqliteSubscribers(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "subscribers",
      "key_column" -> "name",
      "imported_columns" -> List("n", "id", "name", "race condition", "level").mkString(","))))
  }

  test("import from SQLite (INTEGER partitioning - custom number of partitions)") {
    createSqliteSubscribers()
    checkSqliteSubscribers(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "subscribers",
      "key_column" -> "id",
      "num_partitions" -> "2",
      "imported_columns" -> List("n", "id", "name", "race condition", "level").mkString(","))))
  }

  test("import from SQLite (predicates)") {
    createSqliteSubscribers()
    checkSqliteSubscribers(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "subscribers",
      "partition_predicates" -> "id <= 2,id >= 3",
      "imported_columns" -> List("n", "id", "name", "race condition", "level").mkString(","))))
  }

  def createSqliteNonConventionalTable() = {
    val connection = graph_util.JDBCUtil.getConnection(sqliteURL)
    val statement = connection.createStatement()
    statement.executeUpdate(s"""
      DROP TABLE IF EXISTS 'name with space';
      CREATE TABLE 'name with space' (id INTEGER, 'colname with space' INTEGER, a TEXT);
      INSERT INTO 'name with space' VALUES(1, 1, 'x');""")
    connection.close()
  }

  def checkSqliteNonConventionalTable(box: TestBox) = {
    val p = box.box("Use table as vertices").project
    assert(vattr[Long](p, "id") == Seq(1L))
    assert(vattr[Long](p, "colname with space") == Seq(1L))
    assert(vattr[String](p, "a") == Seq("x"))
  }

  test("import from SQLite (non conventional table name - double quote)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "\"name with space\"",
      "key_column" -> "id",
      "imported_columns" -> "id,colname with space,a")))
  }

  test("import from SQLite (non conventional key column name - double quote)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "\"name with space\"",
      "key_column" -> "\"colname with space\"",
      "imported_columns" -> "id,colname with space,a")))
  }

  test("import from SQLite (non conventional table name - single quote)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "'name with space'",
      "key_column" -> "id",
      "imported_columns" -> "id,colname with space,a")))
  }

  test("import from SQLite (non conventional key column name - single quote)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "'name with space' t",
      "key_column" -> "t.'colname with space'",
      "imported_columns" -> "id,colname with space,a")))
  }

  test("import from SQLite (aliased native sql - no keyColumn)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "(SELECT * FROM 'name with space') t",
      "imported_columns" -> "id,colname with space,a")))
  }

  test("import from SQLite (aliased native sql - with keyColumn)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "(SELECT * FROM 'name with space') t",
      "key_column" -> "t.'colname with space'",
      "imported_columns" -> "id,colname with space,a")))
  }

  test("import from SQLite (non aliased native sql - no keyColumn)") {
    createSqliteNonConventionalTable()
    checkSqliteNonConventionalTable(importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "(SELECT * FROM 'name with space')",
      "imported_columns" -> "id,colname with space,a")))
  }
}
