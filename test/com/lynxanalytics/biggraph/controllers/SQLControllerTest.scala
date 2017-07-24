package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.frontend_operations.OperationsTestBase
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.serving

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SQLControllerTest extends BigGraphControllerTestBase with OperationsTestBase {
  override val user = serving.User.fake
  val sqlController = new SQLController(this, ops = null)
  val resourceDir = getClass.getResource("/graph_operations/ImportGraphTest").toString
  graph_util.PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", resourceDir)

  def await[T](f: concurrent.Future[T]): T =
    concurrent.Await.result(f, concurrent.duration.Duration.Inf)

  private def SQLResultToStrings(data: List[List[DynamicValue]]) = {
    data.map { case l: List[DynamicValue] => l.map { dv => dv.string } }
  }

  test("global sql on vertices") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select name from `people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql on edges") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select src_name, dst_name from `people.edges` where edge_weight = 1"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("src_name", "String"), SQLColumn("dst_name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam", "Eve")))
  }

  test("global sql on edge_attributes") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select comment from `people.edge_attributes` where weight = 1"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("comment", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam loves Eve")))
  }

  test("global sql on segmentation's belongs_to") {
    val egSeg = box("Create example graph").box("Segment by String attribute",
      Map("name" -> "gender_seg", "attr" -> "gender"))
    egSeg.snapshotOutput("test_dir/people", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select base_gender from `people.gender_seg.belongs_to` where segment_size = 1"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("base_gender", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Female")))
  }

  test("global sql on vertices from root directory") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "",
        sql = "select name from `test_dir/people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql on vertices with attribute name quoted with backticks") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "project")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select `name` from `people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql with upper case snapshot name") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/PEOPLE", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select name from `PEOPLE.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  // This should work, whether we choose to implement case sensitive or case insensitive
  // SQL in the future.
  test("global sql with upper case attribute name") {
    val eg = box("Create example graph").box("Rename vertex attribute",
      Map("before" -> "name", "after" -> "NAME"))
    eg.snapshotOutput("test_dir/people", "project")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      DataFrameSpec.global(directory = "test_dir",
        sql = "select NAME from `people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("NAME", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  // TODO: Depends on https://app.asana.com/0/194476945034319/354006072569797.
  /*
  test("list project tables") {
    createProject(name = "example1")
    createDirectory(name = "dir")
    createProject(name = "dir/example2")
    run("Create example graph", on = "dir/example2")
    run(
      "Segment by Double attribute",
      params = Map(
        "name" -> "bucketing",
        "attr" -> "age",
        "interval_size" -> "0.1",
        "overlap" -> "no"),
      on = "dir/example2")
    run(
      "Segment by Double attribute",
      params = Map(
        "name" -> "vertices", // This segmentation is named vertices to test extremes.
        "attr" -> "age",
        "interval_size" -> "0.1",
        "overlap" -> "no"),
      on = "dir/example2")

    // List tables and segmentation of a project.
    val res1 = await(
      sqlController.getTableBrowserNodes(
        user, TableBrowserNodeRequest(path = "dir/example2")))
    assert(List(
      TableBrowserNode("dir/example2.edges", "edges", "table"),
      TableBrowserNode("dir/example2.edge_attributes", "edge_attributes", "table"),
      TableBrowserNode("dir/example2.vertices", "vertices", "table"),
      TableBrowserNode("dir/example2.bucketing", "bucketing", "segmentation"),
      TableBrowserNode("dir/example2.vertices", "vertices", "segmentation")) == res1.list)
  }

  def checkExampleGraphColumns(req: TableBrowserNodeRequest, idTypeOverride: String = "ID") = {
    val res = await(sqlController.getTableBrowserNodes(user, req))
    val expected = List(
      TableBrowserNode("", "age", "column", "Double"),
      TableBrowserNode("", "income", "column", "Double"),
      TableBrowserNode("", "id", "column", idTypeOverride),
      TableBrowserNode("", "location", "column", "(Double, Double)"),
      TableBrowserNode("", "name", "column", "String"),
      TableBrowserNode("", "gender", "column", "String"))

    assert(expected.sortBy(_.name) == res.list.sortBy(_.name))
  }

  test("list project table columns") {
    run("Create example graph")
    checkExampleGraphColumns(
      TableBrowserNodeRequest(
        path = "Test_Project.vertices",
        isImplicitTable = true))
  }

  test("list view columns") {
    run("Create example graph")
    sqlController.createViewDFSpec(
      user,
      SQLCreateViewRequest(
        name = "view1",
        privacy = "public-write",
        overwrite = false,
        dfSpec = DataFrameSpec(
          directory = Some(""),
          project = None,
          sql = "SELECT * FROM `Test_Project.vertices`")))

    // Check that columns of view are listed:
    checkExampleGraphColumns(
      TableBrowserNodeRequest(path = "view1"),
      idTypeOverride = "Long")
  }

  test("list table columns") {
    run("Create example graph")
    await(sqlController.exportSQLQueryToTable(
      user,
      SQLExportToTableRequest(
        table = "table1",
        privacy = "public-read",
        overwrite = false,
        dfSpec = DataFrameSpec(
          directory = Some(""),
          project = None,
          sql = "SELECT * FROM `Test_Project.vertices`"))))

    // Check that columns of view are listed:
    checkExampleGraphColumns(
      TableBrowserNodeRequest(path = "table1"),
      idTypeOverride = "Long")
  }
  */
}
