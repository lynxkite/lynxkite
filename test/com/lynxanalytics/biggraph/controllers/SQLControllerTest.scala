package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.frontend_operations.OperationsTestBase
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.serving

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SQLControllerTest extends BigGraphControllerTestBase with OperationsTestBase {
  override val user = serving.User.singleuser
  val sqlController = new SQLController(this, ops = null)
  val workspaceController = new WorkspaceController(this)
  val resourceDir = getClass.getResource("/graph_operations/ImportGraphTest").toString
  graph_util.PrefixRepository.registerPrefix("IMPORTGRAPHTEST$", resourceDir)

  def await[T](f: concurrent.Future[T]): T =
    concurrent.Await.result(f, concurrent.duration.Duration.Inf)

  private def SQLResultToStrings(data: List[List[DynamicValue]]) = {
    data.map { case l: List[DynamicValue] => l.map { dv => dv.string } }
  }

  test("global sql on vertices") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select name from `people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql on edges") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select src_name, dst_name from `people.edges` where edge_weight = 1"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("src_name", "String"), SQLColumn("dst_name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam", "Eve")))
  }

  test("global sql on edge_attributes") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select comment from `people.edge_attributes` where weight = 1"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("comment", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam loves Eve")))
  }

  test("global sql with subquery on edges") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = """select edge_comment
                 from (select edge_comment, edge_weight, avg(dst_age) avg_dst_age
                       from `people.edges`
                       group by 1,2)
                 where avg_dst_age > 20"""),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("edge_comment", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings.flatten.sorted == List(List("Bob envies Adam"), List("Eve loves Adam")).flatten.sorted)
  }

  test("global sql on segmentation's belongs_to") {
    val egSeg = box("Create example graph").box(
      "Segment by String attribute",
      Map("name" -> "gender_seg", "attr" -> "gender"))
    egSeg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select base_gender from `people.gender_seg.belongs_to` where segment_size = 1"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("base_gender", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Female")))
  }

  test("global sql on vertices from root directory") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "",
        sql = "select name from `test_dir/people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql on vertices with attribute name quoted with backticks") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")
    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select `name` from `people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql with upper case snapshot name") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/PEOPLE", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select name from `PEOPLE.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("global sql with dot in the folder name") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/firstname.lastname@lynx.com/eg", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select name from `firstname.lastname@lynx.com/eg.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("name", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  // This should work, whether we choose to implement case sensitive or case insensitive
  // SQL in the future.
  test("global sql with upper case attribute name") {
    val eg = box("Create example graph").box(
      "Rename vertex attributes",
      Map("change_name" -> "NAME"))
    eg.snapshotOutput("test_dir/people", "graph")

    val result = await(sqlController.runSQLQuery(user, SQLQueryRequest(
      TableSpec.global(
        directory = "test_dir",
        sql = "select NAME from `people.vertices` where age < 40"),
      maxRows = 10)))

    assert(result.header == List(SQLColumn("NAME", "String")))
    val resultStrings = SQLResultToStrings(result.data)
    assert(resultStrings == List(List("Adam"), List("Eve"), List("Isolated Joe")))
  }

  test("Export result of global SQL box") {
    val eg = box("Create example graph")
    eg.snapshotOutput("test_dir/people", "graph")

    val dfSpec = TableSpec.global(
      directory = "test_dir",
      sql = "select name from `people.vertices` where age < 40")
    val path = "IMPORTGRAPHTEST$/global-sql-export-test.csv"
    val request = SQLExportToCSVRequest(dfSpec, path, true, ",", "\"")

    val exportTarget = HadoopFile(path)
    exportTarget.deleteIfExists()

    val result = await(sqlController.exportSQLQueryToCSV(user, request))

    exportTarget.deleteIfExists()

  }

  def getMeta(box: TestBox) = {
    val name = "tmp"
    workspaceController.createWorkspace(user, CreateWorkspaceRequest(name))
    workspaceController.setWorkspace(user, SetWorkspaceRequest(WorkspaceReference(name), box.workspace))
    GetOperationMetaRequest(WorkspaceReference(name), box.realBox.id)
  }

  test("list project tables") {
    val project = box("Create example graph")
      .box(
        "Segment by numeric attribute",
        Map(
          "name" -> "bucketing",
          "attr" -> "age",
          "interval_size" -> "0.1",
          "overlap" -> "no"))
      .box(
        "Segment by numeric attribute",
        Map(
          "name" -> "vertices", // This segmentation is named vertices to test extremes.
          "attr" -> "age",
          "interval_size" -> "0.1",
          "overlap" -> "no"))
      .box("SQL1")

    // List tables and segmentation of a project.
    val res = sqlController.getTableBrowserNodesForBox(workspaceController)(
      user, TableBrowserNodeForBoxRequest(getMeta(project), ""))
    assert(res.list == List(
      "bucketing.belongs_to",
      "bucketing.graph_attributes",
      "bucketing.vertices",
      "edge_attributes",
      "edges",
      "graph_attributes",
      "input.bucketing.belongs_to",
      "input.bucketing.graph_attributes",
      "input.bucketing.vertices",
      "input.edge_attributes",
      "input.edges",
      "input.graph_attributes",
      "input.vertices",
      "input.vertices.belongs_to",
      "input.vertices.graph_attributes",
      "input.vertices.vertices",
      "vertices",
      "vertices.belongs_to",
      "vertices.graph_attributes",
      "vertices.vertices").map(t => TableBrowserNode(t, t, "table")))
  }

  def checkExampleGraphColumns(response: TableBrowserNodeResponse) = {
    val expected = List(
      TableBrowserNode("", "age", "column", "Double"),
      TableBrowserNode("", "income", "column", "Double"),
      TableBrowserNode("", "id", "column", "String"),
      TableBrowserNode("", "location", "column", "Vector[Double]"),
      TableBrowserNode("", "name", "column", "String"),
      TableBrowserNode("", "gender", "column", "String"))
    assert(expected.sortBy(_.name) == response.list.sortBy(_.name))
  }

  test("list project table columns") {
    val project = box("Create example graph").box("SQL1")
    val res = sqlController.getTableBrowserNodesForBox(workspaceController)(
      user, TableBrowserNodeForBoxRequest(getMeta(project), "vertices"))
    checkExampleGraphColumns(res)
  }

  /*
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
