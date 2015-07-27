package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class ExportImportOperationTest extends OperationsTestBase {
  test("SQL import & export vertices") {
    run("Example Graph")
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Export vertex attributes to database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "delete" -> "yes",
      "attrs" -> "id,name,age,income,gender"))
    run("Import vertices from a database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "columns" -> "name,age,income,gender",
      "key" -> "id",
      "id-attr" -> "x"))
    val name = project.vertexAttributes("name").runtimeSafeCast[String]
    val income = project.vertexAttributes("income").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(income.rdd.values.collect.toSeq.sorted == Seq("1000.0", "2000.0"))
  }

  test("SQL import & export edges") {
    run("Example Graph")
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Export edge attributes to database", Map(
      "db" -> db,
      "table" -> "example_graph_edges",
      "delete" -> "yes",
      "attrs" -> "weight,comment"))
    run("Import vertices and edges from single database table", Map(
      "db" -> db,
      "table" -> "example_graph_edges",
      "columns" -> "srcVertexId,dstVertexId,weight,comment",
      "key" -> "srcVertexId",
      "src" -> "srcVertexId",
      "dst" -> "dstVertexId"))
    assert(project.vertexSet.rdd.count == 3) // Isolated Joe is lost.
    val weight = project.edgeAttributes("weight").runtimeSafeCast[String]
    val comment = project.edgeAttributes("comment").runtimeSafeCast[String]
    assert(weight.rdd.values.collect.toSeq.sorted == Seq("1.0", "2.0", "3.0", "4.0"))
    assert(comment.rdd.values.collect.toSeq.sorted == Seq("Adam loves Eve", "Bob envies Adam", "Bob loves Eve", "Eve loves Adam"))
  }

  test("CSV import & export vertices") {
    run("Example Graph")
    val path = dataManager.repositoryPath + "/csv-export-test"
    run("Export vertex attributes to file", Map(
      "path" -> path.symbolicName,
      "link" -> "link",
      "attrs" -> "id,name,age,income,gender",
      "format" -> "CSV"))
    val header = (path + "/header").readAsString
    run("Import vertices from CSV files", Map(
      "files" -> (path + "/data/*").symbolicName,
      "header" -> header,
      "delimiter" -> ",",
      "filter" -> "",
      "omitted" -> "",
      "id-attr" -> "x"))
    val name = project.vertexAttributes("name").runtimeSafeCast[String]
    val income = project.vertexAttributes("income").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(income.rdd.values.collect.toSeq.sorted == Seq("", "", "1000.0", "2000.0"))
  }

}
