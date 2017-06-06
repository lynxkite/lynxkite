package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_util.HadoopFile

class ExportBoxTest extends OperationsTestBase {
  graph_util.PrefixRepository.registerPrefix(
    "EXPORTTEST$",
    getClass.getResource("/graph_operations/ExportTest").toString)

  def vattr(project: ProjectEditor, name: String) =
    project.vertexAttributes(name).runtimeSafeCast[String].rdd.values.collect.toSeq.sorted

  def importTestFile = importBox("Import CSV", Map(
    "filename" -> "EXPORTTEST$/table_input.csv",
    "columns" -> "",
    "infer" -> "no"))

  def checkResult(importedAgain: ProjectEditor) = {
    assert(vattr(importedAgain, "name") == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(vattr(importedAgain, "favorite_sport") == Seq(
      "American football", "Basketball", "Football", "Solitaire"))
  }

  test("Export to CSV") {
    val path = "EXPORTTEST$/tmp/exportedCSV"
    val exportResult = importTestFile.box("Export to CSV", Map("path" -> path)).exportResult
    dataManager.get(exportResult)
    val importedAgain = importBox("Import CSV", Map(
      "filename" -> path,
      "columns" -> "",
      "infer" -> "no")).
      box("Import vertices").project
    checkResult(importedAgain)

    val exported = HadoopFile(path)
    exported.delete()
  }

  test("Export to structured file (JSON)") {
    val path = "EXPORTTEST$/tmp/exportedJSON"
    val exportResult = importTestFile.box("Export to JSON", Map("path" -> path)).exportResult
    dataManager.get(exportResult)
    val importedAgain = importBox("Import JSON", Map("filename" -> path)).box("Import vertices").project
    checkResult(importedAgain)

    val exported = HadoopFile(path)
    exported.delete()
  }

  test("Export to JDBC") {
    val sqliteURL =
      s"jdbc:sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    // Seems like this part is needed for registering the jdbc driver.
    val connection = graph_util.JDBCUtil.getConnection(sqliteURL)
    connection.close()
    val exportResult = importTestFile.box("Export to JDBC", Map(
      "jdbcUrl" -> sqliteURL, "table" -> "hobbies",
      "mode" -> "Drop the table if it already exists")).exportResult
    dataManager.get(exportResult)
    val importedAgain = importBox("Import JDBC", Map(
      "jdbc_url" -> sqliteURL,
      "jdbc_table" -> "hobbies",
      "imported_columns" -> "")).
      box("Import vertices").project
    checkResult(importedAgain)
  }
}
