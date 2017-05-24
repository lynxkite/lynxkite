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

  test("Export to CSV") {
    val path = "EXPORTTEST$/tmp/exported"
    val exportResult = importBox("Import CSV", Map(
      "filename" -> "EXPORTTEST$/table_input.csv",
      "columns" -> "",
      "infer" -> "no"))
      .box("Export to CSV",
        Map("path" -> path)
      ).exportResult
    dataManager.get(exportResult)
    val importedAgain = importBox("Import CSV", Map(
      "filename" -> path,
      "columns" -> "",
      "infer" -> "no")).
      box("Import vertices").project

    assert(vattr(importedAgain, "name") == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(vattr(importedAgain, "favorite_sport") == Seq(
      "American football", "Basketball", "Football", "Solitaire"))

    val exported = HadoopFile(path)
    exported.delete()

  }
}
