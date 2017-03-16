package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers.DirectoryEntry
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.table.TableImport

class ImportSegmentationOperationTest extends OperationsTestBase {

  test("Compiles and fails") {
    assert(false)
  }

  /*
  def getTable = {
    val rows = Seq(
      ("Adam", "Good", 0L),
      ("Eve", "Naughty", 1L),
      ("Bob", "Good", 2L),
      ("Isolated Joe", "Naughty", 3L),
      ("Isolated Joe", "Retired", 3L))
    val sql = cleanDataManager.newSQLContext
    val dataFrame = sql.createDataFrame(rows).toDF("base_name", "seg_name", "base_id")
    val table = TableImport.importDataFrameAsync(dataFrame)
    DirectoryEntry.fromName("test_segmentation_import").remove()
    val tableFrame = DirectoryEntry.fromName("test_segmentation_import").asNewTableFrame(table, "")
    s"!checkpoint(${tableFrame.checkpoint}, ${tableFrame.name})|vertices"
  }

  test("Import segmentation for example graph") {
    run("Create example graph")
    run("Import segmentation", Map(
      "table" -> getTable,
      "name" -> "imported",
      "base_id_attr" -> "name",
      "base_id_column" -> "base_name",
      "seg_id_column" -> "seg_name"))
    checkAssertions()
  }

  test("Import segmentation links for example graph") {
    run("Create example graph")
    run("Import segmentation", Map(
      "table" -> getTable,
      "name" -> "imported",
      "base_id_attr" -> "name",
      "base_id_column" -> "base_name",
      "seg_id_column" -> "seg_name"))
    val seg = project.segmentation("imported")
    // Overwrite the links by importing them for the existing base+segmentation.
    run("Import segmentation links", Map(
      "table" -> getTable,
      "base_id_attr" -> "name",
      "seg_id_attr" -> "seg_name",
      "base_id_column" -> "base_name",
      "seg_id_column" -> "seg_name",
      "apply_to_project" -> "|imported"))
    checkAssertions()
  }

  def checkAssertions() = {
    val seg = project.segmentation("imported")
    val belongsTo = seg.belongsTo.toPairSeq
    assert(belongsTo.size == 5)
    val segNames = seg.vertexAttributes("seg_name").runtimeSafeCast[String].rdd.collect.toSeq
    assert(segNames.length == 3)
    val segMap = {
      val nameMap = segNames.toMap
      belongsTo.map { case (vid, sid) => vid -> nameMap(sid) }
    }
    val v = project.vertexAttributes("id").rdd.keys.collect.toSeq
    assert(segMap == Seq(v(0) -> "Good", v(1) -> "Naughty", v(2) -> "Good", v(3) -> "Naughty", v(3) -> "Retired"))
  }

  test("Import segmentation for example graph by Long ID") {
    run("Create example graph")
    run("Import segmentation", Map(
      "table" -> getTable,
      "name" -> "imported",
      "base_id_attr" -> "id",
      "base_id_column" -> "base_id",
      "seg_id_column" -> "seg_name"))
    checkAssertions()
  }
  */
}
