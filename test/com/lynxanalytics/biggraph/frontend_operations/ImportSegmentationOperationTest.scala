package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_operations

class ImportSegmentationOperationTest extends OperationsTestBase {

  def getTable = {
    importSeq(Seq("base_name", "seg_name", "base_id"), Seq(
      ("Adam", "Good", 0L),
      ("Eve", "Naughty", 1L),
      ("Bob", "Good", 2L),
      ("Isolated Joe", "Naughty", 3L),
      ("Isolated Joe", "Retired", 3L)))
  }

  test("Use table as segmentation for example graph") {
    val project = box("Create example graph")
      .box("Use table as segmentation",
        Map(
          "name" -> "imported",
          "base_id_attr" -> "name",
          "base_id_column" -> "base_name",
          "seg_id_column" -> "seg_name"),
        Seq(getTable))
      .project
    checkAssertions(project)
  }

  test("Use table as segmentation links for example graph") {
    val project = box("Create example graph")
      .box("Use table as segmentation",
        Map(
          "name" -> "imported",
          "base_id_attr" -> "name",
          "base_id_column" -> "base_name",
          "seg_id_column" -> "seg_name"),
        Seq(getTable))
      .box("Use table as segmentation links", Map(
        "base_id_attr" -> "name",
        "seg_id_attr" -> "seg_name",
        "base_id_column" -> "base_name",
        "seg_id_column" -> "seg_name",
        "apply_to_project" -> ".imported"),
        Seq(getTable))
      .project
    checkAssertions(project)
  }

  def checkAssertions(project: controllers.ProjectEditor) = {
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

  test("Use table as segmentation for example graph by Long ID") {
    val project = box("Create example graph")
      .box("Use table as segmentation", Map(
        "name" -> "imported",
        "base_id_attr" -> "id",
        "base_id_column" -> "base_id",
        "seg_id_column" -> "seg_name"),
        Seq(getTable))
      .project
    checkAssertions(project)
  }
}
