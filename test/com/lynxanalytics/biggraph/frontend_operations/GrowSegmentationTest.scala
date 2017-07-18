package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class GrowSegmentationTest extends OperationsTestBase {
  test("Grow segmentation - in-neighbors") {
    val project = box("Create example graph")
      .box("Use base project as segmentation", Map("name" -> "seg"))
      .box("Grow segmentation",
        Map("direction" -> "in-neighbors", "apply_to_project" -> ".seg"))
      .project
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (0, 0), (1, 0), (0, 1), (1, 1), (2, 0), (2, 1), (3, 3), (2, 2)))
  }

  test("Grow segmentation - out-neighbors") {
    val project = box("Create example graph")
      .box("Use base project as segmentation", Map("name" -> "seg"))
      .box("Grow segmentation",
        Map("direction" -> "out-neighbors", "apply_to_project" -> ".seg"))
      .project
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (0, 0), (1, 0), (0, 1), (1, 1), (0, 2), (1, 2), (3, 3), (2, 2)))
  }

  test("Grow segmentation - all neighbors") {
    val project = box("Create example graph")
      .box("Use base project as segmentation", Map("name" -> "seg"))
      .box("Grow segmentation",
        Map("direction" -> "all neighbors", "apply_to_project" -> ".seg"))
      .project
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (2, 1), (0, 0), (1, 0), (0, 1), (2, 0), (1, 1), (0, 2), (1, 2), (3, 3), (2, 2)))
  }

  test("Grow segmentation - symmetric neighbors") {
    val project = box("Create example graph")
      .box("Use base project as segmentation", Map("name" -> "seg"))
      .box("Grow segmentation",
        Map("direction" -> "symmetric neighbors", "apply_to_project" -> ".seg"))
      .project
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (0, 0), (1, 0), (0, 1), (1, 1), (3, 3), (2, 2)))
  }
}
