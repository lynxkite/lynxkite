package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class GrowSegmentationTest extends OperationsTestBase {
  test("Grow segmentation - in-neighbors") {
    run("Example Graph")
    run("Copy graph into a segmentation", Map("name" -> "seg"))
    val seg = project.segmentation("seg")
    run("Grow segmentation",
      Map("direction" -> "in-neighbors"),
      on = seg)
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (0, 0), (1, 0), (0, 1), (1, 1), (2, 0), (2, 1), (3, 3), (2, 2)))
  }

  test("Grow segmentation - out-neighbors") {
    run("Example Graph")
    run("Copy graph into a segmentation", Map("name" -> "seg"))
    val seg = project.segmentation("seg")
    run("Grow segmentation",
      Map("direction" -> "out-neighbors"),
      on = seg)
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (0, 0), (1, 0), (0, 1), (1, 1), (0, 2), (1, 2), (3, 3), (2, 2)))
  }

  test("Grow segmentation - all neighbors") {
    run("Example Graph")
    run("Copy graph into a segmentation", Map("name" -> "seg"))
    val seg = project.segmentation("seg")
    run("Grow segmentation",
      Map("direction" -> "all neighbors"),
      on = seg)
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (2, 1), (0, 0), (1, 0), (0, 1), (2, 0), (1, 1), (0, 2), (1, 2), (3, 3), (2, 2)))
  }

  test("Grow segmentation - symmetric neighbors") {
    run("Example Graph")
    run("Copy graph into a segmentation", Map("name" -> "seg"))
    val seg = project.segmentation("seg")
    run("Grow segmentation",
      Map("direction" -> "symmetric neighbors"),
      on = seg)
    val newSeg = project.segmentation("seg")
    assert(newSeg.belongsTo.rdd.map { case (_, e) => e.src -> e.dst }.collect.toSeq == Seq(
      (0, 0), (1, 0), (0, 1), (1, 1), (3, 3), (2, 2)))
  }
}
