package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyGraphIntoASegmentationOperationTest extends OperationsTestBase {
  test("Copy graph into a segmentation") {
    run("Create example graph")
    run("Copy graph into a segmentation", Map("name" -> "seg"))
    val seg = project.segmentation("seg")
    assert(seg.belongsTo.toIdPairSeq == Seq((0, (0, 0)), (1, (1, 1)), (2, (2, 2)), (3, (3, 3))))
    val name = seg.vertexAttributes("name").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
  }

  test("Copy graph into a segmentation discards sub-segmentations") {
    run("Create example graph")
    run("Copy graph into a segmentation", Map("name" -> "seg1"))
    run("Copy graph into a segmentation", Map("name" -> "seg2"))
    val seg2 = project.segmentation("seg2")
    assert(seg2.segmentationNames.isEmpty)
  }

}

