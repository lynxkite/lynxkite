package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyGraphIntoASegmentationOperationTest extends OperationsTestBase {
  test("Use base project as segmentation") {
    val project = box("Create example graph")
      .box("Use base project as segmentation", Map("name" -> "seg")).project
    val seg = project.segmentation("seg")
    assert(seg.belongsTo.toIdPairSeq == Seq((0, (0, 0)), (1, (1, 1)), (2, (2, 2)), (3, (3, 3))))
    val name = seg.vertexAttributes("name").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
  }

  test("Use base project as segmentation discards sub-segmentations") {
    val project = box("Create example graph")
      .box("Use base project as segmentation", Map("name" -> "seg1"))
      .box("Use base project as segmentation", Map("name" -> "seg2")).project
    val seg2 = project.segmentation("seg2")
    assert(seg2.segmentationNames.isEmpty)
  }

}

