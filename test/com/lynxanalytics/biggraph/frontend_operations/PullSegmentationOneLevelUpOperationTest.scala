package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopySegmentationOneLevelUpOperationTest extends OperationsTestBase {
  test("Pull segmentation one level up") {
    val vertices = importCSV("copy-segmentation-one-level-up-vertices.csv")
    val connections = importCSV("copy-segmentation-one-level-up-connections.csv")
    val project = vertices.box("Use table as vertices")
      .box("Use table as segmentation", Map(
        "name" -> "segmentation1",
        "base_id_attr" -> "num",
        "base_id_column" -> "base_num",
        "seg_id_column" -> "seg_num"), Seq(connections))
      .box("Use table as segmentation", Map(
        "name" -> "segmentation2",
        "base_id_attr" -> "seg_num",
        "base_id_column" -> "base_num",
        "seg_id_column" -> "seg_num",
        "apply_to_project" -> "|segmentation1"), Seq(connections))
      .box("Pull segmentation one level up", Map(
        "apply_to_project" -> "|segmentation1|segmentation2"))
      .project

    val segmentation1 = project.segmentation("segmentation1")
    val segmentation2 = segmentation1.segmentation("segmentation2")
    assert(project.segmentationNames.contains("segmentation2"))
    val segmentation2Copy = project.segmentation("segmentation2")
    val segmentSizes = segmentation2Copy
      .belongsTo
      .toPairSeq()
      .groupBy(_._1)
      .values
      .map(_.size)
      .toSeq
      .sorted
    assert(segmentSizes == Seq(1, 2, 3, 3, 3))
  }
}
