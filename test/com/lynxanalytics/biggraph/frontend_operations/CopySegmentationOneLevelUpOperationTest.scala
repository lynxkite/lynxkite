package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopySegmentationOneLevelUpOperationTest extends OperationsTestBase {

  test("Copy segmentation one level up") {
    run("Import vertices from CSV files", Map(
      "files" -> ("OPERATIONSTEST$/copy-segmentation-one-level-up-vertices.csv"),
      "header" -> "num",
      "delimiter" -> ",",
      "id-attr" -> "id",
      "omitted" -> "",
      "allow_corrupt_lines" -> "no",
      "filter" -> ""))
    run("Import segmentation from CSV", Map(
      "files" -> "OPERATIONSTEST$/copy-segmentation-one-level-up-connections.csv",
      "header" -> "base_num,seg_num",
      "delimiter" -> ",",
      "omitted" -> "",
      "allow_corrupt_lines" -> "no",
      "filter" -> "",
      "name" -> "segmentation1",
      "attr" -> "num",
      "base-id-field" -> "base_num",
      "seg-id-field" -> "seg_num"))
    val segmentation1 = project.segmentation("segmentation1")
    run("Import segmentation from CSV", Map(
      "files" -> "OPERATIONSTEST$/copy-segmentation-one-level-up-connections.csv",
      "header" -> "base_num,seg_num",
      "delimiter" -> ",",
      "omitted" -> "",
      "allow_corrupt_lines" -> "no",
      "filter" -> "",
      "name" -> "segmentation2",
      "attr" -> "seg_num",
      "base-id-field" -> "base_num",
      "seg-id-field" -> "seg_num"),
      on = segmentation1)
    val segmentation2 = segmentation1.segmentation("segmentation2")

    run("Copy segmentation one level up", on = segmentation2)

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
