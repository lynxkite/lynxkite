package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ImportSegmentationOperationTest extends OperationsTestBase {
  test("Import segmentation for example graph") {
    run("Example Graph")
    run("Import segmentation from CSV", Map(
      "files" -> "OPERATIONSTEST$/segmentation-for-example-graph.csv",
      "header" -> "base_name,seg_name,ignored_attr",
      "delimiter" -> ",",
      "omitted" -> "",
      "allow_corrupt_lines" -> "no",
      "filter" -> "",
      "name" -> "imported",
      "attr" -> "name",
      "base-id-field" -> "base_name",
      "seg-id-field" -> "seg_name"))
    val seg = project.segmentation("imported")
    val belongsTo = seg.belongsTo.toPairSeq
    assert(belongsTo.size == 5)
    val segNames = seg.vertexAttributes("seg_name").runtimeSafeCast[String].rdd.collect.toSeq
    assert(segNames.length == 3)
    val segMap = {
      val nameMap = segNames.toMap
      belongsTo.map { case (vid, sid) => vid -> nameMap(sid) }
    }
    assert(segMap == Seq(0 -> "Good", 1 -> "Naughty", 2 -> "Good", 3 -> "Retired", 3 -> "Naughty"))
  }
}
