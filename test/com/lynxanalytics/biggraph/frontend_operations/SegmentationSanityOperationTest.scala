// These tests check whether various operations leave segmentations
// in a healthy state. So, there is no single operation this class
// is revolving around.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentationSanityOperationTest extends OperationsTestBase {
  test("Segmentation handles belongsTo edges properly") {
    val project = box("Create example graph")
      .box("Segment by Double attribute",
        Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no"))
      .box("Add constant vertex attribute",
        Map("name" -> "const", "value" -> "1.0", "type" -> "Double", "apply_to_project" -> "|seg"))
      .box("Merge vertices by attribute",
        Map("key" -> "const", "aggregate_bottom" -> "", "aggregate_id" -> "",
          "aggregate_size" -> "", "aggregate_top" -> "", "aggregate_const" -> "count",
          "apply_to_project" -> "|seg")).project
    val seg = project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after filtering (which uses pullBack)") {
    val seg = box("Create example graph")
      .box("Segment by Double attribute",
        Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no"))
      .box("Filter by attributes", Map("filterva_age" -> "> 10",
        "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
        "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
        "filterea_weight" -> "", "filterva_segmentation[seg]" -> ""))
      .project.segmentation("seg")

    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after filtering on the segmentation side (this uses pullBack)") {
    val seg = box("Create example graph")
      .box("Segment by Double attribute",
        Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no"))
      .box("Add rank attribute", Map(
        "rankattr" -> "ranking", "keyattr" -> "top", "order" -> "ascending", "apply_to_project" -> "|seg"))
      .box("Filter by attributes", Map(
        "filterva_ranking" -> "> 0", "filterva_bottom" -> "", "filterva_id" -> "",
        "filterva_size" -> "", "filterva_top" -> "", "apply_to_project" -> "|seg"))
      .project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after merging vertices") {
    val project = box("Create example graph")
      .box("Segment by Double attribute",
        Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no"))
      .box("Merge vertices by attribute", Map("key" -> "gender")).project
    val seg = project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
    val belongsTo = seg.belongsTo.rdd.collect
    // 4 edges:
    assert(belongsTo.toSeq.size == 4)
    // Edges coming from 2 vertices on project side:
    assert(belongsTo.map { case (_, (src)) => src.src }.toSeq.distinct.size == 2)
  }
}

