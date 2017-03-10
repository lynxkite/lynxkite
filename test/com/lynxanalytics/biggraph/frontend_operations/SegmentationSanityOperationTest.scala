// These tests check whether various operations leave segmentations
// in a healthy state. So, there is no single operation this class
// is revolving around.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentationSanityOperationTest extends OperationsTestBase {
  test("Segmentation handles belongsTo edges properly") {
    run("Create example graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no")
    )
    run("Add constant vertex attribute",
      Map("name" -> "const", "value" -> "1.0", "type" -> "Double", "apply_to" -> "|seg"))
    run("Merge vertices by attribute", Map(
      "key" -> "const", "aggregate_bottom" -> "", "aggregate_id" -> "",
      "aggregate_size" -> "", "aggregate_top" -> "", "aggregate_const" -> "count",
      "apply_to" -> "|seg"))
    val seg = project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after filtering (which uses pullBack)") {
    run("Create example graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no")
    )
    val seg = project.segmentation("seg")

    run("Filter by attributes", Map("filterva_age" -> "> 10",
      "filterva_gender" -> "", "filterva_id" -> "", "filterva_income" -> "",
      "filterva_location" -> "", "filterva_name" -> "", "filterea_comment" -> "",
      "filterea_weight" -> "", "filterva_segmentation[seg]" -> ""))

    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after filtering on the segmentation side (this uses pullBack)") {
    run("Create example graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no")
    )
    run("Add rank attribute", Map(
      "rankattr" -> "ranking", "keyattr" -> "top", "order" -> "ascending", "apply_to" -> "|seg"))
    run("Filter by attributes", Map(
      "filterva_ranking" -> "> 0", "filterva_bottom" -> "", "filterva_id" -> "",
      "filterva_size" -> "", "filterva_top" -> "", "apply_to" -> "|seg"))
    val seg = project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after merging vertices") {
    run("Create example graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval_size" -> "17", "overlap" -> "no")
    )
    val seg = project.segmentation("seg")
    run("Merge vertices by attribute",
      Map("key" -> "gender")
    )
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
    val belongsTo = seg.belongsTo.rdd.collect
    // 4 edges:
    assert(belongsTo.toSeq.size == 4)
    // Edges coming from 2 vertices on project side:
    assert(belongsTo.map { case (_, (src)) => src.src }.toSeq.distinct.size == 2)
  }
}

