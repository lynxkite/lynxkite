// These tests check whether various operations leave segmentations
// in a healthy state. So, there is no single operation this class
// is revolving around.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SegmentationSanityOperationTest extends OperationsTestBase {
  test("Segmentation handles belongsTo edges properly") {
    run("Example Graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval-size" -> "17", "overlap" -> "no")
    )
    run("Add constant vertex attribute",
      Map("name" -> "const", "value" -> "1.0", "type" -> "Double", "apply_to" -> "|seg"))
    run("Merge vertices by attribute", Map(
      "key" -> "const", "aggregate-bottom" -> "", "aggregate-id" -> "",
      "aggregate-size" -> "", "aggregate-top" -> "", "aggregate-const" -> "count",
      "apply_to" -> "|seg"))
    val seg = project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after filtering (which uses pullBack)") {
    run("Example Graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval-size" -> "17", "overlap" -> "no")
    )
    val seg = project.segmentation("seg")

    run("Filter by attributes", Map("filterva-age" -> "> 10",
      "filterva-gender" -> "", "filterva-id" -> "", "filterva-income" -> "",
      "filterva-location" -> "", "filterva-name" -> "", "filterea-comment" -> "",
      "filterea-weight" -> "", "filterva-segmentation[seg]" -> ""))

    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after filtering on the segmentation side (this uses pullBack)") {
    run("Example Graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval-size" -> "17", "overlap" -> "no")
    )
    run("Add rank attribute", Map(
      "rankattr" -> "ranking", "keyattr" -> "top", "order" -> "ascending", "apply_to" -> "|seg"))
    run("Filter by attributes", Map(
      "filterva-ranking" -> "> 0", "filterva-bottom" -> "", "filterva-id" -> "",
      "filterva-size" -> "", "filterva-top" -> "", "apply_to" -> "|seg"))
    val seg = project.segmentation("seg")
    assert(seg.vertexSet.gUID == seg.belongsTo.dstVertexSet.gUID)
  }

  test("Segmentation stays sane after merging vertices") {
    run("Example Graph")
    run("Segment by double attribute",
      Map("name" -> "seg", "attr" -> "age", "interval-size" -> "17", "overlap" -> "no")
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

