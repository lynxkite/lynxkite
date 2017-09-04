package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DiscardSegmentationLinksOperationTest extends OperationsTestBase {
  test("Make all segments empty") {
    val bucketed = box("Create example graph")
      .box("Segment by String attribute", Map("name" -> "bucketing", "attr" -> "gender"))
      .box("Make all segments empty", Map("apply_to_project" -> ".bucketing"))
    val bucketing = bucketed.project.segmentation("bucketing")
    assert(bucketing.scalars("!coverage").value == 0)
    assert(bucketing.scalars("!belongsToEdges").value == 0)
    assert(bucketing.scalars("!nonEmpty").value == 0)
    val linked = bucketed
      .box("Define segmentation links from matching attributes",
        Map("apply_to_project" -> ".bucketing", "base_id_attr" -> "gender", "seg_id_attr" ->
          "gender"))
      .project.segmentation("bucketing")
    assert(linked.scalars("!coverage").value == 4)
    assert(linked.scalars("!belongsToEdges").value == 4)
    assert(linked.scalars("!nonEmpty").value == 2)
  }
}
