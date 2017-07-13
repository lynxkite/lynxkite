package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeParallelSegmentationLinksOperationTest extends OperationsTestBase {
  test("Merge parallel segmentation links") {
    val bucketing = box("Create example graph")
      .box("Segment by String attribute", Map("name" -> "bucketing", "attr" -> "gender"))
      .box("Merge vertices by attribute",
        Map("key" -> "gender", "aggregate_gender" -> "", "aggregate_id" -> "",
          "aggregate_income" -> "average", "aggregate_location" -> "", "aggregate_name" -> ""))
      .box("Merge parallel segmentation links", Map("apply_to_project" -> ".bucketing"))
      .project.segmentation("bucketing")
    assert(bucketing.scalars("!coverage").value == 2)
    assert(bucketing.scalars("!belongsToEdges").value == 2)
    assert(bucketing.scalars("!nonEmpty").value == 2)
  }
}
