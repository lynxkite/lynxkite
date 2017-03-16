package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeParallelSegmentationLinksOperationTest extends OperationsTestBase {
  test("Merge parallel segmentation links") {
    val base = box("Create example graph")
      .box("Segment by string attribute", Map("name" -> "bucketing", "attr" -> "gender"))
      .box("Merge vertices by attribute",
        Map("key" -> "gender", "aggregate_gender" -> "", "aggregate_id" -> "",
          "aggregate_income" -> "average", "aggregate_location" -> "", "aggregate_name" -> ""))
    // TODO: fix this
    assert(false)
    //    val bucketing = base.project.segmentation("bucketing")
    //    base.box("Merge parallel segmentation links", Map("apply_to_project" -> "|bucketing"))
    //      .enforceComputation
    //    assert(bucketing.scalars("!coverage").value == 2)
    //    assert(bucketing.scalars("!belongsToEdges").value == 2)
    //    assert(bucketing.scalars("!nonEmpty").value == 2)
  }
}
