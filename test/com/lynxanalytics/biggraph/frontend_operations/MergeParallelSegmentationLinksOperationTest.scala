package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MergeParallelSegmentationLinksOperationTest extends OperationsTestBase {
  test("Merge parallel segmentation links") {
    run("Create example graph")
    run("Segment by string attribute", Map("name" -> "bucketing", "attr" -> "gender"))
    val bucketing = project.segmentation("bucketing")
    run("Merge vertices by attribute",
      Map("key" -> "gender", "aggregate_gender" -> "", "aggregate_id" -> "",
        "aggregate_income" -> "average", "aggregate_location" -> "", "aggregate_name" -> ""))
    run("Merge parallel segmentation links", on = bucketing)
    assert(bucketing.scalars("!coverage").value == 2)
    assert(bucketing.scalars("!belongsToEdges").value == 2)
    assert(bucketing.scalars("!nonEmpty").value == 2)
  }
}
