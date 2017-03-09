package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DiscardSegmentationLinksOperationTest extends OperationsTestBase {
  test("Discard segmentation links") {
    run("Example Graph")
    run("Segment by string attribute", Map("name" -> "bucketing", "attr" -> "gender"))
    val bucketing = project.segmentation("bucketing")
    run("Discard segmentation links", on = bucketing)
    assert(bucketing.scalars("!coverage").value == 0)
    assert(bucketing.scalars("!belongsToEdges").value == 0)
    assert(bucketing.scalars("!nonEmpty").value == 0)
    run("Define segmentation links from matching attributes",
      Map("base-id-attr" -> "gender", "seg-id-attr" -> "gender"),
      on = bucketing)
    assert(bucketing.scalars("!coverage").value == 4)
    assert(bucketing.scalars("!belongsToEdges").value == 4)
    assert(bucketing.scalars("!nonEmpty").value == 2)
  }
}
