package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CreateSegmentationFromSQLTest extends OperationsTestBase {
  test("Create segmentation from SQL") {
    run("Create example graph")
    run(
      "Create segmentation from SQL",
      Map("name" -> "sqltest", "sql" -> "select name,location,age from vertices"))
    val seg = project.segmentation("sqltest")
    assert(seg.vertexSet.toSeq.size == 4)
    assert(seg.vertexAttributes("location").rdd.count == 4)
  }
}

