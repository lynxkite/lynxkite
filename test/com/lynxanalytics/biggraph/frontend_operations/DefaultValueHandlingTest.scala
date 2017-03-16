package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class DefaultValueHandlingTest extends OperationsTestBase {
  test("Invoking PageRank with and without default params yields the same result") {
    val base = box("Create enhanced example graph")
    val resultIfNoParams = base
      .box("Compute PageRank", Map())
      .project
      .vertexAttributes("page_rank")
      .rdd.collect.toMap
    val resultIfYesParams = base
      .box("Compute PageRank", Map("iterations" -> "5", "damping" -> "0.85"))
      .project
      .vertexAttributes("page_rank")
      .rdd.collect.toMap
    assert(resultIfNoParams == resultIfYesParams)
  }
}

