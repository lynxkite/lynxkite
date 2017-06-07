package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class JoinTest extends OperationsTestBase {
  test("Simple vertex attribute join works") {
    val root = box("Create example graph")
    val right = root
      .box("Add constant vertex attribute",
        Map("name" -> "seven", "value" -> "7", "type" -> "Double"))
    val left = root
    val project = box("Join", Map("va" -> "seven", "edges" -> "false"), Seq(left, right)).project

    val values = project.vertexAttributes("seven").rdd.collect.toMap.values.toSeq

    assert(values == Seq(7, 7, 7, 7))
  }
}

