package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyEdgesToBaseProjectTest extends OperationsTestBase {
  test("Copy edges to base graph") {
    val project = box("Create vertices", Map("size" -> "5"))
      .box("Use base graph as segmentation", Map("name" -> "copy"))
      .box(
        "Create random edges",
        Map("degree" -> "5", "seed" -> "0", "apply_to_graph" -> ".copy"))
      .box(
        "Add constant edge attribute",
        Map("name" -> "const", "value" -> "1", "type" -> "Double", "apply_to_graph" -> ".copy"))
      .box("Copy edges to base graph", Map("apply_to_graph" -> ".copy"))
      .project
    assert(project.edgeBundle.toPairSeq.size == 21)
    assert(project.edgeAttributes("const").rdd.count == 21)
    val copy = project.segmentation("copy")
    assert(copy.edgeBundle.toPairSeq.size == 21)
  }
}
