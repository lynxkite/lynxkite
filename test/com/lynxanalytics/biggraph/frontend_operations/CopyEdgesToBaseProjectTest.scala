package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyEdgesToBaseProjectTest extends OperationsTestBase {
  test("Copy edges to base project") {
    run("New vertex set", Map("size" -> "5"))

    run("Copy graph into a segmentation", Map("name" -> "copy"))
    val copy = project.segmentation("copy")

    run(
      "Create random edge bundle",
      Map("degree" -> "5", "seed" -> "0", "apply_to" -> "|copy"))
    assert(copy.edgeBundle.toPairSeq.size == 21)
    run(
      "Add constant edge attribute",
      Map("name" -> "const", "value" -> "1", "type" -> "Double", "apply_to" -> "|copy"))

    run("Copy edges to base project", Map("apply_to" -> "|copy"))
    assert(project.edgeBundle.toPairSeq.size == 21)
    assert(project.edgeAttributes("const").rdd.count == 21)
  }
}

