package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyEdgesToSegmentationTest extends OperationsTestBase {
  test("Copy edges to cliques") {
    val base = box("Create vertices", Map("size" -> "5"))
      .box("Create random edge bundle", Map("degree" -> "5", "seed" -> "0"))
      .box("Add constant edge attribute", Map("name" -> "const", "value" -> "1", "type" -> "Double"))
      .box("Find maximal cliques", Map("name" -> "cliques", "bothdir" -> "false", "min" -> "3"))
    val project1 = base.project
    assert(project1.edgeBundle.toPairSeq.size == 21)
    val cliques = project1.segmentation("cliques")
    assert(cliques.vertexSet.toSeq.size == 2)
    val next = base.box("Copy edges to segmentation", Map("apply_to_project" -> "|cliques"))
    val seg1 = next.project.segmentation("cliques")
    assert(seg1.edgeBundle.toPairSeq.size == 56)
    assert(seg1.edgeAttributes("const").rdd.count == 56)
    val seg2 = next.box("Merge parallel edges", Map("apply_to_project" -> "|cliques"))
      .project.segmentation("cliques")
    assert(seg2.edgeBundle.toPairSeq.size == 4)
  }
}

