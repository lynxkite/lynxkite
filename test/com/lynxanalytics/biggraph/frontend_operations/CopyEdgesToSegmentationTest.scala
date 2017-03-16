package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyEdgesToSegmentationTest extends OperationsTestBase {
  test("Copy edges to cliques") {
    val a = box("Create vertices", Map("size" -> "5"))
      .box("Create random edge bundle", Map("degree" -> "5", "seed" -> "0"))
      .box("Add constant edge attribute", Map("name" -> "const", "value" -> "1", "type" -> "Double"))
      .box("Find maximal cliques", Map("name" -> "cliques", "bothdir" -> "false", "min" -> "3"))
    val project = a.project
    assert(project.edgeBundle.toPairSeq.size == 21)
    val cliques = project.segmentation("cliques")
    assert(cliques.vertexSet.toSeq.size == 2)
    val b = a.box("Copy edges to segmentation", Map("apply_to_project" -> "|cliques"))
    val p = b.project
    assert(p.segmentation("cliques").edgeBundle.toPairSeq.size == 56)
    assert(p.segmentation("cliques").edgeAttributes("const").rdd.count == 56)
    val c = b.box("Merge parallel edges", Map("apply_to_project" -> "|cliques"))
    assert(c.project.segmentation("cliques").edgeBundle.toPairSeq.size == 4)
  }
}

