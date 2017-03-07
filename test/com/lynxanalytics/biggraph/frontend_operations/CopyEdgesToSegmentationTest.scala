package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class CopyEdgesToSegmentationTest extends OperationsTestBase {
  test("Copy edges to cliques") {
    run("New vertex set", Map("size" -> "5"))
    run("Create random edge bundle", Map("degree" -> "5", "seed" -> "0"))
    assert(project.edgeBundle.toPairSeq.size == 21)
    run("Add constant edge attribute", Map("name" -> "const", "value" -> "1", "type" -> "Double"))
    run("Maximal cliques", Map("name" -> "cliques", "bothdir" -> "false", "min" -> "3"))
    val cliques = project.segmentation("cliques")
    assert(cliques.vertexSet.toSeq.size == 2)
    run("Copy edges to segmentation", Map("apply_to" -> "|cliques"))
    assert(cliques.edgeBundle.toPairSeq.size == 56)
    assert(cliques.edgeAttributes("const").rdd.count == 56)
    run("Merge parallel edges", Map("apply_to" -> "|cliques"))
    assert(cliques.edgeBundle.toPairSeq.size == 4)
  }
}

