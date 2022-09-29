// Test that reproduces issue #2175
package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.TestUtils
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class FilterByAttributeOperationTest extends OperationsTestBase {

  test("Filtering doesn't screw up matching partitions") {

    TestUtils.withRestoreGlobals(verticesPerPartition = 2, tolerance = 1.0) {
      val sizeForTwoPartitions = 3
      val project = box(
        "Create vertices",
        Map("size" -> sizeForTwoPartitions.toString))
        .box(
          "Create random edges",
          Map("degree" -> "2.0", "seed" -> "42"))
        .box(
          "Add constant edge attribute",
          Map("name" -> "e", "value" -> "0.0", "type" -> "number"))
        .box(
          "Add constant vertex attribute",
          Map("name" -> "v", "value" -> "0.0", "type" -> "number"))
        .box(
          "Filter by attributes",
          Map("filterva_v" -> "> 0.0", "filterea_e" -> "> 1.0"))
        .project
      project.scalars("!edge_count").value
    }
  }

  test("Filtering by segment ID") {
    val base = box("Create example graph")
      .box(
        "Find connected components",
        Map("name" -> "cc", "directions" -> "ignore directions"))
      .box(
        "Filter by attributes",
        Map("filterva_size" -> "3", "apply_to_graph" -> ".cc"))
    val c1 = base.project.segmentation("cc").vertexSet.rdd.keys.take(1).head
    val project2 = base.box(
      "Filter by attributes",
      Map("filterva_segmentation[cc]" -> s"any($c1)"))
      .project
    assert(project2.vertexSet.rdd.count == 3)
  }
}
