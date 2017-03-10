// Test that reproduces issue #2175
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api.Scripting._

class FilterByAttributeOperationTest extends OperationsTestBase {

  test("Filtering doesn't screw up matching partitions") {

    TestUtils.withRestoreGlobals(verticesPerPartition = 2, tolerance = 1.0) {
      val sizeForTwoPartitions = 3
      run("Create vertices",
        Map("size" -> sizeForTwoPartitions.toString))
      run("Create random edge bundle",
        Map("degree" -> "2.0", "seed" -> "42"))
      run("Add constant edge attribute",
        Map("name" -> "e", "value" -> "0.0", "type" -> "Double"))
      run("Add constant vertex attribute",
        Map("name" -> "v", "value" -> "0.0", "type" -> "Double"))
      run("Filter by attributes",
        Map("filterva_v" -> "> 0.0", "filterea_e" -> "> 1.0"))

      project.scalars("edge_count").value
    }
  }

  test("Filtering by segment ID") {
    run("Create example graph")
    run("Find connected components",
      Map("name" -> "cc", "directions" -> "ignore directions"))
    run("Filter by attributes",
      Map("filterva_size" -> "3", "apply_to" -> "|cc"))
    val c1 = project.segmentation("cc").vertexSet.rdd.keys.take(1).head
    run("Filter by attributes",
      Map("filterva_segmentation[cc]" -> s"any($c1)"))
    assert(project.vertexSet.rdd.count == 3)
  }
}

