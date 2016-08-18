package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SnowballSampleTest extends OperationsTestBase {
  test("Snowball sample with percentage=1.0") {
    run("Example Graph")
    run("Snowball sample",
      Map("percentage" -> "1.0", "radius" -> "0", "seed" -> "123454321", "attrName" -> "distance_from_starting_vertex")
    )

    val size = project.vertexSet.rdd.count.toInt
    assert(size == 4)
  }

  test("Snowball sample with percentage=0.0") {
    run("Example Graph")
    run("Snowball sample",
      Map("percentage" -> "0.0", "radius" -> "0", "seed" -> "123454321", "attrName" -> "distance_from_starting_vertex")
    )

    val size = project.vertexSet.rdd.count.toInt
    assert(size == 0)
  }
}
