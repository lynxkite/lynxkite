package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class CreateSnowballSampleTest extends OperationsTestBase {
  test("Snowball sample with ratio=1.0") {
    val a = box("Create example graph")
      .box("Create snowball sample",
        Map("ratio" -> "1.0", "radius" -> "0", "seed" -> "123454321", "attrName" -> "distance_from_start_point")
      )

    val size = a.project.vertexSet.rdd.count.toInt
    assert(size == 4)
  }

  test("Snowball sample with ratio=0.0") {
    val a = box("Create example graph")
      .box("Create snowball sample",
        Map("ratio" -> "0.0", "radius" -> "0", "seed" -> "123454321", "attrName" -> "distance_from_start_point")
      )

    val size = a.project.vertexSet.rdd.count
    assert(size == 0)
  }

  test("Snowball sample with small graph (1000 vertices, ratio=0.1)") {
    val a = box("Create vertices", Map("size" -> "1000"))
      .box("Create random edge bundle", Map("degree" -> "10.0", "seed" -> "12321"))
      .box("Create snowball sample",
        Map("ratio" -> "0.1", "radius" -> "0", "seed" -> "123454321", "attrName" -> "distance_from_start_point")
      )

    val size = a.project.vertexSet.rdd.count
    assert(size == 110) // Calculated with seed = 123454321.

  }

}
