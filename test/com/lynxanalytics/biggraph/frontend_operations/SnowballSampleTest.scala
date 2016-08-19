package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class SnowballSampleTest extends OperationsTestBase {
  test("Snowball sample with percentage=1.0") {
    run("Example Graph")
    run("Snowball sample",
      Map("ratio" -> "1.0", "radius" -> "0", "seed" -> "123454321")
    )

    val size = project.vertexSet.rdd.count.toInt
    assert(size == 4)
  }

  test("Snowball sample with percentage=0.0") {
    run("Example Graph")
    run("Snowball sample",
      Map("ratio" -> "0.0", "radius" -> "0", "seed" -> "123454321")
    )

    val size = project.vertexSet.rdd.count.toInt
    assert(size == 0)
  }

  test("Snowball sample with small graph (1000 vertices, ratio=0.1)") {
    run("New vertex set", Map("size"->"1000"))
    run("Snowball sample",
      Map("ratio" -> "0.0", "radius" -> "0", "seed" -> "123454321")
    )

    val size = project.vertexSet.rdd.count.toInt
    assert(size <= 30) // prob = 0.999999794
  }

}
