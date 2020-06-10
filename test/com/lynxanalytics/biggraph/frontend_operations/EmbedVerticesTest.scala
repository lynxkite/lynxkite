package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SphynxOnly
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class EmbedVerticesTest extends OperationsTestBase {
  test("t-SNE", SphynxOnly) {
    val embedding = box("Create example graph")
      .box("Embed vertices")
      .box("Reduce attribute dimensions", Map("method" -> "t-SNE"))
      .project.vertexAttributes("embedding").runtimeSafeCast[(Double, Double)]
    val (x, y) = get(embedding).values.unzip
    // Check against all zeroes and NaNs.
    assert(x.max > 0 || x.max < 0)
    assert(y.max > 0 || y.max < 0)
  }

  test("PCA", SphynxOnly) {
    val embedding = box("Create example graph")
      .box("Embed vertices")
      .box("Reduce attribute dimensions", Map("method" -> "PCA"))
      .project.vertexAttributes("embedding").runtimeSafeCast[(Double, Double)]
    val (x, y) = get(embedding).values.unzip
    // Check against all zeroes and NaNs.
    assert(x.max > 0 || x.max < 0)
    assert(y.max > 0 || y.max < 0)
  }

  test("Higher dimensions", SphynxOnly) {
    val embedding = box("Create example graph")
      .box("Embed vertices")
      .box("Reduce attribute dimensions", Map("dimensions" -> "3"))
      .project.vertexAttributes("embedding").runtimeSafeCast[Vector[Double]]
    val x = get(embedding).values
    println(x)
  }
}
