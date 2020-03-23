package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SphynxOnly
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class EmbedVerticesTest extends OperationsTestBase {
  test("example graph", SphynxOnly) {
    val embedding = box("Create example graph")
      .box("Embed vertices")
      .box("Embed with t-SNE")
      .project.vertexAttributes("tsne").runtimeSafeCast[(Double, Double)]
    val (x, y) = get(embedding).values.unzip
    // Check against all zeroes and NaNs.
    assert(x.max > 0 || x.max < 0)
    assert(y.max > 0 || y.max < 0)
  }
}
