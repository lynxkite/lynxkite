package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class TextEmbeddingPythonTest extends OperationsTestBase {
  // TODO: Fix sentence-transformers and openjdk Conda conflict to enable this in CI.
  ignore("embed name", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph").box("Embed String attribute", Map(
      "attr" -> "name",
      "method" -> "SentenceTransformers",
      "model_name" -> "thenlper/gte-small",  // A 30M parameter model. Still pretty good!
    )).project
    val embs = get(g.vertexAttributes("embedding").runtimeSafeCast[Vector[Double]])
    assert(embs.mapValues(v => v.length) == Map(0 -> 384, 1 -> 384, 2 -> 384, 3 -> 384))
  }
}
