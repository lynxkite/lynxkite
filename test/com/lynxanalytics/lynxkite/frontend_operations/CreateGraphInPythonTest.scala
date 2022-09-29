package com.lynxanalytics.lynxkite.frontend_operations

import com.lynxanalytics.lynxkite.SphynxOnly
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class CreateGraphInPythonTest extends OperationsTestBase {
  test("simple graph", SphynxOnly) {
    val p = box(
      "Create graph in Python",
      Map(
        "outputs" -> "vs.name: str, es.weight: float, graph_attributes.hello: str",
        "code" -> """
vs = pd.DataFrame({
  'name': ['Alice', 'Bob', 'Cecil', 'Drew'],
})
es = pd.DataFrame({
  'src': [0, 1, 2],
  'dst': [0, 2, 1],
  'weight': [1, 2, 3],
})
graph_attributes.hello = 'hello'
          """,
      ),
    )
      .box("Compute degree", Map("direction" -> "all edges"))
      .project
    assert(
      get(p.vertexAttributes("name").runtimeSafeCast[String]) ==
        Map(0 -> "Alice", 1 -> "Bob", 2 -> "Cecil", 3 -> "Drew"))
    assert(
      get(p.vertexAttributes("degree").runtimeSafeCast[Double]) ==
        Map(0 -> 2, 1 -> 2, 2 -> 2, 3 -> 0))
    assert(
      get(p.edgeAttributes("weight").runtimeSafeCast[Double]) ==
        Map(0 -> 1, 1 -> 2, 2 -> 3))
    assert(get(p.scalars("hello").runtimeSafeCast[String]) == "hello")
  }
}
