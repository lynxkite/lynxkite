// All NetworKit tests are in a single file for now.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class NetworKitTest extends OperationsTestBase {
  test("Create Barabasi–Albert graph") {
    val g = box("Create Barabasi–Albert graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 100)
  }
  test("Create random graph with certain degrees / Chung–Lu") {
    val g = box(
      "Create random graph with certain degrees",
      Map("seed" -> "1", "algorithm" -> "Chung–Lu")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 110)
  }
  test("Create random graph with certain degrees / Edge switching Markov chain") {
    val g = box(
      "Create random graph with certain degrees",
      Map("seed" -> "1", "algorithm" -> "Edge switching Markov chain")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 125)
  }
  test("Create random graph with certain degrees / Haveli–Hakimi") {
    val g = box(
      "Create random graph with certain degrees",
      Map("seed" -> "1", "algorithm" -> "Haveli–Hakimi")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 125)
  }
  test("Create clustered random graph") {
    val g = box("Create clustered random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 343)
  }
  test("Create Dorogovtsev–Mendes random graph") {
    val g = box("Create Dorogovtsev–Mendes random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 197)
  }
  test("Create Erdős–Rényi graph") {
    val g = box("Create Erdős–Rényi graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 98)
  }
  test("Create hyperbolic random graph") {
    val g = box("Create hyperbolic random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 283)
  }
  test("Create LFR random graph") {
    val g = box("Create LFR random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 164)
  }
  test("Create Mocnik random graph") {
    val g = box("Create Mocnik random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 539)
  }
  test("Create P2P random graph") {
    val g = box("Create P2P random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 324)
  }
}
