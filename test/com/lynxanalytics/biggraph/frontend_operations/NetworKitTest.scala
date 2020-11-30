// All NetworKit tests are in a single file for now.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class NetworKitTest extends OperationsTestBase {
  test("Find k-core decomposition") {
    val g = box("Create example graph").box("Find k-core decomposition").project
    assert(get(g.vertexAttributes("core")) == Map(0 -> 2.0, 1 -> 2.0, 2 -> 2.0, 3 -> 0.0))
  }

  test("Compute centrality") {
    for (
      (algorithm, expected) <- Seq(
        "Harmonic" -> Map(0 -> 2.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0),
        "Lin" -> Map(0 -> 4.5, 1 -> 4.5, 2 -> 1.0, 3 -> 1.0),
        "Average distance" -> Map(0 -> 1.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0),
        "Betweenness" -> Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 0.0),
        "Eigenvector" -> Map(0 -> 0.7, 1 -> 0.7, 2 -> 0.0, 3 -> 0.0),
        "Harmonic Closeness" -> Map(0 -> 0.33, 1 -> 0.33, 2 -> 0.66, 3 -> 0.0),
        "Katz" -> Map(0 -> 0.5, 1 -> 0.5, 2 -> 0.49, 3 -> 0.49),
        "K-Path" -> Map(0 -> 3.84, 1 -> 4.48, 2 -> 0.0, 3 -> 0.0),
        "Sfigality" -> Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0))
    ) {
      println(algorithm)
      val g = box("Create example graph").box("Compute centrality", Map("algorithm" -> algorithm)).project
      val centrality = get(g.vertexAttributes("centrality").runtimeSafeCast[Double])
      assert(centrality.size == expected.size, s"-- in $algorithm")
      for ((k, v) <- centrality) {
        assert(
          Math.abs(v - expected(k.toInt)) < 0.01,
          s"-- $algorithm returned $centrality instead of $expected")
      }
    }
  }

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
