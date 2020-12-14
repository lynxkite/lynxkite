// All NetworKit tests are in a single file for now.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class NetworKitTest extends OperationsTestBase {
  def assertMatch(result: Map[Long, Double], expected: Map[Int, Double], msg: String = "") = {
    assert(result.size == expected.size, msg)
    for ((k, v) <- result) {
      assert(
        Math.abs(v - expected(k.toInt)) < 0.01,
        s"$msg returned $result instead of $expected")
    }
  }

  test("Find k-core decomposition", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph").box("Find k-core decomposition").project
    assert(get(g.vertexAttributes("core")) == Map(0 -> 2.0, 1 -> 2.0, 2 -> 2.0, 3 -> 0.0))
  }

  test("Compute centrality", com.lynxanalytics.biggraph.SphynxOnly) {
    for (
      (algorithm, expected) <- Seq(
        "Closeness (estimate)" -> Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 3 -> 0.0),
        "Harmonic" -> Map(0 -> 2.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0),
        "Laplacian" -> Map(0 -> 4.0, 1 -> 4.0, 2 -> 10.0, 3 -> 0.0),
        "Lin" -> Map(0 -> 4.5, 1 -> 4.5, 2 -> 1.0, 3 -> 1.0),
        "Average distance" -> Map(0 -> 1.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0),
        "Betweenness" -> Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 0.0),
        "Betweenness (estimate)" -> Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 0.0),
        "Eigenvector" -> Map(0 -> 0.7, 1 -> 0.7, 2 -> 0.0, 3 -> 0.0),
        "Harmonic Closeness" -> Map(0 -> 0.33, 1 -> 0.33, 2 -> 0.66, 3 -> 0.0),
        "Katz" -> Map(0 -> 0.5, 1 -> 0.5, 2 -> 0.49, 3 -> 0.49),
        "K-Path" -> Map(0 -> 3.84, 1 -> 4.48, 2 -> 0.0, 3 -> 0.0),
        "Sfigality" -> Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0))
    ) {
      println(algorithm)
      val g = box("Create example graph").box("Compute centrality", Map("algorithm" -> algorithm)).project
      val centrality = get(g.vertexAttributes("centrality").runtimeSafeCast[Double])
      assertMatch(centrality, expected, s"-- in $algorithm")
    }
  }

  test("Create Barabási–Albert graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create Barabási–Albert graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 100)
  }
  test("Create a graph with certain degrees / Chung–Lu", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box(
      "Create a graph with certain degrees",
      Map("seed" -> "1", "algorithm" -> "Chung–Lu")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 110)
  }
  test("Create a graph with certain degrees / Edge switching Markov chain", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box(
      "Create a graph with certain degrees",
      Map("seed" -> "1", "algorithm" -> "Edge switching Markov chain")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 125)
  }
  test("Create a graph with certain degrees / Haveli–Hakimi", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box(
      "Create a graph with certain degrees",
      Map("seed" -> "1", "algorithm" -> "Haveli–Hakimi")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 125)
  }
  test("Create clustered random graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create clustered random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 343)
  }
  test("Create Dorogovtsev–Mendes random graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create Dorogovtsev–Mendes random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 197)
  }
  test("Create Erdős–Rényi graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create Erdős–Rényi graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 91)
  }
  test("Create hyperbolic random graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create hyperbolic random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 283)
  }
  test("Create LFR random graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create LFR random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 164)
  }
  test("Create Mocnik random graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create Mocnik random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 539)
  }
  test("Create P2P random graph", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create P2P random graph", Map("seed" -> "1")).project
    assert(g.vertexSet.countScalar.value == 100)
    assert(g.edgeBundle.countScalar.value == 324)
  }

  test("Find communities with label propagation", com.lynxanalytics.biggraph.SphynxOnly) {
    for (variant <- Seq("classic", "degree-ordered")) {
      println(variant)
      val g = box("Create example graph")
        .box("Find communities with label propagation", Map("variant" -> variant))
        .box(
          "Aggregate from segmentation",
          Map("apply_to_graph" -> ".communities", "aggregate_id" -> "first"))
        .project
      val m = get(g.vertexAttributes("communities_id_first").runtimeSafeCast[String])
      assert(m.keySet == Set(0, 1, 2, 3), s"-- in $variant")
      // The clusters are non-deterministic even with a fixed random seed.
    }
  }

  test("Find communities with the Louvain method", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph")
      .box("Find communities with the Louvain method", Map("resolution" -> "1.5"))
      .box(
        "Aggregate from segmentation",
        Map("apply_to_graph" -> ".communities", "aggregate_id" -> "first"))
      .project
    val m = get(g.vertexAttributes("communities_id_first").runtimeSafeCast[String])
    assert(m.keySet == Set(0, 1, 2, 3))
    // The clusters are non-deterministic even with a fixed random seed.
  }

  test("Place vertices with edge lengths", com.lynxanalytics.biggraph.SphynxOnly) {
    for (
      (algorithm, expected) <- Seq(
        "Pivot MDS" ->
          Map(0 -> Vector(-0.27, -0.77), 1 -> Vector(0.8, 0.15), 2 -> Vector(-0.53, 0.62)),
        "Maxent-Stress" ->
          Map(0 -> Vector(-0.03, 0.5), 1 -> Vector(-0.55, -0.3), 2 -> Vector(0.58, -0.2)))
    ) {
      println(algorithm)
      val g = box("Create example graph")
        .box("Compute degree")
        .box("Filter by attributes", Map("filterva_degree" -> ">0"))
        .box("Place vertices with edge lengths", Map("algorithm" -> algorithm))
        .project
      val p = get(g.vertexAttributes("position").runtimeSafeCast[Vector[Double]])
      assert(p.size == expected.size, s"-- in $algorithm")
      for ((k, v) <- p) {
        for ((a, e) <- v.zip(expected(k.toInt))) {
          assert(
            Math.abs(a - e) < 0.01,
            s"-- $algorithm returned $p instead of $expected")
        }
      }
    }
  }

  test("Score edges with the forest fire model", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph")
      .box("Score edges with the forest fire model", Map("seed" -> "1"))
      .project
    val p = get(g.edgeAttributes("forest fire score").runtimeSafeCast[Double])
    assert(p.size == 4)
    // Even with a seed it's not deterministic.
    for (v <- p.values) {
      assert(v >= 0 && v <= 1)
    }
  }

  test("Compute diameter", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph")
    val exact = g.box("Compute diameter", Map("max_error" -> "0")).project
    assert(1 == get(exact.scalars("diameter").runtimeSafeCast[Double]))
    val estimate = g.box("Compute diameter", Map("max_error" -> "0.1")).project
    assert(1 == get(estimate.scalars("diameter_lower").runtimeSafeCast[Double]))
    assert(1 == get(estimate.scalars("diameter_upper").runtimeSafeCast[Double]))
  }

  test("Compute effective diameter", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph")
      .box("Compute degree")
      .box("Filter by attributes", Map("filterva_degree" -> ">0"))
    val exact = g.box("Compute effective diameter", Map("algorithm" -> "exact")).project
    assert(1 == get(exact.scalars("effective diameter").runtimeSafeCast[Double]))
    val estimate = g.box("Compute effective diameter", Map("algorithm" -> "estimate")).project
    assert(1 == get(estimate.scalars("effective diameter").runtimeSafeCast[Double]))
  }

  test("Compute assortativity", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph")
      .box("Compute assortativity", Map("attribute" -> "age"))
      .project
    assert(Math.abs(-0.033 - get(g.scalars("assortativity").runtimeSafeCast[Double])) < 0.01)
  }

  test("Edge scoring", com.lynxanalytics.biggraph.SphynxOnly) {
    val g = box("Create example graph")
      // It's not deterministic, but with a large enough ratio it's reliable enough.
      .box("Score edges with the forest fire model", Map("burn_ratio" -> "1000000"))
      .project
    val score = get(g.edgeAttributes("forest fire score").runtimeSafeCast[Double])
    val expected = Map(0 -> 1.0, 1 -> 0.33, 2 -> 0.57, 3 -> 0.57)
    assertMatch(score, expected)
  }
}
