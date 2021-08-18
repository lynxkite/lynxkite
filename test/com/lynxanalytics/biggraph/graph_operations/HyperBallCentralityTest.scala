package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

// The algorithm gives an approximation. Asserting on exact centrality
// values works only for small graphs.
class HyperBallCentralityTest extends AnyFunSuite with TestGraphOp {
  test("corner cases - Harmonic") {
    val op = HyperBallCentrality(5, "Harmonic", 8)

    check(op, Map(0 -> Seq()), Map(0 -> 0.0))
    check(op, Map(0 -> Seq(0)), Map(0 -> 0.0))
    check(
      op,
      Map(
        0 -> Seq(1, 1),
        1 -> Seq()),
      Map(0 -> 0.0, 1 -> 1.0))
    check(
      op,
      Map(
        0 -> Seq(1),
        1 -> Seq(0)),
      Map(0 -> 1.0, 1 -> 1.0))
  }

  test("corner cases - Lin") {
    val op = HyperBallCentrality(5, "Lin", 8)

    check(op, Map(0 -> Seq()), Map(0 -> 1.0))
    check(op, Map(0 -> Seq(0)), Map(0 -> 1.0))
    check(
      op,
      Map(
        0 -> Seq(1, 1),
        1 -> Seq()),
      Map(0 -> 1.0, 1 -> 4.0))
    check(
      op,
      Map(
        0 -> Seq(1),
        1 -> Seq(0)),
      Map(0 -> 4.0, 1 -> 4.0))
  }

  test("corner cases - Average distance") {
    val op = HyperBallCentrality(5, "Average distance", 8)

    check(op, Map(0 -> Seq()), Map(0 -> 0.0))
    check(op, Map(0 -> Seq(0)), Map(0 -> 0.0))
    check(
      op,
      Map(
        0 -> Seq(1, 1),
        1 -> Seq()),
      Map(0 -> 0.0, 1 -> 1.0))
    check(
      op,
      Map(
        0 -> Seq(1),
        1 -> Seq(0)),
      Map(0 -> 1.0, 1 -> 1.0))
  }

  test("small example graph - Harmonic") {
    val g = ExampleGraph()().result
    val op = HyperBallCentrality(5, "Harmonic", 8)
    val out = op(op.es, g.edges).result.centrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 2.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0))
  }

  test("small example graph - Lin") {
    val g = ExampleGraph()().result
    val op = HyperBallCentrality(5, "Lin", 8)
    val out = op(op.es, g.edges).result.centrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 4.5, 1 -> 4.5, 2 -> 1.0, 3 -> 1.0))
  }

  test("small example graph - Average distance") {
    val g = ExampleGraph()().result
    val op = HyperBallCentrality(5, "Average distance", 8)
    val out = op(op.es, g.edges).result.centrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 1.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0))
  }

  test("path graph - Harmonic") {
    check(
      HyperBallCentrality(5, "Harmonic", 8),
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(3),
        3 -> Seq()),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 1.5, 3 -> (1.5 + (1.0 / 3.0))))
  }

  test("path graph - Lin") {
    check(
      HyperBallCentrality(5, "Lin", 8),
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(3),
        3 -> Seq()),
      Map(0 -> 1.0, 1 -> 4.0, 2 -> 3.0, 3 -> 16.0 / 6.0))
  }

  test("path graph - Average distance") {
    check(
      HyperBallCentrality(5, "Average distance", 8),
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(3),
        3 -> Seq()),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 1.5, 3 -> 2.0))
  }

  test("tree graph - Harmonic") {
    check(
      HyperBallCentrality(5, "Harmonic", 8),
      Map(
        0 -> Seq(),
        1 -> Seq(0),
        2 -> Seq(0),
        3 -> Seq(1),
        4 -> Seq(1)),
      Map(0 -> 3.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0, 4 -> 0.0))
  }

  test("tree graph - Lin") {
    check(
      HyperBallCentrality(5, "Lin", 8),
      Map(
        0 -> Seq(),
        1 -> Seq(0),
        2 -> Seq(0),
        3 -> Seq(1),
        4 -> Seq(1)),
      Map(0 -> 25.0 / 6.0, 1 -> 4.5, 2 -> 1.0, 3 -> 1.0, 4 -> 1.0))
  }

  test("tree graph - Average distance") {
    check(
      HyperBallCentrality(5, "Average distance", 8),
      Map(
        0 -> Seq(),
        1 -> Seq(0),
        2 -> Seq(0),
        3 -> Seq(1),
        4 -> Seq(1)),
      Map(0 -> 1.5, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 0.0))
  }

  test("triangle graph - Harmonic") {
    check(
      HyperBallCentrality(5, "Harmonic", 8),
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(0)),
      Map(0 -> 1.5, 1 -> 1.5, 2 -> 1.5))
  }

  test("triangle graph - Lin") {
    check(
      HyperBallCentrality(5, "Lin", 8),
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(0)),
      Map(0 -> 3.0, 1 -> 3.0, 2 -> 3.0))
  }

  test("triangle graph - Average distance") {
    check(
      HyperBallCentrality(5, "Average distance", 8),
      Map(
        0 -> Seq(1),
        1 -> Seq(2),
        2 -> Seq(0)),
      Map(0 -> 1.5, 1 -> 1.5, 2 -> 1.5))
  }

  def check(op: HyperBallCentrality, graph: Map[Int, Seq[Int]], result: Map[Int, Double]): Unit = {
    val g = SmallTestGraph(graph).result
    val out = op(op.es, g.es).result.centrality
    assert(out.rdd.collect.toMap == result)
  }
}
