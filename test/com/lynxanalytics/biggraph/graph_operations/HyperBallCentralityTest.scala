package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

// The algorithm gives an approximation. Asserting on exact centrality
// values works only for small graphs.
class HyperBallCentralityTest extends FunSuite with TestGraphOp {
  test("corner cases") {
    val op = HyperBallCentrality(5, "Harmonic")

    check(op, Map(0 -> Seq()), Map(0 -> 0.0))
    check(op, Map(0 -> Seq(0)), Map(0 -> 0.0))
    check(op, Map(
      0 -> Seq(1, 1),
      1 -> Seq()),
      Map(0 -> 0.0, 1 -> 1.0))
    check(op, Map(
      0 -> Seq(1),
      1 -> Seq(0)),
      Map(0 -> 1.0, 1 -> 1.0))
  }

  test("small example graph") {
    val g = ExampleGraph()().result
    val op = HyperBallCentrality(5, "Harmonic")
    val out = op(op.es, g.edges).result.centrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 2.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0))
  }

  test("path graph") {
    check(HyperBallCentrality(5, "Harmonic"), Map(
      0 -> Seq(1),
      1 -> Seq(2),
      2 -> Seq(3),
      3 -> Seq()),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 1.5, 3 -> (1.5 + (1.0 / 3.0))))
  }

  test("tree graph") {
    check(HyperBallCentrality(5, "Harmonic"), Map(
      0 -> Seq(),
      1 -> Seq(0),
      2 -> Seq(0),
      3 -> Seq(1),
      4 -> Seq(1)),
      Map(0 -> 3.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0, 4 -> 0.0))
  }

  test("triangle graph") {
    check(HyperBallCentrality(5, "Harmonic"), Map(
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
