package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

// The algorithm gives an approximation. Asserting on exact centrality
// values works only for small graphs.
class HyperBallCentralityTest extends FunSuite with TestGraphOp {
  test("corner cases") {
    val op = HyperBallCentrality(5)

    val g_isolated = SmallTestGraph(Map(0 -> Seq())).result
    val out_isolated = op(op.es, g_isolated.es).result.harmonicCentrality
    assert(out_isolated.rdd.collect.toMap == Map(0 -> 0.0))

    val g_loop = SmallTestGraph(Map(0 -> Seq(0))).result
    val out_loop = op(op.es, g_loop.es).result.harmonicCentrality
    assert(out_loop.rdd.collect.toMap == Map(0 -> 0.0))

    val g_parallel = SmallTestGraph(Map(
      0 -> Seq(1, 1),
      1 -> Seq())).result
    val out_parallel = op(op.es, g_parallel.es).result.harmonicCentrality
    assert(out_parallel.rdd.collect.toMap == Map(0 -> 0.0, 1 -> 1.0))

    val g_complete = SmallTestGraph(Map(
      0 -> Seq(1),
      1 -> Seq(0))).result
    val out_complete = op(op.es, g_complete.es).result.harmonicCentrality
    assert(out_complete.rdd.collect.toMap == Map(0 -> 1.0, 1 -> 1.0))
  }

  test("small example graph") {
    val g = ExampleGraph()().result
    val op = HyperBallCentrality(5)
    val out = op(op.es, g.edges).result.harmonicCentrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 2.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0))
  }

  test("path graph") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1),
      1 -> Seq(2),
      2 -> Seq(3),
      3 -> Seq())).result
    val op = HyperBallCentrality(5)
    val out = op(op.es, g.es).result.harmonicCentrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 1.5, 3 -> (1.5 + (1.0 / 3.0))))
  }

  test("tree graph") {
    val g = SmallTestGraph(Map(
      0 -> Seq(),
      1 -> Seq(0),
      2 -> Seq(0),
      3 -> Seq(1),
      4 -> Seq(1))).result
    val op = HyperBallCentrality(5)
    val out = op(op.es, g.es).result.harmonicCentrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 3.0, 1 -> 2.0, 2 -> 0.0, 3 -> 0.0, 4 -> 0.0))
  }

  test("triangle graph") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1),
      1 -> Seq(2),
      2 -> Seq(0))).result
    val op = HyperBallCentrality(5)
    val out = op(op.es, g.es).result.harmonicCentrality
    assert(out.rdd.collect.toMap ==
      Map(0 -> 1.5, 1 -> 1.5, 2 -> 1.5))
  }
}
