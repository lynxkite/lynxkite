package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class MakeEdgeBundleSymmetricTest extends AnyFunSuite with TestGraphOp {
  test("Enhanced example graph") {
    val g = EnhancedExampleGraph()().result
    val op = MakeEdgeBundleSymmetric()
    val out = op(op.es, g.edges).result
    assert(out.symmetric.toPairSeq ==
      Seq((0, 1), (1, 0), (2, 2), (2, 4), (2, 5), (2, 5), (4, 2), (4, 5), (5, 2), (5, 2), (5, 4)))
  }

  test("A bit more complex graph") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1, 1, 2, 2),
      1 -> Seq(0, 0),
      2 -> Seq(0, 3),
      3 -> Seq())).result
    val op = MakeEdgeBundleSymmetric()
    val out = op(op.es, g.es).result
    assert(out.symmetric.toPairSeq().sorted == Seq(0 -> 1, 0 -> 1, 0 -> 2, 1 -> 0, 1 -> 0, 2 -> 0))
  }
}
