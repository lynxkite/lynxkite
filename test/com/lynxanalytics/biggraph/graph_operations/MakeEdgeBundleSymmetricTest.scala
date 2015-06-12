package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class MakeEdgeBundleSymmetricTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = MakeEdgeBundleSymmetric()
    val out = op(op.es, g.edges).result
    assert(out.symmetric.toPairSeq == Seq(0 -> 1, 1 -> 0))
  }

  test("A bit more complex graph") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1, 1, 2, 2),
      1 -> Seq(0, 0),
      2 -> Seq(0, 3),
      3 -> Seq()
    )).result
    val op = MakeEdgeBundleSymmetric()
    val out = op(op.es, g.es).result
    assert(out.symmetric.toPairSeq().sorted == Seq(0 -> 1, 0 -> 1, 0 -> 2, 1 -> 0, 1 -> 0, 2 -> 0))
  }
}
