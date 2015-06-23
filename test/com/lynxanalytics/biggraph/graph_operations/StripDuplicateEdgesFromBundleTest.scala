package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class StripDuplicateEdgesFromBundleTest extends FunSuite with TestGraphOp {
  test("A bit more complex graph") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1, 1, 2, 2),
      1 -> Seq(0, 0),
      2 -> Seq(0, 3),
      3 -> Seq(4, 4, 4),
      4 -> Seq()
    )).result
    val op = StripDuplicateEdgesFromBundle()
    val out = op(op.es, g.es).result
    assert(out.unique.toPairSeq().sorted == Seq(0 -> 1, 0 -> 2, 1 -> 0, 2 -> 0, 2 -> 3, 3 -> 4))
  }
}
