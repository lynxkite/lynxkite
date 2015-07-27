package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class AddReversedEdgesTest extends FunSuite with TestGraphOp {
  test("Enhanced example graph") {
    val g = EnhancedExampleGraph()().result
    val op = AddReversedEdges()
    val out = op(op.es, g.edges).result
    assert(out.esPlus.toPairSeq ==
      Seq((0, 1), (0, 1), (0, 2), (1, 0), (1, 0), (1, 2), (2, 0), (2, 1), (2, 2), (2, 2),
        (2, 4), (2, 4), (2, 4), (2, 5), (2, 5), (2, 5), (2, 5), (2, 5), (4, 2), (4, 2), (4, 2),
        (4, 5), (4, 5), (4, 5), (4, 5), (4, 6), (5, 2), (5, 2), (5, 2), (5, 2), (5, 2), (5, 4),
        (5, 4), (5, 4), (5, 4), (5, 7), (6, 4), (7, 5)))
  }
}
