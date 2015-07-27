package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class RemoveNonSymmetricEdgesTest extends FunSuite with TestGraphOp {
  test("Enhanced example graph") {
    val g = EnhancedExampleGraph()().result
    val op = RemoveNonSymmetricEdges()
    val out = op(op.es, g.edges).result
    assert(out.symmetric.toPairSeq == Seq((0, 1), (1, 0), (2, 2), (2, 4), (2, 5), (2, 5),
      (2, 5), (4, 2), (4, 2), (4, 5), (5, 2), (5, 2), (5, 4), (5, 4), (5, 4)))

  }
}
