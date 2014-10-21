package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class AddReversedEdgesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = AddReversedEdges()
    val out = op(op.es, g.edges).result
    assert(out.esPlus.toPairSeq.toSet == Set(0 -> 1, 1 -> 0, 0 -> 2, 2 -> 0, 1 -> 2, 2 -> 1))
  }
}
