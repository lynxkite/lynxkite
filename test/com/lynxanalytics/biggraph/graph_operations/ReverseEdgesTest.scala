package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class ReverseEdgesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = ReverseEdges()
    val out = op(op.esAB, g.edges).result
    assert(out.esBA.toPairSeq == Seq(0 -> 1, 0 -> 2, 1 -> 0, 1 -> 2))
  }

  test("example graph - no double reverse") {
    val g = ExampleGraph()().result
    val e = ReverseEdges.run(ReverseEdges.run(g.edges))
    assert(e.entity eq g.edges.entity)
  }
}
