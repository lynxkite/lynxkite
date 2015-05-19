package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class EdgesFromAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = EdgesFromAttributeMatches[String]()
    val res = op(op.attr, g.gender).result
    assert(res.edges.toPairSeq == Seq(0 -> 2, 0 -> 3, 2 -> 0, 2 -> 3, 3 -> 0, 3 -> 2))
  }
}

class EdgesFromBipartiteAttributeMatchesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g1 = ExampleGraph()().result
    val g2 = SmallTestGraph(Map(0 -> Seq(1)))().result
    val g2g = {
      val op = AddVertexAttribute(Map(0 -> "Male", 1 -> "Female"))
      op(op.vs, g2.vs).result.attr
    }
    val op = EdgesFromBipartiteAttributeMatches[String]()
    val res = op(op.fromAttr, g1.gender)(op.toAttr, g2g).result
    assert(res.edges.toPairSeq == Seq(0 -> 0, 1 -> 1, 2 -> 0, 3 -> 0))
  }
}
