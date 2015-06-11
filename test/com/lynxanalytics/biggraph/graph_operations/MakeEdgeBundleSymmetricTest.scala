package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class RemoveNonSymmetricEdgesTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = RemoveNonSymmetricEdges()
    val out = op(op.es, g.edges).result
    assert(out.symmetric.toPairSeq == Seq(0 -> 1, 1 -> 0))
  }
}
