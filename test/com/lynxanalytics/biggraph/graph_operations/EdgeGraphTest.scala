package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class EdgeGraphTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = EdgeGraph()
    val out = op(op.es, g.edges).result
    assert(out.newVS.toSet == Set(0, 1, 2, 3))
    assert(out.newES.toMap == Map(1 -> 0, 3 -> 1, 2 -> 0, 0 -> 1))
  }
}
