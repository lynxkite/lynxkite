package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class EdgeGraphTest extends AnyFunSuite with TestGraphOp {
  test("example graph") {
    val g = ExampleGraph()().result
    val op = EdgeGraph()
    val out = op(op.es, g.edges).result
    assert(out.newVS.toSeq == Seq(0, 1, 2, 3))
    assert(out.newES.toPairSeq == Seq(0 -> 1, 1 -> 0, 2 -> 0, 3 -> 1))
  }
}
