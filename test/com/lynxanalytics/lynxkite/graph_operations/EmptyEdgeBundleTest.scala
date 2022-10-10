package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class EmptyEdgeBundleTest extends AnyFunSuite with TestGraphOp {
  test("empty means empty") {
    val g1 = ExampleGraph()().result
    val g2 = {
      val op = EdgeGraph()
      op(op.es, g1.edges).result
    }
    val op = EmptyEdgeBundle()
    val res = op(op.src, g1.vertices)(op.dst, g2.newVS).result.eb
    assert(res.toPairSeq == Seq())
  }
}
