package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ColoringTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val eg = ExampleGraph()().result
    val op = Coloring()
    val res = op(op.es, eg.edges)().result
    val color = res.coloring.rdd.collect.toSeq.sorted
    assert(color == Seq(0 -> 2.0, 1 -> 1.0, 2 -> 3.0, 3 -> 1.0))
  }
}
