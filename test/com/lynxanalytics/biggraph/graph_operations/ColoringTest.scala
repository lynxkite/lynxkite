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

  test("complete graph") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2, 3), 1 -> Seq(2, 3), 2 -> Seq(3), 3 -> Seq())).result
    val op = Coloring()
    val res = op(op.es, g.es)().result
    val color = res.coloring.rdd.collect.toSeq.sorted
    assert(color == Seq(0 -> 4.0, 1 -> 3.0, 2 -> 2.0, 3 -> 1.0))
  }
}
