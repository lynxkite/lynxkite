package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class FindTrianglesTest extends FunSuite with TestGraphOp {
  test("no triangles 1") {
    val g = SmallTestGraph(Map(
      0 -> Seq(0, 0, 1, 1, 2),
      1 -> Seq(),
      2 -> Seq(0),
      3 -> Seq(4),
      4 -> Seq(5),
      5 -> Seq(6),
      6 -> Seq(3)
    )).result
    val op = FindTriangles(needsBothDirections = false)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(fmcOut.segments.rdd.count == 0)
  }

  test("no triangles 2") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1),
      1 -> Seq(0, 2, 2),
      2 -> Seq(0, 0, 1)
    )).result
    val op = FindTriangles(needsBothDirections = true)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(fmcOut.segments.rdd.count == 0)
  }
}
