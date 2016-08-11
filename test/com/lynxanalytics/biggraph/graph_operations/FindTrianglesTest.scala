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
    val ftOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(ftOut.segments.rdd.count == 0)
  }

  test("no triangles 2") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1),
      1 -> Seq(0, 2, 2),
      2 -> Seq(0, 0, 1)
    )).result
    val op = FindTriangles(needsBothDirections = true)
    val ftOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(ftOut.segments.rdd.count == 0)
  }

  test("ignore multiple edges") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 1, 1, 1, 1, 2),
      1 -> Seq(2, 2, 2, 0, 0),
      2 -> Seq(0, 0, 1, 1)
    )).result
    val opF = FindTriangles(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTriangles(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert(ftFOut.segments.rdd.count == 1 && ftTOut.segments.rdd.count == 1)
  }

  test("5-size clique") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2, 3, 4),
      1 -> Seq(2, 3, 4),
      2 -> Seq(3, 4),
      3 -> Seq(4),
      4 -> Seq()
    )).result
    val opF = FindTriangles(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTriangles(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert(ftFOut.segments.rdd.count == 10 && ftTOut.segments.rdd.count == 0)
  }

  test("5-size clique both directions") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2, 3, 4),
      1 -> Seq(0, 2, 3, 4),
      2 -> Seq(0, 1, 3, 4),
      3 -> Seq(0, 1, 2, 4),
      4 -> Seq(0, 1, 2, 3)
    )).result
    val opF = FindTriangles(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTriangles(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert(ftFOut.segments.rdd.count == 10 && ftTOut.segments.rdd.count == 10)
  }

  test("planar graph neighbouring triangles") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2),
      1 -> Seq(0, 2, 3),
      2 -> Seq(0, 1, 3),
      3 -> Seq(1, 2, 4, 5),
      4 -> Seq(3, 5),
      5 -> Seq(3, 4)
    )).result
    val opF = FindTriangles(needsBothDirections = false)
    val ftFOut = opF(opF.vs, g.vs)(opF.es, g.es).result
    val opT = FindTriangles(needsBothDirections = true)
    val ftTOut = opT(opT.vs, g.vs)(opT.es, g.es).result
    assert(ftFOut.segments.rdd.count == 3 && ftTOut.segments.rdd.count == 3)
  }
}
