package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class VertexSetIntersectionTest extends FunSuite with TestGraphOp {
  private val parent = CreateVertexSet(100).result
  private val opToDouble = AddDoubleVertexAttribute((0 until 100).map { a => (a, a.toDouble) }.toMap)
  private val parentAttr = opToDouble(opToDouble.vs, parent.vs).result.attr

  def getVertexSet(lo: Double, hi: Double) = {
    val filter = AndFilter(DoubleGE(lo), DoubleLE(hi))
    val op = VertexAttributeFilter(filter)
    op(op.attr, parentAttr).result.fvs
  }

  test("intersect 3 sets") {
    val aVS = getVertexSet(0, 50)
    val bVS = getVertexSet(5, 10)
    val cVS = getVertexSet(8, 99)
    val op = VertexSetIntersection(3)
    val out = op(op.vss(0), aVS)(op.vss(1), bVS)(op.vss(2), cVS).result
    assert(out.intersection.toSeq == Seq(8, 9, 10))
  }

  test("empty set") {
    val aVS = getVertexSet(0, 5)
    val bVS = getVertexSet(10, 12)
    val op = VertexSetIntersection(2)
    val out = op(op.vss(0), aVS)(op.vss(1), bVS).result
    assert(out.intersection.toSeq == Seq())
  }
}
