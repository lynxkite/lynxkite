package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class VertexSetIntersectionTest extends AnyFunSuite with TestGraphOp {

  private val age = {
    val range = 0 until 100
    val everybody = SmallTestGraph(range.map { id => (id, Seq[Int]()) }.toMap).result
    AddVertexAttribute.run(everybody.vs, range.map { a => (a, a.toDouble) }.toMap)
  }

  def agedBetween(lo: Double, hi: Double) = {
    implicit val d = SerializableType.double
    val filter = AndFilter(GE(lo), LE(hi))
    val op = VertexAttributeFilter(filter)
    op(op.attr, age).result.fvs
  }

  test("intersect 3 sets") {
    val aVS = agedBetween(0, 50)
    val bVS = agedBetween(5, 10)
    val cVS = agedBetween(8, 99)
    val op = VertexSetIntersection(3)
    val out = op(op.vss(0), aVS)(op.vss(1), bVS)(op.vss(2), cVS).result
    assert(out.intersection.toSeq == Seq(8, 9, 10))
  }

  test("empty set") {
    val aVS = agedBetween(0, 5)
    val bVS = agedBetween(10, 12)
    val op = VertexSetIntersection(2)
    val out = op(op.vss(0), aVS)(op.vss(1), bVS).result
    assert(out.intersection.toSeq == Seq())
  }
}
