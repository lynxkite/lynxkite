package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class VertexSetIntersectionTest extends FunSuite with TestGraphOp {
  test("intersect 3 sets") {
    val aVS = 0 to 50
    val bVS = 5 to 10
    val cVS = 8 to 100
    val a = SmallTestGraph(aVS.map(_.toInt -> Seq()).toMap).result.vs
    val b = SmallTestGraph(bVS.map(_.toInt -> Seq()).toMap).result.vs
    val c = SmallTestGraph(cVS.map(_.toInt -> Seq()).toMap).result.vs
    val op = VertexSetIntersection(3)
    val out = op(op.vss(0), a)(op.vss(1), b)(op.vss(2), c).result
    assert(out.intersection.toSet == Set(8, 9, 10))
  }

  test("empty set") {
    val aVS = 0 to 5
    val bVS = 10 to 12
    val a = SmallTestGraph(aVS.map(_.toInt -> Seq()).toMap).result.vs
    val b = SmallTestGraph(bVS.map(_.toInt -> Seq()).toMap).result.vs
    val op = VertexSetIntersection(2)
    val out = op(op.vss(0), a)(op.vss(1), b).result
    assert(out.intersection.toSet == Set())
  }
}
