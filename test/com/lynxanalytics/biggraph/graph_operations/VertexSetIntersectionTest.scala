package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class VertexSetIntersectionTest extends FunSuite with TestGraphOp {
  test("example graph") {
    val aVS = 0 to 50
    val bVS = 5 to 10
    val cVS = 8 to 100
    val a = SmallTestGraph(aVS.map(_.toInt -> Seq()).toMap).result.vs
    val b = SmallTestGraph(bVS.map(_.toInt -> Seq()).toMap).result.vs
    val c = SmallTestGraph(cVS.map(_.toInt -> Seq()).toMap).result.vs
    val op = VertexSetIntersection(3)
    val out = op(op.vss, List(a, b, c)).result
    assert(out.clustering.rdd.collect.toMap == Map(0 -> 0.5, 1 -> 0.5, 2 -> 1.0, 3 -> 1.0))
  }
}
