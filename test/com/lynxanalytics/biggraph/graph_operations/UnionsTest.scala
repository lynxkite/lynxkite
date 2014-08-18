package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class UnionsTest extends FunSuite with TestGraphOp {
  test("vertex union of three example graphs") {
    val g = ExampleGraph()().result
    val op = VertexSetUnion(3)
    val verticesEntity = g.vertices.entity
    val out = op(op.vss, Seq(verticesEntity, verticesEntity, verticesEntity)).result
    val sourceVertices = g.vertices.toSet
    val unionVertices = out.union.toSet
    assert(unionVertices.size == sourceVertices.size * 3)
    var targetUnion = Set[ID]()
    for (i <- 0 until 3) {
      val inj = out.injections(i).toPairSet
      // Source set == vertex set of one source
      assert(inj.map(_._1) == sourceVertices)
      targetUnion ++= inj.map(_._2)
    }
    assert(targetUnion == unionVertices)
  }
}
