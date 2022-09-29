package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class UnionsTest extends AnyFunSuite with TestGraphOp {
  test("vertex union of three example graphs") {
    val g = ExampleGraph()().result
    val op = VertexSetUnion(3)
    val verticesEntity = g.vertices.entity
    val out = op(op.vss, Seq(verticesEntity, verticesEntity, verticesEntity)).result
    val sourceVertices = g.vertices.toSeq
    val unionVertices = out.union.toSeq
    assert(unionVertices.size == sourceVertices.size * 3)
    var targetUnion = Set[ID]()
    for (i <- 0 until 3) {
      val inj = out.injections(i).toPairSeq
      // Source set == vertex set of one source
      assert(inj.map(_._1) == sourceVertices)
      targetUnion ++= inj.map(_._2)
    }
    assert(targetUnion.toSeq.sorted == unionVertices)
  }
}
