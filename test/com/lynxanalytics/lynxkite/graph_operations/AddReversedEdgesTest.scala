package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class AddReversedEdgesTest extends AnyFunSuite with TestGraphOp {
  test("Enhanced example graph") {
    val g = EnhancedExampleGraph()().result
    val op = AddReversedEdges(addIsNewAttr = true)
    val out = op(op.es, g.edges).result
    val reversedEdges = g.edges.toPairSeq.map(_.swap)
    val expected = g.edges.toPairSeq ++ reversedEdges
    assert(out.esPlus.toPairSeq == expected.sorted)

    val numOrigEdges = g.edges.rdd.count()
    assert(out.isNew.rdd.filter(_._2 == 0L).count == numOrigEdges)
    assert(out.isNew.rdd.filter(_._2 == 1L).count == numOrigEdges)
  }
}
