package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._

class EmbeddednessTest extends AnyFunSuite with TestGraphOp {
  test("two triangles sharing a common edge") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(2, 3), 2 -> Seq(3), 3 -> Seq()))().result
    val op = Embeddedness()
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map(0 -> 1, 1 -> 1, 2 -> 2, 3 -> 1, 4 -> 1))
  }
}
