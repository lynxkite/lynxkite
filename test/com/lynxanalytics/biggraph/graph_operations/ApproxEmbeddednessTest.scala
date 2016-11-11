package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ApproxEmbeddednessTest extends FunSuite with TestGraphOp {
  test("two triangles sharing a common edge") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(2, 3), 2 -> Seq(3), 3 -> Seq()))().result
    val op = ApproxEmbeddedness(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map(0 -> 1, 1 -> 1, 2 -> 2, 3 -> 1, 4 -> 1))
  }

  test("directed triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0)))().result
    val op = ApproxEmbeddedness(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map(0 -> 1, 1 -> 1, 2 -> 1))
  }

  test("bi-directed triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1)))().result
    val op = ApproxEmbeddedness(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map(0 -> 1, 1 -> 1, 2 -> 1, 3 -> 1, 4 -> 1, 5 -> 1))
  }

  test("line graph") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(3)))().result
    val op = ApproxEmbeddedness(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map(0 -> 0, 1 -> 0, 2 -> 0))
  }

  test("loop edge - vertex has non-loop edges") {
    val g = SmallTestGraph(Map(0 -> Seq(0), 0 -> Seq(1), 2 -> Seq(0)))().result
    val op = ApproxEmbeddedness(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map(0 -> 0, 1 -> 0)) // Undefined for loop edge.
  }

  test("loop edge - vertex has only loop edges") {
    val g = SmallTestGraph(Map(0 -> Seq(0)))().result
    val op = ApproxEmbeddedness(bits = 8)
    val out = op(op.es, g.es).result
    assert(out.embeddedness.rdd.collect.toMap == Map()) // Undefined for loop edge.
  }
}
