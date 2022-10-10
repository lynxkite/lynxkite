package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class DispersionTest extends AnyFunSuite with TestGraphOp {
  test("two triangles sharing a common edge (both directions)") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(2, 3), 2 -> Seq(1, 3), 3 -> Seq()))().result
    val op = Dispersion()
    val out = op(op.es, g.es).result
    assert(out.dispersion.rdd.collect.toSeq.sorted == Seq(0 -> 0, 1 -> 0, 2 -> 1, 3 -> 0, 4 -> 1, 5 -> 0))
  }
  test("maximal clique of 4") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(2, 3), 2 -> Seq(3), 3 -> Seq(0)))().result
    val op = Dispersion()
    val out = op(op.es, g.es).result
    assert(out.dispersion.rdd.collect.toSeq.sorted == Seq(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0))
  }
  test("two triangles connected by an edge") {
    val g = SmallTestGraph(
      Map(0 -> Seq(1, 2), 1 -> Seq(2), 2 -> Seq(3), 3 -> Seq(4, 5), 4 -> Seq(5), 5 -> Seq()))().result
    val op = Dispersion()
    val out = op(op.es, g.es).result
    assert(out.dispersion.rdd.collect.toSeq.sorted ==
      Seq(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0))
  }
  test("two maximal cliques of 4 sharing a common edge") {
    val g = SmallTestGraph(
      Map(0 -> Seq(1, 2, 3), 1 -> Seq(2, 3), 2 -> Seq(3, 4), 3 -> Seq(4, 5), 4 -> Seq(5), 5 -> Seq(2)))().result
    val op = Dispersion()
    val out = op(op.es, g.es).result
    assert(out.dispersion.rdd.collect.toSeq.sorted ==
      Seq(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 4, 7 -> 0, 8 -> 0, 9 -> 0, 10 -> 0))
  }
  test("two triangles sharing a common edge, with loop edges") {
    val g = SmallTestGraph(Map(0 -> Seq(0, 1, 2), 1 -> Seq(1, 2, 3), 2 -> Seq(2, 3), 3 -> Seq(3)))().result
    val op = Dispersion()
    val out = op(op.es, g.es).result
    assert(out.dispersion.rdd.collect.toSeq.sorted == Seq(1 -> 0, 2 -> 0, 4 -> 1, 5 -> 0, 7 -> 0))
  }
}
