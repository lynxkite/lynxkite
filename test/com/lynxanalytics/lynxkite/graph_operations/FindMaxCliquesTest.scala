package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._

class FindMaxCliquesTest extends AnyFunSuite with TestGraphOp {
  test("triangle") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))).result
    val op = FindMaxCliques(3, needsBothDirections = true)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    assert(fmcOut.segments.rdd.count == 1)
  }

  test("test CheckClique") {
    val g = SmallTestGraph(Map(
      0 -> Seq(1, 2, 3),
      1 -> Seq(0, 2),
      2 -> Seq(0, 1),
      3 -> Seq())).result
    val s = SmallTestGraph(Map(
      10 -> Seq(),
      20 -> Seq(),
      30 -> Seq(),
      40 -> Seq())).result
    val bTo = {
      val op = AddEdgeBundle(Seq(
        0 -> 10,
        1 -> 10,
        2 -> 10,
        1 -> 20,
        2 -> 20,
        3 -> 30,
        0 -> 40,
        3 -> 40))
      op(op.vsA, g.vs)(op.vsB, s.vs).result
    }
    val check = {
      val op = CheckClique(needsBothDirections = true)
      op(op.es, g.es)(op.belongsTo, bTo.esAB).result
    }
    // we expect clique 20 to be invalid as it is non maximal
    // we expect clique 40 to be invalid as there is no 3 -> 0 edge
    assert(check.invalid.value.toSet == Set(20, 40))
  }

  test("check if a clique from triangle is a clique") {
    val g = SmallTestGraph(Map(0 -> Seq(1, 2), 1 -> Seq(0, 2), 2 -> Seq(0, 1))).result
    val op = FindMaxCliques(3, needsBothDirections = true)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    val check = {
      val op = CheckClique(needsBothDirections = true)
      op(op.vs, g.vs)(op.es, g.es)(op.cliques, fmcOut.segments)(op.belongsTo, fmcOut.belongsTo).result
    }
    assert(check.invalid.value.size == 0)
  }

  test("check if a clique from a DAG triangle is a clique") {
    val g = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(2), 2 -> Seq(0))).result
    val op = FindMaxCliques(3, needsBothDirections = false)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    val check = {
      val op = CheckClique(needsBothDirections = false)
      op(op.vs, g.vs)(op.es, g.es)(op.cliques, fmcOut.segments)(op.belongsTo, fmcOut.belongsTo).result
    }
    assert(check.invalid.value.size == 0)
  }

  test("another directed triangle clique") {
    val g = SmallTestGraph(Map(0 -> Seq(2), 1 -> Seq(0, 2), 2 -> Seq())).result
    val op = FindMaxCliques(3, needsBothDirections = false)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    val check = {
      val op = CheckClique(needsBothDirections = false)
      op(op.vs, g.vs)(op.es, g.es)(op.cliques, fmcOut.segments)(op.belongsTo, fmcOut.belongsTo).result
    }
    assert(fmcOut.segments.rdd.count == 1)
    assert(check.invalid.value.size == 0)
  }

  test("directed square clique") {
    val g = SmallTestGraph(Map(0 -> Seq(2), 1 -> Seq(0, 2, 3), 2 -> Seq(), 3 -> Seq(0, 2))).result
    val op = FindMaxCliques(3, needsBothDirections = false)
    val fmcOut = op(op.vs, g.vs)(op.es, g.es).result
    val check = {
      val op = CheckClique(needsBothDirections = false)
      op(op.vs, g.vs)(op.es, g.es)(op.cliques, fmcOut.segments)(op.belongsTo, fmcOut.belongsTo).result
    }
    assert(fmcOut.segments.rdd.count == 1)
    assert(check.invalid.value.size == 0)
  }
}
