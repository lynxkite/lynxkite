package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._

class ConcatenateBundlesTest extends FunSuite with TestGraphOperation {
  def concatEdges(AB: Map[Int, Seq[Int]], BC: Map[Int, Seq[Int]]): Map[(Int, Int), Double] = {
    val (vsA, vsB, edgesAB, weightsAB) = helper.groupedGraph(AB)
    val (_, vsC, edgesBC, weightsBC) = helper.groupedGraph(BC)
    val cb = helper.apply(
      ConcatenateBundles(),
      'vsA -> vsA, 'vsB -> vsB, 'vsC -> vsC,
      'edgesAB -> edgesAB, 'edgesBC -> edgesBC,
      'weightsAB -> weightsAB, 'weightsBC -> weightsBC)
    helper.localData(cb.edgeAttributes('weightsAC).runtimeSafeCast[Double])
      .map { case ((a, b), c) => ((a.toInt, b.toInt), c) }
  }

  test("no edge") {
    val AB = Map(10 -> Seq(1))
    val BC = Map(100 -> Seq(20))
    assert(concatEdges(AB, BC) === Map())
  }

  test("isolated edges") {
    val AB = Map(10 -> Seq(1), 20 -> Seq(2))
    val BC = Map(100 -> Seq(10), 200 -> Seq(20))
    assert(concatEdges(AB, BC) === Map((1, 100) -> 1.0, (2, 200) -> 1.0))
  }

  test("one to many to one") {
    val AB = Map(10 -> Seq(1), 20 -> Seq(1), 30 -> Seq(1), 40 -> Seq(1))
    val BC = Map(100 -> Seq(10, 20, 30, 40))
    assert(concatEdges(AB, BC) === Map((1, 100) -> 4.0))
  }

  test("many to one to many") {
    val AB = Map(10 -> Seq(1, 2, 3, 4))
    val BC = Map(100 -> Seq(10), 200 -> Seq(10), 300 -> Seq(10), 400 -> Seq(10))
    assert(concatEdges(AB, BC) === Map(
      (1, 100) -> 1.0, (1, 200) -> 1.0, (1, 300) -> 1.0, (1, 400) -> 1.0,
      (2, 100) -> 1.0, (2, 200) -> 1.0, (2, 300) -> 1.0, (2, 400) -> 1.0,
      (3, 100) -> 1.0, (3, 200) -> 1.0, (3, 300) -> 1.0, (3, 400) -> 1.0,
      (4, 100) -> 1.0, (4, 200) -> 1.0, (4, 300) -> 1.0, (4, 400) -> 1.0))
  }

  test("mix of the above") {
    val AB = Map(10 -> Seq(1, 2), 20 -> Seq(1, 3), 30 -> Seq(4))
    val BC = Map(100 -> Seq(10, 20), 200 -> Seq(20), 300 -> Seq(30), 400 -> Seq(40))
    assert(concatEdges(AB, BC) === Map(
      (1, 100) -> 2.0,
      (2, 100) -> 1.0, (3, 100) -> 1.0, (1, 200) -> 1.0, (3, 200) -> 1.0, (4, 300) -> 1.0))
  }
}
