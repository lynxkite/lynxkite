package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class SetOverlapTest extends FunSuite with TestGraphOp {
  // Creates the graph specified by `nodes` and applies SetOverlap to it.
  // Returns the resulting edges in an easy-to-use format.
  def getOverlaps(nodes: Seq[(Seq[Int], Int)], minOverlap: Int): Map[(Int, Int), Int] = {
    val g = SegmentedTestGraph(nodes).result
    val op = SetOverlap(minOverlap)
    val so = op(op.vs, g.vs)(op.segments, g.segments)(op.belongsTo, g.belongsTo).result

    so.overlaps.rdd.join(so.overlapSize.rdd).map {
      case (id, (edge, value)) =>
        (edge.src.toInt, edge.dst.toInt) -> value
    }.collect.toMap
  }

  test("triangle") {
    val overlaps = getOverlaps(Seq(
      Seq(1, 2) -> 10,
      Seq(2, 3) -> 20,
      Seq(1, 3) -> 30),
      minOverlap = 1)
    assert(overlaps === Map((10, 20) -> 1, (10, 30) -> 1, (20, 10) -> 1, (20, 30) -> 1, (30, 10) -> 1, (30, 20) -> 1))
  }

  test("unsorted sets") {
    val overlaps = getOverlaps(Seq(
      Seq(2, 1) -> 10,
      Seq(3, 2) -> 20,
      Seq(3, 1) -> 30),
      minOverlap = 1)
    assert(overlaps === Map((10, 20) -> 1, (10, 30) -> 1, (20, 10) -> 1, (20, 30) -> 1, (30, 10) -> 1, (30, 20) -> 1))
  }

  test("minOverlap too high") {
    val overlaps = getOverlaps(Seq(
      Seq(1, 2) -> 10,
      Seq(2, 3) -> 20,
      Seq(1, 3) -> 30),
      minOverlap = 2)
    assert(overlaps === Map())
  }

  test("> 70 nodes") {
    // Tries to trigger the use of longer prefixes.
    val N = 100
    assert(SetOverlap.SetListBruteForceLimit < N)
    val overlaps = getOverlaps(
      (0 to N).map(i => Seq(-3, -2, -1, i) -> i),
      minOverlap = 2)
    val expected = for {
      a <- (0 to N)
      b <- (0 to N)
      if a != b
    } yield ((a, b) -> 3)
    assert(overlaps == expected.toMap)
  }
}
