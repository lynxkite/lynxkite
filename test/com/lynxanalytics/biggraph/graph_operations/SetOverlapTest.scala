package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.TestUtils
import com.lynxanalytics.biggraph.graph_api._

class SetOverlapTest extends FunSuite with TestGraphOperation {
  // Creates the graph specified by `nodes` and applies SetOverlap to it.
  // Returns the resulting edges in an easy-to-use format.
  def getOverlaps(nodes: Map[Int, Seq[Int]], minOverlap: Int): Map[(Int, Int), Int] = {
    val (vs, sets, links) = helper.groupedGraph(nodes)
    val so = helper.apply(SetOverlap(minOverlap), 'vs -> vs, 'sets -> sets, 'links -> links)
    helper.localData(so.edgeAttributes('overlap_size).runtimeSafeCast[Int])
      .map { case ((a, b), c) => ((a.toInt, b.toInt), c) }
  }

  test("triangle") {
    val overlaps = getOverlaps(Map(
      0 -> Seq(1, 2),
      1 -> Seq(2, 3),
      2 -> Seq(1, 3)),
      minOverlap = 1)
    assert(overlaps === Map(((0, 1) -> 1), ((0, 2) -> 1), ((1, 0) -> 1), ((1, 2) -> 1), ((2, 0) -> 1), (2, 1) -> 1))
  }

  test("unsorted sets") {
    val overlaps = getOverlaps(Map(
      0 -> Seq(2, 1),
      1 -> Seq(3, 2),
      2 -> Seq(3, 1)),
      minOverlap = 1)
    assert(overlaps === Map(((0, 1) -> 1), ((0, 2) -> 1), ((1, 0) -> 1), ((1, 2) -> 1), ((2, 0) -> 1), (2, 1) -> 1))
  }

  test("minOverlap too high") {
    val overlaps = getOverlaps(Map(
      0 -> Seq(1, 2),
      1 -> Seq(2, 3),
      2 -> Seq(1, 3)),
      minOverlap = 2)
    assert(overlaps === Map())
  }

  test("> 70 nodes") {
    // Tries to trigger the use of longer prefixes.
    val N = 100
    assert(SetOverlap.SetListBruteForceLimit < N)
    val overlaps = getOverlaps(
      (0 to N).map(i => i -> Seq(-3, -2, -1, i)).toMap,
      minOverlap = 2)
    val expected = for {
      a <- (0 to N)
      b <- (0 to N)
      if a != b
    } yield ((a, b) -> 3)
    assert(overlaps == expected.toMap)
  }
}
