package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class AddRankingAttributeTest extends AnyFunSuite with TestGraphOp {
  val graph = SmallTestGraph(
    Map(
      0 -> Seq(),
      1 -> Seq(),
      2 -> Seq(),
      3 -> Seq())).result

  test("double ranking") {
    val sortKey = AddVertexAttribute.run(graph.vs, Map(0 -> 4.0, 2 -> 5.0, 3 -> -1.0))
    val ascOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = true))
    assert(ascOrdinal == Map(0 -> 1, 2 -> 2, 3 -> 0))
    val descOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = false))
    assert(descOrdinal == Map(0 -> 1, 2 -> 0, 3 -> 2))
  }

  test("integer ranking") {
    val sortKey = AddVertexAttribute.run(graph.vs, Map(0 -> 4, 2 -> 5, 3 -> -1))
    val ascOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = true))
    assert(ascOrdinal == Map(0 -> 1, 2 -> 2, 3 -> 0))
    val descOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = false))
    assert(descOrdinal == Map(0 -> 1, 2 -> 0, 3 -> 2))
  }

  test("string ranking") {
    val sortKey = AddVertexAttribute.run(graph.vs, Map(0 -> "4", 2 -> "5", 3 -> "-1"))
    val ascOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = true))
    assert(ascOrdinal == Map(0 -> 1, 2 -> 2, 3 -> 0))
    val descOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = false))
    assert(descOrdinal == Map(0 -> 1, 2 -> 0, 3 -> 2))
  }

}
