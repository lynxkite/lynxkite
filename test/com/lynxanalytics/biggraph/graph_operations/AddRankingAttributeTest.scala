package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class AddRankingAttributeTest extends FunSuite with TestGraphOp {
  val graph = SmallTestGraph(
    Map(
      0 -> Seq(),
      1 -> Seq(),
      2 -> Seq(),
      3 -> Seq())).result

  test("double ranking", com.lynxanalytics.biggraph.SphynxOnly) {
    val sortKey = AddVertexAttribute.run(graph.vs, Map(0 -> 4.0, 2 -> 5.0, 3 -> -1.0))
    val ascOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = true))
    assert(ascOrdinal == Map(0 -> 1, 2 -> 2, 3 -> 0))
    val descOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = false))
    assert(descOrdinal == Map(0 -> 1, 2 -> 0, 3 -> 2))
  }

  test("integer ranking", com.lynxanalytics.biggraph.SphynxOnly) {
    val sortKey = AddVertexAttribute.run(graph.vs, Map(0 -> 4, 2 -> 5, 3 -> -1))
    val ascOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = true))
    assert(ascOrdinal == Map(0 -> 1, 2 -> 2, 3 -> 0))
    val descOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = false))
    assert(descOrdinal == Map(0 -> 1, 2 -> 0, 3 -> 2))
  }

  test("string ranking", com.lynxanalytics.biggraph.SphynxOnly) {
    val sortKey = AddVertexAttribute.run(graph.vs, Map(0 -> "4", 2 -> "5", 3 -> "-1"))
    val ascOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = true))
    assert(ascOrdinal == Map(0 -> 1, 2 -> 2, 3 -> 0))
    val descOrdinal = get(AddRankingAttribute.run(attr = sortKey, ascending = false))
    assert(descOrdinal == Map(0 -> 1, 2 -> 0, 3 -> 2))
  }

}
