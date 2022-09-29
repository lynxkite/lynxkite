package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.GraphTestUtils._
import com.lynxanalytics.lynxkite.graph_api.Scripting._

class DapcstpTest extends AnyFunSuite with TestGraphOp {
  test("shortest path as Steiner-tree", com.lynxanalytics.lynxkite.SphynxOnly) {
    val graph = SmallTestGraph(
      Map(
        0 -> Seq(1, 2),
        1 -> Seq(2),
        2 -> Seq(4),
        3 -> Seq(5),
        4 -> Seq(3),
        5 -> Seq())).result
    val cost = AddVertexAttribute.run(
      graph.es.idSet,
      Map(0 -> 4.0, 1 -> 2.0, 2 -> 5.0, 3 -> 3.0, 4 -> 4.0, 5 -> 11.0))
    val root = AddVertexAttribute.run(graph.vs, Map(0 -> 0.0))
    val gain = AddVertexAttribute.run(
      graph.vs,
      Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 0.0, 4 -> 0.0, 5 -> ((2 + 3 + 4 + 11) + 42.0)))

    val op = Dapcstp()
    val profit = op(op.vs, graph.vs)(
      op.es,
      graph.es)(
      op.edge_costs,
      cost)(
      op.root_costs,
      root)(
      op.gain,
      gain).result.profit.value

    assert(profit == 42.0)
  }
}
