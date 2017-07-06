package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ComputeVertexNeighborhoodTest extends FunSuite with TestGraphOp {

  test("vertex neighborhood of example graph using triplets") {
    val g = ExampleGraph()().result
    val triplets = {
      val op = EdgeAndNeighborMapping()
      op(op.edges, g.edges).result
    }
    val nop = ComputeVertexNeighborhood(Seq(2, 3), 1, 10)
    val nopres = nop(
      nop.vertices, g.vertices)(
        nop.srcMapping, triplets.srcEdges)(
          nop.dstMapping, triplets.dstEdges).result
    assert(nopres.neighborhood.value == Set(0, 1, 2, 3))
  }

  test("vertex neighborhood radius 2") {
    val edges = Map(
      0 -> Seq(1, 2),
      1 -> Seq(3, 4), 2 -> Seq(5, 6),
      3 -> Seq(7, 8), 4 -> Seq(9, 10), 5 -> Seq(11, 12), 6 -> Seq(13, 14))
    val g = SmallTestGraph(edges)().result
    val triplets = {
      val op = EdgeAndNeighborMapping()
      op(op.edges, g.es).result
    }
    val neighborhood = {
      val op = ComputeVertexNeighborhood(Seq(0), 2, 10)
      op(
        op.vertices, g.vs)(
          op.srcMapping, triplets.srcEdges)(
            op.dstMapping, triplets.dstEdges).result.neighborhood.value
    }
    assert(neighborhood == Set(0, 1, 2, 3, 4, 5, 6))
  }

}
