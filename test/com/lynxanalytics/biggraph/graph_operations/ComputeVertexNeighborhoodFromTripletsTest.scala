package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class ComputeVertexNeighborhoodFromTripletsTest extends FunSuite with TestGraphOp {

  test("vertex neighborhood of example graph using triplets") {
    val g = ExampleGraph()().result
    val triplets = {
      val op = TripletMapping()
      op(op.edges, g.edges).result
    }
    val nop = ComputeVertexNeighborhoodFromTriplets(Seq(2, 3), 1)
    val nopres = nop(
      nop.vertices, g.vertices)(
        nop.edges, g.edges)(
          nop.srcTripletMapping, triplets.srcEdges)(
            nop.dstTripletMapping, triplets.dstEdges).result
    assert(nopres.neighborhood.value == Set(0, 1, 2, 3))
  }

}