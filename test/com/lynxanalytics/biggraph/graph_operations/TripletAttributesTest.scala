package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class TripletAttributesTest extends FunSuite with TestGraphOp {
  test("triplets of example graph") {
    val g = ExampleGraph()().result
    val mapping = TripletMapping()
    val mapped = mapping(mapping.edges, g.edges).result
    assert(mapped.srcEdges.rdd.collect.toMap.mapValues(_.toSet) == Map(0 -> Set(0), 1 -> Set(1), 2 -> Set(2, 3), 3 -> Set()))
    assert(mapped.dstEdges.rdd.collect.toMap.mapValues(_.toSet) == Map(0 -> Set(1, 2), 1 -> Set(0, 3), 2 -> Set(), 3 -> Set()))
    val op = VertexToEdgeAttribute[Double]()
    val res = op(op.mapping, mapped.dstEdges)(op.original, g.age)(op.target, g.edges).result
    assert(res.mappedAttribute.rdd.collect.toMap == Map(0 -> 18.2, 1 -> 20.3, 2 -> 20.3, 3 -> 18.2))
  }

  test("edges and neighbors of example graph") {
    val g = ExampleGraph()().result
    val mapping = EdgeAndNeighborMapping()
    val mapped = mapping(mapping.edges, g.edges).result
    assert(mapped.srcEdges.rdd.mapValues(_.map((e, n) => (e, n)).toSet).collect.toMap ==
      Map(0 -> Set((0, 1)), 1 -> Set((1, 0)), 2 -> Set((2, 0), (3, 1)), 3 -> Set()))
    assert(mapped.dstEdges.rdd.mapValues(_.map((e, n) => (e, n)).toSet).collect.toMap ==
      Map(0 -> Set((1, 1), (2, 2)), 1 -> Set((0, 0), (3, 2)), 2 -> Set(), 3 -> Set()))
    val res1 = {
      val op = EdgesForVerticesFromEdgesAndNeighbors(Set(2), None, maxNumEdges = Int.MaxValue)
      op(op.mapping, mapped.srcEdges).result
    }
    assert(res1.edges.value == Some(Vector((2, Edge(2, 0)), (3, Edge(2, 1)))))
    val res2 = {
      val op = EdgesForVerticesFromEdgesAndNeighbors(Set(2), Some(Set(1)), maxNumEdges = Int.MaxValue)
      op(op.mapping, mapped.srcEdges).result
    }
    assert(res2.edges.value == Some(Vector((3, Edge(2, 1)))))
  }
}
