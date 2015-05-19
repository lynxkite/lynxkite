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
}
