package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class UpperBoundFilterTest extends FunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("vertex and edge filtering works") {
    val op = UpperBoundFilter(21.0)
    val filtered = op(op.attr, g.age)(op.vs, g.vertices).result
    assert(filtered.fvs.toSet == Set(0L, 1L, 3L))
    val ind = InducedEdgeBundle()
    val joined = (ind(ind.srcEdges, g.edges)(ind.src, g.vertices)(ind.dst, g.vertices)(ind.srcSubset, filtered.fvs.entity)(ind.dstSubset, filtered.fvs.entity)).result
    assert(joined.induced.toMap == Map(0L -> 1L, 1L -> 0L))
  }
  test("filtering on strings works") {
    val op = UpperBoundFilter("Bz")
    val filtered = op(op.attr, g.name)(op.vs, g.vertices).result
    assert(filtered.fvs.toSet == Set(0L, 2L))
  }
}
