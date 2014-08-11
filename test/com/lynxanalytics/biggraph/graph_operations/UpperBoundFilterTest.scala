package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class UpperBoundFilterTest extends FunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("vertex and edge filtering works") {
    val op = UpperBoundFilter(21.0)
    val f = op(op.attr, g.age)(op.vs, g.vertices).result
    assert(f.fvs.toSet == Set(0L, 1L, 3L))
    val ind = InducedEdgeBundle()
    val joined = ind(ind.edges, g.edges)(ind.srcSubset, f.fvs.entity)(ind.dstSubset, f.fvs.entity)
      .result
    assert(joined.induced.toPairSet == Set(0L -> 1L, 1L -> 0L))
  }
  test("filtering on strings works") {
    val op = UpperBoundFilter("Bz")
    val f = op(op.attr, g.name)(op.vs, g.vertices).result
    assert(f.fvs.toSet == Set(0L, 2L))
  }
}
