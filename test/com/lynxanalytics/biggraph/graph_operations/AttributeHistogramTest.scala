package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.DoubleBucketer

class AttributeHistogramTest extends FunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("works on edges") {
    val bucketer = DoubleBucketer(0.0, 5.0, 2)
    val cop = CountVertices()
    val count = cop(cop.vertices, g.edges.asVertexSet).result.count
    val op = AttributeHistogram(bucketer)
    val out = op(
      op.original, g.edges.asVertexSet)(
        op.filtered, g.edges.asVertexSet)(
          op.attr, g.weight)(
            op.originalCount, count).result
    assert(out.counts.value === Map(0 -> 2, 1 -> 2))
  }
}
