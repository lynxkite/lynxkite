package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.DoubleLinearBucketer

class AttributeHistogramTest extends FunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("works on edges") {
    val bucketer = DoubleLinearBucketer(0.0, 5.0, 2)
    val count = CountVertices.run(g.edges.idSet)
    val op = AttributeHistogram(bucketer)
    val out = op(
      op.original, g.edges.idSet)(
        op.filtered, g.edges.idSet)(
          op.attr, g.weight)(
            op.originalCount, count).result
    assert(out.counts.value === Map(0 -> 2, 1 -> 2))
  }
}
