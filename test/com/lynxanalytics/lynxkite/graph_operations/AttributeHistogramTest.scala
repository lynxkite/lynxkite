package com.lynxanalytics.lynxkite.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_api.Scripting._
import com.lynxanalytics.lynxkite.graph_util.DoubleLinearBucketer

class AttributeHistogramTest extends AnyFunSuite with TestGraphOp {
  val g = ExampleGraph()().result

  test("works on edges") {
    val bucketer = DoubleLinearBucketer(0.0, 5.0, 2)
    val count = Count.run(g.edges.idSet)
    val op = AttributeHistogram(bucketer, 50000)
    val out = op(
      op.original,
      g.edges.idSet)(
      op.filtered,
      g.edges.idSet)(
      op.attr,
      g.weight)(
      op.originalCount,
      count).result
    assert(out.counts.value === Map(0 -> 2, 1 -> 2))
  }
}
