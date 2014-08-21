package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class InducedEdgeBundleTest extends FunSuite with TestGraphOp {
  val example = ExampleGraph()().result
  val adamless = {
    val op = VertexAttributeFilter(new Filter[String] {
      def matches(name: String) = name != "Adam"
    })
    op(op.attr, example.name).result
  }

  test("example graph induce src & dst") {
    val op = InducedEdgeBundle()
    val induced = op(
      op.edges, example.edges)(
        op.srcMapping, ReverseEdges.run(adamless.identity))(
          op.dstMapping, ReverseEdges.run(adamless.identity))
      .result.induced.toPairSet
    assert(induced == Set(2 -> 1))
  }

  test("example graph induce src") {
    val op = InducedEdgeBundle(induceDst = false)
    val induced = op(
      op.edges, example.edges)(
        op.srcMapping, ReverseEdges.run(adamless.identity)).result.induced.toPairSet
    assert(induced == Set(2 -> 1, 1 -> 0, 2 -> 0))
  }

  test("example graph induce dst") {
    val op = InducedEdgeBundle(induceSrc = false)
    val induced = op(
      op.edges, example.edges)(
        op.dstMapping, ReverseEdges.run(adamless.identity)).result.induced.toPairSet
    assert(induced == Set(2 -> 1, 0 -> 1))
  }
}
