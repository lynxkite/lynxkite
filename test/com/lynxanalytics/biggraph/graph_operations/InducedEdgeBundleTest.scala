package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class InducedEdgeBundleTest extends FunSuite with TestGraphOp {
  val example = ExampleGraph()().result
  val bobless = {
    val op = VertexAttributeFilter(NotFilter(OneOf(Set("Bob"))))
    op(op.attr, example.name).result
  }

  test("example graph induce src & dst") {
    val op = InducedEdgeBundle()
    val induced = op(
      op.edges, example.edges)(
        op.srcMapping, ReverseEdges.run(bobless.identity))(
          op.dstMapping, ReverseEdges.run(bobless.identity))
      .result.induced.toPairSeq
    assert(induced == Seq(0 -> 1, 1 -> 0, 4 -> 5, 5 -> 4, 5 -> 4, 5 -> 7, 6 -> 4))
  }

  test("example graph induce src") {
    val op = InducedEdgeBundle(induceDst = false)
    val induced = op(
      op.edges, example.edges)(
        op.srcMapping, ReverseEdges.run(bobless.identity)).result.induced.toPairSeq
    assert(induced ==
      Seq(0 -> 1, 1 -> 0, 4 -> 2, 4 -> 2, 4 -> 5, 5 -> 2, 5 -> 4, 5 -> 4, 5 -> 7, 6 -> 4))
  }

  test("example graph induce dst") {
    val op = InducedEdgeBundle(induceSrc = false)
    val induced = op(
      op.edges, example.edges)(
        op.dstMapping, ReverseEdges.run(bobless.identity)).result.induced.toPairSeq
    assert(induced ==
      Seq(0 -> 1, 1 -> 0, 2 -> 0, 2 -> 1, 2 -> 4, 2 -> 5, 2 -> 5, 4 -> 5, 5 -> 4,
        5 -> 4, 5 -> 7, 6 -> 4))
  }

  test("example graph induce on merged") {
    val merge = {
      val op = MergeVertices[String]()
      op(op.attr, example.gender).result
    }
    val induced = {
      val op = InducedEdgeBundle()
      op(
        op.edges, example.edges)(
          op.srcMapping, merge.belongsTo)(
            op.dstMapping, merge.belongsTo)
        .result.induced
    }
    assert(induced.toPairCounts == Map((7, 3) -> 4, (7, 7) -> 4, (3, 3) -> 2, (3, 7) -> 6))
  }
}
