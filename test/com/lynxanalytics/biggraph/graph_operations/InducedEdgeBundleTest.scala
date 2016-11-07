package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class InducedEdgeBundleTest extends FunSuite with TestGraphOp {
  val example = ExampleGraph()().result
  val adamless = {
    val op = VertexAttributeFilter(NotFilter(OneOf(Set("Adam"))))
    op(op.attr, example.name).result
  }

  test("example graph induce src & dst") {
    val op = InducedEdgeBundle()
    val induced = op(
      op.edges, example.edges)(
        op.srcMapping, ReverseEdges.run(adamless.identity))(
          op.dstMapping, ReverseEdges.run(adamless.identity))
      .result.induced.toPairSeq
    assert(induced == Seq(2 -> 1))
  }

  test("example graph induce src") {
    val op = InducedEdgeBundle(induceDst = false)
    val induced = op(
      op.edges, example.edges)(
        op.srcMapping, ReverseEdges.run(adamless.identity)).result.induced.toPairSeq
    assert(induced == Seq(1 -> 0, 2 -> 0, 2 -> 1))
  }

  test("example graph induce dst") {
    val op = InducedEdgeBundle(induceSrc = false)
    val induced = op(
      op.edges, example.edges)(
        op.dstMapping, ReverseEdges.run(adamless.identity)).result.induced.toPairSeq
    assert(induced == Seq(0 -> 1, 2 -> 1))
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
    val v = merge.segments.rdd.keys.collect.toSeq
    assert(induced.toPairSeq == Seq(v(0) -> v(0), v(0) -> v(1), v(0) -> v(1), v(1) -> v(0)))
  }

  test("induce with non-function mapping") {
    // Start with a graph that has two strongly connected components of two nodes each.
    val graph = SmallTestGraph(Map(0 -> Seq(1), 1 -> Seq(0), 2 -> Seq(3), 3 -> Seq(2))).result
    // Find connected components.
    val components = {
      val op = ConnectedComponents()
      op(op.es, graph.es).result
    }
    // Connect the segments.
    val componentEdges = {
      val op = EdgesFromAttributeMatches[Double]()
      op(op.attr, AddConstantAttribute.run(components.segments, 1.0)).result.edges
    }
    assert(componentEdges.toPairSeq == Seq(0 -> 2, 2 -> 0))
    // Propagate back the new edges to the base vertices.
    val induced = {
      val op = InducedEdgeBundle()
      op(
        op.edges, componentEdges)(
          op.srcMapping, ReverseEdges.run(components.belongsTo))(
            op.dstMapping, ReverseEdges.run(components.belongsTo))
        .result.induced
    }
    val (ids, edges) = induced.toIdPairSeq.unzip
    assert(edges.sorted == Seq(
      0 -> 2, 0 -> 3, 1 -> 2, 1 -> 3,
      2 -> 0, 2 -> 1, 3 -> 0, 3 -> 1))
    assert(ids.size == ids.toSet.size) // No duplicate IDs.
  }
}
