package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class CompareSegmentationEdgesTest extends AnyFunSuite with TestGraphOp {

  test("precision and recall in simple graph segmentations") {
    // Parent graph:
    val graph = SmallTestGraph(
      Map(
        0 -> Seq(),
        1 -> Seq(),
        2 -> Seq(),
        3 -> Seq(),
        4 -> Seq(),
        5 -> Seq())).result
    val vs = graph.vs
    // "golden" segmentation:
    val goldenBelongsTo = {
      val op = LoopEdgeBundle()
      op(op.vs, vs).result.eb
    }
    val goldenEs = {
      val op = AddEdgeBundle(Seq((1, 2), (1, 3), (1, 4), (1, 5)))
      op(op.vsA, vs)(op.vsB, vs).result.esAB
    }
    // "test" segmentation:
    val testVs = graph.vs
    val testBelongsTo = goldenBelongsTo
    val testEs = {
      val op = AddEdgeBundle(Seq((1, 4), (1, 5), (2, 5)))
      op(op.vsA, vs)(op.vsB, vs).result.esAB
    }
    // Run operation:
    val op = CompareSegmentationEdges()
    val result = op(op.goldenBelongsTo, goldenBelongsTo)(
      op.testBelongsTo,
      testBelongsTo)(
      op.goldenEdges,
      goldenEs)(
      op.testEdges,
      testEs).result
    assert(Math.abs(2.0 / 3.0 - result.precision.entity.value) < 0.00001)
    assert(Math.abs(0.5 - result.recall.entity.value) < 0.00001)
  }

}
