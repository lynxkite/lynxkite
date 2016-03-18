package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class NeuralNetworkTest extends FunSuite with TestGraphOp {
  test("lattice") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val op = NeuralNetwork(0)
    val nonEmpty = DeriveJS.deriveFromAttributes[String](
      "side === '' ? undefined : side", Seq("side" -> g.attrs("side")),
      g.vertices).attr
    val isRight = DeriveJS.deriveFromAttributes[Double](
      "side === 'right' ? 1.0 : 0.0", Seq("side" -> nonEmpty),
      g.vertices).attr
    val isRightTruth = DeriveJS.deriveFromAttributes[Double](
      "side === 'right' ? 1.0 : 0.0", Seq("side" -> g.attrs("side_truth")),
      g.vertices).attr
    val prediction = op(op.edges, g.edges)(op.label, isRight).result.prediction
    assert(prediction.rdd.collect.toSet == isRightTruth.rdd.collect.toSet)
  }
}
