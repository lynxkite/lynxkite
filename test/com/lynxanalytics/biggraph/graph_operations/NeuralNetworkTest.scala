package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class NeuralNetworkTest extends FunSuite with TestGraphOp {
  test("lattice") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val sideNum = DeriveJS.deriveFromAttributes[Double](
      "side === '' ? undefined : side === 'left' ? -1.0 : 1.0", Seq("side" -> g.attrs("side")),
      g.vertices).attr
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 20, iterations = 1000, radius = 4,
        hideState = false, forgetFraction = 0.0)
      op(op.edges, g.edges)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices).attr
    assert(isWrong.rdd.values.collect.sum == 0)
  }
}
