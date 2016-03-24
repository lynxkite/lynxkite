package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._

class NeuralNetworkTest extends FunSuite with TestGraphOp {
  test("features") {
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(100)
    val b = vs.randomAttribute(200)
    val c = DeriveJS.deriveFromAttributes[Double]("a - b", Seq("a" -> a, "b" -> b), vs).attr

    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 20, iterations = 1000, radius = 0,
        hideState = false, forgetFraction = 0.0)
      op(op.edges, vs.emptyEdgeBundle)(op.label, c)(op.features, Seq(a, b)).result.prediction
    }
    val error = DeriveJS.deriveFromAttributes[Double](
      "(prediction - c) * (prediction - c)",
      Seq("prediction" -> prediction, "c" -> c),
      vs).attr
    assert(error.rdd.values.collect.sum == 0)
  }

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
