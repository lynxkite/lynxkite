package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._

class NeuralNetworkTest extends FunSuite with TestGraphOp {
  def differenceSquareSum(a: Attribute[Double], b: Attribute[Double]): Double = {
    val diff = DeriveJS.deriveFromAttributes[Double](
      "(a - b) * (a - b)",
      Seq("a" -> a, "b" -> b),
      a.vertexSet).attr
    diff.rdd.values.sum
  }

  // Just output the label.
  test("label, trivial") {
    // The label is a random attribute. It is visible to the vertex.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).derive[Double]("x < 0 ? -1 : 1")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 2, iterations = 5, learningRate = 0.5, radius = 0,
        hideState = false, forgetFraction = 0.0)
      op(op.edges, vs.emptyEdgeBundle)(op.label, a).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn to use a feature. Not possible with this GRU.
  ignore("feature, trivial") {
    // The label and one of the features are the same random attribute.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).derive[Double]("x < 0 ? -1 : 1")
    val b = vs.randomAttribute(1000).derive[Double]("x < 0 ? -1 : 1") // Red herring.
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 4, iterations = 30, learningRate = 0.1, radius = 0,
        hideState = true, forgetFraction = 0.0)
      op(op.edges, vs.emptyEdgeBundle)(op.label, a)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn to use a feature with depth. Not possible with this GRU.
  ignore("feature, trivial, deep") {
    // The label and one of the features are the same random attribute.
    // Propagates through 3 full layers.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).derive[Double]("x < 0 ? -1 : 1")
    val b = vs.randomAttribute(1000).derive[Double]("x < 0 ? -1 : 1") // Red herring.
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 4, iterations = 10, learningRate = 0.2, radius = 3,
        hideState = true, forgetFraction = 0.0)
      op(op.edges, vs.emptyEdgeBundle)(op.label, a)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn simple arithmetic. Not possible with this GRU.
  ignore("features, simple") {
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(100)
    val b = vs.randomAttribute(200)
    val c = DeriveJS.deriveFromAttributes[Double]("a - b", Seq("a" -> a, "b" -> b), vs).attr
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 4, iterations = 1, learningRate = 0.2, radius = 0,
        hideState = true, forgetFraction = 0.0)
      op(op.edges, vs.emptyEdgeBundle)(op.label, c)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, c) < 1)
  }

  // Lattice problem, by hiding state.
  test("lattice, hiding") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val sideNum = g.attr[String]("side").derive[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, iterations = 25, learningRate = 0.2, radius = 3,
        hideState = true, forgetFraction = 0.0)
      op(op.edges, g.edges)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices).attr
    assert(isWrong.rdd.values.sum == 0)
  }

  // Lattice problem, by forgetting. Does not learn well.
  //
  // I believe with low fraction of forgetting the direction of learning will
  // be to predict from the known state. Vertices pushing for the generalized solution are
  // outnumbered by the vertices pushing for the trivial solution. With a high fraction of
  // forgetting the network falls apart and most vertices are in the dark.
  //
  // In theory both problems should be overcome by a high number of iterations, but I could not find
  // a setup that works.
  ignore("lattice, forgetting") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val sideNum = g.attr[String]("side").derive[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, iterations = 200, learningRate = 0.1, radius = 3,
        hideState = false, forgetFraction = 0.6)
      op(op.edges, g.edges)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices).attr
    assert(isWrong.rdd.values.sum == 0)
  }
}
