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

  // Lattice problem, by forgetting.
  test("lattice, forgetting") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val sideNum = g.attr[String]("side").derive[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, iterations = 30, learningRate = 0.2, radius = 3,
        hideState = false, forgetFraction = 0.5)
      op(op.edges, g.edges)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices).attr
    assert(isWrong.rdd.values.sum == 0)
  }

  test("bipartite") {
    def neighbours(total: Int, partition1: Int, vertex: Int): Seq[Int] = {
      if (vertex < partition1) List.range(partition1, total)
      else List.range(0, partition1)
    }
    def edgeListsOfCompleteBipartiteGraph(total: Int, partition1: Int): Map[Int, Seq[Int]] = {
      val edgeList = {
        for {
          v <- Range(0, total)
        } yield {
          (v, neighbours(total, partition1, v))
        }
      }
      edgeList.toMap
    }
    def inWhichPartition(total: Int, partition1: Int): Map[Int, Double] = {
      val partition = {
        for {
          v <- Range(0, total)
        } yield {
          if (v < partition1) (v, 1.0)
          else (v, -1.0)
        }
      }
      partition.toMap
    }

    val g = SmallTestGraph(edgeListsOfCompleteBipartiteGraph(10, 5))
    val vertices = g.result.vs
    val partition = AddVertexAttribute.run(vertices, inWhichPartition(10, 5))

    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, iterations = 25, learningRate = 0.2, radius = 3,
        hideState = true, forgetFraction = 0.0)
      op(op.edges, g.result.es)(op.label, partition).result.prediction
    }
    prediction.rdd.count // HACK: NullPointerException otherwise.
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? -1 : 1; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> partition),
      g.result.vs).attr
    assert(isWrong.rdd.values.sum == 0)
  }

}
