package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.graph_operations._

import util.Random

class NeuralNetworkTest extends FunSuite with TestGraphOp {
  def differenceSquareSum(a: Attribute[Double], b: Attribute[Double]): Double = {
    val diff = DeriveJS.deriveFromAttributes[Double](
      "(a - b) * (a - b)",
      Seq("a" -> a, "b" -> b),
      a.vertexSet)
    diff.rdd.values.sum
  }

  // Just output the label.
  ignore("label, trivial") {
    // The label is a random attribute. It is visible to the vertex.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).deriveX[Double]("x < 0 ? -1 : 1")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 2, learningRate = 0.1, radius = 0,
        hideState = false, forgetFraction = 0.0, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, vs.emptyEdgeBundle)(op.label, a).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn to use a feature.
  ignore("feature, trivial") {
    // The label and one of the features are the same random attribute.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).deriveX[Double]("x < 0 ? -1 : 1")
    val b = vs.randomAttribute(1000).deriveX[Double]("x < 0 ? -1 : 1") // Red herring.
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 4, learningRate = 0.1, radius = 0,
        hideState = true, forgetFraction = 0.0, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, vs.emptyEdgeBundle)(op.label, a)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn to use a feature with depth.
  test("feature, trivial, deep") {
    // The label and one of the features are the same random attribute.
    // Propagates through 3 full layers.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).deriveX[Double]("x < 0 ? -1 : 1")
    val b = vs.randomAttribute(1000).deriveX[Double]("x < 0 ? -1 : 1") // Red herring.
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 4, learningRate = 0.2, radius = 3,
        hideState = true, forgetFraction = 0.0, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, vs.emptyEdgeBundle)(op.label, a)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn simple arithmetic. Not possible with this GRU.
  ignore("features, simple") {
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(100)
    val b = vs.randomAttribute(200)
    val c = DeriveJS.deriveFromAttributes[Double]("a - b", Seq("a" -> a, "b" -> b), vs)
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 2, networkSize = 4, learningRate = 0.2, radius = 0,
        hideState = true, forgetFraction = 0.0, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, vs.emptyEdgeBundle)(op.label, c)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, c) < 1)
  }

  // Lattice problem, by hiding state.
  test("lattice, hiding") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val sideNum = g.attr[String]("side").deriveX[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, learningRate = 1, radius = 3,
        hideState = true, forgetFraction = 0.0, trainingRadius = 3, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 9)
      op(op.edges, g.edges)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices)
    assert(isWrong.rdd.values.sum == 0)
  }

  // Lattice problem, by forgetting.
  ignore("lattice, forgetting") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val sideNum = g.attr[String]("side").deriveX[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, learningRate = 0.1, radius = 3,
        hideState = false, forgetFraction = 0.5, trainingRadius = 3, maxTrainingVertices = 30,
        minTrainingVertices = 30, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, g.edges)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices)
    assert(isWrong.rdd.values.sum == 0)
  }

  //Learning partition in a bipartite graph
  test("bipartite") {
    def neighbors(total: Int, partition1: Int, vertex: Int): Seq[Int] = {
      if (vertex < partition1) partition1 until total
      else 0 until partition1
    }
    def edgeListsOfCompleteBipartiteGraph(total: Int, partition1: Int): Map[Int, Seq[Int]] = {
      (0 until total).map(v => v -> neighbors(total, partition1, v)).toMap
    }
    def inWhichPartition(total: Int, partition1: Int): Map[Int, Double] = {
      (0 until total).map(v => if (v < partition1) (v, 1.0) else (v, -1.0)).toMap
    }

    val g = SmallTestGraph(edgeListsOfCompleteBipartiteGraph(1000, 400))
    val vertices = g.result.vs
    val partition = AddVertexAttribute.run(vertices, inWhichPartition(1000, 400))

    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 4, learningRate = 0.02, radius = 3,
        hideState = true, forgetFraction = 0.0, trainingRadius = 1, maxTrainingVertices = 8,
        minTrainingVertices = 7, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, g.result.es)(op.label, partition).result.prediction
    }
    prediction.rdd.count // HACK: NullPointerException otherwise.
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? -1 : 1; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> partition),
      g.result.vs)
    assert(isWrong.rdd.values.sum == 0)
  }

  //Learn parity of the containing path in a graph consisting of paths.
  ignore("parity of containing path") {
    val numberOfVertices = 1000
    val numberOfPaths = 200

    val r = new Random(9)
    val pathStarts = r.shuffle(1 to numberOfVertices - 1).drop(numberOfVertices - numberOfPaths).sorted
    val edgeList = (0 until numberOfVertices).map(v =>
      if (pathStarts.contains(v) && pathStarts.contains(v + 1)) v -> List()
      else if (pathStarts.contains(v) || v == 0) v -> List(v + 1)
      else if (pathStarts.contains(v + 1) || v == numberOfVertices - 1) v -> List(v - 1)
      else v -> List(v - 1, v + 1)).toMap

    val extendedPathStarts = 0 +: pathStarts :+ (numberOfVertices - 1)
    def inWhichPath(v: Int): Int = {
      (0 until numberOfPaths).indexWhere(i => (extendedPathStarts(i) <= v && v < extendedPathStarts(i + 1)))
    }
    val parityOfContainingPath = {
      (0 until numberOfVertices - 1).map(v =>
        (v, ((extendedPathStarts(inWhichPath(v) + 1) - extendedPathStarts(inWhichPath(v))) % 2) * 2 - 1.0)).toMap
    }

    val g = SmallTestGraph(edgeList)
    val vertices = g.result.vs
    val trueParityAttr = AddVertexAttribute.run(vertices, parityOfContainingPath)
    val a = vertices.randomAttribute(8)
    val parityAttr = DeriveJS.deriveFromAttributes[Double](
      "a < -1 ? undefined : trueParityAttr",
      Seq("a" -> a, "trueParityAttr" -> trueParityAttr),
      vertices)

    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 10, learningRate = 0.1, radius = 4,
        hideState = true, forgetFraction = 0.3, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, g.result.es)(op.label, parityAttr).result.prediction
    }
    prediction.rdd.count
    val isWrong = DeriveJS.deriveFromAttributes[Double](
      "var p = prediction < 0 ? -1 : 1; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> trueParityAttr),
      g.result.vs)
    assert(isWrong.rdd.values.sum == 0)
  }

  // learn Page Rank
  ignore("Page Rank") {
    val vs = CreateVertexSet(1000).result.vs
    val es = {
      val eop = FastRandomEdgeBundle(seed = 7, averageDegree = 20)
      eop(eop.vs, vs).result.es.addReversed
    }
    val truePr = {
      val weight = es.idSet.const(1.0)
      val op = PageRank(dampingFactor = 0.5, iterations = 3)
      val realPr = op(op.es, es)(op.weights, weight).result.pagerank
      DeriveJS.deriveFromAttributes[Double](
        "realPr * 2 - 1", Seq("realPr" -> realPr), vs)

    }
    val a = vs.randomAttribute(6)
    val pr = DeriveJS.deriveFromAttributes[Double](
      "a < -1 ? undefined : truePr", Seq("a" -> a, "truePr" -> truePr), vs)

    val prediction = {
      val op = NeuralNetwork(
        featureCount = 0, networkSize = 20, learningRate = 0.5, radius = 4,
        hideState = true, forgetFraction = 0.0, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 50, subgraphsInTraining = 10,
        numberOfTrainings = 10)
      op(op.edges, es)(op.label, pr).result.prediction
    }
    prediction.rdd.count
    assert(differenceSquareSum(prediction, truePr) < 10)
  }
}
