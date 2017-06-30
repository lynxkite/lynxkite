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
    val diff = DeriveJS.deriveFromAttributes(
      "(a - b) * (a - b)",
      Seq("a" -> a, "b" -> b),
      a.vertexSet).runtimeSafeCast[Double]
    diff.rdd.values.sum
  }

  // Non-distributed training.
  def PredictViaNNOnGraphV1Simple(featureCount: Int,
                                  networkSize: Int,
                                  learningRate: Double,
                                  radius: Int,
                                  hideState: Boolean,
                                  forgetFraction: Double,
                                  iterations: Int,
                                  seed: Int = 15,
                                  knownLabelWeight: Double = 0.5,
                                  gradientCheckOn: Boolean = false,
                                  networkLayout: String = "GRU") = PredictViaNNOnGraphV1(
    featureCount, networkSize, learningRate, radius, hideState, forgetFraction,
    knownLabelWeight = knownLabelWeight,
    seed = seed,
    iterationsInTraining = iterations,
    trainingRadius = -1, // No sampling.
    maxTrainingVertices = 1, minTrainingVertices = 1,
    subgraphsInTraining = 1, numberOfTrainings = 1,
    gradientCheckOn = gradientCheckOn, networkLayout = networkLayout)

  // Just output the label.
  test("label, trivial") {
    // The label is a random attribute. It is visible to the vertex.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).deriveX[Double]("x < 0 ? -1 : 1")
    val prediction = {
      val op = PredictViaNNOnGraphV1Simple(
        featureCount = 0, networkSize = 2, learningRate = 0.5, radius = 0,
        hideState = false, forgetFraction = 0.0, iterations = 60,
        gradientCheckOn = false, networkLayout = "LSTM")
      op(op.edges, vs.emptyEdgeBundle)(op.label, a).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn to use a feature.
  test("feature, trivial") {
    // The label and one of the features are the same random attribute.
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(0).deriveX[Double]("x < 0 ? -1 : 1")
    val b = vs.randomAttribute(1000).deriveX[Double]("x < 0 ? -1 : 1") // Red herring.
    val prediction = {
      val op = PredictViaNNOnGraphV1Simple(
        featureCount = 2, networkSize = 4, learningRate = 0.5, radius = 1,
        hideState = true, forgetFraction = 0.0, iterations = 13,
        gradientCheckOn = false, networkLayout = "GRU")
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
      val op = PredictViaNNOnGraphV1Simple(
        featureCount = 2, networkSize = 4, learningRate = 0.5, radius = 3,
        hideState = true, forgetFraction = 0.0, iterations = 8,
        gradientCheckOn = false, networkLayout = "MLP")
      op(op.edges, vs.emptyEdgeBundle)(op.label, a)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, a) < 1)
  }

  // Learn simple arithmetic. Not possible with this GRU.
  ignore("features, simple") {
    val vs = CreateVertexSet(1000).result.vs
    val a = vs.randomAttribute(100)
    val b = vs.randomAttribute(200)
    val c = DeriveJS.deriveFromAttributes("a - b", Seq("a" -> a, "b" -> b), vs).runtimeSafeCast[Double]
    val prediction = {
      val op = PredictViaNNOnGraphV1Simple(
        featureCount = 2, networkSize = 10, learningRate = 0.2, radius = 0,
        hideState = true, forgetFraction = 0.0, iterations = 50,
        gradientCheckOn = false, networkLayout = "LSTM")
      op(op.edges, vs.emptyEdgeBundle)(op.label, c)(op.features, Seq(a, b)).result.prediction
    }
    assert(differenceSquareSum(prediction, c) < 1)
  }

  // Lattice problem, by hiding state.
  test("lattice, hiding") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val es = g.edges.addReversed
    val sideNum = g.attr[String]("side").deriveX[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = PredictViaNNOnGraphV1Simple(
        featureCount = 0, networkSize = 4, learningRate = 0.1, radius = 3,
        hideState = true, forgetFraction = 0.0, iterations = 35,
        gradientCheckOn = false, networkLayout = "MLP")
      op(op.edges, es)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes(
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices).runtimeSafeCast[Double]
    assert(isWrong.rdd.values.sum == 0)
  }

  // Lattice problem, by forgetting.
  test("lattice, forgetting") {
    val g = TestGraph.fromCSV(
      getClass.getResource("/graph_operations/NeuralNetworkTest/lattice").toString)
    val es = g.edges.addReversed
    val sideNum = g.attr[String]("side").deriveX[Double](
      "x === '' ? undefined : x === 'left' ? -1.0 : 1.0")
    val prediction = {
      val op = PredictViaNNOnGraphV1(
        featureCount = 0, networkSize = 4, learningRate = 0.2, radius = 3,
        hideState = false, forgetFraction = 0.5, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 5, subgraphsInTraining = 5,
        numberOfTrainings = 5, knownLabelWeight = 0.5, seed = 15, gradientCheckOn = false,
        networkLayout = "LSTM")
      op(op.edges, es)(op.label, sideNum).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes(
      "var p = prediction < 0 ? 'left' : 'right'; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> g.attrs("side_truth")),
      g.vertices).runtimeSafeCast[Double]
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
    val truePartition = AddVertexAttribute.run(vertices, inWhichPartition(1000, 400))
    val a = vertices.randomAttribute(13)
    val partition = DeriveJS.deriveFromAttributes(
      "a < -0.7 ? undefined : truePartition",
      Seq("a" -> a, "truePartition" -> truePartition),
      vertices).runtimeSafeCast[Double]

    val prediction = {
      val op = PredictViaNNOnGraphV1(
        featureCount = 0, networkSize = 4, learningRate = 0.2, radius = 4,
        hideState = false, forgetFraction = 0.3, trainingRadius = 1, maxTrainingVertices = 8,
        minTrainingVertices = 7, iterationsInTraining = 3, subgraphsInTraining = 2,
        numberOfTrainings = 1, knownLabelWeight = 0.5, seed = 15, gradientCheckOn = false,
        networkLayout = "MLP")
      op(op.edges, g.result.es)(op.label, partition).result.prediction
    }
    val isWrong = DeriveJS.deriveFromAttributes(
      "var p = prediction < 0 ? -1 : 1; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> truePartition),
      g.result.vs).runtimeSafeCast[Double]
    assert(isWrong.rdd.values.sum == 0)
  }

  //Learn parity of the containing path in a graph consisting of paths.
  ignore("parity of containing path") {
    val numberOfVertices = 1000
    val numberOfPaths = 200
    val lengthOfUnlabeledPaths = 1 to 5

    val r = new Random(9)
    val pathStarts = r.shuffle(1 to numberOfVertices - 1).drop(numberOfVertices - numberOfPaths).sorted
    val edgesOfLabeledVertices = (0 until numberOfVertices).map(v =>
      if (pathStarts.contains(v) && pathStarts.contains(v + 1)) v -> List()
      else if (pathStarts.contains(v) || v == 0) v -> List(v + 1)
      else if (pathStarts.contains(v + 1) || v == numberOfVertices - 1) v -> List(v - 1)
      else v -> List(v - 1, v + 1)).toMap
    var currentVertex = numberOfVertices - 1
    val edgesOfUnlabeledVertices = lengthOfUnlabeledPaths.flatMap { i =>
      (1 to i).map { j =>
        currentVertex += 1
        if (j == 1 && j == i) currentVertex -> List()
        else j match {
          case 1 => currentVertex -> List(currentVertex + 1)
          case `i` => currentVertex -> List(currentVertex - 1)
          case j => currentVertex -> List(currentVertex - 1, currentVertex + 1)
        }
      }
    }
    val edgeList = edgesOfLabeledVertices ++ edgesOfUnlabeledVertices

    val extendedPathStarts = 0 +: pathStarts :+ numberOfVertices
    def inWhichPath(v: Int): Int = {
      (0 until numberOfPaths).indexWhere(i => (extendedPathStarts(i) <= v && v < extendedPathStarts(i + 1)))
    }
    val parityOfContainingPath = {
      (0 until numberOfVertices).map(v =>
        (v, ((extendedPathStarts(inWhichPath(v) + 1) - extendedPathStarts(inWhichPath(v))) % 2) * 2 - 1.0)).toMap
    }

    currentVertex = numberOfVertices - 1
    val unlabeledParityOfContainingPath = lengthOfUnlabeledPaths.flatMap { i =>
      (1 to i).map { j =>
        currentVertex += 1
        currentVertex -> ((i % 2) * 2 - 1.0)
      }
    }.toMap

    val g = SmallTestGraph(edgeList)
    val vertices = g.result.vs
    val trueParityAttrOnLabeledVertices = AddVertexAttribute.run(vertices, parityOfContainingPath)
    val trueParityAttrOnUnlabeledVertices = AddVertexAttribute.run(vertices, unlabeledParityOfContainingPath)
    val a = vertices.randomAttribute(8)
    val parityAttr = DeriveJS.deriveFromAttributes(
      "a < -1 ? undefined : trueParityAttr",
      Seq("a" -> a, "trueParityAttr" -> trueParityAttrOnLabeledVertices),
      vertices).runtimeSafeCast[Double]

    val prediction = {
      val op = PredictViaNNOnGraphV1(
        featureCount = 0, networkSize = 10, learningRate = 0.1, radius = 4,
        hideState = false, forgetFraction = 0.6, trainingRadius = 4, maxTrainingVertices = 20,
        minTrainingVertices = 10, iterationsInTraining = 10, subgraphsInTraining = 10,
        numberOfTrainings = 10, knownLabelWeight = 0.5, seed = 15, gradientCheckOn = false,
        networkLayout = "MLP")
      op(op.edges, g.result.es)(op.label, parityAttr).result.prediction
    }
    val isWrongOnLabeled = DeriveJS.deriveFromAttributes(
      "var p = prediction < 0 ? -1 : 1; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> trueParityAttrOnLabeledVertices),
      g.result.vs).runtimeSafeCast[Double]
    assert(isWrongOnLabeled.rdd.values.sum == 0)

    val isWrongOnUnlabeled = DeriveJS.deriveFromAttributes(
      "var p = prediction < 0 ? -1 : 1; p === truth ? 0.0 : 1.0;",
      Seq("prediction" -> prediction, "truth" -> trueParityAttrOnUnlabeledVertices),
      g.result.vs).runtimeSafeCast[Double]
    assert(isWrongOnUnlabeled.rdd.values.sum == 0)
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
      val maxPr = vs.const(realPr.rdd.values.max)
      val minPr = vs.const(realPr.rdd.values.min)
      DeriveJS.deriveFromAttributes(
        "(realPr - minPr) / (maxPr - minPr) * 2 - 1",
        Seq("realPr" -> realPr, "minPr" -> minPr, "maxPr" -> maxPr), vs).runtimeSafeCast[Double]
    }
    val a = vs.randomAttribute(6)
    val pr = DeriveJS.deriveFromAttributes(
      "a < -1 ? undefined : truePr", Seq("a" -> a, "truePr" -> truePr), vs).runtimeSafeCast[Double]

    val prediction = {
      val op = PredictViaNNOnGraphV1(
        featureCount = 0, networkSize = 4, learningRate = 0.01, radius = 3,
        hideState = false, forgetFraction = 0.25, trainingRadius = 3, maxTrainingVertices = 10,
        minTrainingVertices = 5, iterationsInTraining = 2, subgraphsInTraining = 30,
        numberOfTrainings = 50, knownLabelWeight = 0.4, seed = 15, gradientCheckOn = false,
        networkLayout = "LSTM")
      op(op.edges, es)(op.label, pr).result.prediction
    }
    assert(differenceSquareSum(prediction, truePr) < 10)
  }
}
