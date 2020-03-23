package com.lynxanalytics.biggraph.graph_operations

import org.scalatest.FunSuite

import Math.round

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class GCNTest extends FunSuite with TestGraphOp {
  // Create graph with two disjoint triangles. The target variable is the id of
  // the connected component.
  val graph = SmallTestGraph(
    Map(
      0 -> Seq(1, 2),
      1 -> Seq(0, 2),
      2 -> Seq(0, 1),
      3 -> Seq(4, 5),
      4 -> Seq(3, 5),
      5 -> Seq(3, 4))).result
  val label = AddVertexAttribute.run(graph.vs, Map(1 -> 0.0, 2 -> 0.0, 4 -> 1.0, 5 -> 1.0))
  val features = AddVertexAttribute.run(
    graph.vs,
    Map(0 -> Vector(0.0), 1 -> Vector(0.0), 2 -> Vector(0.0),
      3 -> Vector(0.0), 4 -> Vector(0.0), 5 -> Vector(0.0)))

  def predict(model: Scalar[SphynxModel]): Map[ID, Double] = {
    val predOp = PredictWithGCN()
    val pred = predOp(predOp.vs, graph.vs)(
      predOp.es, graph.es)(
        predOp.label, label)(
          predOp.features, features)(
            predOp.model, model).result.prediction
    get(pred)
  }

  test("train GCN classifier", com.lynxanalytics.biggraph.SphynxOnly) {
    for (convOp <- Seq("GCNConv", "GatedGraphConv")) {
      val op = TrainGCNClassifier(
        iterations = 1000,
        forget = true,
        batchSize = 1,
        learningRate = 0.003,
        numConvLayers = 2,
        hiddenSize = 4,
        convOp = convOp,
        seed = 1)

      val result = op(op.vs, graph.vs)(
        op.es, graph.es)(
          op.label, label)(
            op.features, features).result
      val trainAcc = result.trainAcc.value
      assert(trainAcc == 1)
      val model = result.model
      val pred = predict(model)
      assert(pred == Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 1.0, 4 -> 1.0, 5 -> 1.0))
    }
  }

  test("train GCN regressor", com.lynxanalytics.biggraph.SphynxOnly) {
    for (convOp <- Seq("GCNConv", "GatedGraphConv")) {
      val op = TrainGCNRegressor(
        iterations = 1000,
        forget = true,
        batchSize = 1,
        learningRate = 0.003,
        numConvLayers = 2,
        hiddenSize = 4,
        convOp = convOp,
        seed = 1)
      val result = op(op.vs, graph.vs)(
        op.es, graph.es)(
          op.label, label)(
            op.features, features).result
      val trainMSE = result.trainMSE.value
      assert(trainMSE < 0.1)
      val model = result.model
      val pred = predict(model).mapValues(round(_))
      assert(pred == Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 1.0, 4 -> 1.0, 5 -> 1.0))
    }
  }
}
