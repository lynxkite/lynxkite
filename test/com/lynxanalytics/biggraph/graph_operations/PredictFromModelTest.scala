package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class PredictFromModelTest extends ModelTestBase {
  def checkModel(method: String) {
    val g = graph(4)
    val m = model(
      method = method,
      labelName = "age",
      label = Map(0 -> 25, 1 -> 40, 2 -> 30, 3 -> 60),
      featureNames = List("yob"),
      attrs = Seq(Map(0 -> 1990, 1 -> 1975, 2 -> 1985, 3 -> 1955)),
      graph = g)

    val yob = Seq(AddVertexAttribute.run(g.vs, Map(0 -> 2000.0)))
    val age = predict(m, yob).rdd.values.collect()(0)
    assertRoughlyEquals(age, 15, 1)
  }

  test("test different types of linear regression") {
    checkModel("Linear regression")
    checkModel("Ridge regression")
    checkModel("Lasso")
  }

  test("test decision tree regression") {
    import com.lynxanalytics.biggraph.graph_operations.DataForDecisionTreeTests.{
      trainingData,
      testDataForRegression
    }
    val m = model(
      method = "Decision tree regression",
      labelName = trainingData.labelName,
      label = trainingData.label,
      featureNames = trainingData.featureNames,
      attrs = trainingData.attrs,
      graph(trainingData.vertexNumber))

    val g = graph(testDataForRegression.vertexNumber)
    val attrs = testDataForRegression.attrs
    val features = attrs.map(attr => {
      AddVertexAttribute.run[Double](g.vs, attr)
    })
    val op = PredictFromModel(testDataForRegression.featureNames.size)
    val result = op(op.features, features)(op.model, m).result
    val prediction = result.prediction.rdd.collect.toMap
    assert(prediction.size == 6)
    assertRoughlyEquals(prediction, testDataForRegression.label, 0.1)
  }
}
