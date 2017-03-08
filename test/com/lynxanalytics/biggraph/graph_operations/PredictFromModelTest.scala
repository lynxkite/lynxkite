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
    val m = model(
      method = "Decision tree regression",
      labelName = "length of the walk",
      label = Map(0 -> 1, 1 -> 0, 2 -> 0, 3 -> 7, 4 -> 1, 5 -> 0, 6 -> 1, 7 -> 6),
      featureNames = List("temperature", "rain"),
      attrs = Seq(Map(0 -> -15, 1 -> 20, 2 -> -10, 3 -> 20, 4 -> 35, 5 -> 40, 6 -> -15, 7 -> -15),
        Map(0 -> 0, 1 -> 1, 2 -> 1, 3 -> 0, 4 -> 0, 5 -> 1, 6 -> 0, 7 -> 0)),
      // I love making long walks if it's not raining and the temperature is
      // pleasant. I take only a short walk if it's not raining, but the weather
      // is too hot or too cold. I hate rain, so I just stay at home if it's raining.
      // Sometimes I'm in a really good mood and go on a long walk in spite of
      // the cold weather.
      graph(8))

    val g = graph(numVertices = 6)
    val attrs = Seq(Map(0 -> 20.0, 1 -> 42.0, 2 -> 38.0, 3 -> -16.0, 4 -> -20.0, 5 -> 20.0),
      Map(0 -> 0.0, 1 -> 1.0, 2 -> 0.0, 3 -> 0.0, 4 -> 1.0, 5 -> 1.0))
    val features = attrs.map(attr => {
      AddVertexAttribute.run[Double](g.vs, attr)
    })
    val op = PredictFromModel(2)
    val result = op(op.features, features)(op.model, m).result
    val prediction = result.prediction.rdd.values.collect
    assert(prediction.size == 6)
    assert(prediction(0) == 7.0)
    assert(prediction(1) == 0.0)
    assert(prediction(2) == 1.0)
    assert(prediction(3) > 2.66 && prediction(3) < 2.67)
    assert(prediction(4) == 0.0)
    assert(prediction(5) == 0.0)
  }
}
