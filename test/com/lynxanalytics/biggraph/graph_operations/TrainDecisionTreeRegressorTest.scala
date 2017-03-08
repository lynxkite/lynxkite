package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class TrainDecisionTreeRegressorTest extends ModelTestBase {
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
    val s = m.value.statistics.get
    assert(s == """DecisionTreeRegressionModel of depth 3 with 7 nodes
  If (rain <= 0.0)
   If (temperature <= 20.0)
    If (temperature <= -15.0)
     Predict: 2.6666666666666665
    Else (temperature > -15.0)
     Predict: 7.0
   Else (temperature > 20.0)
    Predict: 1.0
  Else (rain > 0.0)
   Predict: 0.0

Root mean squared error: 1.4433756729740645
Mean squared error: 2.0833333333333335
R-squared: 0.7023809523809523
Mean absolute error: 0.8333333333333333""")
  }
}
