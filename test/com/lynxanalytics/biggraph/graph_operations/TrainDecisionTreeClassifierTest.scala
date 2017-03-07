package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._

class TrainDecisionTreeClassifierTest extends ModelTestBase {
  test("train a decision tree classifier") {
    val m = model(
      method = "Decision tree classification",
      labelName = "length of the walk",
      label = Map(0 -> 1, 1 -> 0, 2 -> 0, 3 -> 2, 4 -> 1, 5 -> 0, 6 -> 1, 7 -> 2),
      featureNames = List("temperature", "rain"),
      attrs = Seq(Map(0 -> -15, 1 -> 20, 2 -> -10, 3 -> 20, 4 -> 35, 5 -> 40, 6 -> -15, 7 -> -15),
        Map(0 -> 0, 1 -> 1, 2 -> 1, 3 -> 0, 4 -> 0, 5 -> 1, 6 -> 0, 7 -> 0)),
      // I love making long walks if it's not raining and the temperature is
      // pleasant. I take only a short walk if it's not raining, but the weather
      // is too hot or too cold. I hate rain, so I just stay at home if it's raining.
      // Sometimes I'm in a really good mood and go on a long walk in spite of
      // the cold weather.
      graph(8))

    assert(m.value.method == "Decision tree classification")
    assert(m.value.featureNames.sorted == List("rain", "temperature"))
    val s = m.value.statistics.get
    assert(s ==
      """DecisionTreeClassificationModel of depth 3 with 7 nodes
  If (rain <= 0.0)
   If (temperature <= 20.0)
    If (temperature <= -15.0)
     Predict: 1.0
    Else (temperature > -15.0)
     Predict: 2.0
   Else (temperature > 20.0)
    Predict: 1.0
  Else (rain > 0.0)
   Predict: 0.0

Accuracy: 0.875
Support: [0.375, 0.375, 0.25]""")
  }
}
