package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.DataForDecisionTreeTests.trainingData

class TrainDecisionTreeRegressorTest extends ModelTestBase {
  test("test decision tree regression") {
    val m = model(
      method = "Decision tree regression",
      labelName = trainingData.labelName,
      label = trainingData.label,
      featureNames = trainingData.featureNames,
      attrs = trainingData.attrs,
      graph(trainingData.vertexNumber))
    val s = m.value.statistics.get
    assert(s == """DecisionTreeRegressionModel of depth 3 with 7 nodes
  If (rain <= 0.0)
   If (temperature <= 20.0)
    If (temperature <= -15.0)
     Predict: 1.3333333333333333
    Else (temperature > -15.0)
     Predict: 2.0
   Else (temperature > 20.0)
    Predict: 1.0
  Else (rain > 0.0)
   Predict: 0.0

Root mean squared error: 0.28867513459481287
Mean squared error: 0.08333333333333333
R-squared: 0.8632478632478633
Mean absolute error: 0.16666666666666666""")
  }
}
