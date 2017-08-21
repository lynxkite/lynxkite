package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations.DataForDecisionTreeTests.trainingData

class TrainDecisionTreeClassifierTest extends ModelTestBase {
  test("train a decision tree classifier") {
    val m = model(
      method = "Decision tree classification",
      labelName = trainingData.labelName,
      label = trainingData.label,
      featureNames = trainingData.featureNames,
      attrs = trainingData.attrs,
      graph(trainingData.vertexNumber))

    assert(m.value.method == "Decision tree classification")
    assert(m.value.featureNames.sorted == List("rain", "temperature"))
    val s = m.value.statistics.get
    assert(s == """DecisionTreeClassificationModel of depth 3 with 7 nodes
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
    assert(m.value.toSQL(sparkContext) == """CASE
 WHEN rain <= 0.0 THEN
  CASE
   WHEN temperature <= 20.0 THEN
    CASE
     WHEN temperature <= -15.0 THEN
      1.0
     ELSE
      2.0
    END
   ELSE
    1.0
  END
 ELSE
  0.0
END AS length of the walk""")
  }
}
